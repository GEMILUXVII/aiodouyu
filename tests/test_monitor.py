"""Tests for the confirmed live-status monitor."""

import asyncio
from types import SimpleNamespace

import pytest

import aiodouyu.monitor as monitor_module
from aiodouyu import LiveStatusMonitor


class FakeDanmakuClient:
    """Minimal async client used to inject protocol messages."""

    def __init__(self):
        self.queue = asyncio.Queue()
        self.closed = False

    def push(self, message):
        self.queue.put_nowait(message)

    async def close(self):
        if not self.closed:
            self.closed = True
            self.queue.put_nowait(None)

    def __aiter__(self):
        return self._messages()

    async def _messages(self):
        while True:
            message = await self.queue.get()
            if message is None:
                return
            yield message


@pytest.mark.asyncio
async def test_rss_transitions_require_http_confirmation(monkeypatch):
    monkeypatch.setattr(monitor_module, "RECONCILE_INTERVAL", 0.01)
    monkeypatch.setattr(monitor_module, "RSS_CONFIRM_RETRY_INTERVAL", 0.01)
    monkeypatch.setattr(monitor_module, "RSS_CONFIRM_WINDOW", 0.02)
    http_status = {"is_live": True}
    calls = []

    async def fake_fetch_room(room_id, *, source, timeout):
        calls.append((room_id, source))
        return SimpleNamespace(
            is_live=http_status["is_live"],
            started_at=100.0 if http_status["is_live"] else None,
        )

    monkeypatch.setattr(monitor_module, "fetch_room", fake_fetch_room)
    client = FakeDanmakuClient()
    events = []
    monitor = LiveStatusMonitor(
        12725169,
        live_callback=lambda room_id, message: events.append("live"),
        offline_callback=lambda room_id, duration: events.append("offline"),
        client_factory=lambda: client,
        notify_cooldown=0,
        offline_confirmation=0,
    )
    monitor.start()

    client.push({"type": "rss", "ss": "1", "ivl": "0"})
    await asyncio.sleep(0.05)
    assert events == ["live"]

    # A contradictory rss packet must not emit offline while HTTP says live.
    client.push({"type": "rss", "ss": "0", "ivl": "0"})
    await asyncio.sleep(0.05)
    assert events == ["live"]
    assert monitor.last_live_status is True
    assert monitor._pending_status is None

    http_status["is_live"] = False
    client.push({"type": "rss", "ss": "0", "ivl": "0"})
    await asyncio.sleep(0.05)
    assert events == ["live", "offline"]
    assert monitor.last_live_status is False
    assert len(calls) >= 3
    assert set(calls) == {(12725169, "betard")}

    await monitor.stop()


@pytest.mark.asyncio
async def test_rss_candidate_waits_for_delayed_http_state(monkeypatch):
    monkeypatch.setattr(monitor_module, "RECONCILE_INTERVAL", 0.005)
    monkeypatch.setattr(monitor_module, "RSS_CONFIRM_RETRY_INTERVAL", 0.01)
    monkeypatch.setattr(monitor_module, "RSS_CONFIRM_WINDOW", 0.1)
    observations = iter([False, True])
    events = []

    async def fake_fetch_room(room_id, *, source, timeout):
        is_live = next(observations)
        return SimpleNamespace(
            is_live=is_live,
            started_at=100.0 if is_live else None,
        )

    monkeypatch.setattr(monitor_module, "fetch_room", fake_fetch_room)
    client = FakeDanmakuClient()
    monitor = LiveStatusMonitor(
        2,
        live_callback=lambda room_id, message: events.append("live"),
        client_factory=lambda: client,
        notify_cooldown=0,
    )
    monitor.start()

    client.push({"type": "rss", "ss": "1", "ivl": "0"})
    await asyncio.sleep(0.08)

    assert events == ["live"]
    assert monitor.last_live_status is True
    await monitor.stop()


@pytest.mark.asyncio
async def test_transient_offline_requires_stable_http_confirmation(monkeypatch):
    monkeypatch.setattr(monitor_module, "RECONCILE_INTERVAL", 0.005)
    http_status = {"is_live": True}
    events = []

    async def fake_fetch_room(room_id, *, source, timeout):
        return SimpleNamespace(
            is_live=http_status["is_live"],
            started_at=100.0 if http_status["is_live"] else None,
        )

    monkeypatch.setattr(monitor_module, "fetch_room", fake_fetch_room)
    client = FakeDanmakuClient()
    monitor = LiveStatusMonitor(
        3,
        live_callback=lambda room_id, message: events.append("live"),
        offline_callback=lambda room_id, duration: events.append("offline"),
        client_factory=lambda: client,
        notify_cooldown=0,
        offline_confirmation=0.05,
    )
    monitor.start()

    client.push({"type": "rss", "ss": "1", "ivl": "0"})
    await asyncio.sleep(0.03)
    assert events == ["live"]

    http_status["is_live"] = False
    client.push({"type": "rss", "ss": "0", "ivl": "0"})
    await asyncio.sleep(0.03)
    assert events == ["live"]

    http_status["is_live"] = True
    await asyncio.sleep(0.06)
    assert events == ["live"]
    assert monitor.last_live_status is True

    http_status["is_live"] = False
    client.push({"type": "rss", "ss": "0", "ivl": "0"})
    await asyncio.sleep(0.09)
    assert events == ["live", "offline"]
    assert monitor.last_live_status is False

    await monitor.stop()


@pytest.mark.asyncio
async def test_failed_confirmation_does_not_emit_candidate(monkeypatch):
    monkeypatch.setattr(monitor_module, "RECONCILE_INTERVAL", 0.01)

    async def failing_fetch_room(room_id, *, source, timeout):
        raise RuntimeError("temporary HTTP failure")

    monkeypatch.setattr(monitor_module, "fetch_room", failing_fetch_room)
    client = FakeDanmakuClient()
    events = []
    monitor = LiveStatusMonitor(
        1,
        live_callback=lambda room_id, message: events.append("live"),
        client_factory=lambda: client,
    )
    monitor.start()

    client.push({"type": "rss", "ss": "1"})
    await asyncio.sleep(0.05)

    assert events == []
    assert monitor.last_live_status is None
    assert monitor._pending_status is True
    assert monitor._pending_needs_resync is True
    assert monitor._resync_pending is True
    assert monitor.is_healthy

    await monitor.stop()
