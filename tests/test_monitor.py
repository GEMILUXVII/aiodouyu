"""Tests for the confirmed live-status monitor."""

import asyncio
import json
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
async def test_live_rss_is_immediate_but_offline_requires_http(monkeypatch):
    monkeypatch.setattr(monitor_module, "RECONCILE_INTERVAL", 0.01)
    monkeypatch.setattr(monitor_module, "RSS_CONFIRM_WINDOW", 0)
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
    await asyncio.sleep(0.02)
    assert events == ["live"]
    # The callback is immediate; HTTP confirmation runs only afterward in the
    # monitor's background reconciliation loop.
    assert calls == [(12725169, "betard")]

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
    assert calls == [
        (12725169, "betard"),
        (12725169, "betard"),
        (12725169, "betard"),
    ]

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
async def test_http_live_discards_stale_offline_event_time(monkeypatch):
    now = {"value": 1000.0}
    http_status = {"is_live": True}
    monkeypatch.setattr(monitor_module.time, "time", lambda: now["value"])

    async def fake_fetch_room(room_id, *, source, timeout):
        return SimpleNamespace(
            is_live=http_status["is_live"],
            started_at=1000.0 if http_status["is_live"] else None,
        )

    monkeypatch.setattr(monitor_module, "fetch_room", fake_fetch_room)
    events = []
    monitor = LiveStatusMonitor(
        12725169,
        live_callback=lambda room_id, message: events.append(("live", None)),
        offline_callback=lambda room_id, duration: events.append(
            ("offline", duration, monitor.last_offline_time)
        ),
        notify_cooldown=30,
        offline_confirmation=5,
    )

    monitor._rss_handler({"type": "rss", "ss": "1", "ivl": "0"})
    now["value"] = 1001.0
    monitor._rss_handler({"type": "rss", "ss": "0", "ivl": "0"})

    now["value"] = 1002.0
    await monitor._resync()
    assert monitor._pending_status is None
    assert monitor.last_live_status is True

    http_status["is_live"] = False
    now["value"] = 1030.0
    await monitor._resync()
    assert monitor._pending_observed_at == 1030.0

    now["value"] = 1035.0
    await monitor._resync()

    assert events == [("live", None), ("offline", 30.0, 1030.0)]
    assert monitor.last_live_status is False


@pytest.mark.asyncio
async def test_reverse_rss_and_cached_http_cannot_undo_realtime_live(monkeypatch):
    now = {"value": 1000.0}
    http_status = {"is_live": False}
    monkeypatch.setattr(monitor_module.time, "time", lambda: now["value"])

    async def fake_fetch_room(room_id, *, source, timeout):
        return SimpleNamespace(
            is_live=http_status["is_live"],
            started_at=1000.0 if http_status["is_live"] else None,
        )

    monkeypatch.setattr(monitor_module, "fetch_room", fake_fetch_room)
    events = []
    monitor = LiveStatusMonitor(
        12725169,
        live_callback=lambda room_id, message: events.append("live"),
        offline_callback=lambda room_id, duration: events.append("offline"),
        notify_cooldown=0,
        offline_confirmation=0,
    )

    monitor._rss_handler({"type": "rss", "ss": "1", "ivl": "0"})
    await monitor._resync()
    assert events == ["live"]
    assert monitor.last_live_status is True

    now["value"] = 1000.118
    monitor._rss_handler({"type": "rss", "ss": "0", "ivl": "0"})
    now["value"] = 1001.0
    await monitor._resync()
    assert events == ["live"]
    assert monitor.last_live_status is True
    assert monitor._pending_status is False
    assert monitor._resync_pending is True

    http_status["is_live"] = True
    now["value"] = 1002.0
    await monitor._resync()
    assert events == ["live"]
    assert monitor.last_live_status is True
    assert monitor._pending_status is None


@pytest.mark.asyncio
async def test_loop_rss_blocks_transient_http_live(monkeypatch):
    now = {"value": 1000.0}
    http_status = {"is_live": True, "is_loop": False}
    monkeypatch.setattr(monitor_module.time, "time", lambda: now["value"])

    async def fake_fetch_room(room_id, *, source, timeout):
        return SimpleNamespace(
            is_live=http_status["is_live"],
            is_loop=http_status["is_loop"],
            started_at=1000.0,
        )

    monkeypatch.setattr(monitor_module, "fetch_room", fake_fetch_room)
    events = []
    monitor = LiveStatusMonitor(
        6979222,
        live_callback=lambda room_id, message: events.append("live"),
        inherit_state={"last_live_status": False},
        notify_cooldown=0,
    )

    monitor._rss_handler({"type": "rss", "ss": "1", "ivl": "1"})
    now["value"] = 1010.0
    await monitor._resync()

    assert events == []
    assert monitor.last_live_status is False
    assert monitor._resync_pending is True

    http_status.update(is_live=False, is_loop=True)
    now["value"] = 1012.0
    await monitor._resync()

    assert events == []
    assert monitor.last_live_status is False


def test_realtime_live_rss_overrides_recent_loop_immediately(monkeypatch):
    now = {"value": 1000.0}
    monkeypatch.setattr(monitor_module.time, "time", lambda: now["value"])
    events = []
    monitor = LiveStatusMonitor(
        6979222,
        live_callback=lambda room_id, message: events.append("live"),
        inherit_state={"last_live_status": False},
        notify_cooldown=0,
    )

    monitor._rss_handler({"type": "rss", "ss": "1", "ivl": "1"})
    now["value"] = 1001.0
    monitor._rss_handler({"type": "rss", "ss": "1", "ivl": "0"})

    assert events == ["live"]
    assert monitor.last_live_status is True


@pytest.mark.asyncio
async def test_http_live_recovers_after_loop_guard_expires(monkeypatch):
    now = {"value": 1000.0}
    monkeypatch.setattr(monitor_module.time, "time", lambda: now["value"])

    async def fake_fetch_room(room_id, *, source, timeout):
        return SimpleNamespace(
            is_live=True,
            is_loop=False,
            started_at=1001.0,
        )

    monkeypatch.setattr(monitor_module, "fetch_room", fake_fetch_room)
    events = []
    monitor = LiveStatusMonitor(
        6979222,
        live_callback=lambda room_id, message: events.append("live"),
        inherit_state={"last_live_status": False},
        notify_cooldown=0,
    )

    monitor._rss_handler({"type": "rss", "ss": "1", "ivl": "1"})
    now["value"] += monitor_module.RSS_CONFIRM_WINDOW - 1
    await monitor._resync()
    assert events == []

    now["value"] += 2
    await monitor._resync()

    assert events == ["live"]
    assert monitor.last_live_status is True


@pytest.mark.asyncio
async def test_short_realtime_session_keeps_original_offline_event_time(monkeypatch):
    now = {"value": 1000.0}
    monkeypatch.setattr(monitor_module.time, "time", lambda: now["value"])

    async def fake_fetch_room(room_id, *, source, timeout):
        return SimpleNamespace(is_live=False, started_at=None)

    monkeypatch.setattr(monitor_module, "fetch_room", fake_fetch_room)
    durations = []
    monitor = LiveStatusMonitor(
        12725169,
        live_callback=lambda room_id, message: None,
        offline_callback=lambda room_id, duration: durations.append(duration),
        notify_cooldown=30,
        offline_confirmation=10,
    )

    monitor._rss_handler({"type": "rss", "ss": "1", "ivl": "0"})
    now["value"] = 1000.118
    monitor._rss_handler({"type": "rss", "ss": "0", "ivl": "0"})

    now["value"] = 1001.0
    await monitor._resync()
    assert durations == []

    now["value"] = 1000.118 + monitor_module.RSS_CONFIRM_WINDOW + 1
    await monitor._resync()
    assert durations == []

    now["value"] += 10.1
    await monitor._resync()

    assert durations == [pytest.approx(0.118)]
    assert monitor.last_offline_time == pytest.approx(1000.118)


@pytest.mark.asyncio
async def test_unconfirmed_realtime_live_closes_with_offline_event(monkeypatch):
    now = {"value": 1000.0}
    monkeypatch.setattr(monitor_module.time, "time", lambda: now["value"])

    async def fake_fetch_room(room_id, *, source, timeout):
        return SimpleNamespace(is_live=False, started_at=None)

    monkeypatch.setattr(monitor_module, "fetch_room", fake_fetch_room)
    events = []
    monitor = LiveStatusMonitor(
        12725169,
        live_callback=lambda room_id, message: events.append("live"),
        offline_callback=lambda room_id, duration: events.append("offline"),
        notify_cooldown=0,
        offline_confirmation=0,
    )

    monitor._rss_handler({"type": "rss", "ss": "1", "ivl": "0"})
    await monitor._resync()
    assert monitor.last_live_status is True

    now["value"] += monitor_module.RSS_CONFIRM_WINDOW + 1
    await monitor._resync()

    assert events == ["live", "offline"]
    assert monitor.last_live_status is False
    assert monitor.live_start_time is None


def test_realtime_live_bypasses_notification_cooldown(monkeypatch):
    monkeypatch.setattr(monitor_module.time, "time", lambda: 1000.0)
    events = []
    monitor = LiveStatusMonitor(
        12725169,
        live_callback=lambda room_id, message: events.append("live"),
        notify_cooldown=30,
        inherit_state={
            "last_live_status": False,
            "last_notify_time": 995.0,
        },
    )

    monitor._rss_handler({"type": "rss", "ss": "1", "ivl": "0"})

    assert events == ["live"]
    assert monitor.last_live_status is True


@pytest.mark.asyncio
async def test_live_rss_does_not_depend_on_http(monkeypatch):
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

    assert events == ["live"]
    assert monitor.last_live_status is True
    assert monitor._pending_status is None
    assert monitor._resync_pending is True
    assert monitor.is_healthy

    await monitor.stop()


def test_inherited_pending_transition_requires_fresh_http_confirmation():
    events = []
    monitor = LiveStatusMonitor(
        4,
        offline_callback=lambda room_id, duration: events.append("offline"),
        inherit_state={
            "last_live_status": True,
            "live_start_time": 100.0,
            "has_announced_live": True,
            "last_notify_time": 0.0,
            "pending_status": False,
            "pending_msg": {"type": "aiodouyu.resync"},
            "pending_observed_at": 200.0,
            "pending_needs_resync": False,
        },
    )

    monitor._reconcile_pending()

    assert events == []
    assert monitor.last_live_status is True
    assert monitor._pending_status is False
    assert monitor._pending_needs_resync is True


@pytest.mark.asyncio
async def test_periodic_resync_runs_without_danmaku_connection(monkeypatch):
    """Periodic audits must work even before the danmaku connection is ready."""
    monkeypatch.setattr(monitor_module, "RECONCILE_INTERVAL", 0.005)
    calls = []

    async def fake_fetch_room(room_id, *, source, timeout):
        calls.append((room_id, source))
        return SimpleNamespace(is_live=False, started_at=None)

    monkeypatch.setattr(monitor_module, "fetch_room", fake_fetch_room)
    client = FakeDanmakuClient()
    monitor = LiveStatusMonitor(
        2,
        client_factory=lambda: client,
        periodic_resync_interval=0.02,
        offline_confirmation=0,
    )
    monitor.start()

    await asyncio.sleep(0.08)

    assert len(calls) >= 2
    assert set(calls) == {(2, "betard")}
    assert monitor.last_live_status is False
    assert monitor.connected is False

    await monitor.stop()


@pytest.mark.asyncio
async def test_periodic_resync_recovers_a_missed_short_live_event(monkeypatch):
    """HTTP fallback must detect live even when no rss packet arrives."""
    monkeypatch.setattr(monitor_module, "RECONCILE_INTERVAL", 0.005)
    state = {"is_live": False}
    initial_poll_finished = asyncio.Event()
    live_seen = asyncio.Event()
    events = []

    async def fake_fetch_room(room_id, *, source, timeout):
        if not state["is_live"]:
            initial_poll_finished.set()
        return SimpleNamespace(
            is_live=state["is_live"],
            started_at=100.0 if state["is_live"] else None,
        )

    def on_live(room_id, message):
        events.append((room_id, message["type"]))
        live_seen.set()

    monkeypatch.setattr(monitor_module, "fetch_room", fake_fetch_room)
    monitor = LiveStatusMonitor(
        12725169,
        live_callback=on_live,
        client_factory=FakeDanmakuClient,
        periodic_resync_interval=0.02,
        notify_cooldown=0,
    )
    monitor.start()

    await asyncio.wait_for(initial_poll_finished.wait(), 0.2)
    assert monitor.last_live_status is False

    state["is_live"] = True
    await asyncio.wait_for(live_seen.wait(), 0.2)

    assert events == [(12725169, "aiodouyu.resync")]
    assert monitor.last_live_status is True

    await monitor.stop()


@pytest.mark.asyncio
async def test_periodic_resync_is_opt_in(monkeypatch):
    monkeypatch.setattr(monitor_module, "RECONCILE_INTERVAL", 0.005)
    calls = []

    async def fake_fetch_room(room_id, *, source, timeout):
        calls.append(room_id)
        return SimpleNamespace(is_live=False, started_at=None)

    monkeypatch.setattr(monitor_module, "fetch_room", fake_fetch_room)
    client = FakeDanmakuClient()
    monitor = LiveStatusMonitor(3, client_factory=lambda: client)
    monitor.start()

    await asyncio.sleep(0.04)

    assert calls == []

    await monitor.stop()


def test_periodic_resync_interval_rejects_negative_values():
    with pytest.raises(ValueError, match="periodic_resync_interval"):
        LiveStatusMonitor(4, periodic_resync_interval=-1)


def test_initial_live_without_announcement_still_emits_offline(monkeypatch):
    now = {"value": 1000.0}
    monkeypatch.setattr(monitor_module.time, "time", lambda: now["value"])
    events = []
    monitor = LiveStatusMonitor(
        5,
        live_callback=lambda room_id, message: events.append("live"),
        offline_callback=lambda room_id, duration: events.append(("offline", duration)),
        announce_initial_live=False,
        notify_cooldown=0,
        offline_confirmation=0,
    )

    monitor._apply_observation(True, {}, started_at=900.0)
    assert events == []

    now["value"] = 1100.0
    monitor._apply_observation(False, {})
    assert events == [("offline", 200.0)]


def test_same_live_snapshot_does_not_rewrite_session_start(monkeypatch):
    monkeypatch.setattr(monitor_module.time, "time", lambda: 1000.0)
    monitor = LiveStatusMonitor(6, announce_initial_live=False)

    monitor._apply_observation(True, {}, started_at=900.0)
    monitor._apply_observation(True, {}, started_at=950.0)
    assert monitor.live_start_time == 900.0

    monitor._apply_observation(True, {}, started_at=850.0)
    assert monitor.live_start_time == 900.0

    monitor._apply_observation(True, {}, started_at=1100.0)
    assert monitor.live_start_time == 900.0


@pytest.mark.parametrize(
    ("last_offline_time", "started_at", "expected"),
    [
        (None, 900.0, 900.0),
        (1000.0, 1000.0, None),
        (1000.0, 1001.0, 1001.0),
    ],
)
def test_resync_started_at_respects_session_boundary(
    last_offline_time, started_at, expected
):
    monitor = LiveStatusMonitor(
        8,
        inherit_state={
            "last_live_status": False,
            "last_offline_time": last_offline_time,
        },
    )

    message = monitor._resync_message(
        SimpleNamespace(is_live=True, started_at=started_at),
        fetched_at=1100.0,
    )

    assert message["room_info"]["started_at"] == expected


def test_quick_reopen_rejects_previous_session_started_at(monkeypatch):
    now = {"value": 1000.0}
    monkeypatch.setattr(monitor_module.time, "time", lambda: now["value"])
    events = []
    monitor = LiveStatusMonitor(
        9,
        live_callback=lambda room_id, message: events.append(
            ("live", monitor.live_start_time)
        ),
        offline_callback=lambda room_id, duration: events.append(("offline", duration)),
        notify_cooldown=0,
        offline_confirmation=0,
    )

    monitor._apply_observation(True, {}, started_at=900.0)
    now["value"] = 1005.0
    monitor._apply_observation(False, {})
    now["value"] = 1006.0
    monitor._apply_observation(True, {}, started_at=900.0)
    now["value"] = 1010.0
    monitor._apply_observation(False, {})

    assert events == [
        ("live", 900.0),
        ("offline", 105.0),
        ("live", 1006.0),
        ("offline", 4.0),
    ]


def test_http_new_session_accepts_started_at_after_last_offline(monkeypatch):
    monkeypatch.setattr(monitor_module.time, "time", lambda: 1100.0)
    starts = []
    monitor = LiveStatusMonitor(
        10,
        live_callback=lambda room_id, message: starts.append(monitor.live_start_time),
        inherit_state={
            "last_live_status": False,
            "last_offline_time": 1000.0,
        },
        notify_cooldown=0,
    )

    monitor._apply_observation(True, {}, started_at=1050.0)

    assert starts == [1050.0]
    assert monitor.live_start_time == 1050.0


@pytest.mark.asyncio
@pytest.mark.parametrize("started_at", [900.0, 1000.0, None, 1101.0])
async def test_http_live_snapshot_must_prove_it_is_after_last_offline(
    monkeypatch, started_at
):
    monkeypatch.setattr(monitor_module.time, "time", lambda: 1100.0)

    async def fake_fetch_room(room_id, *, source, timeout):
        return SimpleNamespace(is_live=True, started_at=started_at)

    monkeypatch.setattr(monitor_module, "fetch_room", fake_fetch_room)
    events = []
    monitor = LiveStatusMonitor(
        12,
        live_callback=lambda room_id, message: events.append("live"),
        inherit_state={
            "last_live_status": False,
            "last_offline_time": 1000.8,
        },
        notify_cooldown=0,
    )

    await monitor._resync()

    assert events == []
    assert monitor.last_live_status is False
    assert monitor.live_start_time is None
    assert monitor._resync_pending is True


@pytest.mark.asyncio
async def test_http_live_snapshot_after_last_offline_starts_new_session(monkeypatch):
    now = {"value": 1100.0}
    started_at = {"value": 900.0}
    monkeypatch.setattr(monitor_module.time, "time", lambda: now["value"])

    async def fake_fetch_room(room_id, *, source, timeout):
        return SimpleNamespace(is_live=True, started_at=started_at["value"])

    monkeypatch.setattr(monitor_module, "fetch_room", fake_fetch_room)
    starts = []
    monitor = LiveStatusMonitor(
        13,
        live_callback=lambda room_id, message: starts.append(monitor.live_start_time),
        inherit_state={
            "last_live_status": False,
            "last_offline_time": 1000.0,
        },
        notify_cooldown=0,
    )

    await monitor._resync()
    assert starts == []
    assert monitor.last_live_status is False

    started_at["value"] = 1050.0
    now["value"] = 1101.0
    await monitor._resync()

    assert starts == [1050.0]
    assert monitor.last_live_status is True
    assert monitor.live_start_time == 1050.0


@pytest.mark.asyncio
async def test_realtime_rss_overrides_ambiguous_http_session_boundary(monkeypatch):
    monkeypatch.setattr(monitor_module.time, "time", lambda: 1100.0)

    async def fake_fetch_room(room_id, *, source, timeout):
        return SimpleNamespace(is_live=True, started_at=1000.0)

    monkeypatch.setattr(monitor_module, "fetch_room", fake_fetch_room)
    starts = []
    monitor = LiveStatusMonitor(
        14,
        live_callback=lambda room_id, message: starts.append(monitor.live_start_time),
        inherit_state={
            "last_live_status": False,
            "last_offline_time": 1000.8,
        },
        notify_cooldown=30,
    )

    await monitor._resync()
    assert starts == []
    assert monitor.last_live_status is False

    monitor._rss_handler({"type": "rss", "ss": "1", "ivl": "0"})

    assert starts == [1100.0]
    assert monitor.last_live_status is True
    assert monitor.live_start_time == 1100.0


def test_realtime_live_start_stays_fixed_through_http_and_duration(monkeypatch):
    now = {"value": 1000.0}
    monkeypatch.setattr(monitor_module.time, "time", lambda: now["value"])
    starts = []
    durations = []
    monitor = LiveStatusMonitor(
        11,
        live_callback=lambda room_id, message: starts.append(monitor.live_start_time),
        offline_callback=lambda room_id, duration: durations.append(duration),
        inherit_state={
            "last_live_status": False,
            "last_offline_time": 900.0,
        },
        notify_cooldown=0,
        offline_confirmation=0,
    )

    monitor._rss_handler({"type": "rss", "ss": "1", "ivl": "0"})
    now["value"] = 1002.0
    monitor._apply_observation(True, {}, started_at=950.0)

    assert starts == [1000.0]
    assert monitor.live_start_time == 1000.0

    now["value"] = 1010.0
    monitor._apply_observation(False, {})

    assert durations == [10.0]
    assert durations[0] == monitor.last_offline_time - starts[0]


@pytest.mark.asyncio
async def test_resync_callback_contains_serializable_room_snapshot(monkeypatch):
    monkeypatch.setattr(monitor_module, "RECONCILE_INTERVAL", 0.005)

    async def fake_fetch_room(room_id, *, source, timeout):
        return SimpleNamespace(
            is_live=True,
            started_at=900,
            title="Title",
            category="Category",
            cover_url="https://example.com/cover.jpg",
        )

    monkeypatch.setattr(monitor_module, "fetch_room", fake_fetch_room)
    client = FakeDanmakuClient()
    messages = []
    monitor = LiveStatusMonitor(
        7,
        live_callback=lambda room_id, message: messages.append(message),
        client_factory=lambda: client,
        periodic_resync_interval=0.01,
        notify_cooldown=0,
    )
    monitor.start()

    await asyncio.sleep(0.04)
    await monitor.stop()

    assert len(messages) == 1
    assert messages[0]["room_info"] == {
        "title": "Title",
        "category": "Category",
        "cover_url": "https://example.com/cover.jpg",
        "started_at": 900,
        "is_live": True,
        "fetched_at": messages[0]["room_info"]["fetched_at"],
    }
    json.dumps(messages[0])
