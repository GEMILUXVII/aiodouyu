"""Low-latency live start and confirmed offline status monitor."""

from __future__ import annotations

import asyncio
import contextlib
import logging
import time
from collections.abc import Callable
from typing import Any

from .client import EVENT_CONNECTED, EVENT_DISCONNECTED, DanmakuClient
from .exceptions import RoomNotFound
from .models import RoomStatus
from .web import fetch_room

__all__ = ["LiveStatusMonitor"]

logger = logging.getLogger(__name__)

# betard 能区分真实直播与视频轮播，open 源不能。
RESYNC_SOURCE = "betard"
RESYNC_TIMEOUT = 10.0
RESYNC_RETRY_BASE = 5.0
RESYNC_RETRY_MAX = 60.0
RESYNC_ROOM_GONE_INTERVAL = 1800.0
RSS_CONFIRM_RETRY_INTERVAL = 2.0
RSS_CONFIRM_WINDOW = 60.0
RECONCILE_INTERVAL = 1.0
STOP_TIMEOUT = 5.0
OFFLINE_CONFIRMATION = 90.0


class LiveStatusMonitor:
    """Monitor one room with a fast live path and confirmed offline path.

    ``DanmakuClient`` keeps raw protocol semantics. This class adds:

    - explicit live ``rss`` events are emitted immediately;
    - offline events still require HTTP confirmation;
    - reconnect and optional periodic HTTP reconciliation recover missed events;
    - HTTP failures retry with backoff instead of killing the monitor.

    回调在事件循环线程同步调用，不能执行阻塞操作。
    """

    def __init__(
        self,
        room_id: int,
        live_callback: Callable[[int, dict], None] | None = None,
        offline_callback: Callable[[int, float], None] | None = None,
        inherit_state: dict[str, Any] | None = None,
        client_factory: Callable[[], DanmakuClient] | None = None,
        notify_cooldown: float = 30.0,
        announce_initial_live: bool = True,
        offline_confirmation: float = OFFLINE_CONFIRMATION,
        periodic_resync_interval: float = 0.0,
    ) -> None:
        if isinstance(room_id, bool) or not isinstance(room_id, int):
            raise TypeError("room_id 必须为 int")
        if room_id <= 0:
            raise ValueError("room_id 必须为正整数")
        if notify_cooldown < 0:
            raise ValueError("notify_cooldown 不能为负数")
        if offline_confirmation < 0:
            raise ValueError("offline_confirmation 不能为负数")
        if periodic_resync_interval < 0:
            raise ValueError("periodic_resync_interval 不能为负数")

        self.room_id = room_id
        self.live_callback = live_callback
        self.offline_callback = offline_callback
        self.created_at = time.time()
        self._client_factory = client_factory or (
            lambda: DanmakuClient(room_id, types={"rss"}, emit_connection_events=True)
        )

        self.last_live_status: bool | None = None
        self.live_start_time: float | None = None
        # Historical name kept for state compatibility. It means that the
        # current live session is eligible for an offline callback, regardless
        # of whether the initial live callback was intentionally suppressed.
        self._has_announced_live = False
        self._last_notify_time = 0.0
        self._notify_cooldown = float(notify_cooldown)
        self._announce_initial_live = announce_initial_live
        self._offline_confirmation = float(offline_confirmation)
        self._periodic_resync_interval = float(periodic_resync_interval)

        self._pending_status: bool | None = None
        self._pending_msg: dict | None = None
        self._pending_started_at: float | None = None
        self._pending_observed_at: float | None = None
        self._pending_confirmed_at: float | None = None
        self._pending_needs_resync = False
        self._live_observed_via_rss = False
        self._live_rss_observed_at: float | None = None

        if inherit_state:
            self.last_live_status = inherit_state.get("last_live_status")
            self.live_start_time = inherit_state.get("live_start_time")
            self._has_announced_live = bool(
                inherit_state.get("has_announced_live", False)
            )
            if self.last_live_status is True:
                self._has_announced_live = True
            self._last_notify_time = float(inherit_state.get("last_notify_time", 0.0))
            self._pending_status = inherit_state.get("pending_status")
            self._pending_msg = inherit_state.get("pending_msg")
            self._pending_started_at = inherit_state.get("pending_started_at")
            self._pending_observed_at = inherit_state.get("pending_observed_at")
            self._pending_confirmed_at = inherit_state.get("pending_confirmed_at")
            self._pending_needs_resync = bool(
                inherit_state.get("pending_needs_resync", False)
            )
            if self._pending_status is not None:
                self._pending_needs_resync = True

        self._stop_flag = False
        self._client: DanmakuClient | None = None
        self._task: asyncio.Task[None] | None = None
        self.connected = False

        self._resync_pending = False
        self._resync_at = 0.0
        self._resync_failures = 0
        self._resync_room_gone = False
        self._obs_seq = 0
        self._conn_gen = 0
        self._next_periodic_resync_at = (
            time.monotonic() if self._periodic_resync_interval > 0 else float("inf")
        )

    @property
    def is_healthy(self) -> bool:
        """消费协程是否仍在运行。"""
        return not self._stop_flag and self._task is not None and not self._task.done()

    def export_state(self) -> dict[str, Any]:
        """导出可传给下一实例 ``inherit_state`` 的状态。"""
        return {
            "last_live_status": self.last_live_status,
            "live_start_time": self.live_start_time,
            "has_announced_live": self._has_announced_live,
            "last_notify_time": self._last_notify_time,
            "pending_status": self._pending_status,
            "pending_msg": self._pending_msg,
            "pending_started_at": self._pending_started_at,
            "pending_observed_at": self._pending_observed_at,
            "pending_confirmed_at": self._pending_confirmed_at,
            "pending_needs_resync": self._pending_needs_resync,
        }

    def _clear_pending(self) -> None:
        self._pending_status = None
        self._pending_msg = None
        self._pending_started_at = None
        self._pending_observed_at = None
        self._pending_confirmed_at = None
        self._pending_needs_resync = False

    @staticmethod
    def _valid_started_at(started_at: float | None, now: float) -> float | None:
        """Return a plausible live start timestamp."""
        if started_at is None:
            return None
        try:
            value = float(started_at)
        except (TypeError, ValueError, OverflowError):
            return None
        return value if 0 < value <= now else None

    def _resync_message(self, info: Any, fetched_at: float) -> dict[str, Any]:
        """Build a JSON-compatible callback message from an HTTP snapshot."""
        started_at = self._valid_started_at(
            getattr(info, "started_at", None), fetched_at
        )
        return {
            "type": "aiodouyu.resync",
            "roomid": str(self.room_id),
            "room_info": {
                "title": str(getattr(info, "title", "") or ""),
                "category": (
                    str(category)
                    if (category := getattr(info, "category", None))
                    else None
                ),
                "cover_url": (
                    str(cover_url)
                    if (cover_url := getattr(info, "cover_url", None))
                    else None
                ),
                "started_at": started_at,
                "is_live": bool(info.is_live),
                "fetched_at": fetched_at,
            },
        }

    def _apply_transition(
        self,
        is_live: bool,
        msg: dict,
        now: float,
        event_time: float | None = None,
    ) -> tuple[Callable | None, tuple]:
        self._obs_seq += 1
        self._clear_pending()
        self.last_live_status = is_live

        if is_live:
            logger.info("斗鱼直播间 %s 开播了", self.room_id)
            if self.live_start_time is None:
                self.live_start_time = now
            self._last_notify_time = now
            self._has_announced_live = True
            if self.live_callback:
                return self.live_callback, (self.room_id, msg)
            return None, ()

        logger.info("斗鱼直播间 %s 下播了", self.room_id)
        self._live_observed_via_rss = False
        self._live_rss_observed_at = None
        duration = 0.0
        if self.live_start_time:
            duration = (
                event_time if event_time is not None else now
            ) - self.live_start_time
            duration = max(duration, 0.0)
            self.live_start_time = None
        announced = self._has_announced_live
        self._has_announced_live = False
        if announced:
            self._last_notify_time = now
            if self.offline_callback:
                return self.offline_callback, (self.room_id, duration)
        else:
            logger.debug(
                "斗鱼直播间 %s 检测到下播,但尚未发布开播通知,忽略",
                self.room_id,
            )
        return None, ()

    def _apply_observation(
        self,
        is_live: bool,
        msg: dict,
        started_at: float | None = None,
        require_offline_confirmation: bool = False,
        announce_initial_live: bool | None = None,
        bypass_cooldown: bool = False,
    ) -> None:
        """应用 HTTP 结果或测试注入的可信状态观测。"""
        if self._stop_flag:
            return
        try:
            self._obs_seq += 1
            now = time.time()
            started_at = self._valid_started_at(started_at, now)
            callback: Callable | None = None
            args: tuple = ()

            if self.last_live_status is None:
                self._clear_pending()
                logger.info(
                    "斗鱼直播间 %s 当前状态: %s",
                    self.room_id,
                    "直播中" if is_live else "未开播",
                )
                self.last_live_status = is_live
                if is_live:
                    self.live_start_time = started_at or now
                    # ``announce_initial_live`` controls only the initial live
                    # callback. A silently adopted session must still be able
                    # to emit its later offline transition.
                    self._has_announced_live = True
                    should_announce = (
                        self._announce_initial_live
                        if announce_initial_live is None
                        else announce_initial_live
                    )
                    if should_announce:
                        self._last_notify_time = now
                        logger.info("斗鱼直播间 %s 开播了 (初始状态)", self.room_id)
                        if self.live_callback:
                            callback = self.live_callback
                            args = (self.room_id, msg)
                    else:
                        logger.info(
                            "斗鱼直播间 %s 已在播(补播报已关闭,静默接管)",
                            self.room_id,
                        )
            elif is_live == self.last_live_status:
                self._clear_pending()
                if is_live:
                    self._has_announced_live = True
                if is_live and started_at is not None:
                    if (
                        self.live_start_time is None
                        or started_at < self.live_start_time
                    ):
                        self.live_start_time = started_at
            elif (
                not is_live
                and self.last_live_status is True
                and self._offline_confirmation > 0
                and require_offline_confirmation
            ):
                if self._pending_status is not False:
                    self._pending_status = False
                    self._pending_msg = msg
                    self._pending_started_at = started_at
                    self._pending_observed_at = now
                    self._pending_confirmed_at = now
                    self._pending_needs_resync = True
                    self._schedule_resync(self._offline_confirmation)
                    logger.info(
                        "斗鱼直播间 %s 检测到下播候选,将在 %.0f 秒后再次确认",
                        self.room_id,
                        self._offline_confirmation,
                    )
                    return
                confirmed_at = self._pending_confirmed_at
                if confirmed_at is None:
                    self._pending_confirmed_at = now
                    self._pending_needs_resync = True
                    self._schedule_resync(self._offline_confirmation)
                    return
                remaining = self._offline_confirmation - (now - confirmed_at)
                if remaining > 0:
                    self._pending_needs_resync = True
                    self._schedule_resync(remaining)
                    return
                self._pending_needs_resync = False
                if now - self._last_notify_time < self._notify_cooldown:
                    self._pending_msg = msg
                else:
                    callback, args = self._apply_transition(
                        False,
                        msg,
                        now,
                        event_time=self._pending_observed_at,
                    )
            elif (
                not bypass_cooldown
                and now - self._last_notify_time < self._notify_cooldown
            ):
                self._pending_status = is_live
                self._pending_msg = msg
                self._pending_started_at = started_at
                self._pending_observed_at = now
                self._pending_confirmed_at = now
                # 本方法只接收 HTTP 结果，pending 已经确认。
                self._pending_needs_resync = False
                logger.debug(
                    "斗鱼直播间 %s 已记录经确认的冷却期待定状态",
                    self.room_id,
                )
            else:
                if is_live and started_at is not None:
                    self.live_start_time = started_at
                callback, args = self._apply_transition(is_live, msg, now)

            if callback:
                callback(*args)
        except Exception:
            logger.exception("处理斗鱼直播间 %s 状态时出错", self.room_id)

    def _rss_handler(self, msg: dict) -> None:
        """Apply live rss immediately and HTTP-confirm offline rss."""
        if self._stop_flag:
            return
        status = RoomStatus.from_dict(msg)
        if status.ss not in {"0", "1"}:
            logger.debug(
                "斗鱼直播间 %s 收到缺少有效 ss 的 rss,已忽略: %r",
                self.room_id,
                msg,
            )
            return

        is_live = status.is_live
        # 直接调用私有方法的旧测试/高级用户可能只传字段、不带 type；
        # 这类显式注入视为可信观测。真实客户端消息始终带 type=rss。
        if msg.get("type") != "rss":
            self._apply_observation(is_live, msg)
            return

        logger.info(
            "斗鱼直播间 %s 收到实时状态 rss: ss=%s, ivl=%s",
            self.room_id,
            status.ss,
            status.ivl,
        )
        self._obs_seq += 1
        if self.last_live_status is not None and is_live == self.last_live_status:
            self._clear_pending()
            return

        if is_live:
            # A real-time room event must not be vetoed by a slower HTTP
            # endpoint. Missing this edge loses the whole session because rss
            # is normally sent only once per transition.
            self._live_observed_via_rss = True
            self._live_rss_observed_at = time.time()
            self._apply_observation(
                True,
                msg,
                announce_initial_live=True,
                bypass_cooldown=True,
            )
            # Confirm the session in the background. The callback above stays
            # latency-sensitive, while this audit lets HTTP recover a missing
            # later offline rss and keeps the session lifecycle closed.
            self._schedule_resync()
            return

        self._pending_status = is_live
        self._pending_msg = msg
        self._pending_started_at = None
        self._pending_observed_at = time.time()
        self._pending_confirmed_at = None
        self._pending_needs_resync = True
        self._schedule_resync()
        logger.debug(
            "斗鱼直播间 %s 收到反向 rss,等待 betard 对账确认",
            self.room_id,
        )

    def _reconcile_pending(self) -> None:
        if self._stop_flag or self._pending_status is None:
            return
        try:
            now = time.time()
            if now - self._last_notify_time < self._notify_cooldown:
                return
            if self._pending_needs_resync:
                return
            if self._pending_status == self.last_live_status:
                self._obs_seq += 1
                self._clear_pending()
                return
            logger.info("斗鱼直播间 %s 冷却结束,应用已确认状态转换", self.room_id)
            if self._pending_status:
                start_base = self._pending_started_at or self._pending_observed_at
                if start_base is not None:
                    self.live_start_time = start_base
            callback, args = self._apply_transition(
                self._pending_status,
                self._pending_msg or {},
                now,
                event_time=self._pending_observed_at,
            )
            if callback:
                callback(*args)
        except Exception:
            logger.exception("校准斗鱼直播间 %s 待定状态时出错", self.room_id)

    def _schedule_resync(self, delay: float = 0.0) -> None:
        self._resync_pending = True
        self._resync_at = time.monotonic() + delay

    def _schedule_next_periodic_resync(self) -> None:
        if self._periodic_resync_interval > 0:
            self._next_periodic_resync_at = (
                time.monotonic() + self._periodic_resync_interval
            )

    async def _resync(self) -> None:
        seq_before = self._obs_seq
        gen_before = self._conn_gen
        try:
            info = await fetch_room(
                self.room_id,
                source=RESYNC_SOURCE,
                timeout=RESYNC_TIMEOUT,
            )
        except RoomNotFound as exc:
            self._schedule_resync(RESYNC_ROOM_GONE_INTERVAL)
            if not self._resync_room_gone:
                self._resync_room_gone = True
                logger.warning(
                    "斗鱼直播间 %s 对账返回房间不存在,%.0f 分钟后复查: %s",
                    self.room_id,
                    RESYNC_ROOM_GONE_INTERVAL / 60,
                    exc,
                )
            return
        except Exception as exc:
            self._resync_failures += 1
            delay = min(
                RESYNC_RETRY_MAX,
                RESYNC_RETRY_BASE * (2 ** min(self._resync_failures - 1, 6)),
            )
            self._schedule_resync(delay)
            logger.warning(
                "斗鱼直播间 %s 状态对账失败(第 %s 次),%.0fs 后重试: %s",
                self.room_id,
                self._resync_failures,
                delay,
                exc,
            )
            return

        self._resync_failures = 0
        self._resync_room_gone = False
        if self._stop_flag:
            self._resync_pending = False
            return
        if seq_before != self._obs_seq or gen_before != self._conn_gen:
            logger.debug("斗鱼直播间 %s 对账快照已过期,丢弃并重拉", self.room_id)
            self._schedule_resync()
            return

        if (
            self._pending_needs_resync
            and self._pending_status is not None
            and info.is_live != self._pending_status
        ):
            observed_at = self._pending_observed_at or time.time()
            age = max(time.time() - observed_at, 0.0)
            if age < RSS_CONFIRM_WINDOW:
                delay = min(
                    RSS_CONFIRM_RETRY_INTERVAL,
                    max(RSS_CONFIRM_WINDOW - age, 0.0),
                )
                self._schedule_resync(delay)
                return
            logger.info(
                "斗鱼直播间 %s 的弹幕状态候选在 %.0f 秒内未获 HTTP 确认,已忽略",
                self.room_id,
                RSS_CONFIRM_WINDOW,
            )
            self._clear_pending()

        if (
            not info.is_live
            and self.last_live_status is True
            and self._live_observed_via_rss
            and self._pending_status is not False
        ):
            # Until HTTP has agreed with a real-time live event at least once,
            # an old offline snapshot must not immediately reverse it.
            observed_at = self._live_rss_observed_at or time.time()
            age = max(time.time() - observed_at, 0.0)
            if age < RSS_CONFIRM_WINDOW:
                delay = min(
                    RSS_CONFIRM_RETRY_INTERVAL,
                    max(RSS_CONFIRM_WINDOW - age, 0.0),
                )
                self._schedule_resync(delay)
                logger.info(
                    "斗鱼直播间 %s 的 HTTP 状态尚未追上实时开播,继续保留并复查",
                    self.room_id,
                )
                return
            logger.warning(
                "斗鱼直播间 %s 的实时开播在 %.0f 秒内未获 HTTP 确认,转入正常下播复核",
                self.room_id,
                RSS_CONFIRM_WINDOW,
            )
            self._live_observed_via_rss = False
            self._live_rss_observed_at = None

        self._resync_pending = False
        self._schedule_next_periodic_resync()
        fetched_at = time.time()
        message = self._resync_message(info, fetched_at)
        if info.is_live:
            self._live_observed_via_rss = False
            self._live_rss_observed_at = None
        self._apply_observation(
            info.is_live,
            message,
            started_at=message["room_info"]["started_at"],
            require_offline_confirmation=True,
        )

    async def _reconcile_loop(self) -> None:
        while True:
            await asyncio.sleep(RECONCILE_INTERVAL)
            try:
                now = time.monotonic()
                if (
                    self._periodic_resync_interval > 0
                    and not self._stop_flag
                    and not self._resync_pending
                    and now >= self._next_periodic_resync_at
                ):
                    self._schedule_resync()
                if (
                    self._resync_pending
                    and not self._stop_flag
                    and time.monotonic() >= self._resync_at
                ):
                    await self._resync()
                self._reconcile_pending()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception(
                    "斗鱼直播间 %s 校准协程出错",
                    self.room_id,
                )

    async def _run(self) -> None:
        client = self._client
        if client is None:
            return
        reconcile_task = asyncio.create_task(self._reconcile_loop())
        try:
            async for msg in client:
                if self._stop_flag:
                    break
                msg_type = msg.get("type")
                if msg_type == EVENT_CONNECTED:
                    self.connected = True
                    self._conn_gen += 1
                    if not (
                        self._resync_room_gone
                        and self._resync_pending
                        and time.monotonic() < self._resync_at
                    ):
                        self._schedule_resync()
                    logger.info(
                        "斗鱼监控器 %s 弹幕连接就绪,已登记状态对账",
                        self.room_id,
                    )
                elif msg_type == EVENT_DISCONNECTED:
                    self.connected = False
                    logger.warning("斗鱼监控器 %s 弹幕连接已断开", self.room_id)
                elif msg_type == "rss":
                    self._rss_handler(msg)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("斗鱼监控器 %s 运行出错", self.room_id)
        finally:
            self.connected = False
            reconcile_task.cancel()
            await asyncio.gather(reconcile_task, return_exceptions=True)
            with contextlib.suppress(Exception):
                await client.close()
            if not self._stop_flag:
                logger.warning(
                    "斗鱼监控器 %s 消费循环退出,等待上层重启",
                    self.room_id,
                )

    def start(self) -> bool:
        """创建后台消费任务；必须在运行中的事件循环上调用。"""
        if self.is_healthy:
            return True
        if self._stop_flag:
            return False
        try:
            self._client = self._client_factory()
            self._task = asyncio.create_task(self._run())
        except Exception:
            logger.exception("斗鱼监控器 %s 启动失败", self.room_id)
            return False
        logger.info("斗鱼监控器 %s 已启动", self.room_id)
        return True

    async def stop(self) -> None:
        """关闭弹幕连接并回收后台任务。"""
        self._stop_flag = True
        try:
            client = self._client
            if client is not None:
                with contextlib.suppress(Exception):
                    await client.close()
            task = self._task
            if task is not None and not task.done():
                _, pending = await asyncio.wait({task}, timeout=STOP_TIMEOUT)
                if pending:
                    task.cancel()
                    await asyncio.gather(task, return_exceptions=True)
        except asyncio.CancelledError:
            if self._task is not None and not self._task.done():
                self._task.cancel()
            raise
        logger.info("斗鱼直播间 %s 监控已停止", self.room_id)
