"""经 HTTP 确认的斗鱼开播/下播状态监控器。"""

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
    """监控单个斗鱼房间并只输出经 HTTP 确认的状态转换。

    ``DanmakuClient`` 保持原始协议流语义。本类在其上增加状态层：

    - ``rss`` 只作为“状态可能变化”的触发信号，不直接作为最终结论；
    - 所有真实 ``rss`` 转换都通过 betard HTTP 快照确认；
    - 连接建立或重连后主动对账，补齐断连窗口内丢失的转换；
    - 对账失败按指数退避持续重试，未经确认的反向状态不会被输出；
    - 冷却期内的转换在到期前再次经过上述确认，不会盲目补发。

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
    ) -> None:
        if isinstance(room_id, bool) or not isinstance(room_id, int):
            raise TypeError("room_id 必须为 int")
        if room_id <= 0:
            raise ValueError("room_id 必须为正整数")
        if notify_cooldown < 0:
            raise ValueError("notify_cooldown 不能为负数")
        if offline_confirmation < 0:
            raise ValueError("offline_confirmation 不能为负数")

        self.room_id = room_id
        self.live_callback = live_callback
        self.offline_callback = offline_callback
        self.created_at = time.time()
        self._client_factory = client_factory or (
            lambda: DanmakuClient(room_id, types={"rss"}, emit_connection_events=True)
        )

        self.last_live_status: bool | None = None
        self.live_start_time: float | None = None
        self._has_announced_live = False
        self._last_notify_time = 0.0
        self._notify_cooldown = float(notify_cooldown)
        self._announce_initial_live = announce_initial_live
        self._offline_confirmation = float(offline_confirmation)
        self._pending_status: bool | None = None
        self._pending_msg: dict | None = None
        self._pending_started_at: float | None = None
        self._pending_observed_at: float | None = None
        self._pending_confirmed_at: float | None = None
        self._pending_needs_resync = False

        if inherit_state:
            self.last_live_status = inherit_state.get("last_live_status")
            self.live_start_time = inherit_state.get("live_start_time")
            self._has_announced_live = bool(
                inherit_state.get("has_announced_live", False)
            )
            self._last_notify_time = float(inherit_state.get("last_notify_time", 0.0))
            self._pending_status = inherit_state.get("pending_status")
            self._pending_msg = inherit_state.get("pending_msg")
            self._pending_started_at = inherit_state.get("pending_started_at")
            self._pending_observed_at = inherit_state.get("pending_observed_at")
            self._pending_confirmed_at = inherit_state.get("pending_confirmed_at")
            self._pending_needs_resync = bool(
                inherit_state.get("pending_needs_resync", False)
            )

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

    @property
    def is_healthy(self) -> bool:
        return not self._stop_flag and self._task is not None and not self._task.done()

    def export_state(self) -> dict[str, Any]:
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
                "斗鱼直播间 %s 检测到下播，但本场未播报开播，忽略",
                self.room_id,
            )
        return None, ()

    def _apply_observation(
        self,
        is_live: bool,
        msg: dict,
        started_at: float | None = None,
        require_offline_confirmation: bool = False,
    ) -> None:
        if self._stop_flag:
            return
        try:
            self._obs_seq += 1
            now = time.time()
            callback: Callable | None = None
            args: tuple = ()
            if self.last_live_status is None:
                self._clear_pending()
                self.last_live_status = is_live
                logger.info(
                    "斗鱼直播间 %s 当前状态: %s",
                    self.room_id,
                    "直播中" if is_live else "未开播",
                )
                if is_live:
                    self.live_start_time = started_at or now
                    if self._announce_initial_live:
                        self._has_announced_live = True
                        self._last_notify_time = now
                        logger.info("斗鱼直播间 %s 开播了（初始状态）", self.room_id)
                        if self.live_callback:
                            callback = self.live_callback
                            args = (self.room_id, msg)
                    else:
                        logger.info(
                            "斗鱼直播间 %s 已在播（补播报关闭，静默接管）",
                            self.room_id,
                        )
            elif is_live == self.last_live_status:
                self._clear_pending()
                if is_live and started_at is not None:
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
                        "斗鱼直播间 %s 检测到下播候选，将在 %.0f 秒后再次确认",
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
            elif now - self._last_notify_time < self._notify_cooldown:
                self._pending_status = is_live
                self._pending_msg = msg
                self._pending_started_at = started_at
                self._pending_observed_at = now
                self._pending_confirmed_at = now
                self._pending_needs_resync = False
            else:
                if is_live and started_at is not None:
                    self.live_start_time = started_at
                callback, args = self._apply_transition(is_live, msg, now)
            if callback:
                callback(*args)
        except Exception:
            logger.exception("处理斗鱼直播间 %s 状态时出错", self.room_id)

    def _rss_handler(self, msg: dict) -> None:
        if self._stop_flag:
            return
        status = RoomStatus.from_dict(msg)
        if status.ss not in {"0", "1"}:
            logger.debug(
                "斗鱼直播间 %s 收到缺少有效 ss 的 rss，已忽略: %r",
                self.room_id,
                msg,
            )
            return
        is_live = status.is_live
        if msg.get("type") != "rss":
            self._apply_observation(is_live, msg)
            return
        self._obs_seq += 1
        if self.last_live_status is not None and is_live == self.last_live_status:
            self._clear_pending()
            return
        self._pending_status = is_live
        self._pending_msg = msg
        self._pending_started_at = None
        self._pending_observed_at = time.time()
        self._pending_confirmed_at = None
        self._pending_needs_resync = True
        self._schedule_resync()
        logger.debug(
            "斗鱼直播间 %s 收到状态变化 rss，等待 HTTP 确认",
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
            logger.info("斗鱼直播间 %s 冷却结束，应用已确认状态转换", self.room_id)
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
                    "斗鱼直播间 %s 对账返回房间不存在，%.0f 分钟后复查: %s",
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
                "斗鱼直播间 %s 状态对账失败（第 %s 次），%.0f 秒后重试: %s",
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
            logger.debug("斗鱼直播间 %s 对账快照已过期，丢弃并重拉", self.room_id)
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
                "斗鱼直播间 %s 的弹幕状态候选在 %.0f 秒内未获 HTTP 确认，已忽略",
                self.room_id,
                RSS_CONFIRM_WINDOW,
            )
            self._clear_pending()
        self._resync_pending = False
        self._apply_observation(
            info.is_live,
            {"type": "aiodouyu.resync", "roomid": str(self.room_id)},
            started_at=float(info.started_at) if info.started_at else None,
            require_offline_confirmation=True,
        )

    async def _reconcile_loop(self) -> None:
        while True:
            await asyncio.sleep(RECONCILE_INTERVAL)
            try:
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
                logger.exception("斗鱼直播间 %s 校准协程出错", self.room_id)

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
                        "斗鱼监控器 %s 弹幕连接就绪，已登记状态对账",
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
                    "斗鱼监控器 %s 消费循环退出，等待上层重启",
                    self.room_id,
                )

    def start(self) -> bool:
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
