"""斗鱼弹幕 asyncio 客户端"""

from __future__ import annotations

import asyncio
import contextlib
import inspect
import logging
import random
import struct
import time
import weakref
from collections.abc import AsyncIterator, Callable
from typing import Any, TypeVar, overload

from . import packet, stt
from .exceptions import ConnectionClosed, ProtocolError

__all__ = ["EVENT_CONNECTED", "EVENT_DISCONNECTED", "DanmakuClient"]

logger = logging.getLogger("aiodouyu")

# 连接生命周期伪事件的 type 值（带命名空间前缀，不会与斗鱼消息类型冲突）。
# 需在构造时传入 emit_connection_events=True 才会产生。
EVENT_CONNECTED = "aiodouyu.connected"
EVENT_DISCONNECTED = "aiodouyu.disconnected"

_LENGTH_STRUCT = struct.Struct("<I")

_Handler = TypeVar("_Handler", bound=Callable[[dict[str, str]], Any])


async def _keepalive_loop(ref: weakref.ref[DanmakuClient]) -> None:
    """按协议周期发送 mrkl 心跳;空闲超时时中止连接触发重连

    模块级函数 + weakref:睡眠期间不持有客户端强引用,客户端被弃置后
    整个对象图可被 GC 回收(见 DanmakuClient._spawn_keepalive)。
    """
    try:
        while True:
            client = ref()
            if client is None:
                return
            interval = client.keepalive_interval
            idle_timeout = client.idle_timeout
            room_id = client.room_id
            del client  # 睡眠期间不持有强引用
            await asyncio.sleep(interval)
            client = ref()
            if client is None:
                return
            if time.monotonic() - client._last_recv > idle_timeout:
                logger.warning(
                    "房间 %s 超过 %.0fs 未收到任何包，中止半开连接",
                    room_id,
                    idle_timeout,
                )
                client._abort()
                return
            await client._send({"type": "mrkl"})
            del client
    except asyncio.CancelledError:
        raise
    except Exception:
        # 发送失败说明连接已坏，中止让读循环尽快退出
        client = ref()
        if client is not None:
            client._abort()


class DanmakuClient:
    """斗鱼弹幕客户端

    连接斗鱼弹幕服务器，产出房间内的全部服务端消息
    （rss 房间状态、chatmsg 弹幕、dgb 礼物、uenter 进房……均为
    ``dict[str, str]``，以 ``type`` 键区分）。

    两种消费方式::

        # 1. 异步迭代（推荐）
        async with DanmakuClient(room_id=9999) as client:
            async for msg in client:
                if msg["type"] == "chatmsg":
                    print(msg.get("nn"), msg.get("txt"))

        # 2. 回调注册
        client = DanmakuClient(9999)
        @client.on("rss")
        def on_status(msg): ...
        await client.run()   # 阻塞直到 close()

    特性：
    - 自动重连（指数退避 + 抖动），可通过 ``reconnect=False`` 关闭；
      存活不足 ``min_uptime`` 的短命连接按失败计,持续退避防热重连循环
    - 空闲超时检测：超过 ``idle_timeout`` 未收到任何包即判定半开连接并重连
    - ``close()`` 立即中止连接与内部任务（含重连退避等待），不遗留任务
    - 纯 asyncio TCP 实现，零第三方依赖

    生命周期约定：一个客户端实例只支持一轮消费。``async for`` 中 ``break``
    后请调用 ``close()`` 释放连接；``close()`` 后不可复用，需新建实例。

    注意：弹幕协议为斗鱼非官方公开接口，字段与端点可能随时变更。
    """

    def __init__(
        self,
        room_id: int,
        *,
        group_id: int = -9999,
        host: str = "danmuproxy.douyu.com",
        port: int = 8601,
        connect_timeout: float = 10.0,
        keepalive_interval: float = 45.0,
        idle_timeout: float = 120.0,
        reconnect: bool = True,
        backoff_initial: float = 1.0,
        backoff_max: float = 60.0,
        min_uptime: float = 5.0,
        types: set[str] | None = None,
        emit_connection_events: bool = False,
    ) -> None:
        """初始化客户端

        Args:
            room_id: 斗鱼房间号（真实 rid，非靓号别名）
            group_id: 弹幕分组，-9999 为海量弹幕分组
            host: 弹幕服务器地址
            port: 弹幕服务器端口（8601/8602/12601/12602）
            connect_timeout: 建立 TCP 连接的超时（秒）
            keepalive_interval: mrkl 心跳间隔（秒），协议要求 45s
            idle_timeout: 收包空闲超时（秒），超时判定连接已死
            reconnect: 断线后是否自动重连
            backoff_initial: 重连退避起始秒数
            backoff_max: 重连退避上限秒数
            min_uptime: 连接最短存活秒数；低于此值的断开按连接失败计
                （继续指数退避），防止服务端"接受即断开"时的热重连循环
            types: 只产出这些 type 的消息；None 表示产出全部。
                连接生命周期伪事件不受此过滤器影响
            emit_connection_events: 是否产出 EVENT_CONNECTED /
                EVENT_DISCONNECTED 伪事件（供消费方感知断连窗口）
        """
        # 显式类型检查:房间号常来自配置/命令参数,字符串形态是最高频
        # 误用,裸比较会抛难以理解的 TypeError('<=' not supported ...)
        if isinstance(room_id, bool) or not isinstance(room_id, int):
            raise TypeError(
                f"room_id 必须为 int,收到 {type(room_id).__name__}"
                f"(字符串请先 int() 转换)"
            )
        if room_id <= 0:
            raise ValueError("room_id 必须为正整数")
        self.room_id = room_id
        self.group_id = group_id
        self.host = host
        self.port = port
        self.connect_timeout = connect_timeout
        self.keepalive_interval = keepalive_interval
        self.idle_timeout = idle_timeout
        self.reconnect = reconnect
        self.backoff_initial = backoff_initial
        self.backoff_max = backoff_max
        self.min_uptime = min_uptime
        self.types = set(types) if types is not None else None
        self.emit_connection_events = emit_connection_events

        self._reader: asyncio.StreamReader | None = None
        self._writer: asyncio.StreamWriter | None = None
        self._keepalive_task: asyncio.Task | None = None
        self._closed = False
        self._close_event = asyncio.Event()
        self._iterating = False
        self._agen: AsyncIterator[dict[str, str]] | None = None
        self._last_recv = 0.0
        self._handlers: dict[str, list[Callable[[dict[str, str]], Any]]] = {}

    # ==================== 公共接口 ====================

    @property
    def connected(self) -> bool:
        """当前是否有存活连接"""
        return self._writer is not None and not self._writer.is_closing()

    @property
    def closed(self) -> bool:
        """客户端是否已被 close()"""
        return self._closed

    async def __aenter__(self) -> DanmakuClient:
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        await self.close()

    def __aiter__(self) -> AsyncIterator[dict[str, str]]:
        agen = self._iterate()
        if not self._iterating:
            # 记录活跃迭代器:close() 对从未驱动过的迭代器主动 aclose;
            # break 弃置且未 close 的客户端依赖 GC 终结器兜底释放连接
            # (keepalive 经 weakref 持有客户端,弃置图对 GC 可达性已断)
            self._agen = agen
        return agen

    @overload
    def on(self, msg_type: str) -> Callable[[_Handler], _Handler]: ...

    @overload
    def on(self, msg_type: str, handler: _Handler) -> _Handler: ...

    def on(
        self, msg_type: str, handler: Callable[[dict[str, str]], Any] | None = None
    ) -> Any:
        """注册消息回调（可用作装饰器）

        Args:
            msg_type: 消息 type 值；``"*"`` 匹配所有消息
            handler: 同步或异步可调用，参数为消息字典
        """

        def register(fn: Callable[[dict[str, str]], Any]):
            self._handlers.setdefault(msg_type, []).append(fn)
            return fn

        return register(handler) if handler is not None else register

    async def run(self) -> None:
        """以回调模式运行，直到 close() 被调用

        回调抛出的异常会被记录日志但不中断消费循环。
        """
        async for message in self:
            msg_type = message.get("type", "")
            for handler in (
                self._handlers.get(msg_type, []) + self._handlers.get("*", [])
            ):
                try:
                    result = handler(message)
                    if inspect.isawaitable(result):
                        await result
                except Exception:
                    logger.exception("消息回调出错 (type=%s)", msg_type)

    async def close(self) -> None:
        """关闭客户端：中止连接、停止心跳，令迭代立即终止

        对处于重连退避等待中的迭代器同样立即生效。
        幂等,可从任意任务调用(包括 ``async for`` 循环体内部)。
        """
        self._closed = True
        self._close_event.set()
        agen = self._agen
        self._agen = None
        if agen is not None and not self._iterating:
            # 只对从未被驱动的迭代器 aclose。正被消费的迭代器不能在此
            # aclose:其 finally 含真实挂起点,终结进行期间消费方若调用
            # __anext__ 会撞上 "generator is already running" 的
            # RuntimeError;_closed 标志 + 下方 _teardown 中止连接已足以
            # 令其在下一次驱动时干净地 StopAsyncIteration
            with contextlib.suppress(Exception):
                aclose = getattr(agen, "aclose", None)
                if aclose is not None:
                    await aclose()
        await self._teardown()

    # ==================== 内部实现 ====================

    def _backoff_delay(self, attempt: int) -> float:
        """计算第 attempt 次重试的退避秒数（含抖动，上限 backoff_max）"""
        raw = self.backoff_initial * (2 ** min(attempt, 32)) * (0.5 + random.random())
        return min(self.backoff_max, raw)

    async def _backoff_wait(self, delay: float) -> bool:
        """退避等待；close() 会立即打断。返回 True 表示客户端已关闭"""
        if self._closed:
            return True
        with contextlib.suppress(asyncio.TimeoutError, TimeoutError):
            await asyncio.wait_for(self._close_event.wait(), timeout=delay)
        return self._closed

    async def _iterate(self) -> AsyncIterator[dict[str, str]]:
        if self._iterating:
            raise RuntimeError("同一客户端只允许一个消费迭代器")
        if self._closed:
            raise ConnectionClosed("客户端已关闭")
        self._iterating = True
        attempt = 0
        try:
            while not self._closed:
                # ---- 建立连接（含退避重试）----
                try:
                    await self._connect()
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    if self._closed:
                        return
                    if not self.reconnect:
                        raise ConnectionClosed(
                            f"连接 {self.host}:{self.port} 失败: {exc}"
                        ) from exc
                    delay = self._backoff_delay(attempt)
                    attempt += 1
                    logger.warning(
                        "房间 %s 连接失败 (%s)，%.1fs 后重试",
                        self.room_id,
                        exc,
                        delay,
                    )
                    if await self._backoff_wait(delay):
                        return
                    continue

                # close() 可能恰在 open_connection 等待期间执行,此时它无
                # 连接可清理;这里必须复查,否则关闭后新连接照常存活
                if self._closed:
                    await self._teardown()
                    return

                connected_at = time.monotonic()
                logger.info("房间 %s 弹幕连接已建立", self.room_id)
                if self.emit_connection_events:
                    yield {"type": EVENT_CONNECTED, "roomid": str(self.room_id)}

                # ---- 收包循环 ----
                try:
                    while True:
                        message = await self._read_message()
                        if self.types is None or message.get("type") in self.types:
                            yield message
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    if not self._closed:
                        logger.warning(
                            "房间 %s 弹幕连接断开: %s", self.room_id, exc
                        )
                finally:
                    await self._teardown()

                if self._closed:
                    return
                if self.emit_connection_events:
                    yield {"type": EVENT_DISCONNECTED, "roomid": str(self.room_id)}
                if not self.reconnect:
                    raise ConnectionClosed("连接已断开且未启用自动重连")

                # 存活足够久的连接视为健康,重置退避;秒断的连接按失败计,
                # 继续退避——防止服务端"接受即断开"(封禁/风控/节点异常)
                # 形成每秒多次的热重连循环
                uptime = time.monotonic() - connected_at
                if uptime >= self.min_uptime:
                    attempt = 0
                else:
                    delay = self._backoff_delay(attempt)
                    attempt += 1
                    logger.warning(
                        "房间 %s 连接存活仅 %.1fs，按失败退避 %.1fs 后重连",
                        self.room_id,
                        uptime,
                        delay,
                    )
                    if await self._backoff_wait(delay):
                        return
        finally:
            self._iterating = False
            await self._teardown()

    async def _open_connection(self) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
        """建立 TCP 连接;close() 可立即打断(抛 ConnectionClosed)

        直接 wait_for(open_connection) 时 close() 只能设标志,连接尝试
        本身无人取消,消费任务会滞留到 connect_timeout(网络故障 SYN
        丢弃场景实测可达数秒)。这里让连接与 close 事件竞速。
        """
        connect_task = asyncio.ensure_future(
            asyncio.open_connection(self.host, self.port)
        )
        close_wait = asyncio.ensure_future(self._close_event.wait())
        try:
            done, _pending = await asyncio.wait(
                {connect_task, close_wait},
                timeout=self.connect_timeout,
                return_when=asyncio.FIRST_COMPLETED,
            )
        except asyncio.CancelledError:
            connect_task.cancel()
            close_wait.cancel()
            await asyncio.gather(connect_task, close_wait, return_exceptions=True)
            raise
        finally:
            close_wait.cancel()

        if connect_task in done and not connect_task.cancelled():
            exc = connect_task.exception()
            if exc is not None:
                raise exc
            reader, writer = connect_task.result()
            if self._closed:
                # close 与连接完成几乎同时:立刻收掉新连接
                with contextlib.suppress(Exception):
                    transport = writer.transport
                    if transport is not None:
                        transport.abort()
                    writer.close()
                raise ConnectionClosed("客户端已关闭")
            return reader, writer

        # 连接未完成:被 close 打断或超时
        connect_task.cancel()
        results = await asyncio.gather(connect_task, return_exceptions=True)
        leftover = results[0]
        if isinstance(leftover, tuple):
            # cancel 与完成竞速中连接侥幸建立:同样收掉,避免泄漏
            with contextlib.suppress(Exception):
                leftover[1].transport.abort()
                leftover[1].close()
        if self._closed:
            raise ConnectionClosed("客户端已关闭")
        raise TimeoutError(
            f"连接 {self.host}:{self.port} 超时({self.connect_timeout:.0f}s)"
        )

    async def _connect(self) -> None:
        reader, writer = await self._open_connection()
        self._reader = reader
        self._writer = writer
        self._last_recv = time.monotonic()
        try:
            await self._send({"type": "loginreq", "roomid": self.room_id})
            await self._send(
                {"type": "joingroup", "rid": self.room_id, "gid": self.group_id}
            )
        except Exception:
            await self._teardown()
            raise
        self._keepalive_task = self._spawn_keepalive()

    async def _send(self, fields: dict[str, object]) -> None:
        writer = self._writer
        if writer is None or writer.is_closing():
            raise ConnectionClosed("连接不可用")
        writer.write(packet.pack(stt.dumps(fields)))
        await writer.drain()

    async def _read_message(self) -> dict[str, str]:
        reader = self._reader
        if reader is None:
            raise ConnectionClosed("连接不可用")
        head = await reader.readexactly(_LENGTH_STRUCT.size)
        (length,) = _LENGTH_STRUCT.unpack(head)
        try:
            packet.validate_length(length)
        except packet.PacketError as exc:
            raise ProtocolError(str(exc)) from exc
        body = await reader.readexactly(length)
        self._last_recv = time.monotonic()
        return stt.loads(packet.extract_payload(body))

    def _spawn_keepalive(self) -> asyncio.Task:
        """创建心跳任务(经 weakref 持有客户端)

        心跳任务经事件循环的 sleep 定时器强可达,若其协程帧强引用
        客户端,被 ``break`` 弃置且未 close() 的客户端连同异步生成器
        会被永久钉死——GC 终结器兜底永远无法触发,连接要等空闲超时
        (最长约 165s)才释放。weakref 使弃置的客户端图可被 GC 回收,
        生成器终结器得以运行 aclose 并及时拆除连接。
        """
        return asyncio.create_task(_keepalive_loop(weakref.ref(self)))

    def _abort(self) -> None:
        writer = self._writer
        if writer is not None:
            transport = writer.transport
            if transport is not None:
                transport.abort()

    async def _teardown(self) -> None:
        task = self._keepalive_task
        self._keepalive_task = None
        if task is not None and task is not asyncio.current_task():
            task.cancel()
            # gather(return_exceptions=True) 把子任务的 CancelledError 作为
            # 结果收集;若当前任务自身在此挂起点被取消,取消仍正常传播
            # (直接 await task 再 except CancelledError 会把两种取消混为一谈,
            # 吞掉当前任务的取消请求)
            await asyncio.gather(task, return_exceptions=True)
        writer = self._writer
        self._reader = None
        self._writer = None
        if writer is not None:
            # 先 abort 立即释放连接(close 是 graceful 关闭,对端不配合
            # FIN 时可拖到 TCP 超时),再有界等待传输真正关闭,避免旧
            # socket 与新连接并存或进程退出时报 unclosed transport
            with contextlib.suppress(Exception):
                transport = writer.transport
                if transport is not None:
                    transport.abort()
                writer.close()
            with contextlib.suppress(Exception):
                await asyncio.wait_for(writer.wait_closed(), timeout=1.0)
