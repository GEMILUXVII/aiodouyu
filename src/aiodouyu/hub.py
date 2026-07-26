"""Multi-room aggregation over a single stream. / 多房间聚合管理器

监控 N 个房间的应用(开播提醒 bot、弹幕采集)都要手写同一套样板:
每房间一个任务、异常隔离、聚合队列、优雅关停。``DanmakuHub`` 把这层
上移进库::

    from aiodouyu import DanmakuHub

    hub = DanmakuHub(types={"rss"}, emit_connection_events=True)
    # 受限网络可让每个房间都走 WebSocket:transport 等参数直接透传
    # hub = DanmakuHub(types={"rss"}, transport="auto")
    await hub.add(9999)
    await hub.add(288016)
    async for room_id, msg in hub:
        print(room_id, msg["type"])
    # ... 其他任务中: await hub.remove(9999) / await hub.close()

语义:

- ``add``/``remove`` 幂等,运行中可动态增删
- 每房间一个内部 :class:`DanmakuClient`(自动重连、退避、空闲检测
  均在客户端层),单房故障只影响自身
- 聚合队列有界:``overflow="block"`` 时慢消费者反压到各房间的 TCP
  接收(稳态不丢消息;``remove``/``close`` 时该房间在途的一条与队列
  残留消息按语义丢弃);``overflow="drop_oldest"`` 时丢最旧消息保新
- ``close()`` 关停全部房间与内部任务,迭代随之终止;与
  ``DanmakuClient`` 一样,一个 Hub 实例只支持一轮消费
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from collections.abc import AsyncIterator, Callable

from .client import DanmakuClient
from .exceptions import ConnectionClosed

__all__ = ["DanmakuHub"]

logger = logging.getLogger("aiodouyu")

_SENTINEL: tuple[int, dict[str, str]] | None = None


class DanmakuHub:
    """N 个房间的统一消息流

    Args:
        types: 透传给每个房间客户端的消息类型过滤
        emit_connection_events: 透传;伪事件同样带房间号产出
        queue_maxsize: 聚合队列容量
        overflow: 队列满时的策略。"block" 反压(默认,不丢消息);
            "drop_oldest" 丢最旧保最新(适合只关心最新状态的消费方)
        client_factory: 自定义客户端工厂 ``fn(room_id) -> DanmakuClient``;
            默认工厂按上述参数与 ``client_kwargs`` 创建
        **client_kwargs: 透传给每个 DanmakuClient 的其余参数,例如
            ``transport="ws"``、``idle_timeout=180``、``backoff_max=30``
    """

    def __init__(
        self,
        *,
        types: set[str] | None = None,
        emit_connection_events: bool = False,
        queue_maxsize: int = 1024,
        overflow: str = "block",
        client_factory: Callable[[int], DanmakuClient] | None = None,
        **client_kwargs,
    ) -> None:
        if overflow not in ("block", "drop_oldest"):
            raise ValueError(f'overflow 必须是 "block" 或 "drop_oldest",收到 {overflow!r}')
        if overflow == "drop_oldest" and queue_maxsize < 1:
            # asyncio.Queue 的 maxsize<=0 是无界语义:队列永不满,
            # drop_oldest 永不生效,反而无上限堆积——与用户意图相反
            raise ValueError(
                "overflow='drop_oldest' 需要 queue_maxsize >= 1"
                f"(收到 {queue_maxsize},该值表示无界队列)"
            )
        self._overflow = overflow
        self._queue: asyncio.Queue = asyncio.Queue(maxsize=queue_maxsize)
        self._factory = client_factory or (
            lambda rid: DanmakuClient(
                rid,
                types=types,
                emit_connection_events=emit_connection_events,
                **client_kwargs,
            )
        )
        # room_id -> (client, pump_task)
        self._rooms: dict[int, tuple[DanmakuClient, asyncio.Task]] = {}
        self._closed = False
        self._iterating = False

    # ==================== 公共接口 ====================

    @property
    def rooms(self) -> set[int]:
        """当前管理的房间号集合"""
        return set(self._rooms)

    @property
    def closed(self) -> bool:
        return self._closed

    async def add(self, room_id: int) -> bool:
        """添加房间(幂等)。返回是否新增"""
        if self._closed:
            raise ConnectionClosed("Hub 已关闭")
        if room_id in self._rooms:
            return False
        client = self._factory(room_id)
        task = asyncio.create_task(self._pump(room_id, client))
        self._rooms[room_id] = (client, task)
        return True

    async def remove(self, room_id: int) -> bool:
        """移除房间(幂等)。返回是否确实移除

        注意:泵可能正阻塞在满队列的 put 上(block 模式且消费者慢或
        缺席),client.close() 只终止客户端迭代、解除不了队列等待——
        必须取消泵任务,否则 remove/close 会永久挂死。被取消的那条
        在途消息随本房间一起丢弃(remove 的语义本就是停止该房间)。
        """
        entry = self._rooms.pop(room_id, None)
        if entry is None:
            return False
        client, task = entry
        try:
            with contextlib.suppress(Exception):
                await client.close()
        finally:
            # 必须在 finally 里:client.close() 含真实挂起点(keepalive
            # gather、最长 1s 的 wait_closed),调用方在此被取消时
            # CancelledError 会越过 suppress(Exception) 直接传播,
            # 而条目已被 pop——不 cancel 就成了 close() 也回收不到的
            # 孤儿泵(block 模式下永久阻塞在满队列 put 上)
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        return True

    async def close(self) -> None:
        """关停全部房间;迭代随之终止。幂等"""
        self._closed = True
        rooms = list(self._rooms)
        for rid in rooms:
            await self.remove(rid)
        # 唤醒可能阻塞在空队列上的消费者
        with contextlib.suppress(asyncio.QueueFull):
            self._queue.put_nowait(_SENTINEL)

    async def __aenter__(self) -> DanmakuHub:
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        await self.close()

    def __aiter__(self) -> AsyncIterator[tuple[int, dict[str, str]]]:
        return self._iterate()

    # ==================== 内部实现 ====================

    async def _iterate(self) -> AsyncIterator[tuple[int, dict[str, str]]]:
        if self._iterating:
            raise RuntimeError("同一 Hub 只允许一个消费迭代器")
        if self._closed:
            raise ConnectionClosed("Hub 已关闭")
        self._iterating = True  # 单轮消费:标志不复位,break 后不可重入
        while True:
            item = await self._queue.get()
            if item is _SENTINEL or self._closed:
                return
            yield item

    async def _put(self, item: tuple[int, dict[str, str]]) -> None:
        if self._overflow == "block":
            # 反压:队列满时本房间的泵挂起,其 TCP 接收随之减速;
            # 不丢消息,代价是慢消费者拖慢所有房间的消费时效
            await self._queue.put(item)
            return
        # drop_oldest:丢最旧保最新
        while True:
            try:
                self._queue.put_nowait(item)
                return
            except asyncio.QueueFull:
                with contextlib.suppress(asyncio.QueueEmpty):
                    dropped = self._queue.get_nowait()
                    if dropped is not _SENTINEL:
                        logger.debug("Hub 队列满,丢弃最旧消息(房间 %s)", dropped[0])

    async def _pump(self, room_id: int, client: DanmakuClient) -> None:
        """单房间泵:把客户端消息打上房间号送进聚合队列"""
        try:
            async for msg in client:
                await self._put((room_id, msg))
                if self._closed:
                    return
        except asyncio.CancelledError:
            raise
        except Exception as e:
            # 单房故障隔离:客户端自带重连,走到这里说明发生了意外异常
            # (如 reconnect=False 的 ConnectionClosed);记日志不传染
            if not self._closed and room_id in self._rooms:
                logger.error("Hub 房间 %s 泵异常退出: %s", room_id, e)
        finally:
            # 自摘条目:泵异常退出后不留僵尸房(否则 hub.rooms 谎报
            # 该房受管、add() 以"已存在"拒绝重加、消息静默停流)。
            # 用任务身份判断,避免误删 remove 后并发 re-add 的新条目
            entry = self._rooms.get(room_id)
            if entry is not None and entry[1] is asyncio.current_task():
                self._rooms.pop(room_id, None)
            with contextlib.suppress(Exception):
                await client.close()
