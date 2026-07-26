"""DanmakuHub 多房间管理器测试(假服务器,不联网)"""

import asyncio
import contextlib

import pytest

from aiodouyu import ConnectionClosed, DanmakuClient, DanmakuHub
from aiodouyu.testing import FakeDanmakuServer

pytestmark = pytest.mark.asyncio


@pytest.fixture
async def server():
    async with FakeDanmakuServer() as srv:
        yield srv


def make_hub(server, **kwargs) -> DanmakuHub:
    def factory(rid: int) -> DanmakuClient:
        return server.make_client(room_id=rid, **kwargs.pop("client_kwargs", {}))

    kwargs.setdefault("client_factory", factory)
    return DanmakuHub(**kwargs)


async def test_two_rooms_aggregated_with_room_tags(server):
    async def script(srv, reader, writer, index):
        # 每条连接按其 loginreq 的房间号回一条 rss
        rid = srv.received[index * 2].get("roomid", "?")
        srv.send(writer, {"type": "rss", "ss": "1", "ivl": "0", "rid": str(rid)})
        await writer.drain()
        await asyncio.sleep(10)

    server.script = script
    hub = make_hub(server)
    await hub.add(111)
    await hub.add(222)
    assert hub.rooms == {111, 222}

    got = {}
    async def consume():
        async for room_id, msg in hub:
            if msg["type"] == "rss":
                got[room_id] = msg
            if len(got) >= 2:
                return

    await asyncio.wait_for(consume(), 5)
    assert set(got) == {111, 222}  # 消息正确打上房间号
    await hub.close()


async def test_add_remove_idempotent(server):
    async def script(srv, reader, writer, index):
        await asyncio.sleep(10)

    server.script = script
    hub = make_hub(server)
    assert await hub.add(1) is True
    assert await hub.add(1) is False  # 幂等
    assert await hub.remove(1) is True
    assert await hub.remove(1) is False
    assert hub.rooms == set()
    await hub.close()


async def test_remove_stops_only_that_room(server):
    async def script(srv, reader, writer, index):
        while True:
            rid = srv.received[index * 2].get("roomid", "?")
            srv.send(writer, {"type": "chatmsg", "rid": str(rid), "txt": "x"})
            await writer.drain()
            await asyncio.sleep(0.05)

    server.script = script
    hub = make_hub(server)
    await hub.add(1)
    await hub.add(2)

    seen_after_remove = []
    removed = asyncio.Event()

    async def consume():
        async for room_id, msg in hub:
            if msg["type"] != "chatmsg":
                continue
            if removed.is_set():
                seen_after_remove.append(room_id)
                if len(seen_after_remove) >= 5:
                    return

    task = asyncio.create_task(consume())
    await asyncio.sleep(0.3)
    await hub.remove(1)
    removed.set()
    await asyncio.wait_for(task, 5)
    # 移除后仅剩房间 2 的消息(队列中可能残留少量房间 1 的旧消息,
    # 但持续流入的必须只有房间 2)
    assert seen_after_remove[-3:] == [2, 2, 2]
    await hub.close()


async def test_close_terminates_iteration(server):
    async def script(srv, reader, writer, index):
        await asyncio.sleep(10)

    server.script = script
    hub = make_hub(server)
    await hub.add(1)

    async def consume():
        async for _ in hub:
            pass

    task = asyncio.create_task(consume())
    await asyncio.sleep(0.2)
    await hub.close()
    await asyncio.wait_for(task, 5)  # 迭代干净结束
    assert hub.closed
    # 关闭后的行为
    with pytest.raises(ConnectionClosed):
        await hub.add(3)
    assert hub.rooms == set()  # 全部房间已清理(泵任务在 remove 中已 await 收尾)


async def test_drop_oldest_overflow(server):
    async def script(srv, reader, writer, index):
        for i in range(20):
            srv.send(writer, {"type": "chatmsg", "txt": str(i)})
        await writer.drain()
        await asyncio.sleep(10)

    server.script = script
    hub = make_hub(server, queue_maxsize=5, overflow="drop_oldest")
    await hub.add(1)
    await asyncio.sleep(0.5)  # 让 20 条消息灌满 5 容量的队列

    got = []
    async def consume():
        async for _, msg in hub:
            if msg["type"] == "chatmsg":
                got.append(int(msg["txt"]))
                if msg["txt"] == "19":
                    return

    await asyncio.wait_for(consume(), 5)
    assert got[-1] == 19  # 最新的保住了
    assert len(got) < 20  # 旧的被丢了
    await hub.close()


async def test_remove_unblocks_pump_stuck_on_full_queue():
    """泵阻塞在满队列 put 上时 remove/close 不得挂死(回归:真死锁)"""

    class FloodClient:
        def __init__(self, rid):
            self.closed = False

        async def close(self):
            self.closed = True

        def __aiter__(self):
            return self._it()

        async def _it(self):
            i = 0
            while not self.closed:
                yield {"type": "chatmsg", "txt": str(i)}
                i += 1
                await asyncio.sleep(0)

    hub = DanmakuHub(queue_maxsize=2, client_factory=FloodClient)
    await hub.add(1)
    await asyncio.sleep(0.1)  # 无消费者,泵必然已阻塞在 put
    await asyncio.wait_for(hub.remove(1), timeout=2.0)  # 修前此处永久挂死
    await asyncio.wait_for(hub.close(), timeout=2.0)


async def test_dead_pump_is_not_a_zombie_room():
    """泵异常退出后必须自摘条目,可被重新 add(回归:僵尸房静默停流)"""

    class DyingClient:
        def __init__(self, rid):
            self.closed = False

        async def close(self):
            self.closed = True

        def __aiter__(self):
            return self._it()

        async def _it(self):
            yield {"type": "chatmsg", "txt": "before death"}
            raise ConnectionClosed("模拟 reconnect=False 的终止性断开")

    hub = DanmakuHub(client_factory=DyingClient)
    await hub.add(7)
    await asyncio.sleep(0.1)
    assert hub.rooms == set(), "泵已死,房间不应仍报告为受管"
    assert await hub.add(7) is True, "死泵后必须允许重新 add"
    await hub.close()


async def test_remove_cancelled_during_close_does_not_orphan_pump():
    """remove 在 client.close 挂起点被取消时,泵不得成为孤儿(回归)"""

    pump_started = asyncio.Event()

    class SlowCloseClient:
        def __init__(self, rid):
            self.closed = False

        async def close(self):
            self.closed = True
            await asyncio.sleep(0.5)  # 真实 close 含 keepalive/wait_closed

        def __aiter__(self):
            return self._it()

        async def _it(self):
            pump_started.set()
            while True:
                yield {"type": "chatmsg", "txt": "x"}
                await asyncio.sleep(0)

    hub = DanmakuHub(queue_maxsize=1, client_factory=SlowCloseClient)
    await hub.add(8)
    await asyncio.wait_for(pump_started.wait(), 2)
    await asyncio.sleep(0.1)  # 泵阻塞在满队列 put 上

    entry_task = hub._rooms[8][1]
    with contextlib.suppress(asyncio.TimeoutError):
        await asyncio.wait_for(hub.remove(8), timeout=0.1)  # 在 close 中被取消
    # 泵的 finally 里还要 await 这个假客户端的慢 close(0.5s),等它跑完
    await asyncio.wait_for(
        asyncio.gather(entry_task, return_exceptions=True), timeout=3
    )
    assert entry_task.cancelled() or entry_task.done(), "泵未被回收,成了孤儿"
    await asyncio.wait_for(hub.close(), timeout=2)


async def test_block_mode_lossless_with_slow_consumer():
    """block 模式的核心卖点:慢消费者不丢消息"""

    total = 50

    class CountingClient:
        def __init__(self, rid):
            self.closed = False
            self._done = asyncio.Event()

        async def close(self):
            self.closed = True
            self._done.set()

        def __aiter__(self):
            return self._it()

        async def _it(self):
            for i in range(total):
                yield {"type": "chatmsg", "txt": str(i)}
            await self._done.wait()  # 保持迭代器存活直到 close

    hub = DanmakuHub(queue_maxsize=2, client_factory=CountingClient)
    await hub.add(9)
    got = []

    async def slow_consume():
        async for _, msg in hub:
            got.append(int(msg["txt"]))
            await asyncio.sleep(0.002)  # 慢消费者
            if len(got) == total:
                return

    await asyncio.wait_for(slow_consume(), 10)
    assert got == list(range(total))  # 一条不丢、保序
    await hub.close()


async def test_invalid_overflow():
    with pytest.raises(ValueError):
        DanmakuHub(overflow="bogus")


async def test_single_iterator_guard(server):
    hub = make_hub(server)
    it1 = hub.__aiter__()
    task = asyncio.create_task(it1.__anext__())
    await asyncio.sleep(0.05)
    with pytest.raises(RuntimeError):
        await hub.__aiter__().__anext__()
    await hub.close()
    with contextlib.suppress(BaseException):
        await asyncio.wait_for(task, 5)
