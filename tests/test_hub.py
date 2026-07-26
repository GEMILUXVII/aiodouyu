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
