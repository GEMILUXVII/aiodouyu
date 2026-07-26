"""DanmakuClient 集成测试（本机假弹幕服务器，不联网）"""

import asyncio
import contextlib
import struct

import pytest

from aiodouyu import (
    EVENT_CONNECTED,
    EVENT_DISCONNECTED,
    ConnectionClosed,
    DanmakuClient,
    packet,
    stt,
)

pytestmark = pytest.mark.asyncio


class FakeDanmakuServer:
    """最小斗鱼弹幕服务器：校验握手、按脚本下发消息"""

    def __init__(self):
        self.server = None
        self.port = None
        self.connections = 0
        self.received: list[dict[str, str]] = []
        # 每次连接建立后调用：async fn(server, reader, writer, conn_index)
        self.script = None
        self._handler_tasks: set[asyncio.Task] = set()

    async def start(self):
        self.server = await asyncio.start_server(self._handle, "127.0.0.1", 0)
        self.port = self.server.sockets[0].getsockname()[1]

    async def stop(self):
        # 必须先取消 handler 任务：Python 3.13 起 wait_closed() 会等待
        # 所有连接 handler 结束，而测试脚本里有长 sleep，不取消会把
        # 每个用例的 teardown 拖慢数十秒
        for task in list(self._handler_tasks):
            task.cancel()
        if self._handler_tasks:
            await asyncio.gather(*self._handler_tasks, return_exceptions=True)
        if self.server:
            self.server.close()
            await self.server.wait_closed()

    async def read_client_message(self, reader) -> dict[str, str]:
        head = await reader.readexactly(4)
        (length,) = struct.unpack("<I", head)
        body = await reader.readexactly(length)
        msg = stt.loads(packet.extract_payload(body))
        self.received.append(msg)
        return msg

    def send(self, writer, fields: dict) -> None:
        writer.write(packet.pack(stt.dumps(fields), msg_type=packet.MSG_TYPE_SERVER))

    async def _handle(self, reader, writer):
        task = asyncio.current_task()
        if task is not None:
            self._handler_tasks.add(task)
            task.add_done_callback(self._handler_tasks.discard)
        index = self.connections
        self.connections += 1
        try:
            login = await self.read_client_message(reader)
            assert login.get("type") == "loginreq"
            join = await self.read_client_message(reader)
            assert join.get("type") == "joingroup"
            self.send(writer, {"type": "loginres", "userid": "0"})
            await writer.drain()
            if self.script:
                await self.script(self, reader, writer, index)
        except (asyncio.IncompleteReadError, ConnectionError):
            pass
        finally:
            with contextlib.suppress(Exception):
                writer.close()


@pytest.fixture
async def server():
    srv = FakeDanmakuServer()
    await srv.start()
    yield srv
    await srv.stop()


def make_client(server, **kwargs) -> DanmakuClient:
    defaults = {
        "room_id": 1234,
        "host": "127.0.0.1",
        "port": server.port,
        "connect_timeout": 2.0,
        "backoff_initial": 0.05,
        "backoff_max": 0.2,
        # 测试连接都是短命的,默认关掉秒断退避,免得拖慢重连类用例;
        # 专测退避的用例自行覆盖
        "min_uptime": 0.0,
    }
    defaults.update(kwargs)
    return DanmakuClient(**defaults)


async def collect(client, count, timeout=5.0):
    messages = []

    async def _consume():
        async for msg in client:
            messages.append(msg)
            if len(messages) >= count:
                return

    await asyncio.wait_for(_consume(), timeout)
    return messages


async def test_handshake_and_messages(server):
    async def script(srv, reader, writer, index):
        srv.send(writer, {"type": "rss", "ss": "1", "ivl": "0"})
        srv.send(writer, {"type": "chatmsg", "nn": "用户", "txt": "弹幕/内容"})
        await writer.drain()
        await asyncio.sleep(10)

    server.script = script
    client = make_client(server)
    try:
        messages = await collect(client, 3)
    finally:
        await client.close()

    assert messages[0]["type"] == "loginres"
    assert messages[1] == {"type": "rss", "ss": "1", "ivl": "0"}
    assert messages[2]["txt"] == "弹幕/内容"  # 转义还原

    login, join = server.received[0], server.received[1]
    assert login == {"type": "loginreq", "roomid": "1234"}
    assert join == {"type": "joingroup", "rid": "1234", "gid": "-9999"}


async def test_types_filter(server):
    async def script(srv, reader, writer, index):
        srv.send(writer, {"type": "chatmsg", "txt": "skip"})
        srv.send(writer, {"type": "rss", "ss": "1", "ivl": "0"})
        await writer.drain()
        await asyncio.sleep(10)

    server.script = script
    client = make_client(server, types={"rss"})
    try:
        messages = await collect(client, 1)
    finally:
        await client.close()
    assert messages == [{"type": "rss", "ss": "1", "ivl": "0"}]


async def test_reconnect_with_events(server):
    async def script(srv, reader, writer, index):
        srv.send(writer, {"type": "rss", "ss": "1", "ivl": "0"})
        await writer.drain()
        if index == 0:
            return  # 第一条连接立即断开，触发重连
        await asyncio.sleep(10)

    server.script = script
    client = make_client(server, emit_connection_events=True, types={"rss"})
    try:
        # 期望序列: connected, rss, disconnected, connected, rss
        messages = await collect(client, 5)
    finally:
        await client.close()

    kinds = [m["type"] for m in messages]
    assert kinds == [
        EVENT_CONNECTED,
        "rss",
        EVENT_DISCONNECTED,
        EVENT_CONNECTED,
        "rss",
    ]
    assert server.connections == 2


async def test_no_reconnect_raises(server):
    async def script(srv, reader, writer, index):
        return  # 握手完成即断开

    server.script = script
    client = make_client(server, reconnect=False)

    async def consume():
        async for _ in client:
            pass

    with pytest.raises(ConnectionClosed):
        await asyncio.wait_for(consume(), 5)
    await client.close()


async def test_close_terminates_iteration(server):
    async def script(srv, reader, writer, index):
        await asyncio.sleep(30)

    server.script = script
    client = make_client(server)

    async def consume():
        async for _ in client:
            pass

    task = asyncio.create_task(consume())
    await asyncio.sleep(0.3)
    await client.close()
    await asyncio.wait_for(task, 5)  # close 后迭代应立即结束
    assert client.closed


async def test_keepalive_sent(server):
    done = asyncio.Event()

    async def script(srv, reader, writer, index):
        msg = await srv.read_client_message(reader)
        assert msg == {"type": "mrkl"}
        done.set()
        await asyncio.sleep(10)

    server.script = script
    client = make_client(server, keepalive_interval=0.1)

    async def consume():
        async for _ in client:
            pass

    task = asyncio.create_task(consume())
    try:
        await asyncio.wait_for(done.wait(), 5)
    finally:
        await client.close()
        with contextlib.suppress(Exception):
            await asyncio.wait_for(task, 5)


async def test_idle_timeout_triggers_reconnect(server):
    async def script(srv, reader, writer, index):
        await asyncio.sleep(30)  # 不发任何消息，制造空闲

    server.script = script
    client = make_client(
        server,
        keepalive_interval=0.05,
        idle_timeout=0.2,
        emit_connection_events=True,
    )

    events = []

    async def consume():
        async for msg in client:
            events.append(msg["type"])
            if events.count(EVENT_CONNECTED) >= 2:
                return

    # 空闲超时 -> 中止 -> 重连出现第二次 connected
    await asyncio.wait_for(consume(), 5)
    await client.close()
    assert EVENT_DISCONNECTED in events


async def test_handler_mode(server):
    async def script(srv, reader, writer, index):
        srv.send(writer, {"type": "rss", "ss": "1", "ivl": "0"})
        await writer.drain()
        await asyncio.sleep(10)

    server.script = script
    client = make_client(server)
    got_sync = asyncio.Event()
    got_async = asyncio.Event()
    got_wildcard = asyncio.Event()

    @client.on("rss")
    def sync_handler(msg):
        assert msg["ss"] == "1"
        got_sync.set()

    @client.on("rss")
    async def async_handler(msg):
        got_async.set()

    @client.on("*")
    def wildcard(msg):
        got_wildcard.set()

    task = asyncio.create_task(client.run())
    try:
        await asyncio.wait_for(
            asyncio.gather(got_sync.wait(), got_async.wait(), got_wildcard.wait()), 5
        )
    finally:
        await client.close()
        with contextlib.suppress(Exception):
            await asyncio.wait_for(task, 5)


async def test_concurrent_iteration_rejected(server):
    client = make_client(server)
    it1 = client.__aiter__()
    task = asyncio.create_task(it1.__anext__())
    await asyncio.sleep(0.1)
    with pytest.raises(RuntimeError):
        await client.__aiter__().__anext__()
    await client.close()
    with contextlib.suppress(Exception):
        await asyncio.wait_for(task, 5)


async def test_invalid_room_id():
    with pytest.raises(ValueError):
        DanmakuClient(room_id=0)


async def test_close_interrupts_backoff_sleep():
    """close() 必须立即打断重连退避等待,而非睡满整个 delay"""
    # 连一个无监听的端口:连接立即失败,进入长退避
    probe = await asyncio.start_server(lambda r, w: None, "127.0.0.1", 0)
    free_port = probe.sockets[0].getsockname()[1]
    probe.close()
    await probe.wait_closed()

    client = DanmakuClient(
        room_id=1234,
        host="127.0.0.1",
        port=free_port,
        connect_timeout=0.5,
        backoff_initial=30.0,  # 不打断的话迭代要挂 15 秒以上
        backoff_max=60.0,
    )

    async def consume():
        async for _ in client:
            pass

    task = asyncio.create_task(consume())
    await asyncio.sleep(0.5)  # 让首次连接失败并进入退避等待
    start = asyncio.get_running_loop().time()
    await client.close()
    await asyncio.wait_for(task, 2.0)
    assert asyncio.get_running_loop().time() - start < 2.0


async def test_close_during_connect_no_zombie_connection(server, monkeypatch):
    """close() 落在 open_connection 等待期间时,连接不得在关闭后存活"""

    async def script(srv, reader, writer, index):
        srv.send(writer, {"type": "rss", "ss": "1", "ivl": "0"})
        await writer.drain()
        await asyncio.sleep(10)

    server.script = script
    client = make_client(server, emit_connection_events=True)

    gate = asyncio.Event()
    entered = asyncio.Event()
    real_open = asyncio.open_connection

    async def gated_open(*args, **kwargs):
        entered.set()
        await gate.wait()
        return await real_open(*args, **kwargs)

    monkeypatch.setattr(asyncio, "open_connection", gated_open)

    messages = []

    async def consume():
        async for msg in client:
            messages.append(msg)

    task = asyncio.create_task(consume())
    await asyncio.wait_for(entered.wait(), 5)  # 消费方挂在 open_connection 中
    await client.close()
    gate.set()  # 放行:连接会建立,但必须被 close 后的复查立即拆除
    await asyncio.wait_for(task, 5)

    assert messages == []  # 关闭后不得产出任何消息(包括 EVENT_CONNECTED)
    assert not client.connected


async def test_short_lived_connections_backoff(server):
    """服务端接受后立即断开时必须持续退避,不得热重连"""

    async def script(srv, reader, writer, index):
        return  # 握手完成即断开

    server.script = script
    client = make_client(
        server,
        backoff_initial=10.0,  # 首次退避 ≥5s(抖动下界 0.5),窗口内不会到期
        backoff_max=60.0,
        min_uptime=5.0,
    )

    async def consume():
        async for _ in client:
            pass

    task = asyncio.create_task(consume())
    await asyncio.sleep(1.0)
    await client.close()
    with contextlib.suppress(Exception):
        await asyncio.wait_for(task, 5)
    # 无退避时 1 秒内会重连数十次;有秒断退避时至多 2 次
    assert server.connections <= 2


async def test_close_while_consumer_suspended_in_loop_body(server):
    """消费方挂起在循环体内时另一任务 close():迭代必须干净结束

    回归:旧实现对活跃迭代器 await aclose(),其 finally 的挂起点与
    消费方下一次 __anext__ 竞争,抛 RuntimeError('generator is
    already running')。
    """

    async def script(srv, reader, writer, index):
        for _ in range(5):
            srv.send(writer, {"type": "chatmsg", "txt": "x"})
        await writer.drain()
        await asyncio.sleep(10)

    server.script = script
    client = make_client(server)
    in_body = asyncio.Event()
    release = asyncio.Event()
    errors: list[BaseException] = []

    async def consume():
        try:
            async for _ in client:
                in_body.set()
                await release.wait()  # 挂起在循环体内,生成器停在 yield
        except RuntimeError as exc:
            errors.append(exc)

    task = asyncio.create_task(consume())
    await asyncio.wait_for(in_body.wait(), 5)
    close_task = asyncio.create_task(client.close())
    # 让 close() 推进若干 tick(旧实现此时 aclose 正卡在生成器 finally)
    for _ in range(3):
        await asyncio.sleep(0)
    release.set()  # 消费方恢复并调用 __anext__
    await asyncio.wait_for(task, 5)
    await asyncio.wait_for(close_task, 5)
    assert errors == []
    assert client.closed


async def test_backoff_delay_capped():
    """抖动不得使退避超过 backoff_max,大 attempt 不得溢出"""
    client = DanmakuClient(room_id=1, backoff_initial=1.0, backoff_max=60.0)
    for attempt in [0, 1, 5, 10, 64, 1000]:
        for _ in range(20):
            delay = client._backoff_delay(attempt)
            assert 0 < delay <= 60.0
