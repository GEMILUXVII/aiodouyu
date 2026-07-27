"""Offline test helpers: a scriptable fake danmaku server. / 离线测试基建

把库内部测试用的假弹幕服务器提炼为公共 API,下游(如 AstrBot 插件)
可以不联网地测试自己的消费逻辑::

    from aiodouyu.testing import FakeDanmakuServer

    async def test_my_consumer():
        async with FakeDanmakuServer() as server:
            async def script(srv, reader, writer, index):
                srv.send(writer, {"type": "rss", "ss": "1", "ivl": "0"})
                await writer.drain()
                await asyncio.sleep(10)

            server.script = script
            client = server.make_client(types={"rss"})
            async for msg in client:
                assert msg["ss"] == "1"
                break
            await client.close()

协议行为:校验 loginreq 后回 loginres,再校验 joingroup,之后执行
``script(server, reader, writer, conn_index)``(每个连接调用一次)。
握手校验失败记入 ``server.errors``(公共 API 不用 assert——
``python -O`` 下 assert 会消失)。
"""

from __future__ import annotations

import asyncio
import contextlib
import struct
from collections.abc import Awaitable, Callable
from typing import Any

from . import packet, stt
from .client import DanmakuClient

__all__ = ["FakeDanmakuServer"]

Script = Callable[
    ["FakeDanmakuServer", asyncio.StreamReader, asyncio.StreamWriter, int],
    Awaitable[None],
]


class FakeDanmakuServer:
    """最小斗鱼弹幕服务器:校验握手、按脚本下发消息

    Attributes:
        port: 监听端口(start 后可用)
        connections: 累计接受的连接数
        received: 收到的全部客户端消息(含握手)
        errors: 协议校验错误列表(非空说明客户端握手不合规)
        login_response_gate: 可选事件;设置后服务端等待事件再回复 loginres
        script: 每个连接握手完成后调用的协程
            ``async fn(server, reader, writer, conn_index)``
    """

    def __init__(self) -> None:
        self.server: asyncio.Server | None = None
        self.port: int | None = None
        self.connections = 0
        self.received: list[dict[str, str]] = []
        self.errors: list[str] = []
        self.login_response_gate: asyncio.Event | None = None
        self.script: Script | None = None
        self._handler_tasks: set[asyncio.Task] = set()

    async def start(self) -> None:
        self.server = await asyncio.start_server(self._handle, "127.0.0.1", 0)
        self.port = self.server.sockets[0].getsockname()[1]

    async def stop(self) -> None:
        # 先取消 handler 任务:Python 3.13 起 wait_closed() 会等所有
        # 连接 handler 结束,而测试脚本里常有长 sleep
        for task in list(self._handler_tasks):
            task.cancel()
        if self._handler_tasks:
            await asyncio.gather(*self._handler_tasks, return_exceptions=True)
        if self.server:
            self.server.close()
            await self.server.wait_closed()

    async def __aenter__(self) -> FakeDanmakuServer:
        await self.start()
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        await self.stop()

    def make_client(self, **overrides: Any) -> DanmakuClient:
        """创建已指向本服务器、短退避参数的客户端(下游测试的第一行)"""
        if self.port is None:
            raise RuntimeError("请先 start()(或使用 async with)")
        defaults: dict[str, Any] = {
            "room_id": 1234,
            "host": "127.0.0.1",
            "port": self.port,
            "connect_timeout": 2.0,
            "backoff_initial": 0.05,
            "backoff_max": 0.2,
            "min_uptime": 0.0,
        }
        defaults.update(overrides)
        return DanmakuClient(**defaults)

    async def read_client_message(self, reader: asyncio.StreamReader) -> dict[str, str]:
        """读取并解码一条客户端消息(脚本内可用)"""
        head = await reader.readexactly(4)
        (length,) = struct.unpack("<I", head)
        body = await reader.readexactly(length)
        msg = stt.loads(packet.extract_payload(body))
        self.received.append(msg)
        return msg

    def send(self, writer: asyncio.StreamWriter, fields: dict) -> None:
        """向客户端下发一条服务端消息(脚本内可用,记得 await drain)"""
        writer.write(packet.pack(stt.dumps(fields), msg_type=packet.MSG_TYPE_SERVER))

    async def _handle(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        task = asyncio.current_task()
        if task is not None:
            self._handler_tasks.add(task)
            task.add_done_callback(self._handler_tasks.discard)
        index = self.connections
        self.connections += 1
        try:
            login = await self.read_client_message(reader)
            if login.get("type") != "loginreq":
                self.errors.append(f"首包不是 loginreq: {login}")
            if self.login_response_gate is not None:
                await self.login_response_gate.wait()
            self.send(writer, {"type": "loginres", "userid": "0"})
            await writer.drain()
            join = await self.read_client_message(reader)
            if join.get("type") != "joingroup":
                self.errors.append(f"第二包不是 joingroup: {join}")
            if self.script:
                await self.script(self, reader, writer, index)
        except (asyncio.IncompleteReadError, ConnectionError):
            pass
        finally:
            with contextlib.suppress(Exception):
                writer.close()
