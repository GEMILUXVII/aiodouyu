"""aiodouyu - 斗鱼弹幕 asyncio 客户端库

连接斗鱼弹幕服务器，以异步迭代或回调方式消费房间消息
（开播状态 rss、弹幕 chatmsg、礼物 dgb 等）。

快速上手::

    import asyncio
    from aiodouyu import DanmakuClient

    async def main():
        async with DanmakuClient(room_id=9999) as client:
            async for msg in client:
                print(msg.get("type"), msg)

    asyncio.run(main())
"""

from __future__ import annotations

import logging

from . import models, packet, stt, transport, web
from .client import EVENT_CONNECTED, EVENT_DISCONNECTED, DanmakuClient
from .exceptions import (
    AiodouyuError,
    ApiError,
    ConnectionClosed,
    ProtocolError,
    RoomNotFound,
)
from .hub import DanmakuHub
from .monitor import LiveStatusMonitor

# 注:包属性 aiodouyu.replay 是回放函数;录制辅助函数经
# `from aiodouyu.replay import write_header, write_message` 取用
from .replay import replay
from .web import RoomInfo, fetch_room, fetch_rooms, resolve_room_id

__version__ = "0.4.11"

__all__ = [
    "EVENT_CONNECTED",
    "EVENT_DISCONNECTED",
    "AiodouyuError",
    "ApiError",
    "ConnectionClosed",
    "DanmakuClient",
    "DanmakuHub",
    "LiveStatusMonitor",
    "ProtocolError",
    "RoomInfo",
    "RoomNotFound",
    "__version__",
    "fetch_room",
    "fetch_rooms",
    "models",
    "packet",
    "replay",
    "resolve_room_id",
    "stt",
    "transport",
    "web",
]

logging.getLogger("aiodouyu").addHandler(logging.NullHandler())
