"""aiodouyu 异常类型"""

from __future__ import annotations

__all__ = [
    "AiodouyuError",
    "ApiError",
    "ConnectionClosed",
    "ProtocolError",
    "RoomNotFound",
]


class AiodouyuError(Exception):
    """aiodouyu 所有异常的基类"""


class ConnectionClosed(AiodouyuError):
    """连接已关闭或不可用"""


class ProtocolError(AiodouyuError):
    """收到无法解析的协议数据（如非法包长度）"""


class ApiError(AiodouyuError):
    """HTTP 接口请求失败或响应无法解析"""


class RoomNotFound(ApiError):
    """房间不存在或不可用"""
