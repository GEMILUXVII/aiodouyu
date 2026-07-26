"""斗鱼弹幕协议二进制成帧

包结构（全部小端序）::

    消息长度 (4 字节) | 消息长度 (4 字节, 重复) | 消息类型 (2 字节)
    | 加密字段 (1 字节, 恒 0) | 保留字段 (1 字节, 恒 0)
    | 数据 (UTF-8) | 结束符 '\\0'

其中"消息长度"= 第二个长度字段(4) + 类型(2) + 加密(1) + 保留(1)
+ 数据长度 + 结束符(1)，即数据长度 + 9。

消息类型：客户端 -> 服务端为 689，服务端 -> 客户端为 690。
"""

from __future__ import annotations

import struct

from .exceptions import AiodouyuError

__all__ = [
    "HEADER_TAIL_LENGTH",
    "MAX_PACKET_SIZE",
    "MSG_TYPE_CLIENT",
    "MSG_TYPE_SERVER",
    "PacketError",
    "extract_payload",
    "pack",
    "validate_length",
]

MSG_TYPE_CLIENT = 689
MSG_TYPE_SERVER = 690

# 首个长度字段之后、载荷之前的头部字节数：长度(4) + 类型(2) + 加密(1) + 保留(1)
HEADER_TAIL_LENGTH = 8

# 单包长度上限（防御协议失步导致的超大内存分配）
MAX_PACKET_SIZE = 1 << 20

_HEADER = struct.Struct("<IIHBB")


class PacketError(AiodouyuError, ValueError):
    """包结构非法

    同时继承 AiodouyuError(与库其余异常同族,裸用 packet 模块时
    except AiodouyuError 可捕获)与 ValueError(保持向后兼容)。
    """


def pack(payload: str, msg_type: int = MSG_TYPE_CLIENT) -> bytes:
    """把 STT 载荷组装为完整二进制包"""
    data = payload.encode("utf-8")
    length = HEADER_TAIL_LENGTH + len(data) + 1
    return _HEADER.pack(length, length, msg_type, 0, 0) + data + b"\x00"


def extract_payload(body: bytes) -> str:
    """从包体提取 STT 载荷

    Args:
        body: 首个长度字段之后的全部字节
              （第二个长度字段 + 类型 + 加密 + 保留 + 数据 + 结束符）

    Returns:
        UTF-8 解码后的载荷字符串（尾部 ``\\0`` 已剥离；
        非法字节以 U+FFFD 替换，不抛异常）
    """
    if len(body) < HEADER_TAIL_LENGTH + 1:
        raise PacketError(f"包体过短: {len(body)} 字节")
    return body[HEADER_TAIL_LENGTH:].rstrip(b"\x00").decode("utf-8", errors="replace")


def validate_length(length: int) -> None:
    """校验首个长度字段的合理性，防止协议失步"""
    if length < HEADER_TAIL_LENGTH + 1 or length > MAX_PACKET_SIZE:
        raise PacketError(f"非法包长度: {length}")
