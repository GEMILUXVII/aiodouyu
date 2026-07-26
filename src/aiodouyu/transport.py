"""Byte-stream transports: raw TCP and a minimal WebSocket client. /
字节流传输层:裸 TCP 与最小 WebSocket 客户端

两种传输对客户端暴露同一字节流接口(读定长/写入/中止),弹幕协议的
成帧(689/690 长度前缀包)在其上原样运行:

- ``TcpTransport``: ``danmuproxy.douyu.com:8601`` 明文 TCP(默认)
- ``WsTransport``: ``wss://danmuproxy.douyu.com:8506``,网页端同款。
  手写最小 RFC 6455 客户端(仅客户端角色、二进制帧、零扩展协商),
  保持零运行时依赖。价值:TCP 8601 常被企业防火墙/部分云出口拦截,
  而 443 系 TLS WebSocket 存活率高;同时对冲非官方 TCP 端点再次迁移

ping/pong 在传输层透明处理;close 帧与 EOF 一致地表现为读取失败,
由客户端层的重连逻辑接管。
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import os
import ssl
import struct

from .exceptions import ProtocolError

__all__ = ["TcpTransport", "WsTransport", "WS_DEFAULT_HOST", "WS_DEFAULT_PORT"]

WS_DEFAULT_HOST = "danmuproxy.douyu.com"
WS_DEFAULT_PORT = 8506

_WS_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
# 单帧载荷上限:防御失步/恶意长度导致的超大分配(与 packet 层一致量级)
_MAX_FRAME_PAYLOAD = 1 << 22

_OP_CONT = 0x0
_OP_TEXT = 0x1
_OP_BINARY = 0x2
_OP_CLOSE = 0x8
_OP_PING = 0x9
_OP_PONG = 0xA


class TcpTransport:
    """裸 TCP 字节流(现行为的抽象化,语义与 0.3 之前完全一致)"""

    def __init__(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        self._reader = reader
        self._writer = writer

    @classmethod
    async def connect(cls, host: str, port: int) -> TcpTransport:
        reader, writer = await asyncio.open_connection(host, port)
        return cls(reader, writer)

    @property
    def is_closing(self) -> bool:
        return self._writer.is_closing()

    async def read_exactly(self, n: int) -> bytes:
        return await self._reader.readexactly(n)

    def write(self, data: bytes) -> None:
        self._writer.write(data)

    async def drain(self) -> None:
        await self._writer.drain()

    def abort(self) -> None:
        transport = self._writer.transport
        if transport is not None:
            transport.abort()

    def close(self) -> None:
        self._writer.close()

    async def wait_closed(self) -> None:
        await self._writer.wait_closed()


def encode_frame(opcode: int, payload: bytes, *, mask: bool = True) -> bytes:
    """编码一个 WebSocket 帧(客户端角色须掩码)"""
    header = bytearray([0x80 | opcode])  # FIN + opcode
    length = len(payload)
    mask_bit = 0x80 if mask else 0
    if length < 126:
        header.append(mask_bit | length)
    elif length < (1 << 16):
        header.append(mask_bit | 126)
        header += struct.pack(">H", length)
    else:
        header.append(mask_bit | 127)
        header += struct.pack(">Q", length)
    if not mask:
        return bytes(header) + payload
    key = os.urandom(4)
    header += key
    masked = bytes(b ^ key[i % 4] for i, b in enumerate(payload))
    return bytes(header) + masked


async def read_frame(read_exactly) -> tuple[int, bool, bytes]:
    """读一个帧,返回 (opcode, fin, payload);服务端帧按 RFC 不掩码,
    但为健壮性同样支持掩码帧"""
    b1, b2 = await read_exactly(2)
    fin = bool(b1 & 0x80)
    opcode = b1 & 0x0F
    masked = bool(b2 & 0x80)
    length = b2 & 0x7F
    if length == 126:
        (length,) = struct.unpack(">H", await read_exactly(2))
    elif length == 127:
        (length,) = struct.unpack(">Q", await read_exactly(8))
    if length > _MAX_FRAME_PAYLOAD:
        raise ProtocolError(f"WebSocket 帧过大: {length}")
    key = await read_exactly(4) if masked else None
    payload = await read_exactly(length) if length else b""
    if key:
        payload = bytes(b ^ key[i % 4] for i, b in enumerate(payload))
    return opcode, fin, payload


class WsTransport:
    """最小 WebSocket 客户端传输

    ``read_exactly`` 从内部缓冲取字节,缓冲由数据帧载荷填充——弹幕的
    长度前缀包可以跨帧/一帧多包,字节流语义与 TCP 完全一致。
    """

    def __init__(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        self._reader = reader
        self._writer = writer
        self._buffer = bytearray()
        self._eof = False

    @staticmethod
    def default_ssl_context() -> ssl.SSLContext:
        """斗鱼 wss 端点可用的 TLS 上下文

        实测:danmuproxy 的 wss 端点只提供 ``AES256-GCM-SHA384``
        (RSA 密钥交换、无 ECDHE、无前向保密),现代 OpenSSL 的默认
        SECLEVEL=2 会直接拒绝并抛
        ``SSLV3_ALERT_HANDSHAKE_FAILURE``。这里只把密码套件安全级别
        降到 1,**证书校验与主机名校验保持开启**(实测无需关闭)。
        """
        ctx = ssl.create_default_context()
        ctx.set_ciphers("DEFAULT@SECLEVEL=1")
        return ctx

    @classmethod
    async def connect(
        cls,
        host: str,
        port: int,
        *,
        use_tls: bool = True,
        resource: str = "/",
        ssl_context: ssl.SSLContext | None = None,
    ) -> WsTransport:
        ssl_ctx = (ssl_context or cls.default_ssl_context()) if use_tls else None
        reader, writer = await asyncio.open_connection(host, port, ssl=ssl_ctx)
        try:
            key = base64.b64encode(os.urandom(16)).decode()
            request = (
                f"GET {resource} HTTP/1.1\r\n"
                f"Host: {host}:{port}\r\n"
                "Upgrade: websocket\r\n"
                "Connection: Upgrade\r\n"
                f"Sec-WebSocket-Key: {key}\r\n"
                "Sec-WebSocket-Version: 13\r\n"
                "\r\n"
            )
            writer.write(request.encode("ascii"))
            await writer.drain()

            status = await reader.readline()
            if b"101" not in status.split(b" ", 2)[1:2] and b" 101 " not in status:
                raise ProtocolError(f"WebSocket 握手被拒: {status!r:.100}")
            accept_expected = base64.b64encode(
                hashlib.sha1((key + _WS_GUID).encode("ascii")).digest()
            ).decode()
            accept_got = None
            while True:
                line = await reader.readline()
                if line in (b"\r\n", b"\n", b""):
                    break
                name, _, value = line.decode("latin1").partition(":")
                if name.strip().lower() == "sec-websocket-accept":
                    accept_got = value.strip()
            if accept_got != accept_expected:
                raise ProtocolError("WebSocket 握手校验失败(Sec-WebSocket-Accept 不匹配)")
            return cls(reader, writer)
        except BaseException:
            transport = writer.transport
            if transport is not None:
                transport.abort()
            writer.close()
            raise

    @property
    def is_closing(self) -> bool:
        return self._writer.is_closing()

    async def read_exactly(self, n: int) -> bytes:
        while len(self._buffer) < n:
            if self._eof:
                raise asyncio.IncompleteReadError(bytes(self._buffer), n)
            opcode, _fin, payload = await read_frame(self._reader.readexactly)
            if opcode in (_OP_BINARY, _OP_TEXT, _OP_CONT):
                self._buffer += payload
            elif opcode == _OP_PING:
                # 传输层透明回 pong(RFC 要求回显载荷)
                self._writer.write(encode_frame(_OP_PONG, payload))
                await self._writer.drain()
            elif opcode == _OP_CLOSE:
                self._eof = True
            # pong(0xA)与未知控制帧:忽略
        out = bytes(self._buffer[:n])
        del self._buffer[:n]
        return out

    def write(self, data: bytes) -> None:
        # 每个弹幕包一个二进制帧(网页端同款粒度)
        self._writer.write(encode_frame(_OP_BINARY, data))

    async def drain(self) -> None:
        await self._writer.drain()

    def abort(self) -> None:
        transport = self._writer.transport
        if transport is not None:
            transport.abort()

    def close(self) -> None:
        self._writer.close()

    async def wait_closed(self) -> None:
        await self._writer.wait_closed()
