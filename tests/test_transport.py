"""传输层测试:WebSocket 帧编解码与字节流语义(离线)"""

import asyncio
import struct

import pytest

from aiodouyu.exceptions import ProtocolError
from aiodouyu.transport import WsTransport, encode_frame, read_frame

pytestmark = pytest.mark.asyncio


def make_reader(data: bytes) -> asyncio.StreamReader:
    reader = asyncio.StreamReader()
    reader.feed_data(data)
    reader.feed_eof()
    return reader


class FakeWriter:
    """记录写出字节的 StreamWriter 替身"""

    def __init__(self):
        self.buffer = bytearray()
        self.closed = False

    def write(self, data):
        self.buffer += data

    async def drain(self):
        pass

    def close(self):
        self.closed = True

    def is_closing(self):
        return self.closed

    @property
    def transport(self):
        return None


async def test_encode_frame_client_must_mask():
    frame = encode_frame(0x2, b"hello")
    assert frame[0] == 0x82  # FIN + binary
    assert frame[1] & 0x80  # MASK 位必须置(客户端角色 RFC 要求)
    assert frame[1] & 0x7F == 5
    key = frame[2:6]
    payload = bytes(b ^ key[i % 4] for i, b in enumerate(frame[6:]))
    assert payload == b"hello"


@pytest.mark.parametrize("size", [0, 5, 125, 126, 200, 65535, 65536, 70000])
async def test_frame_roundtrip_all_length_forms(size):
    """7 位 / 16 位 / 64 位三种长度编码都要往返正确"""
    payload = bytes(range(256)) * (size // 256) + bytes(range(size % 256))
    frame = encode_frame(0x2, payload)
    reader = make_reader(frame)
    opcode, fin, got = await read_frame(reader.readexactly)
    assert opcode == 0x2 and fin is True
    assert got == payload


async def test_read_frame_rejects_oversized():
    # 声明 64 位长度 8MB(超过 4MB 上限)
    header = bytes([0x82, 127]) + struct.pack(">Q", 8 << 20)
    reader = make_reader(header)
    with pytest.raises(ProtocolError):
        await read_frame(reader.readexactly)


async def test_read_exactly_spans_frames():
    """字节流语义:一个弹幕包可以跨多个 WS 帧"""
    data = b"0123456789"
    stream = encode_frame(0x2, data[:3], mask=False) + encode_frame(
        0x2, data[3:], mask=False
    )
    t = WsTransport(make_reader(stream), FakeWriter())
    assert await t.read_exactly(10) == data


async def test_read_exactly_splits_one_frame():
    """反向:一帧里可以有多个包,按需切分"""
    t = WsTransport(make_reader(encode_frame(0x2, b"abcdef", mask=False)), FakeWriter())
    assert await t.read_exactly(2) == b"ab"
    assert await t.read_exactly(4) == b"cdef"


async def test_ping_answered_transparently():
    """控制帧在传输层处理:ping 自动回 pong(载荷回显),不污染字节流"""
    stream = (
        encode_frame(0x9, b"pingdata", mask=False)  # ping
        + encode_frame(0x2, b"payload", mask=False)
    )
    writer = FakeWriter()
    t = WsTransport(make_reader(stream), writer)
    assert await t.read_exactly(7) == b"payload"
    # 写出的必须是 pong(0xA)且回显载荷
    assert writer.buffer[0] & 0x0F == 0xA
    key = writer.buffer[2:6]
    echoed = bytes(b ^ key[i % 4] for i, b in enumerate(writer.buffer[6:]))
    assert echoed == b"pingdata"


async def test_pong_and_unknown_control_ignored():
    stream = (
        encode_frame(0xA, b"pong", mask=False)
        + encode_frame(0xB, b"?", mask=False)  # 未知控制帧
        + encode_frame(0x2, b"data", mask=False)
    )
    t = WsTransport(make_reader(stream), FakeWriter())
    assert await t.read_exactly(4) == b"data"


async def test_close_frame_becomes_eof():
    """close 帧与 EOF 一致地表现为读取失败,交给客户端重连逻辑"""
    stream = encode_frame(0x8, b"", mask=False)
    t = WsTransport(make_reader(stream), FakeWriter())
    with pytest.raises(asyncio.IncompleteReadError):
        await t.read_exactly(1)


async def test_masked_server_frame_supported():
    """服务端帧按 RFC 不掩码,但健壮性上也支持掩码帧"""
    t = WsTransport(make_reader(encode_frame(0x2, b"xyz", mask=True)), FakeWriter())
    assert await t.read_exactly(3) == b"xyz"


async def test_write_wraps_in_binary_frame():
    writer = FakeWriter()
    t = WsTransport(make_reader(b""), writer)
    t.write(b"\x01\x02")
    assert writer.buffer[0] == 0x82  # FIN + binary
    assert writer.buffer[1] & 0x80  # 掩码


async def test_default_ssl_context_relaxes_ciphers_but_verifies():
    """斗鱼 wss 只提供 AES256-GCM-SHA384(SECLEVEL=2 会拒),
    但证书与主机名校验必须保持开启"""
    ctx = WsTransport.default_ssl_context()
    assert ctx.check_hostname is True
    import ssl

    assert ctx.verify_mode == ssl.CERT_REQUIRED
