"""二进制成帧单元测试"""

import struct

import pytest

from aiodouyu import packet


def test_pack_layout():
    data = packet.pack("type@=mrkl/")
    payload = b"type@=mrkl/"
    expected_length = 8 + len(payload) + 1

    length1, length2, msg_type, enc, reserved = struct.unpack("<IIHBB", data[:12])
    assert length1 == length2 == expected_length
    assert msg_type == packet.MSG_TYPE_CLIENT == 689
    assert enc == 0 and reserved == 0
    assert data[12:] == payload + b"\x00"
    # 首个长度字段之后恰好 length 字节
    assert len(data) - 4 == length1


def test_extract_payload_roundtrip():
    data = packet.pack("type@=loginreq/roomid@=1/")
    body = data[4:]
    assert packet.extract_payload(body) == "type@=loginreq/roomid@=1/"


def test_extract_payload_utf8():
    data = packet.pack("txt@=中文弹幕/")
    assert packet.extract_payload(data[4:]) == "txt@=中文弹幕/"


def test_extract_payload_invalid_bytes_replaced():
    body = b"\x00" * 8 + b"ok\xff\xfe" + b"\x00"
    text = packet.extract_payload(body)
    assert text.startswith("ok")


def test_extract_payload_too_short():
    with pytest.raises(packet.PacketError):
        packet.extract_payload(b"\x00" * 5)


@pytest.mark.parametrize("length", [0, 8, packet.MAX_PACKET_SIZE + 1])
def test_validate_length_rejects(length):
    with pytest.raises(packet.PacketError):
        packet.validate_length(length)


def test_validate_length_accepts():
    packet.validate_length(9)
    packet.validate_length(4096)
