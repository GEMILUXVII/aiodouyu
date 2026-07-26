"""STT 编解码单元测试"""

import pytest

from aiodouyu import stt


def test_escape_roundtrip_specials():
    for original in ["@", "/", "@A", "@S", "@@", "//", "a@b/c", "@S@S", "@A/@S"]:
        assert stt.unescape(stt.escape(original)) == original


def test_dumps_basic():
    assert stt.dumps({"type": "loginreq", "roomid": 9999}) == (
        "type@=loginreq/roomid@=9999/"
    )


def test_dumps_escapes_values():
    assert stt.dumps({"k": "a@b/c"}) == "k@=a@Ab@Sc/"


def test_loads_basic():
    msg = stt.loads("type@=rss/ss@=1/ivl@=0/")
    assert msg == {"type": "rss", "ss": "1", "ivl": "0"}


def test_loads_unescapes():
    assert stt.loads("txt@=a@Ab@Sc/") == {"txt": "a@b/c"}


def test_loads_skips_malformed_segments():
    msg = stt.loads("type@=chatmsg//novalue/txt@=hi/")
    assert msg == {"type": "chatmsg", "txt": "hi"}


def test_loads_duplicate_key_last_wins():
    assert stt.loads("k@=1/k@=2/") == {"k": "2"}


def test_loads_empty():
    assert stt.loads("") == {}


@pytest.mark.parametrize(
    "fields",
    [
        {"type": "chatmsg", "txt": "弹幕@测试/内容", "nn": "用户@A@S"},
        {"k": "@S@A@AS@SA"},
    ],
)
def test_dumps_loads_roundtrip(fields):
    assert stt.loads(stt.dumps(fields)) == {k: str(v) for k, v in fields.items()}
