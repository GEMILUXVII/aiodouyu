"""typed models 单元测试(样本取自真实抓包语料)"""

import pytest

from aiodouyu import models

pytestmark = pytest.mark.asyncio

# 真实语料样本(截取自 --record 抓包)
CHATMSG = {
    "type": "chatmsg",
    "rid": "9999",
    "uid": "53697999",
    "nn": "网友甲",
    "txt": "弹幕/内容@测试",
    "cid": "62ec0d915",
    "level": "34",
    "cst": "1785076556771",
    "bnn": "小僵尸",
    "bl": "20",
    "brid": "507882",
}
DGB = {
    "type": "dgb",
    "rid": "9999",
    "gfid": "824",
    "gfn": "粉丝荧光棒",
    "gfcnt": "270",
    "hits": "270",
    "uid": "16219566",
    "nn": "送礼人",
    "level": "41",
    "bnn": "小僵尸",
    "bl": "26",
    "brid": "9999",
}
UENTER_MIN = {
    "type": "uenter",
    "rid": "288016",
    "uid": "1",
    "nn": "路人",
    "level": "12",
    "bnn": "",
}
RSS_LIVE = {"type": "rss", "ss": "1", "ivl": "0", "rid": "9999"}


async def test_chatmsg_fields():
    m = models.parse(CHATMSG)
    assert isinstance(m, models.ChatMsg)
    assert m.nickname == "网友甲" and m.text == "弹幕/内容@测试"
    assert m.uid == 53697999 and m.level == 34
    # 粉丝牌三元组:跨房牌(brid != rid)必须能被识别
    assert m.badge_name == "小僵尸" and m.badge_level == 20
    assert m.badge_room_id == 507882 and m.room_id == 9999
    assert m.badge_room_id != m.room_id
    assert m.sent_at == pytest.approx(1785076556.771)
    assert m.raw is CHATMSG


async def test_gift_fields():
    g = models.parse(DGB)
    assert isinstance(g, models.Gift)
    assert g.gift_id == "824" and g.gift_name == "粉丝荧光棒"
    assert g.count == 270 and g.hits == 270  # 数量字段是 gfcnt(实测)


async def test_uenter_optional_fields():
    u = models.parse(UENTER_MIN)
    assert isinstance(u, models.UserEnter)
    assert u.nickname == "路人"
    # 实测可选字段缺失 -> None;bnn 空串归一为 None(无牌)
    assert u.badge_name is None and u.badge_level is None
    assert u.badge_room_id is None


async def test_room_status_semantics():
    live = models.parse(RSS_LIVE)
    assert isinstance(live, models.RoomStatus)
    assert live.is_live is True and live.is_loop is False
    assert live.room_id == 9999

    loop = models.RoomStatus.from_dict({"type": "rss", "ss": "1", "ivl": "1"})
    assert loop.is_live is False and loop.is_loop is True

    # ivl 不是所有 rss 都携带;缺失时不能把明确的 ss=1 压成下播
    no_ivl = models.RoomStatus.from_dict({"type": "rss", "ss": "1"})
    assert no_ivl.is_live is True and no_ivl.is_loop is False

    off = models.RoomStatus.from_dict({"type": "rss", "ss": "0", "ivl": "0"})
    assert off.is_live is False and off.is_loop is False


async def test_parse_never_raises_on_malformed():
    # 宽松解析:畸形字段落为 None,永不抛异常
    m = models.parse({"type": "chatmsg", "uid": "不是数字", "cst": "x", "bl": ""})
    assert m.uid is None and m.sent_at is None and m.badge_level is None

    g = models.parse({"type": "dgb"})
    assert g.gift_id is None and g.count is None


async def test_parse_unknown_type_returns_none():
    assert models.parse({"type": "pingreq", "tick": "1"}) is None
    assert models.parse({}) is None


async def test_models_are_frozen():
    m = models.parse(RSS_LIVE)
    with pytest.raises(AttributeError):
        m.ss = "0"  # type: ignore[misc]
