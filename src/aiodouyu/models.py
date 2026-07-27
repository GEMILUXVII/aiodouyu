"""Typed models for common danmaku messages. / 常见弹幕消息的类型化模型

可选层:``DanmakuClient`` 默认行为不变(产出 ``dict[str, str]``),
需要类型化字段时用 :func:`parse` 或各模型的 ``from_dict``::

    from aiodouyu import models

    async for msg in client:
        parsed = models.parse(msg)
        if isinstance(parsed, models.ChatMsg):
            print(parsed.nickname, parsed.text)

设计约定(基于真实抓包语料,1600+ 条消息):

- **宽松解析**:字段缺失/畸形一律得 ``None``,永不抛异常——弹幕协议
  是斗鱼非官方接口,字段随时可能漂移,模型的职责是"尽力类型化",
  兜底永远是 ``raw``
- **粉丝牌三元组**:``badge_name/badge_level/badge_room_id`` 必须一起
  看——实测跨房粉丝牌是常态(``brid`` 常 ≠ 当前房间),只看牌名会把
  他房牌当本房牌;无牌时三者均为 ``None``
- 未识别的消息类型 :func:`parse` 返回 ``None``,消费方自行处理 dict
"""

from __future__ import annotations

from dataclasses import dataclass, field

__all__ = ["ChatMsg", "Gift", "RoomStatus", "UserEnter", "parse"]


def _s(msg: dict[str, str], key: str) -> str | None:
    """取字符串字段;缺失或空串得 None"""
    value = msg.get(key)
    return value if value else None


def _i(msg: dict[str, str], key: str) -> int | None:
    """取整数字段;缺失/畸形得 None"""
    value = msg.get(key)
    if value is None:
        return None
    try:
        return int(value)
    except (ValueError, TypeError):
        return None


@dataclass(frozen=True, slots=True)
class ChatMsg:
    """弹幕消息(type=chatmsg)"""

    nickname: str | None  # nn
    text: str | None  # txt
    uid: int | None
    level: int | None  # 用户等级
    badge_name: str | None  # bnn(空串归一为 None)
    badge_level: int | None  # bl
    badge_room_id: int | None  # brid(注意可能不是当前房间)
    room_id: int | None  # rid
    chat_id: str | None  # cid
    sent_at: float | None  # cst,毫秒时间戳转秒
    raw: dict[str, str] = field(repr=False)

    @classmethod
    def from_dict(cls, msg: dict[str, str]) -> ChatMsg:
        cst = _i(msg, "cst")
        return cls(
            nickname=_s(msg, "nn"),
            text=_s(msg, "txt"),
            uid=_i(msg, "uid"),
            level=_i(msg, "level"),
            badge_name=_s(msg, "bnn"),
            badge_level=_i(msg, "bl"),
            badge_room_id=_i(msg, "brid"),
            room_id=_i(msg, "rid"),
            chat_id=_s(msg, "cid"),
            sent_at=cst / 1000.0 if cst else None,
            raw=msg,
        )


@dataclass(frozen=True, slots=True)
class Gift:
    """礼物消息(type=dgb)

    数量字段是 ``gfcnt``(实测,不是 count);礼物名 ``gfn`` 通常直接
    在消息内。价格/图标等元数据不在消息里,需要的话按 gift_id 自行
    查表。
    """

    gift_id: str | None  # gfid
    gift_name: str | None  # gfn
    count: int | None  # gfcnt
    hits: int | None  # 连击数
    nickname: str | None  # nn
    uid: int | None
    level: int | None
    badge_name: str | None
    badge_level: int | None
    badge_room_id: int | None
    room_id: int | None
    raw: dict[str, str] = field(repr=False)

    @classmethod
    def from_dict(cls, msg: dict[str, str]) -> Gift:
        return cls(
            gift_id=_s(msg, "gfid"),
            gift_name=_s(msg, "gfn"),
            count=_i(msg, "gfcnt"),
            hits=_i(msg, "hits"),
            nickname=_s(msg, "nn"),
            uid=_i(msg, "uid"),
            level=_i(msg, "level"),
            badge_name=_s(msg, "bnn"),
            badge_level=_i(msg, "bl"),
            badge_room_id=_i(msg, "brid"),
            room_id=_i(msg, "rid"),
            raw=msg,
        )


@dataclass(frozen=True, slots=True)
class UserEnter:
    """进房消息(type=uenter)

    可选性为实测结论:badge/粉丝等级字段仅部分消息携带。
    """

    nickname: str | None
    uid: int | None
    level: int | None
    badge_name: str | None
    badge_level: int | None
    badge_room_id: int | None
    room_id: int | None
    raw: dict[str, str] = field(repr=False)

    @classmethod
    def from_dict(cls, msg: dict[str, str]) -> UserEnter:
        return cls(
            nickname=_s(msg, "nn"),
            uid=_i(msg, "uid"),
            level=_i(msg, "level"),
            badge_name=_s(msg, "bnn"),
            badge_level=_i(msg, "bl"),
            badge_room_id=_i(msg, "brid"),
            room_id=_i(msg, "rid"),
            raw=msg,
        )


@dataclass(frozen=True, slots=True)
class RoomStatus:
    """直播状态消息(type=rss)

    ``ss=='1'`` 表示开播。``ivl`` 并非每种 rss 都保证携带,仅在
    ``ivl=='1'`` 时明确表示视频轮播;缺失不能反向解释为下播。
    斗鱼只在状态**变化**时推送本消息。
    """

    ss: str | None
    ivl: str | None
    room_id: int | None
    raw: dict[str, str] = field(repr=False)

    @property
    def is_live(self) -> bool:
        """是否真实开播(排除视频轮播)"""
        return self.ss == "1" and self.ivl != "1"

    @property
    def is_loop(self) -> bool:
        """是否处于视频轮播"""
        return self.ss == "1" and self.ivl == "1"

    @classmethod
    def from_dict(cls, msg: dict[str, str]) -> RoomStatus:
        return cls(
            ss=_s(msg, "ss"),
            ivl=msg.get("ivl"),  # '0' 是有意义的值,不做空串归一
            room_id=_i(msg, "rid") or _i(msg, "roomid"),
            raw=msg,
        )


_PARSERS = {
    "chatmsg": ChatMsg.from_dict,
    "dgb": Gift.from_dict,
    "uenter": UserEnter.from_dict,
    "rss": RoomStatus.from_dict,
}


def parse(msg: dict[str, str]) -> ChatMsg | Gift | UserEnter | RoomStatus | None:
    """把消息字典解析为类型化模型;未识别的类型返回 None

    纯函数、永不抛异常(畸形字段落为 None),可安全地对全量流调用。
    """
    parser = _PARSERS.get(msg.get("type", ""))
    return parser(msg) if parser else None
