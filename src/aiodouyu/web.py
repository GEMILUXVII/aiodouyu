"""斗鱼房间信息 HTTP 接口(零依赖)

弹幕连接只在状态**变化**时收到 rss 推送,断连窗口内的变化会丢失;
本模块提供 HTTP 拉取当前房间状态的能力,用于:

- 重连后校准直播状态(配合 ``EVENT_CONNECTED``)
- 获取房间元信息(标题、主播名、分类、封面)用于通知富化

两个数据源:

- ``betard``(默认): ``https://www.douyu.com/betard/{room_id}``,网页端
  内部接口,字段最全,且 ``videoLoop`` 可区分视频轮播与真实开播
- ``open``: ``https://open.douyucdn.cn/api/RoomApi/room/{room_id}``,
  公开 API,更稳定,但无法区分视频轮播

实现基于标准库 ``urllib``,通过 ``asyncio.to_thread`` 移出事件循环,
保持整个库零运行时依赖。适合低频调用(状态校准、添加房间时取元信息);
高频轮询场景请自行限速。
"""

from __future__ import annotations

import asyncio
import http.client
import json
import re
import secrets
import urllib.error
import urllib.request
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Literal

from .exceptions import ApiError, RoomNotFound

__all__ = ["RoomInfo", "fetch_room", "fetch_rooms", "resolve_room_id"]

_TIMEZONE_CST = timezone(timedelta(hours=8))  # 斗鱼时间均为北京时间

_BETARD_URL = "https://www.douyu.com/betard/{room_id}"
_OPEN_URL = "https://open.douyucdn.cn/api/RoomApi/room/{room_id}"
# 靓号只在移动端页面能解析为真实 rid(betard 返回错误页、open 原样回显)
_MOBILE_URL = "https://m.douyu.com/{room_id}"
_RID_PATTERN = re.compile(r'"(?:room_id|rid)"\s*:\s*"?(\d+)')

# betard 是网页端接口,需要浏览器化的请求头
_BETARD_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Referer": "https://www.douyu.com/",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
}

Source = Literal["auto", "betard", "open"]


def _betard_url(room_id: int) -> str:
    """Build a cache-busted URL for Douyu's CDN-cached status endpoint."""
    return f"{_BETARD_URL.format(room_id=room_id)}?_={secrets.token_hex(8)}"


@dataclass
class RoomInfo:
    """房间信息快照(两个数据源归一化后的公共视图)

    Attributes:
        room_id: 真实房间号
        title: 直播间标题
        owner: 主播名
        category: 分类名(如 "DOTA2");数据源缺失时为 None
        is_live: 是否开播。注意 open 源无法识别视频轮播(is_loop 为
            None),轮播房会被报告为开播;需要精确判定时请用
            ``source="betard"``,或在 ``is_loop is None`` 时自行甄别
        is_loop: 是否处于视频轮播。True/False 为 betard 的确定结论;
            None 表示数据源(open)无法判定
        avatar_url: 主播头像 URL
        cover_url: 直播间封面 URL
        started_at: 本场开播时间(epoch 秒)。未开播或数据源缺失时为
            None;注意视频轮播时(is_live=False, is_loop=True)携带的是
            轮播场次的 show_time——判定在播请用 is_live,勿用
            ``started_at is not None``
        online: 热度值(仅 open 源提供)
        source: 本快照来自哪个数据源("betard" / "open")
        raw: 数据源原始房间对象,未归一化字段可从这里取
    """

    room_id: int
    title: str
    owner: str
    category: str | None
    is_live: bool
    is_loop: bool | None
    avatar_url: str | None
    cover_url: str | None
    started_at: int | None
    online: int | None
    source: str
    raw: dict[str, Any] = field(repr=False)


def _http_get_json(url: str, headers: dict[str, str], timeout: float) -> Any:
    """同步 HTTP GET 并解析 JSON(在 to_thread 中运行)"""
    request = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(request, timeout=timeout) as response:
        charset = response.headers.get_content_charset() or "utf-8"
        body = response.read()
    return json.loads(body.decode(charset, errors="replace"))


def _http_get_text(url: str, headers: dict[str, str], timeout: float) -> str:
    """同步 HTTP GET 取文本(在 to_thread 中运行)"""
    request = urllib.request.Request(url, headers=headers)
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            charset = response.headers.get_content_charset() or "utf-8"
            return response.read().decode(charset, errors="replace")
    except urllib.error.HTTPError as exc:
        if exc.code == 404:
            raise RoomNotFound(f"房间不存在: {url} 返回 HTTP 404") from exc
        raise ApiError(f"请求 {url} 失败: HTTP {exc.code}") from exc
    except (
        urllib.error.URLError,
        http.client.HTTPException,
        TimeoutError,
        OSError,
        ValueError,
        LookupError,
    ) as exc:
        raise ApiError(f"请求 {url} 失败: {exc}") from exc


def _to_int(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError, OverflowError):
        # OverflowError: json 默认接受非标常量 Infinity,int(inf) 会抛
        return None


def _parse_betard(room_id: int, payload: Any) -> RoomInfo:
    # 不存在的房间号 betard 也可能返回 200,但 body 中没有 room 对象
    room = payload.get("room") if isinstance(payload, dict) else None
    if not isinstance(room, dict):
        raise RoomNotFound(f"房间 {room_id} 不存在(betard 未返回 room 对象)")

    show_status = _to_int(room.get("show_status"))
    video_loop = _to_int(room.get("videoLoop"))
    is_loop = show_status == 1 and video_loop == 1
    is_live = show_status == 1 and not is_loop
    started_at = _to_int(room.get("show_time")) if is_live or is_loop else None
    # 分类名在 room.second_lvl_name(room.cate_name 并不存在,勿"纠正"回去),
    # 兜底取顶层 game.tag_name
    game = payload.get("game")
    category = room.get("second_lvl_name") or (
        game.get("tag_name") if isinstance(game, dict) else None
    )
    return RoomInfo(
        room_id=_to_int(room.get("room_id")) or room_id,
        title=str(room.get("room_name") or ""),
        owner=str(room.get("owner_name") or room.get("nickname") or ""),
        category=str(category) if category else None,
        is_live=is_live,
        is_loop=is_loop,
        avatar_url=_extract_betard_avatar(room),
        cover_url=str(room.get("room_pic")) if room.get("room_pic") else None,
        started_at=started_at,
        online=None,
        source="betard",
        raw=room,
    )


def _extract_betard_avatar(room: dict[str, Any]) -> str | None:
    # betard 的 avatar 是 {big/middle/small} 对象,偶见直接给字符串
    avatar = room.get("avatar")
    if isinstance(avatar, dict):
        value = avatar.get("big") or avatar.get("middle") or avatar.get("small")
        return str(value) if value else None
    return str(avatar) if avatar else None


def _parse_open(room_id: int, payload: Any) -> RoomInfo:
    if not isinstance(payload, dict):
        raise ApiError(f"open API 返回非对象响应: {type(payload).__name__}")
    error = _to_int(payload.get("error"))
    data = payload.get("data")
    if error != 0:
        # 只有 101(房间未找到)是"房间不存在"的确定性结论;
        # 其余非零码可能是限流等瞬时故障,断言房间不存在会误导调用方
        if error == 101:
            raise RoomNotFound(f"房间 {room_id} 不存在(open API error=101)")
        raise ApiError(f"open API 返回错误码 {payload.get('error')}")
    if not isinstance(data, dict):
        # error=0 但 data 缺失/非对象是响应结构异常,不是房间不存在
        raise ApiError("open API 响应缺少 data 对象")

    room_status = _to_int(data.get("room_status"))
    is_live = room_status == 1
    return RoomInfo(
        room_id=_to_int(data.get("room_id")) or room_id,
        title=str(data.get("room_name") or ""),
        owner=str(data.get("owner_name") or ""),
        category=str(data.get("cate_name")) if data.get("cate_name") else None,
        is_live=is_live,
        is_loop=None,  # open API 无法检测视频轮播,轮播房会报告为开播
        avatar_url=str(data.get("avatar")) if data.get("avatar") else None,
        cover_url=str(data.get("room_thumb")) if data.get("room_thumb") else None,
        started_at=_parse_cst_time(data.get("start_time")) if is_live else None,
        online=_to_int(data.get("online")),
        source="open",
        raw=data,
    )


def _parse_cst_time(value: Any) -> int | None:
    """把 open API 的 "YYYY-mm-dd HH:MM:SS"(北京时间)转为 epoch 秒"""
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        return None
    return int(parsed.replace(tzinfo=_TIMEZONE_CST).timestamp())


async def _fetch(url: str, headers: dict[str, str], timeout: float) -> Any:
    try:
        return await asyncio.to_thread(_http_get_json, url, headers, timeout)
    except urllib.error.HTTPError as exc:
        # 两个接口对不存在的房间都返回 404(实测),映射为明确结论
        if exc.code == 404:
            raise RoomNotFound(f"房间不存在: {url} 返回 HTTP 404") from exc
        raise ApiError(f"请求 {url} 失败: HTTP {exc.code}") from exc
    except (
        urllib.error.URLError,
        http.client.HTTPException,
        TimeoutError,
        OSError,
        ValueError,
        LookupError,
    ) as exc:
        # ValueError 覆盖 json.JSONDecodeError 与畸形 URL;
        # HTTPException 覆盖 IncompleteRead/LineTooLong 等非 OSError 的
        # 传输层异常(RemoteDisconnected 继承 OSError,其余不继承);
        # LookupError 覆盖服务端返回未知 charset 时的 decode 失败
        raise ApiError(f"请求 {url} 失败: {exc}") from exc


async def fetch_room(
    room_id: int,
    *,
    source: Source = "auto",
    timeout: float = 10.0,
) -> RoomInfo:
    """获取房间当前信息

    Args:
        room_id: 斗鱼房间号(真实 rid,非靓号别名)
        source: 数据源。"betard"(默认优先,字段全且能识别轮播)、
            "open"(公开 API,更稳但不识别轮播)、"auto"(先 betard,
            传输层失败时回退 open;房间不存在不回退)
        timeout: 传给 urllib 的超时(秒)。注意这是**逐 socket 操作**
            超时(connect/recv 各自计时),不含 DNS 解析,慢速滴流响应
            的总时长可超过该值;经 to_thread 执行的请求被取消时底层
            线程会继续运行到 socket 层面结束

    Returns:
        归一化的 RoomInfo 快照

    Raises:
        RoomNotFound: 房间不存在
        ApiError: 网络失败或响应无法解析
        TypeError: room_id 不是 int
        ValueError: room_id 非正整数或 source 非法
    """
    if isinstance(room_id, bool) or not isinstance(room_id, int):
        raise TypeError(
            f"room_id 必须为 int,收到 {type(room_id).__name__}"
            f"(字符串请先 int() 转换)"
        )
    if room_id <= 0:
        raise ValueError("room_id 必须为正整数")

    if source == "betard":
        payload = await _fetch(_betard_url(room_id), _BETARD_HEADERS, timeout)
        return _parse_betard(room_id, payload)
    if source == "open":
        payload = await _fetch(_OPEN_URL.format(room_id=room_id), {}, timeout)
        return _parse_open(room_id, payload)
    if source != "auto":
        raise ValueError(f"未知数据源: {source!r}")

    # auto: betard 传输层失败(接口变更/风控)时回退 open;
    # RoomNotFound 是明确结论,不回退(注意它是 ApiError 的子类,须先拦截)
    try:
        payload = await _fetch(_betard_url(room_id), _BETARD_HEADERS, timeout)
        return _parse_betard(room_id, payload)
    except RoomNotFound:
        raise
    except ApiError:
        payload = await _fetch(_OPEN_URL.format(room_id=room_id), {}, timeout)
        return _parse_open(room_id, payload)


async def fetch_rooms(
    room_ids: Iterable[int],
    *,
    source: Source = "auto",
    timeout: float = 10.0,
    concurrency: int = 5,
) -> dict[int, RoomInfo | Exception]:
    """Fetch several rooms with bounded concurrency. / 批量拉取房间信息

    逐房间异常不拖垮整批:失败的房间在结果里以异常对象出现,由调用方
    决定降级方式。并发上限防止一次性打出 N 个请求触发风控;缓存/限速
    等策略留给应用层。

    Args:
        room_ids: 房间号可迭代对象(自动去重,保序)
        source/timeout: 透传 fetch_room
        concurrency: 并发上限

    Returns:
        ``{room_id: RoomInfo | Exception}``
    """
    if concurrency < 1:
        # Semaphore(0) 会永久挂起且无任何报错;负数的库内报错也难懂
        raise ValueError(f"concurrency 必须 >= 1,收到 {concurrency}")
    semaphore = asyncio.Semaphore(concurrency)

    async def one(rid: int) -> tuple[int, RoomInfo | Exception]:
        async with semaphore:
            try:
                return rid, await fetch_room(rid, source=source, timeout=timeout)
            except Exception as exc:
                return rid, exc

    unique = list(dict.fromkeys(room_ids))
    return dict(await asyncio.gather(*(one(r) for r in unique)))


async def resolve_room_id(room_id: int, *, timeout: float = 10.0) -> int:
    """Resolve a vanity room id to the real rid. / 靓号解析为真实房间号

    弹幕连接必须用真实 rid:拿主页 URL 里的靓号(如
    ``douyu.com/6657``,真实 rid 是 6979222)去连,要么无消息要么连错
    房,且无报错、极难自诊。真实 rid 输入原样返回,可无脑套用::

        client = DanmakuClient(await resolve_room_id(6657))

    实现:靓号只在移动端房间页 ``m.douyu.com/{id}`` 的 HTML 里能拿到
    真实 rid——betard 对靓号返回错误页,open API 对靓号原样回显(实测),
    两者都无法解析。因此本函数解析移动端页面;失败时回退为返回原值
    (真实 rid 的输入本就无需解析)。

    Raises:
        ApiError: 页面获取失败且无法判定
        TypeError / ValueError: room_id 非正整数
    """
    if isinstance(room_id, bool) or not isinstance(room_id, int):
        raise TypeError(
            f"room_id 必须为 int,收到 {type(room_id).__name__}"
        )
    if room_id <= 0:
        raise ValueError("room_id 必须为正整数")

    html = await asyncio.to_thread(
        _http_get_text, _MOBILE_URL.format(room_id=room_id), _BETARD_HEADERS, timeout
    )
    match = _RID_PATTERN.search(html)
    if match:
        resolved = _to_int(match.group(1))
        if resolved and resolved > 0:
            return resolved
    # 解析不到:输入本就是真实 rid 的常态路径,原样返回
    return room_id
