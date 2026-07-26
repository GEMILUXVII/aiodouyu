"""web 模块单元测试(canned 响应,不联网)"""

import pytest

from aiodouyu import ApiError, RoomNotFound, web

pytestmark = pytest.mark.asyncio


BETARD_LIVE = {
    "room": {
        "room_id": 9999,
        "room_name": "陪伴每一天",
        "owner_name": "yyfyyf",
        "nickname": "yyfyyf",
        "show_status": 1,
        "videoLoop": 0,
        "second_lvl_name": "DOTA2",
        "room_pic": "https://rpic.douyucdn.cn/cover.avif",
        "avatar": {
            "big": "https://apic.douyucdn.cn/big.jpg",
            "middle": "https://apic.douyucdn.cn/mid.jpg",
        },
        "show_time": 1785045708,
    }
}

OPEN_LIVE = {
    "error": 0,
    "data": {
        "room_id": "9999",
        "room_name": "陪伴每一天",
        "owner_name": "yyfyyf",
        "cate_name": "DOTA2",
        "room_status": "1",
        "start_time": "2026-07-26 14:01:48",
        "avatar": "https://apic.douyucdn.cn/big.jpg",
        "room_thumb": "https://rpic.douyucdn.cn/thumb.png",
        "online": 5145276,
    },
}


def fake_fetch(mapping):
    """构造按 URL 前缀返回 canned payload 的 _fetch 替身

    mapping: {url 前缀 -> payload 或 Exception}
    """

    calls = []

    async def _fake(url, headers, timeout):
        calls.append(url)
        for prefix, payload in mapping.items():
            if url.startswith(prefix):
                if isinstance(payload, Exception):
                    raise payload
                return payload
        raise AssertionError(f"意外的 URL: {url}")

    return _fake, calls


BETARD_PREFIX = "https://www.douyu.com/betard/"
OPEN_PREFIX = "https://open.douyucdn.cn/"


async def test_betard_live(monkeypatch):
    fake, _ = fake_fetch({BETARD_PREFIX: BETARD_LIVE})
    monkeypatch.setattr(web, "_fetch", fake)
    info = await web.fetch_room(9999, source="betard")

    assert info.room_id == 9999
    assert info.title == "陪伴每一天"
    assert info.owner == "yyfyyf"
    assert info.category == "DOTA2"
    assert info.is_live is True
    assert info.is_loop is False
    assert info.avatar_url == "https://apic.douyucdn.cn/big.jpg"
    assert info.cover_url == "https://rpic.douyucdn.cn/cover.avif"
    assert info.started_at == 1785045708
    assert info.online is None
    assert info.source == "betard"
    assert info.raw["show_status"] == 1


async def test_betard_video_loop_not_live(monkeypatch):
    payload = {"room": {**BETARD_LIVE["room"], "videoLoop": 1}}
    fake, _ = fake_fetch({BETARD_PREFIX: payload})
    monkeypatch.setattr(web, "_fetch", fake)
    info = await web.fetch_room(9999, source="betard")

    assert info.is_live is False
    assert info.is_loop is True
    assert info.started_at == 1785045708


async def test_betard_offline(monkeypatch):
    payload = {"room": {**BETARD_LIVE["room"], "show_status": 2}}
    fake, _ = fake_fetch({BETARD_PREFIX: payload})
    monkeypatch.setattr(web, "_fetch", fake)
    info = await web.fetch_room(9999, source="betard")

    assert info.is_live is False
    assert info.is_loop is False
    assert info.started_at is None


async def test_betard_category_falls_back_to_game_tag(monkeypatch):
    room = {**BETARD_LIVE["room"]}
    del room["second_lvl_name"]
    payload = {"room": room, "game": {"tag_name": "英雄联盟"}}
    fake, _ = fake_fetch({BETARD_PREFIX: payload})
    monkeypatch.setattr(web, "_fetch", fake)
    info = await web.fetch_room(9999, source="betard")
    assert info.category == "英雄联盟"


async def test_betard_room_not_found(monkeypatch):
    fake, _ = fake_fetch({BETARD_PREFIX: {"error": 0}})  # 无 room 对象
    monkeypatch.setattr(web, "_fetch", fake)
    with pytest.raises(RoomNotFound):
        await web.fetch_room(123456789, source="betard")


async def test_open_live(monkeypatch):
    fake, _ = fake_fetch({OPEN_PREFIX: OPEN_LIVE})
    monkeypatch.setattr(web, "_fetch", fake)
    info = await web.fetch_room(9999, source="open")

    assert info.room_id == 9999
    assert info.title == "陪伴每一天"
    assert info.category == "DOTA2"
    assert info.is_live is True
    assert info.is_loop is None  # open 源无法判定轮播
    assert info.online == 5145276
    assert info.source == "open"
    # "2026-07-26 14:01:48" 北京时间 == epoch 1784959308? 用公式校验:
    # datetime(2026,7,26,14,1,48, tz=+8).timestamp()
    from datetime import datetime, timedelta, timezone

    expected = int(
        datetime(
            2026, 7, 26, 14, 1, 48, tzinfo=timezone(timedelta(hours=8))
        ).timestamp()
    )
    assert info.started_at == expected


async def test_open_room_not_found(monkeypatch):
    fake, _ = fake_fetch({OPEN_PREFIX: {"error": 101, "data": "Not Found"}})
    monkeypatch.setattr(web, "_fetch", fake)
    with pytest.raises(RoomNotFound):
        await web.fetch_room(123456789, source="open")


async def test_auto_falls_back_to_open_on_api_error(monkeypatch):
    fake, calls = fake_fetch(
        {BETARD_PREFIX: ApiError("模拟风控"), OPEN_PREFIX: OPEN_LIVE}
    )
    monkeypatch.setattr(web, "_fetch", fake)
    info = await web.fetch_room(9999, source="auto")

    assert info.source == "open"
    assert len(calls) == 2


async def test_auto_does_not_fall_back_on_room_not_found(monkeypatch):
    fake, calls = fake_fetch({BETARD_PREFIX: {"no": "room"}, OPEN_PREFIX: OPEN_LIVE})
    monkeypatch.setattr(web, "_fetch", fake)
    with pytest.raises(RoomNotFound):
        await web.fetch_room(9999, source="auto")
    assert len(calls) == 1  # 未回退


async def test_invalid_room_id():
    with pytest.raises(ValueError):
        await web.fetch_room(0)


async def test_invalid_source():
    with pytest.raises(ValueError):
        await web.fetch_room(9999, source="bogus")  # type: ignore[arg-type]


async def test_http_404_maps_to_room_not_found(monkeypatch):
    import io
    import urllib.error

    def raise_404(url, headers, timeout):
        raise urllib.error.HTTPError(url, 404, "Not Found", None, io.BytesIO())

    monkeypatch.setattr(web, "_http_get_json", raise_404)
    with pytest.raises(RoomNotFound):
        await web.fetch_room(999999999, source="open")
    # auto 模式下 betard 404 同样是明确结论,不回退到 open
    with pytest.raises(RoomNotFound):
        await web.fetch_room(999999999, source="auto")


async def test_http_500_maps_to_api_error(monkeypatch):
    import io
    import urllib.error

    def raise_500(url, headers, timeout):
        raise urllib.error.HTTPError(url, 500, "Server Error", None, io.BytesIO())

    monkeypatch.setattr(web, "_http_get_json", raise_500)
    with pytest.raises(ApiError):
        await web.fetch_room(9999, source="open")


async def test_parse_cst_time_invalid():
    assert web._parse_cst_time("not a date") is None
    assert web._parse_cst_time(None) is None
    assert web._parse_cst_time(12345) is None
