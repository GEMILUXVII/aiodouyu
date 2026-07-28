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


@pytest.mark.parametrize("source", ["betard", "auto"])
async def test_betard_status_request_bypasses_cdn_cache(monkeypatch, source):
    calls = []

    async def fake(url, headers, timeout):
        calls.append((url, dict(headers)))
        return BETARD_LIVE

    tokens = iter(["first", "second"])
    monkeypatch.setattr(web.secrets, "token_hex", lambda size: next(tokens))
    monkeypatch.setattr(web, "_fetch", fake)

    await web.fetch_room(9999, source=source)
    await web.fetch_room(9999, source=source)

    assert [url for url, _ in calls] == [
        "https://www.douyu.com/betard/9999?_=first",
        "https://www.douyu.com/betard/9999?_=second",
    ]
    assert all(headers["Cache-Control"] == "no-cache" for _, headers in calls)
    assert all(headers["Pragma"] == "no-cache" for _, headers in calls)


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


async def test_open_transient_error_code_is_api_error(monkeypatch):
    """非 101 的错误码可能是瞬时故障,不得断言为房间不存在"""
    fake, _ = fake_fetch({OPEN_PREFIX: {"error": 999, "data": "rate limited"}})
    monkeypatch.setattr(web, "_fetch", fake)
    with pytest.raises(ApiError) as ei:
        await web.fetch_room(9999, source="open")
    assert not isinstance(ei.value, RoomNotFound)


async def test_open_malformed_data_is_api_error(monkeypatch):
    """error=0 但 data 畸形是响应结构异常,不是房间不存在"""
    fake, _ = fake_fetch({OPEN_PREFIX: {"error": 0, "data": "garbage"}})
    monkeypatch.setattr(web, "_fetch", fake)
    with pytest.raises(ApiError) as ei:
        await web.fetch_room(9999, source="open")
    assert not isinstance(ei.value, RoomNotFound)


async def test_unknown_charset_maps_to_api_error(monkeypatch):
    """服务端返回未知 charset 时 LookupError 不得穿透异常契约"""

    def raise_lookup(url, headers, timeout):
        raise LookupError("unknown encoding: bogus-enc")

    monkeypatch.setattr(web, "_http_get_json", raise_lookup)
    with pytest.raises(ApiError):
        await web.fetch_room(9999, source="open")


async def test_to_int_handles_infinity():
    assert web._to_int(float("inf")) is None
    assert web._to_int(float("-inf")) is None
    assert web._to_int(float("nan")) is None


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


async def test_room_id_string_raises_clear_type_error():
    with pytest.raises(TypeError, match="room_id 必须为 int"):
        await web.fetch_room("9999")  # type: ignore[arg-type]


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


async def test_http_client_exceptions_map_to_api_error(monkeypatch):
    import http.client

    def raise_incomplete_read(url, headers, timeout):
        raise http.client.IncompleteRead(b"partial")

    monkeypatch.setattr(web, "_http_get_json", raise_incomplete_read)
    with pytest.raises(ApiError):
        await web.fetch_room(9999, source="open")


async def test_http_500_maps_to_api_error(monkeypatch):
    import io
    import urllib.error

    def raise_500(url, headers, timeout):
        raise urllib.error.HTTPError(url, 500, "Server Error", None, io.BytesIO())

    monkeypatch.setattr(web, "_http_get_json", raise_500)
    with pytest.raises(ApiError):
        await web.fetch_room(9999, source="open")


async def test_fetch_rooms_batch(monkeypatch):
    """批量拉取:限并发、逐房异常不拖垮整批、去重"""
    calls = []

    async def fake_fetch(url, headers, timeout):
        calls.append(url)
        if "404404" in url:
            raise web.RoomNotFound("gone") if hasattr(web, "RoomNotFound") else Exception
        return OPEN_LIVE

    monkeypatch.setattr(web, "_fetch", fake_fetch)
    result = await web.fetch_rooms(
        [9999, 404404, 9999], source="open", concurrency=2
    )
    assert set(result) == {9999, 404404}  # 去重
    assert result[9999].owner == "yyfyyf"
    assert isinstance(result[404404], Exception)


async def test_resolve_room_id_vanity(monkeypatch):
    """靓号解析:从移动端页面 HTML 抽取真实 rid

    实测:betard 对靓号返回错误页、open API 对靓号原样回显,
    两者都解析不了——只有 m.douyu.com 的页面带真实 rid。
    """
    calls = []

    def fake_text(url, headers, timeout):
        calls.append(url)
        return '{"room":{"room_id":"6979222","nickname":"x"}}'

    monkeypatch.setattr(web, "_http_get_text", fake_text)
    assert await web.resolve_room_id(6657) == 6979222
    assert "m.douyu.com/6657" in calls[0]


async def test_resolve_room_id_falls_back_to_input(monkeypatch):
    """页面里抽不到 rid 时原样返回:输入本就是真实 rid 是常态路径"""

    def fake_text(url, headers, timeout):
        return "<html>no rid here</html>"

    monkeypatch.setattr(web, "_http_get_text", fake_text)
    assert await web.resolve_room_id(9999) == 9999


async def test_resolve_room_id_validates_type():
    with pytest.raises(TypeError):
        await web.resolve_room_id("6657")  # type: ignore[arg-type]
    with pytest.raises(ValueError):
        await web.resolve_room_id(0)


async def test_parse_cst_time_invalid():
    assert web._parse_cst_time("not a date") is None
    assert web._parse_cst_time(None) is None
    assert web._parse_cst_time(12345) is None
