"""录制/回放单元测试(不联网)"""

import asyncio
import io
import json
import time

import pytest

from aiodouyu import EVENT_CONNECTED, replay
from aiodouyu.replay import RECORD_FORMAT, write_header, write_message

pytestmark = pytest.mark.asyncio


def make_dump(messages, header_room=9999, ts_start=1000.0, ts_step=0.5):
    """构造标准录制文件内容(含 header)"""
    fp = io.StringIO()
    write_header(fp, header_room)
    ts = ts_start
    for msg in messages:
        write_message(fp, msg, ts=ts)
        ts += ts_step
    fp.seek(0)
    return fp


async def collect(source, **kwargs):
    return [msg async for msg in replay(source, **kwargs)]


async def test_roundtrip_immediate(tmp_path):
    messages = [
        {"type": EVENT_CONNECTED, "roomid": "9999"},
        {"type": "chatmsg", "nn": "用户", "txt": "弹幕/内容@测试"},
        {"type": "rss", "ss": "1", "ivl": "0"},
    ]
    path = tmp_path / "dump.jsonl"
    with open(path, "w", encoding="utf-8") as fp:  # noqa: ASYNC230
        write_header(fp, 9999)
        for m in messages:
            write_message(fp, m)

    out = await collect(str(path))
    assert out == messages  # 与 DanmakuClient 同构:dict[str, str] 原样


async def test_header_line_content():
    fp = io.StringIO()
    write_header(fp, 1234)
    header = json.loads(fp.getvalue().splitlines()[0])
    assert header["format"] == RECORD_FORMAT
    assert header["version"] == 1
    assert header["room_id"] == 1234
    assert abs(header["recorded_at"] - time.time()) < 60


async def test_headerless_bare_jsonl_tolerated():
    fp = io.StringIO(
        '{"ts": 1.0, "msg": {"type": "rss", "ss": "1", "ivl": "0"}}\n'
        '{"ts": 2.0, "msg": {"type": "chatmsg", "txt": "hi"}}\n'
    )
    out = await collect(fp)
    assert [m["type"] for m in out] == ["rss", "chatmsg"]


async def test_values_coerced_to_str():
    # 手工构造/外部工具产出的 dump 可能带 int,须强转守住同构承诺
    fp = io.StringIO('{"ts": 1.0, "msg": {"type": "rss", "ss": 1, "ivl": 0}}\n')
    out = await collect(fp)
    assert out == [{"type": "rss", "ss": "1", "ivl": "0"}]


async def test_types_filter_with_pseudo_event_exemption():
    fp = make_dump(
        [
            {"type": EVENT_CONNECTED, "roomid": "1"},
            {"type": "chatmsg", "txt": "skip"},
            {"type": "rss", "ss": "1", "ivl": "0"},
        ]
    )
    out = await collect(fp, types={"rss"})
    # 伪事件豁免过滤,与 DanmakuClient 契约一致(插件生产依赖此语义)
    assert [m["type"] for m in out] == [EVENT_CONNECTED, "rss"]


async def test_malformed_lines_skipped():
    fp = io.StringIO(
        "not json at all\n"
        '{"ts": 1.0}\n'  # 无 msg
        '{"ts": 2.0, "msg": "not a dict"}\n'
        '{"ts": 3.0, "msg": {"type": "rss", "ss": "1", "ivl": "0"}}\n'
    )
    out = await collect(fp)
    assert len(out) == 1 and out[0]["type"] == "rss"


async def test_speed_pacing_and_max_gap():
    # 三条消息,间隔 10s + 0.05s;speed=1 且 max_gap=0.05 -> 大空窗被钳制
    fp = make_dump(
        [
            {"type": "a"},
            {"type": "b"},
            {"type": "c"},
        ],
        ts_start=100.0,
        ts_step=0.0,
    )
    # 重写 ts:100, 110(10s 空窗), 110.05
    lines = fp.getvalue().splitlines()
    msgs = [json.loads(ln) for ln in lines[1:]]
    msgs[1]["ts"] = 110.0
    msgs[2]["ts"] = 110.05
    fp2 = io.StringIO(lines[0] + "\n" + "\n".join(json.dumps(m) for m in msgs) + "\n")

    start = asyncio.get_running_loop().time()
    out = await collect(fp2, speed=1.0, max_gap=0.05)
    elapsed = asyncio.get_running_loop().time() - start
    assert len(out) == 3
    assert elapsed < 2.0  # 未钳制的话要 10s+
    assert elapsed >= 0.08  # 两段间隔(0.05 钳制 + 0.05)确实按节奏睡了


async def test_negative_gap_clamped():
    fp = io.StringIO(
        '{"ts": 100.0, "msg": {"type": "a"}}\n'
        '{"ts": 50.0, "msg": {"type": "b"}}\n'  # 时钟回拨
    )
    start = asyncio.get_running_loop().time()
    out = await collect(fp, speed=1.0)
    assert len(out) == 2
    assert asyncio.get_running_loop().time() - start < 1.0


async def test_invalid_speed():
    with pytest.raises(ValueError):
        await collect(io.StringIO(""), speed=0)
    with pytest.raises(ValueError):
        await collect(io.StringIO(""), speed=-1.5)


async def test_immediate_default_ignores_gaps():
    fp = io.StringIO(
        '{"ts": 0.0, "msg": {"type": "a"}}\n'
        '{"ts": 99999.0, "msg": {"type": "b"}}\n'
    )
    start = asyncio.get_running_loop().time()
    out = await collect(fp)  # speed=None 即刻回放
    assert len(out) == 2
    assert asyncio.get_running_loop().time() - start < 0.5
