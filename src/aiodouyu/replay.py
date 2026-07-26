"""Record/replay of danmaku message streams as JSONL. / 弹幕消息流的录制与回放

录制格式(JSONL,UTF-8):

- 首行 header::

    {"format": "aiodouyu-record", "version": 1, "room_id": 9999,
     "recorded_at": 1785000000.0, "aiodouyu": "0.2.0"}

- 其后每行一条消息::

    {"ts": 1785000001.23, "msg": {"type": "chatmsg", ...}}

``replay()`` 产出与 :class:`aiodouyu.DanmakuClient` 完全同构的
``dict[str, str]`` 流——消费代码零改动即可离线运行。录制文件里的
连接伪事件(EVENT_CONNECTED/EVENT_DISCONNECTED)原样吐出,时序天然
正确;回放不合成新的伪事件。

用途:测试夹具、issue 复现(protocol_drift 报告附 dump)、弹幕数据
分析、typed models 的语料建设。
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from collections.abc import AsyncIterator
from os import PathLike
from typing import IO, Any

from .client import EVENT_CONNECTED, EVENT_DISCONNECTED

__all__ = ["RECORD_FORMAT", "RECORD_VERSION", "replay", "write_header", "write_message"]

logger = logging.getLogger("aiodouyu")

RECORD_FORMAT = "aiodouyu-record"
RECORD_VERSION = 1

# 连接伪事件豁免 types 过滤,镜像 DanmakuClient 的文档化契约
_PSEUDO_EVENTS = {EVENT_CONNECTED, EVENT_DISCONNECTED}


def write_header(fp: IO[str], room_id: int) -> None:
    """Write the record-file header line. / 写入录制文件首行 header"""
    from . import __version__

    fp.write(
        json.dumps(
            {
                "format": RECORD_FORMAT,
                "version": RECORD_VERSION,
                "room_id": room_id,
                "recorded_at": time.time(),
                "aiodouyu": __version__,
            },
            ensure_ascii=False,
        )
        + "\n"
    )
    fp.flush()


def write_message(fp: IO[str], msg: dict[str, str], ts: float | None = None) -> None:
    """Append one message line and flush. / 追加一条消息并立即落盘

    逐行 flush:录制的主要用途是事后取证(issue 复现/协议漂移分析),
    进程被杀时丢尾部消息比丢整个缓冲区好得多。
    """
    fp.write(
        json.dumps(
            {"ts": time.time() if ts is None else ts, "msg": msg},
            ensure_ascii=False,
        )
        + "\n"
    )
    fp.flush()


async def replay(
    source: str | PathLike[str] | IO[str],
    *,
    speed: float | None = None,
    max_gap: float | None = None,
    types: set[str] | None = None,
) -> AsyncIterator[dict[str, str]]:
    """Replay a recorded JSONL dump as a DanmakuClient-shaped stream. /
    把录制文件回放为与 DanmakuClient 同构的消息流

    Args:
        source: 录制文件路径或已打开的文本流。带 header 的标准录制与
            无 header 的裸 JSONL(每行 ``{"ts":..., "msg":{...}}``)均可
        speed: ``None``(默认)为即刻回放(测试主场景);数值为倍速乘数,
            按消息间原始间隔 / speed 睡眠。``speed <= 0`` 抛 ValueError
        max_gap: 单次睡眠的上限秒数。真实语料含 45s+ 心跳间隔与深夜
            空窗,不钳制的话 ``speed=1.0`` 的演示/测试会长时间挂住
        types: 只产出这些 type 的消息;None 产出全部。连接伪事件
            (EVENT_CONNECTED/EVENT_DISCONNECTED)不受过滤影响,与
            DanmakuClient 的契约一致

    Yields:
        ``dict[str, str]``,键值均强制为 str(手工构造的 dump 可能带
        int,强转守住"与 DanmakuClient 同构"的承诺)

    Raises:
        ValueError: speed 非法
        OSError: source 路径无法打开

    注意:文件按行惰性读取,本地磁盘的阻塞读在事件循环上可忽略;
    超大文件配合网络文件系统时请自行移到线程中打开。
    """
    if speed is not None and speed <= 0:
        raise ValueError(f"speed 必须为正数或 None,收到 {speed!r}")

    own_fp = not hasattr(source, "read")
    # 本地磁盘的阻塞 open/readline 在事件循环上可忽略(docstring 已注明);
    # 引入线程池反而使回放时序不可预测
    fp: IO[str] = (
        open(source, encoding="utf-8")  # noqa: ASYNC230
        if own_fp
        else source  # type: ignore[arg-type]
    )
    try:
        loop = asyncio.get_running_loop()
        next_at = loop.time()
        prev_ts: float | None = None
        first_line = True
        for line in fp:
            line = line.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
            except ValueError:
                logger.debug("回放:跳过无法解析的行: %.80s", line)
                continue
            if first_line:
                first_line = False
                # 标准录制的首行 header;无 header 的裸 JSONL 直接当消息
                if isinstance(record, dict) and record.get("format") == RECORD_FORMAT:
                    continue
            if not isinstance(record, dict):
                continue
            msg_raw: Any = record.get("msg")
            if not isinstance(msg_raw, dict):
                continue
            msg = {str(k): str(v) for k, v in msg_raw.items()}

            ts = record.get("ts")
            if speed is not None and isinstance(ts, (int, float)):
                if prev_ts is not None:
                    gap = (ts - prev_ts) / speed
                    if gap < 0:
                        gap = 0.0  # NTP 回拨/拼接文件的负增量钳 0
                    if max_gap is not None and gap > max_gap:
                        gap = max_gap
                    next_at += gap
                    delay = next_at - loop.time()
                    if delay > 0:
                        await asyncio.sleep(delay)
                prev_ts = float(ts)

            msg_type = msg.get("type")
            if (
                types is None
                or msg_type in types
                or msg_type in _PSEUDO_EVENTS
            ):
                yield msg
    finally:
        if own_fp:
            fp.close()
