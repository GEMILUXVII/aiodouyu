"""命令行冒烟与录制工具

用真实房间验证协议实现::

    python -m aiodouyu <房间号> [--types rss,chatmsg] [--duration 30]
    python -m aiodouyu <房间号> --info                 # 查询房间信息后退出
    python -m aiodouyu <房间号> --record dump.jsonl    # 录制消息流到 JSONL
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import logging
import sys

from .client import DanmakuClient
from .exceptions import AiodouyuError
from .replay import write_header, write_message
from .web import fetch_room


async def _run(args: argparse.Namespace) -> None:
    # strip 各段:引号内带空格("rss, chatmsg")是 shell 里的自然写法,
    # 不清洗会让 " chatmsg" 永不匹配且无任何提示
    types = {t.strip() for t in args.types.split(",") if t.strip()} or None
    client = DanmakuClient(
        room_id=args.room_id,
        # 录制时收全量流(--types 只影响控制台打印):语料建设与 issue
        # 复现都需要完整消息,过滤是消费端的事
        types=None if args.record else types,
        emit_connection_events=True,
    )

    async def stop_later() -> None:
        await asyncio.sleep(args.duration)
        print(f"--- {args.duration}s 到，关闭 ---")
        await client.close()

    stopper = None
    if args.duration > 0:
        stopper = asyncio.create_task(stop_later())
    record_fp = None
    if args.record:
        # CLI 工具单次打开本地文件,阻塞可忽略
        record_fp = open(args.record, "w", encoding="utf-8")  # noqa: ASYNC230
        write_header(record_fp, args.room_id)
        print(f"--- 录制到 {args.record} ---")
    try:
        async with client:
            async for msg in client:
                if record_fp is not None:
                    write_message(record_fp, msg)
                    if types is not None and msg.get("type") not in types:
                        continue
                print(msg)
    finally:
        if record_fp is not None:
            record_fp.close()
        if stopper:
            stopper.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await stopper


async def _show_info(room_id: int) -> None:
    info = await fetch_room(room_id)
    for name, value in [
        ("房间号", info.room_id),
        ("标题", info.title),
        ("主播", info.owner),
        ("分类", info.category or "-"),
        ("状态", "直播中" if info.is_live else ("视频轮播" if info.is_loop else "未开播")),
        ("开播时间戳", info.started_at or "-"),
        ("热度", info.online if info.online is not None else "-"),
        ("数据源", info.source),
    ]:
        print(f"{name}: {value}")


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="python -m aiodouyu", description="斗鱼弹幕连接冒烟测试"
    )
    parser.add_argument("room_id", type=int, help="斗鱼房间号")
    parser.add_argument(
        "--types", default="", help="逗号分隔的消息类型过滤，如 rss,chatmsg"
    )
    parser.add_argument(
        "--duration",
        type=float,
        default=30.0,
        help="运行秒数，0 表示直到 Ctrl+C（默认 30）",
    )
    parser.add_argument(
        "--info", action="store_true", help="查询房间信息（HTTP）后退出，不连弹幕"
    )
    parser.add_argument(
        "--record",
        default="",
        metavar="FILE",
        help="把完整消息流(不受 --types 影响)录制为 JSONL,可用 aiodouyu.replay 回放",
    )
    args = parser.parse_args()

    # Windows 控制台默认 GBK 编码，弹幕中文会显示为乱码；强制 UTF-8 输出
    # (stderr 也要处理：logging 与错误消息都走 stderr)
    for stream in (sys.stdout, sys.stderr):
        if hasattr(stream, "reconfigure"):
            stream.reconfigure(encoding="utf-8", errors="replace")

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    try:
        asyncio.run(_show_info(args.room_id) if args.info else _run(args))
    except KeyboardInterrupt:
        print("--- 已停止 ---")
        sys.exit(0)
    except (AiodouyuError, ValueError) as exc:
        # 可预期的常规失败(房间不存在/接口失败/非法房间号)给一行
        # 错误信息即可,原始 traceback 会让用户误以为库内部崩溃
        print(f"错误: {exc}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
