"""示例:用 DanmakuHub 同时监控多个直播间

用法: python examples/multi_room.py <房间号1> <房间号2> ...
"""

import asyncio
import sys

from aiodouyu import EVENT_CONNECTED, DanmakuHub, fetch_rooms


async def main(room_ids: list[int]) -> None:
    # 批量拉一次元信息(限并发,单房失败不拖垮整批)
    infos = await fetch_rooms(room_ids, concurrency=3)
    names = {
        rid: info.owner if not isinstance(info, Exception) else str(rid)
        for rid, info in infos.items()
    }

    hub = DanmakuHub(types={"rss", "chatmsg"}, emit_connection_events=True)
    for rid in room_ids:
        await hub.add(rid)

    async with hub:
        async for room_id, msg in hub:
            name = names.get(room_id, room_id)
            if msg["type"] == EVENT_CONNECTED:
                print(f"[{name}] 已连接")
            elif msg["type"] == "rss":
                live = msg.get("ss") == "1" and msg.get("ivl") == "0"
                print(f"[{name}] {'🟢 开播' if live else '⚪ 下播'}")
            elif msg["type"] == "chatmsg":
                print(f"[{name}] {msg.get('nn')}: {msg.get('txt')}")


if __name__ == "__main__":
    asyncio.run(main([int(a) for a in sys.argv[1:]] or [9999]))
