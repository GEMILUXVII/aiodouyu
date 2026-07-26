"""示例：监控直播间开播/下播状态

弹幕连接只在状态变化时收到 rss 推送，断连窗口内的变化会丢失；
本示例在每次连接建立后用 fetch_room() 校准一次当前状态。

用法: python examples/room_status.py <房间号>
"""

import asyncio
import sys

from aiodouyu import EVENT_CONNECTED, ApiError, DanmakuClient, fetch_room


async def main(room_id: int) -> None:
    client = DanmakuClient(
        room_id=room_id,
        types={"rss"},
        emit_connection_events=True,
    )
    async with client:
        async for msg in client:
            if msg["type"] == EVENT_CONNECTED:
                # rss 只在状态变化时推送，重连后主动校准当前状态
                try:
                    info = await fetch_room(room_id)
                except ApiError as exc:
                    print(f"已连接（状态校准失败: {exc}）")
                    continue
                print(f"已连接，当前{'🟢 直播中' if info.is_live else '⚪ 未开播'}: "
                      f"{info.owner} - {info.title}")
            elif msg["type"] == "rss":
                is_live = msg.get("ss") == "1" and msg.get("ivl") == "0"
                print("🟢 开播了!" if is_live else "⚪ 下播了")


if __name__ == "__main__":
    asyncio.run(main(int(sys.argv[1])))
