"""示例：打印直播间实时弹幕

用法: python examples/print_danmaku.py <房间号>
"""

import asyncio
import sys

from aiodouyu import DanmakuClient


async def main(room_id: int) -> None:
    async with DanmakuClient(room_id=room_id, types={"chatmsg"}) as client:
        async for msg in client:
            print(f'[{msg.get("nn", "?")}] {msg.get("txt", "")}')


if __name__ == "__main__":
    asyncio.run(main(int(sys.argv[1])))
