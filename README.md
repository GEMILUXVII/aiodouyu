# aiodouyu

斗鱼弹幕与房间状态的 **asyncio** 客户端库 —— 零依赖、自动重连、干净停止。

[English](#english) | [背景](#为什么有这个库) | [安装](#安装) | [用法](#用法) | [房间信息](#房间信息http) | [协议说明](#协议说明) | [局限与路线图](#局限与路线图)

## 为什么有这个库

Python 生态里的斗鱼弹幕库已全部停止维护：[pydouyu](https://github.com/Kexiii/pydouyu)（2022 年后无更新）、[danmu](https://github.com/littlecodersh/danmu)（2017）、[danmaku](https://github.com/IsoaSFlus/danmaku)（已归档）。aiodouyu 是这个空缺的现代替代品：

- **asyncio 优先**：每个房间一个协程，不再是"每房间三个线程"
- **零运行时依赖**：纯标准库 TCP 实现
- **自动重连**：指数退避 + 抖动；空闲超时检测半开连接
- **干净停止**：`close()` 立即中止，不遗留线程/任务（pydouyu 的停止会泄漏线程）
- **完整消息流**：rss（开播状态）、chatmsg（弹幕）、dgb（礼物）、uenter（进房）……全部透出
- **可测试**：附带完整测试套件与假服务器测试基建

## 安装

```bash
pip install aiodouyu
```

要求 Python >= 3.10。

## 用法

### 异步迭代（推荐）

```python
import asyncio
from aiodouyu import DanmakuClient

async def main():
    async with DanmakuClient(room_id=9999) as client:
        async for msg in client:
            if msg["type"] == "chatmsg":
                print(f'{msg.get("nn")}: {msg.get("txt")}')
            elif msg["type"] == "rss":
                print("开播" if msg.get("ss") == "1" else "下播")

asyncio.run(main())
```

### 回调注册

```python
client = DanmakuClient(room_id=9999)

@client.on("rss")
def on_status(msg):
    print("直播状态变化:", msg)

@client.on("*")          # 通配符匹配所有消息
async def on_any(msg):   # 同步/异步回调均可
    ...

await client.run()       # 运行直到 client.close()
```

### 只订阅关心的消息类型

```python
# 只做开播提醒？只要 rss，其他消息在解码后直接丢弃
client = DanmakuClient(room_id=9999, types={"rss"})
```

### 感知断连窗口

```python
from aiodouyu import EVENT_CONNECTED, EVENT_DISCONNECTED

client = DanmakuClient(room_id=9999, emit_connection_events=True)
async for msg in client:
    if msg["type"] == EVENT_DISCONNECTED:
        ...  # 断连期间可能错过状态变化，可在重连后主动校准
```

> 斗鱼只在状态**变化**时推送 rss。断连窗口内的变化不会补发，
> 建议消费方在收到 `EVENT_CONNECTED` 后通过 `fetch_room()` 校准一次当前状态。

### 命令行冒烟测试

```bash
python -m aiodouyu 9999 --types rss,chatmsg --duration 30
python -m aiodouyu 9999 --info    # 查询房间信息后退出
```

## 房间信息（HTTP）

`web` 模块提供房间信息拉取，同样零依赖。典型用途：重连后校准直播状态、
获取标题/主播名/分类/封面用于通知富化。

```python
from aiodouyu import fetch_room, RoomNotFound

info = await fetch_room(9999)
print(info.title, info.owner, info.category)   # 标题 / 主播名 / 分类
print(info.is_live)      # 是否真实开播（视频轮播不算，见 is_loop）
print(info.started_at)   # 本场开播 epoch 秒；未开播为 None
print(info.cover_url)    # 直播间封面
```

两个数据源，`source` 参数选择：

| source | 端点 | 特点 |
|---|---|---|
| `"betard"`（默认优先） | `www.douyu.com/betard/{rid}` | 字段最全，`videoLoop` 可识别视频轮播 |
| `"open"` | `open.douyucdn.cn/api/RoomApi/room/{rid}` | 公开 API 更稳定，但无法识别轮播 |
| `"auto"`（默认） | 先 betard，传输失败回退 open | 房间不存在（`RoomNotFound`）不回退 |

> open 源无法判定视频轮播：其 `is_loop` 为 `None`，轮播房会被报告为开播。
> 做开播状态校准时建议显式 `source="betard"`（失败则跳过本轮校准），
> 或在 `is_loop is None` 时自行决定是否信任 `is_live`。

配合弹幕客户端做状态校准：

```python
from aiodouyu import EVENT_CONNECTED, DanmakuClient, fetch_room

client = DanmakuClient(room_id=9999, types={"rss"}, emit_connection_events=True)
async for msg in client:
    if msg["type"] == EVENT_CONNECTED:
        info = await fetch_room(9999)     # 断连窗口内的变化在这里补上
        print("当前状态:", info.is_live)
    elif msg["type"] == "rss":
        print("状态变化:", msg.get("ss") == "1" and msg.get("ivl") == "0")
```

## 协议说明

连接 `danmuproxy.douyu.com:8601`（TCP），STT 序列化（`@=` 键值、`/` 分隔、
`@A`/`@S` 转义），小端长度前缀成帧（发 689 / 收 690），`loginreq` + `joingroup`
握手，每 45 秒 `mrkl` 心跳。

**注意**：弹幕协议是斗鱼的非官方公开接口，端点与字段可能随时变更
（历史上已发生过一次域名迁移）。本库不隶属于斗鱼，请合理使用、避免滥用。

## 局限与路线图

当前局限：

- STT 嵌套结构（如 ranklist 的分组数据）不展开，值以原始字符串返回
- 仅 TCP 端点；WebSocket 端点（`wss://danmuproxy.douyu.com:850x`）未实现
- 弹幕消息字段无类型化模型，以 `dict[str, str]` 返回
- `web` 模块的 betard 数据源是网页端内部接口，字段可能随时变更
  （`auto` 模式会自动回退到公开 API）

路线图：WebSocket 传输、常见消息类型的 typed model。欢迎 issue / PR。

## English

Asyncio client for Douyu (斗鱼) live-stream danmaku and room-status events.
Zero runtime dependencies, automatic reconnection with exponential backoff,
idle-timeout detection for half-open connections, and clean shutdown.
Consume messages via `async for` or callback registration. A zero-dependency
`web` module (`fetch_room`) retrieves room metadata and live status over HTTP
for state calibration after reconnects. The barrage protocol is an unofficial
Douyu interface and may change without notice.

```python
async with DanmakuClient(room_id=9999) as client:
    async for msg in client:
        print(msg)

info = await fetch_room(9999)   # title / owner / category / is_live ...
```

## 许可证

[MIT](https://github.com/GEMILUXVII/aiodouyu/blob/master/LICENSE)

## 致谢

协议实现参考了 [pydouyu](https://github.com/Kexiii/pydouyu)（MIT）与社区对
斗鱼弹幕协议的公开分析文章。
