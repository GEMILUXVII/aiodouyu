# aiodouyu

[![ci](https://github.com/GEMILUXVII/aiodouyu/actions/workflows/ci.yml/badge.svg)](https://github.com/GEMILUXVII/aiodouyu/actions/workflows/ci.yml)
[![PyPI](https://img.shields.io/pypi/v/aiodouyu)](https://pypi.org/project/aiodouyu/)
[![Python](https://img.shields.io/pypi/pyversions/aiodouyu)](https://pypi.org/project/aiodouyu/)
[![Downloads](https://img.shields.io/pypi/dm/aiodouyu)](https://pypi.org/project/aiodouyu/)
[![dependencies](https://img.shields.io/badge/dependencies-0-brightgreen)](#为什么有这个库)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue)](https://github.com/GEMILUXVII/aiodouyu/blob/master/LICENSE)

斗鱼弹幕与房间状态的 **asyncio** 客户端库 —— 零依赖、自动重连、干净停止。

[English](#english) | [背景](#为什么有这个库) | [安装](#安装) | [用法](#用法) | [房间信息](#房间信息http) | [多房间](#多房间danmakuhub) | [传输方式](#传输方式tcp--websocket) | [类型化模型](#类型化模型可选) | [录制与回放](#录制与回放) | [协议说明](#协议说明) | [版本与稳定性](#版本与稳定性) | [局限与路线图](#局限与路线图)

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
                # ivl=='0' 排除视频轮播:轮播房的 ss 也是 '1'
                is_live = msg.get("ss") == "1" and msg.get("ivl") == "0"
                print("开播" if is_live else "下播")

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

## 多房间(DanmakuHub)

监控 N 个主播的应用不必手写任务编排、异常隔离、聚合队列、优雅关停
这套样板——Hub 把它们上移进库:

```python
from aiodouyu import DanmakuHub

hub = DanmakuHub(types={"rss"}, emit_connection_events=True)
await hub.add(9999)          # 运行中可动态增删,幂等
await hub.add(288016)
async for room_id, msg in hub:   # 统一聚合流,库负责打房间号
    ...
await hub.close()            # 关停全部房间,不遗留任务
```

**消费速度与背压**:聚合队列有界(默认 1024)。`overflow="block"`
(默认)时慢消费者会反压到各房间的 TCP 接收——不丢消息,代价是消费
时效;只关心最新状态的场景用 `overflow="drop_oldest"` 丢旧保新。
单客户端场景同理:`async for` 不取消息时 TCP 缓冲会积压,长期不消费
可能被服务端判定僵尸连接断开(库会自动重连)。

## 类型化模型（可选）

不想记 `nn`/`txt`/`gfid` 这些协议字段名？`models.parse()` 把常见消息
转成带类型字段的 dataclass（可选层，默认行为不变；字段缺失/畸形得
`None` 永不抛异常，原始字典始终在 `.raw`）：

```python
from aiodouyu import models

async for msg in client:
    parsed = models.parse(msg)          # 未识别类型返回 None
    match parsed:
        case models.ChatMsg():
            print(parsed.nickname, parsed.text)
        case models.Gift():
            print(parsed.nickname, "送出", parsed.gift_name, "x", parsed.count)
        case models.RoomStatus():
            print("开播" if parsed.is_live else "下播/轮播")
```

> 粉丝牌是三元组 `badge_name/badge_level/badge_room_id`——实测跨房
> 粉丝牌是常态（`badge_room_id` 常 ≠ 当前房间），展示时请一起判断。

## 录制与回放

把真实消息流录成 JSONL,离线回放给完全相同的消费代码——测试夹具、
issue 复现(协议漂移报告请附 dump)、弹幕数据分析都用它:

```bash
python -m aiodouyu 9999 --record dump.jsonl --duration 60
```

> `--record` 录制**完整**消息流(`--types` 只影响控制台打印),
> 语料才不会系统性缺字段。文件首行是版本化 header,其后每行
> `{"ts": ..., "msg": {...}}`。

```python
from aiodouyu import replay

# 即刻回放(默认,测试主场景)
async for msg in replay("dump.jsonl"):
    ...  # msg 与 DanmakuClient 产出完全同构

# 按原速回放,但单次空窗最多睡 1 秒(真实语料含 45s+ 心跳空窗)
async for msg in replay("dump.jsonl", speed=1.0, max_gap=1.0):
    ...

# types 过滤与连接伪事件豁免的语义与 DanmakuClient 一致
async for msg in replay("dump.jsonl", types={"rss"}):
    ...
```

## 传输方式(TCP / WebSocket)

默认走明文 TCP(`danmuproxy.douyu.com:8601`)。受限网络可切到网页端同款的
WebSocket 端点:

```python
DanmakuClient(9999, transport="ws")     # wss://danmuproxy.douyu.com:8506
DanmakuClient(9999, transport="auto")   # 先 ws,失败回退 tcp
```

WebSocket 客户端是手写的最小 RFC 6455 实现(仅客户端角色、二进制帧、
零扩展协商),**零依赖不变**;弹幕协议在两种传输上完全一致,ping/pong
在传输层透明处理。

> **ws 的真实收益**是 TLS 包裹让 DPI/协议指纹识别失效,以及对冲非官方
> TCP 端点再次迁移——但 8506 仍是非标高端口,只放行 80/443 出站的
> 端口白名单防火墙会同样拦它。
>
> 实测细节:斗鱼 wss 端点只提供 `AES256-GCM-SHA384`(RSA 密钥交换),
> 现代 OpenSSL 默认 SECLEVEL=2 会直接拒绝握手。库内置 TLS 上下文把
> 安全级别降到 1——注意这**同时**放宽了可接受的证书签名算法与密钥
> 强度(不只密码套件),但证书链与主机名校验仍开启;需要更严格时传
> `ssl_context`。

多房间同样可以走 WebSocket(参数透传给每个客户端):

```python
DanmakuHub(types={"rss"}, transport="auto")
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
- `web` 模块的 betard 数据源是网页端内部接口，字段可能随时变更
  （`auto` 模式会自动回退到公开 API）

路线图：核心能力已完备,后续按 issue 与实际需求演进。
欢迎 issue / PR（贡献指南见 [CONTRIBUTING.md](CONTRIBUTING.md)）。

## 版本与稳定性

本库处于 0.x 阶段，遵循如下承诺（自 0.2 起生效）：

- **补丁版本（0.y.Z）永不破坏 API**；破坏性变更只出现在次版本（0.Y.0）。
  下游建议以 `aiodouyu~=0.y.z` 钉住次版本
- **公共 API 面** = 包根与 `packet`/`stt`/`web`/`replay` 各自的 `__all__`、
  `EVENT_*` 常量值及"连接伪事件不受 types 过滤"的行为、异常继承关系
  （`RoomNotFound ⊂ ApiError ⊂ AiodouyuError`）、`DanmakuClient`/`fetch_room`/
  `replay` 的关键字签名
- **不承诺**：斗鱼协议的 dict 字段与服务端行为（协议漂移出适配性次版本，
  不视为库的破坏）；未来 typed models 的字段**增补**视为兼容
- 弃用流程：先在某个 0.Y 版本标记 `DeprecationWarning`，至少隔一个次
  版本后才移除

> 下游示例：AstrBot 插件 astrbot_plugin_douyu_live 以 `>=0.1.2,<0.2`
> 依赖本库。

## English

Asyncio client for Douyu (斗鱼) live-stream danmaku and room-status events.
**Zero runtime dependencies**, automatic reconnection with exponential
backoff, idle-timeout detection for half-open connections, and clean
shutdown. Consume messages via `async for` or callback registration. A
zero-dependency `web` module (`fetch_room`) retrieves room metadata and live
status over HTTP for state calibration after reconnects, and `replay()`
replays JSONL dumps recorded with `python -m aiodouyu <rid> --record` as a
stream identical in shape to the live client — ideal for tests and issue
reproduction. Patch releases never break the API (see 版本与稳定性). The
barrage protocol is an unofficial Douyu interface and may change without
notice.

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
