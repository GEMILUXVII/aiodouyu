# Changelog

本项目遵循 [语义化版本](https://semver.org/lang/zh-CN/);
0.x 阶段的兼容承诺见 README「版本与稳定性」。

## [0.3.1] - 2026-07-27

Hub 发版后对抗审查(含实测复现)的修复。

### Fixed

- **`remove()` 死锁**:`overflow="block"` 且消费者慢/缺席时,泵阻塞
  在满队列的 `put` 上,`client.close()` 解除不了队列等待,
  `remove()`/`close()` 永久挂死。现取消泵任务收尾
- **`remove()` 被取消时孤儿化泵**:`client.close()` 含真实挂起点,
  调用方在此被取消时 `CancelledError` 越过 `suppress(Exception)`,
  而条目已被摘除——泵成为 `close()` 也回收不到的孤儿。清理移入
  `finally`
- **泵异常退出留下僵尸房**:`hub.rooms` 谎报该房受管、`add()` 以
  "已存在"拒绝重加、消息静默停流。泵现在自摘条目(按任务身份判断,
  不误删并发 re-add 的新条目)
- 单轮消费语义强制(此前 `break` 后能否重入取决于 GC 时机);
  `fetch_rooms(concurrency=0)` 不再静默永久挂起
- 文档如实说明 block 模式的"不丢消息"限于稳态

## [0.3.0] - 2026-07-27

### Added

- **DanmakuHub 多房间管理器**:N 个房间聚合为单一 `(room_id, msg)`
  流;`add`/`remove` 幂等可动态增删、单房故障隔离、有界队列
  (`overflow="block"` 反压不丢消息 / `"drop_oldest"` 丢旧保新)、
  `close()` 干净关停。真实服务器双房验证
- **`fetch_rooms()`**:批量限并发拉取房间信息,逐房异常不拖垮整批
- **`resolve_room_id()`**:靓号解析为真实 rid(弹幕连接必须用真实
  rid,这是新用户第一坑)
- examples/multi_room.py;README 增补多房间与背压说明

## [0.2.0] - 2026-07-27

### Added

- **typed models**(`aiodouyu.models`):chatmsg/dgb/uenter/rss 的
  frozen dataclass 与 `parse()` 纯函数。可选层,默认行为不变;宽松
  解析(缺失/畸形字段得 None,永不抛异常),保留 `raw` 逃生舱。
  字段基于 1600+ 条真实抓包语料定稿(dgb 数量字段是 `gfcnt`、礼物名
  `gfn` 在消息内、粉丝牌三元组建模——实测跨房牌是常态);rss 语义
  与生产状态机对齐(`is_live = ss=='1' and ivl=='0'`)
- **公开测试基建**(`aiodouyu.testing.FakeDanmakuServer`):可脚本化
  的离线假弹幕服务器,含 async context manager 与 `make_client()`;
  库自身测试套件即第一个客户
- **录制/回放**:`python -m aiodouyu <rid> --record dump.jsonl` 把完整
  消息流(不受 `--types` 影响)录制为版本化 JSONL;`aiodouyu.replay()`
  以与 `DanmakuClient` 完全同构的流回放,支持 `speed`(倍速)、
  `max_gap`(空窗钳制)、`types`(伪事件豁免,契约与客户端一致)
- 社区基建:CONTRIBUTING、issue 模板(bug/protocol_drift,要求附
  `--record` dump)、README 徽章与 0.x 稳定性政策、Release notes
  自动化(CHANGELOG 为唯一事实源)
- CI:Python 3.14 矩阵、uvloop 冒烟任务;供应链加固(action SHA pin +
  Dependabot、最小权限、显式 PyPI attestations)

### Fixed

- `_open_connection` 外层取消与连接完成竞速时,侥幸建立的连接被
  立即收掉而非泄漏

## [0.1.2] - 2026-07-27

## [0.1.2] - 2026-07-27

第三轮适配审计(含真实服务器实证测试)后的修复版。

### Fixed

- `close()` 现在能打断**进行中的连接尝试**:此前 close 落在
  `open_connection` 等待期间时,消费任务会滞留到 connect_timeout
  (黑洞网络实测 7.7 秒)。连接现与 close 事件竞速,几乎同时完成的
  连接会被立即收掉而非泄漏
- `break` 弃置且未 close 的客户端现在可被 GC 及时回收:心跳任务改经
  weakref 持有客户端(睡眠期间不持强引用),弃置的对象图不再被事件
  循环定时器钉死,生成器终结器兜底得以生效(此前要等空闲超时约
  165 秒才释放连接)
- `room_id` 传字符串时给出明确的 `TypeError: room_id 必须为 int`,
  不再是难以理解的比较运算报错(`DanmakuClient` 与 `fetch_room` 均改)

## [0.1.1] - 2026-07-27

### Fixed

- `close()` 从其他任务调用时不再与消费方的 `__anext__` 竞争
  (旧实现对活跃迭代器 `aclose()`,可抛
  `RuntimeError('generator is already running')`)
- `fetch_room` 异常契约补漏:未知 charset(`LookupError`)与
  `http.client` 传输异常映射为 `ApiError`;非标 JSON `Infinity`
  不再导致 `OverflowError` 穿透,数值字段安全降级为 `None`
- open API 错误映射修正:仅 `error=101` 判为 `RoomNotFound`,
  其余非零码与畸形响应判为 `ApiError`(限流不再被误报为房间不存在)
- CLI:可预期失败输出一行错误而非 traceback;stderr 同步 UTF-8;
  `--types "rss, chatmsg"` 带空格不再静默失效
- `packet.__all__` 补齐 `PacketError`/`validate_length`;`PacketError`
  纳入 `AiodouyuError` 族(仍是 `ValueError` 子类,向后兼容)
- `DanmakuClient.on()` 增加 `@overload` 标注,py.typed 下装饰器
  用法不再退化为 `Any`
- README 首个示例的开播判定补上 `ivl` 字段(视频轮播不算开播);
  `fetch_room` 的 timeout 文档改为如实描述逐 socket 操作语义

## [0.1.0] - 2026-07-26

首个公开版本。

### Added

- `DanmakuClient`:斗鱼弹幕 asyncio 客户端
  - 异步迭代(`async for`)与回调注册(`@client.on`)两种消费方式
  - 自动重连(指数退避 + 抖动),空闲超时检测半开连接
  - `types` 过滤、`EVENT_CONNECTED`/`EVENT_DISCONNECTED` 连接生命周期伪事件
  - `close()` 干净停止,不遗留任务
- `stt` 模块:斗鱼 STT 序列化格式编解码
- `packet` 模块:弹幕协议二进制成帧
- `web` 模块:房间信息 HTTP 接口(零依赖)
  - `fetch_room()` 支持 betard / open API 双数据源与自动回退
  - `RoomInfo` 归一化快照,`videoLoop` 轮播识别
- 命令行工具 `python -m aiodouyu`(弹幕冒烟测试、`--info` 房间查询)
- 完整类型标注(`py.typed`)
