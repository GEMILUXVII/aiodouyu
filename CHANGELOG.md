# Changelog

本项目遵循 [语义化版本](https://semver.org/lang/zh-CN/);
0.x 阶段的兼容承诺见 README「版本与稳定性」。

## [0.4.11] - 2026-08-06

### Fixed

- 明确收到视频轮播 `rss (ss=1, ivl=1)` 后，短期内不再让冲突的 HTTP
  直播快照覆盖轮播状态，避免轮播启动或重启时误发开播通知
- betard 确认轮播时持续刷新保护状态；真实 `rss (ss=1, ivl=0)` 仍立即
  触发开播，漏掉实时事件时也会在保护窗口结束后恢复 HTTP 兜底

## [0.4.10] - 2026-07-28

### Fixed

- `betard` 状态请求增加唯一查询参数绕过斗鱼 CDN 的 60 秒缓存，避免刚
  开播仍读到“未开播”、刚下播仍读到上一场“直播中”
- 实时开播保护期内，即使紧接着收到反向 `rss`，缓存的 HTTP 下播快照
  也不能撤销开播；HTTP 一旦确认直播会立即清除下播候选
- 已知最近下播边界时，HTTP 直播快照必须携带晚于该边界的有效开播时间
  才能证明是新场，避免重启后把上一场缓存误报为再次开播

## [0.4.9] - 2026-07-28

### Fixed

- 快速下播再开播时，HTTP 返回的不晚于最近下播时间的旧 `started_at`
  不再污染新场次；首次启动或没有下播边界时仍可回溯真实开播时间
- 场次起点在开播回调前确定并保持不变：实时 `rss` 使用观测时间，
  HTTP 发现的直播使用有效 `started_at`，后续对账不再向前改写起点

## [0.4.8] - 2026-07-27

### Added

- `LiveStatusMonitor.last_offline_time` 公开最近一次确认下播的有效事件时间，
  供应用层在二次确认后仍能展示和持久化真实下播时刻

### Fixed

- HTTP 已确认房间仍在直播时立即撤销冲突的下播候选，避免旧候选一直保留，
  并在稍后真正下播时复用错误时间戳、把正常场次误算成约 1 秒

## [0.4.7] - 2026-07-27

### Fixed

- 明确的实时开播 `rss` 现在立即触发开播回调，不再被响应较慢或滞后的
  HTTP 状态否决；HTTP 仍负责下播确认和漏事件对账
- HTTP 尚未确认实时开播前，不会用旧的离线快照反向产生假下播；60 秒仍
  未确认时转入正常下播复核，确保已通知的场次最终产生对应的下播事件
- 实时开播绕过上一场通知冷却，修复快速重新开播被延迟的问题
- `DanmakuClient` 现在等待服务端 `loginres` 后再发送 `joingroup`，且只有
  登录成功后才报告连接就绪，避免“TCP 已连接但房间订阅尚未成功”的假健康状态

## [0.4.6] - 2026-07-27

### Added

- `LiveStatusMonitor` 的 HTTP 对账回调在原有 `msg` 参数中附带可序列化的
  `room_info` 快照，应用层可直接复用标题、分类和封面，避免重复 HTTP 请求

### Fixed

- `announce_initial_live=False` 现在只抑制初始开播回调；静默接管的直播
  后续仍会产生下播回调，不再因“未补报开播”而永久漏掉下播
- 同一场直播的后续 HTTP 快照不再把开播时间向后覆盖，避免直播时长被
  接口抖动逐步缩短；无效或未来时间戳会被忽略

## [0.4.5] - 2026-07-27

### Added

- `LiveStatusMonitor` 新增可选的 `periodic_resync_interval`，即使弹幕连接
  尚未建立或没有收到 `rss`，也能按指定间隔使用 HTTP 对账当前直播状态；
  默认关闭，由应用层按规模和接口预算显式启用

## [0.4.4] - 2026-07-27

### Fixed

- 从旧实例继承的待定状态在应用前强制重新进行 HTTP 对账，避免插件重载
  后在弹幕连接与首次对账完成前直接采用旧快照中的待定下播

## [0.4.3] - 2026-07-27

### Fixed

- `rss` 候选与首次 HTTP 快照冲突时持续短期复查，避免斗鱼 HTTP
  状态更新稍慢导致真实开播/下播被永久吞掉
- 从直播中切换到下播须由 HTTP 状态持续稳定 90 秒后才输出，短时状态
  回退会被自动撤销，进一步避免假下播及紧随其后的重复开播
- 补回高层监控器的启动、连接、初始状态和停止日志，方便宿主接入统一
  日志系统

## [0.4.2] - 2026-07-27

### Added

- 新增 `LiveStatusMonitor` 高层状态监控器：原始 `rss` 仅触发 betard
  HTTP 对账，只有确认后的开播/下播转换才会回调；含断连补偿、失败退避、
  冷却期校准、状态继承与干净停止

### Fixed

- `RoomStatus` 不再把缺少可选 `ivl` 字段的 `ss=1` 消息误判为下播；
  仅显式 `ivl=1` 判定为视频轮播
- 不完整、抖动或与 HTTP 当前状态冲突的 `rss` 不再产生假下播

## [0.4.1] - 2026-07-27

发版后诚实自查(含真实服务器实证)的修复。

### Fixed

- **`resolve_room_id` 此前根本解析不了靓号**(实测推翻了它的设计前提):
  betard 对靓号返回错误页、open API 对靓号原样回显——两者都不解析。
  现改为解析移动端房间页 `m.douyu.com/{id}`,实测三个真实靓号
  (6657→6979222、5232→178432、123455→5526219)全部正确;真实 rid
  输入原样返回
- **`transport="auto"` 在其目标场景里彻底失效**:ws 腿没有独立超时,
  与 tcp 回退共享同一个 `connect_timeout`。防火墙静默 DROP 掉 ws 的
  SYN 时(受限网络最常见形态),ws 尝试挂到 OS TCP 超时(20-130s)
  远超预算,外层超时一到整个拨号被取消,**tcp 回退永远轮不到**——
  即使明文端口完全可用也连不上。ws 腿现在有独立子超时
- **`DanmakuHub` 无法与 WebSocket 传输组合**(0.3 与 0.4 的招牌特性
  不互通):默认工厂只透传两个参数,想要 Hub+ws 只能自己写被标注为
  "测试注入用"的 `client_factory`。Hub 现在接受 `**client_kwargs`
  透传给每个客户端(`DanmakuHub(types={"rss"}, transport="auto")`)
- `overflow="drop_oldest"` 配 `queue_maxsize<=0` 会静默变成无界队列、
  永不丢弃(与用户意图相反),现在明确报错
- WebSocket 握手状态行改为规范化解析(旧的子串兜底会误接受 reason
  里含 "101" 的响应,也会误拒 RFC 允许的空 reason 形式)
- 文档如实化:ws 的收益是 TLS 规避 DPI 与端点迁移对冲,**不是**
  "443 系所以能穿墙"(8506 仍是非标高端口);SECLEVEL=1 同时放宽了
  可接受的证书签名算法与密钥强度,不只是密码套件

## [0.4.0] - 2026-07-27

### Added

- **WebSocket 传输**:`DanmakuClient(..., transport="ws"|"auto")` 走
  网页端同款 `wss://danmuproxy.douyu.com:8506`。手写最小 RFC 6455
  客户端(仅客户端角色、二进制帧、零扩展协商、ping/pong 传输层透明
  处理),**零运行时依赖不变**。动机:TCP 8601 常被企业防火墙拦截,
  且双传输对冲非官方端点再次迁移的风险
  - 内置 TLS 上下文放宽密码套件安全级别(斗鱼端点只提供
    `AES256-GCM-SHA384`,SECLEVEL=2 会拒绝握手),证书与主机名校验
    保持开启;可传 `ssl_context` 覆盖
  - `transport="auto"` 先 ws 失败回退 tcp;默认仍是 `"tcp"`,
    行为与旧版完全一致
- 传输层抽象(`aiodouyu.transport`):`TcpTransport`/`WsTransport`
  对客户端暴露同一字节流接口,弹幕成帧逻辑与传输解耦

> 注:0.3.1 的发布包已包含本模块代码(默认 `transport="tcp"` 时完全
> 惰性,行为无变化),文档与公开导出自 0.4.0 起生效。

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
