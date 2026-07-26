# Changelog

本项目遵循 [语义化版本](https://semver.org/lang/zh-CN/)。

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
- `fetch_room` 异常契约补漏:未知 charset(`LookupError`)、
  非标 JSON `Infinity`(`OverflowError`)、`http.client` 传输异常
  均正确映射为 `ApiError`
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
