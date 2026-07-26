# Changelog

本项目遵循 [语义化版本](https://semver.org/lang/zh-CN/)。

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
