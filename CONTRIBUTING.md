# 贡献指南

感谢你考虑为 aiodouyu 做贡献。提 issue / PR 前请先通读本文,能省去很多来回。

## 开发环境

要求 Python >= 3.10。

```bash
git clone https://github.com/GEMILUXVII/aiodouyu.git
cd aiodouyu
pip install -e .
pip install pytest pytest-asyncio ruff
```

常用命令:

```bash
pytest -q          # 运行测试(全部离线,数秒内完成)
ruff check .       # 代码检查(CI 同款配置)
```

CI 在 Ubuntu / Windows 与 Python 3.10 / 3.13 矩阵上运行同样的两条命令,
提交前请确保本地通过。

## 硬约束:运行时零依赖

**零运行时依赖是本库的核心卖点**(`pyproject.toml` 中 `dependencies = []`),
不是可以商量的实现细节。

- 新增 runtime dependency 的 PR **原则上直接拒绝**,无论该依赖多流行、
  多轻量。请先开 issue 讨论,不要直接提 PR。
- dev 依赖(测试、lint 工具)可议,在 issue 中说明理由即可。
- 标准库能实现的功能,请用标准库实现,即使代码会略长。

## 协议相关改动须附证据

斗鱼弹幕协议是非官方接口,字段与行为只能靠实测确认。任何涉及协议层
(`stt` / `packet` / 握手 / 心跳 / 消息字段语义)的改动,PR 描述中必须附上
真实抓包证据:

```bash
python -m aiodouyu <rid> --record dump.jsonl
```

粘贴 `dump.jsonl` 中能支撑你改动的片段(注意脱敏,见下),并注明抓包时间
与房间号。没有抓包证据的协议改动,即使"看起来对",也无法合入。

> 抓包片段可能包含观众昵称、UID 等信息,粘贴前请自行打码替换。

## 文档语言政策

- **docstring 以中文为主**;公共模块与公共类的 docstring **首行须附一句
  英文摘要**,便于非中文用户在 IDE / API 文档中快速定位。
- README 采用"中文正文 + English 小节"结构,改动涉及用户可见行为时,
  **两个语言的章节须同步更新**,不要只改一边。
- CHANGELOG 使用中文。

## Commit 风格

使用**英文 Conventional Commits**:

```
feat: add websocket transport
fix(web): map open API error=102 to ApiError
docs: sync English section for fetch_room
test: cover heartbeat timeout reconnect path
```

常用类型:`feat` / `fix` / `docs` / `test` / `refactor` / `chore` / `ci`。
一个 commit 只做一件事;正文(如需要)解释"为什么"而非"做了什么"。

## 测试要求

- **改协议层必须带离线测试**:使用测试套件中的 `FakeDanmakuServer`
  (见 `tests/test_client.py`)构造帧序列复现场景,不要依赖真实斗鱼服务器。
- **不接受需要联网的测试进 CI**。CI 必须在无外网环境下可复现;
  需要真实服务器验证的场景,请把结论以抓包证据形式写进 PR 描述,
  测试本身用假服务器回放。
- 修 bug 的 PR 应包含一个"不修则挂"的回归测试。

## 提 PR 之前

1. `pytest -q` 与 `ruff check .` 本地通过;
2. 协议改动附抓包证据,用户可见改动同步 README 中英两节;
3. 在 CHANGELOG 的未发布小节补一行(不确定归类可留空,评审时再定)。

不确定的想法,先开 issue 聊,别憋大招。
