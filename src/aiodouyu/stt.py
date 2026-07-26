"""斗鱼 STT (Serialized To String) 序列化格式编解码

斗鱼弹幕协议使用 STT 格式传输键值数据：
- 键值对以 ``key@=value`` 表示，以 ``/`` 结尾
- 转义规则：``@`` -> ``@A``，``/`` -> ``@S``
  （序列化时先转义 ``@`` 再转义 ``/``；反序列化按相反顺序还原）

本模块只解析单层键值对。嵌套结构（值本身又是 STT 串，如 ranklist
的分组数据）保留为转义还原后的原始字符串，由调用方按需进一步解析。
"""

from __future__ import annotations

__all__ = ["dumps", "escape", "loads", "unescape"]


def escape(text: str) -> str:
    """转义 STT 特殊字符（序列化方向）"""
    return text.replace("@", "@A").replace("/", "@S")


def unescape(text: str) -> str:
    """还原 STT 转义字符（反序列化方向）"""
    return text.replace("@S", "/").replace("@A", "@")


def dumps(fields: dict[str, object]) -> str:
    """把扁平字典序列化为 STT 字符串

    值先经 ``str()`` 转换再转义，因此可直接传入整数（如房间号）。
    """
    return "".join(f"{escape(str(k))}@={escape(str(v))}/" for k, v in fields.items())


def loads(text: str) -> dict[str, str]:
    """把 STT 字符串解析为扁平字典

    - 无 ``@=`` 的段（纯值段）被跳过
    - 后出现的重复键覆盖先出现的
    - 不解析嵌套结构，值以字符串原样返回
    """
    result: dict[str, str] = {}
    for segment in text.split("/"):
        if not segment or "@=" not in segment:
            continue
        key, _, value = segment.partition("@=")
        result[unescape(key)] = unescape(value)
    return result
