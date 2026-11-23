"""PII 脱敏：审计要"问过什么"，不要"用户数据再存一份"。

为什么在写入侧脱、不在读取侧脱：

  · 审计表是**不可变**的（trigger 禁 UPDATE/DELETE）。一旦把明文手机号写进去，
    想擦掉就只能走受控删除通道——成本远高于写入时替换。
  · 读取侧脱敏意味着每个读的人（含直接连库的人）都要记得脱；写入侧只有一处。

脱什么：手机号、身份证、邮箱、银行卡、IPv4。
不脱什么：日期、指标值、表名、SQL。三类都可能长得像数字，误脱会把审计变成废纸：

    '2026-11-08'        日期        —— 11 位？不是，带分隔符，不会命中
    '89250'             指标值      —— 5 位，不够长
    '4994396'           行数        —— 7 位，不够 11 位
    'S000014'           歌曲 ID     —— 字母开头

原则：**宁少脱不误脱**。脱敏的价值在于"确实脱掉了真实 PII"，
把普通数字改成 `***` 只会让人不敢信这张表。
"""
from __future__ import annotations

import re

# 顺序有讲究：长的先匹配。身份证 18 位若不先吃，会被 16~19 位的银行卡规则抢走。
_RULES: list[tuple[str, re.Pattern]] = [
    ("EMAIL", re.compile(r"[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}")),
    # 18 位身份证（末位可能是 X）与 15 位老身份证
    ("IDCARD", re.compile(r"(?<![0-9A-Za-z])(?:\d{17}[\dXx]|\d{15})(?![0-9A-Za-z])")),
    # 银行卡 16~19 位，允许中间有空格/短横（常见写法 6222 0202 0000 1234）
    ("BANKCARD", re.compile(r"(?<!\d)(?:\d[ \-]?){15,18}\d(?!\d)")),
    ("PHONE", re.compile(r"(?<!\d)1[3-9]\d{9}(?!\d)")),
    ("IPV4", re.compile(r"(?<!\d)(?:\d{1,3}\.){3}\d{1,3}(?!\d)")),
]


def redact(text: str) -> tuple[str, int]:
    """返回 (脱敏后的文本, 命中个数)。命中个数用于写 `pii_hits`，
    让人一眼看出"这条问句里原本带着 PII"——这本身就是审计信息。"""
    if not text:
        return text or "", 0
    out = str(text)
    hits = 0
    for tag, rx in _RULES:
        out, n = rx.subn("[%s]" % tag, out)
        hits += n
    return out, hits


def redact_state(state: dict) -> tuple[str, str, int]:
    """把一条问答里要落盘的两处文本一起脱：问句 + 答案摘录。

    分开返回是为了让调用方清楚：这两处都可能含 PII，
    答案里同样会出现"用户张三 13800001111 的播放记录"这种句子。
    """
    q, h1 = redact(state.get("question") or "")
    a, h2 = redact((state.get("answer") or "")[:500])
    return q, a, h1 + h2


def scan(text: str) -> list[str]:
    """只列出命中了哪几类，不改文本。自测与排查用。"""
    return [tag for tag, rx in _RULES if rx.search(text or "")]


if __name__ == "__main__":
    samples = [
        "手机 13800001111 的用户昨天听了多少歌",
        "身份证 110101199003071234 的播放记录",
        "邮箱 zhang.san@example.com 的付费情况",
        "卡号 6222 0202 0000 1234 的消费",
        "来自 192.168.128.163 的请求",
        "2026-11-08 的 DAU 是多少",
        "S000014 这首歌的播放量",
        "4994396 行数据",
    ]
    for s in samples:
        out, n = redact(s)
        print("%-42s → %-52s (%d)" % (s, out, n))
