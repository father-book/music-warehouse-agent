"""安全规则的**读取侧**（给 route 节点在生成 SQL 之前做注入前置判定）。

为什么单独一个模块：R23 在 guard.py 里检查的是"已经生成出来的 SQL 与用户原问"，
但注入攻击应该在**生成之前**就拦掉——否则模型已经吃进了恶意指令。
两边读同一份 `agent/guards/security_patterns.json`，规则只有一处。
"""
from __future__ import annotations

import json
import re
from functools import lru_cache
from pathlib import Path

PATTERNS = Path(__file__).resolve().parents[2] / "agent" / "guards" / "security_patterns.json"


@lru_cache(maxsize=1)
def patterns() -> dict:
    try:
        return json.loads(PATTERNS.read_text(encoding="utf-8"))
    except Exception:
        return {"injection_patterns": [], "injection_words_zh": [],
                "pii_columns": [], "probe_objects": []}


def detect_injection(text: str) -> str | None:
    """命中返回原因字符串，未命中返回 None。"""
    if not text:
        return None
    p = patterns()
    for pat in p.get("injection_patterns") or []:
        try:
            m = re.search(pat, text)
        except re.error:
            continue
        if m:
            return "命中注入模式：%s" % m.group(0)[:60]
    for w in p.get("injection_words_zh") or []:
        if w in text:
            return "命中注入关键词：%s" % w
    return None


def pii_columns() -> frozenset[str]:
    return frozenset(str(x).lower() for x in (patterns().get("pii_columns") or []))
