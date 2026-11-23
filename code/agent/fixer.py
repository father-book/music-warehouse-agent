"""C 阶段：校验失败后的修复。

分两级，顺序不能反：

  L1 机械修复 —— 照护栏给的 fix 原样改，不经过模型。
      免费、确定、零幻觉。R17（删前缀）、R18（加 is_current）都属这一级，
      而这两条恰好是最高频的错。
  L2 模型重写 —— 机械修不了的（restructure / 语义错），带着原 SQL 和
      逐条 REJECT 证据让模型重写。

为什么先机械后模型：一次 LLM 调用约 2k token / 3 秒，而 R18 这种错
护栏已经把"加 dim_user.is_current = 'Y'"写在 fix 里了，让模型再想一遍
纯属浪费，还可能把对的写错。sqlsure 的做法也是"照 fix 原样改"。
"""
from __future__ import annotations

import _bootstrap  # noqa: F401

import re

import guardtool

# 机械修复只认这两类 op。restructure 要改结构、apply_hint 是句人话，都不适合文本替换。
_MECHANICAL_OPS = {"replace", "add_condition"}

_RE_CJK = re.compile(r"[\u4e00-\u9fff]")
_RE_WHERE = re.compile(r"\bWHERE\b", re.I)
_RE_TAIL = re.compile(r"\b(GROUP\s+BY|HAVING|ORDER\s+BY|LIMIT|WINDOW)\b", re.I)
_RE_TABLE_ALIAS = re.compile(
    r"\b(?:FROM|JOIN)\s+([A-Za-z_]\w*)(?:\s+(?:AS\s+)?([A-Za-z_]\w*))?", re.I)

# FROM 后面这些词不是别名，是子句开头
_NOT_ALIAS = {
    "WHERE", "GROUP", "ORDER", "LIMIT", "HAVING", "JOIN", "LEFT", "RIGHT",
    "INNER", "OUTER", "FULL", "CROSS", "ON", "UNION", "WINDOW", "AS", "USING",
}


def alias_map(sql: str) -> dict[str, str]:
    """表名 → SQL 里实际用的别名。

    护栏的 fix 写的是逻辑表名（`dim_user.is_current = 'Y'`），但 SQL 若写成
    `JOIN dim_user u`，Doris 会把原表名隐藏掉，`dim_user.is_current` 直接报
    Unknown column。这一步把条件里的表名换回别名。
    """
    m: dict[str, str] = {}
    for tbl, alias in _RE_TABLE_ALIAS.findall(sql):
        t = tbl.lower()
        if alias and alias.upper() not in _NOT_ALIAS:
            m[t] = alias
        else:
            m.setdefault(t, tbl)
    return m


def rewrite_alias(cond: str, sql: str) -> str:
    """`dim_user.is_current = 'Y'` → `u.is_current = 'Y'`（有别名时）。"""
    amap = alias_map(sql)
    out = cond
    for table, alias in amap.items():
        if alias.lower() == table:
            continue
        out = re.sub(r"\b%s\." % re.escape(table), alias + ".", out, flags=re.I)
    return out



def _strip_db_prefix(name: str) -> str:
    """hive_catalog.music_dw.dwd_play_start → dwd_play_start。

    执行面已经 SWITCH 过，加前缀是纯冗余；而且写全名容易漏改一处就落错库。
    """
    parts = name.split(".")
    return parts[-1] if len(parts) >= 2 else name


def _top_level_pos(sql: str, pattern: re.Pattern) -> int:
    """找括号深度为 0、且不在字符串里的第一个匹配位置。找不到返回 -1。"""
    depth, i, n = 0, 0, len(sql)
    in_str = None
    while i < n:
        ch = sql[i]
        if in_str:
            if ch == in_str:
                in_str = None
            i += 1
            continue
        if ch in ("'", '"', "`"):
            in_str = ch
            i += 1
            continue
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        elif depth == 0:
            m = pattern.match(sql, i)
            if m:
                return i
        i += 1
    return -1


def _add_condition(sql: str, cond: str) -> str:
    """把条件并进顶层 WHERE；没有 WHERE 就地新建一个。"""
    pos = _top_level_pos(sql, _RE_WHERE)
    if pos >= 0:
        end = pos + len("WHERE")
        return sql[:end] + " (%s) AND" % cond + sql[end:]

    # 没有 WHERE：插在 GROUP BY / ORDER BY / LIMIT 之前，都没有就放末尾
    tail = _top_level_pos(sql, _RE_TAIL)
    if tail >= 0:
        return sql[:tail] + "WHERE (%s) " % cond + sql[tail:]
    return sql.rstrip() + " WHERE (%s)" % cond


def mechanical_fix(sql: str, findings: list[dict]) -> tuple[str, list[str]]:
    """按 fix 原样改。返回 (新 SQL, 已应用的动作说明)。

    只处理 REJECT 里 op 在 _MECHANICAL_OPS 内的；
    其余留给 L2。改完必须重跑护栏——机械修复也可能改坏，
    所以这个函数不负责"确认改对了"，只负责"试试看"。
    """
    cur = sql
    applied: list[str] = []

    for f in findings:
        if f.get("level") != "REJECT":
            continue
        fx = f.get("fix") or {}
        op = fx.get("op")
        if op not in _MECHANICAL_OPS:
            continue

        target = (fx.get("target") or "").strip()
        repl = (fx.get("replacement") or "").strip()
        if not target:
            continue

        # 一句中文说明（"SUM(分子)/SUM(分母)"）不是可执行 SQL，别往 SQL 里塞
        if _RE_CJK.search(repl):
            continue

        if op == "replace":
            # 只替换第一次出现：target 是护栏从原 SQL 里取的片段，通常只出现一次。
            # 全局替换会把子查询里同样的片段也改掉，那可能不是护栏想指的那一处。
            if target in cur:
                new = _strip_db_prefix(repl) if repl.count(".") >= 2 else repl
                cur = cur.replace(target, new, 1)
                applied.append("%s: %s → %s" % (f.get("rule"), target, new))

        elif op == "add_condition":
            # 条件里的表名要换成 SQL 实际用的别名，否则 "dim_user.is_current"
            # 在 `JOIN dim_user u` 下会报 Unknown column（实测踩过）
            cond = rewrite_alias(repl, cur)
            if cond and cond not in cur:
                cur = _add_condition(cur, cond)
                applied.append("%s: WHERE + %s" % (f.get("rule"), cond))

    return cur, applied


def try_mechanical(sql: str, findings: list[dict], cfg: dict) -> tuple[str, list[str], str, list[dict]]:
    """机械修复 + 立即复检。

    返回 (候选 SQL, 动作, 复检 verdict, 复检 findings)。
    复检没变好（仍是 reject 且没有新进展）时调用方应该丢弃候选 SQL，
    以免把"改了一半"的版本当成新基线传给模型。
    """
    cand, applied = mechanical_fix(sql, findings)
    if not applied:
        return sql, [], "reject", findings

    verdict, findings2, _ = guardtool.check(cand, cfg)
    if verdict == "reject":
        # 修完还是拒，说明只修掉了表面问题。保留改动（通常是有益的），
        # 但把新的 findings 一起交下去——模型需要知道还剩哪几条。
        return cand, applied, verdict, findings2
    return cand, applied, verdict, findings2


def is_purely_mechanical(findings: list[dict]) -> bool:
    """所有 REJECT 都带可机械执行的 fix 吗？是的话这次重试很可能不用调模型。"""
    rejects = [f for f in findings if f.get("level") == "REJECT"]
    if not rejects:
        return False
    for f in rejects:
        fx = f.get("fix") or {}
        if fx.get("op") not in _MECHANICAL_OPS:
            return False
        repl = (fx.get("replacement") or "")
        if _RE_CJK.search(repl):
            return False
    return True
