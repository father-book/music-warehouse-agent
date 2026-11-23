"""validate 节点：调 20 条护栏。

护栏不在这里重新实现——subprocess 调 agent/guards/guard.py，
所以 Agent 的判断和 CI 里的回归用例永远是同一套规则。
"""
from __future__ import annotations

import _bootstrap  # noqa: F401

import re

import guardtool
import costgate
from state import AgentState

_RE_DATE_LIT = re.compile(r"'(\d{4}-\d{2}-\d{2})'")


def _compare_violation(state: AgentState) -> dict | None:
    """对比题里 SQL 引用了基线窗口 → 打回重写。

    为什么这条必须在执行前拦：如果模型自己把两个窗口都写进 SQL 算出差值，
    `_compare` 的「平移日期复算基线」就会失效——它只平移落在当前窗口内的
    字面量，基线那个会被留下，算出来的基线值毫无意义。
    更糟的是结果看起来是正常数字，没人会发现。

    这不是安全边界，是可修复的生成问题，所以用 REJECT + 明确 fix 走修复闭环。
    """
    cmp = (state.get("time_window") or {}).get("compare") or {}
    if not cmp.get("auto"):
        return None
    cur = {str(state.get("time_start")), str(state.get("time_end"))}
    base = {str(cmp.get("start")), str(cmp.get("end"))} - cur
    if not base:
        return None
    hits = [d for d in _RE_DATE_LIT.findall(state.get("sql") or "") if d in base]
    if not hits:
        return None
    return {
        "level": "REJECT", "rule": "COMPARE-BASE",
        "message": ("这是对比问题，但 SQL 里出现了基线日期 %s。基线与差值由系统"
                    "另行复算：SQL 只查当前窗口（%s）一个值即可。"
                    % ("、".join(sorted(set(hits))),
                       " ~ ".join(sorted(cur - {"None"})) or "当前窗口")),
        "hint": "删掉 SQL 里的基线日期与第二条子查询，只保留当前窗口的聚合",
        "fix": {"op": "replace", "target": "、".join(sorted(set(hits))),
                "replacement": "当前窗口（%s）" % (state.get("time_start") or "")},
    }


def _audit_sql(sql: str) -> str:
    """落审计前脱敏。

    被拦下的 SQL 里可能有手机号字面量（"查 13800138000 的用户"），而 qa_logs
    是不可变表——写进去就擦不掉。脱敏函数复用写入侧那一份（governance.pii），
    这里不重新实现规则；拿不到它就返回空：宁可少一个字段，也不落未脱敏原文。
    """
    if not sql:
        return ""
    try:
        from governance.pii import redact
        return redact(sql)[0]
    except Exception:
        return ""


def validate(state: AgentState, cfg: dict) -> dict:
    if state.get("unsupported"):
        return {
            "verdict": "reject",
            "findings": [],
            "attempts": state.get("attempts", 0),
            "trace": [{"node": "validate", "detail": "跳过：需求本身不可答", "data": {}}],
        }

    sql = state.get("sql") or ""
    verdict, findings, stderr = guardtool.check(
        sql, cfg,
        context=state.get("ctx"),
        decision=state.get("policy_decision"),
        question=state.get("question") or "",
    )

    # 对比题：SQL 必须只碰当前窗口。判定放在护栏之后，因为它和护栏无关，
    # 但一样要能打回重写——这里复用 findings 通道，模型看到的格式是一致的。
    vio = _compare_violation(state)
    if vio:
        findings = list(findings) + [vio]
        if verdict != "reject":
            verdict = "reject"

    # 成本闸（AC-17）：护栏之后、执行之前。
    # 放在护栏之后，是因为护栏挡的"没写 dt"更便宜也更该先报；
    # 只有形状上过了护栏的 SQL 才值得花 30~50ms 去 EXPLAIN。
    gate: dict = {"status": "skipped", "reasons": [], "nodes": [], "rows": 0, "bytes": 0}
    if verdict != "reject":
        try:
            gate = costgate.check(sql, cfg)
        except Exception as e:                 # 闸自身故障不能瘫掉问答
            gate = {"status": "unknown", "reasons": ["成本闸异常：%s" % e],
                    "nodes": [], "rows": 0, "bytes": 0}
        if gate.get("status") == "blocked":
            findings = list(findings) + [costgate.as_finding(gate)]
            verdict = "reject"
            cost_block = True
        else:
            cost_block = False
    else:
        cost_block = False

    # 不可重试的硬拦截：身份/安全边界（R21~R23），以及任何 fix.op == "deny"
    # 的规则（R1 只读、R2 多语句）。后者用 op 判定而不是继续写死规则号——
    # 写死过一次的代价是：新增一条 deny 类规则时忘了改这里，它就会静默变成可修，
    # 模型改一改就绕过去了，而外面看起来一切正常。
    def _hard_block(f: dict) -> bool:
        if f.get("level") != "REJECT":
            return False
        if str(f.get("rule") or "").startswith(("R21", "R22", "R23")):
            return True
        fix = f.get("fix") or {}
        return str(fix.get("op") or "") == "deny"

    security_block = any(_hard_block(f) for f in findings)

    n_reject = sum(1 for f in findings if f.get("level") == "REJECT")
    n_warn = sum(1 for f in findings if f.get("level") == "WARN")
    if stderr.strip() and not findings:
        detail = "护栏有 stderr 输出：%s" % stderr.strip()[-200:]
    else:
        detail = "%s（REJECT %d / WARN %d）" % (verdict, n_reject, n_warn)

    attempt = state.get("attempts", 0) + 1
    return {
        "verdict": verdict,
        "findings": findings,
        "security_block": security_block,
        # 成本闸不是"模型写错了"，是"这条计划太贵"：不重试（重试等于再赌一次扫描），
        # 直接按拒答出口，把 EXPLAIN 算出来的数字讲清楚。
        "cost_block": cost_block,
        "cost_gate": {k: v for k, v in gate.items() if k != "nodes"} | {
            "scan_nodes": [{kk: n[kk] for kk in ("table", "part_scanned",
                                                 "part_total", "rows", "bytes")}
                           for n in (gate.get("nodes") or [])]},        "attempts": attempt,
        # 记一条时间线：第几轮、判成什么、卡在哪几条。narrate 的"修正过程"从这里取。
        # `sql` 一并落下来：审计里只记"被 R9 拦了"而没有原文，事后既复现不了，
        # 也没法进失败案例库（failcase.py 要拿它当"错误写法"的实证）。
        "history": [{"attempt": attempt, "stage": "check", "sql": _audit_sql(sql),
                     "verdict": verdict,
                     "rejects": [f.get("rule") for f in findings
                                 if f.get("level") == "REJECT"]}],
        "trace": [{"node": "validate", "detail": detail,
                   "data": {"findings": findings, "fixes": guardtool.fix_hints(findings),
                            "cost_gate": {k: v for k, v in gate.items()
                                          if k not in ("nodes", "message")}}}],
    }
