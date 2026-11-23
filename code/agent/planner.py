"""Planning：把"想清楚"拆成独立且可校验的一步。

和 prompt.cot 的区别（两者别混着用）
----------------------------------
cot      在同一次调用里让模型先填 plan 字段再填 sql。便宜，但 plan 只是文本，
         程序没法在生成 SQL 之前拦住一个错计划——等发现不对，SQL 已经写完了。
planning 先单独产出一份 JSON 计划，**用规则校验它**（指标存在吗、表在候选里吗、
         时间条件有没有），过了才让它写 SQL。多一次调用，换来的是：
           · 计划错 → 只重做计划，不用重写 SQL
           · 可直接统计"计划合法率"，这是可交付的指标
           · 口径写进计划后，SQL 阶段不再有解释空间

校验只做确定性检查，不调模型。查不出来的问题（比如"该不该排除游客"）仍交给
护栏和 critique——Planning 不是万能药，它只把"能机械判的错"提前到写 SQL 之前。
"""
from __future__ import annotations

import _bootstrap  # noqa: F401

import json

import gateway
import llm
import promptlib
import schemas
import semantic
from pydantic import ValidationError

SHAPES = {"scalar", "detail", "ranking", "comparison", "ratio", "trend", "distribution"}

PLAN_SPEC = """\
【输出格式】
严格输出一个 JSON 对象，不要任何额外文字。这一步**只做计划，不写 SQL**。
{
  "intent": "一句话说明用户要什么",
  "metrics": ["用到的指标码，必须在语义层里存在"],
  "dimensions": ["分组维度，没有就空数组"],
  "tables": ["计划使用的表，必须来自上面给出的候选表"],
  "filters": ["除时间外的过滤条件，没有就空数组"],
  "time_condition": "时间过滤的写法，例如 dt = '2026-11-08'",
  "shape": "scalar | detail | ranking | comparison | ratio | trend | distribution",
  "need_clarify": false,
  "clarify_question": "need_clarify 为 true 时写一句要问用户的话"
}
"""

SYSTEM = """\
你是音乐 App 数仓的查询规划器。给你一个业务问题和语义层上下文，你输出一份
可被程序校验的查询计划。**不要写 SQL**——SQL 是下一步的事。

判断要点：
1. 指标码只能用语义层里真实存在的（uv / net_revenue / arpu …），不要自己造。
2. 表只能从候选表里选；如果候选里没有合适的，把 need_clarify 置 true。
3. 时间条件必须具体到 SQL 能用的形式。
4. 用户的话有歧义（口径二义、时间不明）时，need_clarify 置 true 并写出要问什么，
   不要自己替他选一个。
"""


def build_messages(state: dict, cfg: dict) -> list[dict]:
    user = ["【业务问题】", state.get("question") or "", ""]
    dlg = promptlib.dialogue_block(state)
    if dlg:
        user.append(dlg)
        user.append("")
    user.append(promptlib.context_block(state, cfg))
    user.append("")
    user.append(PLAN_SPEC)
    return [{"role": "system", "content": SYSTEM},
            {"role": "user", "content": "\n".join(user)}]


def validate_plan(plan: dict, state: dict, cfg: dict) -> list[dict]:
    """返回问题列表。level=REJECT 表示计划不可用，WARN 表示可疑但可继续。"""
    out: list[dict] = []
    if not isinstance(plan, dict) or not plan:
        return [{"level": "REJECT", "rule": "PLAN-EMPTY", "message": "没有产出计划"}]

    tables = semantic.all_tables(cfg)
    metrics = semantic.all_metrics(cfg)
    cand = set(state.get("tables") or [])

    shape = str(plan.get("shape") or "")
    if shape and shape not in SHAPES:
        out.append({"level": "WARN", "rule": "PLAN-SHAPE",
                    "message": "计划里的 shape=%s 不是约定值" % shape})

    for code in (plan.get("metrics") or [])[:8]:
        code = str(code)
        # 指标码也可能是列名/表达式，只有"看起来像指标码"的才强校验
        if code in metrics:
            continue
        if any(ch in code for ch in "()* /") or code.isupper():
            continue
        out.append({"level": "WARN", "rule": "PLAN-METRIC",
                    "message": "计划引用了语义层不存在的指标 %s" % code})

    for t in (plan.get("tables") or [])[:8]:
        t = str(t).split(".")[-1]
        if t not in tables:
            out.append({"level": "REJECT", "rule": "PLAN-TABLE",
                        "message": "计划用了不存在的表 %s" % t})
        elif cand and t not in cand:
            # 候选表是检索出来的，计划跳出候选说明检索漏了或者计划跑偏。
            # 只警告：语义层指标反查出来的表也可能不在候选里，那是对的。
            out.append({"level": "WARN", "rule": "PLAN-OFFCAND",
                        "message": "计划用了候选外的表 %s" % t})

    if not str(plan.get("time_condition") or "").strip() and not plan.get("need_clarify"):
        out.append({"level": "REJECT", "rule": "PLAN-TIME",
                    "message": "计划缺时间条件，且没有标记需要澄清"})

    if plan.get("need_clarify") and not str(plan.get("clarify_question") or "").strip():
        out.append({"level": "WARN", "rule": "PLAN-CLARIFY",
                    "message": "标记了需要澄清但没写要问什么"})
    return out


def make_plan(state: dict, cfg: dict) -> tuple[dict, list[dict], dict]:
    """返回 (plan, 问题列表, meta)。模型失败时 plan 为空、meta 带 error。"""
    messages = build_messages(state, cfg)
    est = promptlib.estimate_tokens(messages)
    try:
        out = gateway.chat(messages, cfg, json_mode=True, caller="plan",
                           temperature=0.0, ctx=state.get("ctx"))
    except llm.LLMError as e:
        return {}, [{"level": "WARN", "rule": "PLAN-CALL", "message": str(e)[:180]}], {
            "error": str(e), "est_in": est}
    try:
        plan = schemas.parse_query_plan(out["content"]).model_dump()
    except (schemas.SchemaError, ValidationError) as e:
        # 不合规就降级（不带计划照常生成），不是拒答：Planning 是增强，
        # 不能变成新的单点故障。规则名从 PLAN-PARSE 改成 PLAN-SCHEMA，
        # 是因为现在拦下的不再只是"解析不了"，还有"解析出来但字段不合合同"。
        return {}, [{"level": "WARN", "rule": "PLAN-SCHEMA",
                     "message": "计划不合规：%s" % "；".join(schemas.brief_errors(e))}], {
            "error": "schema", "raw": (out.get("content") or "")[:300], "est_in": est}
    issues = validate_plan(plan, state, cfg)
    usage = out.get("usage") or {}
    meta = {
        "tokens": usage.get("total_tokens"),
        "prompt_tokens": usage.get("prompt_tokens"),
        "cost_cny": out.get("cost_cny"),
        "est_in": est,
        "issues": len(issues),
        "rejects": sum(1 for i in issues if i.get("level") == "REJECT"),
    }
    return plan, issues, meta
