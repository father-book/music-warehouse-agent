#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Planning 自测（离线：模型调用打桩）。

覆盖：
  · 计划校验   —— 不存在的表/缺时间条件要 REJECT；候选外的表只 WARN
  · 计划注入   —— 有效计划进提示词，无效计划不进（免得把 SQL 一起带歪）
  · 与 CoT 互斥 —— 已注入独立计划时不再要 CoT 的 plan 字段
  · 澄清中断   —— 模型说"有歧义"时走 refuse，而不是猜一个口径
"""
from __future__ import annotations

import copy
import json
import sys

import _bootstrap  # noqa: F401

import conf
import node_plan
import planner
import promptlib

PASS = FAIL = 0
FAILS: list[str] = []


def expect(cond, name, extra=""):
    global PASS, FAIL
    if cond:
        PASS += 1
        print("  ok   %s" % name)
    else:
        FAIL += 1
        FAILS.append(name)
        print("  FAIL %s %s" % (name, extra))


CFG = conf.load_config()
GOOD = {"intent": "查 DAU", "metrics": ["uv"], "dimensions": [],
        "tables": ["dwd_session"], "filters": ["event_name = 'app_launch'"],
        "time_condition": "dt = '2026-11-08'", "shape": "scalar",
        "need_clarify": False}
ST = {"question": "2026-11-08 的 DAU 是多少", "lane": "offline",
      "tables": ["dwd_session"], "metrics": [], "dimensions": [],
      "time_window": {"kind": "single", "label": "2026-11-08",
                      "sql_hint": "dt = '2026-11-08'"}}


def on(**kw):
    """构造开关组合。cot 是布尔值，plan/react/fewshot/... 是 dict，两种都要能传。"""
    c = copy.deepcopy(CFG)
    c.setdefault("prompt", {})
    for k, v in kw.items():
        if k == "cot":
            c["prompt"]["cot"] = bool(v)
        else:
            c["prompt"].setdefault(k, {}).update(v if isinstance(v, dict) else {"enabled": v})
    return c


def test_validate():
    print("\n[1] 计划校验")
    expect(planner.validate_plan(GOOD, ST, CFG) == [], "合法计划零问题")
    bad = dict(GOOD, tables=["fact_nope"])
    r = planner.validate_plan(bad, ST, CFG)
    expect(any(x["rule"] == "PLAN-TABLE" and x["level"] == "REJECT" for x in r),
           "不存在的表 → REJECT", str(r))
    r = planner.validate_plan(dict(GOOD, time_condition=""), ST, CFG)
    expect(any(x["rule"] == "PLAN-TIME" and x["level"] == "REJECT" for x in r),
           "缺时间条件 → REJECT")
    # 有澄清标记时缺时间条件不算错（本来就该问）
    r = planner.validate_plan(dict(GOOD, time_condition="", need_clarify=True,
                                   clarify_question="你要哪一天？"), ST, CFG)
    expect(not any(x["rule"] == "PLAN-TIME" for x in r), "待澄清时缺时间不报错")
    r = planner.validate_plan(dict(GOOD, tables=["dws_user_daily"]), ST, CFG)
    expect(all(x["level"] == "WARN" for x in r) and r, "候选外的表只 WARN", str(r))
    r = planner.validate_plan(dict(GOOD, metrics=["no_such_metric"]), ST, CFG)
    expect(any(x["rule"] == "PLAN-METRIC" for x in r), "不存在的指标 → WARN")
    r = planner.validate_plan(dict(GOOD, metrics=["COUNT(DISTINCT user_id)"]), ST, CFG)
    expect(not any(x["rule"] == "PLAN-METRIC" for x in r), "表达式型 metric 不强校验")
    r = planner.validate_plan(dict(GOOD, shape="weird"), ST, CFG)
    expect(any(x["rule"] == "PLAN-SHAPE" for x in r), "非法 shape → WARN")
    expect(planner.validate_plan({}, ST, CFG)[0]["rule"] == "PLAN-EMPTY", "空计划 → REJECT")
    r = planner.validate_plan(dict(GOOD, need_clarify=True, clarify_question=""), ST, CFG)
    expect(any(x["rule"] == "PLAN-CLARIFY" for x in r), "要澄清却没写问什么 → WARN")


def test_inject():
    print("\n[2] 计划注入")
    st = dict(ST)
    st.update({"plan": GOOD, "plan_valid": True})
    off = promptlib.build_generate_messages(st, CFG)[-1]["content"]
    expect("查询计划" not in off, "开关关时不注入计划")
    onm = promptlib.build_generate_messages(st, on(plan=True))[-1]["content"]
    expect("查询计划（已通过规则校验" in onm, "开启后注入计划")
    expect("event_name" in onm, "计划里的过滤条件进了提示词")
    expect("不要改" in onm or "不要再改" in onm, "明确要求遵守计划")
    # 无效计划不注入
    bad = dict(ST)
    bad.update({"plan": GOOD, "plan_valid": False})
    t = promptlib.build_generate_messages(bad, on(plan=True))[-1]["content"]
    expect("查询计划" not in t, "无效计划不注入")
    # 与 CoT 互斥
    both = promptlib.build_generate_messages(st, on(plan=True, cot=True))[-1]["content"]
    expect('"plan"' not in both, "注入独立计划时不再要求 CoT 的 plan 字段")
    cot_only = promptlib.build_generate_messages(ST, on(cot=True))[-1]["content"]
    expect('"plan"' in cot_only, "只开 CoT 时仍有 plan 字段")


def test_node():
    print("\n[3] 节点行为（打桩）")
    cfg = on(plan=True)
    real = planner.make_plan
    try:
        planner.make_plan = lambda s, c: (GOOD, [], {"tokens": 500, "rejects": 0})
        out = node_plan.plan(dict(ST), cfg)
        expect(out["plan_valid"] is True, "合法计划标记为有效")
        expect(out["plan_need_clarify"] is False, "不要求澄清")
        expect(node_plan.after_plan(out) == "generate", "合法计划继续生成")
        expect("计划：可用" in out["trace"][0]["detail"], "trace 写明计划状态")

        planner.make_plan = lambda s, c: (
            dict(GOOD, need_clarify=True, clarify_question="你要看付费会员还是全部会员？"),
            [], {"tokens": 500})
        out = node_plan.plan(dict(ST), cfg)
        expect(out["plan_need_clarify"] is True, "识别出需要澄清")
        expect(out.get("refused") is True, "标记为拒答")
        expect("会员" in out.get("refuse_reason", ""), "澄清问题被带出")
        expect(node_plan.after_plan(out) == "refuse", "需要澄清时走 refuse")

        planner.make_plan = lambda s, c: ({}, [{"level": "WARN", "rule": "PLAN-CALL",
                                                "message": "超时"}], {"error": "timeout"})
        out = node_plan.plan(dict(ST), cfg)
        expect(out["plan_valid"] is False, "模型失败时计划无效")
        expect(node_plan.after_plan(out) == "generate", "计划失败不阻断，仍去生成")
    finally:
        planner.make_plan = real


def test_graph():
    print("\n[4] 图接线")
    import graph
    expect(graph.plan_enabled(CFG) is False, "默认不开 Planning")
    expect(graph.plan_enabled(on(plan=True)) is True, "开关可打开")
    graph.build_graph(CFG)
    graph.build_graph(on(plan=True))
    expect(True, "两种配置都能编译")
    # react 与 plan 同时开时，react 优先（由 after_route 的顺序保证）
    both = on(plan=True, react=True)
    expect(graph.react_enabled(both) and graph.plan_enabled(both),
           "两者可同时为真（是否走由 after_route 判）")


def test_state_declared():
    print("\n[5] 状态字段必须显式声明（LangGraph 只保留注解过的键）")
    from state import AgentState
    ann = AgentState.__annotations__
    for k in ("plan", "plan_issues", "plan_meta", "plan_valid",
              "plan_need_clarify", "react_trace", "generate_via"):
        expect(k in ann, "%s 已在 AgentState 声明" % k)


def main():
    test_validate()
    test_inject()
    test_node()
    test_graph()
    test_state_declared()
    print("\n%d 通过 / %d 失败" % (PASS, FAIL))
    if FAIL:
        print("失败项：" + ", ".join(FAILS))
    return 0 if not FAIL else 1


if __name__ == "__main__":
    sys.exit(main())
