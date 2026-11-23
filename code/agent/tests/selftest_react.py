#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Function Calling / ReAct 自测（离线：模型调用全部打桩）。

覆盖四类真会坏的地方：
  · 工具执行   —— 四个工具都能跑；未知工具/异常都不能把问数带崩
  · 参数解析   —— 模型给的 arguments 是字符串，可能带围栏或残缺
  · ReAct 循环 —— submit 终止、步数用尽回落、重复调用被拦、无工具直接出答案
  · 图接线     —— 默认不走 react；开了才改道，且 stderr 兜底到 refuse
"""
from __future__ import annotations

import copy
import json
import sys

import _bootstrap  # noqa: F401

import conf
import graph
import node_react
import tools

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
Q = "2026-11-08 的 DAU 是多少"
GOOD_SQL = ("SELECT COUNT(DISTINCT COALESCE(NULLIF(user_id, ''), device_id)) AS uv "
            "FROM dwd_session WHERE dt = '2026-11-08' AND event_name = 'app_launch'")


def test_tools():
    print("\n[1] 工具执行")
    t, done = tools.execute("search_semantic", {"query": "各省份播放量", "kind": "table"}, CFG)
    expect("dws_province_daily" in t or "dwd" in t, "检索表返回具体表名", t[:60])
    expect(not done, "检索不终止循环")
    t, _ = tools.execute("search_semantic", {"query": "活跃用户", "kind": "metric"}, CFG)
    expect("uv" in t, "检索指标命中 uv", t[:60])
    t, _ = tools.execute("get_metric_def", {"code": "uv"}, CFG)
    expect("COUNT(DISTINCT" in t, "指标口径返回公式")
    expect("dwd_session" in t, "指标口径返回来源表")
    t, _ = tools.execute("get_metric_def", {"code": "dau"}, CFG)
    expect("uv" in t, "写错指标码时给近似候选", t[:60])

    t, _ = tools.execute("validate_sql", {"sql": "SELECT * FROM dwd_session"}, CFG)
    expect("R9" in t, "SELECT * 被护栏拦下")
    expect("改法" in t, "拦截附带改法")
    t, _ = tools.execute("validate_sql", {"sql": GOOD_SQL}, CFG)
    expect(isinstance(t, str) and len(t) > 0, "合规 SQL 有明确结论", t[:50])

    _, done = tools.execute("submit_sql", {"sql": GOOD_SQL, "reason": "uv 口径"}, CFG)
    expect(done is True, "submit_sql 是终止信号")

    t, _ = tools.execute("no_such_tool", {}, CFG)
    expect("未知工具" in t, "未知工具返回可读错误", t[:40])
    t, _ = tools.execute("search_semantic", {}, CFG)
    expect("不能为空" in t or "没有匹配" in t, "缺参数不抛异常", t[:40])
    # 工具内部异常必须变成文本，不能抛出去
    real = tools._search_semantic
    try:
        tools._search_semantic = lambda a, c: (_ for _ in ()).throw(RuntimeError("boom"))
        t, done = tools.execute("search_semantic", {"query": "x", "kind": "table"}, CFG)
        expect("执行异常" in t and not done, "工具异常转文本返回", t[:50])
    finally:
        tools._search_semantic = real


def test_parse():
    print("\n[2] 工具参数解析")
    expect(tools.parse_args('{"a": 1}') == {"a": 1}, "纯 JSON")
    expect(tools.parse_args('```json\n{"b": 2}\n```') == {"b": 2}, "带代码围栏")
    expect(tools.parse_args("坏掉的") == {}, "无法解析时给空对象而非抛异常")
    expect(tools.parse_args("") == {}, "空串给空对象")
    expect(tools.parse_args({"x": 1}) == {"x": 1}, "已是 dict 原样返回")


def _stub_chat(script: list[dict], calls: list):
    """按脚本回放模型响应。script 里每项是 {"tool": name, "args": {...}} 或 {"text": ...}。"""
    it = iter(script)

    def fake(messages, cfg, **kw):
        calls.append({"n": len(messages), "kw": kw})
        step = next(it, {"text": ""})
        if "tool" in step:
            return {
                "content": "",
                "tool_calls": [{
                    "id": "call_%d" % len(calls),
                    "type": "function",
                    "function": {"name": step["tool"],
                                 "arguments": json.dumps(step.get("args") or {},
                                                         ensure_ascii=False)},
                }],
                "usage": {"prompt_tokens": 100, "completion_tokens": 20},
                "model": "stub", "elapsed_ms": 1, "cost_cny": 0.001,
                "ledger_ok": True, "tokens_in": 100, "tokens_out": 20,
            }
        return {"content": step.get("text", ""), "tool_calls": [],
                "usage": {"prompt_tokens": 100, "completion_tokens": 20},
                "model": "stub", "elapsed_ms": 1, "cost_cny": 0.001,
                "ledger_ok": True, "tokens_in": 100, "tokens_out": 20}
    return fake


def test_react():
    print("\n[3] ReAct 循环（打桩）")
    cfg = copy.deepcopy(CFG)
    cfg["prompt"]["react"]["enabled"] = True
    st = {"question": Q, "lane": "offline", "tables": ["dwd_session"], "metrics": [],
          "time_window": {"kind": "single", "label": "2026-11-08",
                          "sql_hint": "dt = '2026-11-08'"}}
    calls: list = []
    real = node_react.gateway.chat
    try:
        node_react.gateway.chat = _stub_chat([
            {"tool": "search_semantic", "args": {"query": "活跃用户", "kind": "metric"}},
            {"tool": "validate_sql", "args": {"sql": GOOD_SQL}},
            {"tool": "submit_sql", "args": {"sql": GOOD_SQL, "reason": "uv"}},
        ], calls)
        out = node_react.react(st, cfg)
        expect(out["generate_via"] == "react", "走的是 react 路径", out["generate_via"])
        expect("app_launch" in out["sql"], "拿回的是提交的 SQL", out["sql"][:50])
        expect(len(calls) == 3, "恰好 3 轮（submit 后立即停）", str(len(calls)))
        acts = [t.get("act") for t in out["react_trace"]]
        expect(acts == ["search_semantic", "validate_sql", "submit_sql"],
               "trace 按序记下每次动作", str(acts))
        expect(out["generate_meta"]["tool_calls"] == 3, "meta 记下工具调用数")

        # 不调 submit，直接甩文本 → 也要能捞出 SQL
        calls.clear()
        node_react.gateway.chat = _stub_chat([
            {"text": "```sql\n%s\n```" % GOOD_SQL}], calls)
        out = node_react.react(st, cfg)
        expect("app_launch" in out["sql"], "无 submit 时从文本捞 SQL", out["sql"][:50])

        # 只聊天不给 SQL → 回落一次成型（这里回落也会失败，因为打桩没内容）
        calls.clear()
        node_react.gateway.chat = _stub_chat([{"text": "我不确定"}], calls)
        out = node_react.react(st, cfg)
        expect(out["generate_via"] == "react-fallback-generate",
               "没产出时标记为回落", out["generate_via"])

        # 重复调用同一工具同一参数 → 被拦，不重复执行
        calls.clear()
        node_react.gateway.chat = _stub_chat([
            {"tool": "search_semantic", "args": {"query": "活跃用户", "kind": "metric"}},
            {"tool": "search_semantic", "args": {"query": "活跃用户", "kind": "metric"}},
            {"tool": "submit_sql", "args": {"sql": GOOD_SQL, "reason": "ok"}},
        ], calls)
        out = node_react.react(st, cfg)
        obs = [t.get("obs") or "" for t in out["react_trace"]]
        expect(any("已经用同样的参数调过" in o for o in obs),
               "重复调用被识别并提示", str(obs)[:120])

        # 步数用尽 → 必须收敛，不能无限循环。
        # 上限是 max_steps + 2 而不是 +1：回落走的是 generate_with_llm，
        # 而 P1-1 之后它内部对"不合规输出"会带错因重试一次（这条桩每次回的都是
        # 工具调用，不是 SQLDraft，所以两次都不合规）。多花的这 1 次是契约层的
        # 固定行为，不是循环失控——只要它是**常数**上限就达到了这条用例的目的。
        cfg2 = copy.deepcopy(cfg)
        cfg2["prompt"]["react"]["max_steps"] = 2
        calls.clear()
        node_react.gateway.chat = _stub_chat([
            {"tool": "search_semantic", "args": {"query": "a", "kind": "table"}},
            {"tool": "get_metric_def", "args": {"code": "uv"}},
            {"tool": "get_metric_def", "args": {"code": "pv"}},
        ], calls)
        out = node_react.react(st, cfg2)
        expect(len(calls) <= 4, "max_steps 生效，回落最多多调 1 次 + 契约重试 1 次",
               "实际 %d" % len(calls))
        expect(len(calls) >= 2, "步数确实跑完了（不是在第一步就退出）",
               "实际 %d" % len(calls))
    finally:
        node_react.gateway.chat = real


def test_graph_wiring():
    print("\n[4] 图接线")
    expect(graph.react_enabled(CFG) is False, "默认不走 react")
    on = copy.deepcopy(CFG)
    on["prompt"]["react"]["enabled"] = True
    expect(graph.react_enabled(on) is True, "开关可打开")
    # 两种配置都要能编译（编译失败等于整个服务起不来）
    graph.build_graph(CFG)
    graph.build_graph(on)
    expect(True, "两种配置都能编译")
    # 有 SQL 时（调试模式）不能绕进 react
    expect(node_react.after_react({"sql": "SELECT 1"}) == "validate", "有 SQL 去校验")
    expect(node_react.after_react({"sql": ""}) == "refuse", "无 SQL 去拒答")
    # stub 模式下即使开了 react 也不该走（stub 不调工具）
    stub = copy.deepcopy(on)
    stub["generator"]["mode"] = "stub"
    expect(graph.react_enabled(stub) is True, "开关本身为真（是否走由 after_route 判）")


def main():
    test_tools()
    test_parse()
    test_react()
    test_graph_wiring()
    print("\n%d 通过 / %d 失败" % (PASS, FAIL))
    if FAIL:
        print("失败项：" + ", ".join(FAILS))
    return 0 if not FAIL else 1


if __name__ == "__main__":
    sys.exit(main())
