"""ReAct 节点：Thought → Action → Observation 循环，模型自己决定查什么。

和现有 generate 节点的关系
------------------------
**不是替代，是可选前置**。generate 是"一次成型"：给足上下文，模型一把写出 SQL。
ReAct 是"边查边写"：模型先看有哪些表、口径是什么、这版过不过护栏，再提交。

什么时候 ReAct 更强：问题里的表名/指标名和语义层叫法对不上（"游客算不算用户"），
一次成型的模型只能猜；ReAct 可以先去 search_semantic 看真实的表名和 caveats。

什么时候更差：简单单表指标（占评测集一半以上），多绕 2~3 轮只是多花钱。
所以默认关，由 eval/react_ab.py 用同一批判据决定开不开。

失败兜底
--------
ReAct 任何环节出问题（工具炸了、步数用尽、模型只聊天不调工具），一律回落到
普通 generate。宁可退化成原来的行为，也不能因为"新功能没跑通"让用户拿不到答案。
"""
from __future__ import annotations

import _bootstrap  # noqa: F401

import json

import gateway
import llm
import node_generate_llm as gen
import promptlib
import tools
from state import AgentState

REACT_SYSTEM = """\
你是音乐 App 数仓的问数助手。你有一组工具可以调用，用它们把问题查清楚，
最后用 submit_sql 提交一条能在 Doris 上直接执行的 SQL。

工作方式：
1. 不确定用哪张表 → search_semantic(kind=table)
2. 不确定指标口径 → get_metric_def，或 search_semantic(kind=metric)
3. 写完 SQL 一定要先 validate_sql，没过就按它给的改法重写
4. 确认通过后再 submit_sql

铁律（违反必被护栏拦下）：
{rules}

注意：你没有执行 SQL 的工具。不要假装看到了查询结果，也不要根据"假设的数据"
反推 SQL。你只能依据语义层信息和护栏反馈来决定怎么写。
"""


def _messages(state: dict, cfg: dict) -> list[dict]:
    ctx = promptlib.context_block(state, cfg)
    user = ["【业务问题】", state.get("question") or "", ""]
    dlg = promptlib.dialogue_block(state)
    if dlg:
        user.append(dlg)
        user.append("")
    user.append(ctx)
    return [
        {"role": "system", "content": REACT_SYSTEM.format(
            rules=promptlib.RT_RULES if state.get("lane") == "realtime"
            else promptlib.RULES)},
        {"role": "user", "content": "\n".join(user)},
    ]


def _sql_from_text(text: str) -> str:
    """模型有时不调 submit_sql，直接甩一段 SQL 文本。尽量捞出来。"""
    s = (text or "").strip()
    if not s:
        return ""
    if "```" in s:
        parts = s.split("```")
        for i in range(1, len(parts), 2):
            blk = parts[i]
            if blk.lstrip().lower().startswith("sql"):
                blk = blk.split("\n", 1)[-1]
            if blk.strip():
                return gen.clean_sql(blk)
    low = s.lower()
    i = low.find("select")
    j = low.find("with")
    start = min([x for x in (i, j) if x >= 0], default=-1)
    if start < 0:
        return ""
    return gen.clean_sql(s[start:])


def react(state: AgentState, cfg: dict) -> dict:
    rc = (cfg.get("prompt") or {}).get("react") or {}
    max_steps = max(1, int(rc.get("max_steps", 4)))
    max_tools = max(1, int(rc.get("max_tool_calls", 8)))
    question = state.get("question") or ""

    messages = _messages(state, cfg)
    trace: list[dict] = []
    seen: dict[str, int] = {}          # "工具+参数" 去重，防止模型原地打转
    submitted = ""
    reason = ""
    n_tool = 0
    cost = 0.0
    tok = 0
    tin = 0
    tout = 0

    for step in range(1, max_steps + 1):
        try:
            out = gateway.chat(messages, cfg, caller="react", ctx=state.get("ctx"),
                               tools=tools.SPECS, tool_choice="auto",
                               temperature=0.2)
        except llm.LLMError as e:
            trace.append({"step": step, "error": str(e)[:200]})
            break
        cost += float(out.get("cost_cny") or 0.0)
        tin += int(out.get("tokens_in") or 0)
        tout += int(out.get("tokens_out") or 0)
        tok += int(out.get("tokens_in") or 0) + int(out.get("tokens_out") or 0)

        calls = out.get("tool_calls") or []
        if not calls:
            # 没调工具 = 这轮就是它的最终产出
            text = _sql_from_text(out.get("content") or "")
            trace.append({"step": step, "act": "answer", "chars": len(text)})
            if text and not submitted:
                submitted = text
            break

        # 把 assistant 这一轮（含 tool_calls）原样带回历史，否则模型下一轮
        # 看不到自己调过什么，会重复调同一个工具。
        messages.append({"role": "assistant", "content": out.get("content") or "",
                         "tool_calls": calls})

        for tc in calls[:3]:                      # 一轮最多执行 3 个，防上下文爆炸
            fn = tc.get("function") or {}
            name = str(fn.get("name") or "")
            args = tools.parse_args(fn.get("arguments"))
            key = name + "|" + json.dumps(args, ensure_ascii=False, sort_keys=True)
            if seen.get(key):
                obs = "你已经用同样的参数调过 %s 了，换一个查法或直接写 SQL。" % name
            else:
                seen[key] = 1
                n_tool += 1
                obs, done = tools.execute(name, args, cfg, question)
                if done:
                    submitted = str(args.get("sql") or "").strip()
                    reason = str(args.get("reason") or "").strip()
            trace.append({
                "step": step, "act": name,
                "args": {k: (str(v)[:120]) for k, v in args.items()},
                "obs": obs[:400],
            })
            messages.append({"role": "tool", "tool_call_id": tc.get("id") or "",
                             "content": obs})
            if submitted:
                break
            if n_tool >= max_tools:
                break
        if submitted or n_tool >= max_tools:
            break

    # 【2026-11-14】token 必须按 in/out 两个键写出来。
    # 原来只写了 {"tokens": tok}，而 node_generate.py 读的是 meta["prompt_tokens"]，
    # 结果 react 档在评测报表里恒显示 "0+0 tok" —— 看上去像"react 不要钱"，
    # 实际它是最贵的一档（多轮 + 工具结果回灌）。
    meta = {"cost_cny": round(cost, 6), "tokens": tok,
            "prompt_tokens": tin, "completion_tokens": tout,
            "tokens_in": tin, "tokens_out": tout,
            "steps": len(trace), "tool_calls": n_tool,
            "via": "react", "react_trace": trace}

    if submitted:
        sql = gen.clean_sql(submitted)
        detail = "react：%d 步 / %d 次工具调用 ｜ %d tok ｜ %s" % (
            len(trace), n_tool, tok, (reason or "-")[:60])
        # 【2026-11-14】trace 事件必须带 tokens_in/out：
        # 评测（eval/run.py）和成本台账（AuditEvent.from_state）都是按
        # trace[*].data.tokens_in 累加的。react 原来只返回 generate_meta，
        # 既没 trace 也没有这两个键，于是这一档在报表里恒为 "0+0 tok"、
        # 在台账里成本恒为 0 —— 最贵的一档看起来最便宜。
        return {"sql": sql, "generate_detail": detail, "generate_meta": meta,
                "react_trace": trace, "generate_via": "react",
                "trace": [{"node": "react", "detail": detail,
                           "data": {"sql": sql, "meta": meta,
                                    "tokens_in": tin, "tokens_out": tout}}]}

    # 兜底：ReAct 没交出 SQL，回落到一次成型。退化但可用。
    sql, detail, gmeta = gen.generate_with_llm(state, cfg)
    meta.update(gmeta or {})
    meta["via"] = "react-fallback-generate"
    meta["react_trace"] = trace
    tin += int((gmeta or {}).get("prompt_tokens") or 0)
    tout += int((gmeta or {}).get("completion_tokens") or 0)
    meta.update({"tokens_in": tin, "tokens_out": tout, "tokens": tin + tout})
    return {"sql": sql, "generate_detail": "react 未产出，回落：" + detail,
            "generate_meta": meta, "react_trace": trace,
            "generate_via": "react-fallback-generate",
            "trace": [{"node": "react", "detail": "react 未产出，回落：" + detail,
                       "data": {"sql": sql, "meta": meta,
                                "tokens_in": tin, "tokens_out": tout}}]}


def after_react(state: AgentState) -> str:
    """与 generate 之后的路由一致：有 SQL 就去校验，没有就拒答。"""
    return "validate" if (state.get("sql") or "").strip() else "refuse"
