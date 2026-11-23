"""generate 节点：需求 → SQL，以及失败后的修复。

三种路径，由 config.generator.mode 和当前轮次决定：

  stub          A 阶段。返回固定 SQL，唯一目的是把管道打通。
                刻意不接模型：管道自身的 bug 和模型的 bug 混在一起时，分不清谁错。
  llm（首轮）    B 阶段。按语义层给的落点和口径生成。
  llm（重试）    C 阶段。先机械照 fix 改，改不好再让模型带着证据重写。

三条路径返回同一个结构，图不用改。
"""
from __future__ import annotations

import _bootstrap  # noqa: F401

import fixer
from state import AgentState

# A 阶段的靶子：单表、带分区裁剪、能对上 docs/32 的基线
_STUB = """SELECT dau, session_cnt, play_start_cnt, avg_play_per_user, vip_user_cnt
FROM dws_user_daily
WHERE dt = '{dt}'"""


def _rejects(findings: list[dict]) -> list[dict]:
    return [f for f in (findings or []) if f.get("level") == "REJECT"]


def _brief(findings: list[dict]) -> list[str]:
    return ["%s: %s" % (f.get("rule"), (f.get("message") or "")[:80])
            for f in _rejects(findings)]


def _tok(meta: dict) -> dict:
    """把生成节点的 usage 摊到 trace.data 的**顶层**。

    审计侧（`AuditEvent.from_state`）在 data 这一层累加 tokens_in/out，
    而 usage 原本埋在 `data.meta.prompt_tokens` 里——键名对不上，
    导致成本台账一路恒为 0。这里把两个键显式提到顶层，
    元数据仍留在 meta 里给 trace 面板看，两边都不丢。
    """
    return {"tokens_in": int((meta or {}).get("prompt_tokens") or 0),
            "tokens_out": int((meta or {}).get("completion_tokens") or 0)}


def _first(state: AgentState, cfg: dict) -> dict:
    """首轮生成。"""
    from node_generate_llm import generate_with_llm

    sql, detail, meta = generate_with_llm(state, cfg)
    return {
        "sql": sql,
        "generate_error": "" if sql else detail,
        # 输出不合规时 _call_once 内部已经带着错因重试过一次；打上标记让
        # generate() 别再走图层的第二次尝试，否则同一条坏输出要花 4 次调用。
        "generate_schema_failed": bool((meta or {}).get("schema_error")),
        # 标明这条 SQL 出自哪条路径（generate / react / react-fallback-generate）。
        # 有了它，"开了 ReAct 到底走没走"不用翻日志，state 里直接看得到。
        "generate_via": "generate",
        "history": [{"attempt": 1, "stage": "generate", "sql": sql,
                     "detail": detail, "meta": meta}],
        "trace": [{"node": "generate", "detail": detail,
                   "data": {"sql": sql, "meta": meta, **_tok(meta)}}],
    }


def _repair(state: AgentState, cfg: dict) -> dict:
    """重试轮：执行错误直接交给模型，护栏错误先机械修复再交给模型。"""
    prev_sql = state.get("sql") or ""
    findings = state.get("findings") or []
    n = state.get("attempts", 0) + 1

    # ---- 执行失败（SQL 过了护栏但 Doris 不认）----
    # 机械修复只认护栏的 fix，对 Unknown column / 类型错毫无办法，直接走 L2。
    # 把库返回的错误当成一条 REJECT 证据，模型才知道上一版错在哪。
    exec_err = (state.get("result") or {}).get("error")
    if exec_err:
        from node_generate_llm import generate_with_llm
        ev = [{"level": "REJECT", "rule": "EXEC-ERROR",
               "message": str(exec_err)[:300], "fix": {"op": "rewrite"}}]
        sql, detail, meta = generate_with_llm(state, cfg, findings=ev,
                                              prev_sql=prev_sql)
        if not sql:
            sql = prev_sql
            detail = "第 %d 轮 执行错误修复失败，保留原 SQL" % n
        else:
            detail = "第 %d 轮 执行错误重写（%s）" % (n, str(exec_err)[:48])
        return {
            "sql": sql,
            "generate_error": "",
            "history": [{"attempt": n, "stage": "exec_repair", "sql": sql,
                         "detail": detail, "meta": meta}],
            "trace": [{"node": "generate", "detail": detail,
                       "data": {"sql": sql, "meta": meta, **_tok(meta)}}],
        }

    # ---- 形态返工（critique 发起）----
    # 形态问题不是语法问题：fixer 只会改"表名带前缀""缺分区"这类机械点，
    # 对"该 GROUP BY 却做了全表求和"完全无能为力。直接交给模型，
    # 并把上一版结果一起带上（否则它看不见自己交了什么形状）。
    if any(f.get("rule") == "SHAPE" for f in findings):
        from node_generate_llm import generate_with_llm
        sql, detail, meta = generate_with_llm(state, cfg, findings=findings,
                                             prev_sql=prev_sql)
        if not sql:
            sql = prev_sql
            detail = "第 %d 轮 形态返工失败，保留原 SQL" % n
        else:
            detail = "第 %d 轮 形态返工（%s）" % (
                n, (state.get("shape") or {}).get("label") or "形状不符")
        return {
            "sql": sql,
            "generate_error": "",
            "history": [{"attempt": n, "stage": "shape_repair", "sql": sql,
                         "detail": detail, "meta": meta}],
            "trace": [{"node": "generate", "detail": detail,
                       "data": {"sql": sql, "meta": meta, **_tok(meta)}}],
        }

    # ---- L1 机械修复（不花 token）----
    cand, applied, verdict, findings2 = fixer.try_mechanical(prev_sql, findings, cfg)

    if applied and verdict != "reject":
        detail = "第 %d 轮 机械修复 %d 处 → 复检 %s" % (n, len(applied), verdict)
        return {
            "sql": cand,
            "generate_error": "",
            "fix_log": applied,
            "history": [{"attempt": n, "stage": "mechanical", "sql": cand,
                         "applied": applied, "verdict_after": verdict}],
            "trace": [{"node": "generate", "detail": detail,
                       "data": {"sql": cand, "applied": applied}}],
        }

    # 机械修过但还是没过：把改后的版本当作新基线交给模型，
    # 否则模型会重复修已经修好的那几条。
    base_sql = cand if applied else prev_sql
    remaining = findings2 if applied else findings
    note = ("（机械已修 %d 处，剩 %d 条）" % (len(applied), len(_rejects(remaining)))
            if applied else "")

    # ---- L2 模型重写 ----
    from node_generate_llm import generate_with_llm

    sql, detail, meta = generate_with_llm(state, cfg,
                                          findings=remaining, prev_sql=base_sql)
    if not sql:
        # 修复失败就退回机械修过的版本——它至少比原版少几处错
        sql = base_sql
        detail = "第 %d 轮 模型修复失败（%s），退回机械修复版" % (n, detail[:60])
    else:
        detail = "第 %d 轮 %s%s" % (n, detail, note)

    return {
        "sql": sql,
        "generate_error": "",
        "fix_log": applied,
        "history": [{"attempt": n, "stage": "llm_repair", "sql": sql,
                     "applied": applied, "detail": detail, "meta": meta}],
        "trace": [{"node": "generate", "detail": detail,
                   "data": {"sql": sql, "applied": applied, "meta": meta, **_tok(meta)}}],
    }


def generate(state: AgentState, cfg: dict) -> dict:
    if state.get("unsupported"):
        # route 已经判定答不了，不必浪费一次校验
        return {
            "sql": "",
            "trace": [{"node": "generate", "detail": "跳过：%s" % state["unsupported"],
                       "data": {}}],
        }

    mode = cfg["generator"]["mode"]
    day = state.get("time_end") or cfg["lane"]["offline"]["data_until"]

    if mode == "stub":
        sql = _STUB.format(dt=day)
        return {
            "sql": sql,
            "generate_via": "stub",
            "attempts": state.get("attempts", 0),
            "history": [{"attempt": 1, "stage": "stub", "sql": sql}],
            "trace": [{"node": "generate", "detail": "stub 模式：固定 SQL，dt=%s" % day,
                       "data": {"sql": sql}}],
        }

    if mode != "llm":
        raise ValueError("未知的 generator.mode：%s" % mode)

    # ---- 契约失败：不再重试，直接交回空 SQL ----
    # 走到这里说明上一轮模型输出没通过 SQLDraft 校验，而且 _call_once 已经在
    # 内部带着错因重试过一次了。图层再问一遍只会再花两次调用，问法还完全一样。
    # 返回空 SQL 让 validate 判 EMPTY → after_validate 的第二条判据走 refuse；
    # node_refuse 会把"没按约定格式输出"和具体错因一起说给用户（见那里的注释）。
    if state.get("attempts", 0) > 0 and state.get("generate_schema_failed"):
        detail = "输出不合规已重试一次，不再重试：%s" % (
            (state.get("generate_error") or "")[:120])
        return {
            "sql": "",
            "generate_error": state.get("generate_error") or detail,
            "history": [{"attempt": state.get("attempts", 0) + 1,
                         "stage": "schema_abort", "sql": "", "detail": detail}],
            "trace": [{"node": "generate", "detail": detail,
                       "data": {"schema_error": True}}],
        }

    if state.get("attempts", 0) > 0 and state.get("sql"):
        return _repair(state, cfg)
    return _first(state, cfg)
