"""真 HITL（P1-2）的自测：暂停 → 恢复 → 拒绝 → 重复恢复不重复执行。

为什么这套必须存在
------------------
`interrupt()` 最容易踩的三个坑，都不会报错，只会**悄悄错**：

  ① 重复执行。恢复时 LangGraph 会从头重跑被中断的节点；如果重跑后
     need_clarify 变成 False，中断"对不上"，恢复值被丢弃，图从头再跑一遍——
     用户看到的是"补了一句话，结果多查了一次库"。
  ② 恢复期重复花 token。同一个节点被执行两次，就多一次计划调用。
  ③ 中断没有留痕。qa_logs 只记"问过什么"，如果中断不落盘，"这次为什么
     没出数"在审计里就消失了。

所以这里测的是行为，不是接口形状：
  · 暂停时**没有** SQL、没有答案、没有执行；审计有一条 intent=clarify；
  · 恢复后 SQL 只执行**一次**，审计有第二条 intent=query，两条共享 thread_id；
  · 计划只调用**一次**（中断现场缓存住了计划，恢复不重算）；
  · 重复 resume 不重跑图（duplicate_resume=true，execute 计数不变）；
  · 用户说"不查了"→ 不生成 SQL、不执行，答案是"你选择不补充"，不是"系统坏了"；
  · 别人的 thread / 不存在的 thread / 复用被中断的 trace_id → 明确报错。

全部打桩：`gateway.chat` 换成回放器，不花一分钱、不依赖网络；执行仍打真库
（这一条必须真跑，否则"执行了几次"无从断言）。

    python code/agent/tests/selftest_hitl.py
"""
from __future__ import annotations

import _bootstrap  # noqa: F401

import copy
import json
import sys

import gateway
import graph
import node_plan
from conf import load_config
from governance import store
from governance.contracts import RequestContext

OK = 0
FAIL = 0
FAILED: list[str] = []


def ck(name: str, cond: bool, extra: str = "") -> None:
    global OK, FAIL
    if cond:
        OK += 1
        print("  ✓ %s" % name)
    else:
        FAIL += 1
        FAILED.append(name)
        print("  ✗ %s %s" % (name, extra))


PLAN_CLARIFY = {
    "intent": "用户说要看播放，但没说按次数还是按人数",
    "metrics": [], "dimensions": [], "tables": ["ads_daily_overview"],
    "filters": [], "time_condition": "dt = '2026-11-08'", "shape": "scalar",
    "need_clarify": True,
    "clarify_question": "播放量是按播放次数算，还是按播放人数算？",
}
PLAN_OK = dict(PLAN_CLARIFY, need_clarify=False, clarify_question="")
SQL_DRAFT = {
    "sql": "select dau from ads_daily_overview where dt='2026-11-08'",
    "used_tables": ["ads_daily_overview"], "reason": "查当日 DAU", "assumptions": [],
}

CALLS: list[str] = []


def _stub_chat(plan_obj: dict):
    """回放器：caller=plan 回计划，其余回 SQL 草稿。顺带记下调用次序。"""

    def _chat(messages, cfg, **kw):
        caller = kw.get("caller") or "?"
        CALLS.append(caller)
        body = plan_obj if caller == "plan" else SQL_DRAFT
        return {
            "content": json.dumps(body, ensure_ascii=False),
            "model": "stub", "elapsed_ms": 1,
            "usage": {"prompt_tokens": 10, "completion_tokens": 10,
                      "total_tokens": 20},
            "cost_cny": 0.0, "ledger_ok": True, "ledger_error": "", "pii_hits": 0,
        }

    return _chat


def _cfg() -> dict:
    """plan 关着、hitl 开着：证明"能中断"不依赖全局打开 Planning。"""
    cfg = copy.deepcopy(load_config())
    cfg["generator"]["mode"] = "llm"          # 打桩后仍是"真模型"路径
    cfg["hitl"] = {"enabled": True, "max_pending": 50}
    cfg["prompt"]["plan"]["enabled"] = False
    return cfg


def _ctx(**kw) -> RequestContext:
    return RequestContext.system(source="selftest", **kw)


def _audit_row(cfg: dict, trace_id: str) -> dict | None:
    with store.connect(cfg) as conn, conn.cursor() as cur:
        cur.execute(
            "select trace_id, intent, question, answer_excerpt, refused, "
            "trace_json->'hitl'->>'stage', trace_json->'hitl'->>'thread_id' "
            "from qa_logs where trace_id=%s", (trace_id,))
        row = cur.fetchone()
    if not row:
        return None
    return {"trace_id": row[0], "intent": row[1], "question": row[2],
            "answer": row[3], "refused": row[4], "stage": row[5], "thread": row[6]}


def _nodes(st: dict) -> list[str]:
    return [t.get("node") for t in (st.get("trace") or [])]


# ---------------------------------------------------------------- [1] 取消判据
def t_cancel() -> None:
    print("\n[1] “不查了”的判据（整句只由取消词拼成才算）")
    for text in ("算了，不查了", "取消", "算了。", "不用了", "不查了！"):
        ck("「%s」判为取消" % text, node_plan._is_cancel(text))
    for text in ("不用退款口径，用播放量", "按播放人数", "不", "不查了，按人数算"):
        ck("「%s」不当成取消" % text, not node_plan._is_cancel(text))


# ---------------------------------------------------------------- [2] 暂停
def t_pause(cfg: dict) -> tuple[dict, str]:
    print("\n[2] 暂停：有歧义的问题停在 plan，不生成 SQL")
    CALLS.clear()
    gateway.chat = _stub_chat(PLAN_CLARIFY)
    ctx = _ctx()
    tid = "hitl-pause-1"
    st = graph.ask("播放量怎么样", cfg, ctx=ctx, thread_id=tid)

    ck("返回 interrupted", st.get("interrupted") is True)
    ck("带回要问用户的话", bool(st.get("clarify_question")),
       str(st.get("clarify_question"))[:40])
    ck("thread_id 原样带回", st.get("thread_id") == tid)
    ck("没有生成 SQL", not st.get("sql"), repr(st.get("sql")))
    ck("没有答案（answer 为空）", not (st.get("answer") or "").strip())
    ck("没有执行过查询", "execute" not in _nodes(st), str(_nodes(st)))
    ck("trace 停在 plan", _nodes(st)[-1] == "plan", str(_nodes(st)))
    ck("只调了 1 次模型（plan）", CALLS == ["plan"], str(CALLS))
    ck("计划节点确实跑过", "plan" in _nodes(st))

    row = _audit_row(cfg, ctx.trace_id)
    ck("审计有一条事件", row is not None)
    if row:
        ck("审计 intent=clarify", row["intent"] == "clarify", row["intent"])
        ck("审计记下了问用户什么", "播放" in (row["answer"] or ""), row["answer"] or "")
        ck("审计标明 stage=interrupt", row["stage"] == "interrupt", str(row["stage"]))
        ck("审计带上 thread_id", row["thread"] == tid, str(row["thread"]))
    return st, tid


# ---------------------------------------------------------------- [3] 恢复
def t_resume(cfg: dict, tid: str) -> dict:
    print("\n[3] 恢复：补一句话后继续，SQL 只执行一次")
    CALLS.clear()
    gateway.chat = _stub_chat(PLAN_CLARIFY)
    ctx = _ctx()
    st = graph.resume("按播放人数", cfg, ctx=ctx, thread_id=tid)

    ck("恢复后不再中断", not st.get("interrupted"))
    ck("拿到了 SQL", bool(st.get("sql")), repr(st.get("sql")))
    ck("SQL 就是桩里那条", st.get("sql") == SQL_DRAFT["sql"], str(st.get("sql")))
    ck("执行恰好 1 次", _nodes(st).count("execute") == 1, str(_nodes(st)))
    ck("恢复不重算计划（plan 0 次）", CALLS.count("plan") == 0, str(CALLS))
    ck("澄清内容并进了问题", "按播放人数" in (st.get("question") or ""),
       (st.get("question") or "")[:60])
    ck("有答案", bool((st.get("answer") or "").strip()))
    ck("恢复标记写在 state 里", (st.get("hitl") or {}).get("stage") == "resume")

    row = _audit_row(cfg, ctx.trace_id)
    ck("恢复也有独立审计事件", row is not None)
    if row:
        ck("恢复事件 intent=query", row["intent"] == "query", row["intent"])
        ck("恢复事件 stage=resume", row["stage"] == "resume", str(row["stage"]))
        ck("两条事件共享 thread_id", row["thread"] == tid, str(row["thread"]))
    return st


# ---------------------------------------------------------------- [4] 重复恢复
def t_duplicate(cfg: dict, tid: str, first: dict) -> None:
    print("\n[4] 重复 resume：回放同一份结果，不重跑图")
    CALLS.clear()
    gateway.chat = _stub_chat(PLAN_CLARIFY)
    st = graph.resume("再补一句", cfg, ctx=_ctx(), thread_id=tid)

    ck("标了 duplicate_resume", st.get("duplicate_resume") is True)
    ck("答案与第一次相同", st.get("answer") == first.get("answer"))
    ck("execute 仍然只有 1 次", _nodes(st).count("execute") == 1, str(_nodes(st)))
    ck("一次模型都没调", CALLS == [], str(CALLS))


# ---------------------------------------------------------------- [5] 拒绝
def t_decline(cfg: dict) -> None:
    print("\n[5] 拒绝：用户说“不查了”就不查，且说清是他的选择")
    CALLS.clear()
    gateway.chat = _stub_chat(PLAN_CLARIFY)
    ctx1 = _ctx()
    tid = "hitl-decline-1"
    st0 = graph.ask("播放量怎么样", cfg, ctx=ctx1, thread_id=tid)
    ck("先能暂停", st0.get("interrupted") is True)

    ctx2 = _ctx()
    st = graph.resume("算了，不查了", cfg, ctx=ctx2, thread_id=tid)
    ck("判为拒答", st.get("refused") is True)
    ck("没有任何 SQL", not st.get("sql"), repr(st.get("sql")))
    ck("一次都没执行", _nodes(st).count("execute") == 0, str(_nodes(st)))
    ck("只调过计划一次（没有白生成 SQL）", CALLS == ["plan"], str(CALLS))
    ans = st.get("answer") or ""
    ck("答案说明是用户主动不答", "不补充" in ans or "选择" in ans, ans[:60])
    ck("答案不是“系统无法生成”那套措辞", "无法生成" not in ans, ans[:60])


# ---------------------------------------------------------------- [6] 边界
def t_guard(cfg: dict, tid: str, paused: dict) -> None:
    print("\n[6] 边界：越权 / 不存在 / 复用 trace_id 都要明确报错")

    try:
        graph.resume("按人数", cfg, ctx=_ctx(user_id="someone-else"), thread_id=tid)
        ck("别人的 thread 被拒绝", False, "没有抛错")
    except PermissionError:
        ck("别人的 thread 被拒绝（PermissionError）", True)
    except Exception as e:
        ck("别人的 thread 被拒绝", False, "%s: %s" % (type(e).__name__, e))

    try:
        graph.resume("按人数", cfg, ctx=_ctx(), thread_id="thread-does-not-exist")
        ck("不存在的 thread 报错", False, "没有抛错")
    except KeyError:
        ck("不存在的 thread 报 KeyError", True)
    except Exception as e:
        ck("不存在的 thread 报错", False, "%s: %s" % (type(e).__name__, e))

    try:
        graph.resume("   ", cfg, ctx=_ctx(), thread_id=tid)
        ck("空回答被拒绝（不能拿原问题去猜口径）", False, "没有抛错")
    except ValueError:
        ck("空回答被拒绝（ValueError）", True)
    except Exception as e:
        ck("空回答被拒绝", False, "%s: %s" % (type(e).__name__, e))

    try:
        graph.resume("按人数", cfg, ctx=_ctx(trace_id=paused.get("trace_id")),
                     thread_id=tid)
        ck("复用被中断的 trace_id 被拒绝", False, "没有抛错")
    except ValueError:
        ck("复用被中断的 trace_id 报 ValueError（否则撞 qa_logs 主键）", True)
    except Exception as e:
        ck("复用被中断的 trace_id 被拒绝", False, "%s: %s" % (type(e).__name__, e))


# ---------------------------------------------------------------- [7] 老路径
def t_legacy(cfg: dict) -> None:
    print("\n[7] 不带 thread_id：仍是老路径（没有 plan 旁路、没有中断）")
    CALLS.clear()
    gateway.chat = _stub_chat(PLAN_CLARIFY)
    st = graph.ask("播放量怎么样", cfg, ctx=_ctx())

    ck("不中断", not st.get("interrupted"))
    ck("trace 里没有 plan 节点", "plan" not in _nodes(st), str(_nodes(st)))
    ck("直接走 generate", _nodes(st)[0] == "route" and "generate" in _nodes(st),
       str(_nodes(st)))
    ck("模型调用里没有 plan", CALLS == ["generate"], str(CALLS))
    ck("拿到了 SQL（没有因为缺澄清就拒答）", bool(st.get("sql")), repr(st.get("sql")))


def main() -> int:
    cfg = _cfg()
    print("=" * 62)
    print("真 HITL 自测（interrupt + checkpointer + resume）")
    print("=" * 62)

    t_cancel()
    paused, tid = t_pause(cfg)
    first = t_resume(cfg, tid)
    t_duplicate(cfg, tid, first)
    t_decline(cfg)
    # 重新开一条 thread 拿暂停态，专供边界用例（前面那条已经跑完了）
    gateway.chat = _stub_chat(PLAN_CLARIFY)
    paused2 = graph.ask("播放量怎么样", cfg, ctx=_ctx(), thread_id="hitl-guard-1")
    t_guard(cfg, "hitl-guard-1", paused2)
    t_legacy(cfg)

    print("\n" + "-" * 62)
    print("%d 通过 / %d 失败" % (OK, FAIL))
    if FAILED:
        print("失败项：")
        for f in FAILED:
            print("  · %s" % f)
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(main())
