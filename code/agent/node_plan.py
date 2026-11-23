"""Planning 节点（Plan-and-Execute 的第一半）。

它只做一件事：在写 SQL 之前，先让模型产出一份**可被规则校验**的计划。
计划不合法时降级（不注入计划，照常生成），而不是直接让用户拿不到答案——
Planning 是增强，不能变成新的单点故障。

模型明确说"这问题有歧义"时（need_clarify），2026-11-16（P1-2）起不再直接拒答，
而是 `interrupt()` 把整张图挂起：现场存进 checkpointer，用户补一句后用同一个
thread_id 恢复，从"计划已就绪"继续往下走，不用把原问题重问一遍。

两条边界：
  · 只有带 `thread_id` 的请求才中断。CLI 与 128 题评测不带它，行为与以前逐字相同
    （仍然走"直接拒答并说明缺什么"），所以基线不受影响。
  · 恢复时 LangGraph 会**从头重跑本节点**（它的中断是重放语义），计划不缓存的话
    第二次调用可能给出不同的 need_clarify，中断就对不上了。因此中断前把计划放进
    `_PENDING`，恢复时直接复用，既不重复花 token，也保证同一次会话前后一致。
"""
from __future__ import annotations

import re

import _bootstrap  # noqa: F401

from langgraph.types import interrupt

import planner
from state import AgentState

# "不查了"这类回答不该被当成"补充信息"，否则会被拼进问题里再生成一遍 SQL。
# 判据是"整句只由取消词拼成"：先把标点空格去掉，再把取消词逐个抠掉，
# 抠干净才算取消。这样 "算了，不查了" 算取消，而 "不用退款口径，用播放量"
# 会剩下"退款口径用播放量"，不会被误判。
_CANCEL_WORDS = ("取消", "算了", "不查了", "不查", "不问了", "不问", "不用了",
                 "不用", "先不查", "放弃", "跳过", "不了", "没事了", "不需要",
                 "cancel", "no", "stop")
_CANCEL_STRIP = re.compile(r"[\s，,、。.！!？?；;：:…~～\-]+")


def _is_cancel(text: str) -> bool:
    s = _CANCEL_STRIP.sub("", (text or "").lower())
    if not s:
        return False
    rest = s
    for w in sorted(_CANCEL_WORDS, key=len, reverse=True):
        rest = rest.replace(w, "")
    return rest == ""

# thread_id → 中断前算好的计划。只存"会中断"的那一次，恢复后立刻删。
_PENDING: dict[str, dict] = {}


def _trim_pending(cfg: dict) -> None:
    """进程内缓存要有上限：没人恢复的会话不能无限占内存。"""
    max_n = int((cfg.get("hitl") or {}).get("max_pending") or 200)
    while len(_PENDING) > max(1, max_n):
        _PENDING.pop(next(iter(_PENDING)))


def _resumed(state: AgentState, out: dict, clarify_q: str, answer: str) -> dict:
    """拿到用户回答之后：要么带着澄清继续生成，要么按"不查了"收口。"""
    ans = (answer or "").strip()
    step = {"node": "plan", "data": {"clarify_question": clarify_q, "answer": ans}}
    if _is_cancel(ans):
        out.update({
            "clarify_answer": ans,
            "refused": True,
            "refuse_reason": ("你选择不补充信息，这一问就到这里。想查的时候把口径"
                              "（比如「按播放量」还是「按人数」）说清再问一次即可。"),
            # 保持 True，after_plan 据此走 refuse；此时一条 SQL 都不会生成
            "plan_need_clarify": True,
            "trace": list(out.get("trace") or []) + [
                dict(step, detail="已恢复：用户选择不澄清，按不答处理")],
        })
        return out

    base_q = state.get("question") or ""
    out.update({
        "clarify_answer": ans,
        # 澄清内容并进问题本身：generate / validate / 审计读的都是 question，
        # 在这里拼一次，比让每个下游各自拼一遍可靠。
        "question": ("%s\n\n【用户澄清】%s" % (base_q, ans)) if ans else base_q,
        "plan_need_clarify": False,
        "plan": dict(out.get("plan") or {}, need_clarify=False, clarify_question=""),
        "trace": list(out.get("trace") or []) + [
            dict(step, detail="已恢复：按用户澄清继续（%s）" % (ans[:40] or "空回答"))],
    })
    return out


def plan(state: AgentState, cfg: dict) -> dict:
    tid = str(state.get("thread_id") or "")
    cached = _PENDING.get(tid) if tid else None
    if cached:
        p, issues, meta = cached["p"], cached["issues"], cached["meta"]
    else:
        p, issues, meta = planner.make_plan(state, cfg)

    rejects = [i for i in issues if i.get("level") == "REJECT"]
    clarify_q = str(p.get("clarify_question") or "").strip()
    need_clarify = bool(p.get("need_clarify")) and bool(clarify_q)

    detail = "计划：%s" % ("无效" if rejects else ("待澄清" if need_clarify else "可用"))
    if p.get("metrics"):
        detail += " ｜ 指标 %s" % ",".join(str(x) for x in p["metrics"][:3])
    if p.get("tables"):
        detail += " ｜ 表 %s" % ",".join(str(x).split(".")[-1] for x in p["tables"][:3])
    if issues:
        detail += " ｜ %d 条提示" % len(issues)

    out = {
        "plan": p,
        "plan_issues": issues,
        "plan_meta": meta,
        # 只有被 REJECT 才算无效；WARN 不影响注入
        "plan_valid": bool(p) and not rejects,
        "plan_need_clarify": need_clarify,
        "trace": [{"node": "plan", "detail": detail,
                   "data": {"plan": p, "issues": issues, "meta": meta}}],
    }
    if not need_clarify:
        return out

    if not tid:
        # 没有 checkpointer（CLI / 评测 / 没传 thread_id）：老行为，直接拒答
        out["refused"] = True
        out["refuse_reason"] = clarify_q[:200]
        return out

    _PENDING[tid] = {"p": p, "issues": issues, "meta": meta}
    _trim_pending(cfg)
    # 挂起：返回后图不再往下走，现场（含 question/ctx）已由 checkpointer 存好
    answer = interrupt({"question": clarify_q[:200], "thread_id": tid})
    _PENDING.pop(tid, None)
    return _resumed(state, out, clarify_q, str(answer or ""))


def after_plan(state: AgentState) -> str:
    if state.get("plan_need_clarify"):
        return "refuse"
    return "generate"
