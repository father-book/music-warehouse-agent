"""LangGraph 编排。

    START → route ─┬─(元问题)→ meta → END
                   │
                   └─(问数)→ generate → validate ─┬─(pass/warn)→ execute
                                    ▲             │
                                    │             ├─(reject, 有余量)→ generate
                                    └─────────────┘
                                                   execute ─┬─(执行报错, 有余量)→ generate
                                                            ├─(成功)→ critique ─┬→ narrate → END
                                                            │                   └─(形态不符, 首次)→ generate
                                                            └─(拒答/策略拦截)→ narrate

阶段 1 新增两条边界：

1. **安全拦截不进修复闭环**。R21 越权 / R22 PII / R23 注入是身份边界，
   让模型"换个写法再试三次"等于给三次越权机会。validate 标 `security_block`
   后直接 refuse。
2. **每次问答是一条不可变审计事件**。`ask()` 在图跑完后统一落盘，
   因此 CLI / eval / web / 元问题 / 拒答 / 执行失败六条入口都覆盖。

阶段 3 新增 critique：执行成功后回比「问题形态 ↔ 结果形状」。
护栏管不了"答非所问"——`SELECT SUM(x)` 语法合法、口径正确，
但回答不了「排行榜」。这一层只返工一次，且只在形态明显崩塌时触发。

`ask()` 的 ctx 缺省为内部基线上下文（ADR-008），旧调用零改动仍可用；
web 侧必须传登录得到的真实 ctx。
"""
from __future__ import annotations

import _bootstrap  # noqa: F401

import hashlib
import time

from governance import audit
from governance.contracts import RequestContext
from governance.policy import engine
from langgraph.checkpoint.memory import MemorySaver
from langgraph.graph import END, START, StateGraph
from langgraph.types import Command

import answercache
import gateway
import node_execute
import node_critique
import node_generate
import node_meta
import node_narrate
import node_plan
import node_react
import node_refuse
import node_route
import node_validate
from conf import load_config
from state import AgentState


def react_enabled(cfg: dict) -> bool:
    """ReAct 是 opt-in：默认关，基线不受影响。"""
    return bool(((cfg.get("prompt") or {}).get("react") or {}).get("enabled"))


def plan_enabled(cfg: dict) -> bool:
    """独立 Planning 阶段同为 opt-in。"""
    return bool(((cfg.get("prompt") or {}).get("plan") or {}).get("enabled"))


def hitl_enabled(cfg: dict) -> bool:
    """真 HITL 的总开关（P1-2）。默认开，但只在显式带 thread_id 的请求上生效。"""
    return bool((cfg.get("hitl") or {}).get("enabled", True))


# 中断现场存在进程内：MemorySaver 够用（单实例部署），换多实例要换 Redis/PG 版
# checkpointer——这一点写在这里，而不是等上线才发现"换个 worker 就恢复不了"。
_CKPT = MemorySaver()


def _prune_conversations(cfg: dict) -> None:
    """没人恢复的中断现场不能无限占内存：超上限就丢最旧的会话。"""
    max_n = int((cfg.get("hitl") or {}).get("max_pending") or 200)
    try:
        storage = _CKPT.storage            # dict[thread_id][ns][checkpoint_id]
    except Exception:
        return
    while len(storage) > max(1, max_n):
        storage.pop(next(iter(storage)), None)


def _thread_config(thread_id: str) -> dict:
    return {"configurable": {"thread_id": thread_id}, "recursion_limit": 50}


def build_graph(cfg: dict | None = None, checkpointer=None):
    cfg = cfg or load_config()
    max_attempts = int(cfg["limits"]["max_attempts"])

    def after_route(state: AgentState) -> str:
        if state.get("meta_intent"):
            return "meta"
        # 调试模式：调用方直接给了 SQL，跳过生成，只验护栏与执行
        if state.get("sql"):
            return "validate"
        # HITL：要能暂停，就必须先有计划节点（只有它知道"该不该问清楚"）。
        # 只在显式带 thread_id（web 追问模式）且是真模型时这样走——CLI 与
        # 128 题评测不带 thread_id，路由与以前完全一致，基线不受影响。
        if (state.get("thread_id") and hitl_enabled(cfg)
                and cfg["generator"]["mode"] == "llm"):
            return "plan"
        # ReAct 只在真模型下有意义（stub 不调工具），且必须显式开启。
        # 默认走 generate：128 题基线是在那条路上跑出来的。
        if react_enabled(cfg) and cfg["generator"]["mode"] == "llm":
            return "react"
        # Planning 与 ReAct 互斥地排在生成之前：ReAct 自己会边查边写，
        # 再叠一层计划只会把两条思考路径混在一起，谁也说不清是谁的功劳。
        if plan_enabled(cfg) and cfg["generator"]["mode"] == "llm":
            return "plan"
        return "generate"

    def after_validate(state: AgentState) -> str:
        # 安全规则命中：直接拒答，不给模型重试机会
        if state.get("security_block"):
            return "refuse"
        # 成本闸命中：也不重试。重试等于再赌一次大扫描，与"闸"的目的一致。
        if state.get("cost_block"):
            return "refuse"
        if state.get("unsupported"):
            return "refuse"
        if state.get("verdict") != "reject":
            return "execute"
        if cfg["generator"]["mode"] == "stub":
            return "refuse"
        if state.get("generate_error") and state.get("attempts", 0) >= 2:
            return "refuse"
        if state.get("attempts", 0) >= max_attempts:
            return "refuse"
        return "generate"

    def after_execute(state: AgentState) -> str:
        # 策略/安全拒绝：直接出答案，不允许重试绕过
        if state.get("security_block") or state.get("refused"):
            return "narrate"
        res = state.get("result") or {}
        if not res.get("error"):
            return "critique"          # 跑通了才谈得上"形状对不对"
        if cfg["generator"]["mode"] != "llm":
            return "narrate"
        if state.get("exec_retries", 0) >= 2:
            return "narrate"
        if state.get("attempts", 0) >= max_attempts:
            return "narrate"
        return "generate"

    def after_critique(state: AgentState) -> str:
        # 只有本次 critique **刚刚**判定形态不符才返工；
        # 已经返工过的第二轮即使仍不符也直接出答案，避免成本失控。
        return "generate" if state.get("shape_retry_now") else "narrate"

    g = StateGraph(AgentState)
    g.add_node("route", lambda s: node_route.route(s, cfg))
    g.add_node("meta", lambda s: node_meta.answer_meta(s, cfg))
    g.add_node("react", lambda s: node_react.react(s, cfg))
    g.add_node("plan", lambda s: node_plan.plan(s, cfg))
    g.add_node("generate", lambda s: node_generate.generate(s, cfg))
    g.add_node("validate", lambda s: node_validate.validate(s, cfg))
    g.add_node("execute", lambda s: node_execute.execute(s, cfg))
    g.add_node("critique", lambda s: node_critique.critique(s, cfg))
    g.add_node("refuse", lambda s: node_refuse.refuse(s, cfg))
    g.add_node("narrate", lambda s: node_narrate.narrate(s, cfg))

    g.add_edge(START, "route")
    g.add_conditional_edges("route", after_route,
                            {"meta": "meta", "generate": "generate",
                             "react": "react", "plan": "plan",
                             "validate": "validate"})
    g.add_edge("meta", END)
    # react 内部失败会自己回落到一次成型，产不出 SQL 时交给 refuse，与 generate 一致
    g.add_conditional_edges("react", node_react.after_react,
                            {"validate": "validate", "refuse": "refuse"})
    # plan 只有在模型明确要求澄清时才中断；计划不合法只是不注入，不阻断
    g.add_conditional_edges("plan", node_plan.after_plan,
                            {"generate": "generate", "refuse": "refuse"})
    g.add_edge("generate", "validate")
    g.add_conditional_edges("validate", after_validate,
                            {"execute": "execute", "refuse": "refuse", "generate": "generate"})
    g.add_conditional_edges("execute", after_execute,
                            {"critique": "critique", "narrate": "narrate",
                             "generate": "generate"})
    g.add_conditional_edges("critique", after_critique,
                            {"narrate": "narrate", "generate": "generate"})
    g.add_edge("refuse", "narrate")
    g.add_edge("narrate", END)
    # 拓扑与边一条没改，只是可选中挂 checkpointer：不带它时行为与以前逐字相同。
    return g.compile(checkpointer=checkpointer)


def _run(question: str, cfg: dict, ctx: RequestContext, decision,
         sql: str | None, history_turns: list | None = None,
         thread_id: str | None = None,
         memory_turns: list | None = None) -> AgentState:
    """跑一次完整的图。缓存未命中时才走到这里。

    `thread_id` 非空时编译带 checkpointer 的图：只有这样才能在 plan 节点
    interrupt() 把现场存下来，供 `/api/ask/resume` 用同一个 id 恢复。
    """
    app = build_graph(cfg, checkpointer=_CKPT if thread_id else None)
    init: AgentState = {
        "question": question, "trace": [], "attempts": 0,
        "history": [], "fix_log": [], "exec_retries": 0,
        "ctx": ctx.to_dict(), "trace_id": ctx.trace_id,
        "policy_decision": decision.to_dict(),
    }
    if thread_id:
        init["thread_id"] = thread_id
    if history_turns:
        init["history_turns"] = list(history_turns)[-3:]
    if memory_turns:
        init["memory_turns"] = list(memory_turns)[:3]
    if sql:
        # 调试入口：直接给 SQL，跳过生成，只验护栏与执行。
        init["sql"] = sql
        init["time_end"] = cfg["lane"]["offline"]["data_until"]
        init["metrics"] = []
    config = _thread_config(thread_id) if thread_id else {"recursion_limit": 50}
    return app.invoke(init, config)


def _replay(hit: dict, question: str, cfg: dict, ctx: RequestContext,
            decision, key: str) -> AgentState:
    """缓存命中：把上一次的答案原样交回，换成本次的身份与 trace_id。

    只换"请求身份"，不换"答案内容"——答案里的数字来自那次真实执行，
    换掉就会变成"缓存命中后答案变了"，那比不缓存更糟。
    """
    st = dict(hit)
    st.update({
        "question": question,
        "ctx": ctx.to_dict(), "trace_id": ctx.trace_id,
        "policy_decision": decision.to_dict(),
        "cache_hit": True, "cache_key": key[:12],
        "trace": [{"node": "cache",
                   "detail": "命中答案缓存：0 token / 0 查询 / 0 护栏",
                   "data": {"key": key[:12],
                            "policy_hash": decision.policy_hash,
                            "tokens_in": 0, "tokens_out": 0}}],
    })
    # 命中也要进模型台账（cached=true / 0 成本）：否则成本曲线上会出现
    # "少了一次调用却查不到哪去了"的缺口——省下的钱也是要能被解释的。
    ledger_ok, ledger_err = gateway.record_cache_hit(cfg, ctx=ctx, key=key)
    if not ledger_ok:
        st["ledger_error"] = ledger_err
    return st


def _history_salt(history_turns: list | None) -> str:
    """多轮时的缓存盐：把最近两轮的问句折进键里。

    不这么做的话，不同会话里同一句「那昨天呢」会命中同一个缓存条目，
    回放出来的答案是别人那段对话的延续。缓存键必须包含"这个答案是在什么
    上下文里生成的"，否则缓存就成了串号。
    """
    if not history_turns:
        return ""
    tail = [str((t or {}).get("question") or "") for t in history_turns[-2:]]
    raw = "\n".join(tail)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:12]


def ask(question: str, cfg: dict | None = None, sql: str | None = None,
        ctx: RequestContext | None = None,
        history_turns: list | None = None,
        thread_id: str | None = None,
        memory_turns: list | None = None) -> AgentState:
    """一次问答 = 一条带 trace_id 的审计事件。ctx 缺省为内部基线通道。

    `history_turns` 是同一会话里最近几轮的问答摘要（web 侧注入）。
    为空时行为与没有记忆的版本完全一致：prompt 不变、缓存键不变，
    评测集（全是单轮）因此不受影响。

    `thread_id` 是 P1-2 的真 HITL 入口：带上它才会挂 checkpointer，plan 节点
    因此可以 interrupt() 暂停等用户澄清。不带它的调用方（CLI / 128 题评测）
    走的是完全一样的旧路径——没有 checkpointer、没有 plan 旁路、缓存照常。

    阶段 4 加了答案缓存（AC-15）：
      · 命中 → 直接回放，0 token / 0 查询，但仍然落一条审计（否则"谁问了什么"
        会从账上消失）。
      · 未命中 → 跑图，成功才写缓存；`eval`/`cli` 一律不读不写（会让成本统计失真）。
      · 同一问题并发 → 只跑一次，其余等结果后直接命中（单飞）。
      · HITL 请求照常读写缓存：暂停的结果因为 answer 为空，本来就过不了
        `answercache.cacheable`，不会把"没有答案的快照"写进缓存；
        反过来，命中缓存时也不会有 thread 需要恢复（那一轮已经答完了）。
    """
    cfg = cfg or load_config()
    ctx = ctx or RequestContext.system(source="cli")
    thread_id = (thread_id or "").strip()
    use_hitl = bool(thread_id) and hitl_enabled(cfg)

    # 策略只在入口算一次，结果随 state 传给 validate / execute；
    # 修复闭环每重写一版 SQL，execute 都会重新注入（ADR-005）。
    decision = engine(cfg).decide(ctx)

    cache = answercache.get_cache(cfg)
    use_cache = (bool((cfg.get("cache") or {}).get("enabled", True))
                 and answercache.in_scope(ctx))
    salt = _history_salt(history_turns)
    # 记忆进缓存键：改了偏好/删了记忆，旧答案必须失效。
    # 没有记忆时指纹为空，key 与以前逐字相同（eval / cli 全走这一支）。
    if memory_turns:
        try:
            import memory as _memory
            mf = _memory.fingerprint(memory_turns)
            if mf:
                salt = (salt + "|mem:" + mf) if salt else "mem:" + mf
        except Exception:
            pass
    key = "" if (sql or not use_cache) else answercache.make_key(
        question, cfg, decision, extra=salt)

    def _go() -> dict:
        """跑图（HITL 请求额外挂 checkpointer 并把"挂起"翻译成业务字段）。"""
        if use_hitl:
            _prune_conversations(cfg)
        st = _run(question, cfg, ctx, decision, sql, history_turns,
                  thread_id=thread_id if use_hitl else None,
                  memory_turns=memory_turns)
        return _absorb_interrupt(st, thread_id, ctx) if use_hitl else st

    t0 = time.perf_counter()
    if not key:
        state = _go()
    else:
        # 单飞：同一问题十路并发时，只有一路真的去调模型。
        # 用 single_flight 而不是进程内 key_lock —— 后端可能是 Redis，
        # 多实例同时冷启动时也要合并成一次生成，否则"缓存"只是把
        # 峰值从 10 次生成降到 3 次（每个实例各一次）。
        with cache.single_flight(key):
            hit = cache.get(key)
            if hit:
                state = _replay(hit, question, cfg, ctx, decision, key)
            else:
                state = _go()
                if answercache.cacheable(state, ctx):
                    cache.put(key, state)
    state["elapsed_ms"] = int((time.perf_counter() - t0) * 1000)

    # 审计不可变事件：所有入口统一在这里落盘。失败必须显式标注。
    ok, err = audit.record(state, ctx, decision, cfg)
    state["audit_error"] = "" if ok else err
    return state


def _absorb_interrupt(state: dict, thread_id: str, ctx: RequestContext) -> dict:
    """把"图被挂起了"翻译成调用方看得懂的 state。

    LangGraph 把中断放在 `__interrupt__` 里，而它是运行时对象、不是业务字段：
    直接交给 webapp 会被 JSON 序列化成一段看不懂的东西。这里取出口径问题、
    标 `interrupted`，并补一条 trace —— 审计只能看落盘的 trace，不补的话
    "这一次停在哪儿"在事后完全查不到。
    """
    intr = state.get("__interrupt__") or ()
    if not intr:
        return state
    val = getattr(intr[0], "value", None) or {}
    q = str(val.get("question") or "")
    st = dict(state)
    st.pop("__interrupt__", None)
    st.update({
        "interrupted": True,
        "thread_id": thread_id,
        "clarify_question": q,
        "answer": "",
        "hitl": {"thread_id": thread_id, "stage": "interrupt",
                 "clarify_question": q, "trace_id": ctx.trace_id},
        "trace": list(st.get("trace") or []) + [
            {"node": "plan", "detail": "已暂停：等用户澄清（%s）" % (q[:60] or "空问题"),
             "data": {"thread_id": thread_id, "clarify_question": q,
                      "interrupted": True}}],
    })
    return st


def resume(answer: str, cfg: dict | None = None,
           ctx: RequestContext | None = None, thread_id: str = "") -> AgentState:
    """恢复一次被挂起的问答（P1-2）。

    三件事必须成立，否则宁可报错也不猜：
      1. `thread_id` 存在——它是 checkpointer 的唯一钥匙，猜不出来；
      2. 这条 thread 的 ctx 属于当前用户——否则换个账号就能接着别人的问题查数；
      3. 图还没跑完——已经出过答案的直接回放，**不重跑**。

    第 3 条是"重复 resume 不重复执行"的落地：LangGraph 在 thread 完成后
    `get_state().next` 为空，此时再 invoke(Command(resume=...)) 只会返回同一份
    最终状态（实测 execute 不再触发），但这里仍然显式挡一层并打 `duplicate_resume`，
    让调用方知道"你这下没产生新查询"，而不是以为自己又查了一次。
    """
    cfg = cfg or load_config()
    ctx = ctx or RequestContext.system(source="cli")
    thread_id = (thread_id or "").strip()
    if not thread_id:
        raise ValueError("thread_id 不能为空")
    # 空回答不是"补充信息"：让它过去，模型只能拿原问题去猜一个口径，
    # 那正是 HITL 要避免的事。不想查就明说「不查了」。
    if not (answer or "").strip():
        raise ValueError("恢复必须带一句回答；不想查了请回「不查了」")
    if not hitl_enabled(cfg):
        raise RuntimeError("HITL 已关闭（cfg.hitl.enabled=false）")

    app = build_graph(cfg, checkpointer=_CKPT)
    config = _thread_config(thread_id)
    snap = app.get_state(config)
    values = dict(snap.values or {})
    if not values:
        raise KeyError("没有待恢复的问答：thread_id=%s" % thread_id)
    owner = str((values.get("ctx") or {}).get("user_id") or "")
    if owner and owner != ctx.user_id:
        raise PermissionError("这条 thread 不属于当前用户")
    # 审计表的主键是 trace_id：恢复必须是一次**新**请求。复用被中断那次的
    # trace_id 会撞 qa_logs 主键（这张表不可变，撞了就写不进去），
    # 与其让审计静默缺失，不如直接说清楚。
    if str(values.get("trace_id") or "") == ctx.trace_id:
        raise ValueError("恢复请求必须使用新的 trace_id（不能用被中断那次请求的）")
    decision = engine(cfg).decide(ctx)

    if not snap.next:
        out = values
        out.update({
            "trace_id": ctx.trace_id, "ctx": ctx.to_dict(),
            "duplicate_resume": True,
            "hitl": {"thread_id": thread_id, "stage": "resume",
                     "answer": str(answer or "")[:200], "duplicate": True},
        })
        out["audit_error"] = ""
        return out

    t0 = time.perf_counter()
    out = dict(app.invoke(Command(resume=str(answer or "")), config))
    out.pop("__interrupt__", None)
    out.update({
        "trace_id": ctx.trace_id, "ctx": ctx.to_dict(),
        "elapsed_ms": int((time.perf_counter() - t0) * 1000),
        "hitl": {"thread_id": thread_id, "stage": "resume",
                 "answer": str(answer or "")[:200]},
    })
    ok, err = audit.record(out, ctx, decision, cfg)
    out["audit_error"] = "" if ok else err
    return out
