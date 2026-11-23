"""Agent 的状态定义。

刻意只有一个 State 类型：一次问答从进入到出答案，中间所有节点读写的
都是这一个 dict。好处是 trace 天然可复盘——把最终 state 打出来就是全过程。
"""
from __future__ import annotations

import operator
from typing import Annotated, Any, Literal, TypedDict

Lane = Literal["offline", "realtime"]
Verdict = Literal["pass", "warn", "reject"]


class GuardFinding(TypedDict, total=False):
    level: str          # REJECT / WARN / INFO
    rule: str           # R18-FANOUT 等
    message: str
    hint: str
    fix: dict           # {op, target, replacement}，Agent 可照改


class QueryResult(TypedDict, total=False):
    columns: list[str]
    rows: list[list[Any]]
    row_count: int
    elapsed_ms: int
    truncated: bool
    error: str


class TraceStep(TypedDict, total=False):
    node: str
    detail: str
    data: dict


class AgentState(TypedDict, total=False):
    # --- 输入 ---
    question: str
    # 最近几轮问答（web 侧按会话注入）。空 = 单轮，prompt 与没有记忆时逐字相同，
    # 因此评测集（全是单轮）不受影响。只放问答摘要，不放 trace——上下文要省 token。
    history_turns: list[dict]
    # 跨会话长期偏好：按用户 + 问句相关度召回的 Top-K（进入图前已按口径版本过滤）。
    # 只有本人显式要求记住的口径会存进来；空 = 没记忆/单轮/eval/cli，
    # prompt 与缓存键与以前逐字相同。
    memory_turns: list[dict]
    # 本轮沿用了上一轮的什么（指标 / 时间窗口）。非空 = 这是追问。
    # 它必须一路走到答案里：用户看到"沿用上一轮"才知道数字是按哪套口径算的。
    followup: dict
    meta_intent: dict                # 非空 = 元问题（能力/口径/闲聊），直接回答不写 SQL
    meta_answer: bool
    exec_retries: int                # 执行失败后的自愈次数（与 attempts 分开计）

    # --- 阶段 1：治理（ADR-002 / 005 / 006）---
    ctx: dict                        # RequestContext.to_dict()，全链路透传
    trace_id: str
    policy_decision: dict            # PolicyDecision.to_dict()，执行节点据此下推
    applied_sql: str                 # 策略注入后真正执行的 SQL
    security_block: bool             # R21/R22/R23 命中：直接拒答，不进修复闭环
    cost_block: bool                 # R24 成本闸命中：EXPLAIN 说这条计划太贵，拒答不重试
    cost_gate: dict                  # 扫描量估计快照：{status, rows, bytes, scan_nodes, limits}
    audit_error: str                 # 审计落盘失败要显式标注，绝不静默
    elapsed_ms: int

    # --- route 产出 ---
    lane: Lane
    time_start: str                  # YYYY-MM-DD，裁剪后实际可查的起点
    time_end: str                    # YYYY-MM-DD，裁剪后实际可查的终点
    metrics: list[str]               # 指标 code
    metric_hits: list[dict]          # 检索命中明细（含 matched_by，供 prompt 用）
    tables: list[str]                # 候选物理表
    unsupported: str                 # 非空 = 该问题当前答不了，narrate 会据此拒答

    # --- D 阶段：时间窗口 / 检索 / 新鲜度 ---
    time_window: dict                # {label, kind, status, note, requested, sql_hint}
    dimensions: list[str]           # 命中的分组维度 code（来自 dimensions.yaml）
    dimension_cautions: list[str]   # 维度的 caution，要进 prompt 防止误汇总
    retrieved_tables: list[dict]    # BM25 召回明细，trace 用
    freshness: str                  # 数据滞后说明，narrate 必带

    # --- generate 产出 ---
    sql: str
    generate_error: str              # 模型调用/解析失败的原因，refuse 会读它
    generate_detail: str
    generate_meta: dict
    # 模型输出没通过 schemas.SQLDraft 校验、且已在 _call_once 里重试过一次。
    # 有了它 generate() 才不会在图层的第二次尝试里把同一条坏输出再问两遍
    # （那等于 4 次调用换一次拒答）。见 node_generate.generate 的短路分支。
    generate_schema_failed: bool
    # 这条 SQL 是谁产出的：generate / react / react-fallback-generate。
    # 必须落进 state，否则"开了 ReAct 到底有没有用"只能靠日志猜。
    generate_via: str

    # --- prompt 能力开关的产出（默认关闭时为空）---
    # 这些字段**必须显式声明**：LangGraph 按 TypedDict 的注解建 channel，
    # 没声明的键会被静默丢弃。踩过一次：plan 节点明明进了链路，
    # 返回的 state 里却连 plan 键都没有，看起来像"节点没生效"。
    plan: dict                       # Planning 阶段的计划 JSON
    plan_issues: list[dict]          # 计划校验问题（REJECT/WARN）
    plan_meta: dict                  # 计划阶段的 token / 成本
    plan_valid: bool                 # 计划未被 REJECT
    plan_need_clarify: bool          # 模型判定问题有歧义，需要问用户
    react_trace: list[dict]          # ReAct 每步动作与观察

    # --- P1-2 真 HITL：中断与恢复 ---
    # 带 thread_id 才走 checkpointer 与 interrupt；为空 = 老的单轮路径（CLI/评测）。
    thread_id: str
    clarify_question: str            # 暂停时问用户的那句话
    clarify_answer: str              # 恢复时用户补的那句话（空 = 没补）
    interrupted: bool                # 本次请求停在 plan，等用户回答；没有答案
    duplicate_resume: bool           # 重复 resume：直接回放最终状态，没有重跑图
    hitl: dict                       # {thread_id, stage, ...}，进审计 trace_json

    # --- validate 产出 ---
    findings: list[GuardFinding]
    verdict: Verdict
    attempts: int

    # --- C 阶段：每轮尝试的完整记录 ---
    # history 每轮追加一条 {attempt, sql, verdict, rejects}，
    # 答辩要展示的"被拦 → 改 → 过"就是这个列表。
    history: Annotated[list[dict], operator.add]
    fix_log: Annotated[list[str], operator.add]      # 机械修复实际做了哪些替换

    # --- execute 产出 ---
    result: QueryResult
    compare_result: dict             # 对比基线复算结果（cur/base/delta/pct）
    cache_hit: bool                  # 命中答案缓存：0 token、0 查询、0 护栏
    cache_key: str                   # 命中的缓存键（前 12 位，便于排查串号）

    # --- 阶段 3：答案形态契约（shape.py / node_critique）---
    shape: dict                      # 契约快照 {shape,label,min_rows,...}
    shape_issues: list[str]          # 结果形状与契约不符的具体理由
    shape_retries: int               # 因形态不符发起的返工次数（上限 1）
    shape_retry_now: bool            # 本次 critique 刚发起返工，图据此回 generate
    shape_unresolved: bool           # 返工上限用尽仍不符，答案里要如实说明

    # --- narrate 产出 ---
    answer: str
    refused: bool
    refuse_reason: str
    # 答案可信度（trust.py）：四项自检 + 链路信号 → 高/中/低。
    # 放在 state 里而不是塞进答案文本，是因为前端要按级别上色，
    # 会话快照要能原样恢复（`_SNAP_KEYS` 里有它）。
    trust: dict

    # 口径版本（semantic.caliber_digest 的内容指纹）。
    # 单独存一份而不是只留在答案文本里：前端要能把它挂到答案卡上，
    # 审计回放要能按版本过滤（"这版口径之后答的题"），
    # 会话恢复也要原样还原——都靠这个字段，不是靠正则去答案里抠。
    caliber_version: str

    # --- 全程 ---
    # 用 operator.add 做 reducer：节点只管 append 自己那一条，不用读改写
    trace: Annotated[list[TraceStep], operator.add]
