"""阶段 1 核心契约：全链路只认这四个类型（+ RowRule 这个策略原子）。

设计依据 `docs/36-ADR.md`：
  ADR-002 内置 RBAC + 可扩展 IdentityProvider，tenant_id 从第一天预留；
  ADR-005 权限在 SQL 编译期下推；
  ADR-006 每次问答是一条不可变审计事件；
  ADR-008 旧调用路径保持兼容（缺省内部基线上下文）。

关于 `RequestContext.system()`：
  它不是"匿名越权"，是给 CLI / 评测 / 自测用的**内部基线通道**。
  web 侧必须登录拿到真实 ctx，且 `webapp.py` 里禁止构造 system 上下文。
  这样 42 题评测不受权限下推影响，同时生产链路没有无身份入口。
"""
from __future__ import annotations

import datetime as _dt
import hashlib
import json
import uuid
from dataclasses import dataclass, field
from typing import Any

from . import pii


def new_trace_id() -> str:
    """可读、可排序、够唯一：20261110-094700-a1b2c3d4。"""
    now = _dt.datetime.now().strftime("%Y%m%d-%H%M%S")
    return "%s-%s" % (now, uuid.uuid4().hex[:8])


def _utcnow() -> str:
    return _dt.datetime.now(_dt.timezone.utc).isoformat(timespec="milliseconds")


def stable_hash(obj: Any) -> str:
    """给缓存 key 与策略版本用的稳定哈希（dict 排序后再哈希）。"""
    raw = json.dumps(obj, ensure_ascii=False, sort_keys=True, default=str)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]


# ---------------------------------------------------------------- token / 成本
def _sum_tokens(trace: list[dict]) -> tuple[int, int]:
    """从 trace 里累加 token。

    两种形状都要认，因为调用点是分批加进来的：
      · `data.tokens_in`          —— 生成 / 元问题节点显式摊到顶层（当前写法）
      · `data.meta.prompt_tokens` —— 早期只把 usage 塞进 meta 的旧 trace
    只认一种就会出现「成本恒为 0」这种静默失效，所以这里做兼容而不是二选一。
    """
    tin = tout = 0
    for t in trace or []:
        d = t.get("data") or {}
        meta = d.get("meta") or {}
        tin += int(d.get("tokens_in") or meta.get("prompt_tokens") or 0)
        tout += int(d.get("tokens_out") or meta.get("completion_tokens") or 0)
    return tin, tout


def _cost_cny(tin: int, tout: int, cfg: dict | None) -> float:
    """按配置的「每百万 token 单价」换算成本，落库精度 6 位（与 numeric(12,6) 对齐）。

    单价放配置里而不是写死：换模型 / 供应商调价时不该改代码。
    缺配置就返回 0，不猜价格——猜出来的成本比没有成本更糟。
    """
    c = ((cfg or {}).get("cost") or {})
    try:
        pin = float(c.get("price_in_per_mtok") or 0)
        pout = float(c.get("price_out_per_mtok") or 0)
    except (TypeError, ValueError):
        return 0.0
    if not pin and not pout:
        return 0.0
    return round((tin * pin + tout * pout) / 1_000_000.0, 6)


def cost_cny(tin: int, tout: int, cfg: dict | None = None) -> float:
    """对外入口：网关台账与审计事件必须用**同一套**换算。

    曾经两边各写一遍，结果同一次调用在 qa_logs 和 cost_ledger 里差了几分钱，
    对账时查了半天才发现是两处各自 round。同一份价格表只许有一个实现。
    """
    return _cost_cny(tin, tout, cfg)


# ---------------------------------------------------------------- 请求上下文
@dataclass(frozen=True)
class RequestContext:
    """一次问答的身份上下文。

    `role` 与 `scope` 是**两个正交维度**（ADR-009）：
      role  = 岗位（数据分析师 / 数据开发工程师 / 数据产品经理）→ 决定能看到哪些层级、哪些列、多少行
      scope = 数据范围（如 region=east）→ 决定能看到哪些行
    把两者混成一个角色（曾经的 region_east、report_readonly）会导致角色爆炸：
    每多一个地域就多一个角色，每多一个岗位再乘一遍。拆开后是相加而不是相乘。
    """
    trace_id: str
    user_id: str
    tenant_id: str
    role: str
    scope: dict = field(default_factory=dict)   # 数据范围，如 {"region": "east"}
    source: str = "cli"          # cli | eval | web | api
    locale: str = "zh-CN"
    budget_tokens: int = 0       # 0 = 不限；阶段 4 网关按它限流
    unrestricted: bool = False   # 仅内部基线通道，策略引擎据此跳过下推

    @classmethod
    def system(cls, source: str = "cli", **kw) -> "RequestContext":
        """CLI / eval / smoke / selftest 用。保持 ADR-008 的旧调用兼容。"""
        return cls(
            trace_id=kw.pop("trace_id", None) or new_trace_id(),
            user_id=kw.pop("user_id", "internal-baseline"),
            tenant_id=kw.pop("tenant_id", "default"),
            role=kw.pop("role", "system"),
            source=source,
            unrestricted=True,
            **kw,
        )

    def to_dict(self) -> dict:
        return {
            "trace_id": self.trace_id, "user_id": self.user_id,
            "tenant_id": self.tenant_id, "role": self.role,
            "scope": dict(self.scope or {}),
            "source": self.source, "locale": self.locale,
            "budget_tokens": self.budget_tokens,
            "unrestricted": self.unrestricted,
        }


# ---------------------------------------------------------------- 策略原子
@dataclass(frozen=True)
class RowRule:
    """一条行级策略，用结构化字段存，不存 SQL 片段——避免把字符串拼进 SQL。

    table/column 会被白名单校验；values 在编译时逐项转义单引号。
    """
    table: str
    column: str
    op: str = "in"                 # in | eq
    values: tuple[str, ...] = ()

    def to_dict(self) -> dict:
        return {"table": self.table, "column": self.column,
                "op": self.op, "values": list(self.values)}


# ---------------------------------------------------------------- 策略判定
@dataclass(frozen=True)
class PolicyDecision:
    role: str
    allow_tables: frozenset[str] = frozenset()
    deny_tables: frozenset[str] = frozenset()
    row_rules: tuple[RowRule, ...] = ()
    deny_columns: frozenset[str] = frozenset()
    mask_columns: frozenset[str] = frozenset()
    max_rows: int = 5000
    read_only: bool = True         # 岗位能力：本系统所有岗位一律只读，显式声明而非默认
    scope: dict = field(default_factory=dict)   # 数据范围，参与 policy_hash
    unrestricted: bool = False
    policy_hash: str = ""
    reason: str = ""

    @classmethod
    def unrestricted_for(cls, role: str = "system") -> "PolicyDecision":
        return cls(role=role, unrestricted=True,
                   policy_hash=stable_hash({"role": role, "unrestricted": True}),
                   reason="内部基线通道：跳过权限下推（CLI / eval / selftest）")

    @classmethod
    def build(cls, *, role: str, allow_tables=(), deny_tables=(),
              row_rules=(), deny_columns=(), mask_columns=(),
              max_rows: int = 5000, read_only: bool = True,
              scope: dict | None = None, reason: str = "") -> "PolicyDecision":
        """构造时自动算 policy_hash——缓存 key 必须包含它（ADR-005）。

        scope 一定要进 hash：两个同为 data_analyst 但地域不同的用户，
        可见行不同，若 hash 相同就会命中同一份缓存结果，等于越权。
        """
        rr = tuple(row_rules)
        sc = dict(scope or {})
        payload = {
            "role": role,
            "allow": sorted(allow_tables), "deny": sorted(deny_tables),
            "rows": [r.to_dict() for r in rr],
            "deny_cols": sorted(deny_columns), "mask_cols": sorted(mask_columns),
            "max_rows": max_rows, "read_only": read_only,
            "scope": sc,
        }
        return cls(
            role=role,
            allow_tables=frozenset(allow_tables),
            deny_tables=frozenset(deny_tables),
            row_rules=rr,
            deny_columns=frozenset(deny_columns),
            mask_columns=frozenset(mask_columns),
            max_rows=max_rows,
            read_only=read_only,
            scope=sc,
            policy_hash=stable_hash(payload),
            reason=reason,
        )

    @classmethod
    def from_dict(cls, d: dict) -> "PolicyDecision":
        """state 里存的是 dict（要能 JSON 化），执行节点用它还原成对象。"""
        if not d:
            return cls.unrestricted_for()
        return cls(
            role=d.get("role") or "",
            allow_tables=frozenset(d.get("allow_tables") or ()),
            deny_tables=frozenset(d.get("deny_tables") or ()),
            row_rules=tuple(RowRule(**r) for r in (d.get("row_rules") or [])),
            deny_columns=frozenset(d.get("deny_columns") or ()),
            mask_columns=frozenset(d.get("mask_columns") or ()),
            max_rows=int(d.get("max_rows") or 5000),
            read_only=bool(d.get("read_only", True)),
            scope=dict(d.get("scope") or {}),
            unrestricted=bool(d.get("unrestricted")),
            policy_hash=d.get("policy_hash") or "",
            reason=d.get("reason") or "",
        )

    def to_dict(self) -> dict:
        return {
            "role": self.role, "allow_tables": sorted(self.allow_tables),
            "deny_tables": sorted(self.deny_tables),
            "row_rules": [r.to_dict() for r in self.row_rules],
            "deny_columns": sorted(self.deny_columns),
            "mask_columns": sorted(self.mask_columns),
            "max_rows": self.max_rows, "read_only": self.read_only,
            "scope": dict(self.scope or {}),
            "unrestricted": self.unrestricted,
            "policy_hash": self.policy_hash, "reason": self.reason,
        }


def as_decision(d) -> PolicyDecision:
    """把 state 里的 dict / 半成品还原成 PolicyDecision。

    AgentState 必须能 JSON 化，所以 `policy_decision` 在 state 里是 dict；
    每个节点要用它时都得先过这一层。少了这步的后果不是报错而是 AttributeError
    （`'dict' object has no attribute 'mask_columns'`）——已经踩过一次，
    所以统一在这里兜住，节点里不许再写 `if isinstance(...)`。
    """
    if isinstance(d, PolicyDecision):
        return d
    if not d:
        return PolicyDecision.unrestricted_for()
    return PolicyDecision.from_dict(d)


# ---------------------------------------------------------------- 查询计划
@dataclass(frozen=True)
class QueryPlan:
    """一次问数的结构化计划。阶段 3 的混合生成靠它判定走模板还是走 LLM。"""
    sql: str = ""
    tables: tuple[str, ...] = ()
    metrics: tuple[str, ...] = ()
    time_start: str | None = None
    time_end: str | None = None
    engine: str = "doris"
    estimated_rows: int | None = None

    def to_dict(self) -> dict:
        return {"sql": self.sql, "tables": list(self.tables),
                "metrics": list(self.metrics),
                "time": [self.time_start, self.time_end],
                "engine": self.engine, "estimated_rows": self.estimated_rows}


# ---------------------------------------------------------------- 审计事件
@dataclass
class AuditEvent:
    """ADR-006：不可变。只 INSERT，不提供 update/delete 接口。

    元问题回答、拒答、执行失败同样落盘；`intent` 用来区分这四类。
    """
    trace_id: str
    occurred_at: str = field(default_factory=_utcnow)
    tenant_id: str = "default"
    user_id: str = ""
    role: str = ""
    source: str = ""
    intent: str = "query"          # query | meta | refuse | error
    question: str = ""
    time_start: str | None = None
    time_end: str | None = None
    sql_versions: list[dict] = field(default_factory=list)
    final_sql: str | None = None
    applied_sql: str | None = None    # 策略注入后的 SQL（真正执行的那版）
    tables: list[str] = field(default_factory=list)
    row_count: int = 0
    elapsed_ms: int = 0
    tokens_in: int = 0
    tokens_out: int = 0
    cost_cny: float = 0.0
    verdict: str = ""
    refused: bool = False
    refuse_reason: str | None = None
    answer_excerpt: str = ""
    pii_hits: int = 0                 # 问句/答案里被脱敏的 PII 个数
    policy_hash: str = ""
    trace_json: dict = field(default_factory=dict)
    error: str | None = None

    def to_dict(self) -> dict:
        return {
            "trace_id": self.trace_id, "occurred_at": self.occurred_at,
            "tenant_id": self.tenant_id, "user_id": self.user_id,
            "role": self.role, "source": self.source, "intent": self.intent,
            "question": self.question, "time_start": self.time_start,
            "time_end": self.time_end, "sql_versions": self.sql_versions,
            "final_sql": self.final_sql, "applied_sql": self.applied_sql,
            "tables": self.tables, "row_count": self.row_count,
            "elapsed_ms": self.elapsed_ms, "tokens_in": self.tokens_in,
            "tokens_out": self.tokens_out, "cost_cny": self.cost_cny,
            "verdict": self.verdict, "refused": self.refused,
            "refuse_reason": self.refuse_reason,
            "answer_excerpt": self.answer_excerpt,
            "pii_hits": self.pii_hits,
            "policy_hash": self.policy_hash, "trace_json": self.trace_json,
            "error": self.error,
        }

    @classmethod
    def from_state(cls, state: dict, ctx: RequestContext,
                   decision: PolicyDecision | None = None,
                   cfg: dict | None = None) -> "AuditEvent":
        """把 LangGraph 的最终 state 压成一条审计事件。

        这是唯一允许的构造入口——保证字段不散落在各节点里各写一半。
        """
        res = state.get("result") or {}
        history = state.get("history") or []
        mi = state.get("meta_intent") or {}
        if state.get("interrupted"):
            # P1-2：图暂停在 clarify，本次**没有答案**，但它是一条真实事件：
            # 不落盘的话，"这次为什么没出数"在审计里就消失了，用户也无从复盘。
            intent = "clarify"
        elif state.get("refused"):
            intent = "refuse"
        elif res.get("error"):
            intent = "error"
        elif state.get("meta_answer"):
            intent = "meta"
        else:
            intent = "query"

        tin, tout = _sum_tokens(state.get("trace") or [])
        cost = _cost_cny(tin, tout, cfg)

        # 落盘前脱敏（AC-14）。审计表不可变，写进去就擦不掉——
        # 所以"要不要存手机号"这个问题必须在 INSERT 之前回答完。
        q_text, a_text, pii_hits = pii.redact_state(state)
        if state.get("interrupted"):
            # 中断时 answer 是空的，但"我们问用户什么"必须留在审计里；
            # 这段是模型写的，一样要脱敏。
            a_text, h3 = pii.redact((state.get("clarify_question") or "")[:500])
            pii_hits += h3

        return cls(
            trace_id=ctx.trace_id,
            tenant_id=ctx.tenant_id,
            user_id=ctx.user_id,
            role=ctx.role,
            source=ctx.source,
            intent=intent,
            question=q_text,
            time_start=state.get("time_start"),
            time_end=state.get("time_end"),
            sql_versions=[{"attempt": h.get("attempt"), "stage": h.get("stage"),
                           "sql": h.get("sql"), "verdict": h.get("verdict"),
                           "rejects": h.get("rejects")}
                          for h in history],
            final_sql=state.get("sql") or None,
            applied_sql=state.get("applied_sql") or None,
            tables=list(state.get("tables") or []),
            row_count=int(res.get("row_count") or 0),
            elapsed_ms=int(state.get("elapsed_ms") or res.get("elapsed_ms") or 0),
            tokens_in=tin, tokens_out=tout, cost_cny=cost,
            verdict=state.get("verdict") or "",
            refused=bool(state.get("refused")),
            refuse_reason=state.get("refuse_reason") or res.get("error") or None,
            answer_excerpt=a_text,
            pii_hits=pii_hits,
            policy_hash=(decision.policy_hash if decision else ""),
            trace_json={"trace": state.get("trace") or [],
                        "fix_log": state.get("fix_log") or [],
                        "findings": state.get("findings") or [],
                        "time_window": state.get("time_window") or {},
                        # P1-2：中断与恢复是两条独立审计事件，
                        # thread_id 是它们之间唯一的关联键。
                        "hitl": state.get("hitl") or {},
                        "meta_kind": mi.get("kind")},
            error=res.get("error") or state.get("generate_error") or None,
        )
