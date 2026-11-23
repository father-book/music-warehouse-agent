"""结构化输出的契约层：模型交什么货才算"合格"。

为什么要有这一层
----------------
`llm.parse_json_object` 只保证"括号配平、能 json.loads"，不保证字段齐不齐、
类型对不对、有没有多余字段、`sql` 是不是空串或一句自然语言。这四种坏输出
以前都能一路走到 validate，靠护栏兜着。

护栏兜得住语法，兜不住另外两类：
  · `{"sql": "查询 2026-11-08 的 DAU"}` —— 不是 SQL，护栏解析直接抛异常，
    变成一条"护栏内部错误"，看起来像我们的 bug，不像模型的 bug；
  · `{"reason": {...}}` —— reason 是对象，`[:80]` 切片直接 TypeError。

所以分工要分清：
    schema 判"模型有没有按合同交货"（字段/类型/形态）
    护栏  判"这条 SQL 对不对"（口径/分区/权限/成本）
两者都在 generate 之后、execute 之前，但管的是两件事，不要合并，也不要用
schema 去替护栏做判断（比如"指标选对没有"——那需要语义层，schema 不该知道）。

三条设计约束
------------
1. **不绕开 gateway**。不用 `with_structured_output`：它会换掉调用出口，
   台账、预算、限流、出网校验一起丢。这里只对已经拿回来的 `content` 做本地
   校验，取数仍然走 `gateway.chat`。
2. **报错要能贴给人看**。pydantic 的 `ValidationError` 有一屏长，
   `brief_errors()` 只抽前 N 条「字段: 原因」，写进 trace.detail。
3. **多 JSON 视为不合格**。两个对象 = 两个候选答案，静默取第一个是掷硬币。
   `llm.parse_json_object` 的括号配平扫描会"抓到第一个就跑"，那是它的职责
   （脏输出兜底）；到了契约层就得严格：宁可重试一次，也不要猜。
"""
from __future__ import annotations

import json
import re
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator

__all__ = [
    "SchemaError", "SQLDraft", "QueryPlan",
    "clean_sql", "extract_object", "parse_sql_draft", "parse_query_plan",
    "brief_errors",
]

_RE_FENCE = re.compile(r"^\s*```[a-zA-Z]*\s*|\s*```\s*$")
# 与护栏 R1 同一判据：问数模式首关键字必须是 SELECT / WITH。
# 允许 SQL 前面有注释行——注释由 R2 单独拦，这里不越权。
_RE_HEAD = re.compile(r"^(?:--[^\n]*\n\s*)*(?:\(\s*)*(select|with)\b", re.I)


class SchemaError(ValueError):
    """模型输出不是"恰好一个合规的 JSON 对象"。

    和 `ValidationError` 分开：这个是"连 JSON 对象本身都不对"（没有、多个、
    不完整、顶层不是对象），那个是"JSON 对但字段不合合同"。调用方对两者
    的处理一样（重试一次），但 trace 里的说法要区分开，否则排查时看不出
    是模型没按格式输出，还是我们把合同写错了。
    """


# ---------------------------------------------------------------- SQL 清洗

def clean_sql(s: str) -> str:
    """模型很爱加的三样东西：代码围栏、结尾分号、前后空行。"""
    s = (s or "").strip()
    if s.startswith("```"):
        s = _RE_FENCE.sub("", s).strip()
    # 只去掉最后一个分号；语句中间的分号由护栏 R2 拦
    if s.endswith(";"):
        s = s[:-1].rstrip()
    return s


# ---------------------------------------------------------------- 取 JSON 对象

def _scan_objects(text: str) -> tuple[list[str], bool]:
    """扫出所有**顶层** `{...}` 片段，返回 (片段列表, 是否有未闭合的尾巴)。

    只数深度 0→1 的那一层，所以 `{"plan": {...}, "sql": ...}` 是一个对象；
    字符串里的花括号不参与计数（`'{a}'` 这种字面量很常见）。
    """
    spans: list[str] = []
    depth = start = 0
    started = False
    in_str = esc = False
    for i, ch in enumerate(text):
        if in_str:
            if esc:
                esc = False
            elif ch == "\\":
                esc = True
            elif ch == '"':
                in_str = False
            continue
        if ch == '"':
            in_str = True
        elif ch == "{":
            if depth == 0:
                start, started = i, True
            depth += 1
        elif ch == "}":
            if depth == 0:          # 多余的右括号，忽略
                continue
            depth -= 1
            if depth == 0 and started:
                spans.append(text[start:i + 1])
                started = False
    return spans, depth > 0


def extract_object(content: str) -> dict:
    """从模型输出里取出**唯一**那个 JSON 对象。取不到/取到多个都算不合格。"""
    text = (content or "").strip()
    if text.startswith("```"):
        text = _RE_FENCE.sub("", text).strip()
    if not text:
        raise SchemaError("输出为空")

    # 先整段当 JSON 试。成功时用最严格的口径判：顶层必须是对象，
    # `[{"sql": ...}]` 这类"包了一层的数组"也拒——它是另一个形状，
    # 认了它等于以后任何形状都能通融，合同就没了。
    try:
        obj = json.loads(text)
    except json.JSONDecodeError:
        obj = None
    if obj is not None:
        if not isinstance(obj, dict):
            raise SchemaError("顶层是 %s，不是 JSON 对象" % type(obj).__name__)
        return obj

    # 整段不是合法 JSON：模型在 JSON 前后写了话（"好的，我来分析…"），
    # 这时才动用括号配平扫描。
    spans, unclosed = _scan_objects(text)
    if len(spans) > 1:
        raise SchemaError("输出里有 %d 个 JSON 对象，无法确定用哪个" % len(spans))
    if not spans:
        if unclosed:
            raise SchemaError("JSON 没有闭合（括号没配平）：%s" % text[-80:])
        raise SchemaError("输出里没有 JSON 对象：%s" % text[:120])
    try:
        obj = json.loads(spans[0])
    except json.JSONDecodeError as e:
        raise SchemaError("JSON 不合法：%s" % str(e)[:100])
    if not isinstance(obj, dict):
        raise SchemaError("顶层不是 JSON 对象（是 %s）" % type(obj).__name__)
    return obj


# ---------------------------------------------------------------- 合同

class SQLDraft(BaseModel):
    """生成节点的交付合同。

    字段来源是 `promptlib.OUTPUT_SPEC`（以及 CoT 版的 `plan` 段）。
    `extra="forbid"`：多写字段就重试。听起来苛刻，但多余字段通常意味着模型
    换了一套它自己想象的口径（`confidence`、`metric`、`joins`…），那些值不会
    被任何下游校验读到——静默忽略等于让模型以为自己交了别的东西。

    `used_tables` / `reason` 只进 trace 和 detail，不参与判定，所以给默认值：
    缺了不会让整条答案作废（以前也不会），但**类型错了要拦**——类型错往往不是
    少写一个字段，而是模型把整个对象的结构搞混了。
    """

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    sql: str = Field(min_length=1)
    used_tables: list[str] = Field(default_factory=list)
    metric_code: str | None = None
    reason: str = ""
    assumptions: list[str] = Field(default_factory=list)
    # CoT 的思考段：内容由 validate 侧按 plan.source_table 核对，
    # 这里只透传不校验——schema 不该替语义层判断"这个计划好不好"。
    plan: dict[str, Any] | None = None

    @field_validator("sql", mode="before")
    @classmethod
    def _norm_sql(cls, v: Any) -> str:
        if not isinstance(v, str):
            raise ValueError("必须是字符串，实际是 %s" % type(v).__name__)
        s = clean_sql(v)
        if not s:
            raise ValueError("清洗代码围栏/分号后为空")
        if not _RE_HEAD.match(s):
            raise ValueError("必须以 SELECT 或 WITH 开头，实际开头是 %r" % s[:24])
        return s


class QueryPlan(BaseModel):
    """Planning 节点的交付合同，字段与 `planner.PLAN_SPEC` 一一对应。

    这里**不校验 shape 是否在枚举里**：`planner.validate_plan` 对非法 shape
    只给 WARN（可疑但可继续），schema 若直接 REJECT 就把一条 WARN 升级成了
    硬失败，等于偷偷改了口径。合同只管"字段在不在、类型对不对"。

    不合规时调用方降级为"不带计划照常生成"，不是拒答——Planning 是增强，
    不能变成新的单点故障（见 node_plan 的说明）。
    """

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    intent: str = ""
    metrics: list[str] = Field(default_factory=list)
    dimensions: list[str] = Field(default_factory=list)
    tables: list[str] = Field(default_factory=list)
    filters: list[str] = Field(default_factory=list)
    time_condition: str = ""
    shape: str = ""
    need_clarify: bool = False
    clarify_question: str = ""


# ---------------------------------------------------------------- 入口

def parse_sql_draft(content: str) -> SQLDraft:
    """解析 + 校验。抛 `SchemaError` 或 `ValidationError`，两种都由调用方兜。"""
    return SQLDraft.model_validate(extract_object(content))


def parse_query_plan(content: str) -> QueryPlan:
    return QueryPlan.model_validate(extract_object(content))


def brief_errors(e: Exception, n: int = 2) -> list[str]:
    """把 pydantic 的一屏报错压成 n 条「字段: 原因」。

    只取前 n 条：重试提示里塞十条错，模型的反应通常是挑一条改，剩下的照旧。
    """
    if isinstance(e, ValidationError):
        out = []
        for err in e.errors()[:max(1, n)]:
            loc = ".".join(str(x) for x in (err.get("loc") or [])) or "(顶层)"
            msg = str(err.get("msg") or "").replace("Value error, ", "")
            out.append("%s: %s" % (loc, msg))
        return out or [str(e)[:160]]
    return [str(e)[:160]]
