"""Function Calling 工具集：让模型自己决定"先查什么、再查什么"。

为什么只有 4 个工具，且**没有 run_sql**
--------------------------------------
执行 SQL 在这个项目里要过执行闸门（金额类需二次确认令牌、扫描量要过成本闸、
结果要落审计）。把这些塞进一个 model 可自主调用的工具里，等于让模型自己签发
自己的通行证——一次死循环就能扫穿全表，而台账上只看到"调了 run_sql 4 次"。
所以 ReAct 这一层的职责边界是**规划与自检**，不是执行：

  search_semantic  我该看哪些表/指标        （召回，纯本地）
  get_metric_def   这个指标口径到底是什么    （语义层，纯本地）
  validate_sql     我写的这版过不过护栏      （子进程护栏，纯本地）
  submit_sql       我认为这就是最终答案      （终止信号，不执行）

四步全在本地，零外部副作用，也不碰 Doris。模型可以放开手脚自己试错，
错了护栏立刻告诉它——这正是 function calling 该用的地方。
"""
from __future__ import annotations

import _bootstrap  # noqa: F401

import json

import guardtool
import retrieval
import semantic

# 单个工具返回给模型的最大字符数。超了截断：工具结果是上下文预算的消耗大户，
# 一次返回 3000 字的候选表列表会把真正的业务问题挤出去。
MAX_RESULT_CHARS = 1400


def _clip(s: str) -> str:
    s = s or ""
    return s if len(s) <= MAX_RESULT_CHARS else s[:MAX_RESULT_CHARS] + "…（已截断）"


# ---------------------------------------------------------------- 声明
SPECS: list[dict] = [
    {
        "type": "function",
        "function": {
            "name": "search_semantic",
            "description": "在语义层里检索相关的表或指标。不确定该用哪张表/哪个指标时先调它。",
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {"type": "string", "description": "检索词，用业务说法，例如 各省份播放量"},
                    "kind": {"type": "string", "enum": ["table", "metric"],
                             "description": "table=找表，metric=找指标"},
                },
                "required": ["query", "kind"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_metric_def",
            "description": "查一个指标码的完整口径：计算公式、源表、过滤条件、可加性。写 SQL 前必须核对。",
            "parameters": {
                "type": "object",
                "properties": {
                    "code": {"type": "string", "description": "指标码，例如 uv / net_revenue"},
                },
                "required": ["code"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "validate_sql",
            "description": "把写好的 SQL 交给 27 条静态护栏检查。返回被拦下的规则和改法。只检查，不执行。",
            "parameters": {
                "type": "object",
                "properties": {
                    "sql": {"type": "string", "description": "完整单条 SQL"},
                },
                "required": ["sql"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "submit_sql",
            "description": "提交最终 SQL。调用它表示你已经确认这版是对的，会结束本轮。",
            "parameters": {
                "type": "object",
                "properties": {
                    "sql": {"type": "string", "description": "最终 SQL"},
                    "reason": {"type": "string", "description": "一句话说明口径选择"},
                },
                "required": ["sql"],
            },
        },
    },
]


# ---------------------------------------------------------------- 实现
def _search_semantic(args: dict, cfg: dict) -> str:
    q = str(args.get("query") or "").strip()
    kind = str(args.get("kind") or "table")
    if not q:
        return "错误：query 不能为空"
    if kind == "metric":
        hits = retrieval.search_metrics(q, cfg, top=3)
        if not hits:
            return "没有匹配的指标。换一种业务说法再试，或直接用 search_semantic 找表。"
        return "\n".join("%s（%s）｜ 分 %.1f" % (h["code"], h["name_cn"], h["score"])
                         for h in hits)
    hits = retrieval.search_tables(q, cfg, top=4)
    if not hits:
        return "没有匹配的表。"
    lines = []
    for h in hits:
        t = semantic.get_table(h["table"], cfg) or {}
        lines.append("%s（%s）｜ %s 层 ｜ 粒度：%s" % (
            h["table"], h.get("name_cn") or "", t.get("layer") or "-",
            (t.get("grain") or "-")[:40]))
        for cav in (t.get("caveats") or [])[:2]:
            lines.append("   注意：%s" % str(cav)[:90])
    return _clip("\n".join(lines))


def _get_metric_def(args: dict, cfg: dict) -> str:
    code = str(args.get("code") or "").strip()
    if not code:
        return "错误：code 不能为空"
    m = semantic.get_metric(code, cfg)
    if not m:
        # 给近似候选，别让模型卡死在一个写错的指标码上
        near = retrieval.search_metrics(code, cfg, top=3)
        if near:
            return "没有指标 %s。相近的有：%s" % (
                code, "、".join("%s（%s）" % (x["code"], x["name_cn"]) for x in near))
        return "没有指标 %s。" % code
    return _clip(semantic.explain_metric(code, cfg))


def _validate_sql(args: dict, cfg: dict, question: str = "") -> str:
    sql = str(args.get("sql") or "").strip()
    if not sql:
        return "错误：sql 不能为空"
    verdict, findings, stderr = guardtool.check(sql, cfg, question=question)
    rejects = [f for f in findings if f.get("level") == "REJECT"]
    warns = [f for f in findings if f.get("level") == "WARN"]
    if verdict == "pass" and not rejects:
        extra = ("（另有 %d 条提示）" % len(warns)) if warns else ""
        return "通过：这版 SQL 没有触发任何拦截%s。可以 submit_sql。" % extra
    lines = ["未通过（verdict=%s）：" % verdict]
    for f in (rejects or warns)[:6]:
        lines.append("- [%s] %s" % (f.get("rule"), str(f.get("message"))[:120]))
        fx = f.get("fix") or {}
        if fx.get("replacement"):
            lines.append("  改法：%s → %s" % (fx.get("op"), str(fx.get("replacement"))[:80]))
    if stderr.strip():
        lines.append("解析器报错：%s" % stderr.strip()[:160])
    return _clip("\n".join(lines))


def execute(name: str, args: dict, cfg: dict, question: str = "") -> tuple[str, bool]:
    """返回 (结果文本, 是否终止)。

    任何工具抛异常都转成文本返回给模型——让它看到"这个工具炸了"然后自己换路子，
    比整个问数崩掉好。这也符合 ReAct 的本意：观察结果也是信息。
    """
    try:
        if name == "search_semantic":
            return _search_semantic(args, cfg), False
        if name == "get_metric_def":
            return _get_metric_def(args, cfg), False
        if name == "validate_sql":
            return _validate_sql(args, cfg, question), False
        if name == "submit_sql":
            return "已提交。", True
        return "错误：未知工具 %s" % name, False
    except Exception as e:                       # noqa: BLE001
        return "工具执行异常：%s: %s" % (type(e).__name__, str(e)[:160]), False


def parse_args(raw) -> dict:
    """工具参数是模型生成的 JSON 字符串，可能带围栏或残缺。解析失败就当空对象，
    让工具自己报"参数缺失"——比在这里抛异常清楚。"""
    if isinstance(raw, dict):
        return raw
    s = str(raw or "").strip()
    if not s:
        return {}
    if s.startswith("```"):
        s = s.strip("`")
        s = s.split("\n", 1)[-1] if "\n" in s else s
    try:
        v = json.loads(s)
        return v if isinstance(v, dict) else {}
    except Exception:
        return {}
