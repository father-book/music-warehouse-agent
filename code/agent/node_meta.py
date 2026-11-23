"""meta 节点：让 Agent 在「不该写 SQL」的时候也能像个助手。

用户实测批评：问「你能做什么」「帮我用 tensorflow」这类问题，
系统仍然硬走 SQL 生成链路，模型编出不存在的列，执行报 Unknown column
再把原始错误甩给界面。这不是「保守」，是**失败得太生硬**。

这里分三类，全部不查数仓：

  help      能力自述。从语义层**动态**统计表/指标/维度/数据范围，不硬编码数字。
  explain   口径解释。问「DAU 是什么 / 完播率怎么算」时直接返回语义层定义。
  offscope  与数仓无关的请求。交给 LLM 正常回答，但答案开头显式声明
            「未查询数仓」，绝不把通用知识伪装成项目数据。

判定放在 SQL 生成之前：命中元意图就直接出答案，不进护栏、不进执行，
也就不会出现「为了回答『你是谁』去编一条 SELECT」这种荒唐事。
"""
from __future__ import annotations

import _bootstrap  # noqa: F401

import re
from collections import Counter

import gateway
import llm
import semantic

# 有这些词就是**要算数**，一律走问数链路，不许被元意图劫持
_RE_TIME = re.compile(
    r"\d{4}-\d|\d{1,2}\s*[-/月]\s*\d|昨天|昨日|前天|今天|最近|近\s*\d|"
    r"上周|上月|上个月|本周|本月|这个月|季度|\d+\s*天|\d+\s*小时|~|至|到")

_RE_OFFSCOPE = re.compile(
    r"tensorflow|pytorch|sklearn|keras|transformer|写代码|写.{0,3}代码|编程|"
    r"翻译|画图|画一张|画个|讲个笑话|写作文|写诗|爬虫|训练模型|部署|"
    r"做游戏|做网站|开发.{0,6}(app|小程序|系统|网站)|帮我装|配环境|"
    r"install|pip|docker|spring|java|react|vue", re.I)

_RE_HELP = re.compile(
    r"你能\s*(做|干|回答|查|提供|帮)|你会\s*(什么|哪些)|可以\s*(做|回答|查)\s*什么|"
    r"支持\s*(哪些|什么)|有哪些功能|功能介绍|使用说明|怎么用|如何使用|"
    r"你是谁|介绍一下\s*(你|自己|这个项目|系统)|能问什么|问什么|怎么问|"
    r"能力|help|帮助", re.I)

_RE_EXPLAIN = re.compile(r"是什么|什么意思|怎么算|如何计算|口径|定义|指的是|含义|区别")

_GENERAL_SYSTEM = """\
你是「音乐 App 数仓问数助手」，现在用户问了一个与本数仓无关的通用问题。
请用简洁、友好、专业的中文回答他，不要假装查过数据，不要编造本项目的任何数值。
最后用一句话把话题引回：我能帮你查 DAU、播放量、收入等指标，支持指定日期和时间区间。
"""


def detect_meta(q: str, cfg: dict) -> dict | None:
    """返回 {kind, ...} 或 None（None = 正常问数）。"""
    text = (q or "").strip()
    if not text:
        return None

    # 1) 明确的非数仓请求，优先级最高（哪怕句子里带了数字）
    m = _RE_OFFSCOPE.search(text)
    if m:
        return {"kind": "offscope", "matched": m.group(0)}

    # 2) 带时间表达的一律是问数，防止「11-08 的 DAU 是什么」被当成口径解释
    has_time = bool(_RE_TIME.search(text))

    # 3) 口径解释：无时间词 + 命中语义层指标
    if not has_time and _RE_EXPLAIN.search(text):
        hits = semantic.find_metrics(text, cfg, top=1)
        if hits:
            return {"kind": "explain", "code": hits[0]["code"],
                    "matched_by": hits[0].get("matched_by")}

    # 4) 能力自述
    if not has_time and _RE_HELP.search(text):
        return {"kind": "help", "matched": _RE_HELP.search(text).group(0)}

    return None


# ---------------------------------------------------------------- 三种答案
def _help_text(cfg: dict) -> str:
    tables = semantic.all_tables(cfg)
    metrics = semantic.all_metrics(cfg)
    dims = semantic.all_dimensions(cfg)
    off = cfg["lane"]["offline"]
    layers = Counter(str(t.get("layer") or "?") for t in tables.values())
    layer_desc = " / ".join("%s %d 张" % (k, v) for k, v in sorted(layers.items()))
    # 挑几个最有代表性的指标，按语义层实际名字列，不硬编码
    picks = [c for c in ("uv", "play_start_cnt", "complete_rate", "revenue_sum",
                         "gross_revenue", "arpu", "interact_cnt", "retention_d1")
             if c in metrics]
    names = "、".join((metrics[c].get("name_cn") or c) for c in picks)

    return "\n".join([
        "我是这个音乐 App 数仓的**问数助手**：只回答能用数据算出来的问题，"
        "每个答案都附出处、口径和可复算的 SQL。",
        "",
        "【我能查什么】",
        "· %d 张表（%s），执行面固定 `%s.%s`" % (
            len(tables), layer_desc, cfg["execution"]["catalog"], cfg["execution"]["database"]),
        "· %d 个指标：%s 等" % (len(metrics), names),
        "· %d 个维度：%s 等" % (
            len(dims), "、".join(list(dims)[:8])),
        "· 数据范围 %s ~ %s（T+1 离线，35 个分区）" % (
            off.get("data_since"), off["data_until"]),
        "",
        "【怎么问】",
        "· 单日：`2026-11-08 的 DAU 是多少`",
        "· 区间：`最近7天的播放量`、`上个月各省份的活跃用户数`",
        "· 明细：`11-08 有哪些用户启动了 App`",
        "· 口径：`DAU 是什么`、`完播率怎么算`",
        "· 金额：`昨天消费最多的用户是谁`、`11-08 收入金额`",
        "",
        "【我会明确说明边界，而不是硬凑】",
        "· 实时数据（今天 / 最近一小时）——实时链路停在 %s" % off["data_until"],
        "· 超出数据范围的日期、写操作、跨 catalog 查询、未收敛的 SCD2 JOIN",
        "  这四类会被 20 条护栏拦下，并告诉你卡在哪条、可以怎么改。",
        "",
        "直接问我一个带日期或时间范围的业务问题就行。",
    ])


def _explain_text(code: str, cfg: dict) -> str:
    body = semantic.explain_metric(code, cfg)
    return "\n".join([
        "以下是语义层里登记的口径定义（**不含具体数值**）：",
        "",
        body,
        "",
        "要算具体数字，请带上日期或时间范围，例如："
        "`2026-11-08 的%s是多少`。" % ((semantic.get_metric(code, cfg) or {}).get("name_cn") or code),
    ])


def _general_text(q: str, cfg: dict, ctx: dict | None = None) -> tuple[str, dict]:
    """返回 (答案, usage)。第三方问题也走模型，成本必须一起计。"""
    if not (cfg.get("meta") or {}).get("allow_general_chat", True):
        return _help_text(cfg), {}
    usage: dict = {}
    try:
        # 元问题同样经网关：它也是模型调用，凭什么不进台账。
        out = gateway.chat(
            [{"role": "system", "content": _GENERAL_SYSTEM},
             {"role": "user", "content": q}],
            cfg,
            caller="meta",
            ctx=ctx,
            max_tokens=int((cfg.get("meta") or {}).get("general_chat_max_tokens", 400)),
        )
        body = (out.get("content") or "").strip()
        usage = {"tokens_in": int((out.get("usage") or {}).get("prompt_tokens") or 0),
                 "tokens_out": int((out.get("usage") or {}).get("completion_tokens") or 0),
                 "model": out.get("model"), "elapsed_ms": out.get("elapsed_ms"),
                 "cost_cny": out.get("cost_cny"), "ledger_ok": out.get("ledger_ok"),
                 "ledger_error": out.get("ledger_error")}
    except Exception as e:                       # 模型挂了也不能让页面崩
        body = "（通用回答暂时不可用：%s）" % type(e).__name__
    return "\n".join([
        "> **通用回答 · 未查询数仓**，以下内容不是本项目的数据结论。",
        "",
        body,
        "",
        "---",
        _help_text(cfg).split("\n")[0],
        "例如：`2026-11-08 的 DAU 是多少`、`最近7天的播放量`。",
    ]), usage


def answer_meta(state: dict, cfg: dict) -> dict:
    mi = state.get("meta_intent") or {}
    kind = mi.get("kind") or "help"
    q = state.get("question") or ""
    usage: dict = {}
    if kind == "explain":
        text = _explain_text(mi.get("code") or "", cfg)
    elif kind == "offscope":
        text, usage = _general_text(q, cfg, state.get("ctx"))
    else:
        text = _help_text(cfg)
    return {
        "answer": text,
        "meta_answer": True,
        "refused": False,
        "result": {},
        "trace": [{"node": "meta", "detail": "直接回答（%s，未生成 SQL）" % kind,
                   "data": {"meta_intent": mi, **usage}}],
    }
