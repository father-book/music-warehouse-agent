"""route 节点（D 阶段）：问题 → 结构化需求。

三件事，顺序固定：

1. **时间**交给 ``timeparse.parse``：解析用户真正想要的区间，与
   ``[data_since, data_until]`` 求交，裁掉的部分明确记账。实时词 / 完全无数据的
   区间在这里就判不可答——不会让模型拿到一个注定查不出数的窗口。
2. **指标**走 ``semantic.find_metrics``：glossary 精确同义词优先，BM25 补充。
3. **表**以指标 source 为主，倒排检索只作为补充，且**必须命中分组维度才启用**。
   理由：纯指标问题「11-08 的 DAU」在 BM25 里会召出一堆含 dau 列的省份分布表，
   这些表进了 prompt 只会把模型带偏。维度是否命中，由 ``dimensions.yaml`` 判定，
   不靠手写关键词。

A 阶段的原话「宁可拒答，也不要猜」在这里仍然成立，只是「猜」的定义变严了：
把未指定的时间悄悄算成最新分区、把弱召回的表悄悄塞进上下文，都算猜。
"""
from __future__ import annotations

import _bootstrap  # noqa: F401

import retrieval
import datetime as _dt

import realtimelane
import semantic
import timeparse
import node_meta
from governance import security
from governance.contracts import as_decision
from governance.policy import engine as policy_engine
from state import AgentState


def _window_dict(w: timeparse.TimeWindow) -> dict:
    return {
        "label": w.label,
        "kind": w.kind,
        "status": w.status,
        "note": w.note,
        "sql_hint": w.sql_hint(),
        "is_range": w.is_range,
        "requested": ([d.isoformat() for d in w.requested]
                      if w.requested else None),
        "lag_days": w.lag_days,
        # 阶段 4：对比基线。窗口解析出来了就必须一路带下去，
        # 否则 execute 拿不到基线日期，只能让模型自己写——那正是要修的问题。
        "compare": w.compare,
    }


def _collect_tables(q: str, cfg: dict, metrics: list[str],
                    dims: list[dict]) -> tuple[list[str], list[dict]]:
    """主表来自指标定义；检索只补维度相关的表，最多 2 张。"""
    primary: list[str] = []
    for code in metrics:
        m = semantic.get_metric(code, cfg) or {}
        # source（原子指标，单数） / sources（派生指标，复数）/ ads（现成汇总表）
        # 三个都要收。E 阶段 V18 的 ARPU 就是漏了 sources，导致 primary 为空、
        # BM25 兜底召回 dws_revenue_daily，模型顺着用了它的 ARPPU 列。
        cand = ([m.get("source")] if m.get("source") else [])
        cand += list(m.get("sources") or []) + list(m.get("ads") or [])
        for t in cand:
            if t and t not in primary:
                primary.append(t)

    rcfg = cfg.get("retrieval") or {}
    floor = float(rcfg.get("min_score", 0.5))
    hits = semantic.find_tables(q, cfg, top=int(rcfg.get("top_tables", 4)))
    extra_dim, extra_other = [], []

    # 两种情况必须启用检索：
    #   ① 命中分组维度 → 补维表（JOIN 用）
    #   ② 一条指标都没命中 → 此时 primary 为空，若再不召回，
    #      prompt 里就一张表都没有，模型只能凭空编字段（实测会写出 is_active）。
    # 「有指标、无维度」才保持窄召回，避免把无关表塞进上下文。
    if dims or not primary:
        limit = 2 if dims else 3
        for h in hits:
            name = h["table"]
            if name in primary or h["score"] < floor:
                continue
            if name.startswith("dim_") and not extra_dim:
                extra_dim.append(name)
            elif not name.startswith("dim_") and len(extra_other) < limit:
                extra_other.append(name)
            if len(extra_dim) + len(extra_other) >= limit:
                break
    return primary + extra_dim + extra_other, hits


def route(state: AgentState, cfg: dict) -> dict:
    q = (state.get("question") or "").strip()

    # 阶段 1：注入必须**在生成之前**拦。等 SQL 生成出来再让 R23 扫，
    # 模型已经把恶意指令当上下文吃进去了。规则与护栏 R23 同源。
    inj = security.detect_injection(q)
    if inj:
        return {
            "unsupported": "检测到提示注入，已拒绝本次请求：%s。"
                           "用户指令不能覆盖系统规则与数据权限。" % inj,
            "security_block": True,
            "trace": [{"node": "route", "detail": "R23 前置拦截：%s" % inj,
                       "data": {"injection": inj}}],
        }

    # 元问题在生成 SQL 之前就分流：问「你能做什么」不该去编一条 SELECT
    mi = node_meta.detect_meta(q, cfg)
    if mi:
        return {
            "meta_intent": mi,
            "trace": [{"node": "route",
                       "detail": "元问题（%s）→ 直接回答，不生成 SQL" % mi["kind"],
                       "data": {"meta_intent": mi}}],
        }

    w = timeparse.parse(q, cfg)
    c = timeparse.clocks(cfg)

    # 实时词不再一律拒答：纯"现在/此刻"仍拒（链路已停），带历史锚点或只问
    # 实时域的改走 internal.music_dw 的历史快照。判定集中在 realtimelane，
    # 这里只负责把结论落到 state（lane / 时间窗 / 候选表 / 新鲜度）。
    rt = realtimelane.classify(q, cfg, w.status)
    lane = rt["lane"]
    if lane == "realtime":
        a, b = rt["window"] or realtimelane.window(cfg)
        w = timeparse.TimeWindow(
            start=_dt.date.fromisoformat(a), end=_dt.date.fromisoformat(b),
            label="实时链路历史快照窗口", kind="range", status="ok",
            note=rt["note"], lag_days=w.lag_days)

    unsupported = ""
    if rt["unsupported"]:
        unsupported = rt["unsupported"]
    elif w.status == "empty":
        unsupported = w.note

    hits = semantic.find_metrics(q, cfg,
                                 top=int((cfg.get("retrieval") or {}).get("top_metrics", 3)))
    metrics = [h["code"] for h in hits]
    if lane == "realtime":
        # 离线指标字典的公式（dau/arpu/...）不属于实时快照口径，
        # 留在上下文里只会诱导模型往实时表上套离线公式。
        metrics = []

    # ---- 追问兜底：多轮记忆的唯一落地点 ----
    # 只补"这一轮一个字都没提"的部分：指标没命中、时间没指定。
    # 补了必须留痕（followup → trace → 答案元信息），用户能看到
    # "这个数是按上一轮的 DAU 和 11-08 算的"。悄悄替换等于替用户猜问题，
    # 那正是这套系统一直拒绝做的事。
    followup: dict = {}
    turns = state.get("history_turns") or []
    prev = turns[-1] if turns else None
    if prev and w.status == "ok":
        if not metrics:
            pm = [m for m in (prev.get("metrics") or []) if m]
            if pm:
                metrics = pm
                followup["metrics"] = pm
        if w.kind == "default" and prev.get("time_start") and prev.get("time_end"):
            try:
                a = _dt.date.fromisoformat(str(prev["time_start"]))
                b = _dt.date.fromisoformat(str(prev["time_end"]))
                plabel = (prev.get("time_window") or {}).get("label") or ""
                w = timeparse.TimeWindow(
                    start=a, end=b,
                    label="沿用上一轮（%s）" % (plabel or "%s ~ %s" % (a, b)),
                    kind="single" if a == b else "range",
                    status="ok",
                    note="本轮没有指定时间，按上一轮窗口 %s ~ %s 计算。" % (
                        a.isoformat(), b.isoformat()))
                followup["time"] = {"start": a.isoformat(), "end": b.isoformat(),
                                    "label": plabel}
            except ValueError:
                pass

    dims = retrieval.detect_dimensions(q, cfg)
    tables, recalled = _collect_tables(q, cfg, metrics, dims)

    # 数据面隔离：离线问题里不出现 rt 表（它们的 measure 与离线同名但口径不同），
    # 实时问题里不出现离线表（R26 本来也会拒混查，这里在选表阶段就先隔干净）。
    rt_names = realtimelane.table_names(cfg)
    if lane == "realtime":
        tables = [t for t in tables if t in rt_names] or realtimelane.suggest(q, cfg, top=3)
    elif rt_names:
        tables = [t for t in tables if t not in rt_names]

    # 语义边界：表可以声明"哪些问法不该用我"（如歌曲榜按完播率排，别拿去答播放量榜）。
    # 这一层在权限裁剪之前，因为它与"谁能看"无关，是"这表能不能回答这个问题"。
    by_intent = semantic.excluded_by_intent(tables, q, cfg)
    intent_note = ""
    if len(by_intent) != len(tables):
        dropped_i = [t for t in tables if t not in by_intent]
        intent_note = "候选表 %s 与问题口径不符（表声明了 not_for），已移除" % (
            "、".join(dropped_i))
        tables = by_intent

    # 阶段 1：把候选表裁到角色可见范围内。被裁掉的表不是"提示"，是"根本查不了"，
    # 留在上下文里只会让模型写出一条注定被 R21 拦掉的 SQL。
    eng = policy_engine(cfg)
    decision = as_decision(state.get("policy_decision"))
    scoped = eng.visible(tables, decision)
    scope_note = ""
    if len(scoped) != len(tables):
        dropped = [t for t in tables if t not in scoped]
        scope_note = "候选表 %s 超出角色 %s 的可见范围，已移除" % (
            "、".join(dropped) or "—", decision.role)
        if not scoped:
            # 全被裁掉时不能就此空手，否则受限角色 90% 的问题都答不了。
            scoped = eng.substitute(metrics, decision)
            scope_note += "；改用可见汇总表 %s" % ("、".join(scoped) or "（无）")
    tables = scoped

    cautions: list[str] = []
    for d in dims:
        if d.get("caution"):
            cautions.append(str(d["caution"]))
        for n in (d.get("notes") or [])[:2]:
            cautions.append("%s：%s" % (d.get("name_cn") or d["code"], str(n)[:110]))

    detail = ("时间=%s [%s/%s]｜指标=%s｜维度=%s｜候选表=%s"
              % (timeparse.explain(w).replace("｜", "·", 1),
                 w.kind, w.status, metrics or "未命中",
                 [d["code"] for d in dims] or "无", tables or "无"))
    if scope_note:
        detail += "｜权限：%s" % scope_note
    if followup:
        bits = []
        if followup.get("metrics"):
            bits.append("指标 %s（未在问句中出现，取上一轮）" % "、".join(followup["metrics"]))
        if followup.get("time"):
            bits.append("时间窗 %s ~ %s（本轮未指定，取上一轮）"
                        % (followup["time"]["start"], followup["time"]["end"]))
        detail += "｜追问：" + "；".join(bits)

    tw = _window_dict(w)
    if lane == "realtime":
        a, b = rt["window"] or realtimelane.window(cfg)
        tw["sql_hint"] = ("按表选时间列 stat_minute / window_start / stat_date / dt，"
                          "WHERE 必须落在 %s ~ %s（R26）" % (a, b))

    return {
        "lane": lane,
        "time_start": w.start.isoformat(),
        "time_end": w.end.isoformat(),
        "metrics": metrics,
        "metric_hits": hits,
        "tables": tables,
        "unsupported": unsupported,
        "time_window": tw,
        "dimensions": [d["code"] for d in dims],
        "dimension_cautions": cautions,
        "retrieved_tables": recalled,
        "freshness": rt["note"] if lane == "realtime" else timeparse.freshness_note(c),
        "followup": followup,
        "trace": [{"node": "route", "detail": detail,
                   "data": {"window": tw, "matched": hits, "lane": lane,
                            "dimensions": dims, "recalled": recalled,
                            "unsupported": unsupported, "followup": followup}}],
    }
