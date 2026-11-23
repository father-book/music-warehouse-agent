"""narrate 节点：把结果包成「结论 + 数字 + 出处 + 口径 + 复算 SQL」。

**结论来自结果集本身，不来自模型**。`insight.py` 只做确定性推导：
榜首是谁、占合计多少、首末涨跌、比例换算成百分点——每个字都能从
返回的行反推回去，因此仍然满足「答案里没有不可复算的判断」这条底线。

不做趋势归因、不做因果解释——那些无法被复算验证，一旦混进答案，
用户就分不清哪句是算出来的。（docs/23 §5.4 的取舍，这里落实成代码。）
"""
from __future__ import annotations

import _bootstrap  # noqa: F401

import re

import insight
import semantic
import shape
import trust as trustmod
from state import AgentState

_RE_FROM = re.compile(r"\b(?:FROM|JOIN)\s+([A-Za-z_][\w.]*)", re.I)


def _num(v) -> str:
    if isinstance(v, bool):
        return str(v).lower()
    if isinstance(v, int):
        return format(v, ",")
    if isinstance(v, float):
        s = ("%.6f" % v).rstrip("0").rstrip(".")
        return s if s else "0"
    if v is None:
        return "NULL"
    return str(v)


def _render_compare(cr: dict) -> list[str]:
    """对比段。差值来自 execute 的第二次查询，不是模型写的。"""
    if not cr:
        return []
    if cr.get("status") == "unsafe":
        # 口径对不上就不给数字。给了就会有人拿去做决策——
        # "差值 -105" 和 "差值为 0，因为基线算错了" 在报告里长得一模一样。
        return ["对比：不给差值（%s）" % (cr.get("note") or "基线口径与当前不一致")]
    if cr.get("status") != "ok":
        if cr.get("note"):
            return ["对比：未自动计算（%s）" % cr["note"]]
        return []
    cur, base, d = cr.get("cur_value"), cr.get("base_value"), cr.get("delta") or 0
    head = "相差" if cr.get("abs_mode") else "差值"
    sign = "" if cr.get("abs_mode") else ("+" if d > 0 else ("-" if d < 0 else "±"))
    line = "对比：基线 %s = %s ｜ 当前 = %s ｜ %s %s%s" % (
        cr.get("base_label") or "基线", _num(base), _num(cur),
        head, sign, _num(abs(d)))
    p = cr.get("pct")
    if p is not None:
        line += "（%s%.2f%%）" % ("+" if p > 0 else "", p * 100)
    elif not base:
        line += "（基线为 0，不算百分比）"
    out = [line]
    # 口径"能证明一致"和"可能一致"要分开说：窗口内部的日期字面量多半是业务常量
    # （活动日、发布日），平移之后含义会变，但从 SQL 本身判不出来，只能标出来。
    for n in ((cr.get("consistency") or {}).get("notes") or [])[:1]:
        out.append("口径提示：%s" % n)
    return out


def _delta_txt(d) -> str:
    if d is None:
        return "—"
    if d > 0:
        return "↑%d" % d
    if d < 0:
        return "↓%d" % -d
    return "持平"


def _signed(v) -> str:
    if v is None:
        return "—"
    return ("+" if v > 0 else "") + _num(v)


def _rank_lines(cr: dict) -> list[str]:
    """榜单对比的结论。每一条都能从对齐表里逐行核对。

    这里刻意不说"热度上升/下降"——名次变化只能说明相对位置，
    不能说明绝对值涨了。两个都写出来，让人自己判断。
    """
    rows = cr.get("rows") or []
    if not rows:
        return []
    out = ["%s 升 · %s 降 · %s 持平（共 %d 个）"
           % (cr.get("up", 0), cr.get("down", 0), cr.get("same", 0), len(rows))]
    ent, exi = cr.get("entered") or [], cr.get("exited") or []
    if ent or exi:
        out.append("新出现 %d 个 · 掉出 %d 个" % (len(ent), len(exi)))

    top1 = rows[0]
    if cr.get("top1_changed"):
        if top1.get("base_rank"):
            out.append("榜首易主：%s（基线第 %d → 本期第 1）"
                       % (top1["label"], top1["base_rank"]))
        else:
            out.append("榜首易主：%s（基线窗口无此实体）" % top1["label"])
    else:
        out.append("榜首未变：%s（两期均第 1）" % top1["label"])

    b, w = cr.get("best"), cr.get("worst")
    if b and b.get("rank_delta"):
        out.append("上升最多：%s 第 %d → 第 %d（+%d）"
                   % (b["label"], b["base_rank"], b["rank"], b["rank_delta"]))
    if w and w.get("rank_delta"):
        out.append("下降最多：%s 第 %d → 第 %d（%d）"
                   % (w["label"], w["base_rank"], w["rank"], w["rank_delta"]))
    return out


def _w(s) -> int:
    """显示宽度：中日韩字符占两格。"""
    return sum(2 if ord(c) > 0x2E80 else 1 for c in str(s))


def _pad(s, n: int, right: bool = False) -> str:
    """按显示宽度补齐。

    用 `%-24s` 排中文表头会错位——"实体"两个字的显示宽度是 4，
    但 len() 是 2，补出来的列右边界对不上，表格一眼就是歪的。
    """
    s = str(s)
    gap = " " * max(0, n - _w(s))
    return gap + s if right else s + gap


def _render_rank_table(cr: dict) -> list[str]:
    rows = cr.get("rows") or []
    ent = {r["key"] for r in (cr.get("entered") or [])}
    val_name = str(cr.get("value_col") or "值")[:15]
    cols = [("本期", 6, False), ("基线", 6, False), ("变化", 7, False),
            ("实体", 26, False), (val_name, 15, True),
            ("基线值", 13, True), ("值变化", 10, True)]
    body = ["  名次变化（%s vs 本期，各自取前 %d）"
            % (cr.get("base_label") or "基线", cr.get("top_n") or len(rows)),
            "  " + " ".join(_pad(t, w, r) for t, w, r in cols),
            "  " + "-" * (sum(w for _, w, _ in cols) + len(cols) - 1)]
    for x in rows:
        base = "—" if x.get("base_rank") is None else str(x["base_rank"])
        if x["key"] in ent:
            base = "新"
        cells = [
            (x["rank"], 6, False), (base, 6, False),
            (_delta_txt(x.get("rank_delta")), 7, False),
            (str(x["label"])[:24], 26, False),
            (_num(x.get("value")), 15, True),
            (_num(x.get("base_value")) if x.get("base_value") is not None else "—", 13, True),
            (_signed(x.get("value_delta")), 10, True),
        ]
        body.append("  " + " ".join(_pad(v, w, r) for v, w, r in cells))
    exi = cr.get("exited") or []
    if exi:
        body.append("  掉出前 %d：%s" % (
            cr.get("top_n") or len(rows),
            "、".join("%s（基线第 %d）" % (e["label"], e["base_rank"]) for e in exi[:5])))
    if cr.get("base_total"):
        body.append("  基线窗口共 %d 个实体参与排名%s。" % (
            cr["base_total"], "（已按取数上限截断，名次仍有效）"
            if cr.get("base_truncated") else ""))
    return body


def _span_days(state: AgentState) -> int:
    """窗口天数。只用于「日均」这类除法，算不出来就按 1 天处理（不猜）。"""
    s, e = state.get("time_start"), state.get("time_end")
    if not s or not e:
        return 1
    try:
        import datetime as _dt
        return max(1, (_dt.date.fromisoformat(e) - _dt.date.fromisoformat(s)).days + 1)
    except (ValueError, TypeError):
        return 1


def _tables_in(sql: str) -> list[str]:
    """从 SQL 里取真实表名——比从 route 的候选里猜靠谱。"""
    seen = []
    for t in _RE_FROM.findall(sql or ""):
        t = t.split(".")[-1].strip()
        if t and t not in seen:
            seen.append(t)
    return seen


def _location(tables: list[str], cfg: dict, lane: str = "offline") -> str:
    # 实时快照在 internal.music_dw，离线在 hive_catalog.music_dw。
    # 出处是给用户复算用的，写错 catalog 会让他去错的库里找这张表——
    # 2026-11-11 实时接入实测抓到：答案数字对了，出处却写 hive_catalog。
    if lane == "realtime":
        cat, db = "internal", "music_dw"
    else:
        cat = cfg["execution"]["catalog"]
        db = cfg["execution"]["database"]
    return "、".join("%s.%s.%s" % (cat, db, t) for t in tables) or "（未能从 SQL 解析出表名）"


def _render_rows(result: dict) -> str:
    cols = result.get("columns") or []
    rows = result.get("rows") or []
    if not cols or not rows:
        return "（查询成功，但返回 0 行——可能是该日期无数据，或过滤条件过严）"

    if len(rows) == 1:
        return "\n".join("  %-24s %s" % (c, _num(v)) for c, v in zip(cols, rows[0]))

    body = []
    body.append("  " + " | ".join("%-16s" % c for c in cols))
    body.append("  " + "-" * min(80, 18 * len(cols)))
    for r in rows:
        body.append("  " + " | ".join("%-16s" % _num(v) for v in r))
    return "\n".join(body)


def _fix_trail(state: AgentState) -> str:
    """把 D 阶段的"被拦 → 改 → 过"压成一行。只有重试过才显示。"""
    gen = [h for h in (state.get("history") or [])
           if h.get("stage") in ("generate", "mechanical", "llm_repair", "stub",
                                 "shape_repair")]
    if len(gen) <= 1:
        return ""
    steps = []
    for h in gen:
        st = h.get("stage")
        if st == "generate":
            steps.append("首版")
        elif st == "mechanical":
            n = len(h.get("applied") or [])
            steps.append("机械修复 %d 处" % n if n else "机械修复")
        elif st == "llm_repair":
            steps.append("模型重写")
        elif st == "shape_repair":
            steps.append("形态返工")
        else:
            steps.append(str(st))
    # "通过" 只描述护栏，不描述形态。形态返工上限用尽时二者会不一致，
    # 这时候写"最终通过"是在骗人——用户看得见答案形状不对。
    if state.get("refused"):
        tail = "未通过"
    elif state.get("shape_unresolved"):
        tail = "通过校验但形态未达预期"
    else:
        tail = "通过"
    return "修正过程：%s → 最终%s（共 %d 版）" % (" → ".join(steps), tail, len(gen))


def _narrate_cost_block(state: AgentState, cfg: dict) -> dict:
    """成本闸拒绝：先把数字摆出来，再说怎么改。

    为什么不能只写"太贵了不给跑"：用户没法判断这是误拦还是真拦截。
    把"哪张表 / 扫几个分区 / 多少行 / 上限多少"写清楚，
    对方一眼就能看出问题出在"dt 条件没裁掉分区"，而不是"系统不让我查"。
    """
    g = state.get("cost_gate") or {}
    off = cfg["lane"]["offline"]

    def _n(v, digits=0):
        try:
            return (("%." + str(digits) + "f") % float(v)) if digits \
                else "{:,}".format(int(round(float(v))))
        except (TypeError, ValueError):
            return str(v)

    lines = ["这条查询被**成本闸**拦下了，没有执行。", ""]
    nodes = [n for n in (g.get("scan_nodes") or []) if n.get("table")]
    if nodes:
        lines += ["| 表 | 扫描分区 | 预计行数 | 读取量 |", "|---|---|---|---|"]
        for n in nodes:
            lines.append("| `%s` | %s/%s | %s | %s MB |" % (
                n.get("table"), n.get("part_scanned"), n.get("part_total"),
                _n(n.get("rows") or 0), _n((n.get("bytes") or 0) / 1048576, 1)))
        lines.append("")

    lines += [g.get("message") or state.get("refuse_reason") or "", ""]
    lines.append("> 护栏放行了这条 SQL（`WHERE` 里确实有 `dt`），"
                 "但执行计划显示分区裁剪没有生效——"
                 "所以判据看的是计划，不是 SQL 里有没有出现 `dt` 这四个字符。")
    lines += ["", "数据范围：T+1 离线，%s ~ %s。%s。" % (
        off.get("data_since") or "2026-10-05", off["data_until"],
        state.get("freshness") or "")]
    trail = _fix_trail(state)
    if trail:
        lines += ["", trail]
    return {"answer": "\n".join(lines),
            "trace": [{"node": "narrate",
                       "detail": "输出成本闸拒答（预计 %s 行）"
                                 % _n(g.get("rows") or 0),
                       "data": {"reasons": g.get("reasons") or []}}]}


def narrate(state: AgentState, cfg: dict) -> dict:
    if state.get("refused"):
        if state.get("cost_block"):
            return _narrate_cost_block(state, cfg)
        off = cfg["lane"]["offline"]
        answer = "答不了这个问题。\n\n原因：%s\n\n" % state.get("refuse_reason", "")
        answer += "数据范围：T+1 离线，%s ~ %s。%s。" % (
            off.get("data_since") or "2026-10-05", off["data_until"],
            state.get("freshness") or "")
        trail = _fix_trail(state)
        if trail:
            answer += "\n" + trail
        return {"answer": answer, "trace": [{"node": "narrate", "detail": "输出拒答", "data": {}}]}

    res = state.get("result") or {}
    if res.get("error"):
        retries = state.get("exec_retries", 0)
        lines = ["这条查询没能跑通。", "", "数据库返回：%s" % res["error"]]
        if retries:
            lines.append("已自动尝试修正 %d 轮仍未成功，不再继续猜。" % retries)
        lines += ["",
                  "通常是问题超出了现有表/字段的范围，或口径无法用这批数据表达。",
                  "可以换个问法，或直接问「你能做什么」看我支持的范围。",
                  "", "最后一版 SQL：", state.get("sql", "")]
        answer = "\n".join(lines)
        return {"answer": answer, "trace": [{"node": "narrate", "detail": "输出执行错误", "data": {}}]}

    codes = state.get("metrics") or []
    topic = ""
    if codes:
        m = semantic.get_metric(codes[0], cfg) or {}
        topic = " · %s" % m.get("name_cn", codes[0]) if m.get("name_cn") else " · %s" % codes[0]

    w = state.get("time_window") or {}
    ts = state.get("time_start") or state.get("time_end") or "?"
    te = state.get("time_end") or ts
    cover = ts if ts == te else "%s ~ %s" % (ts, te)
    off = cfg["lane"]["offline"]
    tables = _tables_in(state.get("sql") or "")
    contract = shape.classify(state.get("question") or "")
    lines = [
        "%s%s" % (cover, topic),
        "",
    ]

    # 结论段：只放能从结果集反推出来的观察，不给因果、不给判断词。
    obs = insight.build(res, contract, {"days": _span_days(state)})
    cr = state.get("compare_result") or {}
    is_rank = cr.get("mode") == "rank" and cr.get("status") == "ok"
    if is_rank:
        cmp_lines = _rank_lines(cr)
    else:
        cmp_lines = _render_compare(cr)
        if cr.get("status") != "ok":
            # 未复算的对比必须让用户看见——问的是"多了多少"，答案里没有对比
            # 就是没答完，得说清为什么。
            cmp_lines = cmp_lines or (["对比：未自动计算（%s）" % cr["note"]]
                                     if cr.get("note") else [])
    if obs or cmp_lines:
        lines.append("结论：")
        lines.extend("  · %s" % o for o in obs)
        lines.extend("  · %s" % o for o in cmp_lines)
        lines.append("")

    if is_rank:
        # 榜单对比：用「本周 vs 基线」的对齐表取代单纯的结果表，
        # 否则同一批实体在一页里出现两遍，名次信息还被埋在第二张表。
        lines += _render_rank_table(cr)
        lines.append("")
    else:
        lines += [_render_rows(res), ""]

    if state.get("lane") == "realtime":
        import realtimelane
        rs = realtimelane.spec(cfg)
        lines += [
            "出处：%s（实时链路历史快照）" % _location(tables, cfg, "realtime"),
            "覆盖：%s（实时快照 %s ~ %s；不是离线 T+1 数据）" % (
                cover, rs.get("data_since"), rs.get("data_until")),
        ]
    else:
        lines += [
            "出处：%s" % _location(tables, cfg),
            "覆盖：%s（离线数据 %s ~ %s）" % (
                cover, off.get("data_since") or "2026-10-05", off["data_until"]),
        ]

    # 可信度：四项结果自检 + 链路信号。放在出处之前——用户读到这里时
    # 已经看完数，正好该知道"敢不敢用"；放最前面会挡住答案本身。
    tr = trustmod.evaluate(state, cfg)
    tline = trustmod.summary_line(tr)
    if tline:
        lines.append(tline)

    # 口径版本：回答"这个数是按哪版口径算的"。
    # 审计能追溯到 trace 还不够——改了口径之后，旧答案得能自证当时用的是哪一版，
    # 否则复现时拿的是新口径，会得出"当年算错了"的假结论。
    # 版本是语义层内容指纹，不是手写号：手写的号一定会忘记更新。
    cv = ""
    try:
        _cd = semantic.caliber_digest(cfg)
        cv = _cd["version"]
        _c = _cd["counts"]
        lines.append("口径版本：%s（%d 指标 / %d 条口径规则 / %d 张表）"
                     % (cv, _c["metrics"], _c["caliber_rules"], _c["tables"]))
    except Exception:
        # 版本算不出来不能把答案一起带崩：它只是"标注"，不是"结论"。
        cv = ""

    # 追问兜底换掉了什么，必须写进答案。否则用户看到的数字像是"这轮问出来的"，
    # 实际用的是上一轮的指标——这正是"悄悄猜"的一种，只不过猜的是指代。
    fu = state.get("followup") or {}
    if fu.get("metrics"):
        names = []
        for c in fu["metrics"]:
            m = semantic.get_metric(c, cfg) or {}
            names.append(m.get("name_cn") or c)
        lines.append("追问：问句里没有提指标，按上一轮的 %s 计算。" % "、".join(names))
    # 形态返工失败要如实说，不能假装答对了——用户有权知道答案形状可能不对。
    if state.get("shape_unresolved") and state.get("shape_issues"):
        lines.append("形态提示：这条结果的形状与问题不完全匹配（%s），"
                     "已尝试返工一次仍未达预期，请结合上面的数字判断。"
                     % state["shape_issues"][0].split("；")[0][:80])
    if w.get("note"):
        lines.append("数据说明：%s" % w["note"])
    fresh = state.get("freshness") or ""
    if fresh and fresh not in (w.get("note") or ""):
        lines.append("新鲜度：%s" % fresh)

    if codes:
        m = semantic.get_metric(codes[0], cfg) or {}
        if m.get("note"):
            lines.append("口径：%s" % m["note"])
        elif m.get("formula"):
            lines.append("口径：%s%s" % (
                m["formula"], "，过滤 " + m["filter"] if m.get("filter") else ""))

    warns = [f for f in (state.get("findings") or []) if f.get("level") == "WARN"]
    if warns:
        lines.append("提示：" + "；".join(f.get("message", "") for f in warns[:2]))

    trail = _fix_trail(state)
    if trail:
        lines.append(trail)

    if res.get("truncated"):
        lines.append("精度：结果超过 %d 行已截断，实际值见复算 SQL。"
                     % cfg["limits"]["max_rows"])

    lines.append("复算：")
    for ln in (state.get("sql") or "").strip().splitlines():
        lines.append("  " + ln)

    base_sql = (state.get("compare_result") or {}).get("base_sql")
    if base_sql:
        lines.append("复算（基线 %s）：" % (cr.get("base_window") or ""))
        for ln in base_sql.strip().splitlines():
            lines.append("  " + ln)

    return {
        "answer": "\n".join(lines),
        "trust": tr,
        "caliber_version": cv,
        "trace": [{"node": "narrate",
                   "detail": "已包装（出处/口径/复算）｜ 可信度：%s" % tr.get("label", "—"),
                   "data": {"trust": {k: v for k, v in tr.items() if k != "checks"},
                            "caliber_version": cv}}],
    }
