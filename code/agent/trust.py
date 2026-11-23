"""答案可信度：四项结果自检 + 链路信号 → 高/中/低三级。

为什么要做这个
--------------
护栏能保证"SQL 合规"，口径机检能保证"口径对"，评测能保证"离线这一批题对"——
但用户看到的**这一条答案**到底敢不敢用，此前系统一个字都不说。
面试里被问「你怎么给置信度」，正确答案只能是"没做"（docs/57 §4.11 原文交底）。

为什么不是印一个 "0.87"
-----------------------
那个数字无法被复算，用户没法验证，只是把不确定性伪装成确定性。
这里只输出**可指认的证据**：命中了哪条自检、走了几轮修复、检索是精确还是模糊。
级别由规则算，每条理由都能从 state 或一次真实查询反推回来。

四项自检（对应 docs/35 阶段 3 的交付物）
--------------------------------------
1. `empty`     空结果：要数据却一行都没有
2. `magnitude` 量级跳变：同一套 SQL 换到上一个等长窗口再跑一遍，比值异常
3. `dominance` 维度失衡：多行结果里某一行占合计 95% 以上（汇总口径最易出事）
4. `truncated` 结果被截断：结论只覆盖了前 N 行

量级为什么用"时间平移复算"而不是"对比历史日志"
--------------------------------------------
历史日志里的数字来自**不同的问题、不同的口径**，拿它比等于拿苹果比橘子；
而时间平移是**同一条 SQL、同一套口径、只换窗口**，两数相减能被任何人复核。
副作用正好是 docs/35 的验收条件：把某个指标人为放大 10 倍，比值立刻超标。
代价是每次问答多一次查询（默认 20~100ms），可用 `trust.magnitude.enabled` 关掉。

边界：结果不是单值、SQL 里没有日期字面量、基线窗口没数据 —— 都**跳过而不降级**。
"没做这项检查"和"这项检查没过"是两件事，混在一起会让可信度失去意义。
"""
from __future__ import annotations

import _bootstrap  # noqa: F401

import datetime as _dt
import re

import db
import guardtool

_RE_DATE_LIT = re.compile(r"'(\d{4}-\d{2}-\d{2})'")

_LEVELS = {"high": 0, "medium": 1, "low": 2}
_LABEL = {"high": "高", "medium": "中", "low": "低"}


def _num(v):
    if isinstance(v, bool) or v is None:
        return None
    if isinstance(v, (int, float)):
        return float(v)
    try:
        return float(str(v).replace(",", ""))
    except (TypeError, ValueError):
        return None


def _scalar(res: dict):
    rows = res.get("rows") or []
    if len(rows) != 1 or len(rows[0] or []) != 1:
        return None
    return _num(rows[0][0])


def _cfg(cfg: dict) -> dict:
    return ((cfg or {}).get("trust") or {})


# ---------------------------------------------------------------- 四项自检

def check_empty(res: dict) -> dict:
    rows = [r for r in (res.get("rows") or []) if r]
    n = res.get("row_count", len(rows))
    if not rows or n == 0:
        return {"code": "empty", "hit": True, "level": "low",
                "text": "查询返回 0 行：可能是分区没数据，或过滤条件把结果筛空了"}
    return {"code": "empty", "hit": False}


def check_truncated(res: dict) -> dict:
    if res.get("truncated"):
        return {"code": "truncated", "hit": True, "level": "medium",
                "text": "结果超过行数上限已截断，结论只覆盖前 %d 行"
                        % len(res.get("rows") or [])}
    return {"code": "truncated", "hit": False}


def check_dominance(res: dict, cfg: dict) -> dict:
    """单行占合计 95% 以上 —— 汇总题里最常见的"看起来对、其实是明细没拆开"。

    只在"多行且数值列全非负"时判：有负数（退款、差额）时占比本身没意义。
    """
    thr = float((_cfg(cfg).get("dominance") or {}).get("share", 0.95))
    rows = [r for r in (res.get("rows") or []) if r]
    if len(rows) < 3:
        return {"code": "dominance", "hit": False, "note": "行数不足 3，不做失衡判定"}
    cols = res.get("columns") or []
    best = None
    for ci in range(len(cols)):
        vals = []
        for r in rows:
            if ci < len(r):
                v = _num(r[ci])
                if v is not None:
                    vals.append(v)
        if len(vals) < 3 or any(v < 0 for v in vals):
            continue
        total = sum(vals)
        if total <= 0:
            continue
        share = max(vals) / total
        if best is None or share > best[1]:
            best = (cols[ci], share, max(vals), total)
    if best and best[1] > thr:
        return {"code": "dominance", "hit": True, "level": "medium",
                "text": "%s 列最大一行占合计 %.1f%%（阈值 %.0f%%），"
                        "先确认是不是汇总粒度没拆开" % (best[0], best[1] * 100, thr * 100),
                "share": round(best[1], 4)}
    return {"code": "dominance", "hit": False}


def _prev_window(ts: str, te: str) -> tuple[str, str]:
    """上一个等长窗口。单日 → 前一天；7 天 → 前 7 天。"""
    s, e = _dt.date.fromisoformat(ts), _dt.date.fromisoformat(te)
    n = (e - s).days + 1
    pe = s - _dt.timedelta(days=1)
    ps = pe - _dt.timedelta(days=n - 1)
    return ps.isoformat(), pe.isoformat()


def _shift_sql(sql: str, cur: tuple[str, str], base: tuple[str, str]):
    """把 SQL 里的日期字面量整体平移到基线窗口。

    只改字面量、不动结构——`dt = 'x'`、`dt >= 'x' AND dt < 'y'`、`IN (...)`
    三种写法都同样是替换，不用分别适配（与 node_execute 同一套做法）。
    """
    try:
        cs, ce = _dt.date.fromisoformat(cur[0]), _dt.date.fromisoformat(cur[1])
        bs, be = _dt.date.fromisoformat(base[0]), _dt.date.fromisoformat(base[1])
    except ValueError:
        return "", "日期格式不对"
    if (ce - cs) != (be - bs):
        return "", "窗口长度不同"
    delta = (be - ce).days
    hit = []

    def sub(m):
        d = _dt.date.fromisoformat(m.group(1))
        if cs <= d <= ce:
            hit.append(d)
            return "'%s'" % (d + _dt.timedelta(days=delta)).isoformat()
        return m.group(0)

    out = _RE_DATE_LIT.sub(sub, sql)
    if not hit:
        return "", "SQL 里没有当前窗口的日期字面量"
    return out, ""


def check_magnitude(state: dict, cfg: dict, run_sql=None) -> dict:
    """量级跳变：同一条 SQL 换到上一个等长窗口再跑，比值超过阈值即异常。

    返回 hit=False 时有 note 说明**为什么没做**（不是"做了没异常"），
    两者在界面上必须能分开——否则用户会以为"检查过了，没问题"。
    """
    conf = (_cfg(cfg).get("magnitude") or {})
    if not conf.get("enabled", True):
        return {"code": "magnitude", "hit": False, "note": "已关闭"}

    res = state.get("result") or {}
    cur_val = _scalar(res)
    if cur_val is None:
        return {"code": "magnitude", "hit": False, "note": "结果不是单值，不做比值"}

    sql = state.get("applied_sql") or state.get("sql") or ""
    ts, te = state.get("time_start"), state.get("time_end")
    if not (ts and te):
        return {"code": "magnitude", "hit": False, "note": "没有时间窗，无法复算基线"}
    try:
        base_win = _prev_window(str(ts), str(te))
    except ValueError:
        return {"code": "magnitude", "hit": False, "note": "时间窗格式异常"}

    # SQL 若引用了当前窗口之外的日期，说明它自己在算跨期差值，平移没有意义。
    outside = sorted({d for d in _RE_DATE_LIT.findall(sql)
                      if not (str(ts) <= d <= str(te))})
    if outside:
        return {"code": "magnitude", "hit": False,
                "note": "SQL 引用了窗口外日期（%s），已在算跨期，不复算基线"
                        % "、".join(outside[:2])}

    base_sql, why = _shift_sql(sql, (str(ts), str(te)), base_win)
    if why:
        return {"code": "magnitude", "hit": False, "note": why}

    # 平移后的 SQL 也要过一遍护栏再执行——只改字面量理论上安全，
    # 但"理论上"不等于"已验证"，而这是每次问答都会跑的一条查询。
    decision = None
    try:
        from governance.contracts import as_decision
        decision = as_decision(state.get("policy_decision"))
    except Exception:
        decision = None
    if decision is not None:
        v, _, _ = guardtool.check(base_sql, cfg, context=state.get("ctx"),
                                 decision=decision,
                                 question=state.get("question") or "")
        if v == "reject":
            return {"code": "magnitude", "hit": False,
                    "note": "基线 SQL 未通过护栏复核，跳过比值"}

    runner = run_sql or db.run_sql
    try:
        r2 = runner(base_sql, cfg, max_rows=2, trace_id=state.get("trace_id"))
    except Exception as e:                      # 自检是旁路增强，不能带崩主链
        return {"code": "magnitude", "hit": False,
                "note": "基线复算失败：%s" % str(e)[:80]}
    if r2.get("error"):
        return {"code": "magnitude", "hit": False,
                "note": "基线复算失败：%s" % str(r2["error"])[:80]}
    base_val = _scalar({"rows": r2.get("rows") or []})
    if base_val is None:
        return {"code": "magnitude", "hit": False, "note": "基线窗口不是单值结果"}

    floor = float(conf.get("min_base", 0.001))
    if abs(base_val) < floor:
        return {"code": "magnitude", "hit": False,
                "note": "基线值 %s 太小，比值没有意义" % base_val}
    ratio = abs(cur_val) / abs(base_val) if base_val else None
    out = {"code": "magnitude", "hit": False,
           "cur": cur_val, "base": base_val,
           "base_window": "%s ~ %s" % base_win,
           "ratio": round(ratio, 3) if ratio is not None else None}
    up = float(conf.get("ratio", 5.0))
    down = float(conf.get("ratio_down", 0.2))
    # 双条件：比值超标**且**绝对差够大。只看比值时，0.0001→0.0006 也会 5 倍；
    # 只看绝对差时，比率指标的抖动又会误报。两个都要满足才上报。
    delta_floor = float(conf.get("min_delta", 0.01))
    moved = abs(cur_val - base_val) >= delta_floor
    if ratio is not None and (ratio >= up or ratio <= down) and moved:
        out.update({"hit": True, "level": "low",
                    "text": "量级异常：当前 %s，上一窗口 %s ~ %s 是 %s（%.1f 倍）"
                            % (_fmt(cur_val), base_win[0], base_win[1],
                               _fmt(base_val), ratio)})
    return out


def _fmt(v: float) -> str:
    if v is None:
        return "—"
    if abs(v) >= 100:
        return format(int(round(v)), ",")
    s = ("%.6f" % v).rstrip("0").rstrip(".")
    return s or "0"


# ---------------------------------------------------------------- 链路信号

def _signals(state: dict, cfg: dict) -> list[dict]:
    """链路本身留下的证据。它们不改变"结果对不对"，但决定"敢不敢直接用"。"""
    out = []
    findings = state.get("findings") or []
    warns = [f for f in findings if f.get("level") == "WARN"]
    rejects = [f for f in findings if f.get("level") == "REJECT"]
    if rejects:
        # 走到答案里说明已经修好了，但"修过"这件事本身要让人看见。
        out.append({"code": "caliber_repaired", "level": "medium",
                    "text": "口径规则命中 %d 条已自动修复（%s），修复记录见答案末尾"
                            % (len(rejects),
                               "、".join(str(f.get("rule")) for f in rejects[:2]))})
    if warns:
        out.append({"code": "warn", "level": "medium",
                    "text": "护栏/口径提示 %d 条未阻断（%s）"
                            % (len(warns),
                               "；".join(str(f.get("message") or f.get("rule"))[:40]
                                        for f in warns[:2]))})

    rounds = int(state.get("attempts") or 0)
    limit = int((_cfg(cfg).get("degrade") or {}).get("repair_rounds", 2))
    if rounds >= limit:
        out.append({"code": "many_rounds", "level": "medium",
                    "text": "生成被驳回后重写了 %d 轮才通过" % rounds})

    if state.get("shape_unresolved"):
        out.append({"code": "shape_unresolved", "level": "low",
                    "text": "结果形状与问题形态不符，返工一次仍未解决"})
    elif int(state.get("shape_retries") or 0):
        out.append({"code": "shape_retried", "level": "medium",
                    "text": "结果形状不符返工 %d 次" % state.get("shape_retries")})

    if int(state.get("exec_retries") or 0):
        out.append({"code": "exec_retried", "level": "medium",
                    "text": "执行失败自愈 %d 次" % state.get("exec_retries")})

    # 检索信号：指标是"精确命中词条"还是"语义近似"，直接决定口径把握。
    # 精确 = glossary 同义词 / code / 中文名；模糊 = BM25 / 向量召回。
    # （glossary 记成 `glossary:播放量`，要比前缀，不能只比全名。）
    hits = state.get("metric_hits") or []

    def _is_precise(h) -> bool:
        how = str(h.get("matched_by") or "")
        # 精确命中的几种写法都带前缀：code / code_substr / name_cn / glossary:词。
        # 用 startswith 而不是全名相等，是因为 glossary 记的是 `glossary:播放量`——
        # 只比全名会把最精确的那一类误判成模糊（实测踩过一次）。
        return how.startswith(("code", "name_cn", "glossary"))

    if hits and not any(_is_precise(h) for h in hits):
        out.append({"code": "fuzzy_metric", "level": "medium",
                    "text": "指标全靠语义近似命中（%s），建议核对口径"
                            % str(hits[0].get("matched_by") or "vector")})
    if not hits and not state.get("metrics"):
        out.append({"code": "no_metric", "level": "medium",
                    "text": "没命中已登记的指标，口径由模型自行组织"})
    return out


# ---------------------------------------------------------------- 汇总

def evaluate(state: dict, cfg: dict, run_sql=None) -> dict:
    """算一次可信度。纯规则，不调模型。

    返回：{level, label, score, reasons, checks, basis}
      · level  high / medium / low
      · score  100 起扣，只用于排序；判定以 level 为准
      · reasons 每条都能落到 state 的某个字段或一次真实查询
    """
    conf = _cfg(cfg)
    if not conf.get("enabled", True):
        return {"level": "high", "label": "高", "score": 100,
                "reasons": [], "checks": {}, "basis": "可信度检查已关闭"}

    res = state.get("result") or {}
    checks = {
        "empty": check_empty(res),
        "truncated": check_truncated(res),
        "dominance": check_dominance(res, cfg),
        "magnitude": check_magnitude(state, cfg, run_sql=run_sql),
    }
    reasons: list[dict] = []
    for c in checks.values():
        if c.get("hit"):
            reasons.append({"code": c["code"], "level": c.get("level", "medium"),
                            "text": c.get("text") or c["code"]})
    reasons.extend(_signals(state, cfg))

    level = "high"
    for r in reasons:
        if _LEVELS.get(r["level"], 0) > _LEVELS[level]:
            level = r["level"]
    score = max(0, 100 - 35 * sum(1 for r in reasons if r["level"] == "low")
                    - 12 * sum(1 for r in reasons if r["level"] == "medium"))

    skipped = {k: v.get("note") for k, v in checks.items()
               if not v.get("hit") and v.get("note")}
    return {
        "level": level, "label": _LABEL[level], "score": score,
        "reasons": reasons,
        "checks": {k: {kk: vv for kk, vv in v.items() if kk != "text"}
                   for k, v in checks.items()},
        "skipped": skipped,
        "basis": "规则判定（结果自检 %d 项 + 链路信号 %d 条），非模型自评"
                 % (len(checks), len(reasons)),
    }


def summary_line(trust: dict) -> str:
    """答案里的一行。没命中任何问题时也要说清"查了什么、哪项没查成"，
    否则「可信度高」会被读成「系统保证这个数对」——那是两回事。"""
    if not trust:
        return ""
    lv = trust.get("level")
    rs = trust.get("reasons") or []
    skipped = trust.get("skipped") or {}
    tail = ""
    if skipped:
        tail = "；未做：%s" % "、".join(
            "%s（%s）" % (k, str(v)[:40]) for k, v in list(skipped.items())[:2])
    if not rs:
        done = [k for k, v in (trust.get("checks") or {}).items()
                if not v.get("hit") and not v.get("note")]
        return "可信度：%s（自检 %s 全部通过，未走返工%s）" % (
            trust.get("label"), "、".join(done) or "—", tail)
    if lv == "low":
        head = "可信度：低 —— 先别直接拿去汇报"
    elif lv == "medium":
        head = "可信度：中"
    else:
        head = "可信度：高"
    return "%s（%s%s）" % (head, "；".join(r["text"] for r in rs[:3]), tail)
