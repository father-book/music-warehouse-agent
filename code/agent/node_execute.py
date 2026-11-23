"""execute 节点：唯一真正跑 SQL 的地方，只在护栏放行后进入。

阶段 1 加了一道**策略编译**（ADR-005）：

    guard 放行 → 策略引擎把行级/列级权限编译进 SQL → 再复核一次护栏
              → db.run_sql(注入后的版本) → 列脱敏 → 输出

三个要点：
  · 注入在**每一版 SQL** 上重新做。C 阶段修复会重写 SQL，只注入首版会漏。
  · 注入后的 SQL 必须再过一次护栏，防止改写把规则绕没了（R21 校验注入结果）。
  · 策略拒绝时直接拒答并标 `security_block`，**不进重试闭环**——
    否则等于给模型三次机会去试不同的越权写法。

执行失败（数据库报错）仍保留原自愈路径：记 EXEC-ERROR 回 generate 重写。
"""
from __future__ import annotations

import _bootstrap  # noqa: F401

import datetime as _dt
import re

import db
import guardtool
import sqlconsist
from governance.contracts import as_decision
from governance.policy import engine
from state import AgentState

_RE_DATE_LIT = re.compile(r"'(\d{4}-\d{2}-\d{2})'")


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
    """只认「1 行 1 列且是数字」。多行/多列不做对比——那要先定义怎么对齐。"""
    rows = res.get("rows") or []
    if len(rows) != 1 or len(rows[0] or []) != 1:
        return None
    return _num(rows[0][0])


def _shift_sql(sql: str, cur: tuple[str, str],
               base: tuple[str, str]) -> tuple[str, str]:
    """把 SQL 里的当前窗口日期字面量整体平移到基线窗口。

    只改**日期字面量**，不动结构——所以用的是"每个日期各自加偏移"，
    而不是去认 `dt BETWEEN ...` 的语法形态。`dt = 'x'`、`dt >= 'x' and dt < 'y'`、
    `IN (...)` 三种写法都同样是字面量替换，不用分别适配。
    """
    try:
        cs, ce = _dt.date.fromisoformat(cur[0]), _dt.date.fromisoformat(cur[1])
        bs, be = _dt.date.fromisoformat(base[0]), _dt.date.fromisoformat(base[1])
    except ValueError:
        return "", "日期格式不对"
    if (ce - cs) != (be - bs):
        return "", "基线窗口与当前窗口长度不同"
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
        return "", "SQL 里没有当前窗口的日期字面量（可能用了相对日期函数）"
    return out, ""


def _compare(state: AgentState, cfg: dict, sql: str, res: dict,
             decision) -> dict:
    """用同一张表把基线窗口再查一遍，差值由这里算。

    为什么不让模型一条 SQL 算完：
      模型对日期没有感知，基线日期只能照抄。抄错一位数，SQL 合法、口径正确、
      数字合理，没有任何护栏能发现。而复算这条路，基线日期是解析出来的，
      两条 SQL 出自同一个模板，差值是减法——每一步都能被复核。
    """
    cmp = (state.get("time_window") or {}).get("compare") or {}
    out = {"status": "skipped", "note": "", "kind": cmp.get("mode"),
           "base_label": cmp.get("label"), "base_window": "%s ~ %s"
           % (cmp.get("start"), cmp.get("end")), "base_sql": ""}

    cur_val = _scalar(res)
    if cur_val is None:
        out["note"] = "当前结果不是单值（%d 行 %d 列），不做自动差值" % (
            len(res.get("rows") or []), len((res.get("columns") or [])))
        return out

    # 兜底：SQL 若引用了当前窗口之外的日期，说明它自己就在算差值，
    # 平移复算出来的基线值没有意义。宁可说"没复算"，也不给一个假基线。
    cur_lo, cur_hi = str(state.get("time_start")), str(state.get("time_end"))
    outside = sorted({d for d in _RE_DATE_LIT.findall(sql)
                      if not (cur_lo <= d <= cur_hi)})
    if outside:
        out["note"] = ("SQL 引用了当前窗口之外的日期（%s），它自己已经在算差值，"
                       "系统不复算基线" % "、".join(outside[:3]))
        return out

    base_sql, why = _shift_sql(sql, (state.get("time_start"), state.get("time_end")),
                               (cmp.get("start"), cmp.get("end")))
    if why:
        out["note"] = "没能自动生成基线 SQL：%s" % why
        return out

    # 平移"做成了"不等于"口径还一致"——这是两件事，必须分开验。
    # _shift_sql 只负责把日期挪过去，它不知道挪完之后两条 SQL 还等不等价：
    # 若 SQL 里还有 `date_format(dt,'%Y%m%d') = '20261108'` 这种它不认的写法，
    # 平移照样"成功"，但那半边条件留在原地，基线值就是个偏了味的数字。
    cons = sqlconsist.check(sql, base_sql,
                            (state.get("time_start"), state.get("time_end")),
                            (cmp.get("start"), cmp.get("end")))
    out["consistency"] = cons
    if not sqlconsist.ok(cons):
        out["status"] = "unsafe"
        out["note"] = cons.get("message") or "基线口径无法与当前对齐"
        return out
    out["base_sql"] = base_sql

    # 基线 SQL 是一条新 SQL，必须重新过护栏。规则是"任何进入执行面的 SQL
    # 都要过同一套检查"，这里不能因为"是我自己拼的"就跳过。
    v, f, _ = guardtool.check(base_sql, cfg, context=state.get("ctx"),
                              decision=decision, question=state.get("question") or "")
    if v == "reject":
        out["note"] = "基线 SQL 未通过护栏：%s" % "、".join(
            str(x.get("rule")) for x in f if x.get("level") == "REJECT")[:80]
        return out

    r2 = db.run_sql(base_sql, cfg, max_rows=decision.max_rows,
                    trace_id=state.get("trace_id"))
    if r2.get("error"):
        out["note"] = "基线查询失败：%s" % str(r2["error"])[:160]
        return out
    base_val = _scalar(r2)
    if base_val is None:
        out["note"] = "基线结果不是单值，不做自动差值"
        return out

    raw_delta = cur_val - base_val
    # 「相差多少」要绝对值，「少了多少」要符号。同一对数，两种问法。
    abs_mode = bool(cmp.get("abs"))
    delta = abs(raw_delta) if abs_mode else raw_delta
    out.update({
        "status": "ok", "cur_value": cur_val, "base_value": base_val,
        "delta": delta, "raw_delta": raw_delta, "abs_mode": abs_mode,
        "pct": (raw_delta / base_val) if base_val else None,
        "row_count": r2.get("row_count", 0),
    })
    return out


# ---------------------------------------------------------------- 榜单对比
_RE_LIMIT = re.compile(
    r"\s+LIMIT\s+(?:(\d+)\s*,\s*)?(\d+)(?:\s+OFFSET\s+(\d+))?\s*$", re.I)


def _strip_limit(sql: str) -> str:
    """去掉末尾 LIMIT，用来取基线窗口的**全量**名次表。

    只取基线 Top10 是不够的：本周第 3 名上周可能排第 47，
    看不到"第 47"就答不了用户真正问的那个位移。
    """
    return _RE_LIMIT.sub("", (sql or "").rstrip())


def _layout(res: dict):
    """识别「实体 + 指标」的列布局。识别不了就返回 None，不做名次对比。

    不假设列序——模型写 `SELECT cnt, song_name` 也完全合法。
    数值列取「合计最大」的那一列：榜单里常带一个 ROW_NUMBER() 名次列，
    按位置取会把名次当指标，算出"播放量下降 9"这种荒唐结论。

    显示列的选择比看上去讲究：模型常返回 `song_id, song_name, singer, cnt`，
    若简单地取"最后一个字符串列"，表格里显示的会是**歌手**，
    于是出现"那英第 5 名却掉出前 10"这种自相矛盾——同一个歌手有多首歌，
    每一行都长得一样。所以：key 是 id 时找 `*_name` 列，找不到才退回。
    """
    cols = res.get("columns") or []
    rows = [r for r in (res.get("rows") or []) if r]
    if len(cols) < 2 or len(rows) < 2:
        return None
    num_i, str_i = [], []
    for i in range(len(cols)):
        vals = [r[i] for r in rows if i < len(r)]
        if not vals:
            continue
        nonnull = [v for v in vals if v is not None]
        # 空值不破坏数值列判定：榜单里偶尔有指标为 NULL 的行
        if nonnull and all(_num(v) is not None for v in nonnull):
            num_i.append(i)
        else:
            str_i.append(i)
    if not num_i or not str_i:
        return None
    val_i = max(num_i, key=lambda i: sum(abs(_num(r[i]) or 0) for r in rows))
    key_i = str_i[0]
    label_i = key_i
    if re.search(r"(^|_)id$", str(cols[key_i]), re.I):
        rest = [i for i in str_i if i != key_i]
        named = [i for i in rest
                 if re.search(r"(^|_)name$", str(cols[i]), re.I)]
        if named:
            label_i = named[0]
        elif rest:
            label_i = rest[-1]
    return {"key": key_i, "label": label_i, "value": val_i,
            "entity_name": cols[key_i], "value_name": cols[val_i]}


def _outside_dates(state: AgentState, sql: str) -> list[str]:
    lo, hi = str(state.get("time_start")), str(state.get("time_end"))
    return sorted({d for d in _RE_DATE_LIT.findall(sql) if not (lo <= d <= hi)})


def _compare_rank(state: AgentState, cfg: dict, sql: str, res: dict,
                  decision) -> dict:
    """榜单对比：两个窗口各自排名，再对齐到同一批实体上看位移。

    为什么不做"一条 SQL 算两个窗口的名次差"：
      名次是**窗口内**的排序结果，一条 SQL 里要窗口函数 + 双窗口过滤 +
      全外连接才能算出来。模型写不对，人也读不懂，而且错了没人能一眼看出来。
      这里走"两次查询 + 本地对齐"：每条名次都能被单独复核。

    双口径，缺一不可：
      · 各自取 TopN 再对齐 → 谁进榜、谁掉榜
      · 基线取**全量**名次   → 本周第 3 名上周排第几（这是用户问的"变化"）
    """
    lay = _layout(res)
    out = {"status": "skipped", "mode": "rank", "kind": "rank",
           "base_label": None, "base_window": "", "base_sql": ""}
    cmp = (state.get("time_window") or {}).get("compare") or {}
    out["base_label"] = cmp.get("label")
    out["base_window"] = "%s ~ %s" % (cmp.get("start"), cmp.get("end"))
    if not lay:
        out["note"] = ("结果不是「实体 + 指标」的多行榜单（%d 行 %d 列），"
                       "无法做名次对比" % (len(res.get("rows") or []),
                                          len(res.get("columns") or [])))
        return out

    outside = _outside_dates(state, sql)
    if outside:
        out["note"] = ("SQL 引用了当前窗口之外的日期（%s），它自己在算两个窗口，"
                       "系统不重复对齐" % "、".join(outside[:3]))
        return out

    base_sql, why = _shift_sql(sql, (state.get("time_start"), state.get("time_end")),
                               (cmp.get("start"), cmp.get("end")))
    if why:
        out["note"] = "没能自动生成基线 SQL：%s" % why
        return out
    full_sql = _strip_limit(base_sql)

    # 榜单对比在这里也要过一遍口径检查：两边都去掉 LIMIT 再比，
    # 否则"结构不同"会永远是成立——那是取全量基线造成的，不是口径变了。
    cons = sqlconsist.check(_strip_limit(sql), full_sql,
                            (state.get("time_start"), state.get("time_end")),
                            (cmp.get("start"), cmp.get("end")))
    out["consistency"] = cons
    if not sqlconsist.ok(cons):
        out["status"] = "unsafe"
        out["note"] = cons.get("message") or "基线口径无法与当前对齐"
        return out
    out["base_sql"] = full_sql

    v, f, _ = guardtool.check(full_sql, cfg, context=state.get("ctx"),
                              decision=decision, question=state.get("question") or "")
    if v == "reject":
        out["note"] = "基线 SQL 未通过护栏：%s" % "、".join(
            str(x.get("rule")) for x in f if x.get("level") == "REJECT")[:80]
        return out

    r2 = db.run_sql(full_sql, cfg, max_rows=decision.max_rows,
                    trace_id=state.get("trace_id"))
    if r2.get("error"):
        out["note"] = "基线查询失败：%s" % str(r2["error"])[:160]
        return out
    rows2 = [r for r in (r2.get("rows") or []) if r]
    if not rows2:
        out["note"] = "基线窗口没有数据，无法对比名次"
        return out

    # 基线全量名次（顺序即名次：ORDER BY 没被平移改动）
    base_map = {}
    for i, row in enumerate(rows2, 1):
        k = str(row[lay["key"]]) if lay["key"] < len(row) else ""
        if k and k not in base_map:
            base_map[k] = {"rank": i, "value": _num(row[lay["value"]])}

    cur = []
    for i, row in enumerate([r for r in (res.get("rows") or []) if r], 1):
        k = str(row[lay["key"]])
        b = base_map.get(k)
        cv = _num(row[lay["value"]])
        bv = b["value"] if b else None
        cur.append({
            "rank": i, "key": k, "label": str(row[lay["label"]]),
            "value": cv,
            "base_rank": b["rank"] if b else None,
            "base_value": bv,
            "rank_delta": (b["rank"] - i) if b else None,
            "value_delta": (cv - bv) if (cv is not None and bv is not None) else None,
        })

    cur_keys = {r["key"] for r in cur}
    top_n = len(cur)
    exited = []
    for i, row in enumerate(rows2[:top_n], 1):
        k = str(row[lay["key"]])
        if k not in cur_keys:
            exited.append({"key": k, "label": str(row[lay["label"]]),
                           "base_rank": i, "base_value": _num(row[lay["value"]])})

    moved = [r for r in cur if r["rank_delta"]]
    up = sum(1 for r in cur if (r["rank_delta"] or 0) > 0)
    down = sum(1 for r in cur if (r["rank_delta"] or 0) < 0)
    same = sum(1 for r in cur if r["rank_delta"] == 0)
    entered = [r for r in cur if r["base_rank"] is None]

    out.update({
        "status": "ok", "rows": cur, "top_n": top_n,
        "entity_col": lay["entity_name"], "value_col": lay["value_name"],
        "entered": entered, "exited": exited,
        "up": up, "down": down, "same": same,
        "best": max(moved, key=lambda r: r["rank_delta"]) if moved else None,
        "worst": min(moved, key=lambda r: r["rank_delta"]) if moved else None,
        "top1_changed": bool(cur and cur[0]["base_rank"] != 1),
        "base_total": len(rows2),
        "base_truncated": bool(r2.get("truncated")),
        "row_count": r2.get("row_count", 0),
    })
    return out


def _block(reason: str, sql: str) -> dict:
    return {
        "refused": True,
        "refused": True,
        "security_block": True,
        "refuse_reason": "%s\n\n这是服务端权限边界，不能通过改写 SQL 绕过。"
                        "如需更大数据范围，请申请对应角色。" % reason,
        "applied_sql": sql,
        "result": {"columns": [], "rows": [], "row_count": 0,
                   "elapsed_ms": 0, "truncated": False, "error": reason},
        "trace": [{"node": "execute", "detail": "策略拒绝：%s" % reason[:120],
                   "data": {"reason": reason[:300]}}],
    }


def execute(state: AgentState, cfg: dict) -> dict:
    if state.get("verdict") == "reject":
        return {"trace": [{"node": "execute", "detail": "跳过：未通过校验", "data": {}}]}

    sql = state.get("sql") or ""
    eng = engine(cfg)
    # state 里存的是 dict（要 JSON 化），引擎要的是对象。
    decision = as_decision(state.get("policy_decision"))
    applied = eng.apply(sql, decision)

    if applied["error"]:
        return _block("策略引擎拒绝执行：%s" % applied["error"], sql)

    final_sql = applied["sql"]
    if applied["injected"]:
        # 改完必须重验：AST 重写有可能把护栏依赖的结构改掉。
        v2, f2, _ = guardtool.check(
            final_sql, cfg,
            context=state.get("ctx"), decision=decision,
            question=state.get("question") or "")
        if v2 == "reject":
            rules = [f.get("rule") for f in f2 if f.get("level") == "REJECT"]
            return _block("策略注入后的 SQL 未通过护栏复核：%s"
                          % "、".join(str(r) for r in rules[:4]), final_sql)

    # 取数上限来自岗位能力（roles.max_rows），不是全局配置：
    # 工程师抽数核对要 2 万行，产品经理看大盘 1 千行就够，一刀切会逼着人往明细表里翻。
    # 这条以前写在 PolicyDecision 里却没人读——字段存在≠约束生效，所以显式传下去。
    res = db.run_sql(final_sql, cfg, max_rows=decision.max_rows,
                     trace_id=state.get("trace_id"))
    res = eng.mask_result(res, applied["mask_columns"])

    if res.get("error"):
        err = str(res["error"])
        n = state.get("attempts", 0)
        return {
            "result": res,
            "applied_sql": final_sql,
            "exec_retries": state.get("exec_retries", 0) + 1,
            "history": [{"attempt": n, "stage": "exec_error", "sql": final_sql,
                         "verdict": "error", "rejects": ["EXEC-ERROR"],
                         "detail": err[:200]}],
            "trace": [{"node": "execute", "detail": "执行失败：%s" % err[:160],
                       "data": {"error": err[:300],
                                "exec_retries": state.get("exec_retries", 0) + 1,
                                "injected": applied["injected"]}}],
        }

    detail = "%d 行 / %d ms%s" % (
        res["row_count"], res["elapsed_ms"], "（已截断）" if res["truncated"] else "")
    if applied["injected"]:
        detail += " ｜ 行级策略已下推 %d 处" % len(applied["injected"])

    cmp_res = {}
    cmp = (state.get("time_window") or {}).get("compare") or {}
    if cmp and cmp.get("auto"):
        # 单值相减 vs 榜单名次，是两种不同的"变化"。
        # 拿一个通用差值函数套多行结果，只会得到"当前结果不是单值"然后静默跳过——
        # 用户问"榜单变化"，答案里一个字都没提，那是最糟的"看起来答了"。
        if _scalar(res) is not None:
            cmp_res = _compare(state, cfg, final_sql, res, decision)
        else:
            cmp_res = _compare_rank(state, cfg, final_sql, res, decision)
        detail += " ｜ 对比基线 %s" % (
            "已复算" if cmp_res.get("status") == "ok"
            else "未复算（%s）" % (cmp_res.get("note") or "—")[:40])

    out = {
        "result": res,
        "applied_sql": final_sql,
        "trace": [{"node": "execute", "detail": detail,
                   "data": {"row_count": res.get("row_count", 0),
                            "injected": applied["injected"],
                            "masked": applied["mask_columns"],
                            "compare": {k: v for k, v in cmp_res.items()
                                        if k != "base_sql"}}}],
    }
    if cmp_res:
        out["compare_result"] = cmp_res
    return out
