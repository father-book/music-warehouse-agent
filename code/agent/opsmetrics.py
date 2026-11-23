"""运营可观测聚合：从不可变审计日志算成功率 / 时延 / 成本 / 护栏命中率。

为什么不再建一张指标表：
  `qa_logs` 已经是"每次问答的唯一事实"，且数据库层禁止 UPDATE/DELETE。
  再落一份聚合表就会产生第二个事实源，两边对不上时不知道信谁，
  还会多占磁盘（本项目磁盘有限）。这里全部**只读实时聚合**，
  趋势直接按 occurred_at 分组，天然可回放。

口径必须写清，否则"成功率"会骗人：
  · 拒答不等于失败——评测集里大量边界/安全题被拒答是**正确行为**；
    所以分开给 answer_rate / refuse_rate / error_rate，不用一个"成功率"糊过去。
  · availability = 1 - error_rate，只回答"系统有没有崩"。
  · guard_hit_rate 数的是 trace_json.findings 里出现过 REJECT 的问答占比，
    包含最终被修复后成功回答的那些（护栏起作用了，不是问答失败）。
  · rt_share 用 final_sql 里是否出现 internal.music_dw 判定，
    因为早期审计记录的 trace 里没有 lane 字段。
"""
from __future__ import annotations

import datetime as _dt


def _f(v, nd=2):
    if v is None:
        return 0.0
    try:
        return round(float(v), nd)
    except (TypeError, ValueError):
        return 0.0


def _row(cur, sql, args=()):
    cur.execute(sql, args)
    return cur.fetchone()


def collect(cfg: dict, days: int = 7) -> dict:
    from governance import store
    try:
        days = 7 if days is None else int(days)
    except (TypeError, ValueError):
        days = 7
    days = max(1, min(days, 90))
    base = """
        SELECT count(*) AS n,
               count(*) FILTER (WHERE error IS NOT NULL AND error <> '') AS errors,
               count(*) FILTER (WHERE refused) AS refused,
               count(*) FILTER (WHERE intent IN ('query','meta')) AS answered,
               count(*) FILTER (WHERE verdict = 'warn') AS warned,
               count(*) FILTER (WHERE jsonb_path_exists(trace_json,
                   '$.findings[*] ? (@.level == "REJECT")')) AS guard_hits,
               count(*) FILTER (WHERE position('internal.music_dw' in coalesce(final_sql,'')) > 0) AS rt,
               percentile_cont(0.95) WITHIN GROUP (ORDER BY elapsed_ms) AS p95,
               coalesce(sum(cost_cny), 0) AS cost,
               coalesce(sum(tokens_in), 0) AS tin,
               coalesce(sum(tokens_out), 0) AS tout,
               avg(elapsed_ms) AS avg_ms
        FROM qa_logs
        WHERE occurred_at >= now() - (%s * interval '1 day')"""
    daily = """
        SELECT to_char(occurred_at AT TIME ZONE 'Asia/Shanghai', 'YYYY-MM-DD') AS d,
               count(*) AS n,
               count(*) FILTER (WHERE error IS NOT NULL AND error <> '') AS errors,
               count(*) FILTER (WHERE refused) AS refused,
               count(*) FILTER (WHERE jsonb_path_exists(trace_json,
                   '$.findings[*] ? (@.level == "REJECT")')) AS guard_hits,
               percentile_cont(0.95) WITHIN GROUP (ORDER BY elapsed_ms) AS p95,
               coalesce(sum(cost_cny), 0) AS cost
        FROM qa_logs
        WHERE occurred_at >= now() - (%s * interval '1 day')
        GROUP BY 1 ORDER BY 1"""
    roles = """
        SELECT role, count(*) n,
               count(*) FILTER (WHERE refused) refused,
               round(avg(elapsed_ms)) avg_ms
        FROM qa_logs
        WHERE occurred_at >= now() - (%s * interval '1 day')
        GROUP BY role ORDER BY n DESC"""
    with store.connect(cfg) as conn, conn.cursor() as cur:
        r = _row(cur, base, (days,))
        cur.execute(daily, (days,))
        series = [{"date": x[0], "n": int(x[1]), "errors": int(x[2]),
                   "refused": int(x[3]), "guard_hits": int(x[4]),
                   "p95_ms": _f(x[5], 1), "cost_cny": _f(x[6], 4)}
                  for x in cur.fetchall()]
        cur.execute(roles, (days,))
        by_role = [{"role": x[0], "n": int(x[1]), "refused": int(x[2]),
                    "avg_ms": _f(x[3], 0)} for x in cur.fetchall()]

    n = int(r[0])
    errors, refused, answered = int(r[1]), int(r[2]), int(r[3])
    guard_hits, rt = int(r[5]), int(r[6])
    kpi = {
        "questions": n,
        "answered": answered,
        "refused": refused,
        "errors": errors,
        "warned": int(r[4]),
        "guard_hits": guard_hits,
        "rt_queries": rt,
        "answer_rate": _f(100.0 * answered / max(n, 1)),
        "refuse_rate": _f(100.0 * refused / max(n, 1)),
        "error_rate": _f(100.0 * errors / max(n, 1), 3),
        "availability": _f(100.0 * (n - errors) / max(n, 1), 3),
        "guard_hit_rate": _f(100.0 * guard_hits / max(n, 1)),
        "rt_share": _f(100.0 * rt / max(n, 1)),
        "p95_ms": _f(r[7], 1),
        "avg_ms": _f(r[11], 1),
        "cost_cny": _f(r[8], 4),
        "tokens_in": int(r[9]), "tokens_out": int(r[10]),
    }
    return {"ok": True, "days": days, "scope": "aggregate",
            "at": _dt.datetime.now().isoformat(timespec="seconds"),
            "kpi": kpi, "series": series, "by_role": by_role,
            "note": "拒答含预期边界/安全用例，不等于失败；availability 只统计未报错的问答。"
                    "护栏命中含最终修复后成功的问答。"}
