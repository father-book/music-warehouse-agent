"""统一数据质量体检：规则表 → 确定性检查 → 结果落库 → 阈值告警 → 趋势。

为什么需要它（P2 / 论文第四章 L0~L5 的可执行载体）：
  27 条 SQL 护栏、6 条口径机检（含 3 条确定性 REJECT）、175 项结构检查、行数校准、实时质量指标
  分别跑在五个模块里，每次都只回答"这一条过没过"。没人能回答
  "今天整体质量几分""这个月哪条最常亮""昨天是不是比前天差"。
  规则散落还有一个更隐蔽的代价：某条检查**静默失效**时（例如 realtime.yaml
  漂移让 R26 不再生效），面板上不会有任何异常，因为根本没有总览。

设计取舍：
  · 规则是数据（agent/semantic/dq_rules.yaml），阈值可改，代码不写 if value > x
  · 只做**确定性**检查：不调 LLM、不生成 SQL、不造数据，全部可回放
  · 结果落 PostgreSQL 的 dq_runs / dq_results，趋势从库里算，不覆盖历史
  · severity=info 只记录不算失败：实时链路 down 是已知事实，不是质量事故
  · 单条检查异常不拖垮整体，记为失败并保留错误文本（可观测优于静默跳过）
"""
from __future__ import annotations

import datetime as _dt
import json
import os
import re
import subprocess
import sys

import yaml

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.abspath(os.path.join(HERE, "..", ".."))
RULES_YAML = os.path.join(ROOT, "agent", "semantic", "dq_rules.yaml")
GUARD_PY = os.path.join(ROOT, "agent", "guards", "guard.py")
SEM_CHECK = os.path.join(ROOT, "agent", "semantic", "sem_check.py")
BASELINE = os.path.join(ROOT, "eval", "baseline.json")
HISTORY = os.path.join(ROOT, "eval", "history.jsonl")

# DQ02 的三条反向断言：任何一条没被 REJECT，都说明护栏失效了
_NEGATIVE = [
    ("R17 internal 白名单外", "SELECT COUNT(*) FROM internal.music_dw.dwd_play_start "
     "WHERE dt='2026-11-08'"),
    ("R26 实时离线混查", "SELECT a.stat_minute FROM internal.music_dw.ops_data_quality a "
     "JOIN ads_daily_overview b ON a.stat_minute=b.dt "
     "WHERE a.stat_minute>='2026-11-08' LIMIT 5"),
    ("R4 幻觉字段", "SELECT fake_column FROM ads_daily_overview "
     "WHERE dt='2026-11-08' LIMIT 5"),
]
# DQ03：5.24 倍事故的原版 SQL，口径机检必须报 R25 WARN
_BAD_REVENUE = (
    "SELECT dt, SUM(CASE WHEN event_name='order_pay' THEN amount ELSE 0 END) AS rev "
    "FROM dwd_pay WHERE dt BETWEEN '2026-10-05' AND '2026-11-08' GROUP BY dt")


def load_rules() -> dict:
    try:
        with open(RULES_YAML, encoding="utf-8") as f:
            return yaml.safe_load(f) or {}
    except Exception:
        return {}


def last_full_regression(path: str = "") -> tuple[dict | None, str]:
    """取最近一次**全量**回归记录。

    history.jsonl 里混着 quick / --id 等 partial 记录：它们题目数不全，
    拿去算「最近一次回归失败数」几乎必然是 0，等于把门禁做成假绿。
    没有 full 记录时回退最后一条，但说明里必须标出来，不能假装是全量。
    """
    p = path or HISTORY
    recs = []
    try:
        with open(p, encoding="utf-8") as f:
            for line in f:
                if not line.strip():
                    continue
                try:
                    recs.append(json.loads(line))
                except ValueError:
                    continue            # 单行损坏不该让整条体检挂掉
    except OSError:
        return None, "history.jsonl 不可读"
    full = [r for r in recs if not r.get("partial")]
    if full:
        return full[-1], "最近一次全量回归"
    if recs:
        return recs[-1], "无全量记录，回退最近一次（可能不完整）"
    return None, "history.jsonl 无记录"


def _compare(value, op: str, limit) -> bool:
    try:
        if isinstance(value, bool):
            value = int(value)
        if isinstance(limit, bool):
            limit = int(limit)
        a, b = float(value), float(limit)
    except (TypeError, ValueError):
        return False
    return {"==": abs(a - b) < 1e-9, "!=": abs(a - b) >= 1e-9,
            "<=": a <= b, ">=": a >= b, "<": a < b, ">": a > b}.get(op, False)


def _guard_findings(sql: str) -> list[dict]:
    p = subprocess.run([sys.executable, GUARD_PY, "--mode", "query", "--json", sql],
                       cwd=os.path.dirname(GUARD_PY), capture_output=True,
                       text=True, encoding="utf-8", errors="replace", timeout=30)
    try:
        fs = json.loads(p.stdout or "[]")
        return fs if isinstance(fs, list) else (fs.get("findings") or [])
    except Exception:
        return []


def _metric(name: str, cfg: dict) -> tuple[object, str]:
    """返回 (实测值, 说明)。每条只做一件事，异常由 collect 统一兜住。"""
    if name == "fail_count":
        p = subprocess.run([sys.executable, SEM_CHECK], capture_output=True,
                           text=True, encoding="utf-8", errors="replace", timeout=120)
        m = re.search(r"通过\s*\d+\s*项\s*/\s*失败\s*(\d+)\s*项", p.stdout or "")
        n = re.search(r"通过\s*(\d+)\s*项", p.stdout or "")
        return (int(m.group(1)) if m else -1), "sem_check 共 %s 项检查，退出码 %d" % (
            n.group(1) if n else "?", p.returncode)

    if name == "missed_count":
        missed = []
        for label, sql in _NEGATIVE:
            fs = _guard_findings(sql)
            if not any(f.get("level") == "REJECT" for f in fs):
                missed.append(label)
        return len(missed), "未拦住：%s" % ("、".join(missed) or "无（3/3 拦住）")

    if name == "detected":
        fs = _guard_findings(_BAD_REVENUE)
        hit = [f.get("rule") for f in fs if str(f.get("rule", "")).startswith("R25")]
        return (1 if hit else 0), "命中 %s" % ("、".join(hit) or "无（口径机检未生效）")

    if name == "drift_count":
        import etl_rowcount
        rows = etl_rowcount.measure(cfg).get("rows") or {}
        bad = [x for x in etl_rowcount.plan(rows) if x["status"] in ("快照过期", "声明过期")]
        return len(bad), "漂移：%s" % ("、".join(x["table"] for x in bad) or "无")

    if name in ("error_rate_pct", "peak_error_rate_pct"):
        import db
        r = db.run_sql("SELECT SUM(errors_1m), SUM(events_1m), MAX(error_rate) "
                       "FROM internal.music_dw.ops_data_quality", cfg)
        if r.get("error"):
            raise RuntimeError(str(r["error"])[:120])
        e, n, peak = (r["rows"] or [[0, 0, 0]])[0]
        if name == "error_rate_pct":
            return round(100.0 * float(e or 0) / max(float(n or 1), 1), 4), \
                "Σerrors/Σevents（不是 AVG 分钟比率）"
        return round(float(peak or 0), 4), "MAX(error_rate)"

    if name == "lane_up":
        import realtimelane
        s = realtimelane.spec(cfg)
        up = 1 if s.get("status") == "up" else 0
        return up, "链路状态 %s，窗口 %s ~ %s" % (
            s.get("status"), s.get("data_since"), s.get("data_until"))

    if name == "pass_pct":
        with open(BASELINE, encoding="utf-8") as f:
            st = (json.load(f).get("stats") or {}).get("overall") or {}
        n, p = int(st.get("n") or 0), int(st.get("passed") or 0)
        return round(100.0 * p / max(n, 1), 2), "基线 %d 题通过 %d" % (n, p)

    if name == "ci_failed":
        last, note = last_full_regression()
        if last is None or int(last.get("n") or 0) <= 0:
            # 没数据要判失败（-1），不能因为 0-0=0 而显示"回归全绿"
            return -1, note
        return int(last.get("n") or 0) - int(last.get("passed") or 0), \
            "%s %s 题，通过 %s" % (note, last.get("n"), last.get("passed"))

    if name == "exec_ok":
        from governance import store
        with store.connect(cfg) as conn, conn.cursor() as cur:
            cur.execute("SELECT checks->'exec'->>'ok' FROM etl_scripts "
                        "WHERE checks ? 'exec' ORDER BY id DESC LIMIT 1")
            row = cur.fetchone()
            return (1 if row and str(row[0]).lower() == "true" else 0), \
                "最近一次 ETL 执行回读结论：%s" % (row[0] if row else "无记录")

    if name == "rt_table_count":
        with open(os.path.join(ROOT, "agent", "tables.json"), encoding="utf-8") as f:
            meta = json.load(f)
        n = sum(1 for v in meta["tables"].values() if v.get("layer") == "rt")
        return n, "tables.json 中 layer=rt 的表数"

    if name == "grant_gap":
        from governance import store
        import realtimelane
        tabs = sorted(realtimelane.table_names(cfg))
        with store.connect(cfg) as conn, conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM roles")
            roles = int(cur.fetchone()[0])
            cur.execute("SELECT count(*) FROM policies WHERE kind='table' "
                        "AND effect='allow' AND table_name = ANY(%s)", (tabs,))
            got = int(cur.fetchone()[0])
        want = len(tabs) * roles
        return max(want - got, 0), "已授权 %d/%d（%d 表 × %d 角色）" % (
            got, want, len(tabs), roles)

    raise RuntimeError("未实现的指标 %s" % name)


def collect(cfg: dict) -> list[dict]:
    """跑一遍全部检查，返回结构化结果（不写库）。"""
    rules = load_rules()
    out = []
    for chk in rules.get("checks") or []:
        value, detail, err = None, "", ""
        try:
            value, detail = _metric(chk["metric"], cfg)
        except Exception as e:
            err = "%s: %s" % (type(e).__name__, str(e)[:150])
            detail = err
        op, limit = chk.get("op", "=="), chk.get("limit", 0)
        ok = _compare(value, op, limit) if not err else False
        sev = str(chk.get("severity") or "warn").lower()
        out.append({
            "rule_id": chk["id"], "layer": chk.get("layer"),
            "name_cn": chk.get("name_cn"), "severity": sev,
            "value": value, "threshold": "%s %s" % (op, limit),
            "detail": detail, "ok": bool(ok),
            "alert": bool(not ok and sev != "info"),
            "error": err,
        })
    return out


def _ensure_tables(cur):
    cur.execute("""CREATE TABLE IF NOT EXISTS dq_runs (
        run_id   bigserial PRIMARY KEY,
        run_at   timestamptz NOT NULL DEFAULT now(),
        total    int NOT NULL, passed int NOT NULL,
        failed   int NOT NULL, alerts int NOT NULL,
        detail   text NOT NULL DEFAULT '')""")
    cur.execute("""CREATE TABLE IF NOT EXISTS dq_results (
        id        bigserial PRIMARY KEY,
        run_id    bigint NOT NULL REFERENCES dq_runs(run_id) ON DELETE CASCADE,
        rule_id   text NOT NULL, layer text, name_cn text, severity text,
        ok        boolean NOT NULL, value text, threshold text,
        detail    text, alert boolean NOT NULL DEFAULT false)""")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_dq_runs_at ON dq_runs(run_at DESC)")


def persist(results: list[dict], cfg: dict) -> dict:
    from governance import store
    passed = sum(1 for r in results if r["ok"])
    alerts = sum(1 for r in results if r["alert"])
    with store.connect(cfg) as conn, conn.cursor() as cur:
        _ensure_tables(cur)
        cur.execute("INSERT INTO dq_runs(total,passed,failed,alerts,detail) "
                    "VALUES (%s,%s,%s,%s,%s) RETURNING run_id",
                    (len(results), passed, len(results) - passed, alerts,
                     json.dumps({"rules_version": (load_rules().get("version"))},
                                ensure_ascii=False)))
        run_id = cur.fetchone()[0]
        cur.executemany(
            "INSERT INTO dq_results(run_id,rule_id,layer,name_cn,severity,ok,"
            "value,threshold,detail,alert) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
            [(run_id, r["rule_id"], r["layer"], r["name_cn"], r["severity"],
              r["ok"], None if r["value"] is None else str(r["value"]),
              r["threshold"], r["detail"][:600], r["alert"]) for r in results])
    return {"run_id": run_id, "total": len(results), "passed": passed,
            "failed": len(results) - passed, "alerts": alerts}


def run(cfg: dict) -> dict:
    results = collect(cfg)
    try:
        summary = persist(results, cfg)
        stored = True
    except Exception as e:
        summary = {"total": len(results),
                   "passed": sum(1 for r in results if r["ok"]),
                   "failed": sum(1 for r in results if not r["ok"]),
                   "alerts": sum(1 for r in results if r["alert"])}
        summary["error"] = "结果落库失败：%s" % str(e)[:160]
        stored = False
    return {"ok": summary["failed"] == 0, "stored": stored,
            "at": _dt.datetime.now().isoformat(timespec="seconds"),
            "summary": summary,
            "alerts": [r for r in results if r["alert"]],
            "results": results,
            "families": load_rules().get("families") or []}


def report(cfg: dict, limit: int = 7) -> dict:
    """最新一次体检 + 最近 N 次趋势（没有跑过就如实说没有）。"""
    from governance import store
    try:
        with store.connect(cfg) as conn, conn.cursor() as cur:
            _ensure_tables(cur)
            cur.execute("SELECT run_id, run_at, total, passed, failed, alerts "
                        "FROM dq_runs ORDER BY run_at DESC LIMIT %s", (int(limit),))
            runs = [{"run_id": r[0], "at": str(r[1]), "total": r[2],
                     "passed": r[3], "failed": r[4], "alerts": r[5]}
                    for r in cur.fetchall()]
            if not runs:
                return {"ok": False, "reason": "尚未运行过质量体检", "trend": []}
            cur.execute("SELECT rule_id, name_cn, severity, ok, value, threshold, "
                        "detail, alert FROM dq_results WHERE run_id=%s ORDER BY id",
                        (runs[0]["run_id"],))
            results = [{"rule_id": r[0], "name_cn": r[1], "severity": r[2],
                        "ok": r[3], "value": r[4], "threshold": r[5],
                        "detail": r[6], "alert": r[7]} for r in cur.fetchall()]
        return {"ok": True, "latest": runs[0], "results": results,
                "trend": list(reversed(runs)),
                "families": load_rules().get("families") or []}
    except Exception as e:
        return {"ok": False, "reason": str(e)[:200], "trend": []}
