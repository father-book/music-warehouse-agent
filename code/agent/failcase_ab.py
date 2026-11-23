#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""失败案例注入的 A/B：同一批"被护栏拦下的 SQL"，看注入案例能否更快改对。

只跑 generate/validate 两段，**不执行 SQL**：
  · 修复质量由护栏判定，和 CI 回归用的是同一份规则；
  · 不连 Doris，所以不需要起 4 节点集群。

样本是 6 条真实失败模式（R9 / R16 / R17 / R18 / R20 / R5），每条跑「关案例」「开案例」两轮，
temperature=0 固定随机性。判定只有两种：修复后护栏 pass，或仍 reject。

用法：
    python failcase_ab.py            # 6 条 × 2 组
    python failcase_ab.py --n 2      # 每条重复 2 次
    python failcase_ab.py --dry      # 只校验坏 SQL 是否真被对应规则拦下，不调模型
"""
from __future__ import annotations

import _bootstrap  # noqa: F401

import argparse
import json
import re

import conf
import guardtool
import gateway
import promptlib

DAY = "2026-11-08"
WINDOW = {"kind": "single", "label": DAY, "sql_hint": "dt = '%s'" % DAY,
          "status": "ok", "is_range": False, "lag_days": 1,
          "requested": [DAY, DAY], "note": "数据截止 %s（离线 T+1）" % DAY}

# 每条都是"语法合法、能跑、但会被护栏拦下"的真实写法，不是瞎编的错
CASES = [
    {"q": "11-08 有哪些用户启动了 App",
     "bad": "SELECT * FROM dwd_session WHERE dt = '2026-11-08'",
     "tables": ["dwd_session"], "metrics": ["uv"], "rule": "R9"},
    {"q": "11-08 的 DAU 是多少",
     "bad": ("SELECT COUNT(DISTINCT user_id) AS dau FROM dwd_session "
             "WHERE dt = '2026-11-08' AND event_name = 'app_launch'"),
     "tables": ["dwd_session"], "metrics": ["uv"], "rule": "R16"},
    {"q": "11-08 各省份的播放次数",
     "bad": ("SELECT province, SUM(play_start_cnt) AS cnt FROM "
             "internal.music_dw.dwd_play_start WHERE dt = '2026-11-08' GROUP BY province"),
     "tables": ["dwd_play_start"], "metrics": ["play_start_cnt"], "rule": "R17"},
    {"q": "11-08 各省份的付费用户数",
     "bad": ("SELECT d.province, COUNT(DISTINCT p.user_id) AS pay_uv FROM dwd_pay p "
             "JOIN dim_user d ON p.user_id = d.user_id "
             "WHERE p.dt = '2026-11-08' AND p.event_name = 'order_pay' GROUP BY d.province"),
     "tables": ["dwd_pay", "dim_user"], "metrics": [], "rule": "R18"},
    {"q": "11-08 的累计 VIP 用户数",
     "bad": ("SELECT SUM(vip_user_cnt) AS vip_cnt FROM dws_user_daily "
             "WHERE dt <= '2026-11-08'"),
     "tables": ["dws_user_daily"], "metrics": [], "rule": "R20"},
    {"q": "列出 11-08 播放过歌的用户",
     "bad": ("SELECT user_id FROM dwd_play_start WHERE dt = '2026-11-08'"),
     "tables": ["dwd_play_start"], "metrics": [], "rule": "R5"},
]


def _state(c: dict) -> dict:
    return {"question": c["q"], "lane": "offline",
            "time_start": DAY, "time_end": DAY, "time_window": dict(WINDOW, label=DAY),
            "metrics": c["metrics"], "tables": c["tables"],
            "matched": [], "recalled": [], "dimensions": []}


def _check(sql: str, cfg: dict, c: dict) -> tuple[str, list[dict]]:
    verdict, findings, _err = guardtool.check(sql, cfg, question=c["q"])
    return verdict, findings


def _parse_sql(text: str) -> str:
    """模型被要求只输出 JSON；抽不出来就返回空（算这次修复失败）。"""
    t = (text or "").strip()
    m = re.search(r"\{.*\}", t, re.S)
    if m:
        try:
            return (json.loads(m.group(0)) or {}).get("sql") or ""
        except Exception:
            pass
    return ""


def _one(c: dict, on: bool, cfg: dict, findings: list[dict]) -> dict:
    st = _state(c)
    msgs = promptlib.build_generate_messages(st, cfg, findings=findings, prev_sql=c["bad"])
    r = gateway.chat(msgs, cfg, caller="failcase_ab", json_mode=True,
                     temperature=0.0, max_tokens=900, user_id="ab-test", role="data_engineer")
    sql = _parse_sql(r.get("content") or "")
    if not sql:
        return {"ok": False, "why": "输出不可解析", "tokens": r.get("tokens_in", 0) + r.get("tokens_out", 0)}
    verdict, fs = _check(sql, cfg, c)
    rejects = [f.get("rule") for f in fs if f.get("level") == "REJECT"]
    return {"ok": verdict != "reject", "why": "、".join(rejects) or verdict,
            "sql": sql, "tokens": (r.get("tokens_in") or 0) + (r.get("tokens_out") or 0)}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--n", type=int, default=1, help="每条重复次数")
    ap.add_argument("--dry", action="store_true", help="只验证坏 SQL 是否被拦")
    a = ap.parse_args()

    cfg = conf.load_config()
    cfg["cost_gate"] = dict(cfg.get("cost_gate") or {}, enabled=False)   # 不连 Doris

    print("样本自检：坏 SQL 是否真被对应规则拦下")
    bad_ok = 0
    for c in CASES:
        _v, fs = _check(c["bad"], cfg, c)
        rules = [f.get("rule") for f in fs if f.get("level") == "REJECT"]
        hit = any(r.startswith(c["rule"]) for r in rules)
        bad_ok += bool(hit)
        print("  %-4s %s  %s" % (c["rule"], "ok  " if hit else "MISS", "、".join(rules) or "未拦"))
    print("  %d/%d 条命中预期规则" % (bad_ok, len(CASES)))
    if a.dry:
        return 0 if bad_ok == len(CASES) else 1

    off = dict(cfg)
    off["failcase"] = dict(cfg.get("failcase") or {}, enabled=False)

    rows = []
    for c in CASES:
        _v, findings = _check(c["bad"], cfg, c)
        for _ in range(a.n):
            r_off = _one(c, False, off, findings)
            r_on = _one(c, True, cfg, findings)
            rows.append((c["rule"], r_off, r_on))
            print("  %-4s off=%-5s on=%-5s  %s / %s" % (
                c["rule"], "pass" if r_off["ok"] else "fail",
                "pass" if r_on["ok"] else "fail",
                r_off["why"], r_on["why"]))

    n = len(rows)
    off_ok = sum(1 for _, o, _ in rows if o["ok"])
    on_ok = sum(1 for _, _, o in rows if o["ok"])
    off_tk = sum(o["tokens"] for _, o, _ in rows)
    on_tk = sum(o["tokens"] for _, _, o in rows)
    print("\n修复通过：关闭案例 %d/%d，开启案例 %d/%d" % (off_ok, n, on_ok, n))
    print("token：关闭 %d，开启 %d（%+d）" % (off_tk, on_tk, on_tk - off_tk))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
