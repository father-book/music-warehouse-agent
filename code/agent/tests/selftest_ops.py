#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""运营可观测聚合自测（P3）：口径正确 + 只读 + 结构完整。"""
from __future__ import annotations

import sys

import _bootstrap  # noqa: F401

import opsmetrics
from conf import load_config

PASS = FAIL = 0
FAILS: list[str] = []


def expect(cond, name, extra=""):
    global PASS, FAIL
    if cond:
        PASS += 1
        print("  ok   %s" % name)
    else:
        FAIL += 1
        FAILS.append(name)
        print("  FAIL %s %s" % (name, extra))


def main():
    cfg = load_config()
    d = opsmetrics.collect(cfg, 7)
    k = d.get("kpi") or {}

    print("\n[1] 结构与字段")
    expect(d.get("ok") and d.get("scope") == "aggregate", "聚合可用且标明是脱敏聚合")
    expect({"questions", "answered", "refused", "errors", "guard_hits",
            "rt_queries", "availability", "answer_rate", "refuse_rate",
            "error_rate", "guard_hit_rate", "p95_ms", "avg_ms",
            "cost_cny", "tokens_in", "tokens_out"} <= set(k), "KPI 字段齐全")
    expect(k["questions"] > 0, "有真实审计样本（%s 条）" % k.get("questions"))
    expect(bool(d.get("note")) and "拒答" in d["note"], "口径说明写明拒答不等于失败")

    print("\n[2] 口径一致性")
    n = k["questions"]
    expect(abs(k["availability"] - round(100.0 * (n - k["errors"]) / n, 3)) < 0.01,
           "availability = 1 - error_rate（不受拒答影响）")
    expect(abs(k["answer_rate"] - round(100.0 * k["answered"] / n, 2)) < 0.01,
           "answer_rate 按 intent 统计")
    expect(abs(k["guard_hit_rate"] - round(100.0 * k["guard_hits"] / n, 2)) < 0.01,
           "护栏命中率按 REJECT findings 统计")
    expect(k["p95_ms"] >= k["avg_ms"], "P95 ≥ 均值")
    expect(k["cost_cny"] >= 0 and k["tokens_in"] >= 0, "成本与 token 非负")
    expect(k["refuse_rate"] + k["answer_rate"] <= 100.01,
           "拒答率 + 回答率不超 100%")

    print("\n[3] 趋势与分布")
    s = d.get("series") or []
    expect(len(s) >= 1, "至少有 1 天趋势")
    expect([x["date"] for x in s] == sorted(x["date"] for x in s), "趋势按日期升序")
    expect(all({"date", "n", "errors", "refused", "guard_hits", "p95_ms",
                "cost_cny"} <= set(x) for x in s), "每日字段齐全")
    expect(sum(x["n"] for x in s) <= n + 1, "按天汇总不超过总数（窗口边界一致）")
    expect(len(d.get("by_role") or []) >= 1, "角色分布非空")

    print("\n[4] 参数边界与只读")
    expect(opsmetrics.collect(cfg, 0)["days"] == 1, "days=0 被夹到 1")
    expect(opsmetrics.collect(cfg, 999)["days"] == 90, "days=999 被夹到 90")
    d1 = opsmetrics.collect(cfg, 1)
    expect(d1["kpi"]["questions"] <= n, "1 天窗口样本数 ≤ 7 天窗口")
    snap = opsmetrics.collect(cfg, 7)
    expect(snap["kpi"]["questions"] == n, "重复聚合结果稳定（未在聚合中写库）")

    print("\n%d passed, %d failed" % (PASS, FAIL))
    if FAILS:
        print("失败项：" + "、".join(FAILS))
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(main())
