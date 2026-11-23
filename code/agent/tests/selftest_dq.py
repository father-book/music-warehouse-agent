#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""统一 DQ 规则表 / 阈值告警 / 落库趋势自测（P2）。

不造数据：collect 只读现有元数据、只读现有库表、跑确定性子进程。
唯一写动作是 dq.run() 往治理库追加一次体检记录（幂等追加，不清历史）。
"""
from __future__ import annotations

import copy
import sys

import tempfile
import os
import json
import io

import _bootstrap  # noqa: F401

import dq
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


def main(offline: bool = False):
    cfg = load_config()
    rules = dq.load_rules()

    print("\n[1] 规则表自身完整（规则是数据，写错会静默不检查）")
    checks = rules.get("checks") or []
    fams = rules.get("families") or []
    expect(len(checks) == 12, "12 条可执行检查")
    expect(len(fams) == 6, "6 个规则族 L0~L5")
    ids = [c.get("id") for c in checks]
    expect(len(set(ids)) == len(ids) and all(ids), "规则 id 唯一且非空")
    expect(all(c.get("metric") and c.get("op") and c.get("limit") is not None
               and c.get("severity") in ("reject", "warn", "info") and c.get("layer")
               for c in checks), "每条都有指标/比较/阈值/级别/分层")
    expect(all(c.get("why") and c.get("source") for c in checks),
           "每条都写明依据与实现位置")
    expect({f["id"] for f in fams} == {"L0", "L1", "L2", "L3", "L4", "L5"},
           "规则族覆盖 L0~L5")

    print("\n[2] 比较器")
    for v, op, lim, want in [(0, "==", 0, True), (1, "==", 0, False),
                             (4.4, "<=", 10, True), (47.75, "<=", 60, True),
                             (100, ">=", 95, True), (94, ">=", 95, False),
                             (0, ">", 0, False), (1, "!=", 0, True),
                             (None, "==", 0, False), ("abc", "<=", 1, False)]:
        expect(dq._compare(v, op, lim) is want,
               "%r %s %r → %s" % (v, op, lim, want))

    print("\n[2b] DQ09 只认最近一次全量回归（离线，不改真实 history.jsonl）")

    def _hist(rows):
        fd, p = tempfile.mkstemp(suffix=".jsonl")
        os.close(fd)                # 不关的话 Windows 上临时文件删不掉
        with io.open(p, "w", encoding="utf-8") as f:
            for r in rows:
                f.write(json.dumps(r) + "\n")
        return p

    def _ci(path):
        orig = dq.HISTORY
        dq.HISTORY = path
        try:
            return dq._metric("ci_failed", {})
        finally:
            dq.HISTORY = orig

    p1 = _hist([
        {"n": 128, "passed": 128, "partial": False},
        {"n": 126, "passed": 124, "partial": False},
        {"n": 46, "passed": 46, "partial": True},
        {"n": 46, "passed": 46, "partial": True},
    ])
    p2 = _hist([{"n": 46, "passed": 46, "partial": True}])
    p3 = _hist([])
    try:
        last, note = dq.last_full_regression(p1)
        expect(last is not None and last["n"] == 126 and note == "最近一次全量回归",
               "跳过末尾 quick，取到最近一条 full（126 题）")
        v, desc = _ci(p1)
        expect(v == 2, "DQ09 报 2 个失败（不是被 quick 的 0 掩盖）", desc)
        expect("全量" in desc, "说明里标明取自全量回归", desc)
        last2, note2 = dq.last_full_regression(p2)
        expect(last2 is not None and "回退" in note2, "没有 full 时回退并标注")
        v2, desc2 = _ci(p2)
        expect(v2 == 0 and "回退" in desc2, "回退结果诚实标注为可能不完整", desc2)
        v3, desc3 = _ci(p3)
        expect(v3 == -1, "无记录时判失败，不把 0-0 当全绿", desc3)
    finally:
        for p in (p1, p2, p3):
            try:
                os.unlink(p)
            except OSError:
                pass                # 临时文件清理失败不该让自测判红

    if offline:
        # 规则表、比较器、DQ09 取数都不碰数据库/集群；环境没起时也能验证。
        # [3]~[5] 需要 PostgreSQL / Doris 在线，留给完整回归跑。
        print("\n[离线模式] 跳过 [3]~[5]（需 PostgreSQL / Doris 在线）")
        print("\n%d passed, %d failed" % (PASS, FAIL))
        if FAILS:
            print("失败项：" + "、".join(FAILS))
        return 1 if FAIL else 0

    print("\n[3] 实跑一遍（12 条确定性检查）")
    rs = dq.collect(cfg)
    expect(len(rs) == 12, "12 条结果")
    expect(all(r.get("error") == "" for r in rs), "无检查自身异常",
           str([(r["rule_id"], r["error"]) for r in rs if r["error"]]))
    expect(all({"rule_id", "layer", "name_cn", "severity", "value",
                "threshold", "detail", "ok", "alert"} <= set(r) for r in rs),
           "结果字段齐全")
    info = [r for r in rs if r["severity"] == "info"]
    expect(info and all(not r["alert"] for r in info),
           "info 级（实时链路 down）只记录不算告警")
    expect(rs[0]["name_cn"] == "语义层一致性检查失败数" and rs[0]["value"] == 0,
           "DQ01 读到 sem_check 失败数 0")
    dq05 = next(r for r in rs if r["rule_id"] == "DQ05")
    expect(abs(float(dq05["value"]) - 4.4068) < 0.01,
           "DQ05 总体异常率约 4.4068%（Σerrors/Σevents）",
           str(dq05["value"]))
    dq12 = next(r for r in rs if r["rule_id"] == "DQ12")
    expect(dq12["value"] == 0 and "27/27" in dq12["detail"],
           "DQ12 授权缺口 0（9 表 × 3 角色）")

    print("\n[4] 阈值可配置且能触发告警（内存注入，不改 YAML）")
    orig = dq.load_rules
    tightened = copy.deepcopy(rules)
    for c in tightened["checks"]:
        if c["id"] == "DQ05":
            c["limit"] = 3.0
    dq.load_rules = lambda: tightened
    try:
        rs2 = dq.collect(cfg)
        alert5 = next(r for r in rs2 if r["rule_id"] == "DQ05")
        expect(alert5["alert"] and not alert5["ok"],
               "阈值收紧到 3.0 后 DQ05 触发告警",
               str(alert5))
        expect(sum(1 for r in rs2 if r["alert"]) == 1, "只告警受影响的那一条")
    finally:
        dq.load_rules = orig
    expect(not any(r["alert"] for r in dq.collect(cfg)), "恢复原阈值后告警消失")

    print("\n[5] 落库与趋势")
    r = dq.run(cfg)
    expect(r.get("stored") and r["summary"]["total"] == 12,
           "体检结果落库成功", str(r.get("summary")))
    rep = dq.report(cfg)
    expect(rep.get("ok") and len(rep.get("results") or []) == 12,
           "report 能读回最新一次 12 条明细")
    expect(len(rep.get("trend") or []) >= 2, "趋势至少有两次运行（可回放）",
           str(len(rep.get("trend") or [])))
    expect(rep["trend"][0]["run_id"] < rep["trend"][-1]["run_id"],
           "趋势按时间升序（旧→新）")
    expect(len(rep.get("families") or []) == 6, "报告带规则族清单")

    print("\n%d passed, %d failed" % (PASS, FAIL))
    if FAILS:
        print("失败项：" + "、".join(FAILS))
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(main(offline="--offline" in sys.argv))
