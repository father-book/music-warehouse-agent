"""覆盖矩阵：用例不是越多越好，是"每一格都要有人"。

一个通过率 100% 的评测集也可能毫无信息量——如果它只覆盖「单日单表求和」。
真实的反例是阶段 2 结束时：90 条用例里「单表指标」19 条、「越权」10 条，
而「归因」只有 1 条、「分布」0 条。通过率很好看，但只要用户换一种问法
（"各曲风分别多少"、"哪个省份贡献最多"），系统就从没被验证过。

本模块把"覆盖"变成可判定的数字：

  每个业务意图 ≥ N 条，且 ≥ M 个业务域。
  两个维度缺一不可——5 条全挤在一个域里，说明只在一个场景下验过。

横切意图（越权/写操作/闲聊/注入）不进矩阵：它们衡量"边界守不守得住"，
不衡量"业务答得对不对"，混在一起会让覆盖率虚高。

    python eval/matrix.py                 # 打印矩阵
    python eval/matrix.py --json m.json   # 机读，供 gate.py 卡阈值
    python eval/matrix.py --strict        # 有缺口就 exit 1（CI 用）
"""
from __future__ import annotations

import argparse
import io
import json
import sys
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "eval"))


def load() -> tuple[dict, list[dict]]:
    data = yaml.safe_load(io.open(ROOT / "eval" / "cases.yaml", encoding="utf-8").read())
    cases = list(data.get("cases") or [])
    adv = ROOT / "eval" / "adversarial.yaml"
    if adv.exists():
        extra = yaml.safe_load(io.open(adv, encoding="utf-8").read())
        for c in extra.get("cases") or []:
            c.setdefault("intent", "注入对抗")
            c.setdefault("domain", "边界与安全")
        cases += list(extra.get("cases") or [])
    return data, cases


def build(data: dict, cases: list[dict]) -> dict:
    spec = data.get("coverage") or {}
    intents: list[str] = list(spec.get("intents") or [])
    overrides: dict = spec.get("overrides") or {}
    cross = set(spec.get("cross_cut_intents") or [])
    d_min = int(spec.get("default_min", 5))
    d_dom = int(spec.get("default_domains", 2))

    rows = []
    for it in intents:
        sel = [c for c in cases if str(c.get("intent")) == it]
        doms = sorted({str(c.get("domain")) for c in sel})
        ov = overrides.get(it) or {}
        need = int(ov.get("min", d_min))
        need_dom = int(ov.get("domains", d_dom))
        rows.append({
            "intent": it,
            "n": len(sel),
            "domains": doms,
            "need": need,
            "need_domains": need_dom,
            "ok": len(sel) >= need and len(doms) >= need_dom,
            "gap": max(0, need - len(sel)),
            "gap_domains": max(0, need_dom - len(doms)),
        })

    # 不在白名单里的意图 = 分类漏了，或者新加的题忘了归类。两者都要暴露。
    known = set(intents) | cross
    stray = sorted({str(c.get("intent")) for c in cases} - known)

    ok = sum(1 for r in rows if r["ok"])
    return {
        "intents": rows,
        "cross_cut": sorted(cross),
        "unknown_intents": stray,
        "cells_ok": ok,
        "cells_total": len(rows),
        "coverage": round(100.0 * ok / len(rows), 1) if rows else 0.0,
        "total_cases": len(cases),
    }


def render(m: dict) -> str:
    L = ["覆盖矩阵（业务意图 × 域）", ""]
    L.append("  %-10s %5s %5s %10s %6s" % ("意图", "题数", "域数", "门槛", "状态"))
    L.append("  " + "-" * 44)
    for r in m["intents"]:
        L.append("  %-10s %5d %5d %10s %6s" % (
            r["intent"], r["n"], len(r["domains"]),
            "%d 条/%d 域" % (r["need"], r["need_domains"]),
            "✓" if r["ok"] else "✗"))
    L.append("")
    L.append("  %d/%d 格达标，覆盖率 %.1f%%（用例合计 %d 条）"
             % (m["cells_ok"], m["cells_total"], m["coverage"], m["total_cases"]))

    gaps = [r for r in m["intents"] if not r["ok"]]
    if gaps:
        L.append("")
        L.append("  缺口：")
        for r in gaps:
            bits = []
            if r["gap"]:
                bits.append("少 %d 条" % r["gap"])
            if r["gap_domains"]:
                bits.append("少 %d 个域（当前 %s）"
                            % (r["gap_domains"], "、".join(r["domains"]) or "无"))
            L.append("    · %s：%s" % (r["intent"], "，".join(bits)))
    if m["unknown_intents"]:
        L.append("")
        L.append("  未归类意图（不在矩阵白名单里）：%s" % "、".join(m["unknown_intents"]))
    L.append("")
    L.append("  横切意图（不进矩阵）：%s" % "、".join(m["cross_cut"]))
    return "\n".join(L)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--json", default="")
    ap.add_argument("--strict", action="store_true")
    a = ap.parse_args()

    data, cases = load()
    m = build(data, cases)
    print(render(m))
    if a.json:
        io.open(a.json, "w", encoding="utf-8").write(
            json.dumps(m, ensure_ascii=False, indent=1) + "\n")
        print("\n矩阵已写入 %s" % a.json)
    return 1 if (a.strict and m["coverage"] < 100.0) else 0


if __name__ == "__main__":
    sys.exit(main())
