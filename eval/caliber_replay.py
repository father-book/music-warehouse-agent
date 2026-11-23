"""口径规则离线回放：确认新增规则不误伤历史正确 SQL。

    python eval/caliber_replay.py
    python eval/caliber_replay.py --json eval/caliber_replay.json

为什么需要这一步：护栏是「改一条规则、全链路同时生效」的，误伤不会报错，
只会把本来能答对的题推进修复闭环、三轮用尽转拒答 —— 评测里表现为莫名其妙掉分。
所以新规则上线前，必须拿**已经判过 PASS 的历史 SQL** 回放一遍，判据是：

  · status=PASS 的 SQL：新规则产生的 REJECT 必须为 0；
  · 已知的确定性错例（V16 错分母 / C19 笔数当人数）：必须被拦到。

成本 0 token，只跑静态护栏。规则来自 tables.json（由 gen_tables.py 从
metrics.yaml 合并），改完 YAML 忘了重生成，这里会回放出一模一样的旧结果。
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "code" / "agent"))

import _bootstrap  # noqa: E402,F401

import guardtool  # noqa: E402
from conf import load_config  # noqa: E402

NEW_RULES = ("R25-口径-完播率分母", "R25-口径-用户占比分母", "R25-口径-占比分子分母同源")

# 三条确定性错例。它们不一定还在最近一次评测里（C19 已被语义层修好），
# 但规则必须能拦住——否则「防回归」就是空话。
KNOWN_BAD = {
    "V16_bad": (
        "SELECT SUM(CASE WHEN play_progress >= 0.9 THEN 1 ELSE 0 END) / "
        "NULLIF(COUNT(*), 0) AS complete_rate FROM dwd_play_end "
        "WHERE dt = '2026-11-08'", "R25-口径-完播率分母"),
    "C19_bad": (
        "SELECT SUM(CASE WHEN is_vip = 'true' THEN 1 ELSE 0 END) / "
        "NULLIF(COUNT(*), 0) AS vip_ratio FROM dwd_pay "
        "WHERE dt = '2026-11-08'", "R25-口径-用户占比分母"),
    # C15 第 2 轮实测错写法：分子用 dws 别名列、分母用物理列，
    # 原始列名不一致 → C6 REJECT（docs/61 §九、§十）
    "C15_bad": (
        "SELECT MAX(province_play) / "
        "NULLIF(SUM(play_start_cnt), 0) AS top_province_ratio "
        "FROM ads_province_dist WHERE dt = '2026-11-08'",
        "R25-口径-占比分子分母同源"),
}


def collect() -> list[dict]:
    """两个来源合并去重：旧基线 126 题 + 最近一次全量 128 题。"""
    seen = set()
    out = []
    for path in ("eval/baseline.json", "eval/last.json"):
        p = ROOT / path
        if not p.exists():
            continue
        d = json.loads(p.read_text(encoding="utf-8"))
        for c in d.get("cases") or []:
            sql = (c.get("sql") or "").strip()
            if not sql or sql in seen:
                continue
            seen.add(sql)
            out.append({"id": c.get("id"), "status": c.get("status"),
                        "sql": sql, "source": path})
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--json", default="", help="机读结果路径")
    a = ap.parse_args()

    cfg = load_config()
    cases = collect()
    t0 = time.perf_counter()
    hits_pass, hits_fail, errors = [], [], []

    for c in cases:
        try:
            verdict, findings, err = guardtool.check(c["sql"], cfg)
        except Exception as e:
            errors.append({"id": c["id"], "error": "%s: %s" % (type(e).__name__, e)})
            continue
        new = [f for f in findings
               if f.get("level") == "REJECT" and (f.get("rule") or "") in NEW_RULES]
        if err and not findings:
            errors.append({"id": c["id"], "error": err[:160]})
        if new:
            row = {"id": c["id"], "status": c["status"], "source": c["source"],
                   "rules": [f["rule"] for f in new], "sql": c["sql"]}
            (hits_pass if c["status"] == "PASS" else hits_fail).append(row)

    bad_hits = 0
    print("== 口径规则离线回放 ==")
    for name, (sql, want) in KNOWN_BAD.items():
        _v, fs, _e = guardtool.check(sql, cfg)
        got = [f["rule"] for f in fs if f.get("level") == "REJECT"]
        hit = want in got
        bad_hits += int(hit)
        print("  %s %-9s 期望 %s ｜ 实得 %s"
              % ("✓" if hit else "!", name, want, ",".join(got) or "无"))

    print("\nSQL 总数 %d（去重，拒答题无 SQL 故少于题数）｜ 耗时 %.1fs ｜ 护栏异常 %d"
          % (len(cases), time.perf_counter() - t0, len(errors)))
    print("\n[判据 1] 历史 PASS 的 SQL 被新规则打红：%d 条（必须为 0）"
          % len(hits_pass))
    for r in hits_pass:
        print("  ! %-5s %-8s %s" % (r["id"], r["source"], ",".join(r["rules"])))
        print("    %s" % r["sql"][:180])
    print("\n[判据 2] 历史题库里残留的错例被拦下：%d 条"
          "（0 表示题库里已没有这种写法；确定性覆盖由上面的内置错例保证）"
          % len(hits_fail))
    for r in hits_fail:
        print("  ✓ %-5s %-8s %s" % (r["id"], r["source"], ",".join(r["rules"])))
    if errors:
        print("\n[异常] %d 条" % len(errors))
        for e in errors[:5]:
            print("  ! %s %s" % (e["id"], e["error"][:120]))

    ok = (not hits_pass and not errors and bad_hits == len(KNOWN_BAD))
    print("\n结论：%s（内置错例 %d/%d）"
          % ("回放通过，可以上线" if ok else "**有误伤或异常，先收紧规则**",
             bad_hits, len(KNOWN_BAD)))

    if a.json:
        Path(a.json).write_text(json.dumps(
            {"n": len(cases), "hits_pass": hits_pass, "hits_fail": hits_fail,
             "known_bad_hits": bad_hits, "errors": errors, "ok": ok},
            ensure_ascii=False, indent=1), encoding="utf-8")
        print("机读结果已写入 %s" % a.json)
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
