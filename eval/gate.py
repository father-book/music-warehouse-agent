"""回归门禁：拿本次评测结果和基线比，判定"改完有没有变差"。

    python eval/gate.py --update-baseline        # 把本次结果定成新基线
    python eval/gate.py                          # 比对上次基线
    python eval/gate.py --current eval/last.json # 指定本次结果
    python eval/gate.py --threshold 3            # 允许的最大下降（百分点）
    python eval/gate.py --sql-diff               # 额外打印通过用例的 SQL 变化

判定规则（任一触发即拒绝合并，退出码 1）：

  1. **整体加权分下降 > threshold**（默认 3 个百分点）
  2. **任一分域加权分下降 > threshold** —— 只看总分会被"某域暴跌被别的域涨回来"掩盖
  3. **任意用例 PASS → FAIL** —— 单题退化不因为"总数还够"就被放过
  4. **用例数减少** —— 覆盖不能靠删题变好看，这条比分数更重要
  5. **覆盖矩阵回退** —— 用例数没少，但都挤在同一个意图里，等于覆盖变薄；
     矩阵按「业务意图 × 域」算格子，掉格即拒绝。

只有 FAIL → PASS 与分数上升会打印，但不影响退出码。

为什么要有这个文件：docs/34 里记过一个反复踩的坑——
"全绿"不代表没坏，只代表**没测到**。42 题全过之后，我在阶段 1 改出过
4 个真 bug，每一个都不是被评测发现的。门禁的意义是把"改完没变差"
从人的记忆变成一条可执行命令。
"""
from __future__ import annotations

import argparse
import io
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
BASELINE = ROOT / "eval" / "baseline.json"
DEFAULT_CURRENT = ROOT / "eval" / "last.json"


def coverage_now() -> dict | None:
    """现场算一次覆盖矩阵。用例文件是唯一事实来源，不读缓存。

    故意不用 current.json 里存的值：那份文件是上次评测时的快照，
    而"删掉一个意图的题、同时在别处加两条"这种改动会让快照仍然好看。
    """
    try:
        sys.path.insert(0, str(ROOT / "eval"))
        import matrix  # noqa: WPS433
        data, cases = matrix.load()
        return matrix.build(data, cases)
    except Exception:
        return None


def load(path: Path) -> dict:
    if not path.exists():
        raise SystemExit("缺少文件：%s\n先跑 `python eval/run.py --json %s`" % (path, path))
    return json.loads(io.open(path, encoding="utf-8").read())


def _cases(d: dict) -> dict[str, dict]:
    return {c["id"]: c for c in d.get("cases") or []}


def compare(base: dict, cur: dict, threshold: float) -> tuple[list[str], list[str], list[str]]:
    """返回 (致命问题, 改进, 提示)。"""
    fatal: list[str] = []
    better: list[str] = []
    notes: list[str] = []

    bo = base.get("stats", {}).get("overall") or {}
    co = cur.get("stats", {}).get("overall") or {}

    # ---- 1. 整体分 ----
    b_score, c_score = float(bo.get("score") or 0), float(co.get("score") or 0)
    delta = round(c_score - b_score, 1)
    if delta < -threshold:
        fatal.append("整体加权分 %.1f → %.1f（下降 %.1f，超过阈值 %.1f）"
                     % (b_score, c_score, -delta, threshold))
    elif delta > 0:
        better.append("整体加权分 %.1f → %.1f（+%.1f）" % (b_score, c_score, delta))

    # ---- 2. 分域 ----
    bd = base.get("stats", {}).get("domains") or {}
    cd = cur.get("stats", {}).get("domains") or {}
    for name, b in bd.items():
        c = cd.get(name)
        if not c:
            fatal.append("分域「%s」整体消失（基线 %.1f 分，%d 题）"
                         % (name, b.get("score") or 0, b.get("n") or 0))
            continue
        dd = round(float(c.get("score") or 0) - float(b.get("score") or 0), 1)
        if dd < -threshold:
            fatal.append("分域「%s」%.1f → %.1f（下降 %.1f）"
                         % (name, b["score"], c["score"], -dd))
        elif dd > 0:
            better.append("分域「%s」%.1f → %.1f（+%.1f）" % (name, b["score"], c["score"], dd))
    for name in cd:
        if name not in bd:
            notes.append("新增分域「%s」（%d 题，%.1f 分）"
                         % (name, cd[name]["n"], cd[name]["score"]))

    # ---- 3. 单题退化 ----
    bc, cc = _cases(base), _cases(cur)
    regressed = [i for i, c in cc.items()
                 if i in bc and bc[i].get("status") == "PASS" and c.get("status") != "PASS"]
    fixed = [i for i, c in cc.items()
             if i in bc and bc[i].get("status") != "PASS" and c.get("status") == "PASS"]
    for i in regressed:
        fatal.append("用例 %s PASS → FAIL：%s\n      %s"
                     % (i, (cc[i].get("question") or "")[:60], (cc[i].get("note") or "")[:160]))
    for i in fixed:
        better.append("用例 %s FAIL → PASS" % i)

    # ---- 4. 覆盖不能缩 ----
    missing = [i for i in bc if i not in cc]
    added = [i for i in cc if i not in bc]
    if missing:
        fatal.append("用例数减少：%d 条不见了（%s%s）"
                     % (len(missing), ", ".join(sorted(missing)[:8]),
                        " 等" if len(missing) > 8 else ""))
    if added:
        notes.append("新增用例 %d 条（%s%s）"
                     % (len(added), ", ".join(sorted(added)[:8]),
                        " 等" if len(added) > 8 else ""))

    # ---- 提示：通过用例但 SQL 变了 ----
    changed = [i for i, c in cc.items()
               if i in bc and bc[i].get("status") == "PASS" and c.get("status") == "PASS"
               and (bc[i].get("sql") or "").strip() != (c.get("sql") or "").strip()]
    if changed:
        notes.append("通过用例中有 %d 条 SQL 发生变化（%s）——数值对但写法变了，"
                     "可能是口径被悄悄改动" % (len(changed), ", ".join(sorted(changed)[:8])))

    # ---- 提示：多数投票下仍不稳定的题 ----
    # 这些题 status 是 PASS（过半即通过），但 "2/3" 说明它三次里错过一次。
    # 只提示不阻断：真要因为 1/3 的抖动就红，门禁又会变成随机报警——
    # 但完全不说也不行，"多数通过"会把一个 33% 失败率的问题藏起来。
    shaky = []
    for i, c in cc.items():
        v = str(c.get("votes") or "")
        if "/" not in v:
            continue
        try:
            ok_n, tot = (int(x) for x in v.split("/", 1))
        except ValueError:
            continue
        if tot > 1 and ok_n < tot:
            shaky.append((i, v, c.get("status")))
    if shaky:
        notes.append("稳定性不足 %d 条（%s）——多数投票判为通过，但三次里错过：值得单独看"
                     % (len(shaky),
                        "、".join("%s %s" % (i, v) for i, v, _ in
                                 sorted(shaky, key=lambda x: x[1])[:8])))

    return fatal, better, notes


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--current", default=str(DEFAULT_CURRENT))
    ap.add_argument("--baseline", default=str(BASELINE))
    ap.add_argument("--threshold", type=float, default=3.0)
    ap.add_argument("--update-baseline", action="store_true")
    ap.add_argument("--sql-diff", action="store_true")
    a = ap.parse_args()

    cur = load(Path(a.current))
    cov = coverage_now()
    if cov:
        cur["coverage_matrix"] = cov
    co = cur.get("stats", {}).get("overall") or {}
    line = ("本次：%d 题 ｜ L1 %.1f%% L2 %.1f%% L3 %.1f%% ｜ 加权 %.1f ｜ %d+%d tok"
            % (co.get("n", 0), co.get("l1", 0), co.get("l2", 0), co.get("l3", 0),
               co.get("score", 0), co.get("tokens_in", 0), co.get("tokens_out", 0)))
    if cov:
        line += " ｜ 覆盖 %.1f%%（%d/%d 格）" % (
            cov["coverage"], cov["cells_ok"], cov["cells_total"])
    print(line)

    if a.update_baseline:
        Path(a.baseline).write_text(
            json.dumps(cur, ensure_ascii=False, indent=1) + "\n", encoding="utf-8")
        print("基线已更新：%s（%d 题）" % (a.baseline, len(cur.get("cases") or [])))
        return 0

    base_path = Path(a.baseline)
    if not base_path.exists():
        print("没有基线文件 %s，先跑一次 --update-baseline" % base_path)
        return 2

    base = load(base_path)
    print("基线：%s（%s）" % (base.get("generated_at"), base.get("meta", {}).get("model")))

    fatal, better, notes = compare(base, cur, a.threshold)

    # ---- 5. 覆盖矩阵回退 ----
    # 放在 compare 之外，因为它读的是用例文件而不是评测结果：
    # "用例数没少但格子掉了"只有矩阵能看出来。
    bb = (base.get("coverage_matrix") or {}).get("coverage")
    cb = (cov or {}).get("coverage")
    if bb is not None and cb is not None:
        if cb < bb:
            gaps = [r["intent"] for r in cov["intents"] if not r["ok"]]
            fatal.append("覆盖矩阵回退：%.1f%% → %.1f%%（缺口：%s）"
                         % (bb, cb, "、".join(gaps) or "—"))
        elif cb > bb:
            better.append("覆盖矩阵 %.1f%% → %.1f%%" % (bb, cb))
    elif bb is None and cb is not None:
        notes.append("基线没有覆盖矩阵数据，本次 %.1f%% 已记录" % cb)

    if better:
        print("\n改善：")
        for x in better:
            print("  ↑ %s" % x)
    if notes:
        print("\n提示：")
        for x in notes:
            print("  · %s" % x)

    if a.sql_diff:
        bc, cc = _cases(base), _cases(cur)
        rows = [i for i in cc if i in bc and (bc[i].get("sql") or "") != (cc[i].get("sql") or "")]
        if rows:
            print("\nSQL 变化：")
            for i in rows:
                print("  %s\n    旧: %s\n    新: %s"
                      % (i, (bc[i].get("sql") or "—")[:150], (cc[i].get("sql") or "—")[:150]))

    if fatal:
        print("\n门禁：不通过（%d 项）" % len(fatal))
        for x in fatal:
            print("  ✗ %s" % x)
        return 1

    print("\n门禁：通过（阈值 %.1f 个百分点，无单题退化、无覆盖缩减）" % a.threshold)
    return 0


if __name__ == "__main__":
    sys.exit(main())
