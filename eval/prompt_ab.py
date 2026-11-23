"""Prompt 三件套 A/B：CoT / Few-shot 各自的增量到底值不值。

    python eval/prompt_ab.py --limit 20
    python eval/prompt_ab.py --limit 20 --json eval/prompt_ab.json
    python eval/prompt_ab.py --only cot,fewshot      # 只跑这两档

**为什么不跑全链路**：全链路要 Doris（当前未起），而且 L2「数对」需要真执行。
这里换成两个不依赖执行、但能证伪的判据：

  guard_pass   生成的 SQL 通过 26 条静态护栏（该拦的没拦 = 直接不可用）
  table_match  用到的表与评测基线里那条 SQL 完全一致（选错表 = 口径必然错）

两项都是"必要不充分"：过了不代表数对，但没过一定不能用。用它来判"要不要开"，
比凭感觉说"CoT 让模型更严谨"可靠。真实数值正确性仍以 eval/run.py 的 L2 为准。

成本：每次生成约 ¥0.004（deepseek-flash，1.9k token）。20 题 × 4 档 ≈ ¥0.35。
脚本每次运行都会把 token 与成本打出来，不靠估。
"""
from __future__ import annotations

import argparse
import copy
import json
import random
import re
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "code" / "agent"))
sys.path.insert(0, str(ROOT / "code"))

import _bootstrap  # noqa: E402,F401

import conf  # noqa: E402
import guardtool  # noqa: E402
import node_generate_llm as gen  # noqa: E402
import node_route  # noqa: E402
import semantic  # noqa: E402

_RE_TABLE = re.compile(r"\b(?:from|join)\s+([a-zA-Z_][\w.]*)", re.I)
_RE_DATE = re.compile(r"\d{4}-\d{2}-\d{2}")

ARM_NAMES = ["off", "cot", "fewshot", "cot+fewshot"]


def tables_of(sql: str, wl: set[str]) -> set[str]:
    return {t.split(".")[-1].lower() for t in _RE_TABLE.findall(sql or "")} & wl


def pick_cases(limit: int) -> list[dict]:
    """按答案形态分层抽样：随机抽会把 20 个名额全给 scalar，看不出结构差异。"""
    d = json.loads((ROOT / "eval" / "baseline.json").read_text(encoding="utf-8"))
    usable = []
    for c in d["cases"]:
        if str(c.get("status") or "").upper() != "PASS":
            continue
        if not (c.get("sql") or "").strip():
            continue
        # 必须带显式日期：route 要靠它解析时间窗，否则 state 里没有 sql_hint，
        # 测的就不是"生成能力"而是"猜日期"
        if not _RE_DATE.search(c.get("question") or ""):
            continue
        usable.append(c)
    by_shape: dict[str, list[dict]] = {}
    for c in usable:
        by_shape.setdefault(str(c.get("shape") or "scalar"), []).append(c)
    rnd = random.Random(42)
    out: list[dict] = []
    shapes = sorted(by_shape)
    while len(out) < limit and any(by_shape[s] for s in shapes):
        for s in shapes:
            if by_shape[s] and len(out) < limit:
                out.append(by_shape[s].pop(rnd.randrange(len(by_shape[s]))))
    return out


def arm_cfg(base: dict, arm: str) -> dict:
    c = copy.deepcopy(base)
    c.setdefault("prompt", {})
    c["prompt"]["cot"] = arm in ("cot", "cot+fewshot")
    c["prompt"].setdefault("fewshot", {})["enabled"] = arm in ("fewshot", "cot+fewshot")
    c["prompt"].setdefault("self_consistency", {})["enabled"] = False
    return c


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=20)
    ap.add_argument("--json", default="")
    ap.add_argument("--only", default="", help="逗号分隔，如 cot,fewshot")
    args = ap.parse_args()

    arms = [a for a in ARM_NAMES if not args.only or a in args.only.split(",")]
    base = conf.load_config()
    wl = set(semantic.all_tables(base))
    cases = pick_cases(args.limit)
    print("题数 %d，档位 %s\n" % (len(cases), ", ".join(arms)))

    results: dict[str, dict] = {a: {"guard_pass": 0, "table_match": 0, "n": 0,
                                    "tok": 0, "cost": 0.0, "fail": 0,
                                    "sec": 0.0} for a in arms}
    detail: list[dict] = []

    for i, c in enumerate(cases, 1):
        q = c["question"]
        exp = tables_of(c["sql"], wl)
        print("[%2d/%d] %s" % (i, len(cases), q[:48]))
        for arm in arms:
            cfg = arm_cfg(base, arm)
            # route() 返回的是**增量**（LangGraph 负责与入参合并），所以这里必须
            # 自己把 question 并回去。少了这一步，fewshot 拿到空问题→静默不注入，
            # A/B 会得出"few-shot 零影响"的假结论（第一版就这样，token 一模一样）。
            st = {"question": q}
            st.update(node_route.route({"question": q, "ctx": {
                "trace_id": "ab-%s-%s" % (arm, c["id"]),
                "user_id": "internal-baseline", "role": "system"}}, cfg))
            t0 = time.time()
            sql, _detail, meta = gen.generate_with_llm(st, cfg)
            dt = time.time() - t0
            r = results[arm]
            r["n"] += 1
            r["sec"] += dt
            r["tok"] += int(meta.get("prompt_tokens") or 0) + int(
                meta.get("completion_tokens") or 0)
            r["cost"] += float(meta.get("cost_cny") or 0.0)
            if not sql:
                r["fail"] += 1
                print("      %-12s 生成失败 %s" % (arm, str(meta.get("error"))[:60]))
                detail.append({"id": c["id"], "arm": arm, "ok": False,
                               "reason": "no sql", "sql": ""})
                continue
            verdict, findings, _e = guardtool.check(sql, cfg, question=q)
            got = tables_of(sql, wl)
            gpass = verdict != "reject"
            tmatch = got == exp and bool(exp)
            r["guard_pass"] += int(gpass)
            r["table_match"] += int(tmatch)
            flag = ("护栏✓" if gpass else "护栏✗") + (" 表✓" if tmatch else " 表✗")
            print("      %-12s %s  %d tok  %s" % (
                arm, flag, meta.get("prompt_tokens") or 0,
                ("缺:" + ",".join(sorted(exp - got))) if not tmatch else ""))
            detail.append({"id": c["id"], "arm": arm, "ok": True, "guard": verdict,
                           "table_match": tmatch, "got": sorted(got),
                           "exp": sorted(exp), "sql": sql[:400],
                           "rejects": [f.get("rule") for f in findings
                                       if f.get("level") == "REJECT"]})

    print("\n%-14s %10s %10s %10s %10s %10s" % (
        "档位", "护栏通过", "表一致", "均token", "均耗时", "成本¥"))
    for a in arms:
        r = results[a]
        n = max(1, r["n"])
        print("%-14s %8.1f%% %9.1f%% %10d %8.1fs %9.4f" % (
            a, 100 * r["guard_pass"] / n, 100 * r["table_match"] / n,
            r["tok"] // n, r["sec"] / n, r["cost"]))

    b = results[arms[0]]
    print("\n相对 off：")
    for a in arms[1:]:
        r = results[a]
        n = max(1, r["n"])
        nb = max(1, b["n"])
        print("  %-12s 护栏 %+5.1fpp  表一致 %+5.1fpp  token %+5.0f%%  成本 %+.4f" % (
            a,
            100 * (r["guard_pass"] / n - b["guard_pass"] / nb),
            100 * (r["table_match"] / n - b["table_match"] / nb),
            100 * ((r["tok"] / n) / max(1, b["tok"] / nb) - 1),
            r["cost"] - b["cost"]))

    if args.json:
        Path(args.json).write_text(json.dumps({
            "generated_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
            "limit": len(cases), "arms": arms,
            "results": results, "detail": detail,
        }, ensure_ascii=False, indent=1), encoding="utf-8")
        print("\n已写 " + args.json)
    return 0


if __name__ == "__main__":
    sys.exit(main())
