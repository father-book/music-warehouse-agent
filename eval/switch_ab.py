"""四个 Agent 能力开关的端到端 A/B（真执行、真判 L1/L2/L3）。

    python eval/switch_ab.py --limit 12
    python eval/switch_ab.py --limit 12 --arms off,cot,react
    python eval/switch_ab.py --limit 12 --json eval/switch_ab.json

**和 eval/prompt_ab.py 的区别**：prompt_ab 是离线的，只验"护栏过没过 / 表选对没选对"，
因为它写的时候 Doris 没起。这里跑的是完整链路 —— 生成 → 执行 → 判定，
判据直接用 eval/run.py 的 L1/L2/L3，数字和全量评测同一把尺子。

四档开关（config.yaml → prompt.*）：

    cot      结构化 plan 字段前置（先列口径/表/过滤，再写 SQL）
    fewshot  Verified Query 检索（baseline 115 条 + qa_logs 里相似的正确问答）
    react    Function Calling 循环（Thought→Action→Observation，4 轮/8 次上限）
    plan     独立 Planning 节点，planner.validate_plan 分级校验（与 cot 互斥）

**为什么每档都要关掉缓存**：缓存的 key 里虽然有 features_fingerprint，
但"这几档是不是真的走到了不同分支"本身就是被测对象 —— 让缓存参与进来，
一次命中就能把差异抹平，A/B 变成测缓存。所以这一轮 cfgs 统一 cache.enabled=false。

成本：每题约 ¥0.004~0.01（deepseek-flash）。12 题 × 5 档 ≈ ¥0.3~0.6。
每次跑完都会打印 token 与耗时，不靠估。

注意：subset 是**按 id 排序后等距抽样**，不是随机 —— 随机抽会把名额全给同一种题型，
而 react/plan 的差异恰恰只在多表、多步的问题上才看得出来。
"""
from __future__ import annotations

import argparse
import copy
import importlib.util
import json
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "code" / "agent"))
sys.path.insert(0, str(ROOT / "code"))

import _bootstrap  # noqa: E402,F401

import conf  # noqa: E402
from governance.contracts import RequestContext  # noqa: E402
from graph import ask  # noqa: E402


def _load_run_module():
    """把 eval/run.py 当模块加载，复用它的判据，避免两套尺子。"""
    spec = importlib.util.spec_from_file_location("_eval_run", ROOT / "eval" / "run.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


ARMS = ["off", "cot", "fewshot", "react", "plan"]


def apply_arm(cfg: dict, arm: str) -> dict:
    """把某一档开关打到配置副本上，其余保持默认（全关）。"""
    c = copy.deepcopy(cfg)
    p = c.setdefault("prompt", {})
    p["cot"] = arm == "cot"
    p.setdefault("fewshot", {})["enabled"] = arm == "fewshot"
    p.setdefault("react", {})["enabled"] = arm == "react"
    p.setdefault("plan", {})["enabled"] = arm == "plan"
    p.setdefault("self_consistency", {})["enabled"] = False   # 本轮不测（成本 n 倍）
    # A/B 期间关缓存：命中一次就把档位差异抹平了，测的就不是开关了。
    c.setdefault("cache", {})["enabled"] = False
    return c


def pick_cases(cases: list[dict], limit: int) -> list[dict]:
    """等距抽样：只取"真走 LLM 的 value 题"，再按 id 排序均匀取 limit 条。

    池子要两边对齐：
      · cases.yaml 给题干与期望值（才有 expected 可比）；
      · baseline.json 给"上次这条写出的 SQL 是什么"和当时是否 PASS——
        cases.yaml 里根本没有 sql 字段，一开始按 c["sql"] 过滤，结果全被滤掉，
        12 题只剩 P06 一条，五档全是 1 秒返回、0 token，等于没测。
    """
    base = json.loads((ROOT / "eval" / "baseline.json").read_text(encoding="utf-8"))
    ok = {str(c.get("id")) for c in base["cases"]
          if str(c.get("status") or "").upper() == "PASS" and (c.get("sql") or "").strip()}

    usable = []
    for c in cases:
        if c.get("mode") == "stub":
            continue
        if c.get("type") != "value":
            continue
        if "零LLM" in (c.get("tags") or []):
            continue
        if str(c.get("id")) not in ok:
            continue
        usable.append(c)
    usable.sort(key=lambda c: str(c.get("id")))
    if limit <= 0 or limit >= len(usable):
        return usable
    step = len(usable) / float(limit)
    return [usable[int(i * step)] for i in range(limit)]


def run_arm(evalrun, cfg: dict, cases: list[dict], idp) -> tuple[dict, list]:
    rows = []
    t0 = time.perf_counter()
    for c in cases:
        use_cfg = cfg
        if c.get("mode") == "stub":
            use_cfg = copy.deepcopy(cfg)
            use_cfg["generator"]["mode"] = "stub"
        ctx = (idp.context_for_user(c["user"], source="eval")
               if c.get("user") else RequestContext.system(source="eval"))
        ts = time.perf_counter()
        try:
            st = ask(c["question"], use_cfg, c.get("sql"), ctx=ctx)
            status, note, detail = evalrun.judge(c, st)
        except Exception as e:
            status, note = evalrun.FAIL, "异常：%s: %s" % (type(e).__name__, str(e)[:100])
            detail, st = {"sql": "", "findings": []}, {}
        dt = time.perf_counter() - ts
        detail["l1"] = evalrun._l1(c, st)
        detail["l3"] = evalrun._l3(c, st, status == evalrun.PASS)
        rows.append((c, status, note, c.get("_domain", ""), c.get("_intent", ""), detail, dt))
    stats = evalrun.summarize(rows)
    stats["_elapsed"] = time.perf_counter() - t0
    return stats, rows


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=12, help="抽多少题（0=全部 PASS 题）")
    ap.add_argument("--arms", default=",".join(ARMS), help="逗号分隔，如 off,cot,react")
    ap.add_argument("--json", default="", help="机读结果路径")
    a = ap.parse_args()

    evalrun = _load_run_module()
    data = evalrun.load_cases(with_adversarial=False)
    taxonomy = data.get("taxonomy") or {}

    cases = pick_cases(data["cases"], a.limit)
    for c in cases:
        c["_domain"] = evalrun._classify(c, taxonomy, "domain")
        c["_intent"] = evalrun._classify(c, taxonomy, "intent")

    cfg0 = conf.load_config()
    from governance.identity import provider
    idp = provider(cfg0)

    arms = [x.strip() for x in a.arms.split(",") if x.strip()]
    print("== 开关端到端 A/B：%d 题 × %d 档 ==" % (len(cases), len(arms)))
    print("题目：%s\n" % ", ".join(str(c.get("id")) for c in cases))

    results = {}
    for arm in arms:
        cfg = apply_arm(cfg0, arm)
        print("---- %s ----" % arm)
        stats, rows = run_arm(evalrun, cfg, cases, idp)
        o = stats["overall"]
        results[arm] = {"stats": stats,
                        "cases": [{"id": str(c.get("id")), "status": s, "note": n,
                                   "l1": d.get("l1"), "l3": d.get("l3"), "sec": round(dt, 2)}
                                  for c, s, n, _dm, _it, d, dt in rows]}
        print("  L1 %.1f%%  L2 %.1f%%  L3 %.1f%%  通过 %d/%d  加权 %.1f  %.0fs  %d+%d tok"
              % (o["l1"], o["l2"], o["l3"], o["passed"], o["n"], o["score"],
                 stats["_elapsed"], o["tokens_in"], o["tokens_out"]))
        for c, s, n, _dm, _it, _d, dt in rows:
            if s != evalrun.PASS:
                print("    FAIL %-4s %-40s %s" % (c.get("id"), str(c.get("question"))[:40], n[:70]))
        print()

    # ---- 汇总对比表：以 off 为基准算增量
    base = results.get("off") or next(iter(results.values()))
    bo = base["stats"]["overall"]
    print("== 对比（基准 = %s） ==" % ("off" if "off" in results else arms[0]))
    print("  %-9s %6s %6s %6s %8s %8s %9s %8s" %
          ("档位", "L1", "L2", "L3", "加权", "Δ加权", "通过", "耗时s"))
    for arm in arms:
        o = results[arm]["stats"]["overall"]
        print("  %-9s %5.1f%% %5.1f%% %5.1f%% %8.1f %+8.1f %5d/%-3d %8.0f"
              % (arm, o["l1"], o["l2"], o["l3"], o["score"], o["score"] - bo["score"],
                 o["passed"], o["n"], results[arm]["stats"]["_elapsed"]))

    # 逐题翻转（同一题在两档之间 PASS→FAIL / FAIL→PASS）
    if "off" in results:
        print("\n== 逐题翻转（相对 off） ==")
        off = {x["id"]: x["status"] for x in results["off"]["cases"]}
        for arm in arms:
            if arm == "off":
                continue
            cur = {x["id"]: x["status"] for x in results[arm]["cases"]}
            diff = [k for k in off if off[k] != cur.get(k)]
            if not diff:
                print("  %-9s 无翻转" % arm)
            else:
                print("  %-9s %s" % (arm, ", ".join(
                    "%s %s→%s" % (k, off[k], cur[k]) for k in diff)))

    if a.json:
        out = {"cases": [str(c.get("id")) for c in cases],
               "arms": {k: {"overall": v["stats"]["overall"],
                            "elapsed": v["stats"]["_elapsed"],
                            "cases": v["cases"]} for k, v in results.items()},
               "ts": time.strftime("%Y-%m-%d %H:%M:%S")}
        Path(a.json).write_text(json.dumps(out, ensure_ascii=False, indent=1), encoding="utf-8")
        print("\n机读结果已写入 %s" % a.json)
    return 0


if __name__ == "__main__":
    sys.exit(main())
