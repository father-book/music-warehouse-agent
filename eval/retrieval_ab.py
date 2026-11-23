"""检索层 A/B：BM25 / 稠密 / 混合 / rerank 的消融。

    python eval/retrieval_ab.py            # 表格
    python eval/retrieval_ab.py --json eval/retrieval_ab.json

**这不是业务指标。** 它测的是"路由召回的表，是否覆盖最终 SQL 实际用的表"——
一个检索层的代理指标（proxy metric）。全链路通过率要看 eval/run.py 的 L1/L2/L3。
之所以单拎出来：检索层的改动不调模型，几十毫秒能跑完 115 题，可以高频回归；
而全链路每题要 2~4 秒且花 token，不适合每次都跑。

Ground truth 从 baseline.json 的 sql 字段反抽（FROM/JOIN 的表，且必须在 42 张
白名单表内）。不另写期望，避免"自己出题自己判"。

题目只取 SQL 里带表的那部分（126 题里 115 题），剩下 11 题是拒答/元问题，本来
就不该召回表。
"""
from __future__ import annotations

import argparse
import copy
import json
import re
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "code" / "agent"))
sys.path.insert(0, str(ROOT / "code"))

import _bootstrap  # noqa: E402,F401

import conf  # noqa: E402
import retrieval  # noqa: E402
import semantic  # noqa: E402
import vecindex  # noqa: E402

_RE_TABLE = re.compile(r"\b(?:from|join)\s+([a-zA-Z_][\w.]*)", re.I)


def load_cases() -> list[tuple[str, set[str]]]:
    d = json.loads((ROOT / "eval" / "baseline.json").read_text(encoding="utf-8"))
    wl = set(semantic.all_tables(conf.load_config()))
    out = []
    for c in d["cases"]:
        tabs = {t.split(".")[-1].lower() for t in _RE_TABLE.findall(c.get("sql") or "")}
        tabs &= wl
        if tabs:
            out.append((c["question"], tabs))
    return out


def score(fn, cases, k: int) -> tuple[float, float, float]:
    hit = 0
    mrr = 0.0
    t0 = time.time()
    for q, exp in cases:
        top = [x["table"] for x in fn(q)][:k]
        if exp & set(top):
            hit += 1
        for i, g in enumerate(top):
            if g in exp:
                mrr += 1.0 / (i + 1)
                break
    ms = (time.time() - t0) * 1000 / max(1, len(cases))
    return 100.0 * hit / len(cases), mrr / len(cases), ms


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--json", default="")
    ap.add_argument("--k", type=int, default=4)
    ap.add_argument("--rounds", type=int, default=3, help="耗时取几轮均值")
    args = ap.parse_args()

    cases = load_cases()
    base = conf.load_config()
    T = ("table",)

    def mk(backend=None, **kw):
        c = copy.deepcopy(base)
        c["retrieval"].update(kw)
        if backend:
            c["retrieval"]["backend"] = backend
        return c

    c_bm25 = mk("bm25")
    c_hyb = mk("hybrid")
    c_off = mk("hybrid", hybrid_enabled=False)
    c_neu = mk("hybrid", dense_backend="neural")
    c_neu_off = mk("hybrid", hybrid_enabled=False, dense_backend="neural")
    # 预热：首次跑要建索引（SVD 约 0.6s，神经后端要过一遍 ONNX，更久），
    # 不能算进单题耗时。
    for c in (c_bm25, c_hyb, c_neu):
        for q, _ in cases[:10]:
            retrieval.search_tables(q, c, top=args.k)

    rows = [
        ("BM25（现状）", lambda q: retrieval.search_tables(q, c_bm25, top=args.k)),
        ("稠密(LSA) 单路", lambda q: [{"table": c["doc"]["id"], "score": c["score"]}
                                  for c in vecindex.search_dense(q, c_off, top=args.k,
                                                                 kinds=T)]),
        ("BM25 + rerank", lambda q: [{"table": c["doc"]["id"], "score": c["rerank_score"]}
                                     for c in vecindex.rerank(
                                         q, vecindex.search_bm25(q, c_off, top=args.k * 3,
                                                                 kinds=T), c_off)[:args.k]]),
        ("Hybrid α=0.3", lambda q: retrieval.search_tables(q, c_hyb, top=args.k)),
        ("稠密(BGE神经) 单路", lambda q: [{"table": c["doc"]["id"], "score": c["score"]}
                                    for c in vecindex.search_dense(q, c_neu_off, top=args.k,
                                                                   kinds=T)]),
        ("Hybrid α=0.3 BGE", lambda q: retrieval.search_tables(q, c_neu, top=args.k)),
        ("vecindex 内 BM25 基线", lambda q: retrieval.search_tables(q, c_off, top=args.k)),
    ]

    results = []
    for name, fn in rows:
        r, m, _ = score(fn, cases, args.k)
        # 耗时单独跑 rounds 轮取均值，避免被上面的召回统计影响
        times = []
        for _ in range(args.rounds):
            t0 = time.time()
            for q, _ in cases:
                fn(q)
            times.append((time.time() - t0) * 1000 / len(cases))
        ms = sum(times) / len(times)
        results.append({"name": name, "recall": round(r, 1),
                        "mrr": round(m, 3), "ms": round(ms, 2)})

    print("检索层 A/B（%d 题，R@%d，ground truth = SQL 实际用表）\n" % (len(cases), args.k))
    print("注：vecindex 与 retrieval 各有一套文档表示（字段权重不同），所以")
    print("    「BM25（现状）」是主链路基线，「vecindex 内 BM25 基线」是同语料消融基线；")
    print("    看融合增益用后者，看接入收益用前者。\n")
    print("%-22s %8s %8s %10s" % ("方案", "R@%d" % args.k, "MRR", "ms/题"))
    for x in results:
        print("%-22s %7.1f%% %8.3f %10.2f" % (x["name"], x["recall"], x["mrr"], x["ms"]))
    b = results[0]
    print("\n相对 BM25：")
    for x in results[1:]:
        print("  %-18s R@%d %+5.1fpp（相对 %+.0f%%）  MRR %+.3f" % (
            x["name"], args.k, x["recall"] - b["recall"],
            (x["recall"] / b["recall"] - 1) * 100 if b["recall"] else 0,
            x["mrr"] - b["mrr"]))

    if args.json:
        Path(args.json).write_text(json.dumps({
            "generated_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
            "cases": len(cases), "k": args.k,
            "metric": "recall@k over tables referenced by baseline SQL",
            "results": results,
        }, ensure_ascii=False, indent=1), encoding="utf-8")
        print("\n已写 " + args.json)
    return 0


if __name__ == "__main__":
    sys.exit(main())
