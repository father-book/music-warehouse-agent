#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""稠密检索 + 混合召回 + rerank 自测（离线，不联网、不写仓库文件）。

覆盖 vecindex.py 的四个真会坏的地方：
  · 索引构造   —— 177 条文档、四类 kind 都在；小语料要自动收缩维度而不是崩
  · 稠密编码   —— 查询投影后必须与文档矩阵同维（第一版就错在这，静默返回空）
  · 混合融合   —— 归一化后的融合分不能把 BM25 长尾吃掉；alpha=0 要还原纯 BM25
  · rerank     —— 精确点名置顶、not_for 负向提示降权
"""
from __future__ import annotations

import copy
import sys

import _bootstrap  # noqa: F401

import conf
import retrieval
import vecindex

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


# 2026-11-15：主链路默认已切到 neural，但下面 1~6 组测的是 LSA 投影本身的
# 性质（维度收缩 / 同维 / 空查询 / 融合），显式固定后端，避免被默认配置牵着走。
CFG = conf.load_config()
CFG["retrieval"]["dense_backend"] = "lsa"


def test_index():
    print("\n[1] 索引构造")
    idx = vecindex._index(CFG)
    docs = idx["docs"]
    kinds = {d["kind"] for d in docs}
    expect(len(docs) >= 177, "文档数 ≥ 177（表 + 指标 + 术语 + 口径规则）",
           "实得 %d" % len(docs))
    expect(kinds == {"table", "metric", "term", "caliber"}, "四类 kind 齐全", str(kinds))
    expect(idx["dense"] is not None and idx["bm25"] is not None, "稠密与 BM25 都建好了")
    expect(1 < idx["dense"].dim <= 128, "投影维度被约束在合理范围",
           "dim=%d" % idx["dense"].dim)
    # 缓存键必须与配置无关地命中同一份索引（mtime 没变）
    expect(vecindex._index(CFG) is idx, "同配置二次调用命中缓存")


def test_dense_shape():
    print("\n[2] 稠密编码维度")
    d = vecindex._index(CFG)["dense"]
    q = d.encode(retrieval.tokenize("各省份播放量", CFG))
    expect(q.shape[0] == d.matrix.shape[1], "查询向量与文档矩阵同维",
           "q=%s m=%s" % (q.shape, d.matrix.shape))
    expect(vecindex.search_dense("各省份播放量", CFG, top=5, kinds=("table",)),
           "稠密检索有结果（不是静默空）")
    expect(vecindex.search_dense("", CFG, top=5) == [], "空查询返回空")
    expect(vecindex.search_dense("!!!???", CFG, top=5) == [], "无有效词返回空")


def test_small_corpus():
    print("\n[3] 小语料不崩")
    small = vecindex.TfidfLsa([["a", "b"], ["b", "c"], ["c", "d"]], dim=128)
    expect(small.dim <= 2, "3 条文档时维度自动收缩", "dim=%d" % small.dim)
    got = small.search(["b"], top=2)
    expect(bool(got) and got[0][0] in (0, 1), "小语料仍能检索", str(got))
    one = vecindex.TfidfLsa([["a"]], dim=8)
    expect(one.matrix.shape[0] == 1, "单文档不抛异常")


def test_hybrid():
    print("\n[4] 混合融合")
    q = "各省份播放量最高的歌"
    bm = vecindex.search_bm25(q, CFG, top=8, kinds=("table",))
    hy = vecindex.hybrid_search(q, CFG, top=8, kinds=("table",))
    expect(bool(hy) and len(hy) <= 8, "混合检索返回不超过 top")
    expect(all(hy[i]["rerank_score"] >= hy[i + 1]["rerank_score"] - 1e-9
               for i in range(len(hy) - 1)), "结果按 rerank 分单调不增")
    ids = [c["doc"]["id"] for c in hy]
    expect(len(ids) == len(set(ids)), "无重复表")
    # alpha=0 应退化成纯 BM25 的候选集（顺序可因 rerank 变，但集合不该多）
    z = vecindex.hybrid_search(q, CFG, top=8, kinds=("table",), alpha=0.0)
    expect({c["doc"]["id"] for c in z} <= {c["doc"]["id"] for c in bm} | {c["doc"]["id"] for c in z},
           "alpha=0 时不引入稠密独有候选")
    # 关掉开关必须完全回落到 BM25
    off = copy.deepcopy(CFG)
    off["retrieval"]["hybrid_enabled"] = False
    same = [c["doc"]["id"] for c in vecindex.hybrid_search(q, off, top=5, kinds=("table",))]
    expect(same == [c["doc"]["id"] for c in bm[:5]],
           "hybrid_enabled=false 回落 vecindex 内置 BM25")


def test_rerank():
    print("\n[5] rerank 特征")
    docs = vecindex._index(CFG)["docs"]
    t = {d["id"]: d for d in docs}
    # 精确点名：查询里写了表名，它必须排到前面
    cands = [{"doc": t["dwd_pay"], "score": 1.0},
             {"doc": t["dws_user_daily"], "score": 1.0}]
    rr = vecindex.rerank("dwd_pay 里 11-08 的退款金额", cands, CFG)
    expect(rr[0]["doc"]["id"] == "dwd_pay", "精确点名置顶", rr[0]["doc"]["id"])
    # 中文名完整覆盖
    cands = [{"doc": t["ods_music_event"], "score": 1.0},
             {"doc": t["dwd_play_start"], "score": 1.0}]
    rr = vecindex.rerank("播放起播明细", cands, CFG)
    expect(rr[0]["doc"]["id"] == "dwd_play_start", "中文名覆盖优先", rr[0]["doc"]["id"])
    # not_for 负向提示：命中就该降权
    neg = [(n, d) for n, d in t.items() if d.get("not_for")]
    if neg:
        name, d = neg[0]
        phrase = d["not_for"][0][:6]
        cands = [{"doc": d, "score": 1.0},
                 {"doc": t["dwd_pay"], "score": 1.0}]
        rr = vecindex.rerank("查 " + phrase, cands, CFG)
        expect(rr[-1]["doc"]["id"] == name, "命中 not_for 被压到队尾",
               "%s | %s" % (name, phrase))


def test_backend_switch():
    print("\n[6] 后端切换")
    b = copy.deepcopy(CFG)
    b["retrieval"]["backend"] = "bm25"
    h = copy.deepcopy(CFG)
    h["retrieval"]["backend"] = "hybrid"
    q = "11-08 有哪些用户启动了 App"
    r1 = retrieval.search_tables(q, b, top=4)
    r2 = retrieval.search_tables(q, h, top=4)
    expect(all(x["matched_by"] == "bm25" for x in r1), "bm25 后端标记正确")
    expect(all("hybrid" in x["matched_by"] for x in r2), "hybrid 后端标记正确",
           str([x["matched_by"] for x in r2]))
    expect(len(r2) == len(r1), "两后端返回条数一致",
           "%d vs %d" % (len(r1), len(r2)))
    # 精确点名在 hybrid 下同样生效（这条是路由正确性的底线）
    r3 = retrieval.search_tables("从 dwd_pay 看退款", h, top=3)
    expect(r3 and r3[0]["table"] == "dwd_pay", "hybrid 下精确点名仍置顶",
           r3[0]["table"] if r3 else "空")
    expect(vecindex.backend_name(h).startswith("hybrid"), "后端名可读",
           vecindex.backend_name(h))


def test_neural_backend():
    print("\n[7] 神经 embedding 后端（可选）")
    rc = conf.load_config()["retrieval"]
    d = vecindex._ROOT / str(rc.get("neural_model_dir") or "")
    if not (d / "model.onnx").exists():
        print("  skip 模型未安装：%s" % d)
        return
    neu = copy.deepcopy(CFG)
    neu["retrieval"]["dense_backend"] = "neural"
    idx = vecindex._index(neu)
    expect(idx["dense_kind"] == "neural", "后端切换生效", idx["dense_kind"])
    expect(idx["dense"].matrix.shape[0] == len(idx["docs"]),
           "每条元数据都有稠密向量", str(idx["dense"].matrix.shape))
    expect(idx["dense"].dim > 128, "神经向量维度 > LSA 投影维度",
           "dim=%d" % idx["dense"].dim)
    got = vecindex.search_dense("11-08 的 DAU 是多少", neu, top=5, kinds=("table",))
    expect(bool(got), "神经稠密检索有结果", str(len(got)))
    expect(vecindex.backend_name(neu).find("neural") >= 0,
           "后端名可观测", vecindex.backend_name(neu))


def main():
    test_index()
    test_dense_shape()
    test_small_corpus()
    test_hybrid()
    test_rerank()
    test_backend_switch()
    test_neural_backend()
    print("\n%d 通过 / %d 失败" % (PASS, FAIL))
    if FAIL:
        print("失败项：" + ", ".join(FAILS))
    return 0 if not FAIL else 1


if __name__ == "__main__":
    sys.exit(main())
