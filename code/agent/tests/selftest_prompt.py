#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Prompt 三件套自测（离线：模型调用全部打桩，不花 token）。

覆盖：
  · CoT        —— 开关只影响输出规范；plan 要求写在 sql 前面
  · Few-shot   —— 去自身泄漏、形态过滤、低分不注入、库坏了不崩
  · 自洽投票   —— 指纹归一化、多数胜出、平票取首个、n=1 与关闭等价
"""
from __future__ import annotations

import copy
import json
import sys

import _bootstrap  # noqa: F401

import conf
import node_generate_llm as gen
import promptlib
import verified

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


CFG = conf.load_config()
ST = {
    "question": "2026-11-08 的 DAU 是多少", "lane": "offline",
    "time_start": "2026-11-08", "time_end": "2026-11-08",
    "time_window": {"kind": "single", "label": "2026-11-08",
                    "sql_hint": "dt = '2026-11-08'"},
    "metrics": [], "tables": ["dwd_session"], "matched": [], "recalled": [],
    "dimensions": [],
}


def on(**kw):
    c = copy.deepcopy(CFG)
    c.setdefault("prompt", {})
    for k, v in kw.items():
        if k == "fewshot":
            c["prompt"].setdefault("fewshot", {}).update(v)
        elif k == "self_consistency":
            c["prompt"].setdefault("self_consistency", {}).update(v)
        else:
            c["prompt"][k] = v
    return c


def test_cot():
    print("\n[1] CoT 开关")
    off = promptlib.build_generate_messages(ST, CFG)[-1]["content"]
    onm = promptlib.build_generate_messages(ST, on(cot=True))[-1]["content"]
    expect('"plan"' not in off, "默认不含 plan 字段")
    expect('"plan"' in onm, "开启后含 plan 字段")
    expect(onm.index('"plan"') < onm.index('"sql"'), "plan 排在 sql 之前（先推理后落笔）")
    for k in ("metric", "source_table", "filters", "time_condition", "shape"):
        expect(k in onm, "plan 含 %s 字段" % k)
    expect(promptlib.estimate_tokens([{"content": onm}]) >
           promptlib.estimate_tokens([{"content": off}]), "CoT 增加了提示词长度")


def test_fewshot():
    print("\n[2] Few-shot")
    pool = verified.pool(CFG)
    expect(len(pool) > 50, "示例池有足够样本", "实得 %d" % len(pool))
    q = "2026-11-08 的 DAU 是多少"
    got = verified.retrieve_similar(q, CFG, top=2)
    expect(all(x["q"].strip() != q for x in got), "不把问题本身当示例（防泄漏）")
    expect(all(verified._shape_of(x) in ("", verified.guess_shape(q)) for x in got),
           "示例形态与本题一致")
    expect(verified.retrieve_similar("今天天气怎么样", CFG, top=2) == [],
           "不相关问题不给示例")
    expect(verified.retrieve_similar("", CFG, top=2) == [], "空问题返回空")
    # 阈值：把门槛抬到不可能达到的高度，必须一条都不给
    expect(verified.retrieve_similar(q, CFG, top=2, min_score=999) == [],
           "阈值可硬性关闭示例")
    # 渲染
    blk = verified.render_block(got)
    expect("<本次时间窗>" in blk, "示例里的日期已占位，防止模型硬编码")
    expect("形状参考" in blk, "示例块声明了用途（参考形状而非抄答案）")
    # 开关
    off = promptlib.build_generate_messages(ST, CFG)[-1]["content"]
    onm = promptlib.build_generate_messages(ST, on(fewshot={"enabled": True}))[-1]["content"]
    expect("已验证的相似问答" not in off, "默认不注入示例块")
    expect("已验证的相似问答" in onm, "开启后注入示例块")
    # 修复路径不注入
    rep = promptlib.build_generate_messages(
        ST, on(fewshot={"enabled": True}),
        findings=[{"rule": "R9-禁 SELECT *", "level": "REJECT", "message": "禁止 SELECT *"}],
        prev_sql="SELECT * FROM dwd_session")[-1]["content"]
    expect("已验证的相似问答" not in rep, "修复路径不注入示例（避免稀释 findings）")
    # 库不可用时不能崩
    bad = on(fewshot={"enabled": True})
    bad["prompt"]["fewshot"]["enabled"] = True
    real = verified.retrieve_similar
    try:
        verified.retrieve_similar = lambda *a, **k: (_ for _ in ()).throw(RuntimeError("boom"))
        blk2 = promptlib._fewshot_block(ST, bad)
        expect(blk2 == "", "库异常时静默降级为空块")
    finally:
        verified.retrieve_similar = real


def test_fingerprint():
    print("\n[3] SQL 指纹与投票")
    s1 = "SELECT COUNT(DISTINCT user_id) AS uv FROM dwd_session WHERE dt='2026-11-08';"
    s2 = "select count(distinct user_id) as uv\n  from dwd_session where dt = '2026-11-08'"
    s3 = "SELECT COUNT(DISTINCT device_id) FROM dwd_session WHERE dt='2026-11-08'"
    expect(gen.sql_fingerprint(s1) == gen.sql_fingerprint(s2),
           "大小写/空白/分号差异判为同一条")
    expect(gen.sql_fingerprint(s1) != gen.sql_fingerprint(s3),
           "不同口径（user_id vs device_id）不算同一条")
    expect(gen.sql_fingerprint("") == "", "空 SQL 指纹为空")
    expect(gen.sql_fingerprint("这不是 SQL %%") == gen.sql_fingerprint("这不是 SQL %%"),
           "解析失败时退化为字符串归一，不抛异常")
    # 投票
    a = s1
    b, c = s3, "SELECT 1"
    pick = gen._pick([(a, {}, {}), (b, {}, {}), (a, {}, {})])[0]
    expect(pick == a, "多数票胜出")
    tie = gen._pick([(b, {}, {}), (c, {}, {})])[0]
    expect(tie == b, "平票取第一个（即首轮生成）")
    expect(gen._pick([(c, {}, {})])[0] == c, "单候选原样返回")


def test_self_consistency():
    print("\n[4] 自洽采样（打桩，不烧 token）")
    calls = {"n": 0, "temps": []}
    sqls = ["SELECT 1", "SELECT 2", "SELECT 1"]

    def fake_chat(messages, cfg, **kw):
        calls["temps"].append(kw.get("temperature"))
        sql = sqls[min(calls["n"], len(sqls) - 1)]
        calls["n"] += 1
        return {"content": json.dumps({"sql": sql, "used_tables": [], "reason": "t"}),
                "usage": {"total_tokens": 10}, "model": "stub", "elapsed_ms": 1,
                "cost_cny": 0.0, "ledger_ok": True}

    real = gen.gateway.chat
    try:
        gen.gateway.chat = fake_chat
        # 关闭时只调一次
        calls["n"] = 0
        calls["temps"] = []
        sql, detail, meta = gen.generate_with_llm(ST, CFG)
        expect(calls["n"] == 1, "默认只调 1 次模型", "实得 %d" % calls["n"])
        expect(sql == "SELECT 1", "取回第一条")
        expect("samples" not in meta, "关闭时 meta 不记采样信息")

        # 开启时调 n 次且抬温度
        calls["n"] = 0
        calls["temps"] = []
        sql, detail, meta = gen.generate_with_llm(
            ST, on(self_consistency={"enabled": True, "samples": 3, "temperature": 0.7}))
        expect(calls["n"] == 3, "开启后调 3 次", "实得 %d" % calls["n"])
        expect(all(t == 0.7 for t in calls["temps"]), "多采样时温度被抬高",
               str(calls["temps"]))
        expect(sql == "SELECT 1", "多数票选中 SELECT 1", sql)
        expect(meta.get("samples") == 3 and meta.get("distinct") == 2,
               "meta 记下样本数与去重数", str(meta))
        expect(meta.get("agreement") == 0.67, "meta 记下一致率", str(meta.get("agreement")))

        # 修复路径不采样（成本保护）
        calls["n"] = 0
        gen.generate_with_llm(ST, on(self_consistency={"enabled": True, "samples": 3}),
                              findings=[{"rule": "R9", "level": "REJECT",
                                         "message": "禁止 SELECT *"}],
                              prev_sql="SELECT * FROM dwd_session")
        expect(calls["n"] == 1, "修复路径不启用自洽（只调 1 次）", "实得 %d" % calls["n"])
    finally:
        gen.gateway.chat = real


def test_config_defaults():
    print("\n[5] 默认开关必须全关")
    pc = CFG.get("prompt") or {}
    expect(pc.get("cot") is False, "cot 默认关（不改基线）")
    expect((pc.get("fewshot") or {}).get("enabled") is False, "fewshot 默认关")
    expect((pc.get("self_consistency") or {}).get("enabled") is False,
           "self_consistency 默认关")
    # 2026-11-15 修：这里原来断言 backend == "bm25"，但主链路当天已切到 hybrid
    # （115 题检索 A/B：BM25 28.7% → Hybrid(LSA) 47.8% → Hybrid(BGE) 49.6%，
    # MRR 0.193→0.370）。断言没跟着改，于是长期红灯——**红灯被忽略比没有红灯更糟**。
    # 本意是"防止有人悄悄换后端把基线搅乱"，所以改成两条：
    # ① 必须是已知值；② 必须是当前定版的那个（换回去要显式改这里并说明理由）。
    be = (CFG.get("retrieval") or {}).get("backend")
    expect(be in ("bm25", "legacy", "hybrid"),
           "检索后端必须是已知值（实得 %r）" % be)
    expect(be == "hybrid",
           "主链路检索后端为 hybrid（2026-11-15 定版，MRR 0.193→0.370）")


def main():
    test_cot()
    test_fewshot()
    test_fingerprint()
    test_self_consistency()
    test_config_defaults()
    print("\n%d 通过 / %d 失败" % (PASS, FAIL))
    if FAIL:
        print("失败项：" + ", ".join(FAILS))
    return 0 if not FAIL else 1


if __name__ == "__main__":
    sys.exit(main())
