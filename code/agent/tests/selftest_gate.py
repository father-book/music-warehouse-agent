#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""执行闸门 + 行数新鲜度自测（不联网、不写仓库真实文件）。

覆盖两类新能力：
  · etl_gate  —— 金额类识别、令牌绑定（换 SQL / 换记录号即失效）、二次确认结论
  · etl_rowcount —— tsv/yaml 双写、三方对照的四种状态、幂等
"""
from __future__ import annotations

import os
import shutil
import sys
import tempfile

import _bootstrap  # noqa: F401

import etl_gate
import etl_rowcount as rc

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


REV_SQL = ("CREATE TABLE music_dw.dev_rev_daily AS SELECT dt, "
           "SUM(CASE WHEN event_name IN ('vip_subscribe','order_pay') THEN amount "
           "WHEN event_name='order_refund' THEN -amount ELSE 0 END) AS net_rev "
           "FROM dwd_pay WHERE dt <= '2026-11-08' GROUP BY dt")
CNT_SQL = ("CREATE TABLE music_dw.dev_pv_daily AS SELECT dt, COUNT(*) AS pv "
           "FROM dwd_page_view WHERE dt <= '2026-11-08' GROUP BY dt")
DIM_SQL = ("CREATE TABLE music_dw.dev_province_cnt AS SELECT province, COUNT(*) AS cnt "
           "FROM dwd_session WHERE dt <= '2026-11-08' GROUP BY province")


def test_gate():
    print("\n[1] 金额类识别")
    g = etl_gate.gate(1, REV_SQL)
    codes = [r["code"] for r in g["risks"]]
    expect(g["need_confirm"] and "M1" in codes, "金额聚合被识别（M1）")
    expect("M2" in codes, "收入域表被识别（M2）")
    expect("M3" in codes, "输出列像金额被识别（M3）")
    expect(etl_gate.gate(1, CNT_SQL)["need_confirm"] is False, "计数类不拦")
    expect(etl_gate.gate(1, DIM_SQL)["need_confirm"] is False, "维度计数不拦")

    print("\n[1b] 目标表自身不算源表")
    tgt_sql = ("CREATE TABLE music_dw.dev_revenue_only AS SELECT dt, COUNT(*) AS c "
               "FROM dwd_session WHERE dt <= '2026-11-08' GROUP BY dt")
    g1b = etl_gate.gate(1, tgt_sql)
    expect(not any("dev_revenue_only" in r["detail"] for r in g1b["risks"]),
           "表名带 revenue 的目标表不会被当成源表（M2 不误报）")

    print("\n[2] 已有口径告警进确认页（M4）")
    g2 = etl_gate.gate(1, CNT_SQL, {"caliber": [
        {"rule": "R25-口径-收入事件覆盖", "message": "缺 vip_subscribe"}]})
    expect(g2["need_confirm"] and any(r["code"] == "M4" for r in g2["risks"]),
           "生成时报过的黄牌带进确认页（即使本条 SQL 不像金额）")
    expect("vip_subscribe" in g2["risks"][0]["detail"], "告警原文被保留")

    print("\n[3] 令牌绑定")
    t1 = etl_gate.token(1, REV_SQL)
    expect(etl_gate.verify(1, REV_SQL, t1), "同记录同 SQL 令牌有效")
    expect(not etl_gate.verify(1, CNT_SQL, t1), "换 SQL 令牌失效")
    expect(not etl_gate.verify(2, REV_SQL, t1), "换记录号令牌失效")
    expect(not etl_gate.verify(1, REV_SQL, ""), "空令牌不放行")
    expect(not etl_gate.verify(1, REV_SQL, "deadbeefdeadbeef"), "伪造令牌不放行")
    expect(etl_gate.verify(1, REV_SQL, "  %s  " % t1), "首尾空白容错")

    print("\n[4] 别名/列名判定的边界")
    expect(etl_gate.is_money_name("amount"), "amount 是金额")
    expect(etl_gate.is_money_name("net_rev"), "net_rev 是金额")
    expect(etl_gate.is_money_name("pay_amt"), "pay_amt 是金额")
    expect(etl_gate.is_money_name("销售额"), "中文销售额是金额")
    expect(not etl_gate.is_money_name("review_cnt"), "review_cnt 不误判为 rev")
    expect(not etl_gate.is_money_name("uv"), "uv 不是金额")
    expect(not etl_gate.is_money_name("province"), "省份不是金额")


def _tmp_env():
    d = tempfile.mkdtemp(prefix="etlrc_")
    tsv = os.path.join(d, "rowcount.tsv")
    yml = os.path.join(d, "tables.yaml")
    with open(tsv, "w", encoding="utf-8") as f:
        f.write("dwd_pay\t36578\ndev_a\t10\ndev_b\t50\n")
    with open(yml, "w", encoding="utf-8") as f:
        f.write("tables:\n"
                "  dwd_pay:\n    layer: dwd\n    rows: 36578\n"
                "    caveats:\n      - \"x\"\n"
                "  dev_a:\n    layer: dev\n    grain: 天\n    rows: 10\n"
                "  dev_b:\n    layer: dev\n    rows: 50\n")
    return d, tsv, yml


def test_rowcount():
    print("\n[5] 三方对照状态")
    d, tsv, yml = _tmp_env()
    old_tsv, old_yaml = rc.ROWCOUNT_TSV, rc.TABLES_YAML
    rc.ROWCOUNT_TSV, rc.TABLES_YAML = tsv, yml
    try:
        expect(rc.parse_tsv()["dev_a"] == 10, "tsv 解析")
        expect(rc._yaml_rows()["dev_b"] == 50, "yaml rows 解析")
        p = {x["table"]: x for x in rc.plan({"dev_a": 10, "dev_b": 60, "dev_c": 7})}
        expect(p["dev_a"]["status"] == "一致", "一致")
        expect(p["dev_b"]["status"] == "快照过期" and p["dev_b"]["delta"] == 10,
               "快照过期（delta=+10）")
        expect(p["dev_c"]["status"] == "未登记", "只有快照没有语义层条目 → 未登记")
        p2 = {x["table"]: x for x in rc.plan({"dwd_pay": 36578})}
        expect(p2["dwd_pay"]["status"] == "一致", "非 dev 表也能判定")

        print("\n[6] 回写：tsv 覆盖 + yaml 只改 rows 行")
        r = rc.upsert({"dev_a": 12, "dev_b": 60}, regen=False)
        expect(r["ok"] and set(r["yaml"]) == {"dev_a", "dev_b"}, "两张表都回写")
        expect(rc.parse_tsv()["dev_a"] == 12, "tsv 已更新")
        expect(rc._yaml_rows()["dev_a"] == 12, "yaml rows 已更新")
        body = open(yml, encoding="utf-8").read()
        expect("caveats:" in body and "- \"x\"" in body, "yaml 其他内容原样保留")
        expect(body.count("rows: 12") == 1, "只改目标行，未重复插入")

        print("\n[7] 幂等 + 新表追加")
        rc.upsert({"dev_a": 12}, regen=False)
        expect(rc.parse_tsv()["dev_a"] == 12, "同值重写不产生重复行")
        expect(open(tsv, encoding="utf-8").read().count("dev_a\t") == 1,
               "tsv 无重复表名")
        rc.upsert({"dev_c": 7}, regen=False)
        expect(rc.parse_tsv()["dev_c"] == 7, "未登记新表也能追加快照")
        body = open(tsv, encoding="utf-8").read()
        expect(body.splitlines()[-1] == "dev_c\t7", "新表追加在末尾")
    finally:
        rc.ROWCOUNT_TSV, rc.TABLES_YAML = old_tsv, old_yaml
        shutil.rmtree(d, ignore_errors=True)


if __name__ == "__main__":
    test_gate()
    test_rowcount()
    print("\n%d passed, %d failed" % (PASS, FAIL))
    if FAILS:
        print("失败项：" + "、".join(FAILS))
    sys.exit(1 if FAIL else 0)
