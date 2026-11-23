#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""实时链路历史快照接入自测（不造数据、不写仓库文件）。

覆盖四层，缺一层就是"接进来了但答不出来 / 答出来是错的"：
  1. 路由分流（realtimelane + node_route）：纯当前时刻仍拒答，历史快照可查
  2. 提示词（promptlib）：实时 lane 换铁律、换时间块，不能照抄离线规则
  3. 护栏（guard.py R17/R26）：白名单、时间窗、禁混查
  4. 语义层（tables.json / sem_check）：rt 层元数据齐全且与定义同步
"""
from __future__ import annotations

import json
import os
import subprocess
import sys

import _bootstrap  # noqa: F401

import node_route
import promptlib
import realtimelane
from conf import load_config

from _bootstrap import HERE, ROOT  # noqa: E402
HERE = str(HERE); ROOT = str(ROOT)
GUARD = os.path.join(ROOT, "agent", "guards", "guard.py")
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


def guard(sql: str) -> list[str]:
    p = subprocess.run([sys.executable, GUARD, "--mode", "query", "--json", sql],
                       cwd=os.path.dirname(GUARD), capture_output=True,
                       text=True, encoding="utf-8", errors="replace")
    try:
        fs = json.loads(p.stdout or "[]")
        return [f["rule"] for f in (fs if isinstance(fs, list) else fs.get("findings", []))]
    except Exception:
        return ["<解析失败>"]


def main():
    cfg = load_config()
    rt = realtimelane.spec(cfg)

    print("\n[1] 元数据（realtime.yaml → tables.json）")
    expect(len(rt.get("tables") or {}) == 9, "9 张 rt 表已合并进 tables.json")
    expect(rt.get("status") == "down", "链路状态仍诚实标为 down")
    expect("22:17" in str(rt.get("data_until")), "data_until 指向真实最后产出时刻")
    expect(bool(rt.get("freshness_note")), "新鲜度声明存在")
    idx = rt.get("tables") or {}
    expect(all(idx[t]["time_col"] for t in idx), "每张表都有时间列")
    with open(os.path.join(ROOT, "agent", "tables.json"), encoding="utf-8") as f:
        meta = json.load(f)
    expect(all(meta["tables"][t]["layer"] == "rt" for t in idx), "rt 表 layer 正确")
    expect(all(meta["tables"][t]["catalog"] == "internal" for t in idx),
           "rt 表 catalog 是 internal")

    print("\n[2] 路由分流")
    cases = [
        ("今天 DAU 多少", "offline", True),
        ("现在实时错误率是多少", "offline", True),
        ("此刻的播放延迟", "offline", True),
        ("2026-11-08 分钟级错误率峰值是多少", "realtime", False),
        ("实时链路的质量怎么样", "realtime", False),
        ("11月8日 Flink 作业最慢的一次是多久", "realtime", False),
        ("11-08 DAU 是多少", "offline", False),
        ("各省份的活跃用户数", "offline", False),
    ]
    for q, lane, refused in cases:
        r = node_route.route({"question": q}, cfg)
        expect(r["lane"] == lane and bool(r["unsupported"]) == refused,
               "%s → lane=%s / 拒答=%s" % (q, lane, refused),
               "实际 %s / %s" % (r["lane"], bool(r["unsupported"])))
    r_rt = node_route.route({"question": "实时链路的质量怎么样"}, cfg)
    expect("不代表当前时刻" in r_rt["freshness"], "答案携带新鲜度声明")
    expect(all(t in idx for t in r_rt["tables"]), "实时 lane 候选表全是 rt 表",
           str(r_rt["tables"]))
    r_off = node_route.route({"question": "各省份的活跃用户数"}, cfg)
    expect(not any(t in idx for t in r_off["tables"]), "离线 lane 候选表不含 rt 表")
    expect("历史" in node_route.route({"question": "现在实时错误率是多少"}, cfg)["unsupported"],
           "拒答时告诉用户能查历史快照")

    print("\n[3] 提示词按 lane 换规则")
    off = promptlib.build_generate_messages(
        {"question": "11-08 DAU 是多少", "lane": "offline", "tables": [],
         "time_window": {}}, cfg)
    rtmsg = promptlib.build_generate_messages(
        {"question": "实时链路质量如何", "lane": "realtime", "tables": [],
         "time_window": {}}, cfg)
    expect("裸写" in off[0]["content"], "离线铁律要求裸写表名")
    expect("三段式" in rtmsg[0]["content"], "实时铁律要求三段式")
    expect("internal.music_dw" in rtmsg[0]["content"], "实时铁律写明数据面")
    tb = promptlib._time_block({"lane": "realtime", "time_window": {}}, cfg)
    expect("停止产出" in tb and "R26" in tb, "实时时间块声明快照与 R26")

    print("\n[4] 护栏 R17 / R26")
    expect(not guard("SELECT stat_minute, error_rate FROM internal.music_dw.ops_data_quality "
                     "WHERE stat_minute >= '2026-11-08 00:00:00' LIMIT 10"),
           "白名单实时表 + 时间窗 → 放行")
    expect(any(x.startswith("R26") for x in guard(
        "SELECT stat_minute FROM internal.music_dw.ops_data_quality LIMIT 10")),
        "SELECT 里出现时间列但 WHERE 无过滤 → 仍拦")
    expect(any(x.startswith("R26") for x in guard(
        "SELECT stat_minute, SUM(errors_1m) FROM internal.music_dw.ops_data_quality "
        "GROUP BY stat_minute LIMIT 10")), "GROUP BY 不算过滤 → 仍拦")
    expect(any(x.startswith("R17") for x in guard(
        "SELECT COUNT(*) FROM internal.music_dw.dwd_play_start WHERE dt='2026-11-08'")),
        "白名单外的 internal 同名表 → 仍拦")
    expect(any(x.startswith("R26") and "混查" in x for x in guard(
        "SELECT a.stat_minute FROM internal.music_dw.ops_data_quality a "
        "JOIN ads_daily_overview b ON a.stat_minute=b.dt "
        "WHERE a.stat_minute >= '2026-11-08' LIMIT 10")), "实时/离线混查 → 拦")
    expect(not guard("SELECT dt, dau FROM ads_daily_overview "
                     "WHERE dt='2026-11-08' LIMIT 10"), "离线问数不回归")
    expect(not guard("SELECT window_start, song_id FROM internal.music_dw.ads_song_rank_10min "
                     "WHERE window_start >= '2026-11-08' LIMIT 5"),
           "10 分钟表用 window_start → 放行")

    print("\n[5] 语义层一致性")
    p = subprocess.run([sys.executable, os.path.join(ROOT, "agent", "semantic", "sem_check.py")],
                       capture_output=True, text=True, encoding="utf-8", errors="replace")
    expect(p.returncode == 0, "sem_check 通过（含第 15 项实时层自检）")

    print("\n%d passed, %d failed" % (PASS, FAIL))
    if FAILS:
        print("失败项：" + "、".join(FAILS))
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(main())
