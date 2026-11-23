"""costgate 自测：这条 SQL 到底要读多少，判得准不准。

为什么这段必须测
----------------
成本闸的两个错误方向代价不对称：

  · 判松了：`dt <> '1900-01-01'` 这种伪裁剪溜过去，扫 499 万行 / 202MB，
    慢、贵，但**看起来完全正常**——没人会怀疑一条"带了 dt"的 SQL。
  · 判紧了：正常的"近 30 天"也被拦，用户会认为系统坏了。

所以用例成对写：该拦的必须拦，**不该拦的必须放**，后者同样重要。
第 3~6 段全是"不该拦"的用例（30 天窗口、非分区维表、小表全扫、未知表）。

测法上有两条讲究：

1. **判定逻辑喂假计划，不连库**。判据是纯函数 judge(nodes, cfg)，
   假节点能精确构造边界（35/35、30/35、21M 行），比真表更可靠。
2. **真库只验"计划解析对不对"**。最后一段跑真实 EXPLAIN 两条，
   断言解析出的分区数与行数落在合理区间——防止 Doris 改版本后格式变了，
   解析器静默返回空节点（那会让闸永远放行，且所有自测仍然全绿）。

用法：python code/agent/tests/selftest_costgate.py
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import _bootstrap  # noqa: F401
import costgate as C
from conf import load_config

PASS = FAIL = 0
FAILED: list[str] = []

CFG = load_config()

# 真实 EXPLAIN 片段（2026-11-10 从 Doris 163:9030 抓的，原样保留格式）
EXPLAIN_FULL = """PLAN FRAGMENT 1

  PARTITION: RANDOM

  1:VAGGREGATE (update serialize)(153)
  |  output: partial_count(*)[#28]
  |  cardinality=1
  |
  0:VHIVE_SCAN_NODE(172)
     table: ods_music_event
     inputSplitNum=35, totalFileSize=212061396, scanRanges=35
     partition=35/35
     cardinality=23641, numNodes=1
"""

EXPLAIN_ONE = """PLAN FRAGMENT 1

  0:VHIVE_SCAN_NODE(132)
     table: ods_music_event
     inputSplitNum=1, totalFileSize=6065125, scanRanges=1
     partition=1/35
     cardinality=23641, numNodes=1
"""


def ck(name: str, cond: bool, extra: str = "") -> None:
    global PASS, FAIL
    if cond:
        PASS += 1
        print("  ✓ %s" % name)
    else:
        FAIL += 1
        FAILED.append(name)
        print("  ✗ %s  %s" % (name, extra))


def node(table: str, scanned: int, total: int, rows: int | None = None,
         nbytes: int = 0) -> dict:
    """造一个扫节点。rows 不给时按分区占比折算——和 _rows_of 同一套口径，
    否则测的就不是线上那个判据了。"""
    if rows is None:
        tot = C.sizes().get(table)
        rows = int(tot * scanned / total) if (tot is not None and total) else tot
    return {"type": "VHIVE_SCAN_NODE", "table": table, "splits": scanned,
            "bytes": nbytes, "ranges": scanned,
            "part_scanned": scanned, "part_total": total,
            "rows": rows,
            "full_scan": bool(total and scanned >= total)}


def main() -> int:
    print("=" * 70)
    print("成本闸自测（costgate）")
    print("=" * 70)

    # ---------------------------------------------------------------- [1] 解析
    print("\n[1] 计划解析：Doris 的三种写法都要认得")
    n = C.parse_explain(EXPLAIN_FULL)
    ck("全扫解析出 1 个扫节点", len(n) == 1, str(len(n)))
    if n:
        ck("表名识别", n[0]["table"] == "ods_music_event", n[0].get("table", ""))
        ck("分区 35/35", (n[0]["part_scanned"], n[0]["part_total"]) == (35, 35),
           "%s/%s" % (n[0]["part_scanned"], n[0]["part_total"]))
        ck("字节数识别（同一行三个指标都要取到）",
           n[0]["bytes"] == 212061396, str(n[0]["bytes"]))
        ck("按分区占比折行数 ≈ 全表 499 万",
           abs((n[0]["rows"] or 0) - 4994396) <= 100, str(n[0]["rows"]))
        ck("判为全分区扫描", n[0]["full_scan"] is True)

    m = C.parse_explain(EXPLAIN_ONE)
    ck("单分区解析", m and (m[0]["part_scanned"], m[0]["part_total"]) == (1, 35))
    ck("单分区不算全扫", m and m[0]["full_scan"] is False)
    ck("单分区行数 ≈ 499 万/35", m and abs((m[0]["rows"] or 0) - 142697) <= 100,
       str(m[0]["rows"] if m else None))

    # ---------------------------------------------------------------- [2] 该拦
    print("\n[2] 该拦的：全分区 + 大表")
    v = C.judge([node("ods_music_event", 35, 35)], CFG)
    ck("499 万全扫 → blocked", v["status"] == "blocked", v["status"])
    ck("理由里给出分区数与行数",
       any("35 个分区" in r and "4,994,396" in r for r in v["reasons"]),
       str(v["reasons"])[:120])
    ck("给出总行数", v["rows"] == 4994396, str(v["rows"]))

    v = C.judge([node("dwd_impression", 35, 35)], CFG)
    ck("223 万全扫 → blocked（大表门槛 100 万）", v["status"] == "blocked", v["status"])

    # ---------------------------------------------------------------- [3] 不该拦
    print("\n[3] 不该拦的：业务需要的时间范围")
    v = C.judge([node("ods_music_event", 30, 35)], CFG)
    ck("近 30 天（30/35）→ ok", v["status"] == "ok", v["status"])
    ck("30 天预计行数 ≈ 428 万但放行", 4_000_000 < v["rows"] < 4_400_000,
       str(v["rows"]))

    v = C.judge([node("ods_music_event", 1, 35)], CFG)
    ck("单日（1/35）→ ok", v["status"] == "ok", v["status"])

    v = C.judge([node("dwd_play_start", 35, 35)], CFG)
    ck("小表全扫（44 万 < 100 万门槛）→ ok", v["status"] == "ok", v["status"])

    v = C.judge([node("dim_user", 0, 0, rows=1150000)], CFG)
    ck("非分区维表全读（JOIN 必需）→ ok", v["status"] == "ok", v["status"])

    # ---------------------------------------------------------------- [4] 绝对上限
    print("\n[4] 绝对上限：行数与字节数兜底（防笛卡尔积放大）")
    v = C.judge([node("dwd_click", 35, 35, rows=25_000_000)], CFG)
    ck("2500 万行 → blocked", v["status"] == "blocked", v["status"])
    ck("理由是绝对上限", any("绝对上限" in r for r in v["reasons"]), str(v["reasons"]))

    v = C.judge([node("ads_daily_overview", 35, 35, rows=35,
                      nbytes=300 * 1024 * 1024)], CFG)
    ck("300MB → blocked", v["status"] == "blocked", v["status"])
    ck("理由是字节数", any("MB" in r for r in v["reasons"]), str(v["reasons"]))

    # ---------------------------------------------------------------- [5] 缺元数据
    print("\n[5] 缺元数据：估不准时不误杀，但要留痕")
    v = C.judge([node("unknown_big_table", 35, 35, rows=None)], CFG)
    ck("行数未知 → 不拦", v["status"] == "ok", v["status"])
    ck("未知表被记下来", v["unknown_tables"] == ["unknown_big_table"],
       str(v["unknown_tables"]))

    # ---------------------------------------------------------------- [6] 计划拿不到
    print("\n[6] EXPLAIN 失败：无 dt 才拒，有 dt 降级放行")
    real_estimate = C.estimate

    C.estimate = lambda sql, cfg=None: {"nodes": [], "error": "模拟：计划不可用",
                                        "elapsed_ms": 1}
    try:
        v = C.check("SELECT COUNT(*) FROM ods_music_event", CFG)
        ck("无 dt + 大表 → blocked", v["status"] == "blocked", v["status"])
        ck("说明是保守拒绝", "保守" in (v.get("message") or ""), v.get("message", "")[:60])

        v = C.check("SELECT COUNT(*) FROM ods_music_event WHERE dt='2026-11-08'", CFG)
        ck("有 dt → unknown（不拦）", v["status"] == "unknown", v["status"])
        ck("unknown 带原因", bool(v.get("reasons")), str(v.get("reasons")))

        v = C.check("SELECT 1 FROM dim_song", CFG)
        ck("小表 + 无 dt → unknown（不误杀）", v["status"] == "unknown", v["status"])
    finally:
        C.estimate = real_estimate

    # ---------------------------------------------------------------- [7] 开关
    print("\n[7] 关闭成本闸：只允许显式配置")
    off = dict(CFG)
    off["cost_gate"] = {**(CFG.get("cost_gate") or {}), "enabled": False}
    v = C.check("SELECT COUNT(*) FROM ods_music_event", off)
    ck("enabled=false → off，连 EXPLAIN 都不跑", v["status"] == "off", v["status"])

    # ---------------------------------------------------------------- [8] finding
    print("\n[8] 转成护栏 findings：走同一条「打回/拒答」通道")
    f = C.as_finding(C.judge([node("ods_music_event", 35, 35)], CFG))
    ck("规则号是 R24", f["rule"] == "R24-成本闸", f["rule"])
    ck("等级 REJECT", f["level"] == "REJECT", f["level"])
    ck("不给 fix（不是机械替换能修的）", not f.get("fix"))
    ck("message 是给人看的说明", "成本闸" in f["message"] and "dt" in f["message"])

    # ---------------------------------------------------------------- [9] 真库
    print("\n[9] 真库：EXPLAIN 端到端（格式变了这段会红）")
    v = C.check("SELECT COUNT(*) AS c FROM ods_music_event WHERE dt <> '1900-01-01'", CFG)
    ck("伪裁剪（dt <>）→ blocked", v["status"] == "blocked", v["status"])
    ck("真库估到 499 万行", 4_900_000 < (v["rows"] or 0) < 5_100_000, str(v["rows"]))
    ck("真库估到 ~202MB", 200 < (v["bytes"] or 0) / 1048576 < 205,
       "%.1fMB" % ((v["bytes"] or 0) / 1048576))
    ck("EXPLAIN 本身够快（< 2s）", v["elapsed_ms"] < 2000, "%dms" % v["elapsed_ms"])

    v = C.check("SELECT COUNT(*) AS c FROM ods_music_event WHERE dt = '2026-11-08'", CFG)
    ck("正常单日 → ok", v["status"] == "ok", v["status"])
    ck("单日估到 ~14 万行", 130_000 < (v["rows"] or 0) < 160_000, str(v["rows"]))

    v = C.check("SELECT COUNT(*) AS c FROM dwd_play_start WHERE dt BETWEEN "
                "'2026-11-02' AND '2026-11-08'", CFG)
    ck("7 天窗口 → ok", v["status"] == "ok", v["status"])
    ck("7 天估到 ~9 万行", 80_000 < (v["rows"] or 0) < 100_000, str(v["rows"]))

    print("\n" + "=" * 70)
    print("%d 通过 / %d 失败" % (PASS, FAIL))
    if FAILED:
        print("失败项：" + "、".join(FAILED))
    print("=" * 70)
    return 1 if FAIL else 0


if __name__ == "__main__":
    raise SystemExit(main())
