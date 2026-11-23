"""成本闸：在 SQL 真正执行**之前**，用 EXPLAIN 估它要扫多少。

为什么需要它，而不是让护栏 R6 挡住就完事：

  R6 是**列级存在性**检查——"WHERE 里有没有 dt 这个列"。它挡得住"完全忘了写 dt"，
  挡不住"写了 dt 但没裁掉分区"：

      WHERE dt <> '1900-01-01'          -- 有 dt，35 个分区一个不落
      WHERE a.dt = d.dt                 -- 有 dt，但那是 JOIN 条件不是过滤
      WHERE substr(dt,1,7) = '2026-11'  -- 有 dt，函数包住后裁剪不了

  这三种写法 R6 全放行，而实际扫描量是 499 万行 / 212MB。这类 SQL 会跑得很慢、
  很贵，而且**看起来完全正常**——没人会去怀疑一条"带了 dt 条件"的 SQL。

  所以判据从"SQL 长什么样"换成"数据库说它要读多少"。EXPLAIN 是唯一说真话的地方。

三个设计决定：

1. **判"全分区扫描"，不判"行数超阈值"**。
   用户问"近 30 天"时扫 30/35 个分区是**业务需要**，不该拦；
   而 35/35 说明分区裁剪根本没生效，那是实现问题。所以主判据是
   `partition=35/35` 且表足够大；另设一个绝对上限兜住笛卡尔积这类放大。

2. **EXPLAIN 拿不到时只降级，不放行也不误杀**。
   计划取不到（版本差异/权限）时无法证明安全：若 SQL 里连 `dt` 字样都没有，
   仍按"无裁剪"拒；否则放行但标 `unknown`，让 trace 里留一笔"这次没估成"。

3. **给的是人话，不是计划树**。
   拒答要说清"哪张表、要扫几个分区、约多少行、上限多少、该怎么改"。
   EXPLAIN 原文几百行，用户看不懂，也没必要看。

Doris 的 EXPLAIN 里 `cardinality` 不可信（Hive 外表没有统计信息，
全表和单分区都给 23641）。可信的是 `partition=a/b`、`inputSplitNum`、
`totalFileSize`——这三个是优化器按分区元数据算出来的。
"""
from __future__ import annotations

import _bootstrap  # noqa: F401

import json
import re
import time
from pathlib import Path

from conf import load_config
from env import PROJECT_ROOT

# 与 tables.json 放在一起（project/agent/）：都是离线元数据，一起生成、一起版本化。
_SIZES_PATH = PROJECT_ROOT / "agent" / "table_sizes.json"

# 扫节点起始行：`0:VHIVE_SCAN_NODE(132)` / `0:OlapScanNode`
_SCAN_START = re.compile(r"^\s*\d+:([A-Za-z_]*SCAN[A-Za-z_]*)(?:\((\d+)\))?\s*$", re.I)
# 任意节点起始行，用来切块（扫节点块到下一个节点为止）
_NODE_START = re.compile(r"^\s*\d+:[A-Za-z_]+")
_TABLE = re.compile(r"^\s*table\s*:\s*(\S+)\s*$", re.I)
_SPLIT = re.compile(r"inputSplitNum\s*=\s*(\d+)")
_BYTES = re.compile(r"totalFileSize\s*=\s*(\d+)")
_RANGES = re.compile(r"scanRanges\s*=\s*(\d+)")
_PART = re.compile(r"partitions?\s*=\s*(\d+)\s*/\s*(\d+)")

_DEFAULTS = {
    "enabled": True,
    "min_large_rows": 1_000_000,     # 大表门槛：全分区扫描超过这个行数才判
    "max_scan_rows": 20_000_000,     # 绝对上限：任何情况预计超过就拒
    "max_scan_bytes": 268_435_456,   # 256MB
}

_sizes_cache: dict | None = None


def sizes() -> dict:
    """{表名: 总行数}。缺文件不是错误——只是估不准，判据退化为"只看分区"。"""
    global _sizes_cache
    if _sizes_cache is None:
        try:
            with open(_SIZES_PATH, encoding="utf-8") as f:
                _sizes_cache = {k.lower(): int(v)
                                for k, v in (json.load(f).get("rows") or {}).items()}
        except Exception:
            _sizes_cache = {}
    return _sizes_cache


def reload_sizes() -> dict:
    global _sizes_cache
    _sizes_cache = None
    return sizes()


def _conf(cfg: dict) -> dict:
    c = dict(_DEFAULTS)
    c.update({k: v for k, v in (cfg.get("cost_gate") or {}).items() if v is not None})
    return c


# ------------------------------------------------------------------ 解析

def parse_explain(text: str) -> list[dict]:
    """从 EXPLAIN 文本里抽出每个扫节点：表、分区、字节、行数估计。

    不解析整棵计划树——只关心"读多少"，那只需要扫节点。
    """
    lines = (text or "").splitlines()
    nodes: list[dict] = []
    cur: dict | None = None
    in_scan = False

    for line in lines:
        m = _SCAN_START.match(line)
        if m:
            if cur:
                nodes.append(cur)
            cur = {"type": m.group(1), "table": "", "splits": 0,
                   "bytes": 0, "ranges": 0, "part_scanned": 0, "part_total": 0}
            in_scan = True
            continue
        if _NODE_START.match(line) and not _SCAN_START.match(line):
            # 进入下一个（非扫）节点：扫节点块结束
            if cur:
                nodes.append(cur)
                cur = None
            in_scan = False
            continue
        if not in_scan or cur is None:
            continue
        t = _TABLE.match(line)
        if t and not cur["table"]:
            cur["table"] = t.group(1).strip().lower().strip("`")
            continue
        # 三个指标挤在同一行（`inputSplitNum=35, totalFileSize=212061396, scanRanges=35`），
        # 所以这里不能 break——第一版 break 了，bytes 一直是 0，报错文案里就成了 "0.0 MB"。
        for rx, key in ((_SPLIT, "splits"), (_BYTES, "bytes"), (_RANGES, "ranges")):
            mm = rx.search(line)
            if mm:
                cur[key] = int(mm.group(1))
        mp = _PART.search(line)
        if mp:
            cur["part_scanned"], cur["part_total"] = int(mp.group(1)), int(mp.group(2))

    if cur:
        nodes.append(cur)

    for n in nodes:
        n["rows"] = _rows_of(n)
        n["full_scan"] = bool(n["part_total"] and n["part_scanned"] >= n["part_total"])
    return nodes


def _rows_of(n: dict) -> int | None:
    """按分区占比把表总行数折成"这次要读多少行"。"""
    total = sizes().get(n["table"])
    if total is None:
        return None
    if n["part_total"]:
        return int(total * n["part_scanned"] / n["part_total"])
    return int(total)


# ------------------------------------------------------------------ 判定

def judge(nodes: list[dict], cfg: dict) -> dict:
    """纯函数：给定扫节点，判过不过。自测直接喂假节点，不用连库。"""
    c = _conf(cfg)
    reasons: list[str] = []
    rows = 0
    nbytes = 0
    unknown = []

    for n in nodes:
        if n.get("rows") is not None:
            rows += n["rows"]
        else:
            unknown.append(n["table"] or "?")
        nbytes += n.get("bytes") or 0

        if n["full_scan"] and n["rows"] is not None and n["rows"] >= c["min_large_rows"]:
            reasons.append(
                "%s 要扫全部 %d 个分区、约 %s 行（%s MB）——dt 条件没有起到裁剪作用"
                % (n["table"], n["part_total"], _fmt(n["rows"]),
                   _fmt(n["bytes"] / 1048576, 1)))

    if rows > c["max_scan_rows"]:
        reasons.append("预计扫描 %s 行，超过绝对上限 %s 行"
                       % (_fmt(rows), _fmt(c["max_scan_rows"])))
    if nbytes > c["max_scan_bytes"]:
        reasons.append("预计读 %s MB，超过上限 %s MB"
                       % (_fmt(nbytes / 1048576, 1), _fmt(c["max_scan_bytes"] / 1048576, 0)))

    status = "blocked" if reasons else "ok"
    return {
        "status": status,
        "rows": rows,
        "bytes": nbytes,
        "nodes": nodes,
        "reasons": reasons,
        "unknown_tables": unknown,
        "limits": {"min_large_rows": c["min_large_rows"],
                   "max_scan_rows": c["max_scan_rows"],
                   "max_scan_bytes": c["max_scan_bytes"]},
    }


def _fmt(n, digits: int = 0) -> str:
    try:
        if digits:
            return ("%." + str(digits) + "f") % float(n)
        return "{:,}".format(int(round(float(n))))
    except (TypeError, ValueError):
        return str(n)


# ------------------------------------------------------------------ 对外

def estimate(sql: str, cfg: dict | None = None) -> dict:
    """跑 EXPLAIN 拿扫节点。失败不抛，返回 error——调用方决定怎么处置。"""
    import db
    cfg = cfg or load_config()
    t0 = time.time()
    try:
        res = db.run_sql("EXPLAIN " + sql, cfg, max_rows=500)
    except Exception as e:
        return {"nodes": [], "error": "%s: %s" % (type(e).__name__, str(e)[:200]),
                "elapsed_ms": int((time.time() - t0) * 1000)}
    if res.get("error"):
        return {"nodes": [], "error": str(res["error"])[:300],
                "elapsed_ms": int((time.time() - t0) * 1000)}
    text = "\n".join(str(r[0]) if r else "" for r in (res.get("rows") or []))
    return {"nodes": parse_explain(text), "error": "",
            "elapsed_ms": int((time.time() - t0) * 1000)}


def check(sql: str, cfg: dict | None = None) -> dict:
    """完整判据：估 + 判。返回 dict，status ∈ blocked / ok / unknown / off。

    `unknown` 的含义是"没估成"：不拦（否则 EXPLAIN 出问题会瘫掉所有问答），
    但要把这次没估成写进 trace——静默放行才是真问题。
    """
    cfg = cfg or load_config()
    c = _conf(cfg)
    if not c["enabled"]:
        return {"status": "off", "reasons": [], "message": "", "nodes": [],
                "rows": 0, "bytes": 0, "elapsed_ms": 0}

    est = estimate(sql, cfg)
    if est["error"]:
        # 拿不到计划：只有"连 dt 都没提"这种明显情况才拒，其余降级放行。
        if _looks_unbounded(sql) and _touches_large(sql):
            return {"status": "blocked",
                    "reasons": ["执行计划不可用（%s），且 SQL 未按 dt 过滤大表"
                                % est["error"][:120]],
                    "message": "成本闸未能估算扫描量，同时 SQL 没有按 dt 过滤大表，"
                               "按保守策略拒绝执行。请加上时间范围（如 dt = '2026-11-08'）后重试。",
                    "nodes": [], "rows": 0, "bytes": 0,
                    "elapsed_ms": est["elapsed_ms"], "unknown": True}
        return {"status": "unknown", "reasons": ["执行计划不可用：%s" % est["error"][:160]],
                "message": "", "nodes": [], "rows": 0, "bytes": 0,
                "elapsed_ms": est["elapsed_ms"]}

    v = judge(est["nodes"], cfg)
    v["elapsed_ms"] = est["elapsed_ms"]
    v["message"] = _message(v) if v["status"] == "blocked" else ""
    return v


def _message(v: dict) -> str:
    c = v["limits"]
    head = "这条 SQL 的执行计划要读的数据量超过成本闸上限，已拦下未执行。"
    body = "\n".join("· " + r for r in v["reasons"])
    tail = ("成本上限：单表全分区扫描不超过 %s 行；单次查询不超过 %s 行 / %s MB。\n"
            "改法：给每张大表加上**能裁剪分区**的时间条件，例如 `dt = '2026-11-08'`、"
            "`dt BETWEEN '2026-11-01' AND '2026-11-08'`；注意 `dt <> ...`、"
            "`substr(dt,1,7) = ...` 这类写法不会裁剪分区。"
            % (_fmt(c["min_large_rows"]), _fmt(c["max_scan_rows"]),
               _fmt(c["max_scan_bytes"] / 1048576, 0)))
    return head + "\n\n" + body + "\n\n" + tail


def _looks_unbounded(sql: str) -> bool:
    return not re.search(r"\bdt\b", sql or "", re.I)


def _touches_large(sql: str) -> bool:
    s = (sql or "").lower()
    return any(t in s for t, n in sizes().items() if n >= _DEFAULTS["min_large_rows"])


def as_finding(v: dict) -> dict:
    """转成护栏同构的 finding，复用"打回重写 / 拒答"的既有通道。"""
    msg = v.get("message") or (_message(v) if v.get("status") == "blocked"
                               else "") or "成本闸拒绝执行"
    return {
        "level": "REJECT",
        "rule": "R24-成本闸",
        "message": msg,
        "hint": "按 dt 裁剪分区后再查",
        # 不给 fix：这不是机械替换能修的，需要模型理解"怎么改才裁得动分区"
        "fix": {},
    }


# ------------------------------------------------------------------ 维护

def refresh_sizes(cfg: dict | None = None) -> dict:
    """重测各表行数。表结构变了、数据加量后跑一次。"""
    import db
    cfg = cfg or load_config()
    with open(PROJECT_ROOT / "agent" / "tables.json", encoding="utf-8") as f:
        tables = list(json.load(f)["tables"])
    out = {}
    for t in tables:
        try:
            r = db.run_sql("SELECT count(*) AS c FROM " + t, cfg, max_rows=2)
            out[t] = int(r["rows"][0][0])
        except Exception:
            out[t] = None
    payload = {"_comment": "各表总行数，成本闸用它把 EXPLAIN 的分区占比折成行数。"
                           "由 costgate.py --refresh 重测。",
               "measured_at": time.strftime("%Y-%m-%d"),
               "source": "Doris: SELECT count(*) 逐表实测",
               "rows": {k: v for k, v in out.items() if v is not None}}
    with open(_SIZES_PATH, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
        f.write("\n")
    reload_sizes()
    return out


if __name__ == "__main__":
    import sys

    cfg = load_config()
    if "--refresh" in sys.argv:
        print(json.dumps(refresh_sizes(cfg), ensure_ascii=False, indent=1))
        raise SystemExit(0)

    sql = (sys.argv[1] if len(sys.argv) > 1
           else "SELECT count(*) FROM ods_music_event WHERE dt <> '1900-01-01'")
    v = check(sql, cfg)
    print("status:", v["status"], "| rows:", _fmt(v["rows"]), "| bytes:",
          _fmt((v["bytes"] or 0) / 1048576, 1), "MB", "| %dms" % v["elapsed_ms"])
    for n in v["nodes"]:
        print("  节点 %-18s 分区 %s/%s  行 %s  字节 %s" % (
            n["table"] or "?", n["part_scanned"], n["part_total"],
            _fmt(n["rows"] or 0), _fmt(n["bytes"])))
    if v["message"]:
        print("\n" + v["message"])
