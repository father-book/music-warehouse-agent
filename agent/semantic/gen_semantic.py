#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""生成语义层元数据 tables.json —— Agent 的「我有哪些表、哪些字段」的唯一来源。

为什么要从 Hive 抓而不是手写 28 张表的字段：
  手写一定会有表和实际不一致的那天（重造一次就变），而 Agent 的规则 4
  「字段必须存在」正是靠这份清单拦截幻觉字段。清单一旦漂移，规则 4 就会
  把正确字段判成幻觉、或者放过真幻觉。

一次 hive 会话跑完全部 describe：28 次独立启动要 5 分钟，一次会话只要 20 秒。

用法（161）：
  python3 gen_semantic.py --out /root/agent
"""
import argparse
import io
import json
import os
import re
import subprocess

HIVE = "/opt/module/hive/bin/hive"
DB = "music_dw"

# 层级判断只看表名前缀，避免维护第二份清单
def layer_of(t):
    for p in ("ads_", "dws_", "dwd_", "dim_"):
        if t.startswith(p):
            return p[:-1]
    return "other"


def fetch_schema():
    tables = subprocess.run(
        [HIVE, "-S", "-e", "SHOW TABLES IN %s;" % DB],
        stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
        universal_newlines=True, timeout=180).stdout
    names = [l.strip() for l in tables.splitlines()
             if l.strip() and re.match(r"^[a-z_][a-z0-9_]*$", l.strip())]
    print("== 发现 %d 张表 ==" % len(names))

    lines = []
    for t in names:
        lines.append("SELECT '===%s===';" % t)
        lines.append("DESCRIBE %s.%s;" % (DB, t))
    sql = "\n".join(lines)
    path = "/tmp/_desc_all.sql"
    io.open(path, "w", encoding="utf-8").write(sql)

    out = subprocess.run([HIVE, "-S", "-f", path],
                         stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
                         universal_newlines=True, timeout=900).stdout
    return names, out


def parse(out):
    """按 ===表名=== 切段，每段是 describe 的 tab 分隔输出。"""
    schema = {}
    cur = None
    for raw in out.splitlines():
        line = raw.rstrip("\n")
        m = re.match(r"^===([a-z_0-9]+)===$", line.strip())
        if m:
            cur = m.group(1)
            schema[cur] = {"columns": [], "partitioned_by": [], "types": {}}
            continue
        if cur is None or not line.strip():
            continue
        parts = line.split("\t")
        if len(parts) < 2:
            continue
        col = parts[0].strip().strip("`")
        typ = parts[1].strip()
        if not col or col.startswith("#") or col in ("col_name",):
            continue
        # describe 的分区行形如 "dt  string  ...  " 且前面有空行 + "# Partition Information"
        schema[cur]["types"][col] = typ
        schema[cur]["columns"].append(col)
    return schema


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="/root/agent")
    a = ap.parse_args()
    os.makedirs(os.path.join(a.out, "semantic"), exist_ok=True)

    names, out = fetch_schema()
    schema = parse(out)
    print("== 解析到 %d 张表的字段 ==" % len(schema))

    tables = {}
    for t in names:
        info = schema.get(t)
        if not info or not info["columns"]:
            print("   !! %s 没解析到字段，跳过" % t)
            continue
        # describe 会把分区列也列出来（在 Partition Information 之后），
        # 这里用「p_ 前缀不存在、且列名是 dt」的经验规则识别
        parted = ["dt"] if "dt" in info["columns"] else []
        tables[t] = {
            "layer": layer_of(t),
            "partitioned_by": parted,
            "columns": [c for c in info["columns"]],
            "types": info["types"],
        }

    # 比率/均值型字段：跨期不能直接 AVG，必须 SUM(分子)/SUM(分母)
    weighted = []
    for t, info in tables.items():
        for c in info["columns"]:
            if c.endswith(("_rate", "_dod")) or c in (
                    "ctr", "arpu", "avg_progress", "avg_play_per_user", "ad_ctr"):
                weighted.append(c)
    weighted = sorted(set(weighted))

    meta = {"db": DB, "tables": tables, "weighted_columns": weighted,
            "count": len(tables)}
    p = os.path.join(a.out, "tables.json")
    io.open(p, "w", encoding="utf-8").write(json.dumps(meta, ensure_ascii=False, indent=1))
    print("== 写出 %s：%d 张表，%d 个比率字段 ==" % (p, len(tables), len(weighted)))
    for lay in ("dim", "dwd", "dws", "ads"):
        sub = [t for t in tables if tables[t]["layer"] == lay]
        print("   %-4s %2d 张：%s" % (lay, len(sub), ", ".join(sorted(sub)[:4]) + ("..." if len(sub) > 4 else "")))


if __name__ == "__main__":
    main()
