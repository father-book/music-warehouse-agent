#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""通用：去掉 Hive 脚本里「无条件 DROP TABLE」的陷阱。

背景（2026-11-08 实际踩到）：
  11_dwd_domain_tables.sql 每张表写的是
      DROP TABLE IF EXISTS music_dw.X;
      CREATE EXTERNAL TABLE IF NOT EXISTS music_dw.X (...);
      INSERT OVERWRITE TABLE music_dw.X PARTITION (dt) SELECT ... WHERE dt BETWEEN ...
  用 START_DT_OVERRIDE 只补几天时，DROP 把整张表连历史分区一起删掉，
  重建后只剩本次写入的几天 —— 10 张域表从 35 天掉到 4 天。
  HDFS 文件没丢（外部表 DROP 只删元数据），但分区元数据需要 MSCK REPAIR 才能回来。

修法：注释掉 DROP。CREATE 本来就是 IF NOT EXISTS，表存在时跳过，
      INSERT OVERWRITE 只覆盖 SELECT 出来的那几个分区，历史分区保留。

用法：
  python3 fix_ddl_drop.py <sql文件> [保留的表述,逗号分隔]

真需要重建表结构（加字段等）时，手动 DROP 一次再跑原脚本。
"""
import io
import os
import re
import sys

if len(sys.argv) < 2:
    print(__doc__)
    sys.exit(2)

P = sys.argv[1]
KEEP = set(x.strip() for x in (sys.argv[2].split(",") if len(sys.argv) > 2 else []) if x.strip())
BAK = P + ".bak_ddl"

if not os.path.exists(P):
    print("文件不存在:", P)
    sys.exit(2)

s = io.open(P, encoding="utf-8").read()
orig = s

if not re.search(r"^DROP TABLE IF EXISTS", s, re.M):
    print("没有需要处理的 DROP:", P)
    sys.exit(0)

drop_pat = re.compile(r"^DROP TABLE IF EXISTS (music_dw\.\w+);$", re.M)


def repl(m):
    name = m.group(1)
    short = name.split(".")[-1]
    if short in KEEP:
        return m.group(0)
    return ("-- [11-08 修正] 不再无条件 DROP：只补少数几天时会把历史分区一起清掉\n"
            "-- " + m.group(0))


s, n = drop_pat.subn(repl, s)

# 补齐 IF NOT EXISTS（幂等，已存在则不重复加）
s = re.sub(r"CREATE EXTERNAL TABLE (?!IF NOT EXISTS)", "CREATE EXTERNAL TABLE IF NOT EXISTS ", s)

io.open(BAK, "w", encoding="utf-8").write(orig)
io.open(P, "w", encoding="utf-8").write(s)

print("文件:", P)
print("备份:", BAK)
print("处理 DROP:", n, "处，保留:", sorted(KEEP) if KEEP else "无")
left = re.findall(r"^DROP TABLE[^;]*;", s, re.M)
print("剩余生效的 DROP:", left if left else "无（全部注释）")
