#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""修 11_dwd_domain_tables.sql 的 DDL 陷阱。

问题：每张表都是 `DROP TABLE IF EXISTS` + `CREATE EXTERNAL TABLE` + `INSERT OVERWRITE`。
      用 START_DT_OVERRIDE 只补几天时，DROP 会把整张表连历史分区一起清掉，
      重建后只剩本次写入的几天。2026-11-08 实际踩到：10 张域表从 35 天掉到 4 天。

修法：10 张域表改成 `CREATE EXTERNAL TABLE IF NOT EXISTS`，去掉无条件 DROP。
      这样首次建表正常，重复跑只 INSERT OVERWRITE 目标分区，历史分区保留。
      dwd_collect / dwd_share 的 DROP **保留** —— 那是有意合并进 dwd_interaction 的收口动作。

若确实需要重建表结构（加字段等），手动 DROP 后再跑本脚本。
"""
import io
import re
import shutil
import sys

P = "/root/offline/11_dwd_domain_tables.sql"
BAK = P + ".bak20261108b"

# 这两张是有意删除的，不动
KEEP = {"dwd_collect", "dwd_share"}

s = io.open(P, encoding="utf-8").read()
orig = s

drop_pat = re.compile(r"^DROP TABLE IF EXISTS music_dw\.(dwd_\w+);$", re.M)


def repl(m):
    t = m.group(1)
    if t in KEEP:
        return m.group(0)
    return ("-- [11-08 修正] 不再无条件 DROP：只补少数几天时会把历史分区一起清掉\n"
            "-- " + m.group(0))


s, n_drop = drop_pat.subn(repl, s)
s, n_create = re.subn(r"CREATE EXTERNAL TABLE music_dw\.dwd_",
                      "CREATE EXTERNAL TABLE IF NOT EXISTS music_dw.dwd_", s)

io.open(BAK, "w", encoding="utf-8").write(orig)
io.open(P, "w", encoding="utf-8").write(s)

print("备份:", BAK)
print("DROP 处理:", n_drop, "处（其中保留 %d 处 collect/share）" % len(KEEP))
print("CREATE 加 IF NOT EXISTS:", n_create, "处")
print("剩余未注释的 DROP:")
for m in re.finditer(r"^DROP TABLE.*$", s, re.M):
    print("   ", m.group(0))
