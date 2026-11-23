#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""从本地建表 SQL 生成 dev 空壳表 DDL。

为什么不用集群的 `SHOW CREATE TABLE`：
  ① Hive CLI 输出的 COMMENT 中文全变成 `????`（locale 问题），dev 表会失去字段说明；
  ② 末尾会带上 `transient_lastDdlTime` 等自动属性，其中一项的值没加引号，
     直接执行报 `ParseException line 30:0 missing EOF at '2026'`；
  ③ 32 张表要跑一个 1.2KB 的 -e 命令，还得处理"表头被当表名导致整句中止"的坑。
而 sql/offline/*.sql 就是这些表的权威建表语句（与集群同源、UTF-8 中文完好），
本地切出来直接替换库名与路径即可。

只做三件事：
  1. 切出每条 CREATE EXTERNAL TABLE 完整语句（括号/引号感知，不是简单 split(';')）
  2. music_dw. -> music_dev.，/warehouse/music_dw/ -> /warehouse/music_dev/
  3. 校验：不能残留正式库名或正式 LOCATION，否则拒绝写出
"""
import io
import os
import re
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
SQL_DIR = os.path.join(HERE, "..", "..", "sql", "offline")
OUT = os.path.join(HERE, "..", "..", "sql", "agent", "dev_ddl_hive.sql")

SRC = "music_dw"
DST = "music_dev"
EXCLUDE = {"dwd_collect", "dwd_share"}


def cut_statements(text):
    """从一堆 SQL 文本里切出所有 CREATE EXTERNAL TABLE 语句（含结尾分号）。"""
    out = []
    for m in re.finditer(r"CREATE\s+EXTERNAL\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?"
                         r"([a-zA-Z_0-9]+)\.([a-zA-Z_0-9]+)", text, re.I):
        tname = m.group(2)
        start = m.start()
        i, depth, quote = m.end(), 0, None
        while i < len(text):
            ch = text[i]
            if quote:
                if ch == quote:
                    quote = None
            elif ch in "'\"`":
                quote = ch
            elif ch == "(":
                depth += 1
            elif ch == ")":
                depth -= 1
            elif ch == ";" and depth == 0:
                break
            i += 1
        stmt = text[start:i].strip()
        if stmt.upper().startswith("CREATE EXTERNAL TABLE"):
            out.append((tname, stmt))
    return out


def main():
    found = {}
    for fn in sorted(os.listdir(SQL_DIR)):
        if not fn.endswith(".sql"):
            continue
        raw = io.open(os.path.join(SQL_DIR, fn), encoding="utf-8").read()
        # 剥掉整行注释，避免把注释里的示例 DDL 当真
        text = "\n".join(l for l in raw.splitlines()
                         if not l.strip().startswith("--"))
        for tname, stmt in cut_statements(text):
            if tname in EXCLUDE:
                continue
            # 同一张表在多个文件里出现：取更长的那条（通常是 later 版本，字段更全）
            if tname not in found or len(stmt) > len(found[tname]):
                found[tname] = stmt

    blocks = []
    for tname in sorted(found):
        body = found[tname].rstrip(";").strip()
        body = body.replace("%s." % SRC, "%s." % DST)
        body = body.replace("`%s`" % SRC, "`%s`" % DST)
        body = body.replace("/warehouse/%s/" % SRC, "/warehouse/%s/" % DST)
        blocks.append("-- %s -> %s\n%s;\n" % (tname, DST, body))

    sql = ("-- dev 空壳表（Hive）：只建结构，不灌数据、不接调度\n"
           "-- 生成器：agent/guards/gen_dev_ddl3.py（本地源码切出，中文注释完好）\n"
           "-- LOCATION 已全部指向 /warehouse/music_dev/\n"
           "CREATE DATABASE IF NOT EXISTS %s;\nUSE %s;\n\n" % (DST, DST)
           + "\n".join(blocks))

    # 硬校验：残留任何正式库引用就拒绝写出，避免 dev 表指到生产目录
    bad_loc = [l.strip() for l in sql.splitlines() if "/warehouse/%s/" % SRC in l]
    bad_lib = re.findall(r"\b%s\.[a-z_]" % SRC, sql)
    if bad_loc or bad_lib:
        print("!! 校验失败：LOCATION 残留 %d 行，库名残留 %d 处"
              % (len(bad_loc), len(bad_lib)))
        for l in bad_loc[:3]:
            print("   ", l[:120])
        sys.exit(1)

    io.open(OUT, "w", encoding="utf-8").write(sql)
    print("== %d 张表 -> %s ==" % (len(blocks), OUT))
    for lay in ("dim", "dwd", "dws", "ads"):
        n = sum(1 for t in found if t.startswith(lay + "_"))
        print("   %-4s %2d 张" % (lay, n))
    print("   LOCATION/库名残留：0（校验通过）")


if __name__ == "__main__":
    main()
