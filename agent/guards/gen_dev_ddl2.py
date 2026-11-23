#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""从正式表批量导出 dev 空壳表 DDL（一次 Hive 会话，不复用逐表启动）。

与 gen_dev_ddl.py 的区别只有一点：把所有 SHOW CREATE TABLE 塞进**一个**
hive 会话里跑。逐表启动实测 28 张要 10 分钟以上（每次都要连 metastore、
初始化 Hive on Spark），一次会话 30 秒。

绝不执行生成的 SQL，只写文件。
  music_dw.            -> music_dev.
  /warehouse/music_dw/ -> /warehouse/music_dev/
"""
import argparse
import io
import json
import os
import re
import subprocess

HIVE = "/opt/module/hive/bin/hive"
DB = "music_dw"
DST = "music_dev"


def hive(sql):
    p = subprocess.run([HIVE, "-S", "-e", sql], stdout=subprocess.PIPE,
                       stderr=subprocess.DEVNULL, universal_newlines=True, timeout=600)
    return p.stdout or ""


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="/root/offline/dev_ddl")
    a = ap.parse_args()
    os.makedirs(a.out, exist_ok=True)

    raw = hive("SHOW TABLES IN %s;" % DB)
    HEADER = {"tab_name", "table_name"}
    tables = [l.strip() for l in raw.splitlines()
              if re.match(r"^[a-z_][a-z0-9_]*$", l.strip())
              and l.strip() not in HEADER]
    print("== %d 张表 ==" % len(tables))

    # 一个会话里跑完所有 SHOW CREATE，用 ===表名=== 分隔
    stmts = []
    for t in tables:
        stmts.append("SELECT '===%s===';" % t)
        stmts.append("SHOW CREATE TABLE %s.`%s`;" % (DB, t))
    # 必须用 -e 而不是 -f：实测 `hive -S -f file` 对 SHOW CREATE TABLE 不输出结果
    # （describe 还能出，show create 直接空），而 -e 正常。32 条拼起来约 1.2KB，命令行装得下。
    out = subprocess.run([HIVE, "-S",
                          "--hiveconf", "hive.cli.errors.ignore=true",
                          "-e", "\n".join(stmts)], stdout=subprocess.PIPE,
                         stderr=subprocess.DEVNULL, universal_newlines=True,
                         timeout=900).stdout or ""

    blocks, missing = [], []
    cur, buf = None, []
    for line in out.splitlines():
        m = re.match(r"^===([a-z_0-9]+)===$", line.strip())
        if m:
            if cur and buf:
                blocks.append((cur, "\n".join(buf)))
            cur, buf = m.group(1), []
            continue
        if cur is None:
            continue
        if line.strip().upper().startswith("CREATE"):
            buf = [line]
        elif buf:
            buf.append(line)

    if cur and buf:
        blocks.append((cur, "\n".join(buf)))

    sqls = []
    for name, body in blocks:
        body = body.strip().rstrip(";").strip()
        if "CREATE" not in body.upper():
            missing.append(name)
            continue
        body = body.replace("%s." % DB, "%s." % DST)
        body = body.replace("`%s`" % DB, "`%s`" % DST)
        body = body.replace("/warehouse/%s/" % DB, "/warehouse/%s/" % DST)
        sqls.append("-- %s -> %s\n%s;\n" % (name, DST, body))

    header = ("-- dev 空壳表（Hive）由 gen_dev_ddl2.py 生成\n"
              "-- 只有结构，不含数据、不接调度；LOCATION 已改到 /warehouse/music_dev/\n"
              "CREATE DATABASE IF NOT EXISTS %s;\nUSE %s;\n\n" % (DST, DST))
    sql = header + "\n".join(sqls)
    path = os.path.join(a.out, "hive_dev_ddl.sql")
    io.open(path, "w", encoding="utf-8").write(sql)

    leaked = [l for l in sql.splitlines() if "/warehouse/%s/" % DB in l]
    prod_ref = re.findall(r"\b%s\." % DB, sql)
    print("== 写出 %s：%d 张表 ==" % (path, len(sqls)))
    print("   未解析: %d %s" % (len(missing), missing[:5]))
    print("   LOCATION 残留正式路径: %d 行" % len(leaked))
    print("   库名残留 %s.: %d 处" % (DB, len(prod_ref)))
    if leaked or prod_ref:
        print("   !! 必须修到 0 才能执行")

    man = {"generated_by": "gen_dev_ddl2.py", "hive_tables": len(sqls),
           "missing": missing, "leaked_location": len(leaked),
           "prod_ref": len(prod_ref)}
    io.open(os.path.join(a.out, "manifest.json"), "w", encoding="utf-8").write(
        json.dumps(man, ensure_ascii=False, indent=1))
    for n, _ in blocks[:6]:
        print("   -", n)
    print("   ...")


if __name__ == "__main__":
    main()
