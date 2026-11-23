#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""从正式表导出 dev 空壳表的 DDL —— 只生成 SQL 文本，绝不执行。

背景见 docs/26 第二节「坑 3b」：
  Hive 外部表 DDL 里带 LOCATION 'hdfs://.../warehouse/music_dw/xxx'。
  只替换库名、不改 LOCATION，dev 表会指向正式数据目录 —— 即便 dev 表本身是空的，
  它也会读到正式数据，Agent 一旦写入就污染生产。

本脚本同时替换：
  music_dw.        -> music_dev.
  /warehouse/music_dw/ -> /warehouse/music_dev/
  music_offline.   -> music_dev_offline.   （Doris 侧单独一个库，避免表名撞车）

用法（在 161 上跑，那里既有 hive CLI 也有 pymysql）：
  python3 gen_dev_ddl.py --out /root/offline/dev_ddl

产出：
  <out>/hive_dev_ddl.sql     -- 人工审阅后：hive -f
  <out>/doris_dev_ddl.sql    -- 人工审阅后：mysql -h163 -P9030 < ...
  <out>/manifest.json        -- 表名清单与冲突检查

注意：
  - 脚本不建库、不建表、不改正式库。CREATE DATABASE 语句写在 SQL 文件开头，由人执行。
  - dev 表是空壳：不 INSERT、不接调度、不灌数据（用户 11-08 22:00 决定）。
  - 生成后必须 grep 一遍确认没有残留 /warehouse/music_dw/ 路径。
"""
import argparse
import io
import json
import os
import subprocess
import sys

HIVE = "/opt/module/hive/bin/hive"
DORIS = dict(host="192.168.128.163", port=9030, user="root",
             password="", charset="utf8mb4")

SRC_LIBS = ["music_dw", "music_offline"]
DST_LIB = {"music_dw": "music_dev", "music_offline": "music_dev_offline"}


def hive_query(sql):
    """调本机 hive CLI；返回 stdout 文本。"""
    # universal_newlines 而非 text=True：161 上是 Python 3.6，text= 是 3.7 才加的
    p = subprocess.run([HIVE, "-S", "-e", sql], stdout=subprocess.PIPE,
                       stderr=subprocess.DEVNULL, universal_newlines=True, timeout=300)
    return p.stdout or ""


def doris_conn():
    import pymysql
    return pymysql.connect(**DORIS)


def gen_hive(out):
    print("== Hive：导出 music_dw 全部表 ==")
    tables = [ln.strip() for ln in hive_query("SHOW TABLES IN music_dw;").splitlines()
              if ln.strip() and not ln.lower().startswith(("warn", "info", "session"))]
    print("   表数:", len(tables))

    blocks, skipped = [], []
    for t in tables:
        ddl = hive_query("SHOW CREATE TABLE music_dw.`%s`;" % t)
        if "CREATE" not in ddl.upper():
            skipped.append(t)
            continue
        # 只保留从 CREATE 开始的正文，丢掉 CLI 的日志/提示行
        idx = ddl.upper().find("CREATE")
        body = ddl[idx:].strip().rstrip(";").strip()
        body = body.replace("music_dw.", "music_dev.")
        body = body.replace("/warehouse/music_dw/", "/warehouse/music_dev/")
        # 库名出现在表名里（SHOW CREATE TABLE 通常带库前缀）
        body = body.replace("`music_dw`", "`music_dev`")
        blocks.append("-- %s\n%s;\n" % (t, body))

    sql = ("-- dev 空壳表（Hive）由 gen_dev_ddl.py 生成，只建结构不灌数据\n"
           "CREATE DATABASE IF NOT EXISTS music_dev;\n"
           "USE music_dev;\n\n" + "\n".join(blocks))
    path = os.path.join(out, "hive_dev_ddl.sql")
    io.open(path, "w", encoding="utf-8").write(sql)
    print("   写出:", path, "共", len(blocks), "张；跳过", len(skipped), skipped[:5])

    leaked = [ln for ln in sql.splitlines() if "/warehouse/music_dw/" in ln]
    if leaked:
        print("   !! 仍有 %d 行指向正式 LOCATION，请检查：" % len(leaked))
        for ln in leaked[:3]:
            print("      ", ln.strip()[:120])
    else:
        print("   LOCATION 检查通过：无残留正式路径")
    return {"count": len(blocks), "skipped": skipped, "tables": tables,
            "leaked_location": len(leaked)}


def gen_doris(out):
    print("== Doris：导出 music_dw + music_offline ==")
    c = doris_conn()
    cur = c.cursor()
    cur.execute("SELECT TABLE_SCHEMA, TABLE_NAME FROM information_schema.tables "
                "WHERE TABLE_SCHEMA IN ('music_dw','music_offline') ORDER BY 1,2")
    rows = cur.fetchall()
    print("   表数:", len(rows))

    # 冲突检查：两个源库若有同名表，映射到同一个 dev 库会互相覆盖
    by_name = {}
    for lib, t in rows:
        by_name.setdefault(t, []).append(lib)
    conflicts = {t: v for t, v in by_name.items() if len(v) > 1}

    blocks, failed = [], []
    for lib, t in rows:
        try:
            cur.execute("SHOW CREATE TABLE `%s`.`%s`" % (lib, t))
            ddl = cur.fetchone()[1]
        except Exception as e:
            failed.append((lib, t, str(e)[:80]))
            continue
        body = ddl.strip().rstrip(";").strip()
        body = body.replace("`%s`" % lib, "`%s`" % DST_LIB[lib])
        body = body.replace("%s." % lib, "%s." % DST_LIB[lib])
        blocks.append("-- %s.%s -> %s\n%s;\n" % (lib, t, DST_LIB[lib], body))
    c.close()

    sql = ("-- dev 空壳表（Doris）由 gen_dev_ddl.py 生成，只建结构不灌数据\n"
           "CREATE DATABASE IF NOT EXISTS music_dev;\n"
           "CREATE DATABASE IF NOT EXISTS music_dev_offline;\n\n" + "\n".join(blocks))
    path = os.path.join(out, "doris_dev_ddl.sql")
    io.open(path, "w", encoding="utf-8").write(sql)
    print("   写出:", path, "共", len(blocks), "张；失败", len(failed), failed[:3])
    if conflicts:
        print("   同名表分属两个源库（已映射到不同 dev 库，不冲突）:", conflicts)
    return {"count": len(blocks), "failed": failed, "conflicts": conflicts}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="/root/offline/dev_ddl")
    ap.add_argument("--only", choices=["hive", "doris", "all"], default="all")
    a = ap.parse_args()
    os.makedirs(a.out, exist_ok=True)

    manifest = {"generated_by": "gen_dev_ddl.py", "note": "只生成文本，不执行"}
    if a.only in ("hive", "all"):
        manifest["hive"] = gen_hive(a.out)
    if a.only in ("doris", "all"):
        manifest["doris"] = gen_doris(a.out)

    mp = os.path.join(a.out, "manifest.json")
    io.open(mp, "w", encoding="utf-8").write(json.dumps(manifest, ensure_ascii=False, indent=2))
    print("\n== 完成 ==\n   manifest:", mp)
    print("   下一步：人工 grep 检查两个 SQL 文件，确认无正式库 LOCATION / 无 INSERT，再执行。")


if __name__ == "__main__":
    main()
