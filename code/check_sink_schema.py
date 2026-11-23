#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""核对 Flink sink 定义与 Doris 实际表结构是否一致。

为什么需要这个检查：
    Flink Doris Sink 走 Stream Load 的 JSON 导入，按列名匹配。
    如果 SQL 里写的列名和 Doris 表对不上，不会报「列不存在」，
    而是该列被静默丢弃或整行被拒——数据看着写进去了，实际缺字段。
    这类问题在答辩现场被问「你这个完播率字段是哪来的」时最难解释。

检查内容：
    1. SQL 里 sink 的列名集合 vs DESC 得到的实际列名集合
    2. 顺序是否一致（Doris 按名匹配，顺序不影响正确性，但顺序不一致通常意味着
       表结构改动过而 SQL 没同步，值得警示）

用法：
    python code/check_sink_schema.py
    python code/check_sink_schema.py sql/realtime/job_b_dwd.sql
"""

import glob
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from check_flink_sql import parse_tables, strip_comments  # noqa: E402

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DORIS = dict(host="192.168.128.163", port=9030, user="root", password="",
             database="music_dw", charset="utf8mb4", connect_timeout=5)


def actual_columns(cur, table_id):
    db, tbl = table_id.split(".")
    cur.execute(f"DESC `{db}`.`{tbl}`")
    rows = cur.fetchall()
    # DESC 返回 (Field, Type, Null, Key, Default, Extra)
    return [r[0] for r in rows if r[0]]


def main():
    files = sys.argv[1:] or sorted(
        glob.glob(os.path.join(BASE_DIR, "sql/realtime/job_*.sql"))
    )
    if not files:
        print("没有找到 job_*.sql")
        return 1

    try:
        import pymysql
    except ImportError:
        print("未安装 pymysql")
        return 1

    try:
        conn = pymysql.connect(**DORIS)
        cur = conn.cursor()
    except Exception as e:
        print(f"无法连接 Doris: {e}")
        return 1

    problems = 0
    for path in files:
        sql = strip_comments(open(path, encoding="utf-8").read())
        tables = parse_tables(sql)
        sinks = [
            (name, cols, props["table.identifier"])
            for name, (cols, props) in tables.items()
            if props.get("connector") == "doris" and props.get("table.identifier")
        ]
        if not sinks:
            continue

        print(f"\n--- {os.path.basename(path)} ---")
        for name, cols, table_id in sinks:
            try:
                actual = actual_columns(cur, table_id)
            except Exception as e:
                print(f"  [FAIL] {table_id}: {e}")
                problems += 1
                continue

            if cols == actual:
                print(f"  [OK]   {table_id}  ({len(cols)} 列)")
                continue

            only_sql = [c for c in cols if c not in actual]
            only_tbl = [c for c in actual if c not in cols]
            if not only_sql and not only_tbl:
                print(f"  [WARN] {table_id}: 列名一致但顺序不同（通常说明表改过而 SQL 没同步）")
                print(f"         SQL: {cols[:6]}...")
                print(f"         表 : {actual[:6]}...")
                problems += 1
            else:
                print(f"  [FAIL] {table_id}")
                if only_sql:
                    print(f"         SQL 里有、表里没有: {only_sql}")
                if only_tbl:
                    print(f"         表里有、SQL 里没有: {only_tbl}")
                problems += 1

    conn.close()
    print()
    if problems:
        print(f"发现 {problems} 处不一致，先修好再把作业提交上去")
        return 1
    print("所有 sink 与 Doris 表结构一致")
    return 0


if __name__ == "__main__":
    sys.exit(main())
