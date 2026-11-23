#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""给 music_dw.dwd_play_end 增加 4 个 DIM 维度列。

为什么单独写脚本而不是一条 SQL 文件：
    Doris 的 ALTER TABLE 是异步 schema change job，同一张表同时只能有一个未完成的
    job。4 条 ALTER 连着发，后 3 条会报 "Schema change is in progress"。
    必须逐条提交、轮询 SHOW ALTER TABLE COLUMN 到 FINISHED 再发下一条。

幂等：列已存在就跳过，可反复执行。

用法（Windows）：
    python code/add_dim_columns.py
"""
import time

import pymysql

DB = dict(host="192.168.128.163", port=9030, user="root", password="",
          database="music_dw", charset="utf8mb4", autocommit=True)

# 顺序必须与 job_b_dwd_dim.sql 里 sink_dwd_play_end 的列序一致
COLS = [
    ("song_name",   "VARCHAR(128) NULL COMMENT 'DIM-真实歌名（HBase LOOKUP JOIN 退化）'"),
    ("artist_name", "VARCHAR(64)  NULL COMMENT 'DIM-真实歌手'"),
    ("genre",       "VARCHAR(32)  NULL COMMENT 'DIM-曲风'"),
    ("album_name",  "VARCHAR(128) NULL COMMENT 'DIM-专辑名'"),
]


def schema_jobs(cur):
    cur.execute("SHOW ALTER TABLE COLUMN FROM music_dw")
    return cur.fetchall()


def wait_done(cur, timeout=180):
    deadline = time.time() + timeout
    while time.time() < deadline:
        pending = False
        for r in schema_jobs(cur):
            tn = (r.get("TableName") or r.get("tablename") or
                  (r[1] if isinstance(r, (tuple, list)) and len(r) > 1 else ""))
            st = (r.get("State") or r.get("state") or
                  (r[9] if isinstance(r, (tuple, list)) and len(r) > 9 else ""))
            if "dwd_play_end" in str(tn) and str(st).upper() in (
                    "PENDING", "RUNNING", "WAITING_TXN", "UNKNOWN"):
                pending = True
                break
        if not pending:
            return True
        time.sleep(2)
    return False


def main():
    conn = pymysql.connect(**DB)
    cur = conn.cursor(pymysql.cursors.DictCursor)

    cur.execute("DESC music_dw.dwd_play_end")
    existing = {r["Field"] for r in cur.fetchall()}
    print("当前列数：", len(existing))

    for name, typedef in COLS:
        if name in existing:
            print("  跳过 %-12s（已存在）" % name)
            continue
        sql = "ALTER TABLE music_dw.dwd_play_end ADD COLUMN %s %s" % (name, typedef)
        print("  提交 %s" % sql)
        try:
            cur.execute(sql)
        except Exception as e:
            print("    ✗ ALTER 失败：", e)
            return 1
        if not wait_done(cur):
            print("    ✗ 等待 schema change 超时，请手工执行 SHOW ALTER TABLE COLUMN 检查")
            return 1
        print("    ✓ 完成")

    cur.execute("DESC music_dw.dwd_play_end")
    cols = [r["Field"] for r in cur.fetchall()]
    print("\n加列后的列序（末尾 8 列）：")
    for i, c in enumerate(cols[-8:], len(cols) - 7):
        print("  %2d %s" % (i, c))
    missing = [n for n, _ in COLS if n not in cols]
    print("\n缺失列：", missing or "无")
    conn.close()
    return 0 if not missing else 1


if __name__ == "__main__":
    raise SystemExit(main())
