#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""agent_dev 权限实测：能写 music_dev，不能写 music_dw。

为什么不用 `DELETE ... WHERE 1=0` 验证：Doris 对恒假条件会直接短路返回成功，
根本不走到权限检查，用它验证会把「被拒绝」误判成「能删数据」。
这里一律用真实条件（命中真实行）+ 建表 + 回查行数不变。

用法：python3 test_dev_perm.py
"""
import pymysql

HOST, PORT = "192.168.128.163", 9030
DEV_USER, DEV_PWD = "agent_dev", "AgentDev_2026#dw"

rows = []


def try_sql(cur, desc, sql, expect_ok, fetch=False):
    try:
        cur.execute(sql)
        got = "OK"
        data = cur.fetchall() if (fetch and cur.description) else None
    except Exception as e:
        got = "REFUSED: " + str(e).strip().split("\n")[0][:90]
        data = None
    passed = (got == "OK") == expect_ok
    rows.append((passed, desc, "允许" if expect_ok else "拒绝", got, data))
    return data


root = pymysql.connect(host=HOST, port=PORT, user="root", password="",
                       charset="utf8mb4", autocommit=True)
rc = root.cursor()

# ---------- 准备：正式库放一张真表，2 行数据 ----------
for s in ("DROP TABLE IF EXISTS music_dw.zz_perm_test",
          "CREATE TABLE music_dw.zz_perm_test (id INT, v VARCHAR(10)) "
          "UNIQUE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1 "
          'PROPERTIES("replication_num"="1")',
          "INSERT INTO music_dw.zz_perm_test VALUES (1,'a'),(2,'b')"):
    rc.execute(s)

dev = pymysql.connect(host=HOST, port=PORT, user=DEV_USER, password=DEV_PWD,
                      charset="utf8mb4", autocommit=True)
dc = dev.cursor()

# ---------- A. 应成功的 5 项 ----------
d = try_sql(dc, "A1 当前用户", "SELECT CURRENT_USER()", True, fetch=True)
if d:
    rows[-1] = (rows[-1][0], rows[-1][1], rows[-1][2], rows[-1][3], d[0][0])
try_sql(dc, "A2 读正式库", "SELECT COUNT(*) FROM music_dw.dwd_play_start", True, fetch=True)
try_sql(dc, "A3 dev 建表",
        "CREATE TABLE music_dev.zz_probe (id INT) DISTRIBUTED BY HASH(id) BUCKETS 1 "
        'PROPERTIES("replication_num"="1")', True)
try_sql(dc, "A4 dev 插入", "INSERT INTO music_dev.zz_probe VALUES (1)", True)
try_sql(dc, "A5 dev 删表", "DROP TABLE music_dev.zz_probe", True)

# ---------- B. 应被拒绝的 7 项（真实条件）----------
try_sql(dc, "B1 正式库插入", "INSERT INTO music_dw.zz_perm_test VALUES (3,'c')", False)
try_sql(dc, "B2 正式库删除", "DELETE FROM music_dw.zz_perm_test WHERE id = 1", False)
try_sql(dc, "B3 正式库更新", "UPDATE music_dw.zz_perm_test SET v='z' WHERE id = 2", False)
try_sql(dc, "B4 正式库加列", "ALTER TABLE music_dw.zz_perm_test ADD COLUMN x INT", False)
try_sql(dc, "B5 正式库删表", "DROP TABLE music_dw.zz_perm_test", False)
try_sql(dc, "B6 正式库建表",
        "CREATE TABLE music_dw.zz_probe2 (id INT) DISTRIBUTED BY HASH(id) BUCKETS 1 "
        'PROPERTIES("replication_num"="1")', False)
try_sql(dc, "B7 删正式库", "DROP DATABASE music_dw", False)

# ---------- C. 回查：正式库必须还是 2 行、且没有 x 列 ----------
rc.execute("SELECT COUNT(*) FROM music_dw.zz_perm_test")
cnt = rc.fetchone()[0]
rows.append((cnt == 2, "C1 正式库行数仍为 2", "=2", str(cnt), None))
rc.execute("SHOW COLUMNS FROM music_dw.zz_perm_test")
cols = [r[0] for r in rc.fetchall()]
rows.append(("x" not in cols, "C2 B4 加列未生效", "无 x 列", ",".join(cols), None))

# ---------- 收尾 ----------
rc.execute("DROP TABLE IF EXISTS music_dw.zz_perm_test")
rc.execute("DROP TABLE IF EXISTS music_dev.zz_probe")

print("=" * 96)
for ok, desc, exp, got, data in rows:
    print("%s %-18s 期望=%s  实际=%s%s"
          % ("PASS" if ok else "FAIL", desc, exp, got,
             ("  值=" + str(data)) if data is not None else ""))
print("=" * 96)
fails = sum(1 for r in rows if not r[0])
print("合计 %d 项，失败 %d 项" % (len(rows), fails))
