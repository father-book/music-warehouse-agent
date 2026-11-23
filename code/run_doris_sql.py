"""在集群（161）上执行一份 Doris SQL 文件。

按分号切分执行，忽略整行 `--` 注释；用于建表、灌指标字典等一次性 DDL/DML。
    python3 /root/offline/run_doris_sql.py /root/offline/meta_metric.sql

为什么不用 mysql 客户端：160/161 上没装 mysql CLI，但 pymysql 已装，
而且 Doris FE 在 163，从 161 直连 9030 是通的。
"""
import sys
import pymysql

if len(sys.argv) < 2:
    print("用法: python3 run_doris_sql.py <sql 文件>")
    sys.exit(2)

path = sys.argv[1]
lines = [l for l in open(path, encoding="utf-8").read().split("\n")
         if not l.strip().startswith("--")]
stmts = [s.strip() for s in "\n".join(lines).split(";") if s.strip()]

c = pymysql.connect(host="192.168.128.163", port=9030, user="root", password="",
                    charset="utf8mb4", autocommit=True, read_timeout=600,
                    write_timeout=600)
cur = c.cursor()
print("文件 %s：%d 条语句" % (path, len(stmts)))
for i, s in enumerate(stmts, 1):
    head = " ".join(s.split())[:80]
    try:
        cur.execute(s)
        if cur.description and cur.rowcount <= 20:
            print("[%d/%d] %s -> %s" % (i, len(stmts), head, cur.fetchall()))
        else:
            print("[%d/%d] %s -> OK" % (i, len(stmts), head))
    except Exception as e:
        print("[%d/%d] !! 失败 %s : %s" % (i, len(stmts), head, str(e)[:300]))
        sys.exit(3)
c.close()
print("=== DONE ===")
