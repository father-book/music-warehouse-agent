"""按 datart_views.sql 逐条执行视图 SQL，验证 DataArt 侧能取到最新数据。

在 161 上执行（pymysql 已装，Doris FE 在 163:9030）：
    python3 /root/offline/verify_datart.py /root/offline/datart_views.sql

视图是直查 Doris 的 SELECT、没有缓存表，Doris 同步完成即生效；
本脚本用于回归：任何一次 ADS 改表/重灌后，跑一次确认 9 个视图都能取数。
"""
import sys
import pymysql

path = sys.argv[1] if len(sys.argv) > 1 else "/tmp/datart_views.sql"
lines = [l for l in open(path, encoding="utf-8").read().split("\n")
         if not l.strip().startswith("--")]
stmts = [s.strip() for s in "\n".join(lines).split(";") if s.strip()]

c = pymysql.connect(host="192.168.128.163", port=9030, user="root", password="",
                    charset="utf8mb4", autocommit=True, read_timeout=300)
cur = c.cursor()
print("视图数:", len(stmts))
errors = 0
for i, s in enumerate(stmts, 1):
    if not s.upper().startswith("SELECT"):
        print("[%d] SKIP 非 SELECT" % i)
        continue
    try:
        cur.execute(s)
        rows = cur.fetchall()
        cols = [d[0] for d in cur.description] if cur.description else []
        first = rows[0][0] if rows else None
        last = rows[-1][0] if rows else None
        print("V|%d|rows=%s|first=%s|last=%s|cols=%s" %
              (i, len(rows), first, last, ",".join(cols[:3])))
    except Exception as e:
        errors += 1
        print("V|%d|ERROR %s" % (i, str(e)[:200]))
c.close()
print("=== DONE errors=%d ===" % errors)
sys.exit(1 if errors else 0)
