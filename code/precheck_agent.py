# -*- coding: utf-8 -*-
"""Data Agent 开工前置检查：逐项核查方案 2.2/2.3 里的阻塞项是否仍未解决。"""
import pymysql

DORIS = dict(host="192.168.128.163", port=9030, user="root", password="", charset="utf8mb4")


def q(sql, db=None):
    conn = pymysql.connect(**DORIS, database=db) if db else pymysql.connect(**DORIS)
    try:
        with conn.cursor() as c:
            c.execute(sql)
            return c.fetchall()
    finally:
        conn.close()


print("=" * 70)
print("1) Doris 账号与权限")
try:
    rows = q("SELECT User, Host, Password FROM mysql.user")
    for r in rows:
        print("   user=%s host=%s pwd_len=%s" % (r[0], r[1], len(r[2] or "")))
except Exception as e:
    print("   查询失败:", e)
try:
    print("   SHOW GRANTS for root:", q("SHOW GRANTS"))
except Exception as e:
    print("   SHOW GRANTS 失败:", e)

print("=" * 70)
print("2) 实时表注释（预期仍全是 OLAP）")
rows = q("SELECT TABLE_SCHEMA, COUNT(*), SUM(CASE WHEN TABLE_COMMENT='OLAP' OR TABLE_COMMENT='' THEN 1 ELSE 0 END) "
         "FROM information_schema.tables WHERE TABLE_SCHEMA IN ('music_dw','music_offline') GROUP BY TABLE_SCHEMA")
for r in rows:
    print("   %s: 表数=%s 注释为OLAP或空=%s" % (r[0], r[1], r[2]))

print("=" * 70)
print("3) 字段注释覆盖率")
rows = q("SELECT TABLE_SCHEMA, COUNT(*), SUM(CASE WHEN COLUMN_COMMENT='' THEN 1 ELSE 0 END) "
         "FROM information_schema.columns WHERE TABLE_SCHEMA IN ('music_dw','music_offline') GROUP BY TABLE_SCHEMA")
for r in rows:
    tot, empty = int(r[1]), int(r[2])
    print("   %s: 字段=%d 无注释=%d 覆盖=%.1f%%" % (r[0], tot, empty, 100.0 * (tot - empty) / tot))

print("=" * 70)
print("4) meta_metric 现状")
try:
    rows = q("SELECT COUNT(*) FROM music_offline.meta_metric")
    print("   总数:", rows[0][0])
    rows = q("SELECT COUNT(*) FROM music_offline.meta_metric WHERE source_table IS NULL OR source_table=''")
    print("   source_table 为空:", rows[0][0])
    rows = q("SELECT COUNT(*) FROM music_offline.meta_metric WHERE level='派生' AND (source_table IS NULL OR source_table='')")
    print("   派生且无落点:", rows[0][0])
except Exception as e:
    print("   失败:", e)

print("=" * 70)
print("5) 离线库表清单")
rows = q("SELECT TABLE_NAME FROM information_schema.tables WHERE TABLE_SCHEMA='music_offline' ORDER BY 1")
print("   表数=%d: %s" % (len(rows), ", ".join(r[0] for r in rows)))

print("=" * 70)
print("6) 实时链路新鲜度")
try:
    rows = q("SELECT MAX(event_ts), COUNT(*) FROM music_dw.dwd_play_start WHERE dt>=DATE_SUB(CURDATE(), INTERVAL 1 DAY)")
    print("   dwd_play_start max_event_ts=%s 近两日行数=%s" % (rows[0][0], rows[0][1]))
except Exception as e:
    print("   失败:", e)

print("=" * 70)
print("7) 离线 ADS 数据范围")
for t in ("ads_daily_overview", "ads_genre_dist", "ads_province_dist", "ads_song_ranking"):
    try:
        rows = q("SELECT MIN(dt), MAX(dt), COUNT(*) FROM music_offline.%s" % t)
        print("   %-22s %s ~ %s  行=%s" % (t, rows[0][0], rows[0][1], rows[0][2]))
    except Exception as e:
        print("   %s 失败: %s" % (t, e))
print("=" * 70)
