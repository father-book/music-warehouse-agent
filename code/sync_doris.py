"""离线 ADS → Doris 全量刷新 + 逐表对账。

在 161 上执行（本机 pymysql 已装）：
    python3 /root/offline/sync_doris.py --create-ext          # 首次：补建 ads_funnel / ads_revenue
    python3 /root/offline/sync_doris.py                       # 常规：刷新 7 张 ADS 并对账
    python3 /root/offline/sync_doris.py --recreate-revenue    # 改列类型：DROP ads_revenue 后重建并同步

设计要点：
  · 源端校验不硬编码「31 天」——接真实数据源、dt 参数化后会持续增长，
    只要求总览表非空且末日存在，其余表的范围打印出来人工/日志核对。
  · 用 INSERT OVERWRITE（UNIQUE 表下原子替换），不用 INSERT INTO。
  · 留存表末日比其它表少一天是正常的：最后一天的 cohort 没有 7 天观察窗口。
"""
import sys
import pymysql

SQL_FILE = "/root/offline/10_sync_to_doris.sql"
EXT_DDL = "/root/offline/doris_ads_ext.sql"

# (表名, 日期列)
TABS = [
    ("ads_daily_overview", "dt"),
    ("ads_song_ranking", "dt"),
    ("ads_genre_dist", "dt"),
    ("ads_province_dist", "dt"),
    ("ads_user_retention", "cohort_date"),
    ("ads_funnel", "dt"),
    ("ads_revenue", "dt"),
]

c = pymysql.connect(host="192.168.128.163", port=9030, user="root", password="",
                    charset="utf8mb4", autocommit=True, read_timeout=600,
                    write_timeout=600)
cur = c.cursor()


def q(sql):
    cur.execute(sql)
    return cur.fetchall()


def run_file(path, label):
    """按分号切分执行，忽略整行注释。"""
    lines = [l for l in open(path, encoding="utf-8").read().split("\n")
             if not l.strip().startswith("--")]
    stmts = [s.strip() for s in "\n".join(lines).split(";") if s.strip()]
    print("=== %s：%d 条语句 ===" % (label, len(stmts)))
    for i, s in enumerate(stmts, 1):
        print("[%d/%d] %s" % (i, len(stmts), " ".join(s.split())[:75]))
        try:
            cur.execute(s)
            print("    OK")
        except Exception as e:
            print("    !! 失败: %s" % str(e)[:300])
            sys.exit(3)


# CREATE TABLE IF NOT EXISTS 不会改已有表的列类型。金额口径由 DOUBLE 改 DECIMAL
# 时必须先 DROP 再按 doris_ads_ext.sql 重建；重建后本脚本会立刻全量灌回，
# 所以不留数据空窗，也不需要单独写迁移 SQL。
do_ext = "--create-ext" in sys.argv
do_recreate = "--recreate-revenue" in sys.argv
if do_recreate:
    cur.execute("DROP TABLE IF EXISTS music_offline.ads_revenue")
    print("OK  已 DROP ads_revenue（准备按 DECIMAL 口径重建）")
if do_ext or do_recreate:
    run_file(EXT_DDL, "补建/重建 2 张 Doris ADS 表")
    for t in ("ads_funnel", "ads_revenue"):
        print("    %s: %s" % (t, q("select count(*) from music_offline.`%s`" % t)[0][0]))

# Doris 的 external catalog 会缓存库表清单：Hive 侧新建 ADS 表后不刷新会报
# "Unknown table"，12 号扩表时就踩过。每次同步前刷一次，代价很小。
try:
    cur.execute("REFRESH CATALOG hive_catalog")
    print("OK  已刷新 hive_catalog 元数据")
except Exception as e:
    print("!! 刷新 catalog 失败（继续尝试同步）: %s" % str(e)[:200])

print("=== 源端 Hive catalog (music_dw) ===")
for t, dc in TABS:
    try:
        r = q("select count(*), min(%s), max(%s) from hive_catalog.music_dw.`%s`"
              % (dc, dc, t))[0]
        print("SRC|%s|%s|%s|%s" % (t, r[0], r[1], r[2]))
    except Exception as e:
        print("!! 源端 %s 不可读：%s" % (t, str(e)[:200]))
        sys.exit(2)

r = q("select count(*), max(dt) from hive_catalog.music_dw.ads_daily_overview")[0]
if not r[0] or not r[1]:
    print("!! 源端总览表为空（count=%s max=%s），终止同步" % (r[0], r[1]))
    sys.exit(2)
print("源端校验通过：%s 天，末日 %s" % (r[0], r[1]))

run_file(SQL_FILE, "执行同步")

print("=== 目标端 Doris (music_offline) 对账 ===")
for t, dc in TABS:
    r = q("select count(*), min(%s), max(%s) from music_offline.`%s`"
          % (dc, dc, t))[0]
    print("DST|%s|%s|%s|%s" % (t, r[0], r[1], r[2]))

c.close()
print("=== DONE ===")
