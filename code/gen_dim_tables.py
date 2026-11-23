#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
gen_dim_tables.py —— 生成离线维度表数据（歌曲 / 日期 / 用户变更流水）

为什么维表要独立造数，而不是从 ODS 事实表反推：
  ODS 里所有用户属性都由 user_idx 派生（is_vip = user_idx%10==0、city = pick(...,user_idx)），
  同一用户 30 天属性恒定 → 从事实表推出来的拉链表只有一个版本，SCD2 逻辑等于没验证。
  真实维表有自己的业务源（注册系统、会员系统、用户中心变更日志），
  所以这里单独造一张「用户属性变更流水」，再用 SCD2 逻辑合并成拉链表。

产出三个数据集（写到 HDFS，由 SQL 建 EXTERNAL 表指向）：
  1. dim_song         1486 行  歌曲维度（song_id 沿用 CSV 原值 S000001~S001486，与事实表同空间）
  2. dim_date         35 行    日期维度（2026-10-05 ~ 2026-11-08）
  3. ods_user_change  ~115 万行 用户属性变更流水（100 万用户 + 10% 升级 VIP + 5% 换机）

用户变更设计（可复现，用于验证 SCD2）：
  · 全员注册：注册日期 2026-08-01 ~ 2026-09-29 分散（必须早于事实表起始日）
  · 10% 用户（id%10==5）在 2026-10-20 升级为 VIP
  ·  5% 用户（id%20==0）在 2026-10-25 换了一台设备
  → 拉链表里应出现：单版本用户、双版本用户、三版本用户（既升级又换机）

用法（在 161 上）：
  /opt/module/spark/bin/spark-submit --master yarn --deploy-mode client \
      --conf spark.driver.memory=1g --conf spark.executor.memory=1g \
      --conf spark.executor.cores=1 --conf spark.executor.instances=3 \
      --conf spark.pyspark.python=python3 --conf spark.pyspark.driver.python=python3 \
      gen_dim_tables.py
"""
import calendar
import time

from pyspark.sql import SparkSession, Window, functions as F

BASE = "hdfs://WWC160:9820/warehouse/music_dw"
START_DT = "2026-10-05"
# 必须与 gen_events.py 的 --days 一致。原值 30（10-05~11-03），
# 2026-11-09 重造改为 35 天（10-05~11-08）。dim_date 少一天，7 处 JOIN dim_date
# 的报表在缺失那几天会直接丢行，而且不报错。
DAYS = 35
SONG_CNT = 1486   # 与 data/dim/dim_song.csv 的真实歌曲数一致
USER_CNT = 1000000        # v7：事实表的用户池扩到 100 万，维度表必须对齐

_t = time.strptime(START_DT + " 00:00:00", "%Y-%m-%d %H:%M:%S")
BASE_MS = (calendar.timegm(_t) - 8 * 3600) * 1000

spark = (SparkSession.builder
         .appName("gen_dim_tables")
         .config("spark.sql.shuffle.partitions", "8")
         .config("spark.sql.orc.compression.codec", "snappy")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")


def pmod(col, n):
    """正模。Spark 的 % 对负数被除数返回负数，桶号会落到负区间。

    注意集群这个 Spark 版本的 pyspark.sql.functions 里**没有** pmod，
    必须自己实现 —— 写 F.pmod(...) 会直接 AttributeError。
    """
    return (col % n + n) % n


def pick(values, idx):
    return F.element_at(F.array(*[F.lit(x) for x in values]),
                        (idx % len(values) + 1).cast("int"))


# ─────────────────────────────────────────────────────────────────────────────
# 1. dim_song —— 歌曲维度
#    song_id 必须与 ODS 里的格式完全一致：S + 6 位、从 000001 开始（沿用 CSV 原值）。
#    duration 也必须一致：ODS 用 dur = 120 + pmod(song_num, 179)，这里用同一公式，
#    否则维表 join 事实表时会出现「时长对不上」，完播率立刻失真。
#    模 179（质数）而不是 180：180 与 genre 的模 20 不互质，会让「时长」跟着曲风走。
# ─────────────────────────────────────────────────────────────────────────────
SONG_CSV = "hdfs://WWC160:9820/data/dim/dim_song.csv"
ARTIST_CSV = "hdfs://WWC160:9820/data/dim/dim_artist.csv"

# 文件带 UTF-8 BOM ⇒ Spark 读出来的第一列名是 "\ufeffsong_id"，取不到列，
# 所以统一按「第一列」重命名，不写死列名。
_song = spark.read.option("header", True).option("encoding", "UTF-8").csv(SONG_CSV)
_song = _song.withColumnRenamed(_song.columns[0], "song_id")

_artist = spark.read.option("header", True).option("encoding", "UTF-8").csv(ARTIST_CSV)
_artist = _artist.withColumnRenamed(_artist.columns[0], "artist_id")

dim_song = (
    _song.join(_artist.select("artist_id", "artist_name", "country"), ["artist_id"], "left")
         .orderBy("song_id")
         .select(
             # 【不要重写 song_id】2026-11-09 修：旧版这里把 ID 重写成 `sg_%05d`
             # （sg_00000 起），而 gen_events.py 早在 v10 就改用 CSV 原值 `S%06d`
             # （S000001 起）。两边各改一半 —— 事实表 S 开头、维表 sg 开头，
             # `JOIN dim_song ON song_id` 命中 0 行。
             # 更坏的是它不报错：ads_genre_dist / ads_song_ranking 的 INSERT 动态分区
             # 在「0 行」时不会删除旧分区，于是表里留着上一次的 140 / 700 行，
             # 看起来一切正常。实测 2026-11-09：JOIN 命中 0，但两张 ADS 仍有数据。
             F.col("song_id"),
             F.col("song_name"),
             F.col("artist_name").alias("singer"),
             F.col("genre"),
             F.col("duration").cast("int").alias("duration"),
             F.col("release_date"),
             F.col("country").alias("region"),
         )
)
dim_song.write.mode("overwrite").orc(BASE + "/dim_song")
print("== dim_song 写入完成（MusicBrainz 真实数据）：%d 行 ==" % SONG_CNT)


# ─────────────────────────────────────────────────────────────────────────────
# 2. dim_date —— 日期维度（比事实表多留 1 天，方便跨天计算）
# ─────────────────────────────────────────────────────────────────────────────
d = F.col("id")
dim_date = spark.range(0, DAYS).select(
    F.date_format(F.date_add(F.to_date(F.lit(START_DT)), d.cast("int")), "yyyy-MM-dd").alias("dt"),
    # dayofweek: 1=周日 … 7=周六（Spark 语义）。不要用 cast(date as int)，
    # 那样得到的是 20261005 这种 yyyymmdd 数字，对分析毫无用处。
    F.dayofweek(F.date_add(F.to_date(F.lit(START_DT)), d.cast("int"))).cast("int").alias("day_of_week_num"),
    F.date_format(F.date_add(F.to_date(F.lit(START_DT)), d.cast("int")), "EEEE").alias("day_of_week"),
    F.weekofyear(F.date_add(F.to_date(F.lit(START_DT)), d.cast("int"))).cast("int").alias("week_of_year"),
    F.month(F.date_add(F.to_date(F.lit(START_DT)), d.cast("int"))).cast("int").alias("month"),
    F.quarter(F.date_add(F.to_date(F.lit(START_DT)), d.cast("int"))).cast("int").alias("quarter"),
    F.year(F.date_add(F.to_date(F.lit(START_DT)), d.cast("int"))).cast("int").alias("year"),
    F.when(F.dayofweek(F.date_add(F.to_date(F.lit(START_DT)), d.cast("int"))).isin(1, 7), "Y")
     .otherwise("N").alias("is_weekend"),
)
dim_date.write.mode("overwrite").orc(BASE + "/dim_date")
print("== dim_date 写入完成：%d 行 ==" % DAYS)


# ─────────────────────────────────────────────────────────────────────────────
# 3. ods_user_change —— 用户属性变更流水（SCD2 的源）
#    每个用户至少 1 条（注册），部分用户追加变更记录。
#    这里刻意让「第一版 is_vip=false 的用户」在 10-20 升级，形成真实的状态迁移。
# ─────────────────────────────────────────────────────────────────────────────

# ── 地域池：全国 31 个省级行政区，按 2020 七普常住人口加权 ──────────────────
# 【为什么不能用 5 个省】旧版只写死江西/广东/浙江/江苏/四川，dim_user 的
# province 就只有 5 个取值，而事实表明细按 31 省分布 —— 用户维度与行为维度的
# 地域口径直接对不上，任何 JOIN dim_user 取省份的报表都会丢掉其余 26 省。
#
# 【2026-11-09 修正】旧版这里是「要求分布一致，不要求逐行相等」，理由是
# 「画像省份（注册地）和埋点省份（一次上报）本来就可能不同」。这个理由本身没错，
# 但它被用来给一个真 bug 开脱：维表按 u%100 落桶、事实表按 uid%1000 落桶，
# 同一用户必然两个省 —— 这不是「出差/VPN」的业务差异，是两套算式没对齐。
# 现在的约定是：**省份由用户编号唯一决定，维表与事实表逐行相等**。
# 真实的跨省漫游应该体现为「事件里多一个上报省字段」，而不是让两边各算各的。
_REGIONS = [
    ("广东省", ["深圳市", "广州市", "东莞市"], 12601),
    ("山东省", ["济南市", "青岛市", "烟台市"], 10153),
    ("河南省", ["郑州市", "洛阳市", "南阳市"], 9937),
    ("江苏省", ["南京市", "苏州市", "无锡市"], 8475),
    ("四川省", ["成都市", "绵阳市", "南充市"], 8367),
    ("河北省", ["石家庄市", "唐山市", "保定市"], 7461),
    ("湖南省", ["长沙市", "株洲市", "衡阳市"], 6644),
    ("浙江省", ["杭州市", "宁波市", "温州市"], 6457),
    ("安徽省", ["合肥市", "芜湖市", "阜阳市"], 6103),
    ("湖北省", ["武汉市", "宜昌市", "襄阳市"], 5775),
    ("广西壮族自治区", ["南宁市", "桂林市", "柳州市"], 5013),
    ("云南省", ["昆明市", "大理市", "曲靖市"], 4721),
    ("江西省", ["南昌市", "赣州市", "九江市"], 4519),
    ("辽宁省", ["沈阳市", "大连市", "鞍山市"], 4259),
    ("福建省", ["福州市", "厦门市", "泉州市"], 4154),
    ("陕西省", ["西安市", "宝鸡市", "咸阳市"], 3953),
    ("贵州省", ["贵阳市", "遵义市", "毕节市"], 3856),
    ("山西省", ["太原市", "大同市", "临汾市"], 3492),
    ("重庆市", ["重庆市"], 3205),
    ("黑龙江省", ["哈尔滨市", "大庆市", "齐齐哈尔市"], 3185),
    ("新疆维吾尔自治区", ["乌鲁木齐市", "喀什市"], 2585),
    ("甘肃省", ["兰州市", "天水市"], 2502),
    ("上海市", ["上海市"], 2487),
    ("吉林省", ["长春市", "吉林市"], 2407),
    ("内蒙古自治区", ["呼和浩特市", "包头市", "鄂尔多斯市"], 2400),
    ("北京市", ["北京市"], 2189),
    ("天津市", ["天津市"], 1387),
    ("海南省", ["海口市", "三亚市"], 1008),
    ("宁夏回族自治区", ["银川市", "石嘴山市"], 720),
    ("青海省", ["西宁市", "格尔木市"], 592),
    ("西藏自治区", ["拉萨市", "日喀则市"], 366),
]


def _region_buckets(n_buckets=100):
    """按人口权重把 31 个省展开成 n_buckets 个桶，每桶带一个城市。"""
    total = float(sum(r[2] for r in _REGIONS))
    rows = []
    for name, cities, w in _REGIONS:
        cnt = max(1, int(round(w * n_buckets / total)))
        for j in range(cnt):
            if len(rows) >= n_buckets:
                break
            rows.append((name, cities[j % len(cities)]))
    top = _REGIONS[0]
    while len(rows) < n_buckets:
        i = len(rows)
        rows.append((top[0], top[1][i % len(top[1])]))
    return rows[:n_buckets]


# 【地域与 ID 的统一约定 —— 本文件与 gen_events.py 必须逐字一致】
# 两个脚本各跑各的，产出的却是同一批用户，因此有三件事必须共用同一套算式，
# 否则维表与事实表会在「同一个 user_id」上互相打架：
#
#   1. user_id 编号空间：U + 7 位数字，取值 1..1,000,000（un = 索引 + 1）
#      旧版维表写 u_000000（小写 u + 下划线 + 6 位）、事实表写 U024966（大写 U + 6 位），
#      两套 ID 一个都对不上 —— 任何 JOIN dim_user 取省份/年龄/会员的报表都会全部落空，
#      而且 LEFT JOIN 落空只会让指标变 0，不会报错，属于最难发现的一类假数据。
#
#   2. 地域桶：un % 1000 落桶，桶表由 31 省按人口权重展开
#      旧版维表用 100 桶、事实表用 1000 桶，即使 ID 对齐也会落到不同省。
#
#   3. 桶必须由 un（塌缩后的用户编号）决定，不能由 uid（行为模型的 100 万池子）决定
#      事实表 uid∈[0,10^6) 经 pmod(uid,105000)+1 塌缩成 10.5 万个 user_id，
#      而旧版按 uid 定桶 —— 被塌缩到一起的 105 个 uid 各拿一个省，
#      于是同一个 user_id 在明细里横跨多省。2026-11-09 实测 11-08 当天 18 人跨省，
#      分省 DAU 之和 2,144 比全站 2,054 多出 90。这种误差不会报错，只会让
#      「各省 DAU 加起来 ≠ 大盘 DAU」，任何做过分省报表的人都会被它坑一次。
REG_N = 1000                    # 地域桶数量；gen_events.py 必须用同一个值
_REG_BUCKETS = _region_buckets(REG_N)
PROV_POOL = [r[0] for r in _REG_BUCKETS]
CITY_POOL = [r[1] for r in _REG_BUCKETS]

# 1000 个桶若用 element_at(array(lit,lit,...)) 展开，表达式树会带 1000 个字面量，
# 执行计划膨胀到 Spark 打印 "Truncated the string representation of a plan"。
# 改成 1000 行的广播表做 map-side join，成本可以忽略。
_REG_ROWS = [(i, p, c) for i, (p, c) in enumerate(_REG_BUCKETS)]
_reg_df = spark.createDataFrame(_REG_ROWS, ["rb", "province", "city"])

APP_VERSIONS = ["8.1.0", "8.2.0", "8.3.1", "9.0.0"]
OS_LIST = ["android", "android", "ios", "ios", "web"]
DEVICES = ["MI 13", "iPhone 14", "Redmi Note 12", "Pixel 7"]


def _un(c):
    """用户编号（1 起）。维表与事实表共用同一编号空间。"""
    return (c + 1).cast("long")


def _user_id_col(c):
    return F.concat(F.lit("U"), F.lpad(_un(c).cast("string"), 7, "0"))


def _with_region(df, un_col):
    """按 un % REG_N 落桶，join 出 province / city。"""
    df = df.withColumn("rb", pmod(un_col, F.lit(REG_N)))
    return df.join(F.broadcast(_reg_df), ["rb"], "left").drop("rb")


def user_rows(change_date, vip_rule, dev_shift, change_type, id_filter):
    """构造一批变更记录；dev_shift=0 表示设备不变。

    id_filter 必须在 select 之前应用 —— select 后 id 列就不存在了，
    在后面 filter 会因为 "cannot resolve id" 报错（这里踩过一次）。

    【为什么变更场景是「换机」而不是「搬家」】旧版让 5% 用户搬到下一个城市，
    但事实表里的 province 由用户编号固定、不随日期变化 —— 维表说用户 10-25 搬走了，
    明细里 11-01 的事件却还在旧省，两边永远对不上。而「换机」只改 device_model，
    与地域无关，不会制造这种跨表矛盾。
    """
    df = spark.range(0, USER_CNT).filter(id_filter)
    df = _with_region(df, _un(F.col("id")))
    return df.select(
        _user_id_col(F.col("id")).alias("user_id"),
        F.lit(change_date).alias("change_date"),
        vip_rule.alias("is_vip"),
        F.col("city"),
        F.col("province"),
        pick(APP_VERSIONS, F.col("id")).alias("app_version"),
        pick(OS_LIST, F.col("id")).alias("os"),
        pick(DEVICES, F.col("id") + F.lit(dev_shift)).alias("device_model"),
        F.lit(change_type).alias("change_type"),
    )


# 注册：注册日期分散在 2026-08-01 ~ 2026-09-29（前 60 天），保证早于事实表起始日。
# 【不要造「当日新注册」】事实表里这 10.5 万用户的事件贯穿 10-05~11-08 整整 35 天，
# 若把它们标成 11-08 注册，就会出现「事件时间早于注册时间」的时序矛盾。
reg = _with_region(spark.range(0, USER_CNT), _un(F.col("id"))).select(
    _user_id_col(F.col("id")).alias("user_id"),
    F.date_format(F.date_add(F.to_date(F.lit("2026-08-01")),
                             (F.col("id") % 60).cast("int")), "yyyy-MM-dd").alias("change_date"),
    F.when(F.col("id") % 10 == 0, "true").otherwise("false").alias("is_vip"),
    F.col("city"),
    F.col("province"),
    pick(APP_VERSIONS, F.col("id")).alias("app_version"),
    pick(OS_LIST, F.col("id")).alias("os"),
    pick(DEVICES, F.col("id")).alias("device_model"),
    F.lit("register").alias("change_type"),
)

# 变更 1：10% 用户（id%10==5）在 2026-10-20 升级 VIP（这批原本不是 VIP）
vip_up = user_rows("2026-10-20", F.lit("true"), 0, "vip_upgrade", (F.col("id") % 10) == 5)

# 变更 2：5% 用户（id%20==0）在 2026-10-25 换了一台设备
# （只改 device_model，不动 province/city —— 地域必须与事实表逐行一致）
dev_change = user_rows("2026-10-25",
                       F.when(F.col("id") % 10 == 0, "true").otherwise("false"),
                       1, "device_change", (F.col("id") % 20) == 0)

all_change = reg.unionAll(vip_up).unionAll(dev_change)
all_change.write.mode("overwrite").orc(BASE + "/ods_user_change")
print("== ods_user_change 写入完成 ==")

spark.stop()
