#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
gen_offline_ods.py —— 生成离线 ODS 数据（Spark DataFrame 直接写 ORC）

⚠️ 已废弃（2026-11-07 起由 gen_events.py 取代），保留仅作历史参考。
   ⚠️ 不要用它造数：本文件里的 user_id 仍是 u_%06d（小写 u + 下划线 + 6 位），
   而现行事实表是 U + 7 位（1..1,000,000）。跑出来的数据与维表、与现有明细
   全都对不上，且 JOIN 落空不会报错，只会让指标变 0。
   现行造数脚本：gen_events.py（事件）+ gen_dim_tables.py（维表）。

为什么不用 spark-sql：
  161 上这份 Spark 3.3.1 的 jars 里只有 hive-storage-api，没有 spark-hive 模块，
  SparkSQLCLIDriver 根本不存在。而我们不建 Hive 表、只往 HDFS 目录写文件，
  用 DataFrame API + df.write.orc() 就够了，完全不碰 Hive。

为什么用 id 取模而不是 rand()：
  同一个 id 每次生成的行完全一致 —— 可复现、可回归、可对账。
  rand() 每次结果不同，出了问题没法复现，也没法做"重跑两次结果一致"的幂等验证。

═══════════════════════════════════════════════════════════════════════════════
数据模型 v3（2026-11-05）：多天分区 + 会话为主体
═══════════════════════════════════════════════════════════════════════════════
v1 的硬伤：user_id / device_id 按「事件」取模，session_id 按「会话」取模，
          粒度不一致 → 一个会话里出现 20 个不同用户、20 台设备、20 个城市；
          事件类型由 id%100 决定，同会话 id 连续 → 整个会话只有一种事件。

v2 修正：身份属性全部按「会话」派生，事件按歌曲节奏分层。

v3 增加：一次作业写多天分区（partitionBy），而不是循环提交 N 次 Spark。
  · 为什么必须多天：只有一个分区时「分区裁剪」永远演示不出来 ——
    它的价值全在"查 1 天 vs 查 30 天"的对比上。
  · 跨天用户有意重叠 → 次日留存、留存漏斗才算得出来。

v7（2026-11-06）把用户模型换成「出生-死亡」生命周期：
  每个用户有出生日 b 和寿命 L，只在 [b, b+L) 内活跃，池扩到 100 万。
  cohort 的 day_n 留存 = S(n) = P(L>n)，只由 L 的分布决定 → 所有 cohort
  曲线一致且必然衰减（v6 的滑动窗口让第 2 天起的 cohort 恒为 100%）。

v6（2026-11-06）修两个「不报错但结论是假的」问题：
  · 留存等差衰减：旧 user_idx = (dt_idx*7313 + sess) % 100000 让每天的用户集合
    是一段连续区间，相邻两天重叠 = 50000 - 7313d，必然等差。
    改为三层用户池（铁杆 / 高频 / 低频，p = 1.00 / 0.75 / 0.40），
    曲线变成「首日陡降 + 长尾」，见下方 user_idx 处的推导。
  · 曲风与完播率伪相关：旧 song_num 里 dt_idx 项被 5000 整除吃掉，
    genre 与 is_complete 共享 song_no。改为 combo 乘法哈希取高 20 位。
  · 顺带：device_id 改为由 user_id 派生（同一用户固定一台设备）。

数据形态（可复现，答辩可讲）：
  · 一个会话 = 1 用户 + 1 设备 + 1 省市 + 1 IP + 4 个设备属性
  · 一个会话 = 20 条事件 = 5 首歌 × 4 条（起播 / 结束或跳过 / 互动 / 浏览）
  · 事件类型：play_start 25% / play_end 17.5% / skip 7.5% / 其余 4 类各 ~12.5%
    → 结束事件里 70% 播完、30% 跳过（跳过率 30%，符合音乐 App）
  · 同一首歌的 play_start 与 play_end 共享 song_id → 完播率算得出来
  · 会话内事件间隔 1 分钟（20 条 = 19 分钟）；会话起始铺满当天

用法（在 161 上）：
  # 单天 200 万
  spark-submit ... gen_offline_ods.py 2026-11-03 2000000

  # 30 天共 3000 万（起始日 + 总行数 + 天数）
  spark-submit ... gen_offline_ods.py 2026-10-05 30000000 30

参数：起始日期 dt  总行数  天数（默认 1）
"""
import calendar
import sys
import time

from pyspark.sql import SparkSession, Window, functions as F

START_DT = sys.argv[1] if len(sys.argv) > 1 else "2026-11-03"
ROWS = int(sys.argv[2]) if len(sys.argv) > 2 else 1000000
DAYS = int(sys.argv[3]) if len(sys.argv) > 3 else 1

BASE_PATH = "hdfs://WWC160:9820/warehouse/music_dw/ods_music_event"

SESSION_EVENTS = 20      # 每会话事件数
SONGS_PER_SESSION = 5    # 每会话歌曲数（20 / 4）
EVENT_STEP_MS = 60000    # 会话内事件间隔 1 分钟
DAY_MS = 86400000

_t = time.strptime(START_DT + " 00:00:00", "%Y-%m-%d %H:%M:%S")
BASE_MS = (calendar.timegm(_t) - 8 * 3600) * 1000   # 起始日 00:00（东八区）

ROWS_PER_DAY = max(SESSION_EVENTS, ROWS // DAYS)
ROWS = ROWS_PER_DAY * DAYS                     # 归整，保证每天行数一致

# 每天会话数、会话起始步长（铺满当天，留出 19 分钟余量避免越到次日）
SESS_PER_DAY = max(1, ROWS_PER_DAY // SESSION_EVENTS)
SESS_STEP_MS = max(1, DAY_MS // SESS_PER_DAY)
SESS_SPAN_MS = max(1, DAY_MS - SESSION_EVENTS * EVENT_STEP_MS)

spark = (SparkSession.builder
         .appName("gen_offline_ods_%s_%dx%d" % (START_DT, DAYS, ROWS_PER_DAY))
         .config("spark.sql.shuffle.partitions", "64")   # v7 多了 slots join，分区加倍防 OOM
         .config("spark.sql.orc.compression.codec", "snappy")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

def pmod(col, n):
    """正模。Spark 的 % 对有符号被除数返回负数 —— xxhash64 就会返回负 long，
    直接 % 95 会得到 [-94, 94] 而不是 [0, 95)，后续的分档判断全部错位。
    这里统一做 (x % n + n) % n，等价于 SQL 的 pmod。"""
    return (col % n + n) % n

c = F.col("id")
dt_idx = F.floor(c / ROWS_PER_DAY)          # 第几天 0..DAYS-1
local = c % ROWS_PER_DAY                    # 当天内的行号

sess = F.floor(local / SESSION_EVENTS)      # 当天内会话号
seq = local % SESSION_EVENTS                # 会话内序号 0..19
seg = local % 4                             # 一首歌内的位置 0..3
song_no = F.floor((local % SESSION_EVENTS) / 4)   # 会话内第几首歌 0..4


# ── 用户活跃度：生命周期模型（v7 修「多 cohort 留存恒 100%」）─────────────
# v6 的三层滑动窗口解决了「等差衰减」，但留下一个死结：窗口单调右移时，
# 新用户只能从右端进入，而右端在之后几天一直落在窗口内 —— 第 2 天起的
# cohort 留存恒为 100%。数学上没有中间态：要么一天滑出整个窗口（留存 0），
# 要么新用户永远留存。
#
# 改成「出生-死亡」模型：每个用户有出生日 b 和寿命 L，只在 [b, b+L) 内活跃。
#   cohort 的 day_n 留存 = S(n) = P(L > n)，只由 L 的分布决定，与 b 无关
#   → 所有 cohort 曲线完全一致，且必然衰减。
#
# 三个必须算清楚的约束（都是本地复算撞出来的）：
#   ① 候选数必须贴着 5 万。按「剩余寿命」截断会系统性留下健康用户：
#      候选 6.1 万砍到 5 万时 day1 被抬到 65%（真值 49%）；候选 5.15 万
#      时偏差只有 3%，曲线才准。控制手段是出生窗口跨度 ——
#      候选 ≈ 池大小 × E[L] / 跨度，E[L]=4.88、跨度 95 → 5.15 万。
#   ② day0 cohort 会偏高（80% vs 50%）。b<0 的人在数据窗口前就出生了，
#      SQL 的 MIN(dt) 把他们全归到首日 cohort，而他们的剩余寿命是长度
#      偏倚的。真实业务的 D0 cohort 本来就偏高（含存量用户）。
#   ③ 池必须扩到 100 万。10 万池 + 恒定 5 万 DAU 下，长度偏倚抽样给
#      day1 = 1 - 1/E[L]，而 DAU/池 = E[L]/30 —— 要 50% 的 DAU 就得
#      E[L]=15 天，day1 必然是 93%。两个条件在 10 万池下互斥。
USER_CNT = 1000000          # u_ + 6 位正好到 999999
BIRTH_MIN = -65             # 出生窗口 [-65, 30)，跨度 95 天
LIFE_VALUES = [1, 2, 3, 4, 5, 6, 7, 31]
LIFE_CUM = [0.51, 0.66, 0.76, 0.83, 0.87, 0.89, 0.90, 1.0]   # E[L] = 4.88

_h = spark.range(0, USER_CNT).select(F.col("id").alias("user_idx"))
_h = _h.withColumn(
    "birth", F.lit(BIRTH_MIN) + pmod(F.xxhash64(F.col("user_idx"), F.lit(0)),
                                     F.lit(DAYS - BIRTH_MIN)).cast("int"))
_h = _h.withColumn(
    "_hu", pmod(F.xxhash64(F.col("user_idx"), F.lit(1)), F.lit(10000)).cast("double") / 10000.0)

_life = None
for _v, _c in zip(LIFE_VALUES, LIFE_CUM):
    _life = F.when(F.col("_hu") < _c, _v) if _life is None else _life.when(F.col("_hu") < _c, _v)
_h = _h.withColumn("life", _life)

# 展开成「用户 × 活跃日」：100 万行 × 平均 4.88 天 ≈ 490 万行，裁掉窗口外的
_act = (_h.select("user_idx", "birth", "life",
                  F.explode(F.sequence(F.col("birth"),
                                       F.col("birth") + F.col("life") - 1)).alias("dt_idx"))
          .filter((F.col("dt_idx") >= 0) & (F.col("dt_idx") < DAYS)))

# 每天按「剩余寿命」降序取前 SESS_PER_DAY 个 → 当天槽位
_w = Window.partitionBy("dt_idx").orderBy(
    (F.col("birth") + F.col("life") - F.col("dt_idx")).desc(), F.col("user_idx"))
_slots = (_act.withColumn("sess", F.row_number().over(_w) - 1)
             .filter(F.col("sess") < SESS_PER_DAY)
             .select(F.col("dt_idx").cast("int").alias("dt_idx"),
                     F.col("sess").cast("int").alias("sess"),
                     F.col("user_idx")))

user_idx = F.col("user_idx")

# 设备跟着用户走（同一部手机），而不是每天随机换一台
dev_idx = pmod(user_idx * F.lit(7919) + F.lit(17), F.lit(120000))


# ── 歌曲 ID（v6 修「曲风与完播率伪相关」）────────────────────────────────
# 旧写法 song_num = ((dt_idx*50000 + sess)*5 + song_no) % 5000：
#   dt_idx*50000*5 = 250000 是 5000 的整数倍 ⇒ dt_idx 项被完全吃掉，
#   等价于 song_num = (sess*5 + song_no) % 5000，30 天的歌曲分布完全相同。
#   而 dim_song 的 genre = song_id % 20，于是 genre 只由 (5*sess + song_no) % 20
#   决定；is_complete（决定 play_end / skip）用 (sess//1000*3 + seq*7) % 10，
#   也依赖 song_no —— 两条链路共享 song_no，曲风与完播率就出现伪相关：
#   实测 流行 46.4% / 摇滚 49.1% / 民谣 53.9% / R&B 47.9%，跨度 7.5 个点。
#
# 注意：这里**不能**用「combo 乘法哈希取高位」。试过，反而更差 ——
#   combo 是连续整数，combo*C mod 2^32 再右移对连续输入仍是仿射的（增量恒定），
#   实测同一会话 5 首歌的 song_num 恒差 2460（20 的倍数）⇒ 曲风完全一样。
#   乘法哈希只在「输入本身随机」时有效，对连续输入打不散。
#
# 正确做法：让三个变量落在互不干扰的偏移上，且「曲风」与「完播判定」取不同的位段：
#   sess*5       → 周期 1000，只依赖 sess mod 1000（低位）
#   song_no*1007 → 1007 mod 20 = 7，同一会话 5 首歌的曲风错开 7 个槽位
#   dt_idx*7919  → 每天整体平移 2919，日榜不同
#   而 is_complete 用的是 sess//1000（高位）—— 与 sess mod 1000 正交。
SONG_CNT = 1486          # 与 data/dim/dim_song.csv 的真实歌曲数一致
song_num = pmod(
    sess * F.lit(5) + song_no * F.lit(1007) + dt_idx * F.lit(7919), F.lit(SONG_CNT))

# 真实歌曲时长：1486 行的广播表，对 3000 万行主表是 map-side join，几乎零开销。
# 为什么必须用真实值：duration 是 progress 的分母，编一个假时长出来，
# 「平均播放进度」「完播率」就全是假的。
# BOM 同样要处理 —— 按「第一列」重命名，不写死列名。
_song_dim = spark.read.option("header", True).option("encoding", "UTF-8") \
                 .csv("hdfs://WWC160:9820/data/dim/dim_song.csv")
_song_dim = _song_dim.withColumnRenamed(_song_dim.columns[0], "song_id")
_song_dim = _song_dim.select(
    (F.col("song_id").substr(2, 6).cast("int") - 1).alias("song_num"),   # S000001 → 0
    F.col("duration").cast("int").alias("dur_real"))

# 会话起始时刻（当天内铺开，不越界到次日）
sess_start = (sess * SESS_STEP_MS) % SESS_SPAN_MS
evt_ms = F.lit(BASE_MS) + dt_idx * DAY_MS + sess_start + seq * EVENT_STEP_MS

# 歌曲时长用真实值（join 进来的 dur_real），不再用公式编
dur = F.col("dur_real")

# ── 互动类型选择索引（修第 4 个取模耦合 bug）─────────────────────────────
# 原来写 pick([...], local)，但 seg = local % 4 已经把 local 的低 2 位固定住了：
#   seg==2 → local%4 恒为 2 → pick 内部 (local%4+1) 恒为 3 → 永远取数组第 3 个
#   seg==3 → 恒为 4 → 永远取第 4 个
# 结果 share 独占 25%、search 独占 25%、collect 和 page_view 恒为 0。
# 这和 v3「20 个用户一个会话」、v4/v5「song 覆盖不全」是同一个病根：
# **用已被约束固定住的量再去做取模索引，必然退化成一个常量。**
# 改用 sess + song_no：跨会话、跨歌曲都变化，且 %4 均匀。
pair_idx = sess + song_no

# ── 播放进度的抖动（v6 修「曲风完播率的残差伪相关」）─────────────────────
# 原来写 local % 30：local = sess*20 + seq，而 seq = song_no*4 + seg。
# 于是 k 的取值只依赖 (sess mod 3, song_no) —— 每个 song_no 对应一组固定的
# 奇数 k（{1,11,21} / {5,15,25} / …），均值从 11 漂到 19。
# progress = 1 - k/dur，k 均值大的 song_no 达标率就低；而 genre 也依赖 song_no，
# 残差伪相关就是这么漏进来的（改掉 dur 后还剩 3.6 个点）。
# 改用 sess 的独立哈希：与 song_no 正交，也与 genre 用的 sess mod 1000 无关。
# v8：抖动改成「按时长取比例」，修「完播率随歌曲时长漂移」。
# 原来写固定 0~28 秒，progress = (dur - jitter)/dur，要 >= 0.9 必须 jitter <= dur/10。
# 时长还是公式值 120~298 时，这只让 play_end 的达标率从 0.45 漂到 1.0（实测 0.7373），
# 看起来像个业务数字；换成真实时长 47~776 后，各桶达标率变成
#   0.28 / 0.60 / 0.88 / 1.00 / 1.00 —— 完播率直接由「这首歌多长」决定，
# 和用户行为毫无关系，是纯伪相关。
# 改法：progress = 1 - k%（k ∈ [0,14]），任何时长下分布一致，
# 阈值 0.9 只在 k <= 10 时命中 ⇒ 11/15 = 73.3% 的 play_end 算完播。
_jitter = (dur * pmod(sess * F.lit(7919) + dt_idx * F.lit(104729), F.lit(15))
           / F.lit(100)).cast("int")


# ── v5：判定必须跨过「1000 周期」（修 v4 的覆盖缺陷）────────────────────
# 演进过程（三次踩坑，每次都是"不报错但样本不全"）：
#   v3  seg==1 且 local%10<7：gcd(4,10)=2，与 seg 耦合 → song_no 只有 {0,1,3}，覆盖 3000
#   v4  引入 sess：(local*31 + sess*17)%10 → 仍与 song_no 的取模基共享低位，
#       覆盖 3500。原因是 song_num=(sess*5+song_no)%5000，对每个 song_num 其
#       k=song_num//5 固定，候选 sess = k+1000m (m=0..49)，而
#       (k+1000m)%10 = k%10 —— m 完全被 1000（10 的倍数）吃掉，50 个 sess 判定全同,
#       只要该 song_no 不在命中子集，这首歌 50 次全 skip，永远没有 play_end。
#   v5  把 sess//1000 引入判定：同一首歌的 50 个候选日/批次会得到不同结果，
#       每首歌期望 70% × 50 = 35 次 play_end，覆盖不再有结构性空洞。
# 判据：play_end 覆盖的 song 数 = 5000（而不是 3000 / 3500）。
sess_hi = F.floor(sess / 1000)
is_complete = ((sess_hi * 3 + (local % 20) * 7) % 10) < 7


def pick(values, idx):
    """按整数下标取字符串常量（idx 为 Column），等价于 array()[i] 但更可读"""
    return F.element_at(F.array(*[F.lit(x) for x in values]),
                        (idx % len(values) + 1).cast("int"))


dt_series = F.date_format(
    F.date_add(F.to_date(F.lit(START_DT)), dt_idx.cast("int")), "yyyy-MM-dd")

_rows = (spark.range(0, ROWS)
         .withColumn("dt_idx", dt_idx.cast("int"))
         .withColumn("sess", sess.cast("int"))
         .join(_slots, ["dt_idx", "sess"], "inner")
         .withColumn("song_num", song_num)
         .join(F.broadcast(_song_dim), ["song_num"], "left"))

df = _rows.select(
    F.concat(F.lit("e_"), F.lpad(c.cast("string"), 12, "0")).alias("event_id"),

    # 事件类型按 seg 分层：保证「起播 → 结束/跳过 → 互动 → 浏览」的真实节奏
    F.when(seg == 0, F.lit("play_start"))
     .when(seg == 1, F.when(is_complete, F.lit("play_end")).otherwise(F.lit("skip")))
     .when(seg == 2, pick(["search", "collect", "share", "page_view"], pair_idx))
     .otherwise(pick(["collect", "share", "page_view", "search"], pair_idx))
     .alias("event_name"),

    evt_ms.cast("string").alias("event_time"),
    (evt_ms + (local % 900)).cast("string").alias("server_time"),

    F.concat(F.lit("u_"), F.lpad(user_idx.cast("string"), 6, "0")).alias("user_id"),
    F.concat(F.lit("d_"), F.lpad(dev_idx.cast("string"), 6, "0")).alias("device_id"),

    F.when(user_idx % 10 < 8, "true").otherwise("false").alias("is_login"),
    F.when(user_idx % 10 == 0, "true").otherwise("false").alias("is_vip"),

    pick(["8.1.0", "8.2.0", "8.3.1", "9.0.0"], dev_idx).alias("app_version"),
    pick(["android", "android", "android", "ios", "ios", "web", "car"], dev_idx).alias("os"),
    pick(["13", "14", "12", "16"], dev_idx).alias("os_version"),
    pick(["MI 13", "iPhone 14", "Redmi Note 12", "Pixel 7", "vivo X90", "HUAWEI P60"],
         dev_idx).alias("device_model"),
    pick(["wifi", "wifi", "4g", "4g", "5g", "3g"], dev_idx).alias("network"),
    pick(["中国移动", "中国联通", "中国电信"], dev_idx).alias("carrier"),

    # Spark SQL 的 / 是浮点除法（返回 double），必须 floor 后再转字符串，
    # 否则会拼出 "10.12.0.345.0.67" 这种脏 IP
    F.concat(F.lit("10."), (dev_idx % 255).cast("string"), F.lit("."),
             (F.floor(dev_idx / 255) % 255).cast("string"), F.lit("."),
             (F.floor(dev_idx / 65025) % 255).cast("string")).alias("ip"),

    pick(["江西省", "广东省", "浙江省", "江苏省", "四川省"], user_idx).alias("province"),
    pick(["南昌市", "深圳市", "杭州市", "南京市", "成都市"], user_idx).alias("city"),

    F.concat(F.lit("s_"), F.lpad(sess.cast("string"), 8, "0")).alias("session_id"),
    seq.cast("string").alias("session_seq"),

    F.when(local % 100 < 92, "true").otherwise("false").alias("is_foreground"),
    pick(["推荐", "搜索", "歌单", "电台", "排行榜"], local).alias("channel"),
    pick(["home", "player", "search", "playlist", "rank"], local).alias("page_id"),

    # song_id 与 duration 绑定到具体歌曲：同一首歌的 play_start / play_end 拿到同一个 id，
    # 完播率才能算
    F.concat(
        F.lit('{"song_id":"sg_'), F.lpad(F.col("song_num").cast("string"), 5, "0"),
        F.lit('","duration":'), dur.cast("string"),
        F.lit(',"position":'),
        # 与 event_name 用同一个 is_complete 判定：play_end 才给接近末尾的 position，
        # skip 给很小的 position（跳过的语义）。两处必须一致，否则会出现
        # 「skip 事件却播到了 95%」这种自相矛盾的记录。
        F.when(seg == 1,
               F.when(is_complete, dur - _jitter).otherwise(_jitter))
         .otherwise(pmod(local * 13, dur)).cast("string"),
        F.lit("}"),
    ).alias("properties"),

    F.concat(dt_series, F.lit(" 03:00:00")).alias("etl_time"),

    dt_series.alias("dt"),      # 分区列，partitionBy 会把它从数据文件里剔除
)

print("== 目标：%s/dt=*  天数：%d  每天：%d 行（%d 会话）  总行数：%d =="
      % (BASE_PATH, DAYS, ROWS_PER_DAY, SESS_PER_DAY, ROWS))
print("== 生命周期模型：池 %d 人  出生窗口 [%d, %d)  寿命 E[L]=%.2f  候选≈%d =="
      % (USER_CNT, BIRTH_MIN, DAYS, sum(v * (c - p) for v, c, p in
         zip(LIFE_VALUES, LIFE_CUM, [0] + LIFE_CUM[:-1])),
         USER_CNT * 4.88 / (DAYS - BIRTH_MIN)))

if DAYS == 1:
    # 单天：沿用 v2 的行为，直接写 dt=YYYY-MM-DD 目录
    df.drop("dt").write.mode("overwrite").orc("%s/dt=%s" % (BASE_PATH, START_DT))
else:
    # 多天：先按 dt 重分布（每个分区一个 task），再 partitionBy 写出。
    # 不 repartition 的话，每个 shuffle task 都要向 30 个目录同时写 → 文件数爆炸。
    (df.repartition(DAYS, F.col("dt"))
       .write.mode("overwrite").partitionBy("dt").orc(BASE_PATH))

print("== 写入完成 ==")
spark.stop()
