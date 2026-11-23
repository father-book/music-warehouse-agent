#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
gen_events.py —— 音乐 App 埋点生成器（统一入口，26 类事件，双 sink）

═══════════════════════════════════════════════════════════════════════════════
为什么把两个脚本合成一个
═══════════════════════════════════════════════════════════════════════════════
改造前：`gen_music_data.py` 管实时，`gen_offline_ods.py` 管离线，两边各有一套
「事件权重 / 歌曲热度 / 用户活跃度」常量。v6 那次的「曲风与完播率伪相关」，
根因就是改离线参数时忘了同步实时那边 —— 两边分布悄悄分叉，但都不报错。

现在：分布参数全部来自 config/event_weights.json，同一个函数产出同一份事件流，
`--sink` 只决定最后写到哪。口径一致不再靠人工对齐，而是结构上做不到不一致。

  sink=hdfs   历史重造。30 天一次跑完，直接写 ORC 到 ODS 分区目录。
  sink=kafka  每日 T+1 增量。发到 music_events，由 Flume 落 landing，
              再由 spark_landing_to_ods.py 转 ORC。

v10（11-07 晚）：本脚本正式成为**唯一造数器**，实时 Flink 与离线 Flume
消费同一个 topic 里的同一批消息（docs/17 §3.2）。
  · properties 同时带「离线键」与「实时 job_b 键」，两侧各取所需：
    离线 03/04/11 号脚本读 result_cnt / reason / channel / page，
    实时 job_b 读 result_count / refund_reason / share_channel / page_id …
  · 脏数据只在写 Kafka 时注入一次，不再由某个消费者各自造。
  实时侧 gen_music_data.py 退居历史参考，不再作为数据源。

═══════════════════════════════════════════════════════════════════════════════
保留的三个已验证模型（改这个脚本时不要动）
═══════════════════════════════════════════════════════════════════════════════
① 出生-死亡生命周期（v7）：用户有出生日 b 和寿命 L，只在 [b, b+L) 活跃。
   cohort 的 day_n 留存 = S(n) = P(L>n)，只由 L 决定 → 曲线必然衰减。
   【约束】候选数必须贴着 SESS_PER_DAY，池必须 100 万，E[L]=4.88。
② 真实曲库（v8）：时长从 dim_song.csv 读，不是公式编的 —— duration 是
   progress 的分母，编假时长「平均播放进度」「完播率」全是假的。
③ 进度抖动按时长取比例（v8）：progress = 1 - k%，k ∈ [0,14]。
   阈值 0.9 只在 k<=10 命中 ⇒ 11/15 = 73.3% 的 play_end 算完播，
   且不随歌曲真实时长漂移。

═══════════════════════════════════════════════════════════════════════════════
26 类事件的生成策略 —— 两条路
═══════════════════════════════════════════════════════════════════════════════
A. 播放域（play_start / play_end / skip）：**按歌曲循环，不按权重条数**
   每会话 5 首歌，每首必发 play_start，再按 70% 发 play_end、30% 发 skip。
   为什么不用权重直接算条数：权重里 5 / 3.5 / 1.5 的和正好是 5，两者数学等价，
   但按歌曲循环能保证「同一首歌的 play_start 与 play_end 共享 song_id」，
   完播率才算得出来。按类型独立掷骰子会出现「3 条起播配 4 条结束」。

B. 其余 23 类：按「每会话期望条数」生成
   count = floor(w) + bernoulli(frac)，哈希取自 (sess_key, type_idx)，确定性。
   不用 Python UDF —— 100 万会话逐行调 Python 会慢到不可用；
   纯 Spark SQL 表达式 + explode 可以在两分钟内跑完 30 天。

═══════════════════════════════════════════════════════════════════════════════
歌曲热度 Zipf（α=0.6）+ 打散 + 歌曲级完播倾向（v9）
═══════════════════════════════════════════════════════════════════════════════
旧版用均匀取模，1486 首歌播放量几乎一样 —— 歌曲排行榜做出来前 100 名差不到
2%，一眼假。改 Zipf 后 Top1/Top20 ≈ 20^0.6 ≈ 6 倍，接近真实热歌榜。
实现方式：driver 端预先算好逆 CDF 查表（10000 桶），broadcast 到各 executor
按 `u = xxhash64(sess_key, song_no) % 10000` 查表。查表而不是 UDF，
是为了让 3000 万行也能走向量化路径。

v9 修掉两个遗留问题：

① **热度打散**。dim_song.csv 里 song_id 按歌手连号分配，前 30 首全是同一歌手、
   同一曲风。若 Zipf 权重按列表顺序贴，头部概率全落在第一个歌手身上 ——
   这正是 docs/08 P2-28「榜单前十全周杰伦」在离线侧的复现。现在把「热度排名」
   通过一个固定种子的随机排列映射到 song_num，长尾分布作用在打散后的 ID 空间上。

② **D9：让 weighted / avg_song 真正分叉**。W1 只把采样改成 Zipf 是不够的：
   完播判定 `_k` 与歌曲完全独立，每首歌的完播率都约 73%，于是
   `SUM(complete)/SUM(start)` 与 `AVG(complete_rate)` 只差 0.002（实测）。
   现在给每首歌一个由「热度排名」决定的固有完播倾向：头部热歌更容易被随手
   切走（模拟热歌榜的高跳过率），长尾歌略容易听完。**偏置按 Zipf 播放量加权
   配平**，所以整体完播率仍是 51.3%、avg_progress 仍是 0.93，
   但加权口径与等权口径能拉开约 3.5 个百分点（31 天实测 3.52pp、单日 3.66pp；
   5.09pp 是乘 0.7 之前的原始完成倾向差，不是 play_start 口径）。
   注意：Hive 的 `complete_cnt` 用的是 `play_progress >= 0.9`，而
   `play_progress = 1 - k/100`，因此偏置必须作用在 `_k` 上，
   改 properties 里的 complete 标记是无效的。

③ **补齐 properties**。旧版 10 类事件写 `{}`，其余多为恒值。
   现在按 docs/19 §三 事件矩阵给 26 类事件完整私有属性，键名与
   `gen_dwd_domain_sql.py` 的 `get_json_object` 一一对应；取值全部走
   `hidx()` 哈希，避免旧版 `pick(values, _i)` 取模退化
   （search 的 _i 恒为 1 → keyword 恒「夜曲」）。

═══════════════════════════════════════════════════════════════════════════════
用法
═══════════════════════════════════════════════════════════════════════════════
  # 历史重造：30 天，直接写 ODS（约 20 分钟，3000 万行）
  spark-submit gen_events.py --start 2026-10-05 --days 30 --sink hdfs

  # 每日 T+1 增量：写 Kafka（100 万行/天）
  spark-submit gen_events.py --start 2026-11-08 --days 1 --sink kafka

  # 调试：mini profile + 少量行，10 秒内出结果
  spark-submit gen_events.py --start 2026-11-04 --days 1 --events 200000 \
      --profile mini --sink hdfs

参数：
  --start    起始日期 yyyy-MM-dd
  --days     天数，默认 1
  --events   每天总行数，默认 1000000
  --sink     hdfs | kafka，默认 hdfs
  --profile  full（26 类，未跳出会话 55.7 条）| mini（10 类，44.5 条）
  --config   共享权重 JSON 路径
  --topic    Kafka topic，默认 music_events
  --bounce-ratio  跳出会话占比，默认 0.22（v11 新增，见下方「跳出建模」）
  --dry-run  只跑校验不写盘/不发 Kafka（改完参数先 `--dry-run` 看数字）
"""
import argparse
import calendar
import json
import math
import os
import random
import sys
import time

from pyspark.sql import SparkSession, Window, functions as F, Column

# ── 参数 ────────────────────────────────────────────────────────────────────
_ap = argparse.ArgumentParser()
_ap.add_argument("--start", default="2026-11-03")
_ap.add_argument("--days", type=int, default=1)
_ap.add_argument("--events", type=int, default=1000000, help="每天总行数")
_ap.add_argument("--sink", default="hdfs", choices=["hdfs", "kafka"])
_ap.add_argument("--profile", default="full", choices=["full", "mini"])
_ap.add_argument("--config", default="")
_ap.add_argument("--topic", default="music_events")
_ap.add_argument("--recent-minutes", type=int, default=0,
                 help="把事件时间压进最近 N 分钟（实时演示/联调用）；0 = 按整天分布")
_ap.add_argument("--bounce-ratio", type=float, default=0.22,
                 help="跳出会话占比（只发 1 条 app_launch 就结束的会话），默认 0.22；"
                      "0 = 关闭（v11 之前的行为，跳出率恒 0%%）")
_ap.add_argument("--dry-run", action="store_true",
                 help="只跑校验、打印统计，不写 HDFS / 不发 Kafka")
# 兼容旧调用：把位置参数当作 --start / --days 吃进来
_known, _rest = _ap.parse_known_args()
if _rest:
    if len(_rest) >= 1 and _rest[0].startswith("20"):
        _known.start = _rest[0]
    if len(_rest) >= 2 and _rest[1].isdigit():
        _known.days = int(_rest[1])
ARGS = _known

START_DT = ARGS.start
DAYS = ARGS.days
ROWS_PER_DAY = ARGS.events
PROFILE = ARGS.profile
SINK = ARGS.sink
TOPIC = ARGS.topic
# --recent-minutes 的取值在 driver 端固定下来，广播进闭包；0 表示不启用
RECENT_MINUTES = max(0, ARGS.recent_minutes)
DRY_RUN = bool(ARGS.dry_run)
_NOW_MS = int(time.time() * 1000)


# ── event_id 的运行批次码 ───────────────────────────────────────────────────
# 【为什么必须加】
#   event_id 原来是 e_{dt_idx}{sess:08d}{type_idx}{_i:04d}，其中 sess 从 0 连续编号。
#   同一天跑第二次（比如先 --events 200000、再补一批 --events 20000）时，前面的会话号
#   会被重新走一遍 —— 两批「时间差 1~1.5 小时、内容完全不同」的事件拿到同一个 event_id。
#   后果不是多算而是【少算】：全局 COUNT(DISTINCT event_id) 把两次真实播放并成一条。
#   实测 2026-11-08：DWS 逐窗口合计 19,465 条，全局去重只剩 17,728 条，
#   于是 DWS/明细 = 109%（看着像重复，其实是分母被 ID 冲突压小了）。
#   真实埋点的 event_id 由客户端生成 UUID，天然不重复；这里用批次码补上同样的性质。
#   只影响新生成的数据，历史批次不受影响（ID 只是主键，下游无格式依赖）。
def _b36(n, width):
    digs = "0123456789abcdefghijklmnopqrstuvwxyz"
    out = ""
    while n > 0:
        out = digs[n % 36] + out
        n //= 36
    return out.rjust(width, "0")


RUN_TAG = _b36(int(time.time()) % (36 ** 6), 6)   # 6 位 36 进制，约 21 亿种取值

BASE_PATH = "hdfs://WWC160:9820/warehouse/music_dw/ods_music_event"
KAFKA_BOOTSTRAP = "WWC161:9092"
DIM_SONG_HDFS = "hdfs://WWC160:9820/data/dim/dim_song.csv"

DAY_MS = 86400000
# 会话内相邻事件的间隔（毫秒）。offset = (i * EVENT_STEP + TYPE_IDX*137) % 25min。
#
# 【为什么从 60 秒改成 5 秒】2026-11-08 实测：
#   60 秒时 impression（每会话 25 条）的 offset 能到 25 分钟，而 play_start
#   （每会话 5 条）只有 5 分钟。也就是说一个会话的时间跨度是 25 分钟，
#   但"播放类"事件全挤在开头 5 分钟里。为了让 25 分钟跨度的会话整体落在
#   「现在」之前，起点被迫推到 now−25min，于是 play_start/play_end 的最新
#   时间永远比现在早 19~20 分钟 —— 大屏「实时在线（近 5 分钟）」恒为 0，
#   热榜也刷不出新播放。这不是链路问题，是造数器的时间轴太长。
#   压到 5 秒后：会话跨度 = 25×5s + 3.4s ≈ 2.1 分钟，
#   播放事件最晚落在 now−1.7min，稳稳进得了「近 5 分钟」窗口。
#   真实音乐 App 的埋点本来就是秒级密集上报，5 秒比 60 秒更接近真实。
#   play_duration / song_duration 等**属性**仍是真实歌曲时长，不受本参数影响。
EVENT_STEP_MS = 5000
SESS_SPAN_WINDOW_MS = 25 * 60000     # 会话内事件铺在 25 分钟内，不越到次日


# ── 共享配置 ────────────────────────────────────────────────────────────────
def _find_config(p):
    """按顺序找权重文件：显式参数 → 脚本同级 ../config → 常见部署路径"""
    cands = []
    if p:
        cands.append(p)
    here = os.path.dirname(os.path.abspath(__file__))
    cands += [
        os.path.join(here, "..", "config", "event_weights.json"),
        os.path.join(here, "config", "event_weights.json"),
        "/root/config/event_weights.json",
        "./config/event_weights.json",
    ]
    for c in cands:
        if c and os.path.exists(c):
            return os.path.abspath(c)
    raise SystemExit("找不到权重文件 event_weights.json，试过：\n  " + "\n  ".join(cands))


CFG_PATH = _find_config(ARGS.config)
with open(CFG_PATH, encoding="utf-8") as _f:
    CFG = json.load(_f)

WEIGHTS = CFG["events"]
SONGS_PER_SESSION = int(CFG["song_per_session"])
ZIPF_ALPHA = float(CFG["zipf_alpha_song"])

SELECTED = list(WEIGHTS.keys()) if PROFILE == "full" else list(CFG["profiles"]["mini"])
EV_PER_SESSION = sum(WEIGHTS[t] for t in SELECTED)

# ── 跳出会话（v11 新增）─────────────────────────────────────────────────────
# 【为什么要有】
#   大屏「会话粘性」面板的跳出率一直是 0%，而且不是链路算错，是数据里根本
#   不存在「只触发 1 个事件的会话」—— 每个会话固定承载 55.7 条事件（5 首歌
#   的完整生命周期）。跳出率的定义就是 bounce = 事件数 == 1 的会话，分母里
#   没有这种样本，分子必然是 0。
#
#   真实世界里这个样本占比并不低：用户误触打开 App、启动后看一眼就走，
#   埋点上就是「1 条 app_launch，之后什么都没有」。所以这里显式建模：
#   按 sess_key 的哈希挑出 BOUNCE_RATIO 的会话，把它们降级成「只发 1 条
#   app_launch」，其余事件（播放 / 曝光 / 互动 / 商业化 / 页面）全部不发。
#
# 【为什么选 app_launch 做唯一事件，而不是 play_start 或 page_view】
#   ① 用 play_start：这些会话的起播进了分母、却没有对应的 play_end/skip，
#      完播率会被硬生生压低（51.3% → 48.1%），切歌率同样漂移。
#   ② 用 page_view：page_view 每会话本来 3 条，只发 1 条会让「页面浏览」
#      口径凭空缩水，日活侧的页面深度指标失真。
#   ③ 用 app_launch：app_launch 天然就是「每会话恰好 1 条」（权重 = 1），
#      降级后这条不变，于是【会话数 == app_launch 数】这个恒等式仍然成立，
#      而播放 / 曝光 / 收入 / UV 的所有比率口径一个都不受影响 —— 只是这些
#      会话本来就不该贡献任何播放，绝对量随之少掉 BOUNCE_RATIO 那一份。
#   唯一被改变的是「每会话事件数」：55.7 → 43.7。这是数学必然（均值里混进了
#   BOUNCE_RATIO 份的 1），不是 bug；口径见 docs/21 §13。
BOUNCE_RATIO = min(0.6, max(0.0, float(ARGS.bounce_ratio)))
BOUNCE_TYPE = "app_launch"
BOUNCE_EVENTS = 1                      # 跳出会话的事件条数（恒 1，否则不算跳出）
if BOUNCE_RATIO > 0 and BOUNCE_TYPE not in SELECTED:
    # profile 里没有 app_launch 就关掉跳出建模，避免造出「0 事件会话」
    print("== 警告：profile 不含 %s，跳出建模已关闭 ==" % BOUNCE_TYPE)
    BOUNCE_RATIO = 0.0

# 每会话事件数：EV_PER_SESSION 是「未跳出会话」的值，均值要把跳出那部分折进来。
# SESS_PER_DAY 必须按均值反推，否则 --events 这个契约（每天总行数）会凭空少 22%。
EV_PER_SESSION_EFF = EV_PER_SESSION * (1 - BOUNCE_RATIO) + BOUNCE_EVENTS * BOUNCE_RATIO

# 类型序号：既做 event_id 的一部分，也做哈希盐 —— 不同事件类型的掷骰结果必须独立
TYPE_IDX = {t: i for i, t in enumerate(SELECTED)}

PLAY_TYPES = {"play_start", "play_end", "skip", "pause", "resume", "seek", "play_error"}
REC_TYPES = {"impression", "click"}
AD_TYPES = {"ad_impression", "ad_click"}
ORDER_TYPES = {"vip_subscribe", "order_pay", "order_refund"}
SEARCH_TYPES = {"search", "search_click"}
INTERACTION_TYPES = {"collect", "like", "share", "comment", "download",
                     "follow_artist", "playlist_add"}
PAGE_TYPES = {"page_view"}
SESSION_TYPES = {"app_launch", "app_exit"}

# ── D9：歌曲级完播倾向（按 Zipf 播放量配平，详见文件头 ②）────────────────────
# head=20 首占 10000 桶里的 14.42%（实测 _zipf_table(1486, 0.6)）。
# 头部 _k 抬高 5 → P(k<=10)=6/15=40%；长尾按 84.25% 概率 _k 降 1 → 80%，
# 其余保持 73.33%。加权 = 0.1442×0.40 + 0.8558×(0.8425×0.80+0.1575×0.7333)
#      = 0.7333 ✅ 与原模型一致，故整体完播率仍 0.7×0.7333 = 51.33%。
HEAD_RANK = 20
HEAD_K_SHIFT = 5
TAIL_K_SHIFT_P = 8425        # 千分比：长尾歌里 84.25% 拿到 -1 的偏置
ZIPF_SHUFFLE_SEED = 20261107

# 播放域走「按歌曲循环」，剩下的走「按条数」
LOOP_TYPES = {"play_start", "play_end", "skip"}
COUNT_TYPES = [t for t in SELECTED if t not in LOOP_TYPES]

SESS_PER_DAY = max(1, int(ROWS_PER_DAY / EV_PER_SESSION_EFF))
SESS_STEP_MS = max(1, DAY_MS // SESS_PER_DAY)
SESS_SPAN_MS = max(1, DAY_MS - SESS_SPAN_WINDOW_MS)


# ── 生命周期模型常量（改动前先读 docs/16 的推导）────────────────────────────
USER_CNT = 1000000
# ── 与 gen_dim_tables.py 共用的两个常量（改了必须两边一起改）─────────────────
# USER_SPACE：行为池 100 万 uid 经 pmod(uid, USER_SPACE)+1 塌缩成真实 user_id 空间。
#   这个空间必须落在维表 1..1,000,000 之内，否则事件里的用户 JOIN dim_user 会落空。
USER_SPACE = 105000
# REG_N：地域桶数量。维表与事实表用同一个桶数、同一张权重表、同一个算式
#   (un % REG_N)，同一 user_id 才能在两处落到同一个省。
REG_N = 1000
E_LIFE = 4.88                      # LIFE_CUM 推导出的期望寿命
LIFE_VALUES = [1, 2, 3, 4, 5, 6, 7, 31]
LIFE_CUM = [0.51, 0.66, 0.76, 0.83, 0.87, 0.89, 0.90, 1.0]

# 出生窗口跨度必须让「每天候选活跃用户数 ≈ 1.03 × SESS_PER_DAY」。
# 推导：总用户日 = USER_CNT × E[L]；摊到 span 天上，每天候选 = USER_CNT×E[L]/span。
# v8 时每会话 20 条、每天 5 万会话，span=95 正好；现在每会话 55.7 条、
# 每天只有 1.8 万会话，span 必须放大到 264 左右。
# 为什么乘 1.03 而不是 1.00：候选数必须**略多于**槽位。候选恰好等于槽位时，
# 约一半的日期会因为方差而凑不满（那些天等于没截断，留存曲线左右抖动）；
# 候选明显多于槽位时，按「剩余寿命降序」截断又会系统性留下健康用户
# —— v6 那次候选 6.1 万砍到 5 万，day1 留存被抬到 65%（真值 49%）。
# 5.15 万 / 5 万 = 1.03 这个比例是实测出来的最优区间，偏差只剩 3%。
BIRTH_SPAN = max(DAYS, int(round(USER_CNT * E_LIFE / (SESS_PER_DAY * 1.03))))
BIRTH_MIN = DAYS - BIRTH_SPAN


_t = time.strptime(START_DT + " 00:00:00", "%Y-%m-%d %H:%M:%S")
BASE_MS = (calendar.timegm(_t) - 8 * 3600) * 1000


spark = (SparkSession.builder
         .appName("gen_events_%s_%dd_%s" % (START_DT, DAYS, PROFILE))
         .config("spark.sql.shuffle.partitions", "64")
         .config("spark.sql.orc.compression.codec", "snappy")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")


def pmod(col, n):
    """正模。Spark 的 % 对有符号被除数返回负数 —— xxhash64 会返回负 long，
    直接 % 95 会得到 [-94, 94]，后续分档判断全部错位。"""
    return (col % n + n) % n


def pick(values, idx):
    """按整数下标取字符串常量（idx 为 Column）"""
    return F.element_at(F.array(*[F.lit(x) for x in values]),
                        (idx % len(values) + 1).cast("int"))


# ── 地域维度：全国 31 个省级行政区（不含港澳台）──────────────────────────────
# 旧版只硬编码了 5 个省（江西/广东/浙江/江苏/四川）且均匀分布，地图上其余 26 个
# 省级行政区**永远是空白**，一眼就看得出数据是假的。
# 现在按常住人口加权（权重取 2020 七普，单位万人）：广东/山东/河南/江苏/四川靠前，
# 西藏/青海/宁夏垫底，与真实播放量的地域分布基本一致。
# 城市必须跟着省份走 —— 省份和城市各自独立随机会出现「海南省 / 哈尔滨市」。
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


def _region_buckets(n_buckets=1000):
    """把 31 个省级行政区按人口权重展开成 0..999 的桶表。

    用广播 join 而不是 31 个 when 分支：分支多了执行计划会膨胀到
    Spark 打印 Truncated the string representation of a plan，而且每行都要走一遍
    长 case 链。桶表只有 1000 行，broadcast 之后是 map-side join。
    """
    total = float(sum(r[2] for r in _REGIONS))
    rows = []
    for name, cities, w in _REGIONS:
        cnt = max(1, int(round(w * n_buckets / total)))
        for j in range(cnt):
            if len(rows) >= n_buckets:
                break
            rows.append((len(rows), name, cities[j % len(cities)]))
    # 四舍五入后总数可能不足 1000 桶；缺口补给权重最大的省，把误差放在相对偏差最小的位置
    top = _REGIONS[0]
    while len(rows) < n_buckets:
        i = len(rows)
        rows.append((i, top[0], top[1][i % len(top[1])]))
    return rows[:n_buckets]


_region_df = spark.createDataFrame(_region_buckets(), ["rb", "province", "city"])


def hidx(salt, n=1000):
    """均匀哈希下标 0..n-1。

    旧版直接把 `_i` 传给 pick()，而低频事件的 `_i` 几乎恒为 1，
    于是 `keyword` 永远落在常量数组第 2 个（「夜曲」）。所有取值一律走
    (sess_key, _i, salt) 的 xxhash64，salt 隔离不同字段，避免相关。
    """
    return pmod(F.xxhash64(F.col("sess_key"), F.col("_i"), F.lit(salt)), F.lit(n))


def jobj(*pairs):
    """拼合法 JSON 字符串。pairs = (key, value, is_string)。

    is_string=True 自动包双引号（值必须是不含引号/反斜杠的常量）；
    False 自动 CAST 成字符串写裸值。用 F.concat 纯原生表达式，无 UDF。
    """
    parts = [F.lit("{")]
    for i, (k, v, is_str) in enumerate(pairs):
        if i:
            parts.append(F.lit(","))
        parts.append(F.lit('"%s":' % k))
        val = v if isinstance(v, Column) else F.lit(v)
        if is_str:
            parts += [F.lit('"'), val, F.lit('"')]
        else:
            parts.append(val.cast("string"))
    parts.append(F.lit("}"))
    return F.concat(*parts)


# ── 会话骨架：每天 SESS_PER_DAY 个会话，每个会话绑一个用户 ──────────────────
_dt_idx = F.floor(F.col("id") / SESS_PER_DAY)
_sess = F.col("id") % SESS_PER_DAY

_h = spark.range(0, USER_CNT).select(F.col("id").alias("user_idx"))
_h = _h.withColumn("birth", F.lit(BIRTH_MIN) + pmod(F.xxhash64(F.col("user_idx"), F.lit(0)),
                                                   F.lit(DAYS - BIRTH_MIN)).cast("int"))
_h = _h.withColumn("_hu", pmod(F.xxhash64(F.col("user_idx"), F.lit(1)),
                               F.lit(10000)).cast("double") / 10000.0)

_life = None
for _v, _c in zip(LIFE_VALUES, LIFE_CUM):
    _life = F.when(F.col("_hu") < _c, _v) if _life is None else _life.when(F.col("_hu") < _c, _v)
_h = _h.withColumn("life", _life)

# 展开成「用户 × 活跃日」，裁掉窗口外的
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

# ── Zipf 歌曲热度查表 ───────────────────────────────────────────────────────
def _zipf_table(n, alpha, buckets=10000):
    """逆 CDF 查表：桶号 → (热度排名 hot_rank, 打散后的 song_num)。

    桶内取中点求逆，避免 0 号桶永远落第 1 名。hot_rank 是 Zipf 名次（越小越热），
    song_num 通过一个固定种子的随机排列映射 —— 热度不再与 song_id 的连号顺序
    （也就是歌手 / 曲风）绑定，见文件头 ①。
    """
    w = [1.0 / ((i + 1) ** alpha) for i in range(n)]
    tot = sum(w)
    cum, s = [], 0.0
    for x in w:
        s += x
        cum.append(s / tot)
    rows, j = [], 0
    for b in range(buckets):
        u = (b + 0.5) / buckets
        while j < n - 1 and u > cum[j]:
            j += 1
        rows.append((b, j))

    perm = list(range(n))
    random.Random(ZIPF_SHUFFLE_SEED).shuffle(perm)
    return [(b, j, perm[j]) for b, j in rows]


_zipf_rows = _zipf_table(1486, ZIPF_ALPHA)
_zipf = spark.createDataFrame(_zipf_rows, ["_u", "hot_rank", "song_num"])

# 真实歌曲时长（1486 行广播表，对主表是 map-side join）
_song_dim = (spark.read.option("header", True).option("encoding", "UTF-8").csv(DIM_SONG_HDFS)
             .withColumnRenamed("_c0", "song_id"))
_song_dim = _song_dim.select(
    (F.col("song_id").substr(2, 6).cast("int") - 1).alias("song_num"),
    F.col("duration").cast("int").alias("dur_real"))


# ── 会话级公共列（只算一次，26 类事件共用）──────────────────────────────────
def _base_frame():
    b = (spark.range(0, SESS_PER_DAY * DAYS)
         .withColumn("dt_idx", _dt_idx.cast("int"))
         .withColumn("sess", _sess.cast("int"))
         .join(_slots, ["dt_idx", "sess"], "inner")
         .withColumn("sess_key", F.col("dt_idx") * F.lit(1000000) + F.col("sess")))

    # 跳出会话标记：按 sess_key 哈希判定，与歌曲/完播/地域用的哈希盐互斥，
    # 保证「哪些会话跳出」和「这些会话播什么歌」统计独立。
    # 用 F.lit(43) 做盐是为了不和已有的 7/21/31/97/101 撞车。
    if BOUNCE_RATIO > 0:
        b = b.withColumn(
            "_bounce",
            pmod(F.xxhash64(F.col("sess_key"), F.lit(43)), F.lit(100000))
            < F.lit(int(round(BOUNCE_RATIO * 100000))))
    else:
        b = b.withColumn("_bounce", F.lit(False))

    uid = F.col("user_idx")
    # 设备空间原来只有 12 万，而用户池 100 万 —— 平均 8 个用户挤一台设备，
    # 于是「登录 UV」会大于「总 UV」（实测 4,279 vs 4,135）：
    # 用户数按 user_id 去重、设备数按 device_id 去重，压缩映射必然让前者更大。
    # 真实世界里是「一个用户多设备」（UV ≥ 登录用户），这里方向反了，一眼就是假的。
    # 改成 100 万空间后 7919 与 10^6 互质 → 映射是双射，一用户一设备（游客也各占一台）。
    dev = pmod(uid * F.lit(7919) + F.lit(17), F.lit(1000000))

    b = (b
         .withColumn("device_id", F.concat(F.lit("d_"), F.lpad(dev.cast("string"), 6, "0")))
         .withColumn("is_login", F.when(uid % 10 < 8, "true").otherwise("false"))
         # user_id 必须与维表 dim_user 同编号空间，否则「用户结构」永远算不出来。
         #   旧版事件侧写 U024966（大写 U + 6 位、从 1 起、10.5 万），
         #   维表写 u_000000（小写 u + 下划线 + 6 位、从 0 起、100 万）——
         #   两套 ID 一个都对不上，LEFT JOIN 全落空，面板恒为「暂无数据」。
         #   2026-11-09 统一成 U + 7 位，范围 1..1,000,000（7 位才能覆盖维表 100 万）。
         #
         # 「un」是塌缩后的用户编号，是本次修复的核心：
         #   行为模型用 100 万 uid 的池子（生命周期推导依赖它），
         #   写盘时 pmod(uid, USER_SPACE)+1 落到 10.5 万个真实 user_id。
         #   一个 un 对应约 9.5 个 uid，它们的 is_login/设备可能不同，
         #   但只要省份由 un 决定，同一 user_id 的地域就不会漂移。
         #
         # 未登录事件必须留空 user_id，否则「游客」这一栏恒 0：
         # is_login=false 是随机的 20%，但原来无论登录与否都填了 user_id，
         # 于是「游客 = 无 user_id 的设备」永远数不出人，登录 UV 也等于总 UV。
         .withColumn("un", pmod(uid, F.lit(USER_SPACE)) + 1)
         .withColumn("user_id", F.when(F.col("is_login") == "true",
                      F.concat(F.lit("U"), F.lpad(F.col("un").cast("string"), 7, "0")))
                     .otherwise(F.lit("")))
         .withColumn("is_vip", F.when(uid % 10 == 0, "true").otherwise("false"))
         .withColumn("app_version", pick(["8.1.0", "8.2.0", "8.3.1", "9.0.0"], dev))
         .withColumn("os", pick(["android", "android", "android", "ios", "ios", "web", "car"], dev))
         .withColumn("os_version", pick(["13", "14", "12", "16"], dev))
         .withColumn("device_model",
                     pick(["MI 13", "iPhone 14", "Redmi Note 12", "Pixel 7",
                           "vivo X90", "HUAWEI P60"], dev))
         .withColumn("network", pick(["wifi", "wifi", "4g", "4g", "5g", "3g"], dev))
         .withColumn("carrier", pick(["中国移动", "中国联通", "中国电信"], dev))
         # / 是浮点除法，必须 floor 后再转字符串，否则拼出 "10.12.0.345.0.67"
         .withColumn("ip", F.concat(
             F.lit("10."), (dev % 255).cast("string"), F.lit("."),
             (F.floor(dev / 255) % 255).cast("string"), F.lit("."),
             (F.floor(dev / 65025) % 255).cast("string")))
         # 会话 ID 必须同时带「第几天」和「批次码」，两者缺一不可：
         #   · 缺批次码：同一天跑两次，会话号重走一遍，两批不同时间的事件被当成
         #     同一个会话，人均事件数翻倍（实测 55.7 → 112.35），跳出率压成 0%。
         #   · 缺 dt_idx：sess 每天从 0 重新编号，35 天的 s_00000000 会全部撞成
         #     同一个会话。2026-11-09 重造前实测：114,450 个会话去重后只剩 3,270 个，
         #     跳出率 22% 被判成 0%，因为每个"会话"都挂着 35 天的 1,527 条事件。
         # event_id 一直带 dt_idx，所以没这个毛病；session_id 之前漏了。
         .withColumn("session_id", F.concat(F.lit("s_"),
                                           F.lpad(F.col("dt_idx").cast("string"), 3, "0"),
                                           F.lpad(F.col("sess").cast("string"), 8, "0"),
                                           F.lit(RUN_TAG))))

    # 地域由 un 定桶（不是 sess，也不能是 uid）：
    #   · 不能是 sess：同一用户换会话省份会漂移。
    #   · 不能是 uid：uid 是 100 万的池子，un 是塌缩后的 10.5 万 —— 一个 un 对应
    #     约 9.5 个 uid，按 uid 定桶会让同一 user_id 拿到多个省。
    #     2026-11-09 实测：11-08 当天 18 个 user_id 跨省，分省 DAU 之和 2,144
    #     比全站 2,054 多出 90，而任何查询都不会报错，只是数字对不上。
    #   · 必须与 gen_dim_tables.py 完全一致：同一个 REG_N、同一张权重表、同一个
    #     算式 (un % REG_N)，否则维表的省份和明细的省份是两回事。
    b = (b.withColumn("rb", pmod(F.col("un"), F.lit(REG_N)))
          .join(F.broadcast(_region_df), ["rb"], "left")
          .drop("rb"))

    # 会话起始时刻：当天内铺开，留出 25 分钟余量不越界到次日
    # 【必须 cast long】dt_idx 是 int，DAY_MS=86400000 也是 int。两个 int 相乘时
    # Spark 用 32 位整型运算，dt_idx>=25 时 dt_idx*DAY_MS 超过 2^31-1 直接溢出成负数：
    #   25*86400000 = 2,160,000,000 → −2,134,967,296（等价 −24.71 天）
    # 后果是第 26 天以后的事件时间整体倒退回上个月，而 dt 分区列（用 86400 秒算，
    # 34*86400 只有 293 万，不溢出）仍然正确 —— 于是「dt=10-30 的分区里装着 09-10
    # 的事件」，按小时/按天对账全部对不上，且不会有任何报错。
    # 2026-11-09 实测：35 天版本事件时间跨度报成 [−86,590, −15,031] 分钟（约 50 天），
    # 与 dt_idx=25 起的溢出值逐位吻合。cast("long") 后恢复正常。
    b = b.withColumn("sess_start_ms",
                     F.lit(BASE_MS) + F.col("dt_idx").cast("long") * F.lit(DAY_MS)
                     + (F.col("sess") * F.lit(SESS_STEP_MS)) % F.lit(SESS_SPAN_MS))

    # --recent-minutes N：把会话时间压进「最近 N 分钟」。
    # 为什么需要：实时链路按事件时间开窗，Flink 的 watermark 取分区内事件时间的
    # 最小值推进。整天铺开时，先到的"当天末尾"事件会把 watermark 一把推到 24:00，
    # 之后到达的白天事件全被判迟到丢弃 —— DWD 只剩最后几秒的数据，DWS/ADS 窗口
    # 也迟迟不关闭。演示/联调时用这个参数把事件时间收窄到当前窗口内即可。
    # 默认 0（关闭），历史重造仍按整天分布，不受影响。
    #
    # 【起点上界要扣掉会话自身跨度】会话内事件按 offset 铺开，如果 sess_start
    # 允许到 now，末尾事件会溢出到 now+5 分钟，被判 event_time_future 脏数据
    # （第一次跑就是 page_view 10,767 条全丢）。所以上界取 now − 会话跨度，
    # 保证「最新一条事件 = 此刻」，而不是留 25 分钟的虚假余量。
    if RECENT_MINUTES > 0:
        # 会话内事件的相对偏移上界：offset = (i*EVENT_STEP + TYPE_IDX*137) % 25min。
        # i 不是「歌数」—— impression 每会话 25 条，i 最大到 25，偏移接近 25 分钟；
        # play_start 只有 5 条。所以必须按**最大权重**算，否则上界算小、事件会溢出到未来。
        # （上一版按 song_per_session=5 算成 5.06 分钟，实测越界 3,063 条、超前 15.5 分钟。）
        _max_per_sess = int(math.ceil(max(WEIGHTS[t] for t in SELECTED)))
        _sess_span_ms = min(
            SESS_SPAN_WINDOW_MS,
            _max_per_sess * EVENT_STEP_MS + max(TYPE_IDX.values()) * 137)

        # 起点在 N 分钟里铺开、事件整体再向后延展一个会话跨度：
        #   sess_start ∈ [now−N−25min, now−25min)，event_ts ∈ [now−N−25min, now−ε]
        # 覆盖范围是 N+25 分钟（比 N 长，因为一个会话本身横跨 25 分钟），
        # 但**最新一条事件就是此刻**，大屏「实时在线 / 近 5 分钟」立刻有人。
        _span = RECENT_MINUTES * 60000 + _sess_span_ms
        _usable = max(1, RECENT_MINUTES * 60000)

        # 【2026-11-08 二次修：让最新事件紧贴当前时刻】
        # 上一版为了不越界，把起点整体前移了 25 分钟（SESS_SPAN_WINDOW_MS），
        # 结果最新事件只能到 now−4.7 分钟 —— 大屏「实时在线 / 近 5 分钟」正好卡在
        # 窗口边缘，灌完数要等几分钟才有人。这与"数据产生就要能看到"直接冲突。
        # 改成用**真实会话跨度**（上面算出来的 ~5 分钟）而不是 25 分钟硬留白：
        #   sess_start ∈ [now−span, now−span+usable)
        #   event_ts   ∈ [now−span, now]          ← 最新一条就是"此刻"
        # 跨度不变、会话内相对间隔不变（session_seq 语义不动），只是整体右移，
        # 把原来白白丢掉的 20 分钟还给了实时窗口。
        # 起点用「线性映射」而不是「乘步长取模」：
        # 取模的落点取决于 gcd(sess*STEP, usable)，实测只铺到区间前 4/5，
        # 最新事件停在 now−4.5min，「近 5 分钟」正好卡在边缘、时有时无。
        # 线性映射保证 sess 在 [0, SESS_PER_DAY) 上均匀落到 [0, usable)。
        b = b.withColumn(
            "sess_start_ms",
            F.lit(_NOW_MS - _span)
            + (F.col("sess").cast("double") / F.lit(float(max(1, SESS_PER_DAY)))
               * F.lit(float(_usable))).cast("long"))
        # 兜底：任何原因算超的，一律压到现在（宁可挤在同一毫秒，也不造未来事件）
        b = b.withColumn("sess_start_ms", F.least(F.col("sess_start_ms"), F.lit(_NOW_MS)))
    return b


base = _base_frame().cache()
base.count()      # 触发物化，26 次 union 都从这里读


# ── 事件类型 → 每会话条数（floor + bernoulli 小数部分）──────────────────────
def _count_expr(etype):
    w = WEIGHTS[etype]
    whole = int(w)
    frac = w - whole
    if frac <= 1e-9:
        return F.lit(whole)
    thr = int(round(frac * 1000))
    return (F.lit(whole) +
            F.when(pmod(F.xxhash64(F.col("sess_key"), F.lit(TYPE_IDX[etype]), F.lit(101)),
                        F.lit(1000)) < thr, 1).otherwise(0))


# ── 统一收口：把会话级列 + 事件级列拼成 24 列 ───────────────────────────────
# 目标 schema 必须和 music_dw.ods_music_event 完全一致（24 列，dt 是分区列）
def finalize(df, etype, family):
    dur = F.col("dur_real")
    # 进度抖动：按时长取比例，progress = 1 - k%，k ∈ [0,14]
    # v9：把「歌曲级完播倾向」作用在 k 上，而不是改 properties 里的 complete 标记 ——
    # Hive 05 号脚本的 complete_cnt 只看 play_progress >= 0.9（即 k <= 10）。
    _k0 = pmod(F.xxhash64(F.col("sess_key"), F.col("_i"), F.lit(31)), F.lit(15))
    _tail_plus = (pmod(F.xxhash64(F.col("song_num"), F.lit(97)), F.lit(10000))
                  < F.lit(TAIL_K_SHIFT_P))
    _k = (F.when(F.col("hot_rank") < HEAD_RANK,
                 F.least(_k0 + F.lit(HEAD_K_SHIFT), F.lit(14)))
           .when(_tail_plus, F.greatest(_k0 - F.lit(1), F.lit(0)))
           .otherwise(_k0))
    jitter = (dur * _k / F.lit(100)).cast("int")

    # song_id 必须与维表 dim_song 同一个编号空间，否则 job_b 的 join 全落空：
    # 歌名/歌手/曲风全成空串，大屏只能退回显示 ID，曲风一律「未知」。
    #
    # 编号空间以 data/dim/dim_song.csv 为准：S000001~S001486 连续无缺号。
    # _song_dim 里 song_num = substr(song_id,2,6) - 1（S000001 → 0），
    # 所以这里必须 +1 并用 6 位补零还原成 S000123 —— 不是 5 位、不是小写。
    # （v9 曾用 `sg_%05d` 这套自造 ID，与维表不通；因为当时没再灌过数所以一直没暴露。）
    sid = F.when(F.col("song_num").isNotNull(),
                 F.concat(F.lit("S"),
                          F.lpad((F.col("song_num") + F.lit(1)).cast("string"), 6, "0"))
                 ).otherwise(F.lit(""))

    offset = pmod(F.col("_i") * F.lit(EVENT_STEP_MS) + F.lit(TYPE_IDX[etype] * 137),
                  F.lit(SESS_SPAN_WINDOW_MS))
    evt_ms = F.col("sess_start_ms") + offset

    # ── 各域私有属性（docs/19 §三 事件矩阵）──────────────────────────────────
    # 键名与 gen_dwd_domain_sql.py / 03 / 04 号脚本的 get_json_object 一一对应；
    # 26 类事件全覆盖，取值全部走 hidx() 哈希，杜绝 {} 与恒值。
    # etype 是 Python 常量，分支在构建期定死，运行时没有 when 开销。
    _dur_s = F.coalesce(dur, F.lit(0)).cast("string")
    _bool_10 = F.when(_k <= 10, F.lit(1)).otherwise(F.lit(0))

    if etype == "play_end":
        # 03 号脚本只解析 song_id / duration / position；end_reason 写成与
        # DWD 派生口径一致（progress>=0.9 → natural，否则 exit），不引入第二套口径
        props = jobj(
            ("song_id", sid, True),
            ("duration", _dur_s, False),
            ("position", F.coalesce(dur - jitter, F.lit(0)), False),
            ("complete", _bool_10, False),
            ("end_reason", F.when(_k <= 10, F.lit("natural")).otherwise(F.lit("exit")), True),
            # ↓ v10：实时 job_b 兼容键。同一份 JSON 两个消费者，离线 03 号只读上面 5 个键。
            #   play_progress = 1 - k/100 与离线 complete_cnt (progress>=0.9) 同源。
            ("song_duration", _dur_s, False),
            ("play_duration", F.coalesce(dur - jitter, F.lit(0)), False),
            ("play_progress", F.round(F.lit(1.0) - _k.cast("double") / F.lit(100.0), 4), False))
    elif etype == "skip":
        props = jobj(
            ("song_id", sid, True),
            ("duration", _dur_s, False),
            ("position", F.coalesce(jitter, F.lit(0)), False),
            ("complete", 0, False),
            ("skip_type", pick(["manual", "auto"], hidx(24, 100)), True),
            ("played_duration", F.coalesce(jitter, F.lit(0)), False))
    elif etype == "play_start":
        # 04 号脚本的 playlist_id / quality / play_mode / song_index 原本恒 NULL
        props = jobj(
            ("song_id", sid, True),
            ("duration", _dur_s, False),
            ("position", 0, False),
            ("auto", F.when(hidx(21, 100) < 70, F.lit(1)).otherwise(F.lit(0)), False),
            ("source", pick(["recommend", "search", "playlist", "radio"], hidx(22, 100)), True),
            ("quality", pick(["标准", "高品", "无损"], hidx(23, 100)), True),
            ("playlist_id", F.concat(F.lit("pl_"),
                                     F.lpad(hidx(25, 300).cast("string"), 5, "0")), True),
            ("play_mode", pick(["顺序", "随机", "单曲循环"], hidx(26, 100)), True),
            ("song_index", hidx(27, 50) + 1, False))
    elif etype in PLAY_TYPES:          # pause / resume / seek / play_error
        pairs = [("song_id", sid, True),
                 ("position", pmod(hidx(28, 100000), F.coalesce(dur, F.lit(1))), False)]
        if etype == "resume":
            pairs.append(("pause_duration", hidx(29, 120) + 1, False))
        elif etype == "seek":
            pairs += [("from", hidx(30, 300), False),
                      ("to", hidx(31, 300), False),
                      ("direction", pick(["forward", "backward"], hidx(32, 100)), True)]
        elif etype == "play_error":
            _ec = pick(["timeout", "decode", "copyright", "cdn404"], hidx(33, 100))
            pairs += [("error_type", _ec, True),
                      ("error_code", _ec, True), ("error_msg", F.lit("播放失败"), True),
                      ("play_position", pmod(hidx(28, 100000), F.coalesce(dur, F.lit(1))), False)]
        props = jobj(*pairs)
    elif etype in REC_TYPES:           # impression / click
        props = jobj(
            ("song_id", sid, True),
            ("position", F.col("_i") - 1, False),
            ("is_click", 1 if etype == "click" else 0, False),
            ("scene", pick(["roam", "recommend", "search", "playlist"], hidx(34, 100)), True),
            ("target_type", pick(["song", "playlist", "artist", "banner"], hidx(35, 100)), True),
            ("target_id", F.concat(F.lit("t_"),
                                   F.lpad(hidx(36, 100000).cast("string"), 6, "0")), True),
            ("page", pick(["home", "player", "search", "playlist", "rank"], hidx(37, 100)), True))
    elif etype in AD_TYPES:
        _bp = hidx(61, 100)
        props = jobj(
            ("ad_id", F.concat(F.lit("ad_"), F.lpad(hidx(62, 20).cast("string"), 3, "0")), True),
            ("ad_type", pick(["banner", "splash", "feed"], hidx(63, 100)), True),
            ("position", hidx(64, 25), False),
            ("bid_price", F.when(_bp < 45, F.lit("0.15"))
                           .when(_bp < 80, F.lit("0.35"))
                           .when(_bp < 95, F.lit("0.80"))
                           .otherwise(F.lit("1.20")), False))
    elif etype in ORDER_TYPES:
        _oid = F.concat(F.lit("OD"), F.lpad(hidx(54, 1000000000).cast("string"), 12, "0"))
        _ch = pick(["微信支付", "支付宝", "Apple Pay", "余额"], hidx(55, 100))
        if etype == "vip_subscribe":
            _sv = hidx(51, 100)
            props = jobj(
                ("sku_id", F.when(_sv < 52, F.lit("vip_month"))
                            .when(_sv < 78, F.lit("vip_quarter")).otherwise(F.lit("vip_year")), True),
                ("amount", F.when(_sv < 52, F.lit("15.00"))
                            .when(_sv < 78, F.lit("40.00")).otherwise(F.lit("148.00")), False),
                ("pay_channel", _ch, True), ("order_id", _oid, True),
                ("plan_days", F.when(_sv < 52, F.lit(30))
                               .when(_sv < 78, F.lit(90)).otherwise(F.lit(365)), False))
        elif etype == "order_pay":
            _sp = hidx(52, 100)
            props = jobj(
                ("sku_id", F.when(_sp < 80, F.lit("song_single"))
                            .otherwise(F.lit("digital_album")), True),
                ("amount", F.when(_sp < 80, F.lit("3.00")).otherwise(F.lit("25.00")), False),
                ("pay_channel", _ch, True), ("order_id", _oid, True),
                ("plan_days", F.lit(0), False))
        else:                          # order_refund：sku 与金额必须同下标配对
            _sr = hidx(53, 5)
            props = jobj(
                ("sku_id", pick(["vip_month", "vip_quarter", "vip_year",
                                 "song_single", "digital_album"], _sr), True),
                ("amount", pick(["15.00", "40.00", "148.00", "3.00", "25.00"], _sr), False),
                ("pay_channel", _ch, True), ("order_id", _oid, True),
                ("reason", pick(["重复购买", "误操作", "内容不符预期", "其他"], hidx(56, 100)), True),
                ("plan_days", F.lit(0), False),
                ("refund_reason", pick(["重复购买", "误操作", "内容不符预期", "其他"],
                                       hidx(56, 100)), True))
    elif etype in SEARCH_TYPES:
        _kw = pick(["夜曲", "周杰伦", "华语流行", "纯音乐", "摇滚",
                    "晚风", "晴天", "稻香", "民谣", "失恋"], hidx(41, 10000))
        if etype == "search":
            # 8% 零结果 / 30% 1~15 / 42% 16~80 / 20% 81~300，与实时侧同分布
            _rc = hidx(42, 100)
            _cnt = (F.when(_rc < 8, F.lit(0))
                     .when(_rc < 38, hidx(43, 15) + 1)
                     .when(_rc < 80, hidx(44, 65) + 16)
                     .otherwise(hidx(45, 220) + 81))
            props = jobj(
                ("keyword", _kw, True),
                ("search_type", pick(["hot", "suggest", "input"], hidx(46, 100)), True),
                ("result_cnt", _cnt, False),
                ("result_count", _cnt, False), ("clicked_song_id", F.lit(""), True))
        else:
            props = jobj(
                ("keyword", _kw, True),
                ("result_type", pick(["song", "playlist", "artist"], hidx(47, 100)), True),
                # result_id 同样要落在 dim_song 的编号空间（见 sid 处的说明）：
                # hidx(48,1486) 是 0..1485 的序号，+1 后补 6 位 = S000001~S001486。
                ("result_id", F.concat(F.lit("S"),
                                       F.lpad((hidx(48, 1486) + 1).cast("string"), 6, "0")), True),
                ("position", hidx(49, 20), False))
    elif etype in INTERACTION_TYPES:
        if etype == "share":
            _ch2 = pick(["wechat", "qq", "weibo", "link"], hidx(36, 100))
            props = jobj(("song_id", sid, True), ("channel", _ch2, True),
                         ("share_channel", _ch2, True))
        elif etype == "comment":
            props = jobj(("song_id", sid, True), ("comment_len", hidx(37, 200) + 1, False))
        elif etype == "download":
            props = jobj(("song_id", sid, True),
                         ("quality", pick(["标准", "高品", "无损"], hidx(38, 100)), True))
        elif etype == "follow_artist":
            props = jobj(("artist_id", F.concat(F.lit("ar_"),
                          F.lpad(hidx(39, 50).cast("string"), 4, "0")), True),
                         ("action", pick(["follow", "unfollow"], hidx(40, 100)), True))
        elif etype == "playlist_add":
            props = jobj(("song_id", sid, True),
                         ("playlist_id", F.concat(F.lit("pl_"),
                          F.lpad(hidx(50, 300).cast("string"), 5, "0")), True))
        else:                          # collect / like
            _act = pick(["add", "remove"], hidx(35, 100))
            props = jobj(("song_id", sid, True), ("action", _act, True),
                         ("collect_type", F.when(_act == F.lit("remove"), F.lit("cancel"))
                                           .otherwise(F.lit("add")), True))
    elif etype in SESSION_TYPES:
        if etype == "app_launch":
            props = jobj(("launch_type", pick(["cold", "hot"], hidx(11, 100)), True),
                         ("source", pick(["icon", "push", "deeplink"], hidx(12, 100)), True),
                         ("app_version", pick(["8.1.0", "8.2.0", "8.3.1", "9.0.0"],
                                              hidx(13, 100)), True))
        else:
            props = jobj(("stay_duration", hidx(14, 600) + 1, False),
                         ("exit_type", pick(["back", "home", "kill"], hidx(15, 100)), True))
    elif etype in PAGE_TYPES:
        _pg = pick(["home", "player", "search", "playlist", "rank"], hidx(16, 100))
        _fp = pick(["home", "player", "search", "playlist", "rank"], hidx(17, 100))
        props = jobj(("page", _pg, True), ("from_page", _fp, True),
                     ("stay_duration", hidx(18, 600) + 1, False),
                     ("page_id", _pg, True), ("referrer_page", _fp, True))
    else:
        props = F.lit("{}")

    # 「完播」= progress >= 0.9，即 k <= 10。不是「发过 play_end 就算完播」——
    # 这两个概念在 v6 混过一次，导致完播率被算成 70%（实际 51.3%）。
    if etype == "play_end":
        complete = F.when(_k <= 10, F.lit(1)).otherwise(F.lit(0))
    elif etype == "skip":
        complete = F.lit(0)
    else:
        complete = F.lit(None).cast("int")

    # dt 分区列 = START_DT + dt_idx 天。
    # 不用 F.date_add(date, F.col(...))：Spark 3.0 的 date_add 第二个参数只吃 int 常量，
    # 传 Column 会抛 "Column is not iterable"（列参数是 3.1 才加的），而集群上是 3.0.0。
    # 走 unix_timestamp 的纯列算术，各版本行为一致。
    dt_series = F.from_unixtime(
        F.unix_timestamp(F.to_date(F.lit(START_DT)))
        + F.col("dt_idx").cast("int") * F.lit(86400), "yyyy-MM-dd")

    return df.select(
        F.concat(F.lit("e_"),
                 F.lpad(F.col("dt_idx").cast("string"), 3, "0"),
                 F.lpad(F.col("sess").cast("string"), 8, "0"),
                 F.lpad(F.lit(TYPE_IDX[etype]).cast("string"), 3, "0"),
                 F.lpad(F.col("_i").cast("string"), 4, "0"),
                 # 批次码：保证同一天多次运行的 ID 不撞（见文件头 RUN_TAG 的说明）
                 F.lit(RUN_TAG)).alias("event_id"),
        F.lit(etype).alias("event_name"),
        evt_ms.cast("string").alias("event_time"),
        (evt_ms + pmod(F.col("_i") * F.lit(37), F.lit(900))).cast("string").alias("server_time"),
        F.col("user_id"), F.col("device_id"),
        F.col("is_login"), F.col("is_vip"),
        F.col("app_version"), F.col("os"), F.col("os_version"), F.col("device_model"),
        F.col("network"), F.col("carrier"), F.col("ip"),
        F.col("province"), F.col("city"),
        F.col("session_id"),
        F.floor(offset / F.lit(EVENT_STEP_MS)).cast("string").alias("session_seq"),
        F.when(pmod(F.col("_i") * F.lit(13), F.lit(100)) < 92, "true").otherwise("false")
         .alias("is_foreground"),
        pick(["推荐", "搜索", "歌单", "电台", "排行榜"], F.col("_i")).alias("channel"),
        pick(["home", "player", "search", "playlist", "rank"], F.col("_i")).alias("page_id"),
        props.alias("properties"),
        F.concat(dt_series, F.lit(" 03:00:00")).alias("etl_time"),
        complete.alias("_complete"),
        dt_series.alias("dt"),
    )


def _attach_song(df, song_no_col):
    """挂上 song_num（Zipf 查表）→ dur_real（真实时长）。三次 join 都是 broadcast。"""
    df = df.withColumn("_u", pmod(F.xxhash64(F.col("sess_key"), song_no_col, F.lit(7)),
                                  F.lit(len(_zipf_rows))))
    df = df.join(F.broadcast(_zipf), ["_u"], "left")
    df = df.join(F.broadcast(_song_dim), ["song_num"], "left")
    return df.drop("_u")


parts = []

# ── A. 播放域：按歌曲循环 ───────────────────────────────────────────────────
# 跳出会话一首歌都不播（用户启动后直接离开），整个播放域对它们为空集。
_songs = (base.filter(~F.col("_bounce"))
              .withColumn("_i", F.explode(F.sequence(F.lit(1), F.lit(SONGS_PER_SESSION))))
              .withColumn("song_no", F.col("_i") - 1))
_songs = _attach_song(_songs, F.col("song_no")).cache()

# 70% 播完 / 30% 跳过。判定只依赖 (sess_key, 歌曲序号)，与 genre 用的哈希正交
_is_complete = pmod(F.xxhash64(F.col("sess_key"), F.col("_i"), F.lit(21)), F.lit(100)) < 70

parts.append(finalize(_songs, "play_start", "play"))
# 注意：filter 必须在 finalize 之前 —— finalize 只 select 出 24 列，
# 之后再 filter 一个中间列会报 AnalysisException
parts.append(finalize(_songs.filter(_is_complete), "play_end", "play"))
parts.append(finalize(_songs.filter(~_is_complete), "skip", "play"))

# ── B. 其余事件：按条数 explode ─────────────────────────────────────────────
for _t in COUNT_TYPES:
    # 跳出会话只保留 BOUNCE_TYPE(app_launch) 这一条，其余类型全部排除。
    # 排除放在 explode 之前：先缩小 base 再展开条数，比展开后过滤少一轮数据。
    _b = base if _t == BOUNCE_TYPE else base.filter(~F.col("_bounce"))
    _d = (_b.withColumn("_n", _count_expr(_t))
              .filter(F.col("_n") > 0)
              .withColumn("_i", F.explode(F.sequence(F.lit(1), F.col("_n")))))
    if _t in PLAY_TYPES or _t in REC_TYPES or _t in INTERACTION_TYPES:
        # 这些事件也要带歌：用哈希在 5 首里轮转，不能直接用 _i，否则永远只落在前几首。
        # 互动域（collect/like/share/...）的私有属性按 docs/19 设计都带 song_id，
        # 之前那段写 {} 的根因之一就是这里没给它们挂曲库。
        _d = _attach_song(_d, pmod(F.xxhash64(F.col("sess_key"), F.col("_i"), F.lit(3)),
                                   F.lit(SONGS_PER_SESSION)))
    else:
        _d = (_d.withColumn("song_num", F.lit(None).cast("int"))
                .withColumn("dur_real", F.lit(None).cast("int"))
                .withColumn("hot_rank", F.lit(None).cast("int")))
    parts.append(finalize(_d, _t, "other"))

df = parts[0]
for _p in parts[1:]:
    df = df.unionByName(_p)

# ── 校验 + 写出 ─────────────────────────────────────────────────────────────
# 顺序按数据量分两种，目的是别把执行计划算两遍：
#   小任务（≤500 万行）：先校验后写盘 —— 错了就不落脏数据，多算一遍也无所谓。
#   大任务：先写盘再回读文件校验 —— df 没有缓存，先 groupBy 等于把 3000 万行
#          的计划整跑一次，30 天那轮要多花近一倍时间。回读只取
#          event_name + properties 两列，ORC 列裁剪后很快，而且验的是**真正落盘
#          的数据**，比验内存里的 DataFrame 更有说服力。
_TOTAL = DAYS * ROWS_PER_DAY
_BIG = _TOTAL > 5000000


# =============================================================================
# v10：Kafka 唯一入口的脏数据注入
# =============================================================================
# docs/17 §3.2 —— 实时走 Flink、离线走 Flume，消费的是同一个 topic 的同一批消息。
# 所以脏数据只能在「写入 topic 时」注入一次，不能由某个消费者各自造，
# 否则又变成两套数据源。与 gen_music_data.inject_dirt() 相比有两点刻意不同：
#   ① 绝不改 event_name。离线 11 号脚本按 event_name 分 12 张 DWD 域表，
#      改成 PLAY/unknown 会让「12 张 DWD 合计 = ODS」的终态恒等式破掉。
#   ② duration_exceed 只改实时侧新补的 play_duration / play_progress 键；
#      离线 03 号脚本读 duration / position / complete，完全不受影响。
# 能被实时 job_b 规则捕获：device_id_missing、play_progress>1、
# play_duration>song_duration；duplicate 由 landing 按 event_id 去重消除。
KAFKA_DIRT_SEED = 20261107


def _inject_kafka_dirt(ev, rng):
    """就地改一条事件；返回 True 表示调用方需再发一条同 event_id 的重复消息。"""
    r = rng.random()
    if r < 0.010:                                  # missing_field
        ev["device_id"] = ""
    elif r < 0.014 and ev.get("event_name") == "play_end":   # duration_exceed
        p = ev.get("properties")
        p = json.loads(p) if isinstance(p, str) else dict(p or {})
        sd = int(float(p.get("song_duration") or 0))
        p["play_duration"] = sd + 60
        p["play_progress"] = round((sd + 60) / sd, 2) if sd else 1.5
        ev["properties"] = p
    elif r < 0.018:                               # time_reversed
        ev["server_time"] = str(int(ev["event_time"]) - 120000)
    return rng.random() < 0.006                  # duplicate_event_id


def report(stat):
    """stat 需要四列：event_name、complete（1=完播，0/null=不算）、session_id、event_time

    session_id 是 v11 加的：跳出率 / 每会话事件数必须「按会话」算，
    只看 event_name 的条数是推不出「有多少会话只剩 1 条」的。
    多这一列就能在写盘前把跳出率算准，不用等大屏。

    event_time 是「未来事件」的自检：造数器给会话内事件按 offset 铺时间，
    如果起点离 now 太近，末尾几条会落到 now 之后，被 job_b 的
    event_time_future 整批判脏（实测一天 1.1 万条）。这里在落盘前直接报
    「越界几条」，改完 --recent-minutes 能立刻看出来有没有修好。
    """
    stat = stat.cache()
    cnt = {r["event_name"]: r["count"] for r in stat.groupBy("event_name").count().collect()}
    ps, pe = cnt.get("play_start", 0), cnt.get("play_end", 0)
    comp = stat.filter(F.col("complete") == 1).count()
    imp, clk = cnt.get("impression", 0), cnt.get("click", 0)

    # ── 会话级指标（跳出率的唯一可信算法）──────────────────────────────────
    _sess = None
    if "session_id" in stat.columns:
        _sess = (stat.groupBy("session_id").count()
                     .agg(F.count(F.lit(1)).alias("n"),
                          F.sum(F.when(F.col("count") == 1, 1).otherwise(0)).alias("b"),
                          F.sum("count").alias("ev")).first())
    s_n = int(_sess["n"]) if _sess else 0
    s_b = int(_sess["b"] or 0) if _sess else 0
    s_ev = int(_sess["ev"] or 0) if _sess else sum(cnt.values())

    print("=" * 78)
    print("== 配置：%s（profile=%s，%d 类事件，未跳出 %.2f 条/会话）"
          % (CFG_PATH, PROFILE, len(SELECTED), EV_PER_SESSION))
    print("== 目标：%s %s  天数 %d  每天 %d 行（%d 会话）  实际产出 %d 行"
          % (SINK, START_DT, DAYS, ROWS_PER_DAY, SESS_PER_DAY, sum(cnt.values())))
    if BOUNCE_RATIO > 0:
        print("== 跳出建模：%.1f%% 会话只发 1 条 %s；均值 %.3f 条/会话（未跳出口径 %.3f）"
              % (BOUNCE_RATIO * 100, BOUNCE_TYPE, EV_PER_SESSION_EFF, EV_PER_SESSION))
    else:
        print("== 跳出建模：已关闭（跳出率将恒为 0）==")
    # 批次码要打出来：event_id 冲突排查时，拿它就能分清「哪些行是哪次跑出来的」
    print("== 运行批次码：%s（event_id 后缀；同一天多次运行靠它避免主键相撞）" % RUN_TAG)
    print("== 生命周期：池 %d  出生窗口 [%d, %d)  每天候选≈%d（槽位 %d，比例 %.2f）"
          % (USER_CNT, BIRTH_MIN, DAYS, USER_CNT * E_LIFE / BIRTH_SPAN,
             SESS_PER_DAY, USER_CNT * E_LIFE / BIRTH_SPAN / SESS_PER_DAY))
    # 事件时间自检：> now 的会被 job_b 判 event_time_future，一条都不该有
    if "event_time" in stat.columns:
        _ms = F.col("event_time").cast("long")
        _now = int(time.time() * 1000)
        _agg = stat.agg(F.min(_ms).alias("lo"), F.max(_ms).alias("hi")).collect()[0]
        _future = stat.filter(_ms > F.lit(_now)).count()
        _late = (_now - (_agg["lo"] or _now)) / 60000.0
        print("== 事件时间：跨度 [−%.0f 分钟, %+.1f 分钟]（相对现在），越界 %d 条%s =="
              % (_late, ((_agg["hi"] or _now) - _now) / 60000.0, _future,
                 "" if _future == 0 else "  <<< 会被 event_time_future 拦掉"))
    print("== 事件分布 ==")
    for t in SELECTED:
        c = cnt.get(t, 0)
        print("   %-16s %9d   每会话 %.3f" % (t, c, c / float(max(1, SESS_PER_DAY * DAYS))))
    print("== 关键比率（判据见 docs/18 W1-3）==")
    print("   起播 / 完播        %d / %d = %.2f%%（目标 51.3%%）"
          % (ps, comp, 100.0 * comp / max(1, ps)))
    print("   play_end 占起播    %.2f%%（目标 70%%）" % (100.0 * pe / max(1, ps)))
    print("   CTR（点击/曝光）   %.2f%%（目标 20%%）" % (100.0 * clk / max(1, imp)))
    if s_n:
        print("== 会话指标（真实统计，非模型推算）==")
        print("   会话数 %d   跳出 %d   跳出率 %.2f%%（真实区间 20~30%%）"
              % (s_n, s_b, 100.0 * s_b / s_n))
        print("   每会话事件 %.2f（目标 ≈%.2f）   事件合计 %d"
              % (s_ev * 1.0 / s_n, EV_PER_SESSION_EFF, s_ev))
    print("=" * 78)
    stat.unpersist()


if SINK == "hdfs":
    # --dry-run 时不落盘，也就没有「回读文件校验」那一步，所以再大的任务也先算一遍
    if not _BIG or DRY_RUN:
        report(df.select("event_name", F.col("_complete").alias("complete"), "session_id", "event_time"))

    if DRY_RUN:
        print("== --dry-run：跳过写盘 ==")
        spark.stop()
        sys.exit(0)

    out = df.drop("_complete")
    if DAYS == 1:
        _path = "%s/dt=%s" % (BASE_PATH, START_DT)
        out.drop("dt").write.mode("overwrite").orc(_path)
    else:
        _path = BASE_PATH
        # 先按 dt 重分布（每天一个 task），否则每个 shuffle task 都往 30 个目录写 → 文件数爆炸
        (out.repartition(DAYS, F.col("dt"))
            .write.mode("overwrite").partitionBy("dt").orc(_path))
    print("== 写入完成：%s ==" % _path)

    if _BIG:
        # 从落盘文件里判完播：properties 里带 "complete":1 的才算
        # session_id 也要读：跳出率必须按会话算
        chk = spark.read.orc(_path).select("event_name", "properties", "session_id", "event_time")
        report(chk.select("event_name",
                          F.when(F.col("properties").contains('"complete":1'), 1)
                           .otherwise(F.lit(None).cast("int")).alias("complete"),
                          "session_id", "event_time"))

else:
    report(df.select("event_name", F.col("_complete").alias("complete"), "session_id", "event_time"))

    if DRY_RUN:
        print("== --dry-run：跳过发送 Kafka ==")
        spark.stop()
        sys.exit(0)

    # Kafka sink：走 driver 侧单进程生产。
    # 为什么不 foreachPartition：那要求 160 / 163 两个 executor 都装 kafka-python，
    # 而增量每天只有 100 万行，driver 端批量 send（64KB batch + 20ms linger）
    # 一分多钟就发完，不值得为此给集群加依赖。
    from kafka import KafkaProducer

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        acks=1, linger_ms=20, batch_size=1 << 16, compression_type="gzip",
    )
    rng = random.Random(KAFKA_DIRT_SEED)
    n = 0
    dups = 0
    # ── 发送顺序：必须≈事件时间有序（不是可选项，是正确性前提）─────────────
    # 下游作业 C/D 用事件时间开窗 + 5 秒水位线。水位线一到，比它早的水都算迟到、
    # 直接丢弃（allowed lateness = 0）。
    #
    # 而本脚本是「26 类事件各占一个生成分支，union 成一张表」，行序天然是
    # 「按类型成段」——先全部 play_start（横跨整个时间范围），再全部 play_end……
    # 时间上剧烈跳变。实测这样灌进去，作业 C 的窗口只留下 2,957 条 / 共 17,877 起播，
    # 丢了 83%；dwd_play_end topic 里连续 397 条消息的相邻逆序对高达 39%。
    #
    # 真实客户端是「发生即上报」，Kafka 里本来就近似按时间有序。造数器扮演的是
    # 回放器，理应复现这个特性：这里按 event_time 全局排序后再由 driver 顺序 send。
    # 配合 Kafka 默认的粘性分区器，每个分区拿到的都是连续时段 → 三个分区的水位线
    # 同步推进，窗口不再丢数。
    _send_df = df.drop("_complete").orderBy(F.col("event_time").cast("long"))

    # 【时间戳跟随发送时刻 —— 解决"灌完就过时"】
    # 数据在 Spark 里生成时就固化了时间戳（_NOW_MS = 脚本启动时刻），而
    # 这 1.2 万行要发 4~6 分钟。等发完，最新事件已经是 now−6min，
    # 大屏「实时在线（近 5 分钟）」又空了 —— 用户直观感受就是"灌完还是看不到"。
    # 所以发送时把每条的时间戳按「已耗时」整体右移：
    #   event_time += (当前时刻 − 脚本启动时刻)
    # 效果等价于「按真实速率回放」：发完那一刻，最新事件就是此刻。
    # 因为数据本身按 event_time 升序发送，右移量单调不减，顺序不会被打乱，
    # 水印照常平滑推进（不会因右移产生迟到）。
    _shift, _shift_at = 0, 0
    for row in _send_df.toLocalIterator():
        ev = row.asDict()
        if n >= _shift_at:
            _shift = int(time.time() * 1000) - _NOW_MS
            _shift_at = n + 500
        if _shift > 0:
            for _k in ("event_time", "server_time"):
                _v = ev.get(_k)
                if _v:
                    try:
                        ev[_k] = str(int(_v) + _shift)
                    except (TypeError, ValueError):
                        pass
        # v10：Kafka 消息里 properties 必须是 JSON **对象**（埋点规范）。
        # finalize() 里 jobj() 拼的是 STRING，直接 asDict 会变成双重编码的字符串，
        # 实时 job_b 的 $.properties.song_id 会全部取 NULL；离线 landing 解析器
        # 改为 get_json_object 后两种格式都兼容，所以这里统一成对象。
        if isinstance(ev.get("properties"), str):
            try:
                ev["properties"] = json.loads(ev["properties"])
            except Exception:
                pass
        dup = _inject_kafka_dirt(ev, rng)
        producer.send(TOPIC, ev)
        n += 1
        if dup:
            producer.send(TOPIC, dict(ev))
            n += 1
            dups += 1
        if n % 100000 == 0:
            print("   已发送 %d 行" % n)
    producer.flush()
    producer.close()
    print("== 发送完成：%d 行 → %s/%s（含注入重复 %d 条）==" %
          (n, KAFKA_BOOTSTRAP, TOPIC, dups))

spark.stop()
