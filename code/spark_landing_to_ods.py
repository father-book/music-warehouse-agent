#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
spark_landing_to_ods.py —— landing（text.gz）→ ODS（ORC）去重落地

═══════════════════════════════════════════════════════════════════════════════
在链路里的位置
═══════════════════════════════════════════════════════════════════════════════
  造数(gen_events.py --sink kafka) → Kafka → Flume → /landing/music_event/dt=yyyymmdd/*.gz
                                                        ↓ 本脚本
                                              /warehouse/music_dw/ods_music_event/dt=yyyy-MM-dd/*.orc

═══════════════════════════════════════════════════════════════════════════════
三个必须处理的问题
═══════════════════════════════════════════════════════════════════════════════
① **分区日期以事件为准，不以到达为准**
   Flume 的 hdfs.path 用 %Y%m%d 展开的是写入时刻（今天），而我们灌的是昨天的事件。
   所以 landing 的目录名只是"哪天到达的"，真正的业务日期在每条 JSON 的 dt 字段里。
   本脚本按 JSON 里的 dt 写 ODS —— 这和真实生产一致：到达时间 ≠ 事件时间。

② **必须去重，而且去重键不能只用 event_id**
   Kafka 的语义是 at-least-once：Flume 的 memory channel 在 sink 写失败重试、
   或者 offset 提交前进程被杀，都会让同一批事件被落两次。

   2026-11-08 实测发现：只按 event_id 去重会误删真实事件。
   造数器的 event_id 是「时间槽 + 序号」，不含用户/会话，多批重灌时必然撞号：
       K1 event_id                        重复 49.89%
       K2 (event_id, event_time)          重复  0.35%   ← 采用
       K3 (event_id, event_time, user_id) 重复  0.35%（多认 54 条，无必要）
   离线历史分区（10-05~11-04）的 event_id 本来就唯一，所以默认值保持
   event_id 不变，对历史结果完全等价；只有重放多批倒灌的数据时才需要
   --dedup-keys event_id,event_time。

③ **只能读 .gz，不能读 .tmp**
   Flume 写 HDFS 时先写 `.tmp` 后缀的文件，关闭时才改成 `.gz`。
   如果 glob 写成 `*`，会把正在写的半截文件读进来 → gzip 解压失败。
   所以用 `*/*.gz` 精确匹配。

═══════════════════════════════════════════════════════════════════════════════
幂等性
═══════════════════════════════════════════════════════════════════════════════
默认**跳过** ODS 里已经存在且有数据的 dt 分区，避免重跑时 append 出重复数据。
确实要覆盖用 --force（它会先删掉该 dt 的目录再写）。

═══════════════════════════════════════════════════════════════════════════════
用法
═══════════════════════════════════════════════════════════════════════════════
  spark-submit ... spark_landing_to_ods.py                       # 处理全部新分区
  spark-submit ... spark_landing_to_ods.py --dt 2026-11-04       # 只处理某一天
  spark-submit ... spark_landing_to_ods.py --dt 2026-11-04 --force
"""
import argparse
import sys

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, StringType

LANDING = "hdfs://WWC160:9820/landing/music_event"
BASE_PATH = "hdfs://WWC160:9820/warehouse/music_dw/ods_music_event"
HDFS_URI = "hdfs://WWC160:9820"

# ODS 的 24 列（顺序必须和 CREATE TABLE 一致：Hive 读 ORC 是按位置的）
ODS_FIELDS = [
    "event_id", "event_name", "event_time", "server_time",
    "user_id", "device_id", "is_login", "is_vip",
    "app_version", "os", "os_version", "device_model", "network", "carrier", "ip",
    "province", "city", "session_id", "session_seq", "is_foreground",
    "channel", "page_id", "properties", "etl_time",
]
# v10：properties 不再进 from_json 的 schema。
# Kafka 统一入口里它是 JSON 对象，from_json 把 object 映射到 StringType 会得到 NULL；
# 老格式里它是 JSON 字符串。get_json_object("value", "$.properties") 对两者
# 都返回纯 JSON 文本，正好是 ODS properties 列要的内容。
_SCHEMA_FIELDS = [c for c in ODS_FIELDS if c != "properties"]
SCHEMA = StructType([StructField(c, StringType(), True) for c in _SCHEMA_FIELDS]
                    + [StructField("dt", StringType(), True)])

_ap = argparse.ArgumentParser()
_ap.add_argument("--dt", default="", help="只处理某一天（事件日期 yyyy-MM-dd）")
_ap.add_argument("--force", action="store_true", help="覆盖已存在的分区")
_ap.add_argument("--landing", default=LANDING)
_ap.add_argument("--no-dedup", action="store_true", help="跳过去重（调试用）")
_ap.add_argument("--dedup-keys", default="event_id",
                 help="去重键，逗号分隔。重放倒灌数据时用 event_id,event_time")
ARGS = _ap.parse_args()

spark = (SparkSession.builder
         .appName("landing_to_ods")
         .config("spark.sql.shuffle.partitions", "32")
         .config("spark.sql.orc.compression.codec", "snappy")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")


def _fs():
    jvm = spark._jvm
    return jvm, jvm.org.apache.hadoop.fs.FileSystem.get(
        jvm.java.net.URI(HDFS_URI), spark.sparkContext._jsc.hadoopConfiguration())


def _exists_nonempty(path):
    jvm, fs = _fs()
    p = jvm.org.apache.hadoop.fs.Path(path)
    if not fs.exists(p):
        return False
    for st in fs.listStatus(p):
        if st.isFile() and st.getLen() > 0:
            return True
    return False


def _rm(path):
    jvm, fs = _fs()
    fs.delete(jvm.org.apache.hadoop.fs.Path(path), True)


# ── 1. 读 landing：只读已关闭的 .gz（跳过 Flume 正在写的 .tmp）────────────────
raw = spark.read.text("%s/*/*.gz" % ARGS.landing)
n_raw = raw.count()

rows = (raw.select(F.from_json("value", SCHEMA).alias("r"),
                   F.get_json_object("value", "$.properties").alias("properties"))
           .select("r.*", "properties")
           .filter(F.col("event_id").isNotNull())
           # 统一成 yyyy-MM-dd，防止上游写进来 yyyyMMdd
           .withColumn("dt", F.date_format(F.to_date(F.col("dt")), "yyyy-MM-dd")))

if ARGS.dt:
    rows = rows.filter(F.col("dt") == ARGS.dt)

DEDUP_KEYS = [k.strip() for k in ARGS.dedup_keys.split(",") if k.strip()]

if not ARGS.no_dedup:
    before = rows.count()
    rows = rows.dropDuplicates(DEDUP_KEYS)
    after = rows.count()
else:
    before = after = -1

cnt = {r["dt"]: r["count"] for r in rows.groupBy("dt").count().collect()}
print("=" * 78)
print("== landing 原始行数：%d" % n_raw)
if not ARGS.no_dedup:
    print("== 去重（键=%s）：%d → %d（重复 %d）"
          % (",".join(DEDUP_KEYS), before, after, before - after))
print("== 待写入分区：%s" % ", ".join("%s(%d)" % (k, v) for k, v in sorted(cnt.items())))

if not cnt:
    print("== 没有可写入的数据 ==")
    spark.stop()
    sys.exit(0)

# ── 2. 幂等保护：已存在且有数据的分区默认跳过 ────────────────────────────────
todo = []
for d in sorted(cnt):
    target = "%s/dt=%s" % (BASE_PATH, d)
    if _exists_nonempty(target):
        if ARGS.force:
            print("== --force：删除已有分区 %s ==" % target)
            _rm(target)
            todo.append(d)
        else:
            print("== 跳过 %s（ODS 已有数据，要覆盖加 --force）==" % d)
    else:
        todo.append(d)

if not todo:
    print("== 没有需要写入的分区 ==")
    spark.stop()
    sys.exit(0)

rows = rows.filter(F.col("dt").isin(todo))

# ── 3. 写 ORC：append，partitionBy 保证只新增 dt 目录，不动历史分区 ─────────
(rows.select(*ODS_FIELDS, "dt")
     .repartition(len(todo), F.col("dt"))
     .write.mode("append").partitionBy("dt").orc(BASE_PATH))

print("== 写入完成：%s 共 %d 行 ==" % (",".join(todo), sum(cnt[d] for d in todo)))
print("=" * 78)

# ── 4. 校验：回读落盘文件，确认每个分区的行数与预期一致 ──────────────────────
chk = (spark.read.orc(BASE_PATH)
       .filter(F.col("dt").isin(todo))
       .groupBy("dt").count().collect())
for r in sorted(chk, key=lambda x: x["dt"]):
    print("   校验 %s：落盘 %d 行" % (r["dt"], r["count"]))

spark.stop()
