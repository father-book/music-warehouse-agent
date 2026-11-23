#!/bin/bash
# 重算 08/09 两段——修复 dim_song.song_id 前缀错位后，让曲风/榜单用正确维表重算。
#
# 为什么要显式覆写 executor 规格：
#   本集群三个 NodeManager 上报的 YARN 内存总量分别是 3072 / 1024 / 1024 MB，
#   而 hive-site.xml 里 spark.executor.memory=2g（+384 overhead = 2432 容器）。
#   AM 一旦落在 WWC160（唯一有 3072 的节点），剩余 2176 放不下 2432，
#   161/162 又各只有 1024 —— executor 永久 pending，Stage 卡在 0/N。
#   实测 2026-11-09：整个 application 只有 1 个容器（AM），executor 数 0。
#   640 + 384 = 1024 正好等于小节点的容量上限，三个节点都能放，故覆写之。
#
#   ⚠️ 单位必须写 `640m`。写裸数字 `640` 会被 Spark 当成 640 **字节**，
#   直接抛 IllegalArgumentException: Executor memory 640 must be at least
#   471859200（=450MB），SparkContext 起不来。（2026-11-09 实测踩到）
set -e
cd /root/offline
DT=2026-11-08
START_DT=2026-10-05
HIVE=/opt/module/hive/bin/hive
OPTS="--hivevar start_dt=${START_DT} --hivevar end_dt=${DT}
      --hiveconf spark.executor.memory=640m
      --hiveconf spark.executor.memoryOverhead=384m
      --hiveconf spark.executor.instances=3
      --hiveconf spark.executor.cores=2"

date "+%F %T >>> 08 开始"
$HIVE $OPTS -f 08_dws_daily.sql
date "+%F %T <<< 08 完成"

date "+%F %T >>> 09 开始"
$HIVE $OPTS -f 09_ads_tables.sql
date "+%F %T <<< 09 完成"
echo "== SONGID_RERUN_OK =="
