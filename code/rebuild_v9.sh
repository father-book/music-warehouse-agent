#!/bin/bash
# =============================================================================
# v9 重造 31 天 ODS（2026-11-07）
#
# 为什么是「30 天 + 1 天」而不是一次 31 天：
#   BIRTH_MIN = DAYS - BIRTH_SPAN，DAYS 变化会让出生窗口整体平移，
#   用户 ID 分配随之改变、留存曲线也变。W1 定型的口径就是
#   「历史 30 天 --days 30 直写 + 第 31 天 --days 1 增量」，本次
#   只换生成器（补齐属性 / Zipf 打散 / 完播倾向），沿用同样的两段式，
#   保证生命周期模型与 W1 完全可比。
#
# 为什么第 31 天这次也走 hdfs 直写：
#   Kafka topic 里还留着 W1 那批 11-04 的消息，Flume 消费组已消费完；
#   再发一批同 event_id 的消息会让 landing 新旧两批混在一起，
#   spark_landing_to_ods.py 按 event_id 去重时无法保证取到新属性。
#   两条链路的等价性在 W1-4~W1-7 已验证（docs/18），本次不重复重放。
#
# 回滚：/backup/ods_before_20261107/ods_music_event 是改造前 31 天完整副本。
# =============================================================================
cd /root/offline || exit 2

# 容错参数：v9 的 properties 变宽后 shuffle 量比 W1 大，19:22 那次因
# WWC163 本地盘写满 + NM 心跳超时丢了三个 container（FetchFailed 连锁失败）。
# 先清过三台 nm-local-dir 的 jar 缓存（释放约 6G），再放宽网络/心跳超时、
# 关掉推测执行，避免 VM 卡顿被误判成 lost node 后整轮重算。
SPARK="/opt/module/spark/bin/spark-submit --master yarn --deploy-mode client \
  --conf spark.driver.memory=1g --conf spark.executor.memory=2g \
  --conf spark.executor.memoryOverhead=1024 --conf spark.executor.cores=2 \
  --conf spark.executor.instances=2 --conf spark.pyspark.python=python3 \
  --conf spark.pyspark.driver.python=python3 \
  --conf spark.speculation=false \
  --conf spark.network.timeout=300s --conf spark.executor.heartbeatInterval=30s \
  --conf spark.shuffle.io.maxRetries=10 --conf spark.shuffle.io.retryWait=10s"
HIVE=/opt/module/hive/bin/hive

echo "[1/4] 历史 30 天直写 ODS（约 20 分钟）"
$SPARK gen_events.py --start 2026-10-05 --days 30 --events 1000000 \
                     --profile full --sink hdfs > /tmp/v9_hist30.log 2>&1 || {
    echo "!! 历史 30 天失败"; tail -40 /tmp/v9_hist30.log; exit 3; }
grep -E '^== 目标|^== 写入完成|^   起播|^   play_end|^   CTR' /tmp/v9_hist30.log || true

echo "[2/4] 第 31 天 2026-11-04 直写 ODS"
$SPARK gen_events.py --start 2026-11-04 --days 1 --events 1000000 \
                     --profile full --sink hdfs > /tmp/v9_t1.log 2>&1 || {
    echo "!! 第 31 天失败"; tail -40 /tmp/v9_t1.log; exit 4; }
grep -E '^== 目标|^== 写入完成|^   起播|^   play_end|^   CTR' /tmp/v9_t1.log || true

echo "[3/4] 清掉试跑分区 11-05，重新登记分区"
$HIVE -S -e "ALTER TABLE music_dw.ods_music_event DROP IF EXISTS PARTITION (dt='2026-11-05'); MSCK REPAIR TABLE music_dw.ods_music_event;" 2>&1 | tail -3

echo "[4/4] 31 天总行数 / 分区数"
$HIVE -S -e "SELECT count(DISTINCT dt) AS parts, count(*) AS rows FROM music_dw.ods_music_event;" 2>/dev/null | tail -1
echo "[rebuild_v9] DONE"
