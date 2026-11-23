#!/bin/bash
# =============================================================================
# 创建实时链路需要的两个中间 topic
#
# music_events    原始埋点     —— 已有，生成器直接往里写
# ods_music_event ODS 层       —— 作业 A 写，作业 B 读
# dwd_play_end    DWD 层       —— 作业 B 写，作业 C/D 读
# dwd_skip        DWD 层       —— 作业 B 写，作业 C/D 读
#                                 （播放终止流 = dwd_play_end ∪ dwd_skip，
#                                  见 job_b_dwd_dim.sql 3.12 节的原因说明）
#
# v10：Kafka `ods_music_event` 就是实时 ODS（Doris ODS 表已砍）。
# 它与原始 topic music_events 一样保留 3 天 —— 实时侧不分析历史，
# 可回溯/重放窗口就是 3 天；持久原始层是离线 HDFS ODS(ORC)。
# dwd_play_end 保留 7 天，给窗口作业留独立重放余量。
# 副本数 1：Kafka 已收敛为单 broker（docs/17 §3.6.1），rf=3 会创建失败。
# =============================================================================
set -e

KAFKA_HOME=${KAFKA_HOME:-/opt/module/kafka}
BROKER=${BROKER:-WWC161:9092}
RETENTION_MS=${RETENTION_MS:-259200000}

create_topic() {
    local topic=$1
    local partitions=$2
    echo ">>> 创建 topic: $topic (partitions=$partitions)"
    $KAFKA_HOME/bin/kafka-topics.sh \
        --bootstrap-server "$BROKER" \
        --create --if-not-exists \
        --topic "$topic" \
        --partitions "$partitions" \
        --replication-factor 1 \
        --config retention.ms="$RETENTION_MS"
}

create_topic ods_music_event 3
RETENTION_MS=604800000 create_topic dwd_play_end 3
RETENTION_MS=604800000 create_topic dwd_skip 3

echo
echo ">>> 当前 topic 列表"
$KAFKA_HOME/bin/kafka-topics.sh --bootstrap-server "$BROKER" --list

echo
echo ">>> 分区分布"
$KAFKA_HOME/bin/kafka-topics.sh --bootstrap-server "$BROKER" --describe \
    --topic ods_music_event --topic dwd_play_end --topic dwd_skip
