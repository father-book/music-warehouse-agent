#!/bin/bash
# =============================================================================
# T+1 离线链路单步执行器（DolphinScheduler Worker 通过 SSH 调用）
# 用法: bash /root/offline/dag_t1.sh <step> <dt>
#
# 为什么集中到一个脚本：
#   DS 的 SHELL 任务经 160 → SSH → 161 两层传递，引号和 $ 转义极易出错；
#   把所有逻辑放进本文件，DAG 节点只需 `ssh ... bash dag_t1.sh <step> ${dt}`。
#   单步可独立调试，不必提交整个工作流。
#
# 幂等设计：造数/落 ODS 都先检查目标分区，已存在就跳过，
#   因此整条 DAG 可以反复重跑而不污染 Kafka 与 ODS。
#   所有 Hive 脚本都是 start_dt=固定起点 / end_dt=$DT 的全量重算 + INSERT OVERWRITE，
#   同一天多次执行结果一致。
# =============================================================================
set -e

STEP=$1
DT=$2
# 起点默认 2026-10-05（全量重算）；可用 START_DT_OVERRIDE 收窄到某几天，
# 例如补 11-05~11-08 时只覆盖这 4 个分区，不动历史。
# 注意：ads_user_retention 不分 dt 分区、cohort 取范围内 MIN(dt)，
#       只跑少数几天会把历史 cohort 全压到起始日，跑完必须用全量范围重修。
START_DT=${START_DT_OVERRIDE:-2026-10-05}
OFFLINE=/root/offline
HIVE=/opt/module/hive/bin/hive
KAFKA=/opt/module/kafka/bin
SPARK="/opt/module/spark/bin/spark-submit --master yarn --deploy-mode client \
  --conf spark.driver.memory=1g --conf spark.executor.memory=2g \
  --conf spark.executor.memoryOverhead=1024 --conf spark.executor.cores=2 \
  --conf spark.executor.instances=2 --conf spark.pyspark.python=python3 \
  --conf spark.pyspark.driver.python=python3"

[ -n "$STEP" ] && [ -n "$DT" ] || { echo "用法: $0 <step> <dt>"; exit 2; }
cd "$OFFLINE"

case "$STEP" in

  gen)
    # 增量造数 → Kafka。ODS 已有该业务日期就跳过，避免重复灌 topic。
    if hdfs dfs -test -d "/warehouse/music_dw/ods_music_event/dt=$DT"; then
      echo "[gen] ODS 已有 dt=$DT，跳过造数（幂等）"
      exit 0
    fi
    echo "[gen] 生成 $DT 的 100 万行并写入 Kafka"
    $SPARK gen_events.py --start "$DT" --days 1 --events 1000000 \
                         --profile full --sink kafka
    ;;

  wait)
    # 等 Flume 把该批数据消费到 landing（LAG=0），最多 20 分钟。
    for i in $(seq 1 60); do
      LAG=$($KAFKA/kafka-consumer-groups.sh --bootstrap-server WWC161:9092 \
              --describe --group flume-ods 2>/dev/null \
            | awk 'NR>1{s+=$6} END{print s+0}')
      LAG=${LAG:-1}
      if [ "$LAG" = "0" ]; then
        echo "[wait] Flume LAG=0，landing 就绪"
        hdfs dfs -du -s -h /landing/music_event 2>/dev/null | tail -1
        exit 0
      fi
      echo "[wait] 第 $i 次检查，LAG=$LAG，等 20s"
      sleep 20
    done
    echo "[wait] 超时：Flume 仍有积压" >&2
    exit 3
    ;;

  landing)
    # landing(text.gz) → ODS(ORC)，脚本内部按 event_id 去重且默认跳过已有分区。
    # 去重键固定为 (event_id, event_time)：日常单次造数下 event_id 本就唯一，
    # 结果与只用 event_id 等价；但重放多批倒灌数据时 event_id 会撞号
    # （实测单键重复 49.89%），必须用二元组，两条路径保持同一口径。
    $SPARK spark_landing_to_ods.py --dt "$DT" --dedup-keys event_id,event_time
    $HIVE -S -e "MSCK REPAIR TABLE music_dw.ods_music_event;" 2>/dev/null | tail -1
    ;;

  dwd03)
    $HIVE --hivevar start_dt="$START_DT" --hivevar end_dt="$DT" -f 03_dwd_play_end.sql
    ;;
  dwd04)
    $HIVE --hivevar start_dt="$START_DT" --hivevar end_dt="$DT" -f 04_dwd_play_start.sql
    ;;
  dwd11)
    # 10 张域表（含 dwd_skip），覆盖 24 类事件；与 03/04 合起来覆盖全部 26 类。
    $HIVE --hivevar start_dt="$START_DT" --hivevar end_dt="$DT" -f 11_dwd_domain_tables.sql
    ;;
  dws05)
    $HIVE --hivevar start_dt="$START_DT" --hivevar end_dt="$DT" -f 05_dws_song_play_daily.sql
    ;;
  dws08_ads09)
    # 09 依赖 08 产出的 dws_*，串行放在同一节点里。
    $HIVE --hivevar start_dt="$START_DT" --hivevar end_dt="$DT" -f 08_dws_daily.sql
    $HIVE --hivevar start_dt="$START_DT" --hivevar end_dt="$DT" -f 09_ads_tables.sql
    ;;
  ext)
    # 12 号：3 张新 DWS + 2 张新 ADS（漏斗/搜索/收入），只读 DWD 域表。
    # 必须等 dwd11 完成，否则新域表还是上一版数据。
    $HIVE --hivevar start_dt="$START_DT" --hivevar end_dt="$DT" -f 12_dws_ads_ext.sql
    ;;
  sync)
    # Hive ADS → Doris 7 张表（INSERT OVERWRITE 全量刷新，幂等）。
    # 脚本内部先 REFRESH CATALOG hive_catalog，避免 Hive 新建表后元数据缓存不刷新。
    python3 "$OFFLINE/sync_doris.py"
    ;;
  *)
    echo "未知 step: $STEP" >&2
    exit 2
    ;;
esac

echo "[dag_t1] step=$STEP dt=$DT 完成"
