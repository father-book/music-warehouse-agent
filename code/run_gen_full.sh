#!/bin/bash
# 全量重造 35 天 / 500 万行，写入 ODS（覆盖式）
cd /root/offline || exit 1

SPARK=/opt/module/spark/bin/spark-submit

date '+%F %T 开始重造'
$SPARK --master yarn --deploy-mode client \
  --conf spark.driver.memory=2g \
  --conf spark.executor.memory=2g \
  --conf spark.executor.memoryOverhead=768 \
  --conf spark.executor.cores=2 \
  --conf spark.executor.instances=3 \
  --conf spark.pyspark.python=python3 \
  --conf spark.pyspark.driver.python=python3 \
  gen_events.py \
    --start 2026-10-05 --days 35 --events 142857 \
    --profile full --sink hdfs

RC=$?
date '+%F %T 结束'
echo "== GEN_FULL_EXIT=$RC =="
