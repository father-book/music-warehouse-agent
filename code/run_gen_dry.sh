#!/bin/bash
# 重造前的 dry-run：跑完整计算但不落盘，验证行数/比率/主键格式
set -e
cd /root/offline

SPARK=/opt/module/spark/bin/spark-submit

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
    --profile full --sink hdfs --dry-run

echo "== DRY_RUN_EXIT=$? =="
