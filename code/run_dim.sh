#!/bin/bash
# 重建三张维表：dim_song(1486, 纯 S 前缀) / dim_date(35 天) / ods_user_change(31 省)
cd /root/offline || exit 1

date '+%F %T 开始维表'
/opt/module/spark/bin/spark-submit --master yarn --deploy-mode client \
  --conf spark.driver.memory=1g \
  --conf spark.executor.memory=2g \
  --conf spark.executor.memoryOverhead=768 \
  --conf spark.executor.cores=2 \
  --conf spark.executor.instances=3 \
  --conf spark.pyspark.python=python3 \
  --conf spark.pyspark.driver.python=python3 \
  gen_dim_tables.py

RC=$?
date '+%F %T 维表结束'
echo "== DIM_EXIT=$RC =="
