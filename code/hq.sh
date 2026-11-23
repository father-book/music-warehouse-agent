#!/bin/bash
# hq.sh <sqlfile> —— 带 executor 规格覆写的 hive 查询执行器
# 原因见 run_fix_songid.sh 头部注释：本集群 YARN 总量 3072/1024/1024，
# 沿用 hive-site 的 2g executor 会在 AM 落到 WWC160 时永久 pending。
# 输出只保留结果行（Hive CLI 的 log4j INFO 走 stdout，必须过滤）。
set -e
SQL=$1
OUT=/tmp/hq_$(basename "$SQL" .sql).out
/opt/module/hive/bin/hive -S -f "$SQL" \
  --hiveconf spark.executor.memory=640m \
  --hiveconf spark.executor.memoryOverhead=384m \
  --hiveconf spark.executor.instances=3 \
  --hiveconf spark.executor.cores=2 \
  > "$OUT" 2>/tmp/hq_err.log
grep -vE "INFO|WARN|SLF4J|log4j|WARNING" "$OUT" | grep -v "^[[:space:]]*$"
