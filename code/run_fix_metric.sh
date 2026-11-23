#!/bin/bash
# 重算 08/09/12 —— 2026-11-09 口径修正：
#   ① 日活/会话 改用 dwd_session(app_launch)（原先用 play_start，低 22%）
#   ② dws_genre_daily.user_cnt / dws_search_daily.search_uv / dws_revenue_daily.pay_uv
#      统一按 COALESCE(NULLIF(user_id,''), device_id) 去重（原先裸 user_id，游客被压成 1 个）
#   ③ ads_user_retention 的 cohort 实体与活跃来源同步
#
# executor 规格必须覆写，原因见 run_fix_songid.sh 头部（本集群 YARN 只有 3072/1024/1024）。
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

for step in 08_dws_daily 09_ads_tables 12_dws_ads_ext; do
  date "+%F %T >>> $step 开始"
  $HIVE $OPTS -f $step.sql
  date "+%F %T <<< $step 完成"
done
echo "== METRIC_FIX_OK =="
