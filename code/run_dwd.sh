#!/bin/bash
# DWD 三段（03/04/11），覆盖 35 天
cd /root/offline || exit 1
DT=${1:-2026-11-08}

for step in dwd03 dwd04 dwd11; do
  date "+%F %T >>> $step 开始"
  bash dag_t1.sh "$step" "$DT" || { echo "== $step 失败，中止 =="; exit 1; }
  date "+%F %T <<< $step 完成"
done
echo "== DWD_ALL_OK =="
