#!/bin/bash
# DWS + ADS 三段（05 / 08+09 / 12），覆盖 35 天
cd /root/offline || exit 1
DT=${1:-2026-11-08}

for step in dws05 dws08_ads09 ext; do
  date "+%F %T >>> $step 开始"
  bash dag_t1.sh "$step" "$DT" || { echo "== $step 失败，中止 =="; exit 1; }
  date "+%F %T <<< $step 完成"
done
echo "== DWS_ADS_ALL_OK =="
