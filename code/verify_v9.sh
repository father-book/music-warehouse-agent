#!/bin/bash
# =============================================================================
# v9 全链路重跑 + 验收（在 rebuild_v9.sh 成功后执行）
#
# 依赖顺序：03/04/11 只读 ODS → 05 依赖 03/04 → 08+09 依赖 DWD/DWS
#          → 12 依赖 11 → sync 依赖 ADS。全部串行，避免 YARN 争抢。
# 每步独立日志 /tmp/v9_<step>.log，失败即停并打印尾部。
# =============================================================================
cd /root/offline || exit 2
HIVE=/opt/module/hive/bin/hive
DT=2026-11-04

echo "########## A. 离线全链路重跑 ##########"
for s in dwd03 dwd04 dwd11 dws05 dws08_ads09 ext sync; do
    echo "=== STEP $s  $(date +%H:%M:%S) ==="
    bash dag_t1.sh "$s" "$DT" > "/tmp/v9_$s.log" 2>&1
    rc=$?
    echo "    RC=$rc"
    if [ "$rc" -ne 0 ]; then
        echo "!! 失败于 $s"; tail -40 "/tmp/v9_$s.log"; exit 3
    fi
done

echo "########## B. 指标字典 + DataArt 视图 ##########"
python3 run_doris_sql.py meta_metric.sql 2>&1 | tail -5
python3 verify_datart.py datart_views.sql 2>&1 | tail -12

echo "########## C. 造数质量诊断（13 号脚本）##########"
$HIVE --hivevar dt="$DT" -f 13_gen_quality_check.sql > /tmp/diag_v9.log 2>&1
echo "    RC=$?"

echo "########## D. 关键行数对账 ##########"
$HIVE -S -e "
SELECT 'ods_music_event', count(*), count(DISTINCT dt) FROM music_dw.ods_music_event;
SELECT 'dwd_play_start', count(*), count(DISTINCT dt) FROM music_dw.dwd_play_start;
SELECT 'dwd_play_end', count(*), count(DISTINCT dt) FROM music_dw.dwd_play_end;
SELECT 'dws_song_play_daily', count(*), count(DISTINCT dt) FROM music_dw.dws_song_play_daily;
SELECT 'dws_funnel_daily', count(*), count(DISTINCT dt) FROM music_dw.dws_funnel_daily;
SELECT 'dws_search_daily', count(*), count(DISTINCT dt) FROM music_dw.dws_search_daily;
SELECT 'dws_revenue_daily', count(*), count(DISTINCT dt) FROM music_dw.dws_revenue_daily;
SELECT 'ads_daily_overview', count(*), count(DISTINCT dt) FROM music_dw.ads_daily_overview;
SELECT 'ads_song_ranking', count(*), count(DISTINCT dt) FROM music_dw.ads_song_ranking;
SELECT 'ads_funnel', count(*), count(DISTINCT dt) FROM music_dw.ads_funnel;
SELECT 'ads_revenue', count(*), count(DISTINCT dt) FROM music_dw.ads_revenue;
" 2>/dev/null | grep -E '^[a-z_0-9]+[[:space:]]'

echo "########## E. 关键指标（31 天 / 单日）##########"
$HIVE -S -e "
SELECT 'complete_rate_all', ROUND(SUM(complete_cnt)/SUM(play_start_cnt),4)
FROM music_dw.dws_song_play_daily;
SELECT 'revenue_31d', SUM(net_revenue), ROUND(AVG(arpu),2), ROUND(AVG(refund_rate),4), ROUND(AVG(ad_ctr),4)
FROM music_dw.ads_revenue;
SELECT 'search_31d', SUM(zero_result_pv), ROUND(AVG(zero_result_rate),4), COUNT(DISTINCT keyword)
FROM music_dw.dws_search_daily;
" 2>/dev/null | grep -E '^[a-z_0-9]+[[:space:]]'

echo "[verify_v9] DONE"
