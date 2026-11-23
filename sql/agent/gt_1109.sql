-- ============================================================================
-- 2026-11-09 重造后：全量 ground truth 采集
-- 用途：一次性取齐所有需要写进文档/语义层/评测集的真值
-- 运行：hive -f gt_1109.sql 2>/dev/null | grep -E '^[A-Z_]+[[:space:]]'
-- ============================================================================

-- 1. 规模
SELECT 'SCALE' AS k,
       COUNT(*)                              AS ods_rows,
       COUNT(DISTINCT dt)                    AS days,
       COUNT(DISTINCT user_id)               AS users,
       COUNT(DISTINCT session_id)            AS sessions,
       COUNT(DISTINCT province)              AS provinces
FROM music_dw.ods_music_event;

-- 2. 大盘（35 天合计 + 日均）
SELECT 'OVERVIEW' AS k,
       COUNT(*)                                        AS days,
       SUM(dau)                                        AS dau_sum,
       ROUND(AVG(dau), 2)                              AS dau_avg,
       MIN(dau)                                        AS dau_min,
       MAX(dau)                                        AS dau_max,
       SUM(session_cnt)                                AS sess_sum,
       SUM(play_start_cnt)                             AS start_sum,
       SUM(play_end_cnt)                               AS end_sum,
       SUM(complete_cnt)                               AS complete_sum,
       ROUND(SUM(complete_cnt) / SUM(play_start_cnt), 6) AS complete_rate_weighted,
       ROUND(AVG(complete_rate), 6)                    AS complete_rate_naive,
       ROUND(SUM(avg_progress * play_end_cnt) / SUM(play_end_cnt), 6) AS avg_progress_weighted,
       ROUND(AVG(avg_progress), 6)                     AS avg_progress_naive,
       SUM(login_user_cnt)                             AS login_sum,
       SUM(vip_user_cnt)                               AS vip_sum
FROM music_dw.ads_daily_overview;

-- 3. 单日基准（10-15，用于与 35 天口径对照）
SELECT 'DAY_1015' AS k,
       dt, dau, session_cnt, play_start_cnt, play_end_cnt, complete_cnt,
       complete_rate, incomplete_rate, avg_progress, avg_play_per_user
FROM music_dw.ads_daily_overview
WHERE dt = '2026-10-15';

-- 4. 收入
SELECT 'REVENUE' AS k,
       COUNT(*)                          AS days,
       ROUND(SUM(gross_revenue), 2)      AS gross,
       ROUND(SUM(refund_amount), 2)      AS refund,
       ROUND(SUM(net_revenue), 2)        AS net,
       ROUND(SUM(ad_revenue), 2)         AS ad_rev,
       SUM(pay_user_cnt)                 AS pay_sum,
       ROUND(AVG(pay_user_cnt), 2)       AS pay_avg,
       ROUND(SUM(net_revenue) / SUM(pay_user_cnt), 4) AS arppu_recalc,
       MAX(cum_net_revenue)              AS cum_net
FROM music_dw.ads_revenue;

-- 5. 漏斗
SELECT 'FUNNEL' AS k,
       SUM(impression_cnt)   AS imp,
       SUM(click_cnt)        AS clk,
       SUM(play_start_cnt)   AS ps,
       SUM(complete_cnt)     AS cc,
       SUM(interact_cnt)     AS itc,
       SUM(pay_cnt)          AS pay,
       ROUND(SUM(click_cnt) / SUM(impression_cnt), 6)        AS ctr,
       ROUND(SUM(play_start_cnt) / SUM(click_cnt), 6)        AS c2p,
       ROUND(SUM(complete_cnt) / SUM(play_start_cnt), 6)     AS p2c,
       ROUND(SUM(interact_cnt) / SUM(play_start_cnt), 6)     AS p2i,
       ROUND(SUM(pay_cnt) / SUM(impression_cnt), 6)          AS pay_conv
FROM music_dw.ads_funnel;

-- 6. 留存
SELECT 'RETENTION' AS k,
       day_n,
       COUNT(*)                          AS cohorts,
       ROUND(AVG(retention_rate), 6)     AS avg_rt,
       ROUND(MIN(retention_rate), 6)     AS min_rt,
       ROUND(MAX(retention_rate), 6)     AS max_rt
FROM music_dw.ads_user_retention
GROUP BY day_n ORDER BY day_n;

-- 7. 维度基数（Hive 不支持 SELECT 列表里的标量子查询，拆成 UNION ALL）
SELECT 'DIM_PROV'  AS k, COUNT(DISTINCT province) AS n, COUNT(*) AS rows FROM music_dw.ads_province_dist
UNION ALL SELECT 'DIM_GENRE', COUNT(DISTINCT genre), COUNT(*) FROM music_dw.ads_genre_dist
UNION ALL SELECT 'DIM_RANK', COUNT(DISTINCT dt), COUNT(*) FROM music_dw.ads_song_ranking
UNION ALL SELECT 'DIM_RET', COUNT(DISTINCT day_n), COUNT(*) FROM music_dw.ads_user_retention
UNION ALL SELECT 'DIM_USER', COUNT(DISTINCT user_id), COUNT(*) FROM music_dw.dim_user;

-- 8. 省份合计 vs 全站（验证跨省重复计数已消除，两者必须相等）
SELECT 'PROV_TIE' AS k, 'prov_dau_sum' AS item, SUM(dau) AS v
FROM music_dw.ads_province_dist WHERE dt = '2026-11-08'
UNION ALL SELECT 'PROV_TIE', 'overview_dau', dau
FROM music_dw.ads_daily_overview WHERE dt = '2026-11-08'
UNION ALL SELECT 'PROV_TIE', 'share_sum', ROUND(SUM(dau_share), 6)
FROM music_dw.ads_province_dist WHERE dt = '2026-11-08';

-- 9. 时区：dt 与 event_ts 跨日记录（必须 0）
SELECT 'TZ_CHECK' AS k,
       COUNT(*) AS bad_rows
FROM music_dw.dwd_play_end
WHERE SUBSTR(CAST(event_ts AS STRING), 1, 10) <> dt;
