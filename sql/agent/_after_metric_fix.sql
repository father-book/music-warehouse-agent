-- 修正后全量数字采集
SELECT 'G_DAU' AS k, SUM(dau) AS dau_sum, ROUND(AVG(dau), 1) AS dau_avg,
       MIN(dau) AS dau_min, MAX(dau) AS dau_max,
       SUM(session_cnt) AS sess_sum, SUM(play_start_cnt) AS start_sum,
       ROUND(AVG(avg_play_per_user), 2) AS avg_play_per_user
FROM music_dw.dws_user_daily;

SELECT 'G_PROV' AS k, SUM(o.dau) AS site_dau, SUM(p.prov_dau) AS prov_dau,
       SUM(o.dau) - SUM(p.prov_dau) AS diff
FROM music_dw.ads_daily_overview o
JOIN (SELECT dt, SUM(dau) AS prov_dau FROM music_dw.ads_province_dist GROUP BY dt) p
  ON o.dt = p.dt;

SELECT 'G_REV' AS k, SUM(gross_revenue) AS gross, SUM(refund_amount) AS refund,
       SUM(net_revenue) AS net, SUM(ad_revenue) AS ad,
       SUM(pay_user_cnt) AS pay_uv_sum
FROM music_dw.ads_revenue;

SELECT 'G_GENRE' AS k, genre, SUM(play_start_cnt) AS starts,
       ROUND(SUM(play_start_cnt * complete_rate) / SUM(play_start_cnt) * 100, 2) AS wrate35,
       ROUND(SUM(user_cnt) * 1.0 / 35, 1) AS avg_daily_uv
FROM music_dw.ads_genre_dist
GROUP BY genre
ORDER BY wrate35 DESC;

SELECT 'G_RET' AS k, COUNT(*) AS rows_, COUNT(DISTINCT cohort_date) AS cohorts,
       MIN(cohort_date) AS min_c, MAX(cohort_date) AS max_c
FROM music_dw.ads_user_retention;

SELECT 'G_RET0' AS k, day_n, cohort_size, retained_users,
       ROUND(retention_rate * 100, 2) AS pct
FROM music_dw.ads_user_retention
WHERE cohort_date = '2026-10-05'
ORDER BY day_n;

SELECT 'G_ROWS' AS k, 'dws_user_daily' AS t, COUNT(*) AS n FROM music_dw.dws_user_daily
UNION ALL SELECT 'G_ROWS', 'dws_genre_daily',    COUNT(*) FROM music_dw.dws_genre_daily
UNION ALL SELECT 'G_ROWS', 'dws_province_daily', COUNT(*) FROM music_dw.dws_province_daily
UNION ALL SELECT 'G_ROWS', 'dws_search_daily',   COUNT(*) FROM music_dw.dws_search_daily
UNION ALL SELECT 'G_ROWS', 'dws_revenue_daily',  COUNT(*) FROM music_dw.dws_revenue_daily
UNION ALL SELECT 'G_ROWS', 'ads_daily_overview', COUNT(*) FROM music_dw.ads_daily_overview
UNION ALL SELECT 'G_ROWS', 'ads_province_dist',  COUNT(*) FROM music_dw.ads_province_dist
UNION ALL SELECT 'G_ROWS', 'ads_genre_dist',     COUNT(*) FROM music_dw.ads_genre_dist
UNION ALL SELECT 'G_ROWS', 'ads_user_retention', COUNT(*) FROM music_dw.ads_user_retention
UNION ALL SELECT 'G_ROWS', 'ads_revenue',        COUNT(*) FROM music_dw.ads_revenue;

SELECT 'G_LAST3' AS k, dt, dau, session_cnt, play_start_cnt,
       ROUND(complete_rate * 100, 2) AS cr, avg_progress, avg_play_per_user,
       login_user_cnt, vip_user_cnt
FROM music_dw.ads_daily_overview
WHERE dt >= '2026-11-06'
ORDER BY dt;
