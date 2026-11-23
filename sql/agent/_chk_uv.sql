-- 文档刷新用的 UV 侧复核
SELECT 'U_PAY' AS k, COUNT(*) AS pay_rows,
       COUNT(DISTINCT dt) AS days,
       MIN(c) AS min_day_uv, MAX(c) AS max_day_uv, ROUND(AVG(c),1) AS avg_day_uv
FROM (SELECT dt, COUNT(DISTINCT COALESCE(NULLIF(user_id,''), device_id)) AS c
      FROM music_dw.dwd_pay GROUP BY dt) t;

SELECT 'U_GENRE' AS k, genre,
       ROUND(SUM(user_cnt)/35.0, 1) AS avg_daily_uv,
       ROUND(SUM(complete_cnt)*100.0/SUM(play_start_cnt), 2) AS w_complete_pct
FROM music_dw.dws_genre_daily GROUP BY genre ORDER BY avg_daily_uv DESC;

SELECT 'U_SEARCH' AS k, ROUND(SUM(search_uv)/35.0, 1) AS avg_daily_uv, SUM(search_uv) AS sum_uv
FROM music_dw.dws_search_daily;

SELECT 'U_RET' AS k, COUNT(*) AS rows_, COUNT(DISTINCT cohort_date) AS cohorts,
       MIN(cohort_date) AS d0, MAX(cohort_date) AS d1
FROM music_dw.ads_user_retention;
