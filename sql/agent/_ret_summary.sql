-- 留存的三组口径：全 cohort 均值 / 月中 cohort / 首日 cohort
SELECT 'R_AVG' AS k, day_n,
       COUNT(*) AS cohorts,
       ROUND(AVG(retention_rate) * 100, 2) AS avg_pct,
       ROUND(MIN(retention_rate) * 100, 2) AS min_pct,
       ROUND(MAX(retention_rate) * 100, 2) AS max_pct
FROM music_dw.ads_user_retention
GROUP BY day_n
ORDER BY day_n;

SELECT 'R_MID' AS k, day_n, cohort_size, retained_users,
       ROUND(retention_rate * 100, 2) AS pct
FROM music_dw.ads_user_retention
WHERE cohort_date = '2026-10-20'
ORDER BY day_n;

SELECT 'R_LAST' AS k, day_n, cohort_size, retained_users,
       ROUND(retention_rate * 100, 2) AS pct
FROM music_dw.ads_user_retention
WHERE cohort_date = '2026-10-30'
ORDER BY day_n;
