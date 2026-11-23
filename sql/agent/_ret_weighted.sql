SELECT 'W_RET' AS k, day_n,
       SUM(retained_users) AS retained,
       SUM(cohort_size)    AS sizes,
       ROUND(SUM(retained_users) * 100.0 / SUM(cohort_size), 2) AS weighted_pct
FROM music_dw.ads_user_retention
WHERE day_n IN (1, 7)
GROUP BY day_n
ORDER BY day_n;

SELECT 'W_RET_EX0' AS k, day_n,
       ROUND(SUM(retained_users) * 100.0 / SUM(cohort_size), 2) AS weighted_pct
FROM music_dw.ads_user_retention
WHERE day_n IN (1, 7) AND cohort_date <> '2026-10-05'
GROUP BY day_n
ORDER BY day_n;
