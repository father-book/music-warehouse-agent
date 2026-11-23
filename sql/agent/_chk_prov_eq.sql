-- 分省 DAU 之和 = 全站 DAU（逐日），取 11-08 与全量两种口径
SELECT 'PROV_EQ_DAY' AS k, o.dt, o.dau AS site_dau, p.prov_dau,
       o.dau - p.prov_dau AS diff
FROM music_dw.ads_daily_overview o
JOIN (SELECT dt, SUM(dau) AS prov_dau FROM music_dw.ads_province_dist GROUP BY dt) p
  ON o.dt = p.dt
WHERE o.dt = '2026-11-08';

SELECT 'PROV_EQ_ALL' AS k,
       SUM(o.dau) AS site_sum,
       SUM(p.prov_dau) AS prov_sum,
       SUM(o.dau) - SUM(p.prov_dau) AS diff
FROM music_dw.ads_daily_overview o
JOIN (SELECT dt, SUM(dau) AS prov_dau FROM music_dw.ads_province_dist GROUP BY dt) p
  ON o.dt = p.dt;
