-- 文档第 6 节「10 月各曲风完播率」的口径：dt 10-05 ~ 10-31 共 27 天
SELECT 'OCT_GENRE' AS k, genre,
       COUNT(*) AS days,
       SUM(play_start_cnt) AS starts,
       ROUND(SUM(play_start_cnt * complete_rate) / SUM(play_start_cnt) * 100, 2) AS weighted_pct,
       ROUND(AVG(complete_rate) * 100, 2) AS simple_avg_pct
FROM music_dw.ads_genre_dist
WHERE dt BETWEEN '2026-10-05' AND '2026-10-31'
GROUP BY genre
ORDER BY weighted_pct DESC;

-- 10 月全站完播率（用于文档口径对照）
SELECT 'OCT_ALL' AS k,
       COUNT(*) AS days,
       ROUND(SUM(complete_cnt) * 100.0 / SUM(play_start_cnt), 2) AS complete_pct,
       ROUND(SUM(avg_progress * play_end_cnt) / SUM(play_end_cnt), 4) AS w_avg_progress
FROM music_dw.ads_daily_overview
WHERE dt BETWEEN '2026-10-05' AND '2026-10-31';

-- 最后三天日报（用于「环比降 X 个百分点」这类叙述）
SELECT 'DAILY' AS k, dt, dau, play_start_cnt,
       ROUND(complete_rate * 100, 2) AS complete_pct,
       avg_progress
FROM music_dw.ads_daily_overview
WHERE dt >= '2026-11-06'
ORDER BY dt;
