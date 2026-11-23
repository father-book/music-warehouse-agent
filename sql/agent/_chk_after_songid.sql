SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;

-- 1) 行数：全部 35 天分区
SELECT 'ROWS' AS k, 'dws_genre_daily' AS t, COUNT(*) AS n FROM music_dw.dws_genre_daily
UNION ALL SELECT 'ROWS', 'ads_genre_dist',    COUNT(*) FROM music_dw.ads_genre_dist
UNION ALL SELECT 'ROWS', 'ads_song_ranking',  COUNT(*) FROM music_dw.ads_song_ranking
UNION ALL SELECT 'ROWS', 'ads_daily_overview',COUNT(*) FROM music_dw.ads_daily_overview
UNION ALL SELECT 'ROWS', 'ads_province_dist', COUNT(*) FROM music_dw.ads_province_dist
UNION ALL SELECT 'ROWS', 'ads_user_retention',COUNT(*) FROM music_dw.ads_user_retention;

-- 2) 曲风完播率（按起播量加权，10 月整月）
SELECT 'GENRE' AS k, genre,
       COUNT(*) AS days,
       SUM(play_start_cnt) AS starts,
       ROUND(SUM(play_start_cnt * complete_rate) / SUM(play_start_cnt) * 100, 2) AS weighted_rate_pct
FROM music_dw.ads_genre_dist
GROUP BY genre
ORDER BY weighted_rate_pct DESC;

-- 3) 各曲风起播占比（10 月整月）
SELECT 'SHARE' AS k, genre,
       ROUND(SUM(play_start_cnt) * 100.0 / SUM(SUM(play_start_cnt)) OVER (), 2) AS share_pct
FROM music_dw.ads_genre_dist
GROUP BY genre
ORDER BY share_pct DESC;

-- 4) 榜单：累计去重歌数 + 维表字段是否还为空
SELECT 'RANK' AS k,
       COUNT(DISTINCT song_id) AS distinct_songs,
       SUM(CASE WHEN song_name IS NULL OR song_name = '' THEN 1 ELSE 0 END) AS empty_name,
       SUM(CASE WHEN genre IS NULL OR genre = '' THEN 1 ELSE 0 END) AS empty_genre
FROM music_dw.ads_song_ranking;

-- 5) 榜单抽样：11-08 前三
SELECT 'RANK_TOP3' AS k, dt, rank_no, song_id, song_name, genre,
       play_start_cnt, ROUND(complete_rate * 100, 2) AS rate_pct
FROM music_dw.ads_song_ranking
WHERE dt = '2026-11-08' AND rank_no <= 3
ORDER BY rank_no;
