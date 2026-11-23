-- 断言 1：曲风起播之和 = 全站起播（同「分省 DAU 之和 = 全站 DAU」的地域断言）
SELECT 'SUM_CHECK' AS k, 'genre' AS src, SUM(play_start_cnt) AS v FROM music_dw.ads_genre_dist
UNION ALL
SELECT 'SUM_CHECK', 'user_daily', SUM(play_start_cnt) FROM music_dw.dws_user_daily;

-- 断言 2：维表曲风的「曲目数」分布（文档里那个 64%/25%/17%/4% 到底指什么）
SELECT 'DIM_GENRE' AS k, genre, COUNT(*) AS songs,
       ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS song_share_pct
FROM music_dw.dim_song
GROUP BY genre
ORDER BY songs DESC;

-- 断言 3：榜单每日 20 首、35 天齐
SELECT 'RANK_DAYS' AS k, COUNT(DISTINCT dt) AS days, COUNT(*) AS rows_all
FROM music_dw.ads_song_ranking;
