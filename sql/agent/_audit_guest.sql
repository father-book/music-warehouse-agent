-- A. 游客占比：11-08 起播事件里 user_id='' 的比例
SELECT 'A_GUEST' AS k,
       COUNT(*)                                                     AS total_starts,
       SUM(IF(user_id = '' OR user_id IS NULL, 1, 0))               AS guest_starts,
       ROUND(SUM(IF(user_id = '' OR user_id IS NULL, 1, 0)) * 100.0 / COUNT(*), 2) AS guest_pct,
       COUNT(DISTINCT IF(user_id = '', device_id, NULL))            AS guest_devices,
       COUNT(DISTINCT COALESCE(NULLIF(user_id, ''), device_id))     AS dau_new,
       COUNT(DISTINCT user_id)                                      AS dau_old
FROM music_dw.dwd_play_start WHERE dt = '2026-11-08';

-- B. dws_genre_daily.user_cnt 的两种算法对比（11-08）
SELECT 'B_GENRE_UV' AS k, so.genre,
       COUNT(DISTINCT d.user_id)                                 AS uv_old,
       COUNT(DISTINCT COALESCE(NULLIF(d.user_id, ''), d.device_id)) AS uv_new
FROM music_dw.dwd_play_start d
JOIN music_dw.dim_song so ON d.song_id = so.song_id
WHERE d.dt = '2026-11-08'
GROUP BY so.genre;

-- C. 只浏览不播放的用户是否被 DAU 漏掉（11-08）
--    Hive 不支持 SELECT 里的标量子查询，改 UNION ALL
SELECT 'C_DAU_COVER' AS k, 'play_start' AS src,
       COUNT(DISTINCT COALESCE(NULLIF(user_id, ''), device_id)) AS v
FROM music_dw.dwd_play_start WHERE dt = '2026-11-08'
UNION ALL
SELECT 'C_DAU_COVER', 'session',
       COUNT(DISTINCT COALESCE(NULLIF(user_id, ''), device_id))
FROM music_dw.dwd_session WHERE dt = '2026-11-08'
UNION ALL
SELECT 'C_DAU_COVER', 'page_view',
       COUNT(DISTINCT COALESCE(NULLIF(user_id, ''), device_id))
FROM music_dw.dwd_page_view WHERE dt = '2026-11-08';

-- D. 留存 cohort 里 '' 这个实体是否自成一档
SELECT 'D_EMPTY_COHORT' AS k,
       COUNT(*) AS cnt, MIN(c) AS min_dt, MAX(c) AS max_dt
FROM (
    SELECT user_id, MIN(dt) AS c
    FROM music_dw.dwd_play_start
    WHERE dt BETWEEN '2026-10-05' AND '2026-11-08'
    GROUP BY user_id
) t WHERE user_id = '';

-- E. 搜索 UV 的游客压缩（11-08）
SELECT 'E_SEARCH_UV' AS k,
       COUNT(*)                                                      AS search_pv,
       COUNT(DISTINCT IF(event_name = 'search', user_id, NULL))       AS uv_old,
       COUNT(DISTINCT IF(event_name = 'search',
                         COALESCE(NULLIF(user_id, ''), device_id), NULL)) AS uv_new
FROM music_dw.dwd_search WHERE dt = '2026-11-08';
