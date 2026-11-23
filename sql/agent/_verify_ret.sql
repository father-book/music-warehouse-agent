-- 独立验算：10-05 cohort 的 D1~D7，两种活跃定义对照
-- ① 启动口径（与 ads_user_retention 现行口径一致）
SELECT 'V_LAUNCH' AS k, DATEDIFF(a.dt, f.c) AS day_n,
       COUNT(DISTINCT a.e) AS retained
FROM (
    SELECT e, MIN(dt) AS c
    FROM (SELECT dt, COALESCE(NULLIF(user_id, ''), device_id) AS e
          FROM music_dw.dwd_session
          WHERE dt BETWEEN '2026-10-05' AND '2026-11-08' AND event_name = 'app_launch') x
    GROUP BY e
) f
JOIN (SELECT DISTINCT dt, COALESCE(NULLIF(user_id, ''), device_id) AS e
      FROM music_dw.dwd_session
      WHERE dt BETWEEN '2026-10-05' AND '2026-11-08' AND event_name = 'app_launch') a
  ON f.e = a.e
WHERE f.c = '2026-10-05' AND DATEDIFF(a.dt, f.c) BETWEEN 1 AND 7
GROUP BY DATEDIFF(a.dt, f.c);

-- ② 播放口径（旧口径，用于解释 42.42% 是怎么来的）
SELECT 'V_PLAY' AS k, DATEDIFF(a.dt, f.c) AS day_n,
       COUNT(DISTINCT a.e) AS retained
FROM (
    SELECT e, MIN(dt) AS c
    FROM (SELECT dt, COALESCE(NULLIF(user_id, ''), device_id) AS e
          FROM music_dw.dwd_play_start
          WHERE dt BETWEEN '2026-10-05' AND '2026-11-08') x
    GROUP BY e
) f
JOIN (SELECT DISTINCT dt, COALESCE(NULLIF(user_id, ''), device_id) AS e
      FROM music_dw.dwd_play_start
      WHERE dt BETWEEN '2026-10-05' AND '2026-11-08') a
  ON f.e = a.e
WHERE f.c = '2026-10-05' AND DATEDIFF(a.dt, f.c) BETWEEN 1 AND 7
GROUP BY DATEDIFF(a.dt, f.c);

-- ③ 两种口径下 10-05 cohort 的规模
SELECT 'V_SIZE' AS k, 'launch' AS src, COUNT(*) AS cohort_size
FROM (
    SELECT e, MIN(dt) AS c
    FROM (SELECT dt, COALESCE(NULLIF(user_id, ''), device_id) AS e
          FROM music_dw.dwd_session
          WHERE dt BETWEEN '2026-10-05' AND '2026-11-08' AND event_name = 'app_launch') x
    GROUP BY e
) t WHERE c = '2026-10-05'
UNION ALL
SELECT 'V_SIZE', 'play', COUNT(*)
FROM (
    SELECT e, MIN(dt) AS c
    FROM (SELECT dt, COALESCE(NULLIF(user_id, ''), device_id) AS e
          FROM music_dw.dwd_play_start
          WHERE dt BETWEEN '2026-10-05' AND '2026-11-08') x
    GROUP BY e
) t WHERE c = '2026-10-05';
