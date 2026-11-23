-- 11-08 单日：日活口径三种来源对比 + 会话覆盖 + 付费游客
SELECT 'S1_LAUNCH' AS k,
       COUNT(*) AS rows_,
       COUNT(DISTINCT COALESCE(NULLIF(user_id, ''), device_id)) AS entities,
       COUNT(DISTINCT session_id) AS sessions
FROM music_dw.dwd_session WHERE dt = '2026-11-08' AND event_name = 'app_launch'
UNION ALL
SELECT 'S1_PLAY',
       COUNT(*),
       COUNT(DISTINCT COALESCE(NULLIF(user_id, ''), device_id)),
       COUNT(DISTINCT session_id)
FROM music_dw.dwd_play_start WHERE dt = '2026-11-08'
UNION ALL
SELECT 'S1_ALL_EVENTS',
       COUNT(*),
       COUNT(DISTINCT COALESCE(NULLIF(user_id, ''), device_id)),
       COUNT(DISTINCT session_id)
FROM music_dw.ods_music_event WHERE dt = '2026-11-08';

-- 付费事件里有没有游客（游客无法付费？需证实）
SELECT 'S2_PAY_GUEST' AS k,
       COUNT(*) AS pay_rows,
       SUM(IF(user_id = '' OR user_id IS NULL, 1, 0)) AS guest_rows,
       COUNT(DISTINCT COALESCE(NULLIF(user_id, ''), device_id)) AS pay_entities
FROM music_dw.dwd_pay WHERE dt = '2026-11-08';

-- 会话里有没有「只启动不播放」的会话（决定 session_cnt 走哪张表）
SELECT 'S3_SESS' AS k,
       COUNT(DISTINCT session_id) AS launch_sessions,
       COUNT(DISTINCT IF(has_play = 1, session_id, NULL)) AS with_play
FROM (
    SELECT s.session_id,
           MAX(IF(p.session_id IS NOT NULL, 1, 0)) AS has_play
    FROM (SELECT DISTINCT session_id FROM music_dw.dwd_session
          WHERE dt = '2026-11-08' AND event_name = 'app_launch') s
    LEFT JOIN (SELECT DISTINCT session_id FROM music_dw.dwd_play_start
               WHERE dt = '2026-11-08') p
      ON s.session_id = p.session_id
    GROUP BY s.session_id
) t;
