-- =============================================================================
-- verify_v6.sql —— 验证 v6 造数修补后的两个核心指标
-- 执行：hive -f sql/offline/verify_v6.sql     （约 2 分钟，扫 3000 万行两次）
--
-- 只看三件事：
--   ① ODS 总量是不是 3000 万 / 30 天（确认造数真的跑完了）
--   ② 留存曲线是不是「首日陡降 + 长尾」—— 改前是每天精确 -7313 的直线
--   ③ 曲风完播率是不是四条几乎重合 —— 改前跨度 7.5 个百分点
-- =============================================================================

-- ① 总量与分区数
SELECT 'total' AS k, COUNT(*) AS v FROM music_dw.ods_music_event
WHERE dt BETWEEN '2026-10-05' AND '2026-11-03'
UNION ALL
SELECT 'days', COUNT(DISTINCT dt) FROM music_dw.ods_music_event
WHERE dt BETWEEN '2026-10-05' AND '2026-11-03'
UNION ALL
SELECT 'songs', COUNT(DISTINCT get_json_object(properties, '$.song_id'))
FROM music_dw.ods_music_event
WHERE dt BETWEEN '2026-10-05' AND '2026-11-03'
UNION ALL
SELECT 'users', COUNT(DISTINCT user_id) FROM music_dw.ods_music_event
WHERE dt BETWEEN '2026-10-05' AND '2026-11-03';

-- ② 留存：D0 同期群（2026-10-05 首次活跃）的 day1~day7
SELECT f.cohort_date,
       DATEDIFF(a.dt, f.cohort_date) AS day_n,
       COUNT(DISTINCT a.user_id)     AS retained
FROM (SELECT user_id, MIN(dt) AS cohort_date
      FROM music_dw.ods_music_event
      WHERE event_name = 'play_start' AND dt BETWEEN '2026-10-05' AND '2026-11-03'
      GROUP BY user_id) f
JOIN (SELECT DISTINCT user_id, dt
      FROM music_dw.ods_music_event
      WHERE event_name = 'play_start' AND dt BETWEEN '2026-10-05' AND '2026-11-03') a
  ON f.user_id = a.user_id
WHERE f.cohort_date = '2026-10-05'
  AND DATEDIFF(a.dt, f.cohort_date) BETWEEN 1 AND 7
GROUP BY f.cohort_date, DATEDIFF(a.dt, f.cohort_date)
ORDER BY day_n;

-- ③ 曲风完播率（只看 play_end 里的 progress>=0.9 占比，各曲风起播量固定，可直接比较）
SELECT so.genre,
       COUNT(*) AS ends,
       SUM(IF(CAST(get_json_object(o.properties, '$.position') AS INT) * 1.0 /
              CAST(get_json_object(o.properties, '$.duration') AS INT) >= 0.9, 1, 0)) AS completes,
       ROUND(SUM(IF(CAST(get_json_object(o.properties, '$.position') AS INT) * 1.0 /
              CAST(get_json_object(o.properties, '$.duration') AS INT) >= 0.9, 1, 0))
             / COUNT(*), 4) AS complete_ratio
FROM music_dw.ods_music_event o
JOIN music_dw.dim_song so ON get_json_object(o.properties, '$.song_id') = so.song_id
WHERE o.event_name = 'play_end' AND o.dt BETWEEN '2026-10-05' AND '2026-11-03'
GROUP BY so.genre
ORDER BY ends DESC;
