-- 10-15 单日：DWD 明细 vs ADS 的交叉校验值（供 docs/23 §9.3 表格刷新）
SELECT 'X_DAU' AS k, COUNT(DISTINCT COALESCE(NULLIF(user_id,''), device_id)) AS dau
FROM music_dw.dwd_session
WHERE dt='2026-10-15' AND event_name='app_launch';

SELECT 'X_STARTS' AS k, COUNT(*) AS starts FROM music_dw.dwd_play_start WHERE dt='2026-10-15';
SELECT 'X_ENDS'   AS k, COUNT(*) AS ends,
       SUM(CASE WHEN end_reason='natural' THEN 1 ELSE 0 END) AS natural_ends
FROM music_dw.dwd_play_end WHERE dt='2026-10-15';

SELECT 'X_PROG' AS k,
       ROUND(SUM(avg_progress*play_end_cnt)/SUM(play_end_cnt), 5) AS w_avg_progress,
       ROUND(SUM(avg_progress)/COUNT(*), 5) AS eq_avg_progress
FROM music_dw.dws_song_play_daily WHERE dt='2026-10-15';
