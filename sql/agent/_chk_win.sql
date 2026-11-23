-- count_distinct 类指标的「窗口级」值（不能拿逐日之和当基线）
SELECT 'W_PLAY' AS k,
       COUNT(DISTINCT COALESCE(NULLIF(user_id,''), device_id)) AS uv_win,
       COUNT(DISTINCT user_id) AS uv_win_naked
FROM music_dw.dwd_play_start;

SELECT 'W_PAY' AS k,
       COUNT(DISTINCT COALESCE(NULLIF(user_id,''), device_id)) AS uv_win,
       COUNT(DISTINCT user_id) AS uv_win_naked
FROM music_dw.dwd_pay
WHERE event_name IN ('vip_subscribe','order_pay');

SELECT 'W_SEARCH' AS k,
       COUNT(DISTINCT COALESCE(NULLIF(user_id,''), device_id)) AS uv_win
FROM music_dw.dwd_interaction WHERE event_name = 'search';
