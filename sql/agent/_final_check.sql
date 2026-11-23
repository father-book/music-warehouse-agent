-- 文档最终数字复核
SELECT 'F_UV' AS k,
       COUNT(DISTINCT COALESCE(NULLIF(user_id, ''), device_id)) AS uv_window,
       COUNT(DISTINCT session_id) AS sessions
FROM music_dw.dwd_session
WHERE dt BETWEEN '2026-10-05' AND '2026-11-08' AND event_name = 'app_launch';

SELECT 'F_CORE' AS k,
       SUM(play_start_cnt) AS starts,
       SUM(complete_cnt)   AS completes,
       ROUND(SUM(complete_cnt) * 100.0 / SUM(play_start_cnt), 2) AS complete_pct
FROM music_dw.dws_song_play_daily;

SELECT 'F_FUNNEL' AS k,
       SUM(impression_cnt) AS imp, SUM(click_cnt) AS clk,
       SUM(complete_cnt) AS comp, SUM(interact_cnt) AS inter, SUM(pay_cnt) AS pays,
       ROUND(SUM(click_cnt) * 100.0 / SUM(impression_cnt), 2) AS ctr_pct,
       ROUND(SUM(pay_cnt) * 100.0 / SUM(impression_cnt), 3) AS pay_conv_pct,
       ROUND(SUM(interact_cnt) * 100.0 / SUM(play_start_cnt), 2) AS interact_pct
FROM music_dw.ads_funnel;

SELECT 'F_REV' AS k, SUM(net_revenue) AS net, SUM(pay_user_cnt) AS pay_uv,
       ROUND(SUM(net_revenue) / SUM(pay_user_cnt), 2) AS arppu_daily_avg
FROM music_dw.ads_revenue;
