-- avg_progress 三种算法的对照，确认 0.9389 / 0.9329 到底指哪一个
SELECT 'P_RAW' AS k,
       ROUND(AVG(play_progress), 5) AS play_weighted_raw,
       COUNT(*) AS ends
FROM music_dw.dwd_play_end WHERE dt='2026-10-15';

SELECT 'P_SONG' AS k,
       ROUND(AVG(avg_progress), 5) AS song_equal,
       ROUND(SUM(avg_progress*play_end_cnt)/SUM(play_end_cnt), 5) AS play_weighted,
       COUNT(*) AS songs
FROM music_dw.dws_song_play_daily WHERE dt='2026-10-15';

SELECT 'P_ADS' AS k, avg_progress, complete_rate, dau, session_cnt
FROM music_dw.ads_daily_overview WHERE dt='2026-10-15';

SELECT 'P_WIN' AS k,
       ROUND(AVG(avg_progress), 5) AS song_equal_win,
       ROUND(SUM(avg_progress*play_end_cnt)/SUM(play_end_cnt), 5) AS play_weighted_win
FROM music_dw.dws_song_play_daily;
