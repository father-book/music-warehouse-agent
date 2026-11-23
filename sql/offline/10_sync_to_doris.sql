-- 离线 ADS → Doris（全量刷新，7 张表）
-- 用 INSERT OVERWRITE 而不是 INSERT INTO：UNIQUE 表下前者是原子替换，
-- 后者只覆盖同 key 的行 —— cohort 数变少时旧行会残留下来。
-- 由 code/sync_doris.py 解析执行（去掉注释行后按分号切分）。
-- 前置：music_offline.ads_funnel / ads_revenue 已由 sql/doris_ads_ext.sql 建好。
INSERT OVERWRITE TABLE music_offline.ads_daily_overview
SELECT CAST(dt AS DATE), dau, session_cnt, play_start_cnt, play_end_cnt, complete_cnt,
       complete_rate, incomplete_rate, avg_progress, avg_play_per_user, login_user_cnt, vip_user_cnt
FROM hive_catalog.music_dw.ads_daily_overview;

INSERT OVERWRITE TABLE music_offline.ads_song_ranking
SELECT CAST(dt AS DATE), rank_no, song_id, song_name, singer, genre,
       play_start_cnt, complete_cnt, complete_rate, avg_progress
FROM hive_catalog.music_dw.ads_song_ranking;

INSERT OVERWRITE TABLE music_offline.ads_genre_dist
SELECT CAST(dt AS DATE), genre, play_start_cnt, start_share, complete_rate, user_cnt
FROM hive_catalog.music_dw.ads_genre_dist;

INSERT OVERWRITE TABLE music_offline.ads_province_dist
SELECT CAST(dt AS DATE), province, dau, play_start_cnt, session_cnt, dau_share
FROM hive_catalog.music_dw.ads_province_dist;

INSERT OVERWRITE TABLE music_offline.ads_user_retention
SELECT CAST(cohort_date AS DATE), day_n, cohort_size, retained_users, retention_rate
FROM hive_catalog.music_dw.ads_user_retention;

-- 12 号扩表新增的两张 ADS
INSERT OVERWRITE TABLE music_offline.ads_funnel
SELECT CAST(dt AS DATE), impression_cnt, click_cnt, ctr,
       play_start_cnt, click_to_play_rate, complete_cnt, complete_rate,
       interact_cnt, interact_rate, pay_cnt, pay_conv_rate, pay_cnt_dod
FROM hive_catalog.music_dw.ads_funnel;

INSERT OVERWRITE TABLE music_offline.ads_revenue
SELECT CAST(dt AS DATE), gross_revenue, refund_amount, net_revenue, ad_revenue,
       pay_user_cnt, arpu, refund_rate, ad_ctr, net_revenue_dod, cum_net_revenue
FROM hive_catalog.music_dw.ads_revenue;
