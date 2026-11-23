-- =============================================================================
-- datart_views.sql —— DataArt 五个数据视图的查询 SQL
-- 数据源：Doris 192.168.128.163:9030，库 music_offline（MySQL 协议）
-- 用法：DataArt → 数据视图 → 新建 → SQL 模式，逐段粘贴
--
-- 约定：
--   · 中文别名必须用反引号包住（含 ( ) % 等字符，不加会语法错误）
--   · 比率一律 ×100，视图层一次算好，图表不用再设百分比格式
--   · dt / cohort_date 是 DATE 类型，作图时把「日期级别」设为「日」
-- =============================================================================

-- ① 日报概览   ← music_offline.ads_daily_overview
SELECT dt                              AS `日期`,
       dau                             AS `日活用户`,
       session_cnt                     AS `会话数`,
       play_start_cnt                  AS `起播次数`,
       play_end_cnt                    AS `结束次数`,
       complete_cnt                    AS `完播次数`,
       ROUND(complete_rate * 100, 2)   AS `完播率(%)`,
       ROUND(incomplete_rate * 100, 2) AS `未正常结束率(%)`,
       ROUND(avg_progress * 100, 2)    AS `平均播放进度(%)`,
       avg_play_per_user               AS `人均起播`,
       login_user_cnt                  AS `登录用户数`,
       vip_user_cnt                    AS `会员用户数`
FROM music_offline.ads_daily_overview;

-- ② 歌曲榜   ← music_offline.ads_song_ranking
SELECT dt                             AS `日期`,
       rank_no                        AS `排名`,
       song_name                      AS `歌曲`,
       singer                         AS `歌手`,
       genre                          AS `曲风`,
       play_start_cnt                 AS `起播次数`,
       complete_cnt                   AS `完播次数`,
       ROUND(complete_rate * 100, 2)  AS `完播率(%)`,
       ROUND(avg_progress * 100, 2)   AS `平均进度(%)`
FROM music_offline.ads_song_ranking;

-- ③ 曲风分布   ← music_offline.ads_genre_dist
SELECT dt                             AS `日期`,
       genre                          AS `曲风`,
       play_start_cnt                 AS `起播次数`,
       ROUND(start_share * 100, 2)    AS `起播占比(%)`,
       ROUND(complete_rate * 100, 2)  AS `完播率(%)`,
       user_cnt                       AS `听众人数`
FROM music_offline.ads_genre_dist;

-- ④ 省份分布   ← music_offline.ads_province_dist
SELECT dt                             AS `日期`,
       province                       AS `省份`,
       dau                            AS `日活用户`,
       play_start_cnt                 AS `起播次数`,
       session_cnt                    AS `会话数`,
       ROUND(dau_share * 100, 2)      AS `日活占比(%)`
FROM music_offline.ads_province_dist;

-- ⑤ 用户留存   ← music_offline.ads_user_retention（无 dt 字段）
SELECT cohort_date                        AS `同期群`,
       day_n                              AS `第N天`,
       cohort_size                        AS `群规模`,
       retained_users                     AS `留存人数`,
       ROUND(retention_rate * 100, 2)     AS `留存率(%)`
FROM music_offline.ads_user_retention;

-- ⑥ 可选：只取最新一天的歌曲 Top20（避免把 600 行全画出来）
SELECT rank_no                        AS `排名`,
       song_name                      AS `歌曲`,
       singer                         AS `歌手`,
       genre                          AS `曲风`,
       play_start_cnt                 AS `起播次数`,
       ROUND(complete_rate * 100, 2)  AS `完播率(%)`
FROM music_offline.ads_song_ranking
WHERE dt = (SELECT MAX(dt) FROM music_offline.ads_song_ranking)
ORDER BY rank_no;

-- ⑦ 可选：D0 / D1 两个同期群的留存对比（要画多系列折线就先筛成两条）
--    用子查询取最早两个 cohort，不写死日期，接真实数据源后仍然有效
SELECT cohort_date                        AS `同期群`,
       day_n                              AS `第N天`,
       ROUND(retention_rate * 100, 2)     AS `留存率(%)`
FROM music_offline.ads_user_retention
WHERE cohort_date IN (
    SELECT cohort_date FROM music_offline.ads_user_retention
    GROUP BY cohort_date ORDER BY cohort_date LIMIT 2
)
ORDER BY cohort_date, day_n;

-- ⑧ 转化漏斗   ← music_offline.ads_funnel（12 号扩表新增）
SELECT dt                                  AS `日期`,
       impression_cnt                      AS `曝光`,
       click_cnt                           AS `点击`,
       ROUND(ctr * 100, 2)                 AS `点击率(%)`,
       play_start_cnt                      AS `起播`,
       complete_cnt                        AS `完播`,
       ROUND(complete_rate * 100, 2)       AS `完播率(%)`,
       interact_cnt                        AS `互动`,
       ROUND(interact_rate * 100, 2)       AS `互动率(%)`,
       pay_cnt                             AS `付费`,
       ROUND(pay_conv_rate * 100, 4)       AS `付费转化率(%)`
FROM music_offline.ads_funnel;

-- ⑨ 收入报表   ← music_offline.ads_revenue（12 号扩表新增，商业化看板）
SELECT dt                                  AS `日期`,
       gross_revenue                       AS `毛收入(¥)`,
       refund_amount                       AS `退款(¥)`,
       net_revenue                         AS `净收入(¥)`,
       ad_revenue                          AS `广告收入(¥)`,
       pay_user_cnt                        AS `付费用户数`,
       arpu                                AS `ARPU(¥)`,
       ROUND(refund_rate * 100, 2)         AS `退款率(%)`,
       ROUND(ad_ctr * 100, 2)             AS `广告点击率(%)`,
       cum_net_revenue                     AS `累计净收入(¥)`
FROM music_offline.ads_revenue;
