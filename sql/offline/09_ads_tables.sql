-- =============================================================================
-- 09_ads_tables.sql —— ADS：5 张应用层报表
-- 执行：hive --hivevar start_dt=2026-10-05 --hivevar end_dt=2026-11-03 -f 09_ads_tables.sql
--
-- 建表：
--   ads_daily_overview   日报概览（一行一天）
--   ads_song_ranking     歌曲榜（一天 Top20）
--   ads_genre_dist       曲风分布
--   ads_province_dist    省份分布
--   ads_user_retention   留存（同期群，不分 dt 分区）
--
-- ADS 的原则：**只查 DWS，不碰 DWD/ODS**。
--   一张日报概览扫 dws_user_daily(30 行) + dws_song_play_daily(4.46 万行)，
--   而不是扫 525 万行明细。这就是分层的实际收益。
--
-- 一个数据局限的演进（答辩要讲清版本，否则会误读）：
--   W1 时歌曲榜按「播放次数」排没有区分度 —— 真实曲库 1486 首（MusicBrainz 抓取），
--   每首每天约 168 次起播，所以榜单改用「完播率」排序；但当时各歌完播率
--   也几乎相同（0.5133~0.5142），名次差异只剩采样噪声。
--   v9 重造后（docs/16 §十五）：播放量服从 Zipf α=0.6 且热度已打散到全曲库，
--   并给热歌加了完播率偏置（加权/等权实测分叉 3.52 个百分点）—— 播放量与完播率
--   现在都有区分度，榜单不再是纯噪声。排序逻辑沿用完播率，无需改表。
--   但数据终究是模拟的，**不能当业务结论用**。
-- =============================================================================

SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;
SET hive.exec.max.dynamic.partitions = 200;
SET hive.exec.max.dynamic.partitions.pernode = 100;
SET hive.stats.autogather = false;


-- ===========================================================================
-- 1. ads_daily_overview —— 日报概览
-- ===========================================================================
CREATE EXTERNAL TABLE IF NOT EXISTS music_dw.ads_daily_overview
(
    dau               BIGINT COMMENT '日活用户',
    session_cnt       BIGINT COMMENT '会话数',
    play_start_cnt    BIGINT COMMENT '起播次数',
    play_end_cnt      BIGINT COMMENT '结束次数',
    complete_cnt      BIGINT COMMENT '完播次数',
    complete_rate     DOUBLE COMMENT '完播率 = 完播 / 起播',
    incomplete_rate   DOUBLE COMMENT '未正常结束率 = 1 - 结束/起播',
    avg_progress      DOUBLE COMMENT '平均播放进度（仅结束事件）',
    avg_play_per_user DOUBLE COMMENT '人均起播次数',
    login_user_cnt    BIGINT COMMENT '登录用户',
    vip_user_cnt      BIGINT COMMENT '会员用户'
)
COMMENT 'ADS-日报概览'
PARTITIONED BY (dt STRING)
STORED AS ORC
LOCATION 'hdfs://WWC160:9820/warehouse/music_dw/ads_daily_overview'
TBLPROPERTIES ('orc.compress' = 'SNAPPY');

INSERT OVERWRITE TABLE music_dw.ads_daily_overview PARTITION (dt)
SELECT u.dau,
       u.session_cnt,
       u.play_start_cnt,
       NVL(e.play_end_cnt, 0)                                        AS play_end_cnt,
       NVL(e.complete_cnt, 0)                                        AS complete_cnt,
       ROUND(NVL(e.complete_cnt, 0) / NULLIF(u.play_start_cnt, 0), 4) AS complete_rate,
       ROUND(1 - NVL(e.play_end_cnt, 0) / NULLIF(u.play_start_cnt, 0), 4) AS incomplete_rate,
       ROUND(e.avg_progress, 4)                                      AS avg_progress,
       u.avg_play_per_user,
       u.login_user_cnt,
       u.vip_user_cnt,
       u.dt
FROM music_dw.dws_user_daily u
LEFT JOIN (
    SELECT dt,
           SUM(play_end_cnt)  AS play_end_cnt,
           SUM(complete_cnt)  AS complete_cnt,
           -- 【不要写 AVG(avg_progress)】那是「每首歌的平均进度的平均」，
           -- 让播了 10 次的歌和播了 10 万次的歌等权。歌曲层每天有 1400+ 首，
           -- 绝大多数是长尾（起播 1~2 次），等权平均会被长尾系统性拉偏，
           -- 而且偏的方向取决于当天长尾有多少首，指标会莫名其妙地跳。
           -- 按结束次数加权后，结果等于「全体结束事件的进度均值」，
           -- 这才是用户口径的「平均听到多少」。
           SUM(avg_progress * play_end_cnt) / NULLIF(SUM(play_end_cnt), 0) AS avg_progress
    FROM music_dw.dws_song_play_daily
    WHERE dt BETWEEN '${start_dt}' AND '${end_dt}'
    GROUP BY dt
) e ON u.dt = e.dt
WHERE u.dt BETWEEN '${start_dt}' AND '${end_dt}'
DISTRIBUTE BY u.dt;


-- ===========================================================================
-- 2. ads_song_ranking —— 歌曲榜 Top20（按完播率，因为播放量均匀无区分度）
-- ===========================================================================
CREATE EXTERNAL TABLE IF NOT EXISTS music_dw.ads_song_ranking
(
    rank_no        INT,
    song_id        STRING,
    song_name      STRING,
    singer         STRING,
    genre          STRING,
    play_start_cnt BIGINT,
    complete_cnt   BIGINT,
    complete_rate  DOUBLE,
    avg_progress   DOUBLE
)
COMMENT 'ADS-歌曲榜 Top20'
PARTITIONED BY (dt STRING)
STORED AS ORC
LOCATION 'hdfs://WWC160:9820/warehouse/music_dw/ads_song_ranking'
TBLPROPERTIES ('orc.compress' = 'SNAPPY');

INSERT OVERWRITE TABLE music_dw.ads_song_ranking PARTITION (dt)
SELECT t.rank_no, t.song_id, t.song_name, t.singer, t.genre,
       t.play_start_cnt, t.complete_cnt, t.complete_rate, t.avg_progress, t.dt
FROM (
    SELECT d.song_name, d.singer, d.genre,
           s.song_id, s.play_start_cnt, s.complete_cnt, s.complete_rate, s.avg_progress, s.dt,
           ROW_NUMBER() OVER (PARTITION BY s.dt ORDER BY s.complete_rate DESC, s.song_id) AS rank_no
    FROM music_dw.dws_song_play_daily s
    JOIN music_dw.dim_song d ON s.song_id = d.song_id
    WHERE s.dt BETWEEN '${start_dt}' AND '${end_dt}'
      AND s.play_start_cnt >= 20            -- 过滤样本过少的歌，避免 1 次播放 100% 完播刷榜
) t
WHERE t.rank_no <= 20
DISTRIBUTE BY t.dt;


-- ===========================================================================
-- 3. ads_genre_dist —— 曲风分布
-- ===========================================================================
CREATE EXTERNAL TABLE IF NOT EXISTS music_dw.ads_genre_dist
(
    genre          STRING,
    play_start_cnt BIGINT,
    start_share    DOUBLE COMMENT '该曲风起播占全站比例',
    complete_rate  DOUBLE,
    user_cnt       BIGINT
)
COMMENT 'ADS-曲风分布'
PARTITIONED BY (dt STRING)
STORED AS ORC
LOCATION 'hdfs://WWC160:9820/warehouse/music_dw/ads_genre_dist'
TBLPROPERTIES ('orc.compress' = 'SNAPPY');

INSERT OVERWRITE TABLE music_dw.ads_genre_dist PARTITION (dt)
SELECT genre,
       play_start_cnt,
       ROUND(play_start_cnt * 1.0 / SUM(play_start_cnt) OVER (PARTITION BY dt), 4) AS start_share,
       complete_rate,
       user_cnt,
       dt
FROM music_dw.dws_genre_daily
WHERE dt BETWEEN '${start_dt}' AND '${end_dt}'
DISTRIBUTE BY dt;


-- ===========================================================================
-- 4. ads_province_dist —— 省份分布
-- ===========================================================================
CREATE EXTERNAL TABLE IF NOT EXISTS music_dw.ads_province_dist
(
    province       STRING,
    dau            BIGINT,
    play_start_cnt BIGINT,
    session_cnt    BIGINT,
    dau_share      DOUBLE COMMENT '该省日活占全站比例'
)
COMMENT 'ADS-省份分布'
PARTITIONED BY (dt STRING)
STORED AS ORC
LOCATION 'hdfs://WWC160:9820/warehouse/music_dw/ads_province_dist'
TBLPROPERTIES ('orc.compress' = 'SNAPPY');

INSERT OVERWRITE TABLE music_dw.ads_province_dist PARTITION (dt)
SELECT p.province,
       p.dau,
       p.play_start_cnt,
       p.session_cnt,
       ROUND(p.dau * 1.0 / SUM(p.dau) OVER (PARTITION BY p.dt), 4) AS dau_share,
       p.dt
FROM (
    SELECT dt, province,
           SUM(dau)            AS dau,
           SUM(play_start_cnt) AS play_start_cnt,
           SUM(session_cnt)    AS session_cnt
    FROM music_dw.dws_province_daily
    WHERE dt BETWEEN '${start_dt}' AND '${end_dt}'
    GROUP BY dt, province
) p
DISTRIBUTE BY p.dt;


-- ===========================================================================
-- 5. ads_user_retention —— 留存（同期群分析，不分 dt 分区）
--    口径：以「首次活跃日」为 cohort，统计第 1~7 天回访的用户数与留存率。
--    为什么不用注册日做 cohort：造数里注册日在 2026-08~09，事实数据在 10~11，
--    两者不重叠，无法计算「注册后第 N 天」。
--
--    造数用的是「出生-死亡」生命周期模型（见 code/gen_offline_ods.py）：
--    每个用户有出生日 b 和寿命 L，只在 [b, b+L) 内活跃。cohort 的 day_n 留存
--    = S(n) = P(L > n)，只由 L 的分布决定、与 b 无关 → 所有 cohort 曲线一致。
--    实测 10-06 / 10-12 两个 cohort 的 7 天曲线差 < 1.5 个百分点。
--
--    **D0 cohort 会明显偏高（80% vs 50%），这是「存量用户」造成的，不是缺陷：**
--    b<0 的用户在数据窗口开始前就出生了，MIN(dt) 把他们统一归到首日 cohort，
--    而他们的剩余寿命是「长度偏倚」的（长寿命用户更可能被抽到，平均剩余寿命
--    = E[L²]/2E[L] 而非 E[L]）。真实业务的 D0 cohort 同样偏高。
-- ===========================================================================
CREATE EXTERNAL TABLE IF NOT EXISTS music_dw.ads_user_retention
(
    cohort_date    STRING COMMENT '同期群日期（首次活跃日）',
    day_n          INT    COMMENT '第 N 天（1~7）',
    cohort_size    BIGINT COMMENT '该同期群总人数',
    retained_users BIGINT COMMENT '第 N 天回访人数',
    retention_rate DOUBLE COMMENT '留存率 = retained / cohort_size'
)
COMMENT 'ADS-用户留存（同期群）'
STORED AS ORC
LOCATION 'hdfs://WWC160:9820/warehouse/music_dw/ads_user_retention'
TBLPROPERTIES ('orc.compress' = 'SNAPPY');

-- 【2026-11-09 两次修正】
--   ① 实体：旧版直接按 user_id 分组，全体游客被压成 1 个 '' —— 于是有且只有一个
--      幽灵 cohort（实测 cohort_date = 2026-10-05、size 含 1 个空实体），
--      且此后每天都被判定为"回访"，让该 cohort 的 D1~D7 各多 1 人。
--      改用 COALESCE(NULLIF(user_id,''), device_id)，与 DWs 的 DAU 实体一致。
--   ② 活跃来源：旧版从 dwd_play_start 取，把「启动但没播」的人排除在活跃之外。
--      留存和日活必须用同一个"活跃"定义，否则 D1 留存会系统性偏低。
--      改从 dwd_session 的 app_launch 取，与 dws_user_daily 同源。
INSERT OVERWRITE TABLE music_dw.ads_user_retention
SELECT r.cohort_date,
       r.day_n,
       c.cohort_size,
       r.retained_users,
       ROUND(r.retained_users / c.cohort_size, 4) AS retention_rate
FROM (
    SELECT f.cohort_date,
           DATEDIFF(a.dt, f.cohort_date) AS day_n,
           COUNT(DISTINCT a.entity)      AS retained_users
    FROM (
        SELECT entity, MIN(dt) AS cohort_date
        FROM (
            SELECT dt, COALESCE(NULLIF(user_id, ''), device_id) AS entity
            FROM music_dw.dwd_session
            WHERE dt BETWEEN '${start_dt}' AND '${end_dt}'
              AND event_name = 'app_launch'
        ) e0
        GROUP BY entity
    ) f
    JOIN (
        SELECT DISTINCT dt,
               COALESCE(NULLIF(user_id, ''), device_id) AS entity
        FROM music_dw.dwd_session
        WHERE dt BETWEEN '${start_dt}' AND '${end_dt}'
          AND event_name = 'app_launch'
    ) a ON f.entity = a.entity
    WHERE DATEDIFF(a.dt, f.cohort_date) BETWEEN 1 AND 7
    GROUP BY f.cohort_date, DATEDIFF(a.dt, f.cohort_date)
) r
JOIN (
    SELECT cohort_date, COUNT(*) AS cohort_size
    FROM (
        SELECT entity, MIN(dt) AS cohort_date
        FROM (
            SELECT dt, COALESCE(NULLIF(user_id, ''), device_id) AS entity
            FROM music_dw.dwd_session
            WHERE dt BETWEEN '${start_dt}' AND '${end_dt}'
              AND event_name = 'app_launch'
        ) e1
        GROUP BY entity
    ) t
    GROUP BY cohort_date
) c ON r.cohort_date = c.cohort_date;


-- ===========================================================================
-- 验收
-- ===========================================================================
-- ① 五张表行数
SELECT 'ads_daily_overview' t, count(*) c FROM music_dw.ads_daily_overview
UNION ALL SELECT 'ads_song_ranking',   count(*) FROM music_dw.ads_song_ranking
UNION ALL SELECT 'ads_genre_dist',     count(*) FROM music_dw.ads_genre_dist
UNION ALL SELECT 'ads_province_dist',  count(*) FROM music_dw.ads_province_dist
UNION ALL SELECT 'ads_user_retention', count(*) FROM music_dw.ads_user_retention;

-- ② 日报概览（最近 3 天）
SELECT dt, dau, play_start_cnt, complete_cnt, complete_rate, incomplete_rate, avg_progress
FROM music_dw.ads_daily_overview ORDER BY dt DESC LIMIT 3;

-- ③ 留存（只取 D0 cohort，原因见上面第 5 节的口径说明）
--    期望：49.30 / 31.58 / 23.98 / 19.86 / 17.22 / 15.37 / 14.02（%）
SELECT cohort_date, day_n, cohort_size, retained_users, retention_rate
FROM music_dw.ads_user_retention
WHERE cohort_date = '${start_dt}'
ORDER BY day_n;
