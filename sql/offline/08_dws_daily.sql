-- =============================================================================
-- 08_dws_daily.sql —— DWS：3 张日粒度汇总表
-- 执行：hive --hivevar start_dt=2026-10-05 --hivevar end_dt=2026-11-03 -f 08_dws_daily.sql
--
-- 建表：dws_user_daily / dws_genre_daily / dws_province_daily
-- （dws_song_play_daily 已由 05 建好）
--
-- DWS 的定位：**从亿级明细压到万级汇总**，让 ADS 不再碰明细。
--   2250 万行 DWD → 30 天 × 3 个维度 ≈ 几百到 15 万行
--   下游查「昨天完播率多少」只需扫 30 行，而不是 525 万行。
--
-- 口径约定（与 05 保持一致）：
--   完播率 = progress ≥ 0.9 的结束次数 ÷ **起播次数**
--   分母一律用 dwd_play_start，不用 dwd_play_end —— 跳过的事件没有结束记录，
--   用结束次数作分母会把跳过的人剔除，完播率虚高。
-- =============================================================================

SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;
SET hive.exec.max.dynamic.partitions = 200;
SET hive.exec.max.dynamic.partitions.pernode = 100;
SET hive.stats.autogather = false;


-- ===========================================================================
-- 1. dws_user_daily —— 日活 / 会话 / 人均行为
--    基于 dwd_play_start：每个会话的第一条事件必为 play_start（造数设计），
--    所以它的用户集合 = 全部活跃用户，且会话数无重复计算。
--    不用"所有事件 union"是为了避免扫 2250 万行只为拿一个 DAU。
-- ===========================================================================
CREATE EXTERNAL TABLE IF NOT EXISTS music_dw.dws_user_daily
(
    dau                BIGINT COMMENT '日活用户数',
    session_cnt        BIGINT COMMENT '会话数',
    play_start_cnt     BIGINT COMMENT '起播次数',
    avg_play_per_user  DOUBLE COMMENT '人均起播次数',
    avg_sess_per_user  DOUBLE COMMENT '人均会话数',
    login_user_cnt     BIGINT COMMENT '登录用户数',
    vip_user_cnt       BIGINT COMMENT '会员用户数'
)
COMMENT 'DWS-用户日粒度活跃指标'
PARTITIONED BY (dt STRING)
STORED AS ORC
LOCATION 'hdfs://WWC160:9820/warehouse/music_dw/dws_user_daily'
TBLPROPERTIES ('orc.compress' = 'SNAPPY');

-- 【DAU 的实体定义：用户为主体，游客按设备补】
-- 直接 COUNT(DISTINCT user_id) 有个致命问题：未登录事件的 user_id 是空串，
-- 全站聚合时所有游客被压成 1 个"用户"，而按省/市分组时每个分组各算 1 个 ——
-- 于是「分省 DAU 之和」永远大于「全站 DAU」。
-- 2026-11-09 实测 11-08：全站 2,054，分省之和 2,125，多 71；
-- 差的正是 72 个含游客的 (省,市) 分组 —— 每个分组贡献 1 个空值，全站只算 1 个。
-- 改用 COALESCE(NULLIF(user_id,''), device_id)：登录用户按 user_id 去重，
-- 游客按 device_id 去重（命名空间 U.../d_... 不重叠，不会互相吞并），
-- 每个实体只有唯一省份，分省之和必然等于全站。
--
-- 【2026-11-09 第二次修正：活跃的"来源表"也错了】
-- 旧版从 dwd_play_start 取 DAU，理由是"每个会话的第一条事件必为 play_start"。
-- 这条理由是错的。实测 11-08：
--     dwd_session(app_launch) 去重实体  3,237
--     dwd_play_start       去重实体  2,525   → 少 712 人，低 22%
-- 差额是"启动了 App 但当天一次都没播"的人（只浏览/只搜索/启动即退）。
-- 会话数同样低估 22%（3,270 vs 2,542）。
-- 「日活」必须是「启动过 App 的人」，不是「播过歌的人」。
-- 因此 dau / session_cnt / 登录 / 会员 一律取 dwd_session 的 app_launch；
-- play_start_cnt 仍取 dwd_play_start —— 那是"起播次数"，与用户集合无关。
INSERT OVERWRITE TABLE music_dw.dws_user_daily PARTITION (dt)
SELECT dau,
       session_cnt,
       play_start_cnt,
       ROUND(play_start_cnt / NULLIF(dau, 0), 2)   AS avg_play_per_user,
       ROUND(session_cnt    / NULLIF(dau, 0), 2)   AS avg_sess_per_user,
       login_user_cnt,
       vip_user_cnt,
       dt
FROM (
    SELECT NVL(a.dau, 0)             AS dau,
           NVL(a.session_cnt, 0)     AS session_cnt,
           NVL(p.play_start_cnt, 0)  AS play_start_cnt,
           NVL(a.login_user_cnt, 0)  AS login_user_cnt,
           NVL(a.vip_user_cnt, 0)    AS vip_user_cnt,
           COALESCE(a.dt, p.dt)      AS dt
    FROM (
        SELECT COUNT(DISTINCT COALESCE(NULLIF(user_id, ''), device_id)) AS dau,
               COUNT(DISTINCT session_id)                               AS session_cnt,
               COUNT(DISTINCT IF(is_login = 'true', user_id, NULL))     AS login_user_cnt,
               COUNT(DISTINCT IF(is_vip   = 'true', user_id, NULL))     AS vip_user_cnt,
               dt
        FROM music_dw.dwd_session
        WHERE dt BETWEEN '${start_dt}' AND '${end_dt}'
          AND event_name = 'app_launch'
        GROUP BY dt
    ) a
    FULL OUTER JOIN (
        SELECT dt, COUNT(*) AS play_start_cnt
        FROM music_dw.dwd_play_start
        WHERE dt BETWEEN '${start_dt}' AND '${end_dt}'
        GROUP BY dt
    ) p ON a.dt = p.dt
) t
DISTRIBUTE BY dt;


-- ===========================================================================
-- 2. dws_genre_daily —— 曲风日粒度（需要 join dim_song）
--    用 LEFT JOIN 从 play_start 出发：保证分母（起播）完整，
--    即使某曲风当天没有 play_end 记录，也保留一行完播率 = 0。
-- ===========================================================================
CREATE EXTERNAL TABLE IF NOT EXISTS music_dw.dws_genre_daily
(
    genre          STRING COMMENT '曲风',
    play_start_cnt BIGINT COMMENT '起播次数（分母）',
    play_end_cnt   BIGINT COMMENT '结束次数',
    complete_cnt   BIGINT COMMENT '完播次数（progress>=0.9）',
    complete_rate  DOUBLE COMMENT '完播率 = complete_cnt / play_start_cnt',
    avg_progress   DOUBLE COMMENT '平均播放进度（仅结束事件）',
    user_cnt       BIGINT COMMENT '收听用户数'
)
COMMENT 'DWS-曲风日粒度播放指标'
PARTITIONED BY (dt STRING)
STORED AS ORC
LOCATION 'hdfs://WWC160:9820/warehouse/music_dw/dws_genre_daily'
TBLPROPERTIES ('orc.compress' = 'SNAPPY');

INSERT OVERWRITE TABLE music_dw.dws_genre_daily PARTITION (dt)
SELECT s.genre,
       s.start_cnt,
       NVL(e.end_cnt, 0)                                          AS play_end_cnt,
       NVL(e.complete_cnt, 0)                                     AS complete_cnt,
       ROUND(NVL(e.complete_cnt, 0) / NULLIF(s.start_cnt, 0), 4)  AS complete_rate,
       ROUND(e.avg_progress, 4)                                   AS avg_progress,
       s.user_cnt,
       s.dt
FROM (
    SELECT d.dt, so.genre,
           COUNT(*)                     AS start_cnt,
           -- 【2026-11-09 修】旧版 COUNT(DISTINCT d.user_id) 把全体游客压成 1 个空值，
           -- 每个曲风各算 1 个 —— 于是「收听用户数」系统性偏低。
           -- 实测 11-08：流行 2,028 → 2,493（+23%）、摇滚 1,479 → 1,822（+23%）。
           -- 与 dws_user_daily / dws_province_daily 用同一个实体定义。
           COUNT(DISTINCT COALESCE(NULLIF(d.user_id, ''), d.device_id)) AS user_cnt
    FROM music_dw.dwd_play_start d
    JOIN music_dw.dim_song so ON d.song_id = so.song_id
    WHERE d.dt BETWEEN '${start_dt}' AND '${end_dt}'
    GROUP BY d.dt, so.genre
) s
LEFT JOIN (
    SELECT e.dt, so.genre,
           COUNT(*)                             AS end_cnt,
           SUM(IF(e.play_progress >= 0.9, 1, 0)) AS complete_cnt,
           AVG(e.play_progress)                 AS avg_progress
    FROM music_dw.dwd_play_end e
    JOIN music_dw.dim_song so ON e.song_id = so.song_id
    WHERE e.dt BETWEEN '${start_dt}' AND '${end_dt}'
    GROUP BY e.dt, so.genre
) e
ON s.dt = e.dt AND s.genre = e.genre
DISTRIBUTE BY s.dt;


-- ===========================================================================
-- 3. dws_province_daily —— 省份日粒度
-- ===========================================================================
CREATE EXTERNAL TABLE IF NOT EXISTS music_dw.dws_province_daily
(
    province       STRING,
    city           STRING,
    dau            BIGINT COMMENT '该省市活跃用户数',
    play_start_cnt BIGINT COMMENT '起播次数',
    session_cnt    BIGINT COMMENT '会话数'
)
COMMENT 'DWS-省份城市日粒度播放指标'
PARTITIONED BY (dt STRING)
STORED AS ORC
LOCATION 'hdfs://WWC160:9820/warehouse/music_dw/dws_province_daily'
TBLPROPERTIES ('orc.compress' = 'SNAPPY');

INSERT OVERWRITE TABLE music_dw.dws_province_daily PARTITION (dt)
SELECT province,
       city,
       dau,
       play_start_cnt,
       session_cnt,
       dt
FROM (
    -- 与 dws_user_daily 用同一套口径：活跃/会话取 dwd_session(app_launch)，
    -- 起播次数取 dwd_play_start。两处必须同源，否则「分省之和 = 全站」对不上。
    -- 实体定义同样是 COALESCE(NULLIF(user_id,''), device_id)：
    -- 旧版 COUNT(DISTINCT user_id) 会让每个含游客的 (省,市) 组各计 1 个空值。
    SELECT COALESCE(a.province, p.province) AS province,
           COALESCE(a.city, p.city)         AS city,
           NVL(a.dau, 0)                    AS dau,
           NVL(p.play_start_cnt, 0)         AS play_start_cnt,
           NVL(a.session_cnt, 0)            AS session_cnt,
           COALESCE(a.dt, p.dt)             AS dt
    FROM (
        SELECT dt, province, city,
               COUNT(DISTINCT COALESCE(NULLIF(user_id, ''), device_id)) AS dau,
               COUNT(DISTINCT session_id)                               AS session_cnt
        FROM music_dw.dwd_session
        WHERE dt BETWEEN '${start_dt}' AND '${end_dt}'
          AND event_name = 'app_launch'
        GROUP BY dt, province, city
    ) a
    FULL OUTER JOIN (
        SELECT dt, province, city, COUNT(*) AS play_start_cnt
        FROM music_dw.dwd_play_start
        WHERE dt BETWEEN '${start_dt}' AND '${end_dt}'
        GROUP BY dt, province, city
    ) p
      ON a.dt = p.dt AND a.province = p.province AND a.city = p.city
) t
DISTRIBUTE BY dt;


-- ===========================================================================
-- 验收
-- ===========================================================================
-- ① 三张表的行数（user_daily 30 行、genre_daily 120 行、province_daily 150 行）
SELECT 'dws_user_daily' tbl, count(*) c FROM music_dw.dws_user_daily
UNION ALL SELECT 'dws_genre_daily',    count(*) FROM music_dw.dws_genre_daily
UNION ALL SELECT 'dws_province_daily', count(*) FROM music_dw.dws_province_daily;

-- ② 日活趋势（30 行，每天应 50000 人）
SELECT dt, dau, session_cnt, play_start_cnt, avg_play_per_user
FROM music_dw.dws_user_daily
WHERE dt BETWEEN '${start_dt}' AND '${end_dt}'
ORDER BY dt;

-- ③ 曲风完播率（4 种曲风，应在整体 51.62% 附近 ±0.5 点）
--    实测 0.5145 / 0.5188 / 0.5173 / 0.5146，极差 0.43 点 —— 属采样噪声。
--    v6 之前极差 7.5 点（46.4~53.9），那是取模伪相关，不是业务结论。
SELECT genre,
       sum(play_start_cnt) AS starts,
       sum(complete_cnt)   AS completes,
       round(sum(complete_cnt) / sum(play_start_cnt), 4) AS complete_rate
FROM music_dw.dws_genre_daily
WHERE dt BETWEEN '${start_dt}' AND '${end_dt}'
GROUP BY genre ORDER BY starts DESC;
