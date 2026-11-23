-- =============================================================================
-- 06_dim_tables.sql —— 维度表：dim_song / dim_date / dim_user（SCD2 拉链表）
-- 执行：hive -f 06_dim_tables.sql
-- 前置：先跑 code/gen_dim_tables.py 生成三个 HDFS 数据集
--
-- ┌─ SCD2 在这里解决什么问题 ─────────────────────────────────────────────────┐
-- │ 实时链路的用户维表是「直接 TRUNCATE 重灌」——用户改了城市，旧城市就没了，   │
-- │ 昨天的报表按今天维度重算会变数字（历史不可复现）。这是评审里点的短板之一。  │
-- │ 拉链表用 start_date / end_date / is_current 保留每个版本：                │
-- │   查「今天有效的用户」→ WHERE is_current = 'Y'                           │
-- │   查「10-22 那天的用户」→ WHERE '2026-10-22' BETWEEN start_date AND end_date│
-- │ 核心不变式：**任一时间点，每个用户有且仅有一个有效版本**。                 │
-- └──────────────────────────────────────────────────────────────────────────┘
--
-- 为什么维表不从事实施表反推：
--   ODS 里用户属性全由 user_idx 派生，同一用户 30 天属性恒定 → 推出来的拉链表
--   只有一个版本，SCD2 逻辑等于没验证。所以维表有自己的业务源（变更流水）。
-- =============================================================================

SET hive.stats.autogather = false;   -- StatsTask 在这个环境会失败（不影响数据），关掉

-- ---------------------------------------------------------------------------
-- 1. 用户属性变更流水（维表的源表，ODS 层）
-- ---------------------------------------------------------------------------
CREATE EXTERNAL TABLE IF NOT EXISTS music_dw.ods_user_change
(
    user_id      STRING COMMENT '用户ID',
    change_date  STRING COMMENT '变更生效日期 yyyy-MM-dd',
    is_vip       STRING COMMENT '是否会员',
    city         STRING,
    province     STRING,
    app_version  STRING,
    os           STRING,
    device_model STRING,
    change_type  STRING COMMENT 'register/vip_upgrade/city_change'
)
COMMENT 'ODS-用户属性变更流水（SCD2 的源）'
STORED AS ORC
LOCATION 'hdfs://WWC160:9820/warehouse/music_dw/ods_user_change'
TBLPROPERTIES ('orc.compress' = 'SNAPPY');


-- ---------------------------------------------------------------------------
-- 2. 用户拉链表（SCD2）
--    LEAD() 取下一次变更日期，减 1 天即为当前版本的失效日期；
--    最后一次变更是当前版本，end_date 用哨兵值 '9999-12-31'。
-- ---------------------------------------------------------------------------
CREATE EXTERNAL TABLE IF NOT EXISTS music_dw.dim_user
(
    user_id        STRING COMMENT '用户ID',
    is_vip         STRING COMMENT '是否会员（当前版本）',
    city           STRING,
    province       STRING,
    app_version    STRING,
    os             STRING,
    device_model   STRING,
    register_date  STRING COMMENT '注册日期（首次变更日期）',
    start_date     STRING COMMENT '本版本生效日期',
    end_date       STRING COMMENT '本版本失效日期（9999-12-31 表示当前有效）',
    is_current     STRING COMMENT '是否当前版本 Y/N',
    change_type    STRING COMMENT '本版本的变更类型'
)
COMMENT 'DIM-用户拉链表（SCD2）'
STORED AS ORC
LOCATION 'hdfs://WWC160:9820/warehouse/music_dw/dim_user'
TBLPROPERTIES ('orc.compress' = 'SNAPPY');

INSERT OVERWRITE TABLE music_dw.dim_user
SELECT user_id,
       is_vip,
       city,
       province,
       app_version,
       os,
       device_model,
       register_date,
       change_date AS start_date,
       IF(next_date IS NULL, '9999-12-31', date_sub(next_date, 1)) AS end_date,
       IF(next_date IS NULL, 'Y', 'N')                             AS is_current,
       change_type
FROM (
    SELECT user_id, change_date, is_vip, city, province, app_version, os,
           device_model, change_type,
           LEAD(change_date) OVER (PARTITION BY user_id ORDER BY change_date) AS next_date,
           MIN(change_date)  OVER (PARTITION BY user_id)                      AS register_date
    FROM music_dw.ods_user_change
) x;


-- ---------------------------------------------------------------------------
-- 3. 歌曲维度
-- ---------------------------------------------------------------------------
CREATE EXTERNAL TABLE IF NOT EXISTS music_dw.dim_song
(
    song_id       STRING COMMENT '歌曲ID，与事实表 song_id 完全对齐',
    song_name     STRING,
    singer        STRING,
    genre         STRING COMMENT '曲风：流行/摇滚/民谣/R&B',
    duration      INT    COMMENT '时长（秒），与 dwd_play_end.song_duration 同源',
    release_date  STRING,
    region        STRING
)
COMMENT 'DIM-歌曲维度'
STORED AS ORC
LOCATION 'hdfs://WWC160:9820/warehouse/music_dw/dim_song'
TBLPROPERTIES ('orc.compress' = 'SNAPPY');


-- ---------------------------------------------------------------------------
-- 4. 日期维度
-- ---------------------------------------------------------------------------
CREATE EXTERNAL TABLE IF NOT EXISTS music_dw.dim_date
(
    dt              STRING COMMENT '日期 yyyy-MM-dd',
    day_of_week_num INT    COMMENT '1=周日 … 7=周六',
    day_of_week     STRING,
    week_of_year    INT,
    month           INT,
    quarter         INT,
    year            INT,
    is_weekend      STRING COMMENT 'Y/N'
)
COMMENT 'DIM-日期维度'
STORED AS ORC
LOCATION 'hdfs://WWC160:9820/warehouse/music_dw/dim_date'
TBLPROPERTIES ('orc.compress' = 'SNAPPY');


-- ===========================================================================
-- 验收（三条缺一不可）
-- ===========================================================================

-- ① 表行数
SELECT 'dim_song' AS tbl, count(*) AS cnt FROM music_dw.dim_song
UNION ALL SELECT 'dim_date', count(*) FROM music_dw.dim_date
UNION ALL SELECT 'dim_user', count(*) FROM music_dw.dim_user
UNION ALL SELECT 'ods_user_change', count(*) FROM music_dw.ods_user_change;

-- ② 版本分布：应 850000 人 1 版、150000 人 2 版（10% VIP 升级 + 5% 搬城，两者互斥）
--    总行数 = 85万 + 15万×2 = 115 万（与事实表池 100 万对齐后的规模）
SELECT ver_cnt, count(*) AS users
FROM (SELECT user_id, count(*) AS ver_cnt FROM music_dw.dim_user GROUP BY user_id) t
GROUP BY ver_cnt ORDER BY ver_cnt;

-- ③ SCD2 核心不变式：任一时间点每个用户有且仅有一个有效版本，应都是 1000000
SELECT '2026-10-10' AS snap_date, count(*) AS active_users FROM music_dw.dim_user
 WHERE '2026-10-10' BETWEEN start_date AND end_date
UNION ALL
SELECT '2026-10-22', count(*) FROM music_dw.dim_user
 WHERE '2026-10-22' BETWEEN start_date AND end_date
UNION ALL
SELECT '2026-10-26', count(*) FROM music_dw.dim_user
 WHERE '2026-10-26' BETWEEN start_date AND end_date
UNION ALL
SELECT '2026-11-03', count(*) FROM music_dw.dim_user
 WHERE '2026-11-03' BETWEEN start_date AND end_date;
