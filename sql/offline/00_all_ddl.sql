-- =============================================================================
-- 00_all_ddl.sql —— 音乐场景离线数仓：全部 19 张表的建表语句
--
-- 用途：一键重建库表结构（不含数据装载）。各表的分区写入逻辑见对应编号脚本。
-- 执行：hive -f 00_all_ddl.sql
--
-- 分层   表数   说明
-- ODS     2     贴源，全 STRING，24 列事件 + 用户属性变更流水
-- DWD     5     明细，按事件类型拆表（解耦后再聚合）
-- DIM     3     dim_song(1486 真实曲目) / dim_user(SCD2 拉链) / dim_date
-- DWS     4     日粒度汇总：歌曲 / 用户 / 曲风 / 省份
-- ADS     5     报表层，直接对接 Doris 与 DataArt
-- =============================================================================

-- =============================================================================
-- 01_ddl_database.sql —— 离线数仓库（Hive 侧，单库 + 表名前缀分层）
-- 执行：hive -f sql/offline/01_ddl_database.sql
--
-- 为什么是「一个库 + 前缀」而不是 ods/dwd/dws/ads 四个库：
--
--   1) 与实时链路完全对称。实时那条链路的四层都在 Doris 的 music_dw 里，
--      表名就是 ods_/dwd_/dws_/ads_ 前缀。离线这边用同样的库名、同样的表名，
--      于是批流对账时可以写两句几乎相同的 SQL：
--          SELECT ... FROM music_dw.dwd_play_end   -- Doris（实时）
--          SELECT ... FROM music_dw.dwd_play_end   -- Hive（离线）
--      只有引擎不同，SQL 一字不改。这比「库名不同、表名也不同」更能证明
--      「批流同源」——同名同构，数字必须一致。
--
--   2) 表少的时候一层一库是过度设计。本项目全量约 20 张表，
--      Hive 的 `SHOW TABLES` 一眼能扫完，前缀已经足够分层。
--
--   3) 一层一库真正的价值在「按库授权」：分析师只给 ads 库的读权限、
--      DWD 库只给 ETL 账号写。这是数据安全需求，不是建模需求。
--      本项目单用户（root）开发，没有这个场景。将来真要做权限隔离，
--      再拆库也不影响表名（`ALTER TABLE ... RENAME` 迁移路径很短）。
--
-- 前缀约定（与 Doris 侧一致）：
--   ods_  原始层：按 dt 分区落 HDFS，不清洗、不解析 properties
--   dwd_  明细层：清洗、类型转换、维度退化、拉链表
--   dws_  汇总层：按主题轻度聚合（1d / 7d 等时间粒度）
--   ads_  应用层：直接对接报表与批流对账
-- =============================================================================

CREATE DATABASE IF NOT EXISTS music_dw
COMMENT '音乐APP数仓（离线/Hive侧，四层用表名前缀区分）'
LOCATION 'hdfs://WWC160:9820/warehouse/music_dw';

USE music_dw;
SHOW DATABASES;

-- =============================================================================
-- ODS  贴源层（2 张）
-- =============================================================================

CREATE EXTERNAL TABLE IF NOT EXISTS music_dw.ods_music_event
(
    event_id      STRING COMMENT '事件唯一ID',
    event_name    STRING COMMENT 'play_start/play_end/skip/collect/search/share/page_view',
    event_time    STRING COMMENT '客户端事件时间（毫秒时间戳）',
    server_time   STRING COMMENT '服务端接收时间（毫秒时间戳）',
    user_id       STRING COMMENT '用户ID，未登录为空',
    device_id     STRING COMMENT '设备ID',
    is_login      STRING COMMENT '是否登录',
    is_vip        STRING COMMENT '是否会员',
    app_version   STRING COMMENT 'APP版本',
    os            STRING COMMENT 'android/ios/web/car',
    os_version    STRING COMMENT '系统版本',
    device_model  STRING COMMENT '设备型号',
    network       STRING COMMENT 'wifi/4g/5g/3g',
    carrier       STRING COMMENT '运营商',
    ip            STRING COMMENT '客户端IP',
    province      STRING COMMENT '省份',
    city          STRING COMMENT '城市',
    session_id    STRING COMMENT '会话ID',
    session_seq   STRING COMMENT '会话内事件序号',
    is_foreground STRING COMMENT '前后台标记',
    channel       STRING COMMENT '曝光来源渠道',
    page_id       STRING COMMENT '事件发生页面',
    properties    STRING COMMENT '事件私有属性 JSON 原文，不解析',
    etl_time      STRING COMMENT '入仓时间（yyyy-MM-dd HH:mm:ss）'
)
COMMENT '离线ODS-音乐APP埋点原始明细（24列，与 Doris 版同构）'
PARTITIONED BY (dt STRING COMMENT '事件日期 yyyy-MM-dd')
STORED AS ORC
LOCATION 'hdfs://WWC160:9820/warehouse/music_dw/ods_music_event'
TBLPROPERTIES ('orc.compress' = 'SNAPPY');

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

-- =============================================================================
-- DWD  明细层（5 张）
-- =============================================================================

CREATE EXTERNAL TABLE IF NOT EXISTS music_dw.dwd_play_start
(
    event_id       STRING    COMMENT '事件ID',
    event_ts       TIMESTAMP COMMENT '事件时间',
    server_ts      TIMESTAMP COMMENT '服务端接收时间',
    user_id        STRING,
    device_id      STRING,
    is_login       STRING,
    is_vip         STRING,
    app_version    STRING,
    os             STRING,
    os_version     STRING,
    device_model   STRING,
    network        STRING,
    carrier        STRING,
    ip             STRING,
    province       STRING,
    city           STRING,
    session_id     STRING,
    session_seq    STRING,
    is_foreground  STRING,
    channel        STRING,
    page_id        STRING,
    song_id        STRING    COMMENT '$.song_id',
    playlist_id    STRING    COMMENT '$.playlist_id（造数未生成，恒 NULL）',
    quality        STRING    COMMENT '$.quality（造数未生成，恒 NULL）',
    play_mode      STRING    COMMENT '$.play_mode（造数未生成，恒 NULL）',
    song_index     INT       COMMENT '$.song_index（造数未生成，恒 NULL）',
    etl_time       TIMESTAMP COMMENT '入仓时间'
)
COMMENT 'DWD-播放开始明细（离线，与实时同名同构）'
PARTITIONED BY (dt STRING COMMENT '事件日期 yyyy-MM-dd')
STORED AS ORC
LOCATION 'hdfs://WWC160:9820/warehouse/music_dw/dwd_play_start'
TBLPROPERTIES ('orc.compress' = 'SNAPPY');

CREATE EXTERNAL TABLE IF NOT EXISTS music_dw.dwd_play_end
(
    event_id       STRING    COMMENT '事件ID',
    event_ts       TIMESTAMP COMMENT '事件时间（由 event_time 毫秒戳转换）',
    server_ts      TIMESTAMP COMMENT '服务端接收时间',
    user_id        STRING,
    device_id      STRING,
    is_login       STRING,
    is_vip         STRING,
    app_version    STRING,
    os             STRING,
    os_version     STRING,
    device_model   STRING,
    network        STRING,
    carrier        STRING,
    ip             STRING,
    province       STRING,
    city           STRING,
    session_id     STRING,
    session_seq    STRING,
    is_foreground  STRING,
    channel        STRING,
    page_id        STRING    COMMENT '来源页面',
    song_id        STRING    COMMENT '歌曲ID（来自 properties）',
    song_duration  INT       COMMENT '歌曲总时长（秒）',
    play_duration  INT       COMMENT '实际播放时长（秒）',
    play_progress  DOUBLE    COMMENT '播放进度 0~1',
    end_reason     STRING    COMMENT '结束原因：natural/exit/skip',
    etl_time       TIMESTAMP COMMENT '入仓时间，用于血缘追溯'
)
COMMENT 'DWD-播放结束明细（离线，与实时同名同构）'
PARTITIONED BY (dt STRING COMMENT '事件日期 yyyy-MM-dd')
STORED AS ORC
LOCATION 'hdfs://WWC160:9820/warehouse/music_dw/dwd_play_end'
TBLPROPERTIES ('orc.compress' = 'SNAPPY');

CREATE EXTERNAL TABLE IF NOT EXISTS music_dw.dwd_skip
(
    event_id        STRING    COMMENT '事件ID',
    event_ts        TIMESTAMP COMMENT '事件时间',
    server_ts       TIMESTAMP COMMENT '服务端接收时间',
    user_id         STRING,
    device_id       STRING,
    is_login        STRING,
    is_vip          STRING,
    app_version     STRING,
    os              STRING,
    os_version      STRING,
    device_model    STRING,
    network         STRING,
    carrier         STRING,
    ip              STRING,
    province        STRING,
    city            STRING,
    session_id      STRING,
    session_seq     STRING,
    is_foreground   STRING,
    channel         STRING,
    page_id         STRING,
    song_id         STRING COMMENT '$.song_id',
    played_duration INT    COMMENT '跳过前已播放秒数（由 $.position 推导）',
    skip_type       STRING COMMENT '$.skip_type（造数固定 manual）',
    etl_time        TIMESTAMP
)
COMMENT 'DWD-切歌明细（离线）'
PARTITIONED BY (dt STRING)
STORED AS ORC
LOCATION 'hdfs://WWC160:9820/warehouse/music_dw/dwd_skip'
TBLPROPERTIES ('orc.compress' = 'SNAPPY');

CREATE EXTERNAL TABLE IF NOT EXISTS music_dw.dwd_collect
(
    event_id        STRING    COMMENT '事件ID',
    event_ts        TIMESTAMP,
    server_ts       TIMESTAMP,
    user_id         STRING,
    device_id       STRING,
    is_login        STRING,
    is_vip          STRING,
    app_version     STRING,
    os              STRING,
    os_version      STRING,
    device_model    STRING,
    network         STRING,
    carrier         STRING,
    ip              STRING,
    province        STRING,
    city            STRING,
    session_id      STRING,
    session_seq     STRING,
    is_foreground   STRING,
    channel         STRING,
    page_id         STRING,
    song_id         STRING COMMENT '$.song_id',
    collect_type    STRING COMMENT '$.collect_type（造数固定 normal）',
    etl_time        TIMESTAMP
)
COMMENT 'DWD-收藏明细（离线）'
PARTITIONED BY (dt STRING)
STORED AS ORC
LOCATION 'hdfs://WWC160:9820/warehouse/music_dw/dwd_collect'
TBLPROPERTIES ('orc.compress' = 'SNAPPY');

CREATE EXTERNAL TABLE IF NOT EXISTS music_dw.dwd_share
(
    event_id        STRING    COMMENT '事件ID',
    event_ts        TIMESTAMP,
    server_ts       TIMESTAMP,
    user_id         STRING,
    device_id       STRING,
    is_login        STRING,
    is_vip          STRING,
    app_version     STRING,
    os              STRING,
    os_version      STRING,
    device_model    STRING,
    network         STRING,
    carrier         STRING,
    ip              STRING,
    province        STRING,
    city            STRING,
    session_id      STRING,
    session_seq     STRING,
    is_foreground   STRING,
    channel         STRING,
    page_id         STRING,
    song_id         STRING COMMENT '$.song_id',
    share_channel   STRING COMMENT '$.share_channel（由 channel 推导）',
    etl_time        TIMESTAMP
)
COMMENT 'DWD-分享明细（离线）'
PARTITIONED BY (dt STRING)
STORED AS ORC
LOCATION 'hdfs://WWC160:9820/warehouse/music_dw/dwd_share'
TBLPROPERTIES ('orc.compress' = 'SNAPPY');

-- =============================================================================
-- DIM  维度层（3 张）
-- =============================================================================

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

-- =============================================================================
-- DWS  汇总层（4 张）
-- =============================================================================

CREATE EXTERNAL TABLE IF NOT EXISTS music_dw.dws_song_play_daily
(
    song_id            STRING  COMMENT '歌曲ID',
    play_start_cnt     BIGINT  COMMENT '起播次数（完播率分母）',
    play_end_cnt       BIGINT  COMMENT '结束次数（含播完与中断）',
    complete_cnt       BIGINT  COMMENT '完播次数（progress >= 0.9）',
    complete_rate      DOUBLE  COMMENT '完播率 = complete_cnt / play_start_cnt',
    skip_play_cnt      BIGINT  COMMENT '未播完次数 = 起播 - 完播',
    avg_progress       DOUBLE  COMMENT '平均播放进度',
    avg_play_duration  DOUBLE  COMMENT '平均实际播放时长（秒）'
)
COMMENT 'DWS-歌曲日粒度播放与完播指标'
PARTITIONED BY (dt STRING COMMENT '统计日期 yyyy-MM-dd')
STORED AS ORC
LOCATION 'hdfs://WWC160:9820/warehouse/music_dw/dws_song_play_daily'
TBLPROPERTIES ('orc.compress' = 'SNAPPY');

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

-- =============================================================================
-- ADS  应用层（5 张）
-- =============================================================================

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
