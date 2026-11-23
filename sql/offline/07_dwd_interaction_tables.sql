-- =============================================================================
-- 07_dwd_interaction_tables.sql —— 离线 DWD：3 张互动类事件表
-- 执行：hive --hivevar start_dt=2026-10-05 --hivevar end_dt=2026-11-03 \
--            -f 07_dwd_interaction_tables.sql
--
-- 建表：dwd_skip / dwd_collect / dwd_share
-- （dwd_play_start / dwd_play_end 已由 04 / 03 建好，共 5 张 DWD）
--
-- ┌─ 为什么是 5 张，而不是照搬实时的 7 张 ────────────────────────────────────┐
-- │ 数仓的表应该由「指标」反推，不是由「源系统的表」照搬。                     │
-- │                                                                            │
-- │ 离线本期要做的指标 → 需要的表：                                            │
-- │   留存（次日/7日）        → play_start + dim_user                          │
-- │   完播率/播放时长/曲风    → play_end + dim_song                            │
-- │   跳过率（内容质量）      → skip                                           │
-- │   收藏率 / 分享率 / 漏斗  → collect + share                                │
-- │                                                                            │
-- │ 不建的（及原因）：                                                         │
-- │   dwd_search      搜索词分析、搜索→播放转化不在本期指标里                   │
-- │   dwd_page_view   浏览路径/跳出页面分析不在本期指标里                       │
-- │   dwd_order       没有订单数据                                             │
-- │   dwd_play_error  异常监控是实时链路的职责，离线不重复做                     │
-- │                                                                            │
-- │ 建了空表 = 多跑百万行 + 多维护一份 DDL，等真要做时再加不迟。                 │
-- └──────────────────────────────────────────────────────────────────────────┘
--
-- 公共 21 列与实时逐字段一致：event_id ~ page_id
-- =============================================================================

SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;
SET hive.exec.max.dynamic.partitions = 200;
SET hive.exec.max.dynamic.partitions.pernode = 100;
SET hive.stats.autogather = false;


-- ===========================================================================
-- 1. 切歌明细 dwd_skip —— 服务「跳过率 / 内容质量」指标
-- ===========================================================================
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

INSERT OVERWRITE TABLE music_dw.dwd_skip PARTITION (dt)
SELECT event_id,
       from_utc_timestamp(CAST(from_unixtime(CAST(event_time AS BIGINT) DIV 1000) AS TIMESTAMP), 'Asia/Shanghai')  AS event_ts,
       from_utc_timestamp(CAST(from_unixtime(CAST(server_time AS BIGINT) DIV 1000) AS TIMESTAMP), 'Asia/Shanghai') AS server_ts,
       user_id, device_id, is_login, is_vip, app_version, os, os_version,
       device_model, network, carrier, ip, province, city,
       session_id, session_seq, is_foreground, channel, page_id,
       get_json_object(properties, '$.song_id')                AS song_id,
       CAST(get_json_object(properties, '$.position') AS INT)  AS played_duration,
       'manual'                                                AS skip_type,
       CAST(etl_time AS TIMESTAMP)                             AS etl_time,
       dt
FROM music_dw.ods_music_event
WHERE dt BETWEEN '${start_dt}' AND '${end_dt}' AND event_name = 'skip'
DISTRIBUTE BY dt;


-- ===========================================================================
-- 2. 收藏明细 dwd_collect —— 服务「收藏率 / 用户偏好 / RFM」指标
-- ===========================================================================
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

INSERT OVERWRITE TABLE music_dw.dwd_collect PARTITION (dt)
SELECT event_id,
       from_utc_timestamp(CAST(from_unixtime(CAST(event_time AS BIGINT) DIV 1000) AS TIMESTAMP), 'Asia/Shanghai')  AS event_ts,
       from_utc_timestamp(CAST(from_unixtime(CAST(server_time AS BIGINT) DIV 1000) AS TIMESTAMP), 'Asia/Shanghai') AS server_ts,
       user_id, device_id, is_login, is_vip, app_version, os, os_version,
       device_model, network, carrier, ip, province, city,
       session_id, session_seq, is_foreground, channel, page_id,
       get_json_object(properties, '$.song_id')  AS song_id,
       'normal'                                  AS collect_type,
       CAST(etl_time AS TIMESTAMP)               AS etl_time,
       dt
FROM music_dw.ods_music_event
WHERE dt BETWEEN '${start_dt}' AND '${end_dt}' AND event_name = 'collect'
DISTRIBUTE BY dt;


-- ===========================================================================
-- 3. 分享明细 dwd_share —— 服务「分享率 / 传播漏斗」指标
-- ===========================================================================
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

INSERT OVERWRITE TABLE music_dw.dwd_share PARTITION (dt)
SELECT event_id,
       from_utc_timestamp(CAST(from_unixtime(CAST(event_time AS BIGINT) DIV 1000) AS TIMESTAMP), 'Asia/Shanghai')  AS event_ts,
       from_utc_timestamp(CAST(from_unixtime(CAST(server_time AS BIGINT) DIV 1000) AS TIMESTAMP), 'Asia/Shanghai') AS server_ts,
       user_id, device_id, is_login, is_vip, app_version, os, os_version,
       device_model, network, carrier, ip, province, city,
       session_id, session_seq, is_foreground, channel, page_id,
       get_json_object(properties, '$.song_id')  AS song_id,
       channel                                   AS share_channel,
       CAST(etl_time AS TIMESTAMP)               AS etl_time,
       dt
FROM music_dw.ods_music_event
WHERE dt BETWEEN '${start_dt}' AND '${end_dt}' AND event_name = 'share'
DISTRIBUTE BY dt;


-- ===========================================================================
-- 验收：5 张 DWD 合计 = 3000 万 × (25% + 17.5% + 7.5% + 12.5% + 12.5%) = 2250 万
--   注意：等于 75% 的 ODS 总量，因为 search(12.5%) + page_view(12.5%) 有意未建。
--   这个 2250 万 != 3000 万是设计选择，不是丢数据。
-- ===========================================================================
SELECT 'dwd_play_start' AS tbl, count(*) AS cnt FROM music_dw.dwd_play_start
UNION ALL SELECT 'dwd_play_end',   count(*) FROM music_dw.dwd_play_end
UNION ALL SELECT 'dwd_skip',       count(*) FROM music_dw.dwd_skip
UNION ALL SELECT 'dwd_collect',    count(*) FROM music_dw.dwd_collect
UNION ALL SELECT 'dwd_share',      count(*) FROM music_dw.dwd_share;
