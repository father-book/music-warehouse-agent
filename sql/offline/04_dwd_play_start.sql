-- =============================================================================
-- 04_dwd_play_start.sql —— 离线 DWD：播放开始明细（完播率的分母）
-- 执行：hive --hivevar start_dt=2026-10-05 --hivevar end_dt=2026-11-03 -f 04_dwd_play_start.sql
--
-- ┌─ 为什么完播率必须有这张表 ────────────────────────────────────────────────┐
-- │ 完播率 = progress ≥ 0.9 的结束次数 ÷ 起播次数                            │
-- │                                                                          │
-- │ 分母不能用「结束次数」：跳过的事件不产生 play_end，用结束次数作分母会把    │
-- │ 跳过的人从统计里剔除，算出来虚高。我们数据里 play_start 25%、play_end     │
-- │ 17.5%，两个分母算出来差 43% —— 答辩被问「完播率怎么算的」时必须说清用哪个。│
-- └──────────────────────────────────────────────────────────────────────────┘
--
-- 动态分区：一次 INSERT 写多天，而不是循环提交 N 次。
--   30 天 × 40 秒/次 ≈ 20 分钟；动态分区一次约 2 分钟。
--   代价是必须用 DISTRIBUTE BY dt 控制写入分布，否则每个 shuffle task 会向
--   所有分区目录同时写 → 小文件爆炸（和 ODS 造数里 repartition 是同一个问题）。
--
-- v9 起 gen_events.py 已补齐 playlist_id / quality / play_mode / song_index，
--   这四列不再是 NULL，与实时侧字段对齐；指标链路仍不依赖它们。
-- =============================================================================

SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;
SET hive.exec.max.dynamic.partitions = 200;
SET hive.exec.max.dynamic.partitions.pernode = 100;

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
    playlist_id    STRING    COMMENT '$.playlist_id（v9 已生成）',
    quality        STRING    COMMENT '$.quality（v9 已生成）',
    play_mode      STRING    COMMENT '$.play_mode（v9 已生成）',
    song_index     INT       COMMENT '$.song_index（v9 已生成）',
    etl_time       TIMESTAMP COMMENT '入仓时间'
)
COMMENT 'DWD-播放开始明细（离线，与实时同名同构）'
PARTITIONED BY (dt STRING COMMENT '事件日期 yyyy-MM-dd')
STORED AS ORC
LOCATION 'hdfs://WWC160:9820/warehouse/music_dw/dwd_play_start'
TBLPROPERTIES ('orc.compress' = 'SNAPPY');


INSERT OVERWRITE TABLE music_dw.dwd_play_start PARTITION (dt)
SELECT
    event_id,
    from_utc_timestamp(CAST(from_unixtime(CAST(event_time AS BIGINT) DIV 1000) AS TIMESTAMP), 'Asia/Shanghai')  AS event_ts,
    from_utc_timestamp(CAST(from_unixtime(CAST(server_time AS BIGINT) DIV 1000) AS TIMESTAMP), 'Asia/Shanghai') AS server_ts,
    user_id,
    device_id,
    is_login,
    is_vip,
    app_version,
    os,
    os_version,
    device_model,
    network,
    carrier,
    ip,
    province,
    city,
    session_id,
    session_seq,
    is_foreground,
    channel,
    page_id,
    get_json_object(properties, '$.song_id')                AS song_id,
    get_json_object(properties, '$.playlist_id')            AS playlist_id,
    get_json_object(properties, '$.quality')                AS quality,
    get_json_object(properties, '$.play_mode')              AS play_mode,
    CAST(get_json_object(properties, '$.song_index') AS INT) AS song_index,
    CAST(etl_time AS TIMESTAMP)                             AS etl_time,
    dt                                                      -- 动态分区列，必须是最后一列
FROM music_dw.ods_music_event
WHERE dt BETWEEN '${start_dt}' AND '${end_dt}'
  AND event_name = 'play_start'
DISTRIBUTE BY dt;


-- 验证：每天应 250000 行（100 万 × 25%）
SELECT dt, count(*) AS cnt, count(DISTINCT song_id) AS songs
FROM music_dw.dwd_play_start
WHERE dt BETWEEN '${start_dt}' AND '${end_dt}'
GROUP BY dt ORDER BY dt;
