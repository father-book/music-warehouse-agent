-- =============================================================================
-- 03_dwd_play_end.sql —— 离线 DWD：播放结束明细（清洗 + JSON 解析）
-- 执行（多天，动态分区一次写完）：
--   hive --hivevar start_dt=2026-10-05 --hivevar end_dt=2026-11-03 -f 03_dwd_play_end.sql
-- 单天也兼容：--hivevar start_dt=2026-11-03 --hivevar end_dt=2026-11-03
--
-- ┌─ 和实时链路的关系 ────────────────────────────────────────────────────────┐
-- │ 实时：Flink 消费 dwd_play_end 的 Kafka topic 后写 Doris                  │
-- │ 离线：Hive on Spark 从 ODS 清洗后写 HDFS                                 │
-- │ 两者字段逐个对齐，唯一差异是分区类型（实时 dt DATE / 离线 dt STRING）       │
-- └───────────────────────────────────────────────────────────────────────────┘
--
-- 做三件事：
--   1. 类型转换：event_time / server_time 毫秒字符串 → TIMESTAMP
--   2. JSON 解析：properties 里取 song_id / duration / position，只解析一次（子查询）
--   3. 派生字段：play_progress（播放进度）、end_reason（结束原因）
--
-- 为什么必须独立成表，而不是在 ADS 里直接算：
--   ODS 是字符串原文，每次查询都要 CAST + get_json_object；
--   3000 万行 × 每次多花几十秒解析。DWD 落一次，下游全用干净类型。
--   这也是"分层到底省了什么"的答案：省的是重复计算，不是代码行数。
--
-- 为什么用动态分区而不是循环 30 次静态分区：
--   30 次 × 40 秒 ≈ 20 分钟，其中大部分是重复的 Spark 提交开销。
--   动态分区一次约 2 分钟。代价是必须 DISTRIBUTE BY dt 控制写入分布 ——
--   否则每个 shuffle task 向所有分区目录同时写 → 小文件爆炸（900 个）。
-- =============================================================================

SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;
SET hive.exec.max.dynamic.partitions = 200;
SET hive.exec.max.dynamic.partitions.pernode = 100;

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


-- ---------------------------------------------------------------------------
-- 清洗：只取 play_end 事件
-- 子查询先解析一次 JSON，外层再算 progress / end_reason，
-- 避免 get_json_object 被重复调用 3 次（100 万行就是 300 万次解析）
-- ---------------------------------------------------------------------------
INSERT OVERWRITE TABLE music_dw.dwd_play_end PARTITION (dt)
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
    song_id,
    song_duration,
    play_duration,
    ROUND(play_duration / NULLIF(song_duration, 0), 4) AS play_progress,
    CASE
        WHEN play_duration / NULLIF(song_duration, 0) >= 0.90 THEN 'natural'
        WHEN play_duration / NULLIF(song_duration, 0) >= 0.60 THEN 'exit'
        ELSE 'skip'
    END AS end_reason,
    CAST(etl_time AS TIMESTAMP) AS etl_time,
    dt                          -- 动态分区列，必须是最后一列
FROM (
    SELECT
        event_id,
        event_time,
        server_time,
        user_id, device_id, is_login, is_vip, app_version, os, os_version,
        device_model, network, carrier, ip, province, city,
        session_id, session_seq, is_foreground, channel, page_id,
        get_json_object(properties, '$.song_id')                        AS song_id,
        CAST(get_json_object(properties, '$.duration') AS INT)          AS song_duration,
        CAST(get_json_object(properties, '$.position') AS INT)          AS play_duration,
        etl_time,
        dt
    FROM music_dw.ods_music_event
    WHERE dt BETWEEN '${start_dt}' AND '${end_dt}'
      AND event_name = 'play_end'
) t
DISTRIBUTE BY dt;


-- ---------------------------------------------------------------------------
-- 验证：每天应 175,000 行（100 万 × 17.5%），songs 应 1486
-- ---------------------------------------------------------------------------
SELECT dt,
       count(*)                            AS cnt,
       count(DISTINCT song_id)             AS songs,
       round(avg(play_progress), 4)        AS avg_progress,
       sum(if(end_reason = 'natural', 1, 0)) AS natural_cnt
FROM music_dw.dwd_play_end
WHERE dt BETWEEN '${start_dt}' AND '${end_dt}'
GROUP BY dt ORDER BY dt;
