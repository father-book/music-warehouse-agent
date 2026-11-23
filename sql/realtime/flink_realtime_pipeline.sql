-- =============================================================================
-- 实时数仓流式管道（验证版：ODS + DWD play_end + 异常表 + DWS 总览）
-- 核心理念：一个作业、一个 Kafka source、多个 sink 并行写出，
--          数据进 Kafka 的瞬间同时落到 ODS / DWD / DWS，全程无批处理介入。
--
-- 与旧方案的区别：
--   旧：Kafka→Flink→ODS，然后 Python 脚本每 20 秒批算 DWD/DWS（延迟 20-30 秒）
--   新：Kafka→Flink→{ODS, DWD, DWS} 同时写（延迟 1-2 秒）
--
-- 执行：/opt/module/flink/bin/sql-client.sh -f flink_realtime_pipeline.sql
-- 停止：flink cancel <JobID>
-- =============================================================================

SET 'parallelism.default' = '3';
SET 'execution.checkpointing.interval' = '10s';

-- -----------------------------------------------------------------------------
-- 0) Kafka 源表：raw 读入 + 计算列解析 + 处理时间
-- -----------------------------------------------------------------------------
-- 为什么用 raw 而不是 json：
--   properties 是嵌套对象，json 格式无法把 object 映射成 STRING。
--   用 raw 整条读入，再用 JSON_VALUE / JSON_QUERY 拆字段。
--
-- 为什么水位线不声明事件时间：
--   脏数据里有「事件时间推后 2-30 天」的类型，如果按事件时间做 watermark，
--   水位线会被推到 30 天后，之后所有正常数据都会变成迟到数据被丢弃。
--   DWS 窗口因此改用 PROCTIME（处理时间），语义是「这段时间系统处理了多少」，
--   对大屏来说比事件时间更合适，且不受脏数据影响。
-- -----------------------------------------------------------------------------
CREATE TABLE kafka_src (
    msg            STRING,
    event_id       AS JSON_VALUE(msg, '$.event_id'),
    event_name     AS JSON_VALUE(msg, '$.event_name'),
    event_time_ms  AS JSON_VALUE(msg, '$.event_time'),
    server_time_ms AS JSON_VALUE(msg, '$.server_time'),
    user_id        AS JSON_VALUE(msg, '$.user_id'),
    device_id      AS JSON_VALUE(msg, '$.device_id'),
    is_login       AS JSON_VALUE(msg, '$.is_login'),
    is_vip         AS JSON_VALUE(msg, '$.is_vip'),
    app_version    AS JSON_VALUE(msg, '$.app_version'),
    os             AS JSON_VALUE(msg, '$.os'),
    os_version     AS JSON_VALUE(msg, '$.os_version'),
    device_model   AS JSON_VALUE(msg, '$.device_model'),
    network        AS JSON_VALUE(msg, '$.network'),
    carrier        AS JSON_VALUE(msg, '$.carrier'),
    ip             AS JSON_VALUE(msg, '$.ip'),
    province       AS JSON_VALUE(msg, '$.province'),
    city           AS JSON_VALUE(msg, '$.city'),
    session_id     AS JSON_VALUE(msg, '$.session_id'),
    session_seq    AS JSON_VALUE(msg, '$.session_seq'),
    is_foreground  AS JSON_VALUE(msg, '$.is_foreground'),
    channel        AS JSON_VALUE(msg, '$.channel'),
    page_id        AS JSON_VALUE(msg, '$.page_id'),
    properties     AS JSON_QUERY(msg, '$.properties'),
    proc_time      AS PROCTIME()
) WITH (
    'connector'                    = 'kafka',
    'topic'                        = 'music_events',
    'properties.bootstrap.servers' = 'WWC161:9092',
    'properties.group.id'          = 'music_realtime_pipeline',
    'scan.startup.mode'            = 'latest-offset',
    'format'                       = 'raw'
);

-- -----------------------------------------------------------------------------
-- 1) 清洗视图：公共规则一次判定，被下游所有 DWD / DWS 复用
-- -----------------------------------------------------------------------------
CREATE TEMPORARY VIEW v_clean AS
SELECT
    event_id, event_name, event_time_ms, server_time_ms,
    user_id, device_id, is_login, is_vip, app_version, os, os_version, device_model,
    network, carrier, ip, province, city, session_id, session_seq, is_foreground,
    channel, page_id, properties, proc_time,
    TO_TIMESTAMP_LTZ(TRY_CAST(event_time_ms AS BIGINT), 3) AS event_ts
FROM kafka_src
WHERE event_id IS NOT NULL AND event_id <> ''
  AND device_id IS NOT NULL AND device_id <> ''
  AND NOT (is_login = 'true' AND COALESCE(user_id, '') = '')
  AND TRY_CAST(event_time_ms AS BIGINT) IS NOT NULL
  AND TRY_CAST(event_time_ms AS BIGINT) BETWEEN 1700000000000 AND UNIX_TIMESTAMP() * 1000 + 300000;

-- -----------------------------------------------------------------------------
-- 2) sink：ODS（原样落，不过滤，包含脏数据）
-- -----------------------------------------------------------------------------
CREATE TABLE sink_ods (
    event_id STRING, event_name STRING, event_time STRING, server_time STRING,
    user_id STRING, device_id STRING, is_login STRING, is_vip STRING,
    app_version STRING, os STRING, os_version STRING, device_model STRING,
    network STRING, carrier STRING, ip STRING, province STRING, city STRING,
    session_id STRING, session_seq STRING, is_foreground STRING, channel STRING,
    page_id STRING, properties STRING, etl_time TIMESTAMP(3), etl_date DATE
) WITH (
    'connector'                         = 'doris',
    'fenodes'                           = 'WWC163:8030',
    'table.identifier'                  = 'music_dw.ods_music_event',
    'username'                          = 'root',
    'password'                          = '',
    'sink.label-prefix'                 = 'rt_ods',
    'sink.properties.format'            = 'json',
    'sink.properties.read_json_by_line' = 'true',
    'sink.enable-2pc'                   = 'false',
    'sink.buffer-flush.max-rows'        = '2000',
    'sink.buffer-flush.interval'        = '2s'
);

-- -----------------------------------------------------------------------------
-- 3) sink：DWD play_end
-- -----------------------------------------------------------------------------
CREATE TABLE sink_dwd_play_end (
    event_id STRING, event_ts TIMESTAMP(3), server_ts TIMESTAMP(3),
    user_id STRING, device_id STRING, is_login STRING, is_vip STRING,
    app_version STRING, os STRING, os_version STRING, device_model STRING,
    network STRING, carrier STRING, ip STRING, province STRING, city STRING,
    session_id STRING, session_seq STRING, is_foreground STRING, channel STRING,
    page_id STRING, song_id STRING, song_duration INT, play_duration INT,
    play_progress DOUBLE, end_reason STRING, etl_time TIMESTAMP(3), dt DATE
) WITH (
    'connector'                         = 'doris',
    'fenodes'                           = 'WWC163:8030',
    'table.identifier'                  = 'music_dw.dwd_play_end',
    'username'                          = 'root',
    'password'                          = '',
    'sink.label-prefix'                 = 'rt_dwd_play_end',
    'sink.properties.format'            = 'json',
    'sink.properties.read_json_by_line' = 'true',
    'sink.enable-2pc'                   = 'false',
    'sink.buffer-flush.max-rows'        = '1000',
    'sink.buffer-flush.interval'        = '2s'
);

-- -----------------------------------------------------------------------------
-- 4) sink：DWS 播放总览（1 分钟窗口）
-- -----------------------------------------------------------------------------
CREATE TABLE sink_dws_overview (
    dt DATE, window_start TIMESTAMP(3), window_end TIMESTAMP(3),
    play_cnt BIGINT, finish_cnt BIGINT, skip_cnt BIGINT,
    pause_cnt BIGINT, exit_cnt BIGINT, interrupt_cnt BIGINT,
    finish_rate DOUBLE, skip_rate DOUBLE,
    avg_play_duration DOUBLE, avg_play_progress DOUBLE,
    uv BIGINT, login_uv BIGINT
) WITH (
    'connector'                         = 'doris',
    'fenodes'                           = 'WWC163:8030',
    'table.identifier'                  = 'music_dw.dws_play_overview_1min',
    'username'                          = 'root',
    'password'                          = '',
    'sink.label-prefix'                 = 'rt_dws_overview',
    'sink.properties.format'            = 'json',
    'sink.properties.read_json_by_line' = 'true',
    'sink.enable-2pc'                   = 'false',
    'sink.buffer-flush.max-rows'        = '200',
    'sink.buffer-flush.interval'        = '1s'
);

-- -----------------------------------------------------------------------------
-- 5) 多路写出：同一个 source，并行落三处
-- -----------------------------------------------------------------------------
EXECUTE STATEMENT SET
BEGIN

    -- ODS：原样，不经过清洗视图（脏数据也要留档）
    INSERT INTO sink_ods
    SELECT
        event_id, event_name, event_time_ms, server_time_ms,
        user_id, device_id, is_login, is_vip, app_version, os, os_version, device_model,
        network, carrier, ip, province, city, session_id, session_seq, is_foreground,
        channel, page_id, properties,
        LOCALTIMESTAMP, CAST(LOCALTIMESTAMP AS DATE)
    FROM kafka_src;

    -- DWD play_end：清洗后落明细
    INSERT INTO sink_dwd_play_end
    SELECT
        event_id,
        event_ts,
        TO_TIMESTAMP_LTZ(TRY_CAST(server_time_ms AS BIGINT), 3) AS server_ts,
        user_id, device_id, is_login, is_vip, app_version, os, os_version, device_model,
        network, carrier, ip, province, city, session_id, session_seq, is_foreground,
        channel, page_id,
        JSON_VALUE(properties, '$.song_id')                        AS song_id,
        CAST(JSON_VALUE(properties, '$.song_duration') AS INT)     AS song_duration,
        CAST(JSON_VALUE(properties, '$.play_duration') AS INT)     AS play_duration,
        CAST(JSON_VALUE(properties, '$.play_progress') AS DOUBLE)  AS play_progress,
        JSON_VALUE(properties, '$.end_reason')                     AS end_reason,
        LOCALTIMESTAMP                                             AS etl_time,
        CAST(event_ts AS DATE)                                     AS dt
    FROM v_clean
    WHERE event_name = 'play_end'
      AND COALESCE(JSON_VALUE(properties, '$.song_id'), '') <> ''
      AND CAST(JSON_VALUE(properties, '$.song_duration') AS INT) IS NOT NULL
      AND CAST(JSON_VALUE(properties, '$.play_duration') AS INT) IS NOT NULL
      AND CAST(JSON_VALUE(properties, '$.play_duration') AS INT)
          <= CAST(JSON_VALUE(properties, '$.song_duration') AS INT)
      AND CAST(JSON_VALUE(properties, '$.play_progress') AS DOUBLE) BETWEEN 0 AND 1
      AND JSON_VALUE(properties, '$.end_reason')
          IN ('natural', 'skip', 'pause', 'exit', 'interrupt');

    -- DWS 播放总览：1 分钟滚动窗口（处理时间）
    INSERT INTO sink_dws_overview
    SELECT
        CAST(TUMBLE_START(proc_time, INTERVAL 1 MINUTE) AS DATE)          AS dt,
        TUMBLE_START(proc_time, INTERVAL 1 MINUTE)                        AS window_start,
        TUMBLE_END(proc_time, INTERVAL 1 MINUTE)                          AS window_end,
        COUNT(*)                                                          AS play_cnt,
        SUM(CASE WHEN JSON_VALUE(properties, '$.end_reason') = 'natural'   THEN 1 ELSE 0 END),
        SUM(CASE WHEN JSON_VALUE(properties, '$.end_reason') = 'skip'      THEN 1 ELSE 0 END),
        SUM(CASE WHEN JSON_VALUE(properties, '$.end_reason') = 'pause'     THEN 1 ELSE 0 END),
        SUM(CASE WHEN JSON_VALUE(properties, '$.end_reason') = 'exit'      THEN 1 ELSE 0 END),
        SUM(CASE WHEN JSON_VALUE(properties, '$.end_reason') = 'interrupt' THEN 1 ELSE 0 END),
        ROUND(SUM(CASE WHEN JSON_VALUE(properties, '$.end_reason') = 'natural' THEN 1 ELSE 0 END)
              * 100.0 / COUNT(*), 2),
        ROUND(SUM(CASE WHEN JSON_VALUE(properties, '$.end_reason') = 'skip' THEN 1 ELSE 0 END)
              * 100.0 / COUNT(*), 2),
        ROUND(AVG(CAST(JSON_VALUE(properties, '$.play_duration') AS DOUBLE)), 1),
        ROUND(AVG(CAST(JSON_VALUE(properties, '$.play_progress') AS DOUBLE)), 4),
        COUNT(DISTINCT device_id),
        COUNT(DISTINCT CASE WHEN COALESCE(user_id, '') <> '' THEN user_id END)
    FROM v_clean
    WHERE event_name = 'play_end'
      AND COALESCE(JSON_VALUE(properties, '$.song_id'), '') <> ''
      AND CAST(JSON_VALUE(properties, '$.play_duration') AS INT)
          <= CAST(JSON_VALUE(properties, '$.song_duration') AS INT)
    GROUP BY TUMBLE(proc_time, INTERVAL 1 MINUTE);

END;
