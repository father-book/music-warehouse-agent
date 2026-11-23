-- =============================================================================
-- Flink SQL 作业：Kafka 埋点 → Doris ODS（常驻实时作业）
-- 本版特性（2026-10-31）：
--   1. 常驻：不设 scan.bounded.mode，作业持续 RUNNING，来一条处理一条
--   2. 解析：Kafka 消息按 raw 读入，用 JSON_VALUE 拆公共属性，JSON_QUERY 取 properties
--   3. 低延迟：Doris sink 缓冲调小到 200 行 / 1 秒，数据尽快可见
--   4. 有延迟指标：写入 etl_time，可算 etl_time - server_time
-- 执行：/opt/module/flink/bin/sql-client.sh -f flink_ods_job.sql
-- 停止：/opt/module/flink/bin/flink cancel <JobID>
-- =============================================================================

SET 'parallelism.default' = '3';
SET 'execution.checkpointing.interval' = '10s';

-- -----------------------------------------------------------------------------
-- 1) Kafka 源表（raw 格式）
-- -----------------------------------------------------------------------------
-- 为什么用 raw 而不是 json 格式：
--   properties 是嵌套对象，而 Doris 侧要存 STRING。用 json 格式无法把 object 映射成
--   STRING；用 raw 读入整条消息再用 JSON 函数拆分，既能保留嵌套结构原样，
--   又能在消息格式非法时仍然落库（字段为 NULL），符合 ODS「收得下」的原则。
--
-- scan.startup.mode = latest-offset：
--   从作业启动那一刻的最新位点开始消费，只处理新产生的数据。
--   ⚠️ 与之前的作业不同，这里**不设** scan.bounded.mode，因此作业不会读完就退出。
-- -----------------------------------------------------------------------------
CREATE TABLE kafka_music_event
(
    msg STRING
) WITH (
    'connector'                    = 'kafka',
    'topic'                        = 'music_events',
    'properties.bootstrap.servers' = 'WWC161:9092',
    'properties.group.id'          = 'music_ods_stream',
    'scan.startup.mode'            = 'latest-offset',
    'format'                       = 'raw'
);

-- -----------------------------------------------------------------------------
-- 2) Doris ODS 目标表
-- -----------------------------------------------------------------------------
-- sink.buffer-flush：调小到 200 行 / 1 秒，小流量下也能很快看到数据。
--   默认 5000 行 / 3 秒会让 10 条/秒 的场景等 3 秒以上才可见。
-- sink.enable-2pc = false：2PC 只在 Unique 模型下有幂等意义，ODS 是 Duplicate 模型，
--   开启可能报错；此处接受 at-least-once。
-- -----------------------------------------------------------------------------
CREATE TABLE doris_ods_music_event
(
    event_id      STRING,
    event_name    STRING,
    event_time    STRING,
    server_time   STRING,
    user_id       STRING,
    device_id     STRING,
    is_login      STRING,
    is_vip        STRING,
    app_version   STRING,
    os            STRING,
    os_version    STRING,
    device_model  STRING,
    network       STRING,
    carrier       STRING,
    ip            STRING,
    province      STRING,
    city          STRING,
    session_id    STRING,
    session_seq   STRING,
    is_foreground STRING,
    channel       STRING,
    page_id       STRING,
    properties    STRING,
    etl_time      TIMESTAMP(3),
    etl_date      DATE
) WITH (
    'connector'                         = 'doris',
    'fenodes'                           = 'WWC163:8030',
    'table.identifier'                  = 'music_dw.ods_music_event',
    'username'                          = 'root',
    'password'                          = '',
    'sink.label-prefix'                 = 'ods_music_event',
    'sink.properties.format'            = 'json',
    'sink.properties.read_json_by_line' = 'true',
    'sink.enable-2pc'                   = 'false',
    'sink.buffer-flush.max-rows'        = '200',
    'sink.buffer-flush.interval'        = '1s'
);

-- -----------------------------------------------------------------------------
-- 3) 解析并写入
-- -----------------------------------------------------------------------------
-- etl_date 取 LOCALTIMESTAMP（入仓日期）而非 server_time：
--   后者可能被脏数据推到未来，导致分区不存在。入仓日期永远是今天。
--   注意：若作业跨天长期运行，个别实例的 LOCALTIMESTAMP 可能仍停在启动日，
--   生产环境应定期重启或改用处理时间语义，此处作为已知限制记录。
-- -----------------------------------------------------------------------------
INSERT INTO doris_ods_music_event
SELECT
    JSON_VALUE(msg, '$.event_id')      AS event_id,
    JSON_VALUE(msg, '$.event_name')    AS event_name,
    JSON_VALUE(msg, '$.event_time')    AS event_time,
    JSON_VALUE(msg, '$.server_time')   AS server_time,
    JSON_VALUE(msg, '$.user_id')       AS user_id,
    JSON_VALUE(msg, '$.device_id')     AS device_id,
    JSON_VALUE(msg, '$.is_login')      AS is_login,
    JSON_VALUE(msg, '$.is_vip')        AS is_vip,
    JSON_VALUE(msg, '$.app_version')   AS app_version,
    JSON_VALUE(msg, '$.os')            AS os,
    JSON_VALUE(msg, '$.os_version')    AS os_version,
    JSON_VALUE(msg, '$.device_model')  AS device_model,
    JSON_VALUE(msg, '$.network')       AS network,
    JSON_VALUE(msg, '$.carrier')       AS carrier,
    JSON_VALUE(msg, '$.ip')            AS ip,
    JSON_VALUE(msg, '$.province')      AS province,
    JSON_VALUE(msg, '$.city')          AS city,
    JSON_VALUE(msg, '$.session_id')    AS session_id,
    JSON_VALUE(msg, '$.session_seq')   AS session_seq,
    JSON_VALUE(msg, '$.is_foreground') AS is_foreground,
    JSON_VALUE(msg, '$.channel')       AS channel,
    JSON_VALUE(msg, '$.page_id')       AS page_id,
    JSON_QUERY(msg, '$.properties')    AS properties,
    LOCALTIMESTAMP                     AS etl_time,
    CAST(LOCALTIMESTAMP AS DATE)       AS etl_date
FROM kafka_music_event;

-- =============================================================================
-- 验证(作业运行中执行)
-- =============================================================================
-- 1. 行数是否持续增长（隔 10 秒查两次）
--    SELECT COUNT(*) FROM music_dw.ods_music_event;
--
-- 2. 端到端延迟（秒）
--    SELECT
--        COUNT(*) AS cnt,
--        ROUND(AVG(TIMESTAMPDIFF(SECOND, FROM_UNIXTIME(CAST(server_time AS BIGINT)/1000), etl_time)), 2) AS avg_delay_s,
--        ROUND(MAX(TIMESTAMPDIFF(SECOND, FROM_UNIXTIME(CAST(server_time AS BIGINT)/1000), etl_time)), 2) AS max_delay_s
--    FROM music_dw.ods_music_event;
--
-- 3. 事件类型分布
--    SELECT event_name, COUNT(*) FROM music_dw.ods_music_event GROUP BY event_name;
--
-- 4. properties 是否原样保留
--    SELECT event_name, properties FROM music_dw.ods_music_event WHERE event_name='play_end' LIMIT 3;
