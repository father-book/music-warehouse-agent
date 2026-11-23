-- =============================================================================
-- Flink SQL：Kafka 埋点 → Doris ODS（原样搬运，不做清洗）
-- -----------------------------------------------------------------------------
-- 前置条件（顺序不能错）
--   1. Doris 里已执行 sql/ods/doris_ods_music_event.sql 建好 ODS 表
--   2. Kafka 里 topic music_events 已灌好数据（见文件末尾"灌数据"一节）
--   3. 再启动本作业 —— 这一点很关键，原因见下方 scan.bounded.mode 说明
--
-- 执行方式：sql-client.sh 里逐段执行，或者 flink run -f <本文件>
-- =============================================================================


-- -----------------------------------------------------------------------------
-- 1) Kafka 源表
-- -----------------------------------------------------------------------------
-- 三个参数是本作业能否"读完就停"的关键，别照抄教程：
--
--   scan.startup.mode = earliest-offset
--       从最早位点读。重跑作业时能重新读到全部历史数据，保证结果可复现。
--
--   scan.bounded.mode = latest-offset
--       把无界流当成"有界"作业：只消费到【作业启动那一刻】的最新的位点，读完自动结束。
--       不加这个，INSERT 作业会永久 RUNNING，你永远看不到"完成"，也不好判断数据齐没齐。
--       ⚠️ 代价：必须在启动作业"之前"把数据灌进 Kafka，否则读到的是空。
--
--   json.ignore-parse-errors = true
--       解析失败的行直接跳过。当前数据是脚本生成的合法 JSON，只有"值异常"没有"格式异常"，
--       所以开着不影响质检。将来若要造非法 JSON 测格式校验，必须改成死信队列，
--       否则这类脏数据会静默消失、质检永远抓不到。
-- -----------------------------------------------------------------------------
CREATE TABLE kafka_music_event
(
    event_id      STRING,
    user_id       STRING,
    song_id       STRING,
    event_type    STRING,
    keyword       STRING,
    genre         STRING,
    play_duration STRING,
    song_duration STRING,
    start_time    STRING,
    end_time      STRING,
    channel       STRING,
    device        STRING,
    network       STRING,
    city          STRING,
    session_id    STRING,
    `ts`          STRING,
    `dt`          STRING
) WITH (
    'connector'                    = 'kafka',
    'topic'                        = 'music_events',
    'properties.bootstrap.servers' = 'kafka:9092',
    'properties.group.id'          = 'music_ods_loader',
    'scan.startup.mode'            = 'earliest-offset',
    'scan.bounded.mode'            = 'latest-offset',
    'format'                       = 'json',
    'json.ignore-parse-errors'     = 'true',
    'json.fail-on-missing-field'   = 'false'
);


-- -----------------------------------------------------------------------------
-- 2) Doris ODS 目标表
-- -----------------------------------------------------------------------------
-- sink.label-prefix：Stream Load 的导入标签前缀。若报 "label already exists"，
-- 换一个前缀或加时间戳后缀即可（同一前缀在不同作业里并发导入会冲突）。
-- -----------------------------------------------------------------------------
CREATE TABLE doris_ods_music_event
(
    event_id      STRING,
    user_id       STRING,
    song_id       STRING,
    event_type    STRING,
    keyword       STRING,
    genre         STRING,
    play_duration STRING,
    song_duration STRING,
    start_time    STRING,
    end_time      STRING,
    channel       STRING,
    device        STRING,
    network       STRING,
    city          STRING,
    session_id    STRING,
    `ts`          STRING,
    `dt`          DATE,
    etl_time      TIMESTAMP(3)
) WITH (
    'connector'           = 'doris',
    'fenodes'             = 'doris-fe:8030',
    'table.identifier'    = 'music_dw.ods_music_event',
    'username'            = 'root',
    'password'            = '',
    'sink.label-prefix'   = 'ods_music_event',
    'sink.properties.format'          = 'json',
    'sink.properties.read_json_by_line' = 'true',
    'sink.enable-2pc'     = 'true'
);


-- -----------------------------------------------------------------------------
-- 3) 搬运作业：字段一一对应，只额外补一个入仓时间
-- -----------------------------------------------------------------------------
-- 注意 dt 从 STRING 转 DATE：若某行 dt 为空或格式非法，这一行会因 NOT NULL 被拒。
-- 这是期望行为——ODS 分区字段是唯一的强约束，不该为脏数据让步。
-- -----------------------------------------------------------------------------
INSERT INTO doris_ods_music_event
SELECT
    event_id,
    user_id,
    song_id,
    event_type,
    keyword,
    genre,
    play_duration,
    song_duration,
    start_time,
    end_time,
    channel,
    device,
    network,
    city,
    session_id,
    `ts`,
    CAST(`dt` AS DATE)   AS `dt`,
    LOCALTIMESTAMP       AS etl_time
FROM kafka_music_event;


-- =============================================================================
-- 灌数据（在启动作业之前做）
-- =============================================================================
-- 方式一：一次性全量灌入，最省事，适合验证链路
--   kafka-console-producer.sh \
--     --bootstrap-server localhost:9092 \
--     --topic music_events \
--     < data/events/events_kafka.jsonl
--
--   51,730 行大约几十秒灌完，控制台会刷一堆 > 提示符，属正常。
--   灌完先用下面这条确认条数对得上，再启 Flink 作业：
--   kafka-run-class.sh kafka.tools.GetOffsetShell \
--     --bootstrap-server localhost:9092 --topic music_events
--
-- 方式二：限速灌入，用 Kafka 自带压测工具，不写一行 Python
--   kafka-producer-perf-test.sh \
--     --topic music_events \
--     --num-records 51730 \
--     --throughput 200 \
--     --record-size 420 \
--     --producer-props bootstrap.servers=localhost:9092
--   （注意：这个工具发的是填充字节，不是真实 JSON，只适合压测 Kafka 本身，
--     不适合当业务数据源。灌真实数据还是用方式一。）
--
-- =============================================================================
-- 验证结果
-- =============================================================================
-- Doris 侧执行：
--   SELECT COUNT(*) FROM music_dw.ods_music_event;              -- 应 = 51730（含 1549 条脏数据）
--   SELECT dt, COUNT(*) FROM music_dw.ods_music_event GROUP BY dt ORDER BY dt;  -- 应覆盖 10-24 ~ 10-30
--
-- 关键验证：脏数据必须"还在"
--   -- 重复 event_id 应能查到 308 组（对应 dirty_manifest 里 duplicate_event_id 条数）
--   SELECT event_id, COUNT(*) c FROM music_dw.ods_music_event
--   GROUP BY event_id HAVING c > 1 LIMIT 20;
--
--   -- 时长异常应能查到 163 条
--   SELECT COUNT(*) FROM music_dw.ods_music_event
--   WHERE event_type = 'play' AND CAST(play_duration AS INT) > CAST(song_duration AS INT);
--
--   如果这两条查出来是 0，说明 ODS 模型选错了（大概率误用了 UNIQUE），
--   脏数据已被去重或覆盖，后面的质检实验全部作废。
