-- =============================================================================
-- 作业 B：DWD 层清洗与分流
--   topic ods_music_event ──┬──→ dwd_play_end          （明细）
--                           ├──→ dwd_play_start        （明细）
--                           ├──→ dwd_skip              （明细）
--                           ├──→ dwd_search            （明细）
--                           ├──→ dwd_collect           （明细）
--                           ├──→ dwd_share             （明细）
--                           ├──→ dwd_page_view         （明细）
--                           ├──→ dwd_quality_error     （异常旁路）
--                           └──→ topic dwd_play_end    （给作业 C 做窗口聚合）
--
-- 为什么 8 个 sink 放在一个作业里是对的：
--   它们全部来自【同一个 source】、共用【同一套判定规则】，属于「一次判定、
--   多路输出」的分流，不是「多层计算压在一起」。拆成 7 个作业会导致同一份
--   数据被 7 个 consumer group 各消费一遍，且公共规则重复判定 7 次。
--
-- 与批式版的关键差异：
--   批式版要扫两遍 ODS（一遍写异常、一遍写明细），因为 Doris 一条 INSERT 只能
--   写一张表。流式版一次消费、9 路输出，判定只做一次，逻辑严格互补：
--     命中规则 → 异常表；rule_name IS NULL → 对应明细表。
--   这消除了批式版「两套 WHERE 条件要人工保证互补」的隐患。
--
-- 为什么每条数据只记一条规则：
--   CASE 自上而下命中即停。一条数据若同时违反多个规则，只记优先级最高的那个。
--   否则「规则命中数之和」会超过「异常数据条数」，召回率统计会做出 >100% 的错误结论。
-- =============================================================================

SET 'pipeline.name' = 'rt_job_b_dwd';
SET 'parallelism.default' = '2';
SET 'execution.checkpointing.interval' = '10s';

-- -----------------------------------------------------------------------------
-- 0) 源：ODS topic（原始报文）
-- -----------------------------------------------------------------------------
CREATE TABLE src_ods (
    msg STRING
) WITH (
    'connector'                    = 'kafka',
    'topic'                        = 'ods_music_event',
    'properties.bootstrap.servers' = 'WWC161:9092',
    'properties.group.id'          = 'rt_job_b_dwd_r3',
    -- 位点策略：group-offsets + auto.offset.reset=latest
    --   首次启动（无提交位点）→ 从最新位置开始，不追历史存量；
    --   之后重启 → 从上次提交的位点继续，既不重复也不丢。
    -- 为什么不用 earliest：层间 topic 里存着历次实验的历史消息，
    -- 从最早读会把已被 TRUNCATE 掉的旧数据重新灌进 DWD，导致对账永远不平。
    'scan.startup.mode'            = 'group-offsets',
    'properties.auto.offset.reset' = 'latest',
    'format'                       = 'raw'
);

-- -----------------------------------------------------------------------------
-- 1) 解析视图：公共属性 + 7 类事件私有属性，每个 JSON 只解析一次
-- -----------------------------------------------------------------------------
CREATE TEMPORARY VIEW v_parsed AS
SELECT
    JSON_VALUE(msg, '$.event_id')                            AS event_id,
    JSON_VALUE(msg, '$.event_name')                          AS event_name,
    JSON_VALUE(msg, '$.event_time')                          AS event_time_ms,
    JSON_VALUE(msg, '$.server_time')                         AS server_time_ms,
    JSON_VALUE(msg, '$.user_id')                             AS user_id,
    JSON_VALUE(msg, '$.device_id')                           AS device_id,
    JSON_VALUE(msg, '$.is_login')                            AS is_login,
    JSON_VALUE(msg, '$.is_vip')                              AS is_vip,
    JSON_VALUE(msg, '$.app_version')                         AS app_version,
    JSON_VALUE(msg, '$.os')                                  AS os,
    JSON_VALUE(msg, '$.os_version')                          AS os_version,
    JSON_VALUE(msg, '$.device_model')                        AS device_model,
    JSON_VALUE(msg, '$.network')                             AS network,
    JSON_VALUE(msg, '$.carrier')                             AS carrier,
    JSON_VALUE(msg, '$.ip')                                  AS ip,
    JSON_VALUE(msg, '$.province')                            AS province,
    JSON_VALUE(msg, '$.city')                                AS city,
    JSON_VALUE(msg, '$.session_id')                          AS session_id,
    JSON_VALUE(msg, '$.session_seq')                         AS session_seq,
    JSON_VALUE(msg, '$.is_foreground')                       AS is_foreground,
    JSON_VALUE(msg, '$.channel')                             AS channel,
    JSON_VALUE(msg, '$.page_id')                             AS page_id,
    JSON_QUERY(msg, '$.properties')                          AS properties,

    -- 时间转换：FROM_UNIXTIME 返回 'yyyy-MM-dd HH:mm:ss' 字符串，
    -- 再 TO_TIMESTAMP 转 TIMESTAMP(3)。比 TO_TIMESTAMP_LTZ 更少时区歧义。
    TO_TIMESTAMP(FROM_UNIXTIME(CAST(JSON_VALUE(msg, '$.event_time')  AS BIGINT) / 1000)) AS event_ts,
    TO_TIMESTAMP(FROM_UNIXTIME(CAST(JSON_VALUE(msg, '$.server_time') AS BIGINT) / 1000)) AS server_ts,

    -- play_end
    JSON_VALUE(msg, '$.properties.song_id')                  AS p_song_id,
    CAST(JSON_VALUE(msg, '$.properties.song_duration') AS INT)    AS p_song_duration,
    CAST(JSON_VALUE(msg, '$.properties.play_duration') AS INT)    AS p_play_duration,
    CAST(JSON_VALUE(msg, '$.properties.play_progress') AS DOUBLE) AS p_play_progress,
    JSON_VALUE(msg, '$.properties.end_reason')               AS p_end_reason,
    -- play_start
    JSON_VALUE(msg, '$.properties.playlist_id')              AS p_playlist_id,
    JSON_VALUE(msg, '$.properties.quality')                  AS p_quality,
    JSON_VALUE(msg, '$.properties.play_mode')                AS p_play_mode,
    CAST(JSON_VALUE(msg, '$.properties.song_index') AS INT)  AS p_song_index,
    -- skip
    CAST(JSON_VALUE(msg, '$.properties.played_duration') AS INT)  AS p_played_duration,
    JSON_VALUE(msg, '$.properties.skip_type')                AS p_skip_type,
    -- search
    JSON_VALUE(msg, '$.properties.keyword')                  AS p_keyword,
    JSON_VALUE(msg, '$.properties.search_type')              AS p_search_type,
    CAST(JSON_VALUE(msg, '$.properties.result_count') AS INT)     AS p_result_count,
    JSON_VALUE(msg, '$.properties.clicked_song_id')          AS p_clicked_song_id,
    -- collect
    JSON_VALUE(msg, '$.properties.collect_type')             AS p_collect_type,
    -- share
    JSON_VALUE(msg, '$.properties.share_channel')            AS p_share_channel,
    -- page_view
    JSON_VALUE(msg, '$.properties.page_id')                  AS p_view_page_id,
    JSON_VALUE(msg, '$.properties.referrer_page')            AS p_referrer_page,
    CAST(JSON_VALUE(msg, '$.properties.stay_duration') AS INT)    AS p_stay_duration
FROM src_ods;

-- -----------------------------------------------------------------------------
-- 2) 判定视图：一条数据只产出一个 rule_name（优先级自上而下）
-- -----------------------------------------------------------------------------
-- 优先级设计原则：
--   ① 结构性缺陷（事件名、主键）优先于业务属性缺陷
--   ② 公共规则优先于事件私有规则
--   ③ 时间规则优先于属性规则（时间不可信时，其余属性无从判断）
-- -----------------------------------------------------------------------------
CREATE TEMPORARY VIEW v_tagged AS
SELECT
    *,
    CASE
        -- ① 事件名不在白名单：无法归属任何明细表
        WHEN event_name IS NULL
          OR event_name NOT IN ('play_start','play_end','skip','search','collect','share','page_view')
            THEN 'event_name_invalid'

        -- ② 公共规则（7 类事件通用）
        WHEN COALESCE(event_id, '') = ''
            THEN 'event_id_missing'
        WHEN COALESCE(device_id, '') = ''
            THEN 'device_id_missing'
        WHEN is_login = 'true' AND COALESCE(user_id, '') = ''
            THEN 'user_id_missing_when_login'
        WHEN event_ts IS NULL
            THEN 'event_time_missing'
        WHEN CAST(event_time_ms AS BIGINT) > UNIX_TIMESTAMP() * 1000 + 300000
            THEN 'event_time_future'
        WHEN CAST(event_time_ms AS BIGINT) < 1700000000000
            THEN 'event_time_too_old'

        -- ③ play_end 私有规则
        WHEN event_name = 'play_end' AND COALESCE(p_song_id, '') = ''
            THEN 'song_id_missing'
        WHEN event_name = 'play_end' AND p_song_duration IS NULL
            THEN 'song_duration_missing'
        WHEN event_name = 'play_end' AND p_play_duration IS NULL
            THEN 'play_duration_missing'
        WHEN event_name = 'play_end' AND p_play_duration > p_song_duration
            THEN 'duration_exceed'
        WHEN event_name = 'play_end' AND (p_play_progress IS NULL OR p_play_progress < 0 OR p_play_progress > 1)
            THEN 'progress_invalid'
        WHEN event_name = 'play_end'
          AND (p_end_reason IS NULL OR p_end_reason NOT IN ('natural','skip','pause','exit','interrupt'))
            THEN 'end_reason_invalid'

        -- ④ play_start 私有规则
        WHEN event_name = 'play_start' AND COALESCE(p_song_id, '') = ''
            THEN 'song_id_missing'

        -- ⑤ skip 私有规则
        WHEN event_name = 'skip' AND COALESCE(p_song_id, '') = ''
            THEN 'song_id_missing'
        WHEN event_name = 'skip' AND p_played_duration IS NULL
            THEN 'played_duration_missing'
        WHEN event_name = 'skip' AND p_played_duration < 0
            THEN 'played_duration_negative'

        -- ⑥ search 私有规则
        WHEN event_name = 'search' AND COALESCE(p_keyword, '') = ''
            THEN 'keyword_missing'
        WHEN event_name = 'search' AND p_result_count < 0
            THEN 'result_count_negative'

        -- ⑦ collect 私有规则
        WHEN event_name = 'collect' AND COALESCE(p_song_id, '') = ''
            THEN 'song_id_missing'
        WHEN event_name = 'collect' AND COALESCE(p_collect_type, '') = ''
            THEN 'collect_type_missing'
        WHEN event_name = 'collect' AND p_collect_type NOT IN ('add','cancel')
            THEN 'collect_type_invalid'

        -- ⑧ share 私有规则
        WHEN event_name = 'share' AND COALESCE(p_song_id, '') = ''
            THEN 'song_id_missing'
        WHEN event_name = 'share' AND COALESCE(p_share_channel, '') = ''
            THEN 'share_channel_missing'

        -- ⑨ page_view 私有规则
        WHEN event_name = 'page_view' AND COALESCE(p_view_page_id, '') = ''
            THEN 'view_page_id_missing'
        WHEN event_name = 'page_view' AND p_stay_duration < 0
            THEN 'stay_duration_negative'

        ELSE NULL
    END AS rule_name,
    CASE
        WHEN event_name IS NULL
          OR event_name NOT IN ('play_start','play_end','skip','search','collect','share','page_view')
            THEN CONCAT('event_name=', COALESCE(event_name, 'NULL'))
        WHEN COALESCE(event_id, '') = ''
            THEN 'event_id 为空'
        WHEN COALESCE(device_id, '') = ''
            THEN 'device_id 为空（设备不可识别）'
        WHEN is_login = 'true' AND COALESCE(user_id, '') = ''
            THEN 'is_login=true 但 user_id 为空（登录态矛盾）'
        WHEN event_ts IS NULL
            THEN CONCAT('event_time=', COALESCE(event_time_ms, 'NULL'), ' 无法转为时间戳')
        WHEN CAST(event_time_ms AS BIGINT) > UNIX_TIMESTAMP() * 1000 + 300000
            THEN CONCAT('event_time 超前 5 分钟以上: ', event_time_ms)
        WHEN CAST(event_time_ms AS BIGINT) < 1700000000000
            THEN CONCAT('event_time 早于 2023: ', event_time_ms)
        WHEN event_name = 'play_end' AND COALESCE(p_song_id, '') = ''
            THEN 'properties.song_id 为空'
        WHEN event_name = 'play_end' AND p_song_duration IS NULL
            THEN 'properties.song_duration 缺失'
        WHEN event_name = 'play_end' AND p_play_duration IS NULL
            THEN 'properties.play_duration 缺失'
        WHEN event_name = 'play_end' AND p_play_duration > p_song_duration
            THEN CONCAT('play_duration=', CAST(p_play_duration AS STRING),
                        ' > song_duration=', CAST(p_song_duration AS STRING))
        WHEN event_name = 'play_end' AND (p_play_progress IS NULL OR p_play_progress < 0 OR p_play_progress > 1)
            THEN CONCAT('play_progress=', COALESCE(CAST(p_play_progress AS STRING), 'NULL'))
        WHEN event_name = 'play_end'
          AND (p_end_reason IS NULL OR p_end_reason NOT IN ('natural','skip','pause','exit','interrupt'))
            THEN CONCAT('end_reason=', COALESCE(p_end_reason, 'NULL'))
        WHEN event_name = 'play_start' AND COALESCE(p_song_id, '') = ''
            THEN 'properties.song_id 为空'
        WHEN event_name = 'skip' AND COALESCE(p_song_id, '') = ''
            THEN 'properties.song_id 为空'
        WHEN event_name = 'skip' AND p_played_duration IS NULL
            THEN 'properties.played_duration 缺失'
        WHEN event_name = 'skip' AND p_played_duration < 0
            THEN CONCAT('played_duration=', CAST(p_played_duration AS STRING))
        WHEN event_name = 'search' AND COALESCE(p_keyword, '') = ''
            THEN 'properties.keyword 为空'
        WHEN event_name = 'search' AND p_result_count < 0
            THEN CONCAT('result_count=', CAST(p_result_count AS STRING))
        WHEN event_name = 'collect' AND COALESCE(p_song_id, '') = ''
            THEN 'properties.song_id 为空'
        WHEN event_name = 'collect' AND COALESCE(p_collect_type, '') = ''
            THEN 'properties.collect_type 缺失'
        WHEN event_name = 'collect' AND p_collect_type NOT IN ('add','cancel')
            THEN CONCAT('collect_type=', p_collect_type)
        WHEN event_name = 'share' AND COALESCE(p_song_id, '') = ''
            THEN 'properties.song_id 为空'
        WHEN event_name = 'share' AND COALESCE(p_share_channel, '') = ''
            THEN 'properties.share_channel 缺失'
        WHEN event_name = 'page_view' AND COALESCE(p_view_page_id, '') = ''
            THEN 'properties.page_id 为空'
        WHEN event_name = 'page_view' AND p_stay_duration < 0
            THEN CONCAT('stay_duration=', CAST(p_stay_duration AS STRING))
        ELSE ''
    END AS detail
FROM v_parsed;

-- =============================================================================
-- 3) 九个 sink
-- =============================================================================

-- 3.1 异常表
CREATE TABLE sink_err (
    event_id       STRING,
    rule_name      STRING,
    event_name     STRING,
    rule_level     STRING,
    detail         STRING,
    song_id        STRING,
    user_id        STRING,
    device_id      STRING,
    event_ts       TIMESTAMP(3),
    raw_properties STRING,
    etl_time       TIMESTAMP(3),
    dt             DATE
) WITH (
    'connector'                         = 'doris',
    'fenodes'                           = 'WWC163:8030',
    'table.identifier'                  = 'music_dw.dwd_quality_error',
    'username'                          = 'root',
    'password'                          = '',
    'sink.label-prefix'                 = 'job_b_err',
    'sink.properties.format'            = 'json',
    'sink.properties.read_json_by_line' = 'true',
    'sink.enable-2pc'                   = 'false',
    'sink.buffer-flush.max-rows'        = '200',
    'sink.buffer-flush.interval'        = '2s'
);

-- 3.2 play_end 明细
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
    'sink.label-prefix'                 = 'job_b_play_end',
    'sink.properties.format'            = 'json',
    'sink.properties.read_json_by_line' = 'true',
    'sink.enable-2pc'                   = 'false',
    'sink.buffer-flush.max-rows'        = '200',
    'sink.buffer-flush.interval'        = '2s'
);

-- 3.3 play_start 明细
CREATE TABLE sink_dwd_play_start (
    event_id STRING, event_ts TIMESTAMP(3), server_ts TIMESTAMP(3),
    user_id STRING, device_id STRING, is_login STRING, is_vip STRING,
    app_version STRING, os STRING, os_version STRING, device_model STRING,
    network STRING, carrier STRING, ip STRING, province STRING, city STRING,
    session_id STRING, session_seq STRING, is_foreground STRING, channel STRING,
    page_id STRING, song_id STRING, playlist_id STRING, quality STRING,
    play_mode STRING, song_index INT, etl_time TIMESTAMP(3), dt DATE
) WITH (
    'connector' = 'doris', 'fenodes' = 'WWC163:8030',
    'table.identifier' = 'music_dw.dwd_play_start', 'username' = 'root', 'password' = '',
    'sink.label-prefix' = 'job_b_play_start',
    'sink.properties.format' = 'json', 'sink.properties.read_json_by_line' = 'true',
    'sink.enable-2pc' = 'false',
    'sink.buffer-flush.max-rows' = '200', 'sink.buffer-flush.interval' = '2s'
);

-- 3.4 skip 明细
CREATE TABLE sink_dwd_skip (
    event_id STRING, event_ts TIMESTAMP(3), server_ts TIMESTAMP(3),
    user_id STRING, device_id STRING, is_login STRING, is_vip STRING,
    app_version STRING, os STRING, os_version STRING, device_model STRING,
    network STRING, carrier STRING, ip STRING, province STRING, city STRING,
    session_id STRING, session_seq STRING, is_foreground STRING, channel STRING,
    page_id STRING, song_id STRING, played_duration INT, skip_type STRING,
    etl_time TIMESTAMP(3), dt DATE
) WITH (
    'connector' = 'doris', 'fenodes' = 'WWC163:8030',
    'table.identifier' = 'music_dw.dwd_skip', 'username' = 'root', 'password' = '',
    'sink.label-prefix' = 'job_b_skip',
    'sink.properties.format' = 'json', 'sink.properties.read_json_by_line' = 'true',
    'sink.enable-2pc' = 'false',
    'sink.buffer-flush.max-rows' = '200', 'sink.buffer-flush.interval' = '2s'
);

-- 3.5 search 明细
CREATE TABLE sink_dwd_search (
    event_id STRING, event_ts TIMESTAMP(3), server_ts TIMESTAMP(3),
    user_id STRING, device_id STRING, is_login STRING, is_vip STRING,
    app_version STRING, os STRING, os_version STRING, device_model STRING,
    network STRING, carrier STRING, ip STRING, province STRING, city STRING,
    session_id STRING, session_seq STRING, is_foreground STRING, channel STRING,
    page_id STRING, keyword STRING, search_type STRING, result_count INT,
    clicked_song_id STRING, etl_time TIMESTAMP(3), dt DATE
) WITH (
    'connector' = 'doris', 'fenodes' = 'WWC163:8030',
    'table.identifier' = 'music_dw.dwd_search', 'username' = 'root', 'password' = '',
    'sink.label-prefix' = 'job_b_search',
    'sink.properties.format' = 'json', 'sink.properties.read_json_by_line' = 'true',
    'sink.enable-2pc' = 'false',
    'sink.buffer-flush.max-rows' = '200', 'sink.buffer-flush.interval' = '2s'
);

-- 3.6 collect 明细
CREATE TABLE sink_dwd_collect (
    event_id STRING, event_ts TIMESTAMP(3), server_ts TIMESTAMP(3),
    user_id STRING, device_id STRING, is_login STRING, is_vip STRING,
    app_version STRING, os STRING, os_version STRING, device_model STRING,
    network STRING, carrier STRING, ip STRING, province STRING, city STRING,
    session_id STRING, session_seq STRING, is_foreground STRING, channel STRING,
    page_id STRING, song_id STRING, collect_type STRING, etl_time TIMESTAMP(3), dt DATE
) WITH (
    'connector' = 'doris', 'fenodes' = 'WWC163:8030',
    'table.identifier' = 'music_dw.dwd_collect', 'username' = 'root', 'password' = '',
    'sink.label-prefix' = 'job_b_collect',
    'sink.properties.format' = 'json', 'sink.properties.read_json_by_line' = 'true',
    'sink.enable-2pc' = 'false',
    'sink.buffer-flush.max-rows' = '200', 'sink.buffer-flush.interval' = '2s'
);

-- 3.7 share 明细
CREATE TABLE sink_dwd_share (
    event_id STRING, event_ts TIMESTAMP(3), server_ts TIMESTAMP(3),
    user_id STRING, device_id STRING, is_login STRING, is_vip STRING,
    app_version STRING, os STRING, os_version STRING, device_model STRING,
    network STRING, carrier STRING, ip STRING, province STRING, city STRING,
    session_id STRING, session_seq STRING, is_foreground STRING, channel STRING,
    page_id STRING, song_id STRING, share_channel STRING, etl_time TIMESTAMP(3), dt DATE
) WITH (
    'connector' = 'doris', 'fenodes' = 'WWC163:8030',
    'table.identifier' = 'music_dw.dwd_share', 'username' = 'root', 'password' = '',
    'sink.label-prefix' = 'job_b_share',
    'sink.properties.format' = 'json', 'sink.properties.read_json_by_line' = 'true',
    'sink.enable-2pc' = 'false',
    'sink.buffer-flush.max-rows' = '200', 'sink.buffer-flush.interval' = '2s'
);

-- 3.8 page_view 明细
CREATE TABLE sink_dwd_page_view (
    event_id STRING, event_ts TIMESTAMP(3), server_ts TIMESTAMP(3),
    user_id STRING, device_id STRING, is_login STRING, is_vip STRING,
    app_version STRING, os STRING, os_version STRING, device_model STRING,
    network STRING, carrier STRING, ip STRING, province STRING, city STRING,
    session_id STRING, session_seq STRING, is_foreground STRING, channel STRING,
    page_id STRING, view_page_id STRING, referrer_page STRING, stay_duration INT,
    etl_time TIMESTAMP(3), dt DATE
) WITH (
    'connector' = 'doris', 'fenodes' = 'WWC163:8030',
    'table.identifier' = 'music_dw.dwd_page_view', 'username' = 'root', 'password' = '',
    'sink.label-prefix' = 'job_b_page_view',
    'sink.properties.format' = 'json', 'sink.properties.read_json_by_line' = 'true',
    'sink.enable-2pc' = 'false',
    'sink.buffer-flush.max-rows' = '200', 'sink.buffer-flush.interval' = '2s'
);

-- 3.9 DWD topic：把清洗后的 play_end 送给作业 C
-- 时间列转成 STRING 再输出，避免 JSON 序列化对 TIMESTAMP 格式的解释差异。
-- 列名必须与 job_c_dws.sql 的 src_dwd_play_end 完全一致（json format 按 key 匹配，
-- 名字差一个字符就读成 NULL）。所以这里用 _str 后缀，作业 C 再用计算列转回 TIMESTAMP。
CREATE TABLE sink_topic_play_end (
    event_id STRING, event_ts_str STRING, server_ts_str STRING,
    user_id STRING, device_id STRING, is_login STRING, is_vip STRING,
    app_version STRING, os STRING, os_version STRING, device_model STRING,
    network STRING, carrier STRING, province STRING, city STRING,
    session_id STRING, channel STRING, page_id STRING,
    song_id STRING, song_duration INT, play_duration INT,
    play_progress DOUBLE, end_reason STRING, etl_time_str STRING
) WITH (
    'connector'                    = 'kafka',
    'topic'                        = 'dwd_play_end',
    'properties.bootstrap.servers' = 'WWC161:9092',
    'format'                       = 'json',
    'sink.partitioner'             = 'default'
);

-- =============================================================================
-- 4) 九路写出：判定一次，出口九个
-- =============================================================================
EXECUTE STATEMENT SET
BEGIN

    -- 异常旁路
    INSERT INTO sink_err
    SELECT
        event_id, rule_name, event_name, 'block', detail,
        p_song_id, user_id, device_id, event_ts, properties,
        LOCALTIMESTAMP, CAST(LOCALTIMESTAMP AS DATE)
    FROM v_tagged
    WHERE rule_name IS NOT NULL;

    -- play_end
    INSERT INTO sink_dwd_play_end
    SELECT
        event_id, event_ts, server_ts, user_id, device_id, is_login, is_vip,
        app_version, os, os_version, device_model, network, carrier, ip, province, city,
        session_id, session_seq, is_foreground, channel, page_id,
        p_song_id, p_song_duration, p_play_duration, p_play_progress, p_end_reason,
        LOCALTIMESTAMP, CAST(event_ts AS DATE)
    FROM v_tagged
    WHERE rule_name IS NULL AND event_name = 'play_end';

    -- play_start
    INSERT INTO sink_dwd_play_start
    SELECT
        event_id, event_ts, server_ts, user_id, device_id, is_login, is_vip,
        app_version, os, os_version, device_model, network, carrier, ip, province, city,
        session_id, session_seq, is_foreground, channel, page_id,
        p_song_id, p_playlist_id, p_quality, p_play_mode, p_song_index,
        LOCALTIMESTAMP, CAST(event_ts AS DATE)
    FROM v_tagged
    WHERE rule_name IS NULL AND event_name = 'play_start';

    -- skip
    INSERT INTO sink_dwd_skip
    SELECT
        event_id, event_ts, server_ts, user_id, device_id, is_login, is_vip,
        app_version, os, os_version, device_model, network, carrier, ip, province, city,
        session_id, session_seq, is_foreground, channel, page_id,
        p_song_id, p_played_duration, p_skip_type,
        LOCALTIMESTAMP, CAST(event_ts AS DATE)
    FROM v_tagged
    WHERE rule_name IS NULL AND event_name = 'skip';

    -- search
    INSERT INTO sink_dwd_search
    SELECT
        event_id, event_ts, server_ts, user_id, device_id, is_login, is_vip,
        app_version, os, os_version, device_model, network, carrier, ip, province, city,
        session_id, session_seq, is_foreground, channel, page_id,
        p_keyword, p_search_type, p_result_count, p_clicked_song_id,
        LOCALTIMESTAMP, CAST(event_ts AS DATE)
    FROM v_tagged
    WHERE rule_name IS NULL AND event_name = 'search';

    -- collect
    INSERT INTO sink_dwd_collect
    SELECT
        event_id, event_ts, server_ts, user_id, device_id, is_login, is_vip,
        app_version, os, os_version, device_model, network, carrier, ip, province, city,
        session_id, session_seq, is_foreground, channel, page_id,
        p_song_id, p_collect_type,
        LOCALTIMESTAMP, CAST(event_ts AS DATE)
    FROM v_tagged
    WHERE rule_name IS NULL AND event_name = 'collect';

    -- share
    INSERT INTO sink_dwd_share
    SELECT
        event_id, event_ts, server_ts, user_id, device_id, is_login, is_vip,
        app_version, os, os_version, device_model, network, carrier, ip, province, city,
        session_id, session_seq, is_foreground, channel, page_id,
        p_song_id, p_share_channel,
        LOCALTIMESTAMP, CAST(event_ts AS DATE)
    FROM v_tagged
    WHERE rule_name IS NULL AND event_name = 'share';

    -- page_view
    INSERT INTO sink_dwd_page_view
    SELECT
        event_id, event_ts, server_ts, user_id, device_id, is_login, is_vip,
        app_version, os, os_version, device_model, network, carrier, ip, province, city,
        session_id, session_seq, is_foreground, channel, page_id,
        p_view_page_id, p_referrer_page, p_stay_duration,
        LOCALTIMESTAMP, CAST(event_ts AS DATE)
    FROM v_tagged
    WHERE rule_name IS NULL AND event_name = 'page_view';

    -- DWD topic：给作业 C
    -- 注意这里的过滤条件与 dwd_play_end 的 INSERT 完全一致，
    -- 保证「进了 Doris 明细表的数据」与「送进 DWS 的数据」是同一批。
    INSERT INTO sink_topic_play_end
    SELECT
        event_id,
        DATE_FORMAT(event_ts, 'yyyy-MM-dd HH:mm:ss'),
        DATE_FORMAT(server_ts, 'yyyy-MM-dd HH:mm:ss'),
        user_id, device_id, is_login, is_vip,
        app_version, os, os_version, device_model, network, carrier, province, city,
        session_id, channel, page_id,
        p_song_id, p_song_duration, p_play_duration, p_play_progress, p_end_reason,
        DATE_FORMAT(LOCALTIMESTAMP, 'yyyy-MM-dd HH:mm:ss')
    FROM v_tagged
    WHERE rule_name IS NULL AND event_name = 'play_end';

END;

-- =============================================================================
-- 验证：对账必须严丝合缝
-- =============================================================================
-- 恒等式：ods_total = 7 张明细之和 + 异常表条数
--
-- SELECT
--   (SELECT COUNT(*) FROM music_dw.ods_music_event) AS ods_cnt,
--   (SELECT COUNT(*) FROM music_dw.dwd_play_end)  + (SELECT COUNT(*) FROM music_dw.dwd_play_start)
-- + (SELECT COUNT(*) FROM music_dw.dwd_skip)      + (SELECT COUNT(*) FROM music_dw.dwd_search)
-- + (SELECT COUNT(*) FROM music_dw.dwd_collect)   + (SELECT COUNT(*) FROM music_dw.dwd_share)
-- + (SELECT COUNT(*) FROM music_dw.dwd_page_view) AS dwd_cnt,
--   (SELECT COUNT(*) FROM music_dw.dwd_quality_error) AS err_cnt;
--
-- 规则分布（每条数据只记一条，合计 = err_cnt）：
-- SELECT rule_name, COUNT(*) FROM music_dw.dwd_quality_error GROUP BY rule_name ORDER BY 2 DESC;
