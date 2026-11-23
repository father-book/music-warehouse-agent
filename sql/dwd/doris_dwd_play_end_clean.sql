-- =============================================================================
-- DWD-播放结束明细清洗（批式版）
-- 执行位置：任意能连 Doris 9030 的机器
-- 前置：已执行 doris_dwd_tables.sql
-- =============================================================================
-- 分流逻辑：
--   解析 JSON → 逐条判定 → 命中规则写异常表，全部通过写 DWD 明细表
--   两张表用同一套判定条件，保证"被拦下的不会漏进 DWD，进了 DWD 的一定干净"
--
-- 为什么两次扫描 ODS：
--   Doris 不支持一条 INSERT 写两张表。两次扫描换来逻辑对齐，比写两套条件更可靠。
--   数据量上来后可改为 Flink 双 sink（一次消费，两个出口）。
--
-- 规则优先级（CASE 从上到下，命中即停）：
--   一条数据只记一条规则，避免规则串扰导致召回率统计超过 100%
-- =============================================================================

USE music_dw;

-- -----------------------------------------------------------------------------
-- 1) 异常数据 → dwd_quality_error
-- -----------------------------------------------------------------------------
INSERT INTO dwd_quality_error
SELECT
    t.event_id,
    t.rule_name,
    t.event_name,
    'block'                 AS rule_level,
    t.detail,
    t.p_song_id,
    t.user_id,
    t.device_id,
    t.event_ts,
    t.properties,
    t.etl_time,
    CURDATE()               AS dt
FROM (
    SELECT
        event_id,
        event_name,
        user_id,
        device_id,
        properties,
        etl_time,
        get_json_string(properties, '$.song_id')                        AS p_song_id,
        CAST(get_json_string(properties, '$.song_duration') AS INT)     AS p_song_dur,
        CAST(get_json_string(properties, '$.play_duration') AS INT)     AS p_play_dur,
        CAST(get_json_string(properties, '$.play_progress') AS DOUBLE)  AS p_progress,
        get_json_string(properties, '$.end_reason')                     AS p_end_reason,
        CASE WHEN event_time REGEXP '^[0-9]{13}$'
             THEN FROM_UNIXTIME(CAST(event_time AS BIGINT) / 1000)
             ELSE NULL END                                              AS event_ts,
        CASE
            WHEN event_id IS NULL OR event_id = ''
                THEN 'event_id_missing'
            WHEN COALESCE(device_id, '') = ''
                THEN 'device_id_missing'
            WHEN is_login = 'true' AND (user_id IS NULL OR user_id = '')
                THEN 'user_id_missing_when_login'
            WHEN event_time IS NULL OR event_time NOT REGEXP '^[0-9]{13}$'
                THEN 'event_time_missing'
            WHEN CAST(event_time AS BIGINT) > UNIX_TIMESTAMP(NOW()) * 1000 + 300000
                THEN 'event_time_future'
            WHEN CAST(event_time AS BIGINT) < 1700000000000
                THEN 'event_time_too_old'
            WHEN get_json_string(properties, '$.song_id') IS NULL
              OR get_json_string(properties, '$.song_id') = ''
                THEN 'song_id_missing'
            WHEN get_json_string(properties, '$.song_duration') IS NULL
                THEN 'song_duration_missing'
            WHEN get_json_string(properties, '$.play_duration') IS NULL
                THEN 'play_duration_missing'
            WHEN CAST(get_json_string(properties, '$.play_duration') AS INT)
               > CAST(get_json_string(properties, '$.song_duration') AS INT)
                THEN 'duration_exceed'
            WHEN CAST(get_json_string(properties, '$.play_progress') AS DOUBLE) < 0
              OR CAST(get_json_string(properties, '$.play_progress') AS DOUBLE) > 1
                THEN 'progress_invalid'
            WHEN get_json_string(properties, '$.end_reason') IS NULL
              OR get_json_string(properties, '$.end_reason')
                 NOT IN ('natural', 'skip', 'pause', 'exit', 'interrupt')
                THEN 'end_reason_invalid'
            ELSE NULL
        END AS rule_name,
        CASE
            WHEN event_id IS NULL OR event_id = ''
                THEN 'event_id 为空'
            WHEN COALESCE(device_id, '') = ''
                THEN 'device_id 为空（设备不可识别）'
            WHEN is_login = 'true' AND (user_id IS NULL OR user_id = '')
                THEN CONCAT('is_login=true 但 user_id 为空（登录态矛盾）')
            WHEN event_time IS NULL OR event_time NOT REGEXP '^[0-9]{13}$'
                THEN CONCAT('event_time=', COALESCE(event_time, 'NULL'))
            WHEN CAST(event_time AS BIGINT) > UNIX_TIMESTAMP(NOW()) * 1000 + 300000
                THEN CONCAT('event_time 超前 5 分钟以上: ', event_time)
            WHEN CAST(event_time AS BIGINT) < 1700000000000
                THEN CONCAT('event_time 早于 2023: ', event_time)
            WHEN get_json_string(properties, '$.song_id') IS NULL
              OR get_json_string(properties, '$.song_id') = ''
                THEN 'properties.song_id 为空'
            WHEN get_json_string(properties, '$.song_duration') IS NULL
                THEN 'properties.song_duration 缺失'
            WHEN get_json_string(properties, '$.play_duration') IS NULL
                THEN 'properties.play_duration 缺失'
            WHEN CAST(get_json_string(properties, '$.play_duration') AS INT)
               > CAST(get_json_string(properties, '$.song_duration') AS INT)
                THEN CONCAT('play_duration=', get_json_string(properties, '$.play_duration'),
                            ' > song_duration=', get_json_string(properties, '$.song_duration'))
            WHEN CAST(get_json_string(properties, '$.play_progress') AS DOUBLE) < 0
              OR CAST(get_json_string(properties, '$.play_progress') AS DOUBLE) > 1
                THEN CONCAT('play_progress=', get_json_string(properties, '$.play_progress'))
            WHEN get_json_string(properties, '$.end_reason') IS NULL
              OR get_json_string(properties, '$.end_reason')
                 NOT IN ('natural', 'skip', 'pause', 'exit', 'interrupt')
                THEN CONCAT('end_reason=', COALESCE(get_json_string(properties, '$.end_reason'), 'NULL'))
            ELSE ''
        END AS detail
    FROM ods_music_event
    WHERE event_name = 'play_end'
) t
WHERE t.rule_name IS NOT NULL;

-- -----------------------------------------------------------------------------
-- 2) 干净数据 → dwd_play_end
-- -----------------------------------------------------------------------------
-- 条件与上面严格互补：任一规则命中即排除
-- -----------------------------------------------------------------------------
INSERT INTO dwd_play_end
SELECT
    t.event_id,
    t.event_ts,
    t.server_ts,
    t.user_id,
    t.device_id,
    t.is_login,
    t.is_vip,
    t.app_version,
    t.os,
    t.os_version,
    t.device_model,
    t.network,
    t.carrier,
    t.ip,
    t.province,
    t.city,
    t.session_id,
    t.session_seq,
    t.is_foreground,
    t.channel,
    t.page_id,
    t.p_song_id                    AS song_id,
    t.p_song_dur                   AS song_duration,
    t.p_play_dur                   AS play_duration,
    t.p_progress                   AS play_progress,
    t.p_end_reason                 AS end_reason,
    t.etl_time,
    DATE(t.event_ts)               AS dt
FROM (
    SELECT
        event_id, event_name, user_id, device_id,
        is_login, is_vip, app_version, os, os_version, device_model,
        network, carrier, ip, province, city, session_id, session_seq,
        is_foreground, channel, page_id, properties, etl_time,
        get_json_string(properties, '$.song_id')                        AS p_song_id,
        CAST(get_json_string(properties, '$.song_duration') AS INT)     AS p_song_dur,
        CAST(get_json_string(properties, '$.play_duration') AS INT)     AS p_play_dur,
        CAST(get_json_string(properties, '$.play_progress') AS DOUBLE)  AS p_progress,
        get_json_string(properties, '$.end_reason')                     AS p_end_reason,
        CASE WHEN event_time REGEXP '^[0-9]{13}$'
             THEN FROM_UNIXTIME(CAST(event_time AS BIGINT) / 1000)
             ELSE NULL END                                              AS event_ts,
        CASE WHEN server_time REGEXP '^[0-9]{13}$'
             THEN FROM_UNIXTIME(CAST(server_time AS BIGINT) / 1000)
             ELSE NULL END                                              AS server_ts
    FROM ods_music_event
    WHERE event_name = 'play_end'
) t
WHERE t.event_id IS NOT NULL AND t.event_id <> ''
  AND COALESCE(t.device_id, '') <> ''
  AND NOT (t.is_login = 'true' AND COALESCE(t.user_id, '') = '')
  AND t.event_ts IS NOT NULL
  AND CAST(t.event_ts AS DATETIME) <= NOW() + INTERVAL 5 MINUTE
  AND t.p_song_id IS NOT NULL AND t.p_song_id <> ''
  AND t.p_song_dur IS NOT NULL
  AND t.p_play_dur IS NOT NULL
  AND t.p_play_dur <= t.p_song_dur
  AND t.p_progress IS NOT NULL AND t.p_progress >= 0 AND t.p_progress <= 1
  AND t.p_end_reason IN ('natural', 'skip', 'pause', 'exit', 'interrupt');

-- =============================================================================
-- 验证（执行后运行）
-- =============================================================================
-- 1. 两端条数应当互补（DWD 条数 + 异常条数 = ODS 中 play_end 条数）
--    SELECT
--      (SELECT COUNT(*) FROM dwd_play_end)      AS dwd_cnt,
--      (SELECT COUNT(*) FROM dwd_quality_error) AS err_cnt,
--      (SELECT COUNT(*) FROM ods_music_event WHERE event_name='play_end') AS ods_cnt;
--
-- 2. 规则分布
--    SELECT rule_name, COUNT(*) FROM dwd_quality_error GROUP BY rule_name ORDER BY 2 DESC;
--
-- 3. 完播率与切歌率（DWD 的第一个可用指标）
--    SELECT
--      COUNT(*) AS plays,
--      ROUND(SUM(CASE WHEN end_reason='natural' THEN 1 ELSE 0 END)/COUNT(*)*100, 2) AS finish_rate_pct,
--      ROUND(SUM(CASE WHEN end_reason='skip'    THEN 1 ELSE 0 END)/COUNT(*)*100, 2) AS skip_rate_pct
--    FROM dwd_play_end;
