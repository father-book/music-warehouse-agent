-- 自动生成：DWD 清洗（公共规则 + 6 类事件 + 6 张明细）
-- 注意：dwd_play_end 的清洗见 doris_dwd_play_end_clean.sql
USE music_dw;

-- =============================================================================
-- Part 0  事件名非法（不在白名单）
-- =============================================================================
-- 这类数据无法归属任何 DWD 明细表，但必须被记录——
-- 否则 ODS 总行数会比「DWD 合计 + 异常合计」多出一截，对账时无法解释。
INSERT INTO dwd_quality_error
SELECT event_id,
       'event_name_invalid'                       AS rule_name,
       event_name,
       'block'                                    AS rule_level,
       CONCAT('event_name=', COALESCE(event_name, 'NULL')) AS detail,
       get_json_string(properties, '$.song_id'),
       user_id, device_id,
       CASE WHEN event_time REGEXP '^[0-9]{13}$'
            THEN FROM_UNIXTIME(CAST(event_time AS BIGINT)/1000) ELSE NULL END,
       properties, etl_time, CURDATE()
FROM ods_music_event
WHERE event_name IS NULL
   OR event_name NOT IN ('play_start','play_end','skip','search','collect','share','page_view');


-- =============================================================================
-- Part 1  公共规则异常（7 类事件通用）
-- =============================================================================
INSERT INTO dwd_quality_error
SELECT t.event_id, t.rule_name, t.event_name, 'block', t.detail,
       get_json_string(t.properties, '$.song_id'), t.user_id, t.device_id,
       t.event_ts, t.properties, t.etl_time, CURDATE()
FROM (
    SELECT event_id, event_name, user_id, device_id, is_login, event_time,
           properties, etl_time,
           CASE WHEN event_time REGEXP '^[0-9]{13}$'
                THEN FROM_UNIXTIME(CAST(event_time AS BIGINT)/1000) ELSE NULL END AS event_ts,
           CASE
               WHEN event_id IS NULL OR event_id = '' THEN 'event_id_missing'
               WHEN COALESCE(device_id,'') = ''      THEN 'device_id_missing'
               WHEN is_login = 'true' AND COALESCE(user_id,'') = ''
                                                     THEN 'user_id_missing_when_login'
               WHEN event_time IS NULL OR event_time NOT REGEXP '^[0-9]{13}$'
                                                     THEN 'event_time_missing'
               WHEN CAST(event_time AS BIGINT) > UNIX_TIMESTAMP(NOW())*1000 + 300000
                                                     THEN 'event_time_future'
               WHEN CAST(event_time AS BIGINT) < 1700000000000
                                                     THEN 'event_time_too_old'
               ELSE NULL END AS rule_name,
           CASE
               WHEN event_id IS NULL OR event_id = '' THEN 'event_id 为空'
               WHEN COALESCE(device_id,'') = ''      THEN 'device_id 为空（设备不可识别）'
               WHEN is_login = 'true' AND COALESCE(user_id,'') = ''
                                                     THEN 'is_login=true 但 user_id 为空'
               WHEN event_time IS NULL OR event_time NOT REGEXP '^[0-9]{13}$'
                                                     THEN CONCAT('event_time=', COALESCE(event_time,'NULL'))
               WHEN CAST(event_time AS BIGINT) > UNIX_TIMESTAMP(NOW())*1000 + 300000
                                                     THEN CONCAT('event_time 超前: ', event_time)
               WHEN CAST(event_time AS BIGINT) < 1700000000000
                                                     THEN CONCAT('event_time 过早: ', event_time)
               ELSE '' END AS detail
    FROM ods_music_event
    WHERE event_name IN ('play_start','skip','search','collect','share','page_view')
) t
WHERE t.rule_name IS NOT NULL;


-- -----------------------------------------------------------------------------
-- play_start 事件属性异常
-- -----------------------------------------------------------------------------
INSERT INTO dwd_quality_error
SELECT t.event_id,
       CASE
               WHEN t.p_song_id IS NULL OR t.p_song_id = '' THEN 'song_id_missing'
           ELSE NULL END AS rule_name,
       t.event_name, 'block',
       CASE
               WHEN t.p_song_id IS NULL OR t.p_song_id = '' THEN 'song_id 缺失'
           ELSE '' END   AS detail,
       get_json_string(t.properties, '$.song_id'), t.user_id, t.device_id,
       t.event_ts, t.properties, t.etl_time, CURDATE()
FROM (
    SELECT u.*,
        get_json_string(properties, '$.song_id') AS p_song_id,
        get_json_string(properties, '$.playlist_id') AS p_playlist_id,
        get_json_string(properties, '$.quality') AS p_quality,
        get_json_string(properties, '$.play_mode') AS p_play_mode,
        CAST(get_json_string(properties, '$.song_index') AS INT) AS p_song_index
    FROM (
        SELECT event_id, event_name, user_id, device_id, is_login, event_time,
               properties, etl_time,
               CASE WHEN event_time REGEXP '^[0-9]{13}$'
                    THEN FROM_UNIXTIME(CAST(event_time AS BIGINT)/1000) ELSE NULL END AS event_ts
        FROM ods_music_event
        WHERE event_name = 'play_start'
    ) u
) t
WHERE (t.event_id IS NOT NULL AND t.event_id <> '')
  AND (COALESCE(t.device_id,'') <> '')
  AND NOT (t.is_login = 'true' AND COALESCE(t.user_id,'') = '')
  AND t.event_time REGEXP '^[0-9]{13}$'
  AND CAST(t.event_time AS BIGINT) <= UNIX_TIMESTAMP(NOW())*1000 + 300000
  AND CAST(t.event_time AS BIGINT) >= 1700000000000
  AND (t.p_song_id IS NULL OR t.p_song_id = '');


-- -----------------------------------------------------------------------------
-- skip 事件属性异常
-- -----------------------------------------------------------------------------
INSERT INTO dwd_quality_error
SELECT t.event_id,
       CASE
               WHEN t.p_song_id IS NULL OR t.p_song_id = '' THEN 'song_id_missing'
               WHEN t.p_played_duration IS NULL THEN 'played_duration_missing'
               WHEN p_played_duration < 0 THEN 'played_duration_negative'
           ELSE NULL END AS rule_name,
       t.event_name, 'block',
       CASE
               WHEN t.p_song_id IS NULL OR t.p_song_id = '' THEN 'song_id 缺失'
               WHEN t.p_played_duration IS NULL THEN 'played_duration 缺失'
               WHEN p_played_duration < 0 THEN 'played_duration<0'
           ELSE '' END   AS detail,
       get_json_string(t.properties, '$.song_id'), t.user_id, t.device_id,
       t.event_ts, t.properties, t.etl_time, CURDATE()
FROM (
    SELECT u.*,
        get_json_string(properties, '$.song_id') AS p_song_id,
        CAST(get_json_string(properties, '$.played_duration') AS INT) AS p_played_duration,
        get_json_string(properties, '$.skip_type') AS p_skip_type
    FROM (
        SELECT event_id, event_name, user_id, device_id, is_login, event_time,
               properties, etl_time,
               CASE WHEN event_time REGEXP '^[0-9]{13}$'
                    THEN FROM_UNIXTIME(CAST(event_time AS BIGINT)/1000) ELSE NULL END AS event_ts
        FROM ods_music_event
        WHERE event_name = 'skip'
    ) u
) t
WHERE (t.event_id IS NOT NULL AND t.event_id <> '')
  AND (COALESCE(t.device_id,'') <> '')
  AND NOT (t.is_login = 'true' AND COALESCE(t.user_id,'') = '')
  AND t.event_time REGEXP '^[0-9]{13}$'
  AND CAST(t.event_time AS BIGINT) <= UNIX_TIMESTAMP(NOW())*1000 + 300000
  AND CAST(t.event_time AS BIGINT) >= 1700000000000
  AND (t.p_song_id IS NULL OR t.p_song_id = '' OR t.p_played_duration IS NULL OR (p_played_duration < 0));


-- -----------------------------------------------------------------------------
-- search 事件属性异常
-- -----------------------------------------------------------------------------
INSERT INTO dwd_quality_error
SELECT t.event_id,
       CASE
               WHEN t.p_keyword IS NULL OR t.p_keyword = '' THEN 'keyword_missing'
               WHEN p_result_count < 0 THEN 'result_count_negative'
           ELSE NULL END AS rule_name,
       t.event_name, 'block',
       CASE
               WHEN t.p_keyword IS NULL OR t.p_keyword = '' THEN 'keyword 缺失'
               WHEN p_result_count < 0 THEN 'result_count<0'
           ELSE '' END   AS detail,
       get_json_string(t.properties, '$.song_id'), t.user_id, t.device_id,
       t.event_ts, t.properties, t.etl_time, CURDATE()
FROM (
    SELECT u.*,
        get_json_string(properties, '$.keyword') AS p_keyword,
        get_json_string(properties, '$.search_type') AS p_search_type,
        CAST(get_json_string(properties, '$.result_count') AS INT) AS p_result_count,
        get_json_string(properties, '$.clicked_song_id') AS p_clicked_song_id
    FROM (
        SELECT event_id, event_name, user_id, device_id, is_login, event_time,
               properties, etl_time,
               CASE WHEN event_time REGEXP '^[0-9]{13}$'
                    THEN FROM_UNIXTIME(CAST(event_time AS BIGINT)/1000) ELSE NULL END AS event_ts
        FROM ods_music_event
        WHERE event_name = 'search'
    ) u
) t
WHERE (t.event_id IS NOT NULL AND t.event_id <> '')
  AND (COALESCE(t.device_id,'') <> '')
  AND NOT (t.is_login = 'true' AND COALESCE(t.user_id,'') = '')
  AND t.event_time REGEXP '^[0-9]{13}$'
  AND CAST(t.event_time AS BIGINT) <= UNIX_TIMESTAMP(NOW())*1000 + 300000
  AND CAST(t.event_time AS BIGINT) >= 1700000000000
  AND (t.p_keyword IS NULL OR t.p_keyword = '' OR (p_result_count < 0));


-- -----------------------------------------------------------------------------
-- collect 事件属性异常
-- -----------------------------------------------------------------------------
INSERT INTO dwd_quality_error
SELECT t.event_id,
       CASE
               WHEN t.p_song_id IS NULL OR t.p_song_id = '' THEN 'song_id_missing'
               WHEN t.p_collect_type IS NULL OR t.p_collect_type = '' THEN 'collect_type_missing'
               WHEN p_collect_type NOT IN ('add','cancel') THEN 'collect_type_invalid'
           ELSE NULL END AS rule_name,
       t.event_name, 'block',
       CASE
               WHEN t.p_song_id IS NULL OR t.p_song_id = '' THEN 'song_id 缺失'
               WHEN t.p_collect_type IS NULL OR t.p_collect_type = '' THEN 'collect_type 缺失'
               WHEN p_collect_type NOT IN ('add','cancel') THEN 'collect_type 非 add/cancel'
           ELSE '' END   AS detail,
       get_json_string(t.properties, '$.song_id'), t.user_id, t.device_id,
       t.event_ts, t.properties, t.etl_time, CURDATE()
FROM (
    SELECT u.*,
        get_json_string(properties, '$.song_id') AS p_song_id,
        get_json_string(properties, '$.collect_type') AS p_collect_type
    FROM (
        SELECT event_id, event_name, user_id, device_id, is_login, event_time,
               properties, etl_time,
               CASE WHEN event_time REGEXP '^[0-9]{13}$'
                    THEN FROM_UNIXTIME(CAST(event_time AS BIGINT)/1000) ELSE NULL END AS event_ts
        FROM ods_music_event
        WHERE event_name = 'collect'
    ) u
) t
WHERE (t.event_id IS NOT NULL AND t.event_id <> '')
  AND (COALESCE(t.device_id,'') <> '')
  AND NOT (t.is_login = 'true' AND COALESCE(t.user_id,'') = '')
  AND t.event_time REGEXP '^[0-9]{13}$'
  AND CAST(t.event_time AS BIGINT) <= UNIX_TIMESTAMP(NOW())*1000 + 300000
  AND CAST(t.event_time AS BIGINT) >= 1700000000000
  AND (t.p_song_id IS NULL OR t.p_song_id = '' OR t.p_collect_type IS NULL OR t.p_collect_type = '' OR (p_collect_type NOT IN ('add','cancel')));


-- -----------------------------------------------------------------------------
-- share 事件属性异常
-- -----------------------------------------------------------------------------
INSERT INTO dwd_quality_error
SELECT t.event_id,
       CASE
               WHEN t.p_song_id IS NULL OR t.p_song_id = '' THEN 'song_id_missing'
               WHEN t.p_share_channel IS NULL OR t.p_share_channel = '' THEN 'share_channel_missing'
           ELSE NULL END AS rule_name,
       t.event_name, 'block',
       CASE
               WHEN t.p_song_id IS NULL OR t.p_song_id = '' THEN 'song_id 缺失'
               WHEN t.p_share_channel IS NULL OR t.p_share_channel = '' THEN 'share_channel 缺失'
           ELSE '' END   AS detail,
       get_json_string(t.properties, '$.song_id'), t.user_id, t.device_id,
       t.event_ts, t.properties, t.etl_time, CURDATE()
FROM (
    SELECT u.*,
        get_json_string(properties, '$.song_id') AS p_song_id,
        get_json_string(properties, '$.share_channel') AS p_share_channel
    FROM (
        SELECT event_id, event_name, user_id, device_id, is_login, event_time,
               properties, etl_time,
               CASE WHEN event_time REGEXP '^[0-9]{13}$'
                    THEN FROM_UNIXTIME(CAST(event_time AS BIGINT)/1000) ELSE NULL END AS event_ts
        FROM ods_music_event
        WHERE event_name = 'share'
    ) u
) t
WHERE (t.event_id IS NOT NULL AND t.event_id <> '')
  AND (COALESCE(t.device_id,'') <> '')
  AND NOT (t.is_login = 'true' AND COALESCE(t.user_id,'') = '')
  AND t.event_time REGEXP '^[0-9]{13}$'
  AND CAST(t.event_time AS BIGINT) <= UNIX_TIMESTAMP(NOW())*1000 + 300000
  AND CAST(t.event_time AS BIGINT) >= 1700000000000
  AND (t.p_song_id IS NULL OR t.p_song_id = '' OR t.p_share_channel IS NULL OR t.p_share_channel = '');


-- -----------------------------------------------------------------------------
-- page_view 事件属性异常
-- -----------------------------------------------------------------------------
INSERT INTO dwd_quality_error
SELECT t.event_id,
       CASE
               WHEN t.p_view_page_id IS NULL OR t.p_view_page_id = '' THEN 'view_page_id_missing'
               WHEN p_stay_duration < 0 THEN 'stay_duration_negative'
           ELSE NULL END AS rule_name,
       t.event_name, 'block',
       CASE
               WHEN t.p_view_page_id IS NULL OR t.p_view_page_id = '' THEN 'view_page_id 缺失'
               WHEN p_stay_duration < 0 THEN 'stay_duration<0'
           ELSE '' END   AS detail,
       get_json_string(t.properties, '$.song_id'), t.user_id, t.device_id,
       t.event_ts, t.properties, t.etl_time, CURDATE()
FROM (
    SELECT u.*,
        get_json_string(properties, '$.page_id') AS p_view_page_id,
        get_json_string(properties, '$.referrer_page') AS p_referrer_page,
        CAST(get_json_string(properties, '$.stay_duration') AS INT) AS p_stay_duration
    FROM (
        SELECT event_id, event_name, user_id, device_id, is_login, event_time,
               properties, etl_time,
               CASE WHEN event_time REGEXP '^[0-9]{13}$'
                    THEN FROM_UNIXTIME(CAST(event_time AS BIGINT)/1000) ELSE NULL END AS event_ts
        FROM ods_music_event
        WHERE event_name = 'page_view'
    ) u
) t
WHERE (t.event_id IS NOT NULL AND t.event_id <> '')
  AND (COALESCE(t.device_id,'') <> '')
  AND NOT (t.is_login = 'true' AND COALESCE(t.user_id,'') = '')
  AND t.event_time REGEXP '^[0-9]{13}$'
  AND CAST(t.event_time AS BIGINT) <= UNIX_TIMESTAMP(NOW())*1000 + 300000
  AND CAST(t.event_time AS BIGINT) >= 1700000000000
  AND (t.p_view_page_id IS NULL OR t.p_view_page_id = '' OR (p_stay_duration < 0));


-- -----------------------------------------------------------------------------
-- dwd_play_start 明细写入
-- -----------------------------------------------------------------------------
INSERT INTO dwd_play_start
SELECT
    t.event_id,
    t.event_ts,
    COALESCE(t.server_ts, t.event_ts),
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
    t.p_song_id,
    t.p_playlist_id,
    t.p_quality,
    t.p_play_mode,
    t.p_song_index,
    t.etl_time,
    DATE(t.event_ts)
FROM (
    SELECT u.*,
        get_json_string(properties, '$.song_id') AS p_song_id,
        get_json_string(properties, '$.playlist_id') AS p_playlist_id,
        get_json_string(properties, '$.quality') AS p_quality,
        get_json_string(properties, '$.play_mode') AS p_play_mode,
        CAST(get_json_string(properties, '$.song_index') AS INT) AS p_song_index
    FROM (
        SELECT event_id, event_name, user_id, device_id,
               is_login, is_vip, app_version, os, os_version, device_model,
               network, carrier, ip, province, city, session_id, session_seq,
               is_foreground, channel, page_id, properties, etl_time, event_time,
               CASE WHEN event_time REGEXP '^[0-9]{13}$'
                    THEN FROM_UNIXTIME(CAST(event_time AS BIGINT)/1000) ELSE NULL END AS event_ts,
               CASE WHEN server_time REGEXP '^[0-9]{13}$'
                    THEN FROM_UNIXTIME(CAST(server_time AS BIGINT)/1000) ELSE NULL END AS server_ts
        FROM ods_music_event
        WHERE event_name = 'play_start'
    ) u
) t
WHERE (t.event_id IS NOT NULL AND t.event_id <> '')
  AND (COALESCE(t.device_id,'') <> '')
  AND NOT (t.is_login = 'true' AND COALESCE(t.user_id,'') = '')
  AND t.event_time REGEXP '^[0-9]{13}$'
  AND CAST(t.event_time AS BIGINT) <= UNIX_TIMESTAMP(NOW())*1000 + 300000
  AND CAST(t.event_time AS BIGINT) >= 1700000000000
  AND t.p_song_id IS NOT NULL AND t.p_song_id <> '';


-- -----------------------------------------------------------------------------
-- dwd_skip 明细写入
-- -----------------------------------------------------------------------------
INSERT INTO dwd_skip
SELECT
    t.event_id,
    t.event_ts,
    COALESCE(t.server_ts, t.event_ts),
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
    t.p_song_id,
    t.p_played_duration,
    t.p_skip_type,
    t.etl_time,
    DATE(t.event_ts)
FROM (
    SELECT u.*,
        get_json_string(properties, '$.song_id') AS p_song_id,
        CAST(get_json_string(properties, '$.played_duration') AS INT) AS p_played_duration,
        get_json_string(properties, '$.skip_type') AS p_skip_type
    FROM (
        SELECT event_id, event_name, user_id, device_id,
               is_login, is_vip, app_version, os, os_version, device_model,
               network, carrier, ip, province, city, session_id, session_seq,
               is_foreground, channel, page_id, properties, etl_time, event_time,
               CASE WHEN event_time REGEXP '^[0-9]{13}$'
                    THEN FROM_UNIXTIME(CAST(event_time AS BIGINT)/1000) ELSE NULL END AS event_ts,
               CASE WHEN server_time REGEXP '^[0-9]{13}$'
                    THEN FROM_UNIXTIME(CAST(server_time AS BIGINT)/1000) ELSE NULL END AS server_ts
        FROM ods_music_event
        WHERE event_name = 'skip'
    ) u
) t
WHERE (t.event_id IS NOT NULL AND t.event_id <> '')
  AND (COALESCE(t.device_id,'') <> '')
  AND NOT (t.is_login = 'true' AND COALESCE(t.user_id,'') = '')
  AND t.event_time REGEXP '^[0-9]{13}$'
  AND CAST(t.event_time AS BIGINT) <= UNIX_TIMESTAMP(NOW())*1000 + 300000
  AND CAST(t.event_time AS BIGINT) >= 1700000000000
  AND t.p_song_id IS NOT NULL AND t.p_song_id <> ''
  AND t.p_played_duration IS NOT NULL
  AND NOT (p_played_duration < 0);


-- -----------------------------------------------------------------------------
-- dwd_search 明细写入
-- -----------------------------------------------------------------------------
INSERT INTO dwd_search
SELECT
    t.event_id,
    t.event_ts,
    COALESCE(t.server_ts, t.event_ts),
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
    t.p_keyword,
    t.p_search_type,
    t.p_result_count,
    t.p_clicked_song_id,
    t.etl_time,
    DATE(t.event_ts)
FROM (
    SELECT u.*,
        get_json_string(properties, '$.keyword') AS p_keyword,
        get_json_string(properties, '$.search_type') AS p_search_type,
        CAST(get_json_string(properties, '$.result_count') AS INT) AS p_result_count,
        get_json_string(properties, '$.clicked_song_id') AS p_clicked_song_id
    FROM (
        SELECT event_id, event_name, user_id, device_id,
               is_login, is_vip, app_version, os, os_version, device_model,
               network, carrier, ip, province, city, session_id, session_seq,
               is_foreground, channel, page_id, properties, etl_time, event_time,
               CASE WHEN event_time REGEXP '^[0-9]{13}$'
                    THEN FROM_UNIXTIME(CAST(event_time AS BIGINT)/1000) ELSE NULL END AS event_ts,
               CASE WHEN server_time REGEXP '^[0-9]{13}$'
                    THEN FROM_UNIXTIME(CAST(server_time AS BIGINT)/1000) ELSE NULL END AS server_ts
        FROM ods_music_event
        WHERE event_name = 'search'
    ) u
) t
WHERE (t.event_id IS NOT NULL AND t.event_id <> '')
  AND (COALESCE(t.device_id,'') <> '')
  AND NOT (t.is_login = 'true' AND COALESCE(t.user_id,'') = '')
  AND t.event_time REGEXP '^[0-9]{13}$'
  AND CAST(t.event_time AS BIGINT) <= UNIX_TIMESTAMP(NOW())*1000 + 300000
  AND CAST(t.event_time AS BIGINT) >= 1700000000000
  AND t.p_keyword IS NOT NULL AND t.p_keyword <> ''
  AND NOT (p_result_count < 0);


-- -----------------------------------------------------------------------------
-- dwd_collect 明细写入
-- -----------------------------------------------------------------------------
INSERT INTO dwd_collect
SELECT
    t.event_id,
    t.event_ts,
    COALESCE(t.server_ts, t.event_ts),
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
    t.p_song_id,
    t.p_collect_type,
    t.etl_time,
    DATE(t.event_ts)
FROM (
    SELECT u.*,
        get_json_string(properties, '$.song_id') AS p_song_id,
        get_json_string(properties, '$.collect_type') AS p_collect_type
    FROM (
        SELECT event_id, event_name, user_id, device_id,
               is_login, is_vip, app_version, os, os_version, device_model,
               network, carrier, ip, province, city, session_id, session_seq,
               is_foreground, channel, page_id, properties, etl_time, event_time,
               CASE WHEN event_time REGEXP '^[0-9]{13}$'
                    THEN FROM_UNIXTIME(CAST(event_time AS BIGINT)/1000) ELSE NULL END AS event_ts,
               CASE WHEN server_time REGEXP '^[0-9]{13}$'
                    THEN FROM_UNIXTIME(CAST(server_time AS BIGINT)/1000) ELSE NULL END AS server_ts
        FROM ods_music_event
        WHERE event_name = 'collect'
    ) u
) t
WHERE (t.event_id IS NOT NULL AND t.event_id <> '')
  AND (COALESCE(t.device_id,'') <> '')
  AND NOT (t.is_login = 'true' AND COALESCE(t.user_id,'') = '')
  AND t.event_time REGEXP '^[0-9]{13}$'
  AND CAST(t.event_time AS BIGINT) <= UNIX_TIMESTAMP(NOW())*1000 + 300000
  AND CAST(t.event_time AS BIGINT) >= 1700000000000
  AND t.p_song_id IS NOT NULL AND t.p_song_id <> ''
  AND t.p_collect_type IS NOT NULL AND t.p_collect_type <> ''
  AND NOT (p_collect_type NOT IN ('add','cancel'));


-- -----------------------------------------------------------------------------
-- dwd_share 明细写入
-- -----------------------------------------------------------------------------
INSERT INTO dwd_share
SELECT
    t.event_id,
    t.event_ts,
    COALESCE(t.server_ts, t.event_ts),
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
    t.p_song_id,
    t.p_share_channel,
    t.etl_time,
    DATE(t.event_ts)
FROM (
    SELECT u.*,
        get_json_string(properties, '$.song_id') AS p_song_id,
        get_json_string(properties, '$.share_channel') AS p_share_channel
    FROM (
        SELECT event_id, event_name, user_id, device_id,
               is_login, is_vip, app_version, os, os_version, device_model,
               network, carrier, ip, province, city, session_id, session_seq,
               is_foreground, channel, page_id, properties, etl_time, event_time,
               CASE WHEN event_time REGEXP '^[0-9]{13}$'
                    THEN FROM_UNIXTIME(CAST(event_time AS BIGINT)/1000) ELSE NULL END AS event_ts,
               CASE WHEN server_time REGEXP '^[0-9]{13}$'
                    THEN FROM_UNIXTIME(CAST(server_time AS BIGINT)/1000) ELSE NULL END AS server_ts
        FROM ods_music_event
        WHERE event_name = 'share'
    ) u
) t
WHERE (t.event_id IS NOT NULL AND t.event_id <> '')
  AND (COALESCE(t.device_id,'') <> '')
  AND NOT (t.is_login = 'true' AND COALESCE(t.user_id,'') = '')
  AND t.event_time REGEXP '^[0-9]{13}$'
  AND CAST(t.event_time AS BIGINT) <= UNIX_TIMESTAMP(NOW())*1000 + 300000
  AND CAST(t.event_time AS BIGINT) >= 1700000000000
  AND t.p_song_id IS NOT NULL AND t.p_song_id <> ''
  AND t.p_share_channel IS NOT NULL AND t.p_share_channel <> '';


-- -----------------------------------------------------------------------------
-- dwd_page_view 明细写入
-- -----------------------------------------------------------------------------
INSERT INTO dwd_page_view
SELECT
    t.event_id,
    t.event_ts,
    COALESCE(t.server_ts, t.event_ts),
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
    t.p_view_page_id,
    t.p_referrer_page,
    t.p_stay_duration,
    t.etl_time,
    DATE(t.event_ts)
FROM (
    SELECT u.*,
        get_json_string(properties, '$.page_id') AS p_view_page_id,
        get_json_string(properties, '$.referrer_page') AS p_referrer_page,
        CAST(get_json_string(properties, '$.stay_duration') AS INT) AS p_stay_duration
    FROM (
        SELECT event_id, event_name, user_id, device_id,
               is_login, is_vip, app_version, os, os_version, device_model,
               network, carrier, ip, province, city, session_id, session_seq,
               is_foreground, channel, page_id, properties, etl_time, event_time,
               CASE WHEN event_time REGEXP '^[0-9]{13}$'
                    THEN FROM_UNIXTIME(CAST(event_time AS BIGINT)/1000) ELSE NULL END AS event_ts,
               CASE WHEN server_time REGEXP '^[0-9]{13}$'
                    THEN FROM_UNIXTIME(CAST(server_time AS BIGINT)/1000) ELSE NULL END AS server_ts
        FROM ods_music_event
        WHERE event_name = 'page_view'
    ) u
) t
WHERE (t.event_id IS NOT NULL AND t.event_id <> '')
  AND (COALESCE(t.device_id,'') <> '')
  AND NOT (t.is_login = 'true' AND COALESCE(t.user_id,'') = '')
  AND t.event_time REGEXP '^[0-9]{13}$'
  AND CAST(t.event_time AS BIGINT) <= UNIX_TIMESTAMP(NOW())*1000 + 300000
  AND CAST(t.event_time AS BIGINT) >= 1700000000000
  AND t.p_view_page_id IS NOT NULL AND t.p_view_page_id <> ''
  AND NOT (p_stay_duration < 0);
