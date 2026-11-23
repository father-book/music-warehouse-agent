-- =============================================================================
-- 11_dwd_domain_tables.sql —— 离线 DWD 域表（docs/19 §四 的 12 张目标态）
-- 由 code/gen_dwd_domain_sql.py 生成，不要手改本文件，改生成器后重跑。
--
-- 执行：
--   hive --hivevar start_dt=2026-10-05 --hivevar end_dt=2026-11-04 \
--        -f 11_dwd_domain_tables.sql
--
-- 本文件负责 10 张表：9 张新域表 + 接管 dwd_skip（替代 07 号脚本）。
-- dwd_play_start / dwd_play_end 仍由 04 / 03 负责（含派生列，下游指标依赖）。
-- 12 张表覆盖 26 类事件，无遗漏、无重叠。
--
-- 事件名以 config/event_weights.json 为准：docs/19 §四 把 dwd_pay 写成
-- vip_expose/vip_click/pay_success，与事件矩阵及生成器不一致，此处按实际
-- 的 vip_subscribe/order_pay/order_refund 落地（文档已加勘误）。
--
-- 每张表都保留 properties_raw（JSON 原文）：表结构按 docs/19 目标态设计，
-- v9 生成器已补齐 26 类事件的私有属性，此前多为 NULL 的私有列现在均有取值。
-- =============================================================================

SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;
SET hive.exec.max.dynamic.partitions = 200;
SET hive.exec.max.dynamic.partitions.pernode = 100;
SET hive.stats.autogather = false;


-- dwd_skip: 切歌明细
-- 事件: skip
-- [11-08 修正] 不再无条件 DROP：只补少数几天时会把历史分区一起清掉
-- DROP TABLE IF EXISTS music_dw.dwd_skip;
CREATE EXTERNAL TABLE IF NOT EXISTS music_dw.dwd_skip (
    event_id           STRING     COMMENT '事件ID',
    event_ts           TIMESTAMP  COMMENT '事件时间',
    server_ts          TIMESTAMP  COMMENT '服务端接收时间',
    user_id            STRING     COMMENT '用户ID',
    device_id          STRING     COMMENT '设备ID',
    is_login           STRING     COMMENT '是否登录',
    is_vip             STRING     COMMENT '是否会员',
    app_version        STRING     COMMENT 'APP版本',
    os                 STRING     COMMENT '操作系统',
    os_version         STRING     COMMENT '系统版本',
    device_model       STRING     COMMENT '设备型号',
    network            STRING     COMMENT '网络类型',
    carrier            STRING     COMMENT '运营商',
    ip                 STRING     COMMENT '客户端IP',
    province           STRING     COMMENT '省份',
    city               STRING     COMMENT '城市',
    session_id         STRING     COMMENT '会话ID',
    session_seq        STRING     COMMENT '会话内序号',
    is_foreground      STRING     COMMENT '前后台标记',
    channel            STRING     COMMENT '渠道',
    page_id            STRING     COMMENT '页面ID',
    event_name         STRING     COMMENT '域内事件名',
    song_id            STRING     COMMENT '跳过歌曲',
    played_duration    INT        COMMENT '跳过前已播放秒数',
    skip_type          STRING     COMMENT '跳过类型 manual/auto（v9 已生成）',
    properties_raw     STRING     COMMENT '事件私有属性 JSON 原文（保真）',
    etl_time           TIMESTAMP  COMMENT '入仓时间'
)
COMMENT 'DWD-切歌明细（离线）'
PARTITIONED BY (dt STRING)
STORED AS ORC
LOCATION 'hdfs://WWC160:9820/warehouse/music_dw/dwd_skip'
TBLPROPERTIES ('orc.compress' = 'SNAPPY');

INSERT OVERWRITE TABLE music_dw.dwd_skip PARTITION (dt)
SELECT
    event_id,
    from_utc_timestamp(CAST(from_unixtime(CAST(event_time AS BIGINT) DIV 1000) AS TIMESTAMP), 'Asia/Shanghai')  AS event_ts,
    from_utc_timestamp(CAST(from_unixtime(CAST(server_time AS BIGINT) DIV 1000) AS TIMESTAMP), 'Asia/Shanghai') AS server_ts,
    user_id, device_id, is_login, is_vip, app_version, os, os_version,
    device_model, network, carrier, ip, province, city,
    session_id, session_seq, is_foreground, channel, page_id,
    event_name,
    get_json_object(properties, '$.song_id') AS song_id,
    CAST(get_json_object(properties, '$.position') AS INT) AS played_duration,
    get_json_object(properties, '$.skip_type') AS skip_type,
    properties                              AS properties_raw,
    CAST(etl_time AS TIMESTAMP)             AS etl_time,
    dt
FROM music_dw.ods_music_event
WHERE dt BETWEEN '${start_dt}' AND '${end_dt}'
  AND event_name IN ('skip')
DISTRIBUTE BY dt;


-- dwd_session: 会话明细
-- 事件: app_launch, app_exit
-- [11-08 修正] 不再无条件 DROP：只补少数几天时会把历史分区一起清掉
-- DROP TABLE IF EXISTS music_dw.dwd_session;
CREATE EXTERNAL TABLE IF NOT EXISTS music_dw.dwd_session (
    event_id           STRING     COMMENT '事件ID',
    event_ts           TIMESTAMP  COMMENT '事件时间',
    server_ts          TIMESTAMP  COMMENT '服务端接收时间',
    user_id            STRING     COMMENT '用户ID',
    device_id          STRING     COMMENT '设备ID',
    is_login           STRING     COMMENT '是否登录',
    is_vip             STRING     COMMENT '是否会员',
    app_version        STRING     COMMENT 'APP版本',
    os                 STRING     COMMENT '操作系统',
    os_version         STRING     COMMENT '系统版本',
    device_model       STRING     COMMENT '设备型号',
    network            STRING     COMMENT '网络类型',
    carrier            STRING     COMMENT '运营商',
    ip                 STRING     COMMENT '客户端IP',
    province           STRING     COMMENT '省份',
    city               STRING     COMMENT '城市',
    session_id         STRING     COMMENT '会话ID',
    session_seq        STRING     COMMENT '会话内序号',
    is_foreground      STRING     COMMENT '前后台标记',
    channel            STRING     COMMENT '渠道',
    page_id            STRING     COMMENT '页面ID',
    event_name         STRING     COMMENT '域内事件名',
    launch_type        STRING     COMMENT '冷/热启动',
    source             STRING     COMMENT '启动来源',
    stay_duration      BIGINT     COMMENT '会话停留秒数',
    exit_type          STRING     COMMENT '退出方式',
    properties_raw     STRING     COMMENT '事件私有属性 JSON 原文（保真）',
    etl_time           TIMESTAMP  COMMENT '入仓时间'
)
COMMENT 'DWD-会话明细（离线）'
PARTITIONED BY (dt STRING)
STORED AS ORC
LOCATION 'hdfs://WWC160:9820/warehouse/music_dw/dwd_session'
TBLPROPERTIES ('orc.compress' = 'SNAPPY');

INSERT OVERWRITE TABLE music_dw.dwd_session PARTITION (dt)
SELECT
    event_id,
    from_utc_timestamp(CAST(from_unixtime(CAST(event_time AS BIGINT) DIV 1000) AS TIMESTAMP), 'Asia/Shanghai')  AS event_ts,
    from_utc_timestamp(CAST(from_unixtime(CAST(server_time AS BIGINT) DIV 1000) AS TIMESTAMP), 'Asia/Shanghai') AS server_ts,
    user_id, device_id, is_login, is_vip, app_version, os, os_version,
    device_model, network, carrier, ip, province, city,
    session_id, session_seq, is_foreground, channel, page_id,
    event_name,
    get_json_object(properties, '$.launch_type') AS launch_type,
    get_json_object(properties, '$.source') AS source,
    CAST(get_json_object(properties, '$.stay_duration') AS BIGINT) AS stay_duration,
    get_json_object(properties, '$.exit_type') AS exit_type,
    properties                              AS properties_raw,
    CAST(etl_time AS TIMESTAMP)             AS etl_time,
    dt
FROM music_dw.ods_music_event
WHERE dt BETWEEN '${start_dt}' AND '${end_dt}'
  AND event_name IN ('app_launch', 'app_exit')
DISTRIBUTE BY dt;


-- dwd_impression: 曝光明细
-- 事件: impression
-- [11-08 修正] 不再无条件 DROP：只补少数几天时会把历史分区一起清掉
-- DROP TABLE IF EXISTS music_dw.dwd_impression;
CREATE EXTERNAL TABLE IF NOT EXISTS music_dw.dwd_impression (
    event_id           STRING     COMMENT '事件ID',
    event_ts           TIMESTAMP  COMMENT '事件时间',
    server_ts          TIMESTAMP  COMMENT '服务端接收时间',
    user_id            STRING     COMMENT '用户ID',
    device_id          STRING     COMMENT '设备ID',
    is_login           STRING     COMMENT '是否登录',
    is_vip             STRING     COMMENT '是否会员',
    app_version        STRING     COMMENT 'APP版本',
    os                 STRING     COMMENT '操作系统',
    os_version         STRING     COMMENT '系统版本',
    device_model       STRING     COMMENT '设备型号',
    network            STRING     COMMENT '网络类型',
    carrier            STRING     COMMENT '运营商',
    ip                 STRING     COMMENT '客户端IP',
    province           STRING     COMMENT '省份',
    city               STRING     COMMENT '城市',
    session_id         STRING     COMMENT '会话ID',
    session_seq        STRING     COMMENT '会话内序号',
    is_foreground      STRING     COMMENT '前后台标记',
    channel            STRING     COMMENT '渠道',
    page_id            STRING     COMMENT '页面ID',
    event_name         STRING     COMMENT '域内事件名',
    song_id            STRING     COMMENT '曝光歌曲',
    position           INT        COMMENT '坑位 0~24',
    scene              STRING     COMMENT '曝光场景',
    is_click           INT        COMMENT '是否点击（CTR 分子标记）',
    properties_raw     STRING     COMMENT '事件私有属性 JSON 原文（保真）',
    etl_time           TIMESTAMP  COMMENT '入仓时间'
)
COMMENT 'DWD-曝光明细（离线）'
PARTITIONED BY (dt STRING)
STORED AS ORC
LOCATION 'hdfs://WWC160:9820/warehouse/music_dw/dwd_impression'
TBLPROPERTIES ('orc.compress' = 'SNAPPY');

INSERT OVERWRITE TABLE music_dw.dwd_impression PARTITION (dt)
SELECT
    event_id,
    from_utc_timestamp(CAST(from_unixtime(CAST(event_time AS BIGINT) DIV 1000) AS TIMESTAMP), 'Asia/Shanghai')  AS event_ts,
    from_utc_timestamp(CAST(from_unixtime(CAST(server_time AS BIGINT) DIV 1000) AS TIMESTAMP), 'Asia/Shanghai') AS server_ts,
    user_id, device_id, is_login, is_vip, app_version, os, os_version,
    device_model, network, carrier, ip, province, city,
    session_id, session_seq, is_foreground, channel, page_id,
    event_name,
    get_json_object(properties, '$.song_id') AS song_id,
    CAST(get_json_object(properties, '$.position') AS INT) AS position,
    get_json_object(properties, '$.scene') AS scene,
    CAST(get_json_object(properties, '$.is_click') AS INT) AS is_click,
    properties                              AS properties_raw,
    CAST(etl_time AS TIMESTAMP)             AS etl_time,
    dt
FROM music_dw.ods_music_event
WHERE dt BETWEEN '${start_dt}' AND '${end_dt}'
  AND event_name IN ('impression')
DISTRIBUTE BY dt;


-- dwd_click: 点击明细
-- 事件: click
-- [11-08 修正] 不再无条件 DROP：只补少数几天时会把历史分区一起清掉
-- DROP TABLE IF EXISTS music_dw.dwd_click;
CREATE EXTERNAL TABLE IF NOT EXISTS music_dw.dwd_click (
    event_id           STRING     COMMENT '事件ID',
    event_ts           TIMESTAMP  COMMENT '事件时间',
    server_ts          TIMESTAMP  COMMENT '服务端接收时间',
    user_id            STRING     COMMENT '用户ID',
    device_id          STRING     COMMENT '设备ID',
    is_login           STRING     COMMENT '是否登录',
    is_vip             STRING     COMMENT '是否会员',
    app_version        STRING     COMMENT 'APP版本',
    os                 STRING     COMMENT '操作系统',
    os_version         STRING     COMMENT '系统版本',
    device_model       STRING     COMMENT '设备型号',
    network            STRING     COMMENT '网络类型',
    carrier            STRING     COMMENT '运营商',
    ip                 STRING     COMMENT '客户端IP',
    province           STRING     COMMENT '省份',
    city               STRING     COMMENT '城市',
    session_id         STRING     COMMENT '会话ID',
    session_seq        STRING     COMMENT '会话内序号',
    is_foreground      STRING     COMMENT '前后台标记',
    channel            STRING     COMMENT '渠道',
    page_id            STRING     COMMENT '页面ID',
    event_name         STRING     COMMENT '域内事件名',
    song_id            STRING     COMMENT '点击歌曲',
    target_type        STRING     COMMENT '目标类型',
    target_id          STRING     COMMENT '目标ID',
    position           INT        COMMENT '点击坑位',
    page               STRING     COMMENT '所在页面',
    is_click           INT        COMMENT '点击标记',
    properties_raw     STRING     COMMENT '事件私有属性 JSON 原文（保真）',
    etl_time           TIMESTAMP  COMMENT '入仓时间'
)
COMMENT 'DWD-点击明细（离线）'
PARTITIONED BY (dt STRING)
STORED AS ORC
LOCATION 'hdfs://WWC160:9820/warehouse/music_dw/dwd_click'
TBLPROPERTIES ('orc.compress' = 'SNAPPY');

INSERT OVERWRITE TABLE music_dw.dwd_click PARTITION (dt)
SELECT
    event_id,
    from_utc_timestamp(CAST(from_unixtime(CAST(event_time AS BIGINT) DIV 1000) AS TIMESTAMP), 'Asia/Shanghai')  AS event_ts,
    from_utc_timestamp(CAST(from_unixtime(CAST(server_time AS BIGINT) DIV 1000) AS TIMESTAMP), 'Asia/Shanghai') AS server_ts,
    user_id, device_id, is_login, is_vip, app_version, os, os_version,
    device_model, network, carrier, ip, province, city,
    session_id, session_seq, is_foreground, channel, page_id,
    event_name,
    get_json_object(properties, '$.song_id') AS song_id,
    get_json_object(properties, '$.target_type') AS target_type,
    get_json_object(properties, '$.target_id') AS target_id,
    CAST(get_json_object(properties, '$.position') AS INT) AS position,
    get_json_object(properties, '$.page') AS page,
    CAST(get_json_object(properties, '$.is_click') AS INT) AS is_click,
    properties                              AS properties_raw,
    CAST(etl_time AS TIMESTAMP)             AS etl_time,
    dt
FROM music_dw.ods_music_event
WHERE dt BETWEEN '${start_dt}' AND '${end_dt}'
  AND event_name IN ('click')
DISTRIBUTE BY dt;


-- dwd_play_control: 播放控制明细
-- 事件: pause, resume, seek, play_error
-- [11-08 修正] 不再无条件 DROP：只补少数几天时会把历史分区一起清掉
-- DROP TABLE IF EXISTS music_dw.dwd_play_control;
CREATE EXTERNAL TABLE IF NOT EXISTS music_dw.dwd_play_control (
    event_id           STRING     COMMENT '事件ID',
    event_ts           TIMESTAMP  COMMENT '事件时间',
    server_ts          TIMESTAMP  COMMENT '服务端接收时间',
    user_id            STRING     COMMENT '用户ID',
    device_id          STRING     COMMENT '设备ID',
    is_login           STRING     COMMENT '是否登录',
    is_vip             STRING     COMMENT '是否会员',
    app_version        STRING     COMMENT 'APP版本',
    os                 STRING     COMMENT '操作系统',
    os_version         STRING     COMMENT '系统版本',
    device_model       STRING     COMMENT '设备型号',
    network            STRING     COMMENT '网络类型',
    carrier            STRING     COMMENT '运营商',
    ip                 STRING     COMMENT '客户端IP',
    province           STRING     COMMENT '省份',
    city               STRING     COMMENT '城市',
    session_id         STRING     COMMENT '会话ID',
    session_seq        STRING     COMMENT '会话内序号',
    is_foreground      STRING     COMMENT '前后台标记',
    channel            STRING     COMMENT '渠道',
    page_id            STRING     COMMENT '页面ID',
    event_name         STRING     COMMENT '域内事件名',
    song_id            STRING     COMMENT '歌曲',
    position           INT        COMMENT '操作位置（秒）',
    from_pos           INT        COMMENT '拖动起点（seek）',
    to_pos             INT        COMMENT '拖动终点（seek）',
    direction          STRING     COMMENT '拖动方向',
    pause_duration     INT        COMMENT '暂停时长',
    error_type         STRING     COMMENT '播放错误类型',
    properties_raw     STRING     COMMENT '事件私有属性 JSON 原文（保真）',
    etl_time           TIMESTAMP  COMMENT '入仓时间'
)
COMMENT 'DWD-播放控制明细（离线）'
PARTITIONED BY (dt STRING)
STORED AS ORC
LOCATION 'hdfs://WWC160:9820/warehouse/music_dw/dwd_play_control'
TBLPROPERTIES ('orc.compress' = 'SNAPPY');

INSERT OVERWRITE TABLE music_dw.dwd_play_control PARTITION (dt)
SELECT
    event_id,
    from_utc_timestamp(CAST(from_unixtime(CAST(event_time AS BIGINT) DIV 1000) AS TIMESTAMP), 'Asia/Shanghai')  AS event_ts,
    from_utc_timestamp(CAST(from_unixtime(CAST(server_time AS BIGINT) DIV 1000) AS TIMESTAMP), 'Asia/Shanghai') AS server_ts,
    user_id, device_id, is_login, is_vip, app_version, os, os_version,
    device_model, network, carrier, ip, province, city,
    session_id, session_seq, is_foreground, channel, page_id,
    event_name,
    get_json_object(properties, '$.song_id') AS song_id,
    CAST(get_json_object(properties, '$.position') AS INT) AS position,
    CAST(get_json_object(properties, '$.from') AS INT) AS from_pos,
    CAST(get_json_object(properties, '$.to') AS INT) AS to_pos,
    get_json_object(properties, '$.direction') AS direction,
    CAST(get_json_object(properties, '$.pause_duration') AS INT) AS pause_duration,
    get_json_object(properties, '$.error_type') AS error_type,
    properties                              AS properties_raw,
    CAST(etl_time AS TIMESTAMP)             AS etl_time,
    dt
FROM music_dw.ods_music_event
WHERE dt BETWEEN '${start_dt}' AND '${end_dt}'
  AND event_name IN ('pause', 'resume', 'seek', 'play_error')
DISTRIBUTE BY dt;


-- dwd_interaction: 互动域统一明细
-- 事件: collect, like, share, comment, download, follow_artist, playlist_add
-- [11-08 修正] 不再无条件 DROP：只补少数几天时会把历史分区一起清掉
-- DROP TABLE IF EXISTS music_dw.dwd_interaction;
CREATE EXTERNAL TABLE IF NOT EXISTS music_dw.dwd_interaction (
    event_id           STRING     COMMENT '事件ID',
    event_ts           TIMESTAMP  COMMENT '事件时间',
    server_ts          TIMESTAMP  COMMENT '服务端接收时间',
    user_id            STRING     COMMENT '用户ID',
    device_id          STRING     COMMENT '设备ID',
    is_login           STRING     COMMENT '是否登录',
    is_vip             STRING     COMMENT '是否会员',
    app_version        STRING     COMMENT 'APP版本',
    os                 STRING     COMMENT '操作系统',
    os_version         STRING     COMMENT '系统版本',
    device_model       STRING     COMMENT '设备型号',
    network            STRING     COMMENT '网络类型',
    carrier            STRING     COMMENT '运营商',
    ip                 STRING     COMMENT '客户端IP',
    province           STRING     COMMENT '省份',
    city               STRING     COMMENT '城市',
    session_id         STRING     COMMENT '会话ID',
    session_seq        STRING     COMMENT '会话内序号',
    is_foreground      STRING     COMMENT '前后台标记',
    channel            STRING     COMMENT '渠道',
    page_id            STRING     COMMENT '页面ID',
    event_name         STRING     COMMENT '域内事件名',
    song_id            STRING     COMMENT '歌曲',
    action             STRING     COMMENT 'add/remove 等动作',
    share_channel      STRING     COMMENT '分享渠道',
    comment_len        INT        COMMENT '评论长度',
    quality            STRING     COMMENT '下载音质',
    artist_id          STRING     COMMENT '歌手ID',
    playlist_id        STRING     COMMENT '歌单ID',
    properties_raw     STRING     COMMENT '事件私有属性 JSON 原文（保真）',
    etl_time           TIMESTAMP  COMMENT '入仓时间'
)
COMMENT 'DWD-互动域统一明细（离线）'
PARTITIONED BY (dt STRING)
STORED AS ORC
LOCATION 'hdfs://WWC160:9820/warehouse/music_dw/dwd_interaction'
TBLPROPERTIES ('orc.compress' = 'SNAPPY');

INSERT OVERWRITE TABLE music_dw.dwd_interaction PARTITION (dt)
SELECT
    event_id,
    from_utc_timestamp(CAST(from_unixtime(CAST(event_time AS BIGINT) DIV 1000) AS TIMESTAMP), 'Asia/Shanghai')  AS event_ts,
    from_utc_timestamp(CAST(from_unixtime(CAST(server_time AS BIGINT) DIV 1000) AS TIMESTAMP), 'Asia/Shanghai') AS server_ts,
    user_id, device_id, is_login, is_vip, app_version, os, os_version,
    device_model, network, carrier, ip, province, city,
    session_id, session_seq, is_foreground, channel, page_id,
    event_name,
    get_json_object(properties, '$.song_id') AS song_id,
    get_json_object(properties, '$.action') AS action,
    get_json_object(properties, '$.channel') AS share_channel,
    CAST(get_json_object(properties, '$.comment_len') AS INT) AS comment_len,
    get_json_object(properties, '$.quality') AS quality,
    get_json_object(properties, '$.artist_id') AS artist_id,
    get_json_object(properties, '$.playlist_id') AS playlist_id,
    properties                              AS properties_raw,
    CAST(etl_time AS TIMESTAMP)             AS etl_time,
    dt
FROM music_dw.ods_music_event
WHERE dt BETWEEN '${start_dt}' AND '${end_dt}'
  AND event_name IN ('collect', 'like', 'share', 'comment', 'download', 'follow_artist', 'playlist_add')
DISTRIBUTE BY dt;


-- dwd_search: 搜索域明细
-- 事件: search, search_click
-- [11-08 修正] 不再无条件 DROP：只补少数几天时会把历史分区一起清掉
-- DROP TABLE IF EXISTS music_dw.dwd_search;
CREATE EXTERNAL TABLE IF NOT EXISTS music_dw.dwd_search (
    event_id           STRING     COMMENT '事件ID',
    event_ts           TIMESTAMP  COMMENT '事件时间',
    server_ts          TIMESTAMP  COMMENT '服务端接收时间',
    user_id            STRING     COMMENT '用户ID',
    device_id          STRING     COMMENT '设备ID',
    is_login           STRING     COMMENT '是否登录',
    is_vip             STRING     COMMENT '是否会员',
    app_version        STRING     COMMENT 'APP版本',
    os                 STRING     COMMENT '操作系统',
    os_version         STRING     COMMENT '系统版本',
    device_model       STRING     COMMENT '设备型号',
    network            STRING     COMMENT '网络类型',
    carrier            STRING     COMMENT '运营商',
    ip                 STRING     COMMENT '客户端IP',
    province           STRING     COMMENT '省份',
    city               STRING     COMMENT '城市',
    session_id         STRING     COMMENT '会话ID',
    session_seq        STRING     COMMENT '会话内序号',
    is_foreground      STRING     COMMENT '前后台标记',
    channel            STRING     COMMENT '渠道',
    page_id            STRING     COMMENT '页面ID',
    event_name         STRING     COMMENT '域内事件名',
    keyword            STRING     COMMENT '搜索词',
    search_type        STRING     COMMENT 'hot/suggest/input',
    result_cnt         INT        COMMENT '结果数（零结果率）',
    result_type        STRING     COMMENT '结果类型',
    result_id          STRING     COMMENT '结果ID',
    position           INT        COMMENT '结果位置',
    properties_raw     STRING     COMMENT '事件私有属性 JSON 原文（保真）',
    etl_time           TIMESTAMP  COMMENT '入仓时间'
)
COMMENT 'DWD-搜索域明细（离线）'
PARTITIONED BY (dt STRING)
STORED AS ORC
LOCATION 'hdfs://WWC160:9820/warehouse/music_dw/dwd_search'
TBLPROPERTIES ('orc.compress' = 'SNAPPY');

INSERT OVERWRITE TABLE music_dw.dwd_search PARTITION (dt)
SELECT
    event_id,
    from_utc_timestamp(CAST(from_unixtime(CAST(event_time AS BIGINT) DIV 1000) AS TIMESTAMP), 'Asia/Shanghai')  AS event_ts,
    from_utc_timestamp(CAST(from_unixtime(CAST(server_time AS BIGINT) DIV 1000) AS TIMESTAMP), 'Asia/Shanghai') AS server_ts,
    user_id, device_id, is_login, is_vip, app_version, os, os_version,
    device_model, network, carrier, ip, province, city,
    session_id, session_seq, is_foreground, channel, page_id,
    event_name,
    get_json_object(properties, '$.keyword') AS keyword,
    get_json_object(properties, '$.search_type') AS search_type,
    CAST(get_json_object(properties, '$.result_cnt') AS INT) AS result_cnt,
    get_json_object(properties, '$.result_type') AS result_type,
    get_json_object(properties, '$.result_id') AS result_id,
    CAST(get_json_object(properties, '$.position') AS INT) AS position,
    properties                              AS properties_raw,
    CAST(etl_time AS TIMESTAMP)             AS etl_time,
    dt
FROM music_dw.ods_music_event
WHERE dt BETWEEN '${start_dt}' AND '${end_dt}'
  AND event_name IN ('search', 'search_click')
DISTRIBUTE BY dt;


-- dwd_ad: 广告域明细
-- 事件: ad_impression, ad_click
-- [11-08 修正] 不再无条件 DROP：只补少数几天时会把历史分区一起清掉
-- DROP TABLE IF EXISTS music_dw.dwd_ad;
CREATE EXTERNAL TABLE IF NOT EXISTS music_dw.dwd_ad (
    event_id           STRING     COMMENT '事件ID',
    event_ts           TIMESTAMP  COMMENT '事件时间',
    server_ts          TIMESTAMP  COMMENT '服务端接收时间',
    user_id            STRING     COMMENT '用户ID',
    device_id          STRING     COMMENT '设备ID',
    is_login           STRING     COMMENT '是否登录',
    is_vip             STRING     COMMENT '是否会员',
    app_version        STRING     COMMENT 'APP版本',
    os                 STRING     COMMENT '操作系统',
    os_version         STRING     COMMENT '系统版本',
    device_model       STRING     COMMENT '设备型号',
    network            STRING     COMMENT '网络类型',
    carrier            STRING     COMMENT '运营商',
    ip                 STRING     COMMENT '客户端IP',
    province           STRING     COMMENT '省份',
    city               STRING     COMMENT '城市',
    session_id         STRING     COMMENT '会话ID',
    session_seq        STRING     COMMENT '会话内序号',
    is_foreground      STRING     COMMENT '前后台标记',
    channel            STRING     COMMENT '渠道',
    page_id            STRING     COMMENT '页面ID',
    event_name         STRING     COMMENT '域内事件名',
    ad_id              STRING     COMMENT '广告ID',
    ad_type            STRING     COMMENT '广告形式',
    position           INT        COMMENT '广告位',
    bid_price          DECIMAL(18,2) COMMENT '出价（eCPM 估算，金额口径 DECIMAL）',
    properties_raw     STRING     COMMENT '事件私有属性 JSON 原文（保真）',
    etl_time           TIMESTAMP  COMMENT '入仓时间'
)
COMMENT 'DWD-广告域明细（离线）'
PARTITIONED BY (dt STRING)
STORED AS ORC
LOCATION 'hdfs://WWC160:9820/warehouse/music_dw/dwd_ad'
TBLPROPERTIES ('orc.compress' = 'SNAPPY');

INSERT OVERWRITE TABLE music_dw.dwd_ad PARTITION (dt)
SELECT
    event_id,
    from_utc_timestamp(CAST(from_unixtime(CAST(event_time AS BIGINT) DIV 1000) AS TIMESTAMP), 'Asia/Shanghai')  AS event_ts,
    from_utc_timestamp(CAST(from_unixtime(CAST(server_time AS BIGINT) DIV 1000) AS TIMESTAMP), 'Asia/Shanghai') AS server_ts,
    user_id, device_id, is_login, is_vip, app_version, os, os_version,
    device_model, network, carrier, ip, province, city,
    session_id, session_seq, is_foreground, channel, page_id,
    event_name,
    get_json_object(properties, '$.ad_id') AS ad_id,
    get_json_object(properties, '$.ad_type') AS ad_type,
    CAST(get_json_object(properties, '$.position') AS INT) AS position,
    CAST(get_json_object(properties, '$.bid_price') AS DECIMAL(18,2)) AS bid_price,
    properties                              AS properties_raw,
    CAST(etl_time AS TIMESTAMP)             AS etl_time,
    dt
FROM music_dw.ods_music_event
WHERE dt BETWEEN '${start_dt}' AND '${end_dt}'
  AND event_name IN ('ad_impression', 'ad_click')
DISTRIBUTE BY dt;


-- dwd_pay: 商业化域明细
-- 事件: vip_subscribe, order_pay, order_refund
-- [11-08 修正] 不再无条件 DROP：只补少数几天时会把历史分区一起清掉
-- DROP TABLE IF EXISTS music_dw.dwd_pay;
CREATE EXTERNAL TABLE IF NOT EXISTS music_dw.dwd_pay (
    event_id           STRING     COMMENT '事件ID',
    event_ts           TIMESTAMP  COMMENT '事件时间',
    server_ts          TIMESTAMP  COMMENT '服务端接收时间',
    user_id            STRING     COMMENT '用户ID',
    device_id          STRING     COMMENT '设备ID',
    is_login           STRING     COMMENT '是否登录',
    is_vip             STRING     COMMENT '是否会员',
    app_version        STRING     COMMENT 'APP版本',
    os                 STRING     COMMENT '操作系统',
    os_version         STRING     COMMENT '系统版本',
    device_model       STRING     COMMENT '设备型号',
    network            STRING     COMMENT '网络类型',
    carrier            STRING     COMMENT '运营商',
    ip                 STRING     COMMENT '客户端IP',
    province           STRING     COMMENT '省份',
    city               STRING     COMMENT '城市',
    session_id         STRING     COMMENT '会话ID',
    session_seq        STRING     COMMENT '会话内序号',
    is_foreground      STRING     COMMENT '前后台标记',
    channel            STRING     COMMENT '渠道',
    page_id            STRING     COMMENT '页面ID',
    event_name         STRING     COMMENT '域内事件名',
    sku_id             STRING     COMMENT '商品SKU',
    amount             DECIMAL(18,2) COMMENT '金额（DECIMAL，docs/20 §1.3）',
    pay_channel        STRING     COMMENT '支付渠道',
    order_id           STRING     COMMENT '订单号',
    reason             STRING     COMMENT '退款原因',
    properties_raw     STRING     COMMENT '事件私有属性 JSON 原文（保真）',
    etl_time           TIMESTAMP  COMMENT '入仓时间'
)
COMMENT 'DWD-商业化域明细（离线）'
PARTITIONED BY (dt STRING)
STORED AS ORC
LOCATION 'hdfs://WWC160:9820/warehouse/music_dw/dwd_pay'
TBLPROPERTIES ('orc.compress' = 'SNAPPY');

INSERT OVERWRITE TABLE music_dw.dwd_pay PARTITION (dt)
SELECT
    event_id,
    from_utc_timestamp(CAST(from_unixtime(CAST(event_time AS BIGINT) DIV 1000) AS TIMESTAMP), 'Asia/Shanghai')  AS event_ts,
    from_utc_timestamp(CAST(from_unixtime(CAST(server_time AS BIGINT) DIV 1000) AS TIMESTAMP), 'Asia/Shanghai') AS server_ts,
    user_id, device_id, is_login, is_vip, app_version, os, os_version,
    device_model, network, carrier, ip, province, city,
    session_id, session_seq, is_foreground, channel, page_id,
    event_name,
    get_json_object(properties, '$.sku_id') AS sku_id,
    CAST(get_json_object(properties, '$.amount') AS DECIMAL(18,2)) AS amount,
    get_json_object(properties, '$.pay_channel') AS pay_channel,
    get_json_object(properties, '$.order_id') AS order_id,
    get_json_object(properties, '$.reason') AS reason,
    properties                              AS properties_raw,
    CAST(etl_time AS TIMESTAMP)             AS etl_time,
    dt
FROM music_dw.ods_music_event
WHERE dt BETWEEN '${start_dt}' AND '${end_dt}'
  AND event_name IN ('vip_subscribe', 'order_pay', 'order_refund')
DISTRIBUTE BY dt;


-- dwd_page_view: 页面浏览明细
-- 事件: page_view
-- [11-08 修正] 不再无条件 DROP：只补少数几天时会把历史分区一起清掉
-- DROP TABLE IF EXISTS music_dw.dwd_page_view;
CREATE EXTERNAL TABLE IF NOT EXISTS music_dw.dwd_page_view (
    event_id           STRING     COMMENT '事件ID',
    event_ts           TIMESTAMP  COMMENT '事件时间',
    server_ts          TIMESTAMP  COMMENT '服务端接收时间',
    user_id            STRING     COMMENT '用户ID',
    device_id          STRING     COMMENT '设备ID',
    is_login           STRING     COMMENT '是否登录',
    is_vip             STRING     COMMENT '是否会员',
    app_version        STRING     COMMENT 'APP版本',
    os                 STRING     COMMENT '操作系统',
    os_version         STRING     COMMENT '系统版本',
    device_model       STRING     COMMENT '设备型号',
    network            STRING     COMMENT '网络类型',
    carrier            STRING     COMMENT '运营商',
    ip                 STRING     COMMENT '客户端IP',
    province           STRING     COMMENT '省份',
    city               STRING     COMMENT '城市',
    session_id         STRING     COMMENT '会话ID',
    session_seq        STRING     COMMENT '会话内序号',
    is_foreground      STRING     COMMENT '前后台标记',
    channel            STRING     COMMENT '渠道',
    page_id            STRING     COMMENT '页面ID',
    event_name         STRING     COMMENT '域内事件名',
    page               STRING     COMMENT '页面',
    from_page          STRING     COMMENT '来源页面',
    stay_duration      INT        COMMENT '停留秒数',
    properties_raw     STRING     COMMENT '事件私有属性 JSON 原文（保真）',
    etl_time           TIMESTAMP  COMMENT '入仓时间'
)
COMMENT 'DWD-页面浏览明细（离线）'
PARTITIONED BY (dt STRING)
STORED AS ORC
LOCATION 'hdfs://WWC160:9820/warehouse/music_dw/dwd_page_view'
TBLPROPERTIES ('orc.compress' = 'SNAPPY');

INSERT OVERWRITE TABLE music_dw.dwd_page_view PARTITION (dt)
SELECT
    event_id,
    from_utc_timestamp(CAST(from_unixtime(CAST(event_time AS BIGINT) DIV 1000) AS TIMESTAMP), 'Asia/Shanghai')  AS event_ts,
    from_utc_timestamp(CAST(from_unixtime(CAST(server_time AS BIGINT) DIV 1000) AS TIMESTAMP), 'Asia/Shanghai') AS server_ts,
    user_id, device_id, is_login, is_vip, app_version, os, os_version,
    device_model, network, carrier, ip, province, city,
    session_id, session_seq, is_foreground, channel, page_id,
    event_name,
    get_json_object(properties, '$.page') AS page,
    get_json_object(properties, '$.from_page') AS from_page,
    CAST(get_json_object(properties, '$.stay_duration') AS INT) AS stay_duration,
    properties                              AS properties_raw,
    CAST(etl_time AS TIMESTAMP)             AS etl_time,
    dt
FROM music_dw.ods_music_event
WHERE dt BETWEEN '${start_dt}' AND '${end_dt}'
  AND event_name IN ('page_view')
DISTRIBUTE BY dt;


-- ===========================================================================
-- 合并收口：collect / share 已并入 dwd_interaction，删掉两张旧表
-- 下游 05/08/09 未引用它们（已 grep 确认），外部表 DROP 只删元数据不删 HDFS。
-- 07 号脚本自此不再进 DAG（它是历史脚本，保留但不执行）。
-- ===========================================================================
DROP TABLE IF EXISTS music_dw.dwd_collect;
DROP TABLE IF EXISTS music_dw.dwd_share;

-- 验收：10 张新表逐表行数

SELECT 'dwd_skip' AS tbl, count(*) AS cnt FROM music_dw.dwd_skip;
SELECT 'dwd_session' AS tbl, count(*) AS cnt FROM music_dw.dwd_session;
SELECT 'dwd_impression' AS tbl, count(*) AS cnt FROM music_dw.dwd_impression;
SELECT 'dwd_click' AS tbl, count(*) AS cnt FROM music_dw.dwd_click;
SELECT 'dwd_play_control' AS tbl, count(*) AS cnt FROM music_dw.dwd_play_control;
SELECT 'dwd_interaction' AS tbl, count(*) AS cnt FROM music_dw.dwd_interaction;
SELECT 'dwd_search' AS tbl, count(*) AS cnt FROM music_dw.dwd_search;
SELECT 'dwd_ad' AS tbl, count(*) AS cnt FROM music_dw.dwd_ad;
SELECT 'dwd_pay' AS tbl, count(*) AS cnt FROM music_dw.dwd_pay;
SELECT 'dwd_page_view' AS tbl, count(*) AS cnt FROM music_dw.dwd_page_view;

SHOW TABLES IN music_dw LIKE 'dwd_*';
