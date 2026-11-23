-- 自动生成：DWD 明细表建表语句（6 张，dwd_play_end 见 doris_dwd_tables.sql）
-- 生成器：code/gen_dwd_sql.py
USE music_dw;


-- 播放开始明细
CREATE TABLE IF NOT EXISTS dwd_play_start (
    event_id        VARCHAR(64)  NOT NULL COMMENT '事件ID',
    event_ts        DATETIME     NOT NULL COMMENT '事件时间',
    server_ts       DATETIME     NULL     COMMENT '服务端接收时间',
    user_id         VARCHAR(32)  NULL,
    device_id       VARCHAR(32)  NULL,
    is_login        VARCHAR(8)   NULL,
    is_vip          VARCHAR(8)   NULL,
    app_version     VARCHAR(16)  NULL,
    os              VARCHAR(16)  NULL,
    os_version      VARCHAR(16)  NULL,
    device_model    VARCHAR(64)  NULL,
    network         VARCHAR(16)  NULL,
    carrier         VARCHAR(32)  NULL,
    ip              VARCHAR(64)  NULL,
    province        VARCHAR(32)  NULL,
    city            VARCHAR(32)  NULL,
    session_id      VARCHAR(64)  NULL,
    session_seq     VARCHAR(16)  NULL,
    is_foreground   VARCHAR(8)   NULL,
    channel         VARCHAR(32)  NULL,
    page_id         VARCHAR(32)  NULL,
    song_id         VARCHAR(32)  NULL     COMMENT '$.song_id',
    playlist_id     VARCHAR(32)  NULL     COMMENT '$.playlist_id',
    quality         VARCHAR(16)  NULL     COMMENT '$.quality',
    play_mode       VARCHAR(16)  NULL     COMMENT '$.play_mode',
    song_index      INT          NULL     COMMENT '$.song_index',
    etl_time        DATETIME     NULL     COMMENT '入仓时间',
    dt              DATE         NOT NULL COMMENT '事件日期，分区键'
) ENGINE=OLAP
DUPLICATE KEY(event_id, event_ts)
COMMENT 'DWD-播放开始明细'
PARTITION BY RANGE(dt) ()
DISTRIBUTED BY HASH(event_id) BUCKETS 8
PROPERTIES (
    "replication_allocation" = "tag.location.default: 1",
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "DAY",
    "dynamic_partition.start" = "-365",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.buckets" = "8"
);


-- 切歌明细
CREATE TABLE IF NOT EXISTS dwd_skip (
    event_id        VARCHAR(64)  NOT NULL COMMENT '事件ID',
    event_ts        DATETIME     NOT NULL COMMENT '事件时间',
    server_ts       DATETIME     NULL     COMMENT '服务端接收时间',
    user_id         VARCHAR(32)  NULL,
    device_id       VARCHAR(32)  NULL,
    is_login        VARCHAR(8)   NULL,
    is_vip          VARCHAR(8)   NULL,
    app_version     VARCHAR(16)  NULL,
    os              VARCHAR(16)  NULL,
    os_version      VARCHAR(16)  NULL,
    device_model    VARCHAR(64)  NULL,
    network         VARCHAR(16)  NULL,
    carrier         VARCHAR(32)  NULL,
    ip              VARCHAR(64)  NULL,
    province        VARCHAR(32)  NULL,
    city            VARCHAR(32)  NULL,
    session_id      VARCHAR(64)  NULL,
    session_seq     VARCHAR(16)  NULL,
    is_foreground   VARCHAR(8)   NULL,
    channel         VARCHAR(32)  NULL,
    page_id         VARCHAR(32)  NULL,
    song_id         VARCHAR(32)  NULL     COMMENT '$.song_id',
    played_duration INT          NULL     COMMENT '$.played_duration',
    skip_type       VARCHAR(16)  NULL     COMMENT '$.skip_type',
    etl_time        DATETIME     NULL     COMMENT '入仓时间',
    dt              DATE         NOT NULL COMMENT '事件日期，分区键'
) ENGINE=OLAP
DUPLICATE KEY(event_id, event_ts)
COMMENT 'DWD-切歌明细'
PARTITION BY RANGE(dt) ()
DISTRIBUTED BY HASH(event_id) BUCKETS 8
PROPERTIES (
    "replication_allocation" = "tag.location.default: 1",
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "DAY",
    "dynamic_partition.start" = "-365",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.buckets" = "8"
);


-- 搜索明细
CREATE TABLE IF NOT EXISTS dwd_search (
    event_id        VARCHAR(64)  NOT NULL COMMENT '事件ID',
    event_ts        DATETIME     NOT NULL COMMENT '事件时间',
    server_ts       DATETIME     NULL     COMMENT '服务端接收时间',
    user_id         VARCHAR(32)  NULL,
    device_id       VARCHAR(32)  NULL,
    is_login        VARCHAR(8)   NULL,
    is_vip          VARCHAR(8)   NULL,
    app_version     VARCHAR(16)  NULL,
    os              VARCHAR(16)  NULL,
    os_version      VARCHAR(16)  NULL,
    device_model    VARCHAR(64)  NULL,
    network         VARCHAR(16)  NULL,
    carrier         VARCHAR(32)  NULL,
    ip              VARCHAR(64)  NULL,
    province        VARCHAR(32)  NULL,
    city            VARCHAR(32)  NULL,
    session_id      VARCHAR(64)  NULL,
    session_seq     VARCHAR(16)  NULL,
    is_foreground   VARCHAR(8)   NULL,
    channel         VARCHAR(32)  NULL,
    page_id         VARCHAR(32)  NULL,
    keyword         VARCHAR(128) NULL     COMMENT '$.keyword',
    search_type     VARCHAR(32)  NULL     COMMENT '$.search_type',
    result_count    INT          NULL     COMMENT '$.result_count',
    clicked_song_id VARCHAR(32)  NULL     COMMENT '$.clicked_song_id',
    etl_time        DATETIME     NULL     COMMENT '入仓时间',
    dt              DATE         NOT NULL COMMENT '事件日期，分区键'
) ENGINE=OLAP
DUPLICATE KEY(event_id, event_ts)
COMMENT 'DWD-搜索明细'
PARTITION BY RANGE(dt) ()
DISTRIBUTED BY HASH(event_id) BUCKETS 8
PROPERTIES (
    "replication_allocation" = "tag.location.default: 1",
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "DAY",
    "dynamic_partition.start" = "-365",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.buckets" = "8"
);


-- 收藏明细
CREATE TABLE IF NOT EXISTS dwd_collect (
    event_id        VARCHAR(64)  NOT NULL COMMENT '事件ID',
    event_ts        DATETIME     NOT NULL COMMENT '事件时间',
    server_ts       DATETIME     NULL     COMMENT '服务端接收时间',
    user_id         VARCHAR(32)  NULL,
    device_id       VARCHAR(32)  NULL,
    is_login        VARCHAR(8)   NULL,
    is_vip          VARCHAR(8)   NULL,
    app_version     VARCHAR(16)  NULL,
    os              VARCHAR(16)  NULL,
    os_version      VARCHAR(16)  NULL,
    device_model    VARCHAR(64)  NULL,
    network         VARCHAR(16)  NULL,
    carrier         VARCHAR(32)  NULL,
    ip              VARCHAR(64)  NULL,
    province        VARCHAR(32)  NULL,
    city            VARCHAR(32)  NULL,
    session_id      VARCHAR(64)  NULL,
    session_seq     VARCHAR(16)  NULL,
    is_foreground   VARCHAR(8)   NULL,
    channel         VARCHAR(32)  NULL,
    page_id         VARCHAR(32)  NULL,
    song_id         VARCHAR(32)  NULL     COMMENT '$.song_id',
    collect_type    VARCHAR(16)  NULL     COMMENT '$.collect_type',
    etl_time        DATETIME     NULL     COMMENT '入仓时间',
    dt              DATE         NOT NULL COMMENT '事件日期，分区键'
) ENGINE=OLAP
DUPLICATE KEY(event_id, event_ts)
COMMENT 'DWD-收藏明细'
PARTITION BY RANGE(dt) ()
DISTRIBUTED BY HASH(event_id) BUCKETS 8
PROPERTIES (
    "replication_allocation" = "tag.location.default: 1",
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "DAY",
    "dynamic_partition.start" = "-365",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.buckets" = "8"
);


-- 分享明细
CREATE TABLE IF NOT EXISTS dwd_share (
    event_id        VARCHAR(64)  NOT NULL COMMENT '事件ID',
    event_ts        DATETIME     NOT NULL COMMENT '事件时间',
    server_ts       DATETIME     NULL     COMMENT '服务端接收时间',
    user_id         VARCHAR(32)  NULL,
    device_id       VARCHAR(32)  NULL,
    is_login        VARCHAR(8)   NULL,
    is_vip          VARCHAR(8)   NULL,
    app_version     VARCHAR(16)  NULL,
    os              VARCHAR(16)  NULL,
    os_version      VARCHAR(16)  NULL,
    device_model    VARCHAR(64)  NULL,
    network         VARCHAR(16)  NULL,
    carrier         VARCHAR(32)  NULL,
    ip              VARCHAR(64)  NULL,
    province        VARCHAR(32)  NULL,
    city            VARCHAR(32)  NULL,
    session_id      VARCHAR(64)  NULL,
    session_seq     VARCHAR(16)  NULL,
    is_foreground   VARCHAR(8)   NULL,
    channel         VARCHAR(32)  NULL,
    page_id         VARCHAR(32)  NULL,
    song_id         VARCHAR(32)  NULL     COMMENT '$.song_id',
    share_channel   VARCHAR(32)  NULL     COMMENT '$.share_channel',
    etl_time        DATETIME     NULL     COMMENT '入仓时间',
    dt              DATE         NOT NULL COMMENT '事件日期，分区键'
) ENGINE=OLAP
DUPLICATE KEY(event_id, event_ts)
COMMENT 'DWD-分享明细'
PARTITION BY RANGE(dt) ()
DISTRIBUTED BY HASH(event_id) BUCKETS 8
PROPERTIES (
    "replication_allocation" = "tag.location.default: 1",
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "DAY",
    "dynamic_partition.start" = "-365",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.buckets" = "8"
);


-- 页面浏览明细
CREATE TABLE IF NOT EXISTS dwd_page_view (
    event_id        VARCHAR(64)  NOT NULL COMMENT '事件ID',
    event_ts        DATETIME     NOT NULL COMMENT '事件时间',
    server_ts       DATETIME     NULL     COMMENT '服务端接收时间',
    user_id         VARCHAR(32)  NULL,
    device_id       VARCHAR(32)  NULL,
    is_login        VARCHAR(8)   NULL,
    is_vip          VARCHAR(8)   NULL,
    app_version     VARCHAR(16)  NULL,
    os              VARCHAR(16)  NULL,
    os_version      VARCHAR(16)  NULL,
    device_model    VARCHAR(64)  NULL,
    network         VARCHAR(16)  NULL,
    carrier         VARCHAR(32)  NULL,
    ip              VARCHAR(64)  NULL,
    province        VARCHAR(32)  NULL,
    city            VARCHAR(32)  NULL,
    session_id      VARCHAR(64)  NULL,
    session_seq     VARCHAR(16)  NULL,
    is_foreground   VARCHAR(8)   NULL,
    channel         VARCHAR(32)  NULL,
    page_id         VARCHAR(32)  NULL,
    view_page_id    VARCHAR(32)  NULL     COMMENT '$.page_id',
    referrer_page   VARCHAR(32)  NULL     COMMENT '$.referrer_page',
    stay_duration   INT          NULL     COMMENT '$.stay_duration',
    etl_time        DATETIME     NULL     COMMENT '入仓时间',
    dt              DATE         NOT NULL COMMENT '事件日期，分区键'
) ENGINE=OLAP
DUPLICATE KEY(event_id, event_ts)
COMMENT 'DWD-页面浏览明细'
PARTITION BY RANGE(dt) ()
DISTRIBUTED BY HASH(event_id) BUCKETS 8
PROPERTIES (
    "replication_allocation" = "tag.location.default: 1",
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "DAY",
    "dynamic_partition.start" = "-365",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.buckets" = "8"
);


-- 商业化订单明细（订阅 / 单曲购买 / 退款）
-- 三类事件合成一张表：字段完全一致（订单号、SKU、金额、渠道），
-- 拆成三张只会让下游每次算收入都要 UNION，得不偿失。
CREATE TABLE IF NOT EXISTS dwd_order (
    event_id        VARCHAR(64)  NOT NULL COMMENT '事件ID',
    event_ts        DATETIME     NOT NULL COMMENT '事件时间',
    server_ts       DATETIME     NULL     COMMENT '服务端接收时间',
    user_id         VARCHAR(32)  NULL,
    device_id       VARCHAR(32)  NULL,
    is_login        VARCHAR(8)   NULL,
    is_vip          VARCHAR(8)   NULL,
    app_version     VARCHAR(16)  NULL,
    os              VARCHAR(16)  NULL,
    os_version      VARCHAR(16)  NULL,
    device_model    VARCHAR(64)  NULL,
    network         VARCHAR(16)  NULL,
    carrier         VARCHAR(32)  NULL,
    ip              VARCHAR(64)  NULL,
    province        VARCHAR(32)  NULL,
    city            VARCHAR(32)  NULL,
    session_id      VARCHAR(64)  NULL,
    session_seq     VARCHAR(16)  NULL,
    is_foreground   VARCHAR(8)   NULL,
    channel         VARCHAR(32)  NULL,
    page_id         VARCHAR(32)  NULL,
    event_name      VARCHAR(32)  NULL     COMMENT 'vip_subscribe/order_pay/order_refund',
    order_id        VARCHAR(64)  NULL     COMMENT '$.order_id',
    sku_id          VARCHAR(32)  NULL     COMMENT '$.sku_id',
    amount          DOUBLE       NULL     COMMENT '$.amount 金额(元)，取自 SKU 单价',
    pay_channel     VARCHAR(32)  NULL     COMMENT '$.pay_channel',
    plan_days       INT          NULL     COMMENT '$.plan_days 订阅有效天数',
    refund_reason   VARCHAR(64)  NULL     COMMENT '$.refund_reason 仅退款事件',
    song_id         VARCHAR(32)  NULL     COMMENT '$.song_id 单曲购买时非空',
    etl_time        DATETIME     NULL     COMMENT '入仓时间',
    dt              DATE         NOT NULL COMMENT '事件日期，分区键'
) ENGINE=OLAP
DUPLICATE KEY(event_id, event_ts)
COMMENT 'DWD-商业化订单明细'
PARTITION BY RANGE(dt) ()
DISTRIBUTED BY HASH(event_id) BUCKETS 8
PROPERTIES (
    "replication_allocation" = "tag.location.default: 1",
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "DAY",
    "dynamic_partition.start" = "-365",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.buckets" = "8"
);

-- 客户端播放失败明细
CREATE TABLE IF NOT EXISTS dwd_play_error (
    event_id        VARCHAR(64)  NOT NULL COMMENT '事件ID',
    event_ts        DATETIME     NOT NULL COMMENT '事件时间',
    server_ts       DATETIME     NULL     COMMENT '服务端接收时间',
    user_id         VARCHAR(32)  NULL,
    device_id       VARCHAR(32)  NULL,
    is_login        VARCHAR(8)   NULL,
    is_vip          VARCHAR(8)   NULL,
    app_version     VARCHAR(16)  NULL,
    os              VARCHAR(16)  NULL,
    os_version      VARCHAR(16)  NULL,
    device_model    VARCHAR(64)  NULL,
    network         VARCHAR(16)  NULL,
    carrier         VARCHAR(32)  NULL,
    ip              VARCHAR(64)  NULL,
    province        VARCHAR(32)  NULL,
    city            VARCHAR(32)  NULL,
    session_id      VARCHAR(64)  NULL,
    session_seq     VARCHAR(16)  NULL,
    is_foreground   VARCHAR(8)   NULL,
    channel         VARCHAR(32)  NULL,
    page_id         VARCHAR(32)  NULL,
    song_id         VARCHAR(32)  NULL     COMMENT '$.song_id',
    error_code      VARCHAR(16)  NULL     COMMENT '$.error_code E1001..E1004',
    error_msg       VARCHAR(64)  NULL     COMMENT '$.error_msg',
    play_position   INT          NULL     COMMENT '$.play_position 失败时播放进度(秒)',
    network_at_error VARCHAR(16) NULL     COMMENT '失败时的网络类型',
    etl_time        DATETIME     NULL     COMMENT '入仓时间',
    dt              DATE         NOT NULL COMMENT '事件日期，分区键'
) ENGINE=OLAP
DUPLICATE KEY(event_id, event_ts)
COMMENT 'DWD-客户端播放失败明细'
PARTITION BY RANGE(dt) ()
DISTRIBUTED BY HASH(event_id) BUCKETS 8
PROPERTIES (
    "replication_allocation" = "tag.location.default: 1",
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "DAY",
    "dynamic_partition.start" = "-365",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.buckets" = "8"
);
