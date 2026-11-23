-- dev 空壳表（Hive）：只建结构，不灌数据、不接调度
-- 生成器：agent/guards/gen_dev_ddl3.py（本地源码切出，中文注释完好）
-- LOCATION 已全部指向 /warehouse/music_dev/
CREATE DATABASE IF NOT EXISTS music_dev;
USE music_dev;

-- ads_daily_overview -> music_dev
CREATE EXTERNAL TABLE IF NOT EXISTS music_dev.ads_daily_overview
(
    dau               BIGINT COMMENT '日活用户',
    session_cnt       BIGINT COMMENT '会话数',
    play_start_cnt    BIGINT COMMENT '起播次数',
    play_end_cnt      BIGINT COMMENT '结束次数',
    complete_cnt      BIGINT COMMENT '完播次数',
    complete_rate     DOUBLE COMMENT '完播率 = 完播 / 起播',
    incomplete_rate   DOUBLE COMMENT '未正常结束率 = 1 - 结束/起播',
    avg_progress      DOUBLE COMMENT '平均播放进度（仅结束事件）',
    avg_play_per_user DOUBLE COMMENT '人均起播次数',
    login_user_cnt    BIGINT COMMENT '登录用户',
    vip_user_cnt      BIGINT COMMENT '会员用户'
)
COMMENT 'ADS-日报概览'
PARTITIONED BY (dt STRING)
STORED AS ORC
LOCATION 'hdfs://WWC160:9820/warehouse/music_dev/ads_daily_overview'
TBLPROPERTIES ('orc.compress' = 'SNAPPY');

-- ads_funnel -> music_dev
CREATE EXTERNAL TABLE IF NOT EXISTS music_dev.ads_funnel (
    impression_cnt      BIGINT  COMMENT '曝光次数',
    click_cnt           BIGINT  COMMENT '点击次数',
    ctr                 DOUBLE  COMMENT '点击率',
    play_start_cnt      BIGINT  COMMENT '起播次数',
    click_to_play_rate  DOUBLE  COMMENT '点击→起播',
    complete_cnt        BIGINT  COMMENT '完播次数',
    complete_rate       DOUBLE  COMMENT '完播率',
    interact_cnt        BIGINT  COMMENT '互动次数',
    interact_rate       DOUBLE  COMMENT '互动率',
    pay_cnt             BIGINT  COMMENT '付费次数',
    pay_conv_rate       DOUBLE  COMMENT '付费转化率',
    pay_cnt_dod         DOUBLE  COMMENT '付费次数日环比'
)
COMMENT 'ADS-转化漏斗报表'
PARTITIONED BY (dt STRING)
STORED AS ORC
LOCATION 'hdfs://WWC160:9820/warehouse/music_dev/ads_funnel'
TBLPROPERTIES ('orc.compress' = 'SNAPPY');

-- ads_genre_dist -> music_dev
CREATE EXTERNAL TABLE IF NOT EXISTS music_dev.ads_genre_dist
(
    genre          STRING,
    play_start_cnt BIGINT,
    start_share    DOUBLE COMMENT '该曲风起播占全站比例',
    complete_rate  DOUBLE,
    user_cnt       BIGINT
)
COMMENT 'ADS-曲风分布'
PARTITIONED BY (dt STRING)
STORED AS ORC
LOCATION 'hdfs://WWC160:9820/warehouse/music_dev/ads_genre_dist'
TBLPROPERTIES ('orc.compress' = 'SNAPPY');

-- ads_province_dist -> music_dev
CREATE EXTERNAL TABLE IF NOT EXISTS music_dev.ads_province_dist
(
    province       STRING,
    dau            BIGINT,
    play_start_cnt BIGINT,
    session_cnt    BIGINT,
    dau_share      DOUBLE COMMENT '该省日活占全站比例'
)
COMMENT 'ADS-省份分布'
PARTITIONED BY (dt STRING)
STORED AS ORC
LOCATION 'hdfs://WWC160:9820/warehouse/music_dev/ads_province_dist'
TBLPROPERTIES ('orc.compress' = 'SNAPPY');

-- ads_revenue -> music_dev
CREATE EXTERNAL TABLE IF NOT EXISTS music_dev.ads_revenue (
    gross_revenue       DECIMAL(18,2) COMMENT '毛收入',
    refund_amount       DECIMAL(18,2) COMMENT '退款金额',
    net_revenue         DECIMAL(18,2) COMMENT '净收入',
    ad_revenue          DECIMAL(18,2) COMMENT '广告收入（估算）',
    pay_user_cnt        BIGINT        COMMENT '付费用户数',
    arpu                DECIMAL(18,2) COMMENT '每付费用户平均收入',
    refund_rate         DOUBLE        COMMENT '退款率（比率）',
    ad_ctr              DOUBLE        COMMENT '广告点击率（比率）',
    net_revenue_dod     DOUBLE        COMMENT '净收入日环比（比率）',
    cum_net_revenue     DECIMAL(18,2) COMMENT '周期内累计净收入'
)
COMMENT 'ADS-收入报表'
PARTITIONED BY (dt STRING)
STORED AS ORC
LOCATION 'hdfs://WWC160:9820/warehouse/music_dev/ads_revenue'
TBLPROPERTIES ('orc.compress' = 'SNAPPY');

-- ads_song_ranking -> music_dev
CREATE EXTERNAL TABLE IF NOT EXISTS music_dev.ads_song_ranking
(
    rank_no        INT,
    song_id        STRING,
    song_name      STRING,
    singer         STRING,
    genre          STRING,
    play_start_cnt BIGINT,
    complete_cnt   BIGINT,
    complete_rate  DOUBLE,
    avg_progress   DOUBLE
)
COMMENT 'ADS-歌曲榜 Top20'
PARTITIONED BY (dt STRING)
STORED AS ORC
LOCATION 'hdfs://WWC160:9820/warehouse/music_dev/ads_song_ranking'
TBLPROPERTIES ('orc.compress' = 'SNAPPY');

-- ads_user_retention -> music_dev
CREATE EXTERNAL TABLE IF NOT EXISTS music_dev.ads_user_retention
(
    cohort_date    STRING COMMENT '同期群日期（首次活跃日）',
    day_n          INT    COMMENT '第 N 天（1~7）',
    cohort_size    BIGINT COMMENT '该同期群总人数',
    retained_users BIGINT COMMENT '第 N 天回访人数',
    retention_rate DOUBLE COMMENT '留存率 = retained / cohort_size'
)
COMMENT 'ADS-用户留存（同期群）'
STORED AS ORC
LOCATION 'hdfs://WWC160:9820/warehouse/music_dev/ads_user_retention'
TBLPROPERTIES ('orc.compress' = 'SNAPPY');

-- dim_date -> music_dev
CREATE EXTERNAL TABLE IF NOT EXISTS music_dev.dim_date
(
    dt              STRING COMMENT '日期 yyyy-MM-dd',
    day_of_week_num INT    COMMENT '1=周日 … 7=周六',
    day_of_week     STRING,
    week_of_year    INT,
    month           INT,
    quarter         INT,
    year            INT,
    is_weekend      STRING COMMENT 'Y/N'
)
COMMENT 'DIM-日期维度'
STORED AS ORC
LOCATION 'hdfs://WWC160:9820/warehouse/music_dev/dim_date'
TBLPROPERTIES ('orc.compress' = 'SNAPPY');

-- dim_song -> music_dev
CREATE EXTERNAL TABLE IF NOT EXISTS music_dev.dim_song
(
    song_id       STRING COMMENT '歌曲ID，与事实表 song_id 完全对齐',
    song_name     STRING,
    singer        STRING,
    genre         STRING COMMENT '曲风：流行/摇滚/民谣/R&B',
    duration      INT    COMMENT '时长（秒），与 dwd_play_end.song_duration 同源',
    release_date  STRING,
    region        STRING
)
COMMENT 'DIM-歌曲维度'
STORED AS ORC
LOCATION 'hdfs://WWC160:9820/warehouse/music_dev/dim_song'
TBLPROPERTIES ('orc.compress' = 'SNAPPY');

-- dim_user -> music_dev
CREATE EXTERNAL TABLE IF NOT EXISTS music_dev.dim_user
(
    user_id        STRING COMMENT '用户ID',
    is_vip         STRING COMMENT '是否会员（当前版本）',
    city           STRING,
    province       STRING,
    app_version    STRING,
    os             STRING,
    device_model   STRING,
    register_date  STRING COMMENT '注册日期（首次变更日期）',
    start_date     STRING COMMENT '本版本生效日期',
    end_date       STRING COMMENT '本版本失效日期（9999-12-31 表示当前有效）',
    is_current     STRING COMMENT '是否当前版本 Y/N',
    change_type    STRING COMMENT '本版本的变更类型'
)
COMMENT 'DIM-用户拉链表（SCD2）'
STORED AS ORC
LOCATION 'hdfs://WWC160:9820/warehouse/music_dev/dim_user'
TBLPROPERTIES ('orc.compress' = 'SNAPPY');

-- dwd_ad -> music_dev
CREATE EXTERNAL TABLE IF NOT EXISTS music_dev.dwd_ad (
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
LOCATION 'hdfs://WWC160:9820/warehouse/music_dev/dwd_ad'
TBLPROPERTIES ('orc.compress' = 'SNAPPY');

-- dwd_click -> music_dev
CREATE EXTERNAL TABLE IF NOT EXISTS music_dev.dwd_click (
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
LOCATION 'hdfs://WWC160:9820/warehouse/music_dev/dwd_click'
TBLPROPERTIES ('orc.compress' = 'SNAPPY');

-- dwd_impression -> music_dev
CREATE EXTERNAL TABLE IF NOT EXISTS music_dev.dwd_impression (
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
LOCATION 'hdfs://WWC160:9820/warehouse/music_dev/dwd_impression'
TBLPROPERTIES ('orc.compress' = 'SNAPPY');

-- dwd_interaction -> music_dev
CREATE EXTERNAL TABLE IF NOT EXISTS music_dev.dwd_interaction (
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
LOCATION 'hdfs://WWC160:9820/warehouse/music_dev/dwd_interaction'
TBLPROPERTIES ('orc.compress' = 'SNAPPY');

-- dwd_page_view -> music_dev
CREATE EXTERNAL TABLE IF NOT EXISTS music_dev.dwd_page_view (
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
LOCATION 'hdfs://WWC160:9820/warehouse/music_dev/dwd_page_view'
TBLPROPERTIES ('orc.compress' = 'SNAPPY');

-- dwd_pay -> music_dev
CREATE EXTERNAL TABLE IF NOT EXISTS music_dev.dwd_pay (
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
LOCATION 'hdfs://WWC160:9820/warehouse/music_dev/dwd_pay'
TBLPROPERTIES ('orc.compress' = 'SNAPPY');

-- dwd_play_control -> music_dev
CREATE EXTERNAL TABLE IF NOT EXISTS music_dev.dwd_play_control (
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
LOCATION 'hdfs://WWC160:9820/warehouse/music_dev/dwd_play_control'
TBLPROPERTIES ('orc.compress' = 'SNAPPY');

-- dwd_play_end -> music_dev
CREATE EXTERNAL TABLE IF NOT EXISTS music_dev.dwd_play_end
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
LOCATION 'hdfs://WWC160:9820/warehouse/music_dev/dwd_play_end'
TBLPROPERTIES ('orc.compress' = 'SNAPPY');

-- dwd_play_start -> music_dev
CREATE EXTERNAL TABLE IF NOT EXISTS music_dev.dwd_play_start
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
    playlist_id    STRING    COMMENT '$.playlist_id（造数未生成，恒 NULL）',
    quality        STRING    COMMENT '$.quality（造数未生成，恒 NULL）',
    play_mode      STRING    COMMENT '$.play_mode（造数未生成，恒 NULL）',
    song_index     INT       COMMENT '$.song_index（造数未生成，恒 NULL）',
    etl_time       TIMESTAMP COMMENT '入仓时间'
)
COMMENT 'DWD-播放开始明细（离线，与实时同名同构）'
PARTITIONED BY (dt STRING COMMENT '事件日期 yyyy-MM-dd')
STORED AS ORC
LOCATION 'hdfs://WWC160:9820/warehouse/music_dev/dwd_play_start'
TBLPROPERTIES ('orc.compress' = 'SNAPPY');

-- dwd_search -> music_dev
CREATE EXTERNAL TABLE IF NOT EXISTS music_dev.dwd_search (
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
LOCATION 'hdfs://WWC160:9820/warehouse/music_dev/dwd_search'
TBLPROPERTIES ('orc.compress' = 'SNAPPY');

-- dwd_session -> music_dev
CREATE EXTERNAL TABLE IF NOT EXISTS music_dev.dwd_session (
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
LOCATION 'hdfs://WWC160:9820/warehouse/music_dev/dwd_session'
TBLPROPERTIES ('orc.compress' = 'SNAPPY');

-- dwd_skip -> music_dev
CREATE EXTERNAL TABLE IF NOT EXISTS music_dev.dwd_skip (
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
LOCATION 'hdfs://WWC160:9820/warehouse/music_dev/dwd_skip'
TBLPROPERTIES ('orc.compress' = 'SNAPPY');

-- dws_funnel_daily -> music_dev
CREATE EXTERNAL TABLE IF NOT EXISTS music_dev.dws_funnel_daily (
    impression_cnt      BIGINT  COMMENT '曝光次数（漏斗第 1 层）',
    click_cnt           BIGINT  COMMENT '点击次数（第 2 层）',
    play_start_cnt      BIGINT  COMMENT '起播次数（第 3 层）',
    complete_cnt        BIGINT  COMMENT '完播次数（第 4 层，progress>=0.9）',
    interact_cnt        BIGINT  COMMENT '互动次数（第 5 层，7 类互动事件合计）',
    pay_cnt             BIGINT  COMMENT '付费次数（第 6 层，订阅+下单，不含退款）',
    ctr                 DOUBLE  COMMENT '点击率 = 点击 / 曝光',
    click_to_play_rate  DOUBLE  COMMENT '点击→起播 = 起播 / 点击',
    complete_rate       DOUBLE  COMMENT '完播率 = 完播 / 起播',
    interact_rate       DOUBLE  COMMENT '互动率 = 互动 / 起播',
    pay_conv_rate       DOUBLE  COMMENT '付费转化率 = 付费 / 曝光'
)
COMMENT 'DWS-六层转化漏斗（日）'
PARTITIONED BY (dt STRING)
STORED AS ORC
LOCATION 'hdfs://WWC160:9820/warehouse/music_dev/dws_funnel_daily'
TBLPROPERTIES ('orc.compress' = 'SNAPPY');

-- dws_genre_daily -> music_dev
CREATE EXTERNAL TABLE IF NOT EXISTS music_dev.dws_genre_daily
(
    genre          STRING COMMENT '曲风',
    play_start_cnt BIGINT COMMENT '起播次数（分母）',
    play_end_cnt   BIGINT COMMENT '结束次数',
    complete_cnt   BIGINT COMMENT '完播次数（progress>=0.9）',
    complete_rate  DOUBLE COMMENT '完播率 = complete_cnt / play_start_cnt',
    avg_progress   DOUBLE COMMENT '平均播放进度（仅结束事件）',
    user_cnt       BIGINT COMMENT '收听用户数'
)
COMMENT 'DWS-曲风日粒度播放指标'
PARTITIONED BY (dt STRING)
STORED AS ORC
LOCATION 'hdfs://WWC160:9820/warehouse/music_dev/dws_genre_daily'
TBLPROPERTIES ('orc.compress' = 'SNAPPY');

-- dws_province_daily -> music_dev
CREATE EXTERNAL TABLE IF NOT EXISTS music_dev.dws_province_daily
(
    province       STRING,
    city           STRING,
    dau            BIGINT COMMENT '该省市活跃用户数',
    play_start_cnt BIGINT COMMENT '起播次数',
    session_cnt    BIGINT COMMENT '会话数'
)
COMMENT 'DWS-省份城市日粒度播放指标'
PARTITIONED BY (dt STRING)
STORED AS ORC
LOCATION 'hdfs://WWC160:9820/warehouse/music_dev/dws_province_daily'
TBLPROPERTIES ('orc.compress' = 'SNAPPY');

-- dws_revenue_daily -> music_dev
CREATE EXTERNAL TABLE IF NOT EXISTS music_dev.dws_revenue_daily (
    vip_subscribe_cnt   BIGINT  COMMENT '会员订阅次数',
    order_pay_cnt       BIGINT  COMMENT '订单支付次数',
    refund_cnt          BIGINT  COMMENT '退款次数',
    pay_user_cnt        BIGINT  COMMENT '付费用户数（订阅+下单的 distinct）',
    gross_revenue       DECIMAL(18,2) COMMENT '毛收入 = 订阅 + 下单',
    refund_amount       DECIMAL(18,2) COMMENT '退款金额',
    net_revenue         DECIMAL(18,2) COMMENT '净收入 = 毛收入 - 退款',
    arpu                DECIMAL(18,2) COMMENT '每付费用户平均收入 = 净收入 / 付费用户数',
    refund_rate         DOUBLE         COMMENT '退款率 = 退款次数 / 下单次数（比率）',
    ad_impression_cnt   BIGINT        COMMENT '广告曝光次数',
    ad_click_cnt        BIGINT        COMMENT '广告点击次数',
    ad_ctr              DOUBLE        COMMENT '广告点击率（比率）',
    ad_revenue          DECIMAL(18,2) COMMENT '广告收入（点击 × 出价，估算）'
)
COMMENT 'DWS-商业化日汇总（会员 + 广告）'
PARTITIONED BY (dt STRING)
STORED AS ORC
LOCATION 'hdfs://WWC160:9820/warehouse/music_dev/dws_revenue_daily'
TBLPROPERTIES ('orc.compress' = 'SNAPPY');

-- dws_search_daily -> music_dev
CREATE EXTERNAL TABLE IF NOT EXISTS music_dev.dws_search_daily (
    keyword             STRING  COMMENT '搜索词',
    search_pv           BIGINT  COMMENT '搜索次数',
    search_uv           BIGINT  COMMENT '搜索用户数',
    search_click_pv     BIGINT  COMMENT '搜索结果点击次数',
    search_click_rate   DOUBLE  COMMENT '搜索点击率 = 点击 / 搜索',
    zero_result_pv      BIGINT  COMMENT '零结果搜索次数（result_cnt=0）',
    zero_result_rate    DOUBLE  COMMENT '零结果率 = 零结果次数 / 搜索次数',
    avg_result_cnt      DOUBLE  COMMENT '平均返回结果数'
)
COMMENT 'DWS-搜索域日粒度（按关键词）'
PARTITIONED BY (dt STRING)
STORED AS ORC
LOCATION 'hdfs://WWC160:9820/warehouse/music_dev/dws_search_daily'
TBLPROPERTIES ('orc.compress' = 'SNAPPY');

-- dws_song_play_daily -> music_dev
CREATE EXTERNAL TABLE IF NOT EXISTS music_dev.dws_song_play_daily
(
    song_id            STRING  COMMENT '歌曲ID',
    play_start_cnt     BIGINT  COMMENT '起播次数（完播率分母）',
    play_end_cnt       BIGINT  COMMENT '结束次数（含播完与中断）',
    complete_cnt       BIGINT  COMMENT '完播次数（progress >= 0.9）',
    complete_rate      DOUBLE  COMMENT '完播率 = complete_cnt / play_start_cnt',
    skip_play_cnt      BIGINT  COMMENT '未播完次数 = 起播 - 完播',
    avg_progress       DOUBLE  COMMENT '平均播放进度',
    avg_play_duration  DOUBLE  COMMENT '平均实际播放时长（秒）'
)
COMMENT 'DWS-歌曲日粒度播放与完播指标'
PARTITIONED BY (dt STRING COMMENT '统计日期 yyyy-MM-dd')
STORED AS ORC
LOCATION 'hdfs://WWC160:9820/warehouse/music_dev/dws_song_play_daily'
TBLPROPERTIES ('orc.compress' = 'SNAPPY');

-- dws_user_daily -> music_dev
CREATE EXTERNAL TABLE IF NOT EXISTS music_dev.dws_user_daily
(
    dau                BIGINT COMMENT '日活用户数',
    session_cnt        BIGINT COMMENT '会话数',
    play_start_cnt     BIGINT COMMENT '起播次数',
    avg_play_per_user  DOUBLE COMMENT '人均起播次数',
    avg_sess_per_user  DOUBLE COMMENT '人均会话数',
    login_user_cnt     BIGINT COMMENT '登录用户数',
    vip_user_cnt       BIGINT COMMENT '会员用户数'
)
COMMENT 'DWS-用户日粒度活跃指标'
PARTITIONED BY (dt STRING)
STORED AS ORC
LOCATION 'hdfs://WWC160:9820/warehouse/music_dev/dws_user_daily'
TBLPROPERTIES ('orc.compress' = 'SNAPPY');

-- ods_music_event -> music_dev
CREATE EXTERNAL TABLE IF NOT EXISTS music_dev.ods_music_event
(
    event_id      STRING COMMENT '事件唯一ID',
    event_name    STRING COMMENT 'play_start/play_end/skip/collect/search/share/page_view',
    event_time    STRING COMMENT '客户端事件时间（毫秒时间戳）',
    server_time   STRING COMMENT '服务端接收时间（毫秒时间戳）',
    user_id       STRING COMMENT '用户ID，未登录为空',
    device_id     STRING COMMENT '设备ID',
    is_login      STRING COMMENT '是否登录',
    is_vip        STRING COMMENT '是否会员',
    app_version   STRING COMMENT 'APP版本',
    os            STRING COMMENT 'android/ios/web/car',
    os_version    STRING COMMENT '系统版本',
    device_model  STRING COMMENT '设备型号',
    network       STRING COMMENT 'wifi/4g/5g/3g',
    carrier       STRING COMMENT '运营商',
    ip            STRING COMMENT '客户端IP',
    province      STRING COMMENT '省份',
    city          STRING COMMENT '城市',
    session_id    STRING COMMENT '会话ID',
    session_seq   STRING COMMENT '会话内事件序号',
    is_foreground STRING COMMENT '前后台标记',
    channel       STRING COMMENT '曝光来源渠道',
    page_id       STRING COMMENT '事件发生页面',
    properties    STRING COMMENT '事件私有属性 JSON 原文，不解析',
    etl_time      STRING COMMENT '入仓时间（yyyy-MM-dd HH:mm:ss）'
)
COMMENT '离线ODS-音乐APP埋点原始明细（24列，与 Doris 版同构）'
PARTITIONED BY (dt STRING COMMENT '事件日期 yyyy-MM-dd')
STORED AS ORC
LOCATION 'hdfs://WWC160:9820/warehouse/music_dev/ods_music_event'
TBLPROPERTIES ('orc.compress' = 'SNAPPY');

-- ods_user_change -> music_dev
CREATE EXTERNAL TABLE IF NOT EXISTS music_dev.ods_user_change
(
    user_id      STRING COMMENT '用户ID',
    change_date  STRING COMMENT '变更生效日期 yyyy-MM-dd',
    is_vip       STRING COMMENT '是否会员',
    city         STRING,
    province     STRING,
    app_version  STRING,
    os           STRING,
    device_model STRING,
    change_type  STRING COMMENT 'register/vip_upgrade/city_change'
)
COMMENT 'ODS-用户属性变更流水（SCD2 的源）'
STORED AS ORC
LOCATION 'hdfs://WWC160:9820/warehouse/music_dev/ods_user_change'
TBLPROPERTIES ('orc.compress' = 'SNAPPY');
