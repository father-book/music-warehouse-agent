-- =============================================================================
-- DWD 层建表：play_end 明细表 + 质检异常表
-- 执行位置：任意能连 Doris 9030 的机器
-- 执行方式：mysql -hWWC163 -P9030 -uroot < doris_dwd_tables.sql
-- =============================================================================
-- 设计要点：
--   1. 明细表按【事件时间 dt】分区（数据已清洗，事件时间可信），
--      与 ODS 按【入仓日期 etl_date】分区形成对照 —— 这是分层设计的体现。
--   2. 异常表按【入仓日期 dt】分区：异常数据的 event_time 本身不可信
--      （可能就是因为它才被拦下），用它分区会导致写入失败。
--   3. 两张表都是 Duplicate 模型：明细层保留原始粒度，不去重。
--      重复上报由质检规则拦截，不靠存储层消化。
-- =============================================================================

USE music_dw;

-- -----------------------------------------------------------------------------
-- 1) 播放结束明细表
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dwd_play_end (
    event_id        VARCHAR(64)  NOT NULL COMMENT '事件ID',
    event_ts        DATETIME     NOT NULL COMMENT '事件时间（由 event_time 毫秒戳转换）',
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
    page_id         VARCHAR(32)  NULL COMMENT '来源页面',
    song_id         VARCHAR(32)  NOT NULL COMMENT '歌曲ID（来自 properties）',
    song_duration   INT          NULL     COMMENT '歌曲总时长（秒）',
    play_duration   INT          NULL     COMMENT '实际播放时长（秒）',
    play_progress   DOUBLE       NULL     COMMENT '播放进度 0~1',
    end_reason      VARCHAR(16)  NULL     COMMENT '结束原因：natural/skip/pause/exit/interrupt',
    etl_time        DATETIME     NULL     COMMENT '入仓时间，用于血缘追溯',
    dt              DATE         NOT NULL COMMENT '事件日期，分区键'
) ENGINE=OLAP
DUPLICATE KEY(event_id, event_ts)
COMMENT 'DWD-播放结束明细'
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

-- -----------------------------------------------------------------------------
-- 2) 质检异常表（第四章核心）
-- -----------------------------------------------------------------------------
-- rule_level：block = 拦截不进 DWD；warn = 放行但记录
-- 一条数据只记一行（用 CASE 取主因），避免规则串扰导致召回率超 100%
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dwd_quality_error (
    event_id        VARCHAR(64)  NULL,
    rule_name       VARCHAR(64)  NOT NULL COMMENT '命中的规则名',
    event_name      VARCHAR(32)  NULL,
    rule_level      VARCHAR(16)  NULL     COMMENT 'block / warn',
    detail          VARCHAR(512) NULL     COMMENT '具体命中值，便于举证',
    song_id         VARCHAR(32)  NULL,
    user_id         VARCHAR(32)  NULL,
    device_id       VARCHAR(32)  NULL,
    event_ts        DATETIME     NULL,
    raw_properties  TEXT         NULL     COMMENT '原始 properties，事后可复核',
    etl_time        DATETIME     NULL,
    dt              DATE         NOT NULL COMMENT '入仓日期，分区键'
) ENGINE=OLAP
DUPLICATE KEY(event_id, rule_name)
COMMENT 'DWD-质检异常明细'
PARTITION BY RANGE(dt) ()
DISTRIBUTED BY HASH(event_id) BUCKETS 4
PROPERTIES (
    "replication_allocation" = "tag.location.default: 1",
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "DAY",
    -- 兜底/异常表只留 3 天：这两张表没有分析价值，只用于当班排查
    -- 异常分布；留一年会无限涨（unrouted 每天 ~24 万行 / 20MB）。
    -- 其余明细表仍是 365 天，不要跟着改。
    "dynamic_partition.start" = "-3",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.buckets" = "4"
);

-- =============================================================================
-- v10（11-07）：实时侧未建明细表的合法事件兜底
-- 上游统一成 gen_events.py 的 26 类事件后，实时只路由 11 类到明细表；
-- 其余 15 类不能判脏（会污染 dwd_quality_error），也不能静默丢（对账不闭合）。
-- 原文落这张表，恒等式：ODS 入口 = 各 DWD 明细 + 异常 + dwd_unrouted。
-- =============================================================================
CREATE TABLE IF NOT EXISTS dwd_unrouted (
    event_id       VARCHAR(64)  NULL,
    event_name     VARCHAR(32)  NULL,
    event_ts       DATETIME     NULL COMMENT '事件时间（与其余 DWD 同型）',
    server_ts      DATETIME     NULL COMMENT '客户端上报时间',
    user_id        VARCHAR(32)  NULL,
    device_id      VARCHAR(32)  NULL,
    properties     TEXT         NULL COMMENT '事件私有属性 JSON 原文',
    etl_time       DATETIME     NULL,
    dt             DATE         NOT NULL COMMENT '入仓日期，分区键'
) ENGINE=OLAP
DUPLICATE KEY(event_id, event_name)
COMMENT 'DWD-实时侧未建明细表的合法事件兜底（Kafka-as-ODS 恒等式闭环）'
PARTITION BY RANGE(dt) ()
DISTRIBUTED BY HASH(event_id) BUCKETS 4
PROPERTIES (
    "replication_allocation" = "tag.location.default: 1",
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "DAY",
    -- 兜底/异常表只留 3 天：这两张表没有分析价值，只用于当班排查
    -- 异常分布；留一年会无限涨（unrouted 每天 ~24 万行 / 20MB）。
    -- 其余明细表仍是 365 天，不要跟着改。
    "dynamic_partition.start" = "-3",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.buckets" = "4"
);
