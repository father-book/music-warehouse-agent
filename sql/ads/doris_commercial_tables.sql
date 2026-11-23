-- =============================================================================
-- 商业化与客户端质量 ADS 表
--
-- 为什么放在 ADS 而不是让作业 D 算：
--   作业 D 的源是 dwd_play_end（播放主题），订单在 dwd_order、错误在 dwd_play_error，
--   让 D 同时消费三个 topic 会让一个作业承担三套口径，且拓扑一变就要整体重放。
--   这里沿用「今日累计」已验证过的微批方式：采集器每分钟从明细表聚合一次，
--   结果写 UNIQUE KEY 表 —— 重跑幂等、不占 slot、口径随时可改。
-- =============================================================================

CREATE TABLE IF NOT EXISTS ads_commercial_daily (
    stat_date       DATE         NOT NULL COMMENT '统计日期',
    pay_order_cnt   BIGINT       NULL     COMMENT '已支付订单数（vip_subscribe + order_pay）',
    pay_user_cnt    BIGINT       NULL     COMMENT '付费用户数（去重 user_id）',
    active_user_cnt BIGINT       NULL     COMMENT '当日活跃用户数（用于算付费率）',
    pay_rate        DOUBLE       NULL     COMMENT '付费率 % = 付费用户 / 活跃用户',
    revenue         DOUBLE       NULL     COMMENT '收入(元) = Σ 已支付金额',
    refund_amt      DOUBLE       NULL     COMMENT '退款金额(元)',
    net_revenue     DOUBLE       NULL     COMMENT '净收入 = 收入 - 退款',
    refund_cnt      BIGINT       NULL     COMMENT '退款笔数',
    subscribe_cnt   BIGINT       NULL     COMMENT '订阅订单数',
    single_cnt      BIGINT       NULL     COMMENT '单曲/专辑购买订单数',
    arppu           DOUBLE       NULL     COMMENT 'ARPPU = 净收入 / 付费用户数',
    updated_at      DATETIME     NULL
) ENGINE=OLAP
UNIQUE KEY(stat_date)
COMMENT 'ADS-商业化日报（微批，每分钟刷新当日累计）'
DISTRIBUTED BY HASH(stat_date) BUCKETS 1
PROPERTIES ("replication_allocation" = "tag.location.default: 1");

CREATE TABLE IF NOT EXISTS ads_sku_daily (
    stat_date   DATE         NOT NULL COMMENT '统计日期',
    sku_id      VARCHAR(32)  NOT NULL COMMENT 'SKU',
    order_cnt   BIGINT       NULL     COMMENT '订单数',
    revenue     DOUBLE       NULL     COMMENT '收入(元)',
    updated_at  DATETIME     NULL
) ENGINE=OLAP
UNIQUE KEY(stat_date, sku_id)
COMMENT 'ADS-SKU 销售构成（微批）'
DISTRIBUTED BY HASH(stat_date) BUCKETS 1
PROPERTIES ("replication_allocation" = "tag.location.default: 1");

CREATE TABLE IF NOT EXISTS ads_client_quality_1min (
    stat_minute     DATETIME     NOT NULL COMMENT '统计分钟',
    play_cnt        BIGINT       NULL     COMMENT '播放开始数（分母）',
    error_cnt       BIGINT       NULL     COMMENT '播放失败数',
    error_rate      DOUBLE       NULL     COMMENT '播放失败率 % = 失败 / (播放 + 失败)',
    top_error_code  VARCHAR(16)  NULL     COMMENT '最高频错误码',
    top_error_cnt   BIGINT       NULL     COMMENT '最高频错误次数',
    updated_at      DATETIME     NULL
) ENGINE=OLAP
UNIQUE KEY(stat_minute)
COMMENT 'ADS-客户端播放质量（分钟级）'
PARTITION BY RANGE(stat_minute) ()
DISTRIBUTED BY HASH(stat_minute) BUCKETS 1
PROPERTIES (
    "replication_allocation" = "tag.location.default: 1",
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "DAY",
    "dynamic_partition.start" = "-7",
    "dynamic_partition.end" = "1",
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.buckets" = "1"
);
