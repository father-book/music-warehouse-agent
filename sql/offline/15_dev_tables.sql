-- =============================================================================
-- 15_dev_tables.sql —— DEV：开发层（数据开发工程师经 Agent ETL 流程产出）
--
-- 口径约定：
--   net_revenue 与 metrics.yaml 的 revenue_sum（收入金额·净）**逐分对齐**：
--     收入 = vip_subscribe + order_pay（正） + order_refund（负）
--     漏掉 vip_subscribe 会低 5.24 倍（82% 毛收入在 VIP 订阅，见 metrics.yaml trap）
--   实际建表走 Hive CTAS（managed 表，TextFile），本 DDL 是结构权威描述。
--   35 天净收入合计 823,410 与 ads_revenue.net_revenue 合计逐分相等（2026-11-11 实测）。
-- =============================================================================

CREATE EXTERNAL TABLE IF NOT EXISTS music_dw.dev_province_revenue_daily
(
    province      STRING COMMENT '省份',
    dt            STRING COMMENT '日期',
    pay_order_cnt BIGINT COMMENT '支付订单数（仅 order_pay）',
    net_revenue   DECIMAL(18,2) COMMENT '收入金额·净（vip_subscribe+order_pay−order_refund）'
)
COMMENT 'DEV-省份×日收入净额（由 dwd_pay 聚合，工程师开发表）'
STORED AS TEXTFILE
LOCATION '/warehouse/music_dw/dev_province_revenue_daily';

-- ---- dev_province_daily_top20_user（Agent 登记于 2026-11-11）----

CREATE EXTERNAL TABLE IF NOT EXISTS music_dw.dev_province_daily_top20_user
(
    province             STRING,
    dt                   STRING,
    user_id              STRING,
    gross_revenue        DECIMAL(28, 2),
    rn                   INT
)
COMMENT 'DEV-分省每日消费金额Top20用户（Agent ETL 生成）'
STORED AS TEXTFILE
LOCATION '/warehouse/music_dw/dev_province_daily_top20_user';
