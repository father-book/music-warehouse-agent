-- =============================================================================
-- doris_ads_ext.sql —— 扩表后新增的 2 张 Doris ADS 表
-- 数据源：Doris FE 192.168.128.163:9030（MySQL 协议，root 空密码）
-- 执行：python code/sync_doris.py --create-ext  （或 mysql 客户端直接 source）
--
-- 与 sql/offline/doris_ads.sql 的前 5 张表保持同样的 UNIQUE KEY + 单 bucket +
-- 单副本（单机 BE），字段顺序必须与 10_sync_to_doris.sql 的 SELECT 一致。
-- 金额列用 DECIMAL(18,2)，与 Hive 侧 12 号脚本、docs/20 §1.3 保持一致。
-- 注意：CREATE TABLE IF NOT EXISTS 不会改已有表结构 —— 改类型必须先 DROP 再建，
-- 随后立刻跑 sync_doris.py --create-ext 把数据灌回。
-- =============================================================================

CREATE DATABASE IF NOT EXISTS music_offline;

CREATE TABLE IF NOT EXISTS music_offline.ads_funnel (
  dt                  DATE NOT NULL COMMENT '日期',
  impression_cnt      BIGINT       COMMENT '曝光次数',
  click_cnt           BIGINT       COMMENT '点击次数',
  ctr                 DOUBLE       COMMENT '点击率',
  play_start_cnt      BIGINT       COMMENT '起播次数',
  click_to_play_rate  DOUBLE       COMMENT '点击→起播',
  complete_cnt        BIGINT       COMMENT '完播次数',
  complete_rate       DOUBLE       COMMENT '完播率',
  interact_cnt        BIGINT       COMMENT '互动次数',
  interact_rate       DOUBLE       COMMENT '互动率',
  pay_cnt             BIGINT       COMMENT '付费次数',
  pay_conv_rate       DOUBLE       COMMENT '付费转化率',
  pay_cnt_dod         DOUBLE       COMMENT '付费次数日环比'
) ENGINE=OLAP UNIQUE KEY(dt)
DISTRIBUTED BY HASH(dt) BUCKETS 1 PROPERTIES ("replication_num" = "1");

CREATE TABLE IF NOT EXISTS music_offline.ads_revenue (
  dt                DATE NOT NULL COMMENT '日期',
  gross_revenue     DECIMAL(18,2) COMMENT '毛收入',
  refund_amount     DECIMAL(18,2) COMMENT '退款金额',
  net_revenue       DECIMAL(18,2) COMMENT '净收入',
  ad_revenue        DECIMAL(18,2) COMMENT '广告收入（估算）',
  pay_user_cnt      BIGINT        COMMENT '付费用户数',
  arpu              DECIMAL(18,2) COMMENT '每付费用户平均收入',
  refund_rate       DOUBLE        COMMENT '退款率（比率）',
  ad_ctr            DOUBLE        COMMENT '广告点击率（比率）',
  net_revenue_dod   DOUBLE        COMMENT '净收入日环比（比率）',
  cum_net_revenue   DECIMAL(18,2) COMMENT '周期内累计净收入'
) ENGINE=OLAP UNIQUE KEY(dt)
DISTRIBUTED BY HASH(dt) BUCKETS 1 PROPERTIES ("replication_num" = "1");
