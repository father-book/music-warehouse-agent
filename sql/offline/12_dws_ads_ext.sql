-- =============================================================================
-- 12_dws_ads_ext.sql —— DWS/ADS 扩表：漏斗、搜索、收入（docs/19 §5.1 / §5.2）
-- 执行：
--   hive --hivevar start_dt=2026-10-05 --hivevar end_dt=2026-11-04 \
--        -f 12_dws_ads_ext.sql
--
-- 新增 3 张 DWS + 2 张 ADS，全部基于 11 号脚本建好的 DWD 域表，
-- 完播数复用 dws_song_play_daily（与 ads_daily_overview 同源，不引入第二套口径）。
--
-- 【v9 造数口径变化，2026-11-07 重造 31 天后生效】
--   1) ORDER_TYPES 的 amount 不再恒 15.0，改为按 SKU 单价取值
--      （vip_month 15 / vip_quarter 40 / vip_year 148 / song_single 3 /
--        digital_album 25，与实时侧 gen_music_data.py 的 SKU_PRICE 一致）。
--      退款事件的 amount 仍是正数，净收入必须按 event_name 手工区分正负，
--      不能 SUM(amount) 了事。金额列为 DECIMAL(18,2)，收入可与「单价 × 订单数」对账。
--   2) SEARCH_TYPES 的 keyword 改为 10 词哈希取值，result_cnt 按
--      8% 零结果 / 30% 1~15 / 42% 16~80 / 20% 81~300 分档 ——
--      零结果率不再是恒 0，dws_search_daily 从 31 行变成最多 10 词 × 31 天。
--      旧版 pick(values, _i) 取模退化（低频事件 _i 恒 1 → keyword 恒「夜曲」）
--      已由 hidx() 哈希下标修复。
--   3) AD_TYPES 的 bid_price 改为 0.15/0.35/0.80/1.20 四档，
--      广告收入仍按「点击 × 出价」估算，不是真实竞价结果。
--   4) 以上仍是「一份模拟数据」，不能用于真实业务归因；但字段已有区分度，
--      可用来演示按 SKU / 搜索词 / 广告位下钻。
--
-- 【金额口径】按 docs/20 §1.3，金额列一律 DECIMAL(18,2)，不用 DOUBLE：
--   dwd_pay.amount / dwd_ad.bid_price（11 号脚本）→ dws_revenue_daily
--   → ads_revenue → Doris music_offline.ads_revenue，全链路同类型。
--   比率类（refund_rate / ad_ctr / *_dod / ctr）不是金额，仍用 DOUBLE。
--   聚合入口显式 CAST，避免 SUM 中途退化成 DOUBLE 再写回 DECIMAL 的假精度。
-- =============================================================================

SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;
SET hive.exec.max.dynamic.partitions = 200;
SET hive.exec.max.dynamic.partitions.pernode = 100;
SET hive.stats.autogather = false;


-- ===========================================================================
-- 1. dws_funnel_daily —— 六层漏斗（一行一天）
--    曝光 → 点击 → 起播 → 完播 → 互动 → 付费
--    六个来源分别聚合后 UNION ALL 再按 dt 条件求和，避免 6 表 JOIN 的膨胀。
-- ===========================================================================
-- [11-08 修正] 不再无条件 DROP：只补少数几天时会把历史分区一起清掉
-- DROP TABLE IF EXISTS music_dw.dws_funnel_daily;
CREATE EXTERNAL TABLE IF NOT EXISTS music_dw.dws_funnel_daily (
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
LOCATION 'hdfs://WWC160:9820/warehouse/music_dw/dws_funnel_daily'
TBLPROPERTIES ('orc.compress' = 'SNAPPY');

INSERT OVERWRITE TABLE music_dw.dws_funnel_daily PARTITION (dt)
SELECT
    SUM(CASE WHEN step = 'impression'  THEN cnt ELSE 0 END) AS impression_cnt,
    SUM(CASE WHEN step = 'click'       THEN cnt ELSE 0 END) AS click_cnt,
    SUM(CASE WHEN step = 'play_start'  THEN cnt ELSE 0 END) AS play_start_cnt,
    SUM(CASE WHEN step = 'complete'    THEN cnt ELSE 0 END) AS complete_cnt,
    SUM(CASE WHEN step = 'interact'    THEN cnt ELSE 0 END) AS interact_cnt,
    SUM(CASE WHEN step = 'pay'         THEN cnt ELSE 0 END) AS pay_cnt,
    ROUND(SUM(CASE WHEN step = 'click'      THEN cnt ELSE 0 END)
          / NULLIF(SUM(CASE WHEN step = 'impression' THEN cnt ELSE 0 END), 0), 4) AS ctr,
    ROUND(SUM(CASE WHEN step = 'play_start' THEN cnt ELSE 0 END)
          / NULLIF(SUM(CASE WHEN step = 'click'      THEN cnt ELSE 0 END), 0), 4) AS click_to_play_rate,
    ROUND(SUM(CASE WHEN step = 'complete'   THEN cnt ELSE 0 END)
          / NULLIF(SUM(CASE WHEN step = 'play_start' THEN cnt ELSE 0 END), 0), 4) AS complete_rate,
    ROUND(SUM(CASE WHEN step = 'interact'   THEN cnt ELSE 0 END)
          / NULLIF(SUM(CASE WHEN step = 'play_start' THEN cnt ELSE 0 END), 0), 4) AS interact_rate,
    ROUND(SUM(CASE WHEN step = 'pay'        THEN cnt ELSE 0 END)
          / NULLIF(SUM(CASE WHEN step = 'impression' THEN cnt ELSE 0 END), 0), 6) AS pay_conv_rate,
    dt
FROM (
    SELECT dt, 'impression' AS step, COUNT(*) AS cnt
    FROM music_dw.dwd_impression
    WHERE dt BETWEEN '${start_dt}' AND '${end_dt}' GROUP BY dt
    UNION ALL
    SELECT dt, 'click' AS step, COUNT(*) AS cnt
    FROM music_dw.dwd_click
    WHERE dt BETWEEN '${start_dt}' AND '${end_dt}' GROUP BY dt
    UNION ALL
    SELECT dt, 'play_start' AS step, SUM(play_start_cnt) AS cnt
    FROM music_dw.dws_song_play_daily
    WHERE dt BETWEEN '${start_dt}' AND '${end_dt}' GROUP BY dt
    UNION ALL
    SELECT dt, 'complete' AS step, SUM(complete_cnt) AS cnt
    FROM music_dw.dws_song_play_daily
    WHERE dt BETWEEN '${start_dt}' AND '${end_dt}' GROUP BY dt
    UNION ALL
    SELECT dt, 'interact' AS step, COUNT(*) AS cnt
    FROM music_dw.dwd_interaction
    WHERE dt BETWEEN '${start_dt}' AND '${end_dt}' GROUP BY dt
    UNION ALL
    SELECT dt, 'pay' AS step, COUNT(*) AS cnt
    FROM music_dw.dwd_pay
    WHERE dt BETWEEN '${start_dt}' AND '${end_dt}'
      AND event_name IN ('vip_subscribe', 'order_pay')
    GROUP BY dt
) u
GROUP BY dt;


-- ===========================================================================
-- 2. dws_search_daily —— 搜索词粒度（一天 × 关键词）
--    v9 起 keyword 有 10 个取值、result_cnt 含 0，零结果率与词间区分度都是真的。
-- ===========================================================================
-- [11-08 修正] 不再无条件 DROP：只补少数几天时会把历史分区一起清掉
-- DROP TABLE IF EXISTS music_dw.dws_search_daily;
CREATE EXTERNAL TABLE IF NOT EXISTS music_dw.dws_search_daily (
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
LOCATION 'hdfs://WWC160:9820/warehouse/music_dw/dws_search_daily'
TBLPROPERTIES ('orc.compress' = 'SNAPPY');

INSERT OVERWRITE TABLE music_dw.dws_search_daily PARTITION (dt)
SELECT
    NVL(keyword, '(unknown)') AS keyword,
    SUM(CASE WHEN event_name = 'search'       THEN 1 ELSE 0 END) AS search_pv,
    -- 【2026-11-09 修】旧版 COUNT(DISTINCT user_id) 把全体游客压成 1 个空值。
    -- 实测 11-08：搜索 UV 1,047 → 1,277（+22%）。实体定义与 dws_user_daily 一致。
    COUNT(DISTINCT CASE WHEN event_name = 'search'
                        THEN COALESCE(NULLIF(user_id, ''), device_id) END) AS search_uv,
    SUM(CASE WHEN event_name = 'search_click' THEN 1 ELSE 0 END) AS search_click_pv,
    ROUND(SUM(CASE WHEN event_name = 'search_click' THEN 1 ELSE 0 END)
          / NULLIF(SUM(CASE WHEN event_name = 'search' THEN 1 ELSE 0 END), 0), 4) AS search_click_rate,
    SUM(CASE WHEN event_name = 'search' AND NVL(result_cnt, 0) = 0 THEN 1 ELSE 0 END) AS zero_result_pv,
    ROUND(SUM(CASE WHEN event_name = 'search' AND NVL(result_cnt, 0) = 0 THEN 1 ELSE 0 END)
          / NULLIF(SUM(CASE WHEN event_name = 'search' THEN 1 ELSE 0 END), 0), 4) AS zero_result_rate,
    ROUND(AVG(CASE WHEN event_name = 'search' THEN result_cnt END), 2) AS avg_result_cnt,
    dt
FROM music_dw.dwd_search
WHERE dt BETWEEN '${start_dt}' AND '${end_dt}'
GROUP BY dt, NVL(keyword, '(unknown)');


-- ===========================================================================
-- 3. dws_revenue_daily —— 商业化日汇总（一行一天）
--    会员侧：订阅 + 下单 - 退款；退款事件的 amount 是正数，必须减掉。
--    广告侧：点击 × bid_price 估算收入，ad_ctr = 点击 / 曝光。
--    pay_uv 单独作为一行参与 UNION，避免按事件名分组后 SUM(uv) 重复计用户。
-- ===========================================================================
-- [11-08 修正] 不再无条件 DROP：只补少数几天时会把历史分区一起清掉
-- DROP TABLE IF EXISTS music_dw.dws_revenue_daily;
CREATE EXTERNAL TABLE IF NOT EXISTS music_dw.dws_revenue_daily (
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
LOCATION 'hdfs://WWC160:9820/warehouse/music_dw/dws_revenue_daily'
TBLPROPERTIES ('orc.compress' = 'SNAPPY');

INSERT OVERWRITE TABLE music_dw.dws_revenue_daily PARTITION (dt)
SELECT
    SUM(CASE WHEN k = 'vip_subscribe'   THEN c ELSE 0 END) AS vip_subscribe_cnt,
    SUM(CASE WHEN k = 'order_pay'       THEN c ELSE 0 END) AS order_pay_cnt,
    SUM(CASE WHEN k = 'order_refund'    THEN c ELSE 0 END) AS refund_cnt,
    SUM(CASE WHEN k = 'pay_uv'          THEN c ELSE 0 END) AS pay_user_cnt,
    CAST(ROUND(SUM(CASE WHEN k IN ('vip_subscribe', 'order_pay') THEN a ELSE 0 END), 2)
         AS DECIMAL(18,2)) AS gross_revenue,
    CAST(ROUND(SUM(CASE WHEN k = 'order_refund' THEN a ELSE 0 END), 2)
         AS DECIMAL(18,2)) AS refund_amount,
    CAST(ROUND(SUM(CASE WHEN k IN ('vip_subscribe', 'order_pay') THEN a ELSE 0 END)
          - SUM(CASE WHEN k = 'order_refund' THEN a ELSE 0 END), 2)
         AS DECIMAL(18,2)) AS net_revenue,
    CAST(ROUND((SUM(CASE WHEN k IN ('vip_subscribe', 'order_pay') THEN a ELSE 0 END)
           - SUM(CASE WHEN k = 'order_refund' THEN a ELSE 0 END))
          / NULLIF(SUM(CASE WHEN k = 'pay_uv' THEN c ELSE 0 END), 0), 2)
         AS DECIMAL(18,2)) AS arpu,
    ROUND(SUM(CASE WHEN k = 'order_refund' THEN c ELSE 0 END)
          / NULLIF(SUM(CASE WHEN k = 'order_pay' THEN c ELSE 0 END), 0), 4) AS refund_rate,
    SUM(CASE WHEN k = 'ad_impression'   THEN c ELSE 0 END) AS ad_impression_cnt,
    SUM(CASE WHEN k = 'ad_click'        THEN c ELSE 0 END) AS ad_click_cnt,
    ROUND(SUM(CASE WHEN k = 'ad_click'       THEN c ELSE 0 END)
          / NULLIF(SUM(CASE WHEN k = 'ad_impression' THEN c ELSE 0 END), 0), 4) AS ad_ctr,
    CAST(ROUND(SUM(CASE WHEN k = 'ad_click' THEN a ELSE 0 END), 2)
         AS DECIMAL(18,2)) AS ad_revenue,
    dt
FROM (
    SELECT dt, event_name AS k, COUNT(*) AS c,
           SUM(NVL(amount, CAST(0 AS DECIMAL(18,2)))) AS a
    FROM music_dw.dwd_pay
    WHERE dt BETWEEN '${start_dt}' AND '${end_dt}' GROUP BY dt, event_name
    UNION ALL
    -- 付费用户数同样按「用户为主体、游客按设备补」去重。
    -- 生成器里 is_login = uid%10<8，即 20% 用户是永久游客，他们照样会下单/订阅；
    -- 用裸 user_id 会把这批人全部压成 1 个空值（实测 11-08：1,046 条支付里 207 条无 user_id）。
    SELECT dt, 'pay_uv' AS k,
           COUNT(DISTINCT COALESCE(NULLIF(user_id, ''), device_id)) AS c,
           CAST(0 AS DECIMAL(18,2)) AS a
    FROM music_dw.dwd_pay
    WHERE dt BETWEEN '${start_dt}' AND '${end_dt}'
      AND event_name IN ('vip_subscribe', 'order_pay') GROUP BY dt
    UNION ALL
    SELECT dt, event_name AS k, COUNT(*) AS c,
           SUM(NVL(bid_price, CAST(0 AS DECIMAL(18,2)))) AS a
    FROM music_dw.dwd_ad
    WHERE dt BETWEEN '${start_dt}' AND '${end_dt}' GROUP BY dt, event_name
) u
GROUP BY dt;


-- ===========================================================================
-- 4. ads_funnel —— 漏斗报表（只查 dws_funnel_daily）
--    额外给日环比，证明 ADS 层做的是「展示口径」而不是重复搬运。
-- ===========================================================================
-- [11-08 修正] 不再无条件 DROP：只补少数几天时会把历史分区一起清掉
-- DROP TABLE IF EXISTS music_dw.ads_funnel;
CREATE EXTERNAL TABLE IF NOT EXISTS music_dw.ads_funnel (
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
LOCATION 'hdfs://WWC160:9820/warehouse/music_dw/ads_funnel'
TBLPROPERTIES ('orc.compress' = 'SNAPPY');

INSERT OVERWRITE TABLE music_dw.ads_funnel PARTITION (dt)
SELECT
    impression_cnt, click_cnt, ctr,
    play_start_cnt, click_to_play_rate,
    complete_cnt, complete_rate,
    interact_cnt, interact_rate,
    pay_cnt, pay_conv_rate,
    ROUND((pay_cnt - LAG(pay_cnt, 1) OVER (ORDER BY dt))
          / NULLIF(LAG(pay_cnt, 1) OVER (ORDER BY dt), 0), 4) AS pay_cnt_dod,
    dt
FROM music_dw.dws_funnel_daily
WHERE dt BETWEEN '${start_dt}' AND '${end_dt}';


-- ===========================================================================
-- 5. ads_revenue —— 收入报表（只查 dws_revenue_daily）
--    日环比 + 全周期累计，DataArt 商业化看板直接读。
-- ===========================================================================
-- [11-08 修正] 不再无条件 DROP：只补少数几天时会把历史分区一起清掉
-- DROP TABLE IF EXISTS music_dw.ads_revenue;
CREATE EXTERNAL TABLE IF NOT EXISTS music_dw.ads_revenue (
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
LOCATION 'hdfs://WWC160:9820/warehouse/music_dw/ads_revenue'
TBLPROPERTIES ('orc.compress' = 'SNAPPY');

INSERT OVERWRITE TABLE music_dw.ads_revenue PARTITION (dt)
SELECT
    gross_revenue, refund_amount, net_revenue, ad_revenue,
    pay_user_cnt, arpu, refund_rate, ad_ctr,
    ROUND((net_revenue - LAG(net_revenue, 1) OVER (ORDER BY dt))
          / NULLIF(LAG(net_revenue, 1) OVER (ORDER BY dt), 0), 4) AS net_revenue_dod,
    CAST(ROUND(SUM(net_revenue) OVER (ORDER BY dt ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), 2)
         AS DECIMAL(18,2)) AS cum_net_revenue,
    dt
FROM music_dw.dws_revenue_daily
WHERE dt BETWEEN '${start_dt}' AND '${end_dt}';


-- ===========================================================================
-- 验收：5 张表行数 + 漏斗/收入首末日指标
-- ===========================================================================
SELECT CONCAT('V|dws_funnel_daily|', COUNT(*)) FROM music_dw.dws_funnel_daily;
SELECT CONCAT('V|dws_search_daily|', COUNT(*)) FROM music_dw.dws_search_daily;
SELECT CONCAT('V|dws_revenue_daily|', COUNT(*)) FROM music_dw.dws_revenue_daily;
SELECT CONCAT('V|ads_funnel|', COUNT(*)) FROM music_dw.ads_funnel;
SELECT CONCAT('V|ads_revenue|', COUNT(*)) FROM music_dw.ads_revenue;
SELECT CONCAT('F|', MIN(dt), '|', MAX(dt), '|',
              CAST(ROUND(AVG(ctr), 4) AS STRING), '|',
              CAST(ROUND(AVG(complete_rate), 4) AS STRING), '|',
              CAST(ROUND(AVG(pay_conv_rate), 6) AS STRING))
FROM music_dw.dws_funnel_daily;
SELECT CONCAT('R|', CAST(ROUND(SUM(net_revenue), 2) AS STRING), '|',
              CAST(ROUND(AVG(arpu), 2) AS STRING), '|',
              CAST(ROUND(AVG(refund_rate), 4) AS STRING), '|',
              CAST(ROUND(AVG(ad_ctr), 4) AS STRING))
FROM music_dw.dws_revenue_daily;
