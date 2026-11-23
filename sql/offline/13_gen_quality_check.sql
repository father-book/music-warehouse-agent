-- ============================================================================
-- 13 · 造数质量诊断（v9 重造前后通用验收）
--
-- 用法：hive --hivevar dt=2026-11-04 -f 13_gen_quality_check.sql
--
-- 检查项目（对应 docs/16 §十 / docs/18 W1 遗留项 / v9 改造）：
--   ① properties 是否还是 {}（v9 前 10 类事件全空）
--   ② 歌曲热度是否服从 Zipf（Top1/Top20 ≈ 6 倍；均匀取模时 ≈ 1.0 倍）
--   ③ keyword / result_cnt / amount / bid_price 是否有区分度
--   ④ D9：weighted_rate 与 avg_song_rate 是否分叉（v9 目标 gap ≈ 0.05）
--   ⑤ 新增私有键能否被 get_json_object 解析（JSON 合法性）
--   ⑥ 零结果率 / SKU 单价 / 出价分档
--   ⑦ DWD 私有列非空率（须在 11 号脚本重跑后执行）
-- ============================================================================
USE music_dw;

-- 试跑分区是 Spark 直写 HDFS 的，Hive 元数据可能没登记；正式分区已存在则无操作
ALTER TABLE music_dw.ods_music_event ADD IF NOT EXISTS PARTITION (dt = '${dt}');

SELECT '=== ① 事件分布与 properties 空值率 ===' AS section;
SELECT event_name,
       COUNT(*)                                                        AS cnt,
       SUM(CASE WHEN properties IS NULL OR properties = '' OR properties = '{}'
                THEN 1 ELSE 0 END)                                       AS empty_cnt,
       COUNT(DISTINCT properties)                                        AS distinct_props
FROM ods_music_event
WHERE dt = '${dt}'
GROUP BY event_name
ORDER BY event_name;

SELECT '=== ② 歌曲热度 Top20（play_start 起播量）===' AS section;
SELECT COALESCE(get_json_object(properties, '$.song_id'), 'NULL') AS song_id,
       COUNT(*) AS starts
FROM ods_music_event
WHERE dt = '${dt}' AND event_name = 'play_start'
GROUP BY COALESCE(get_json_object(properties, '$.song_id'), 'NULL')
ORDER BY starts DESC
LIMIT 20;

SELECT '=== ③ 搜索词 / 结果数 / 金额分布 ===' AS section;
SELECT get_json_object(properties, '$.keyword')                       AS keyword,
       get_json_object(properties, '$.result_cnt')                    AS result_cnt,
       COUNT(*)                                                       AS cnt
FROM ods_music_event
WHERE dt = '${dt}' AND event_name = 'search'
GROUP BY get_json_object(properties, '$.keyword'),
         get_json_object(properties, '$.result_cnt')
ORDER BY cnt DESC
LIMIT 10;

SELECT event_name,
       get_json_object(properties, '$.sku_id')                        AS sku_id,
       get_json_object(properties, '$.amount')                        AS amount,
       COUNT(*)                                                       AS cnt
FROM ods_music_event
WHERE dt = '${dt}'
  AND event_name IN ('vip_subscribe', 'order_pay', 'order_refund')
GROUP BY event_name, get_json_object(properties, '$.sku_id'),
         get_json_object(properties, '$.amount')
ORDER BY event_name, sku_id
LIMIT 20;

SELECT '=== ④ D9：weighted / avg_song 分叉 ===' AS section;
SELECT ROUND(SUM(complete_cnt) / SUM(play_start_cnt), 4)             AS weighted_rate,
       ROUND(AVG(complete_rate), 4)                                  AS avg_song_rate,
       ROUND(AVG(complete_rate) - SUM(complete_cnt) / SUM(play_start_cnt), 4) AS gap,
       COUNT(*)                                                       AS song_rows
FROM dws_song_play_daily
WHERE dt = '${dt}';

SELECT '=== ⑤ 私有键解析（v9 新增字段）===' AS section;
SELECT event_name,
       COUNT(*)                                                       AS cnt,
       COUNT(get_json_object(properties, '$.song_id'))                 AS song_id_ok,
       COUNT(get_json_object(properties, '$.keyword'))                 AS keyword_ok,
       COUNT(get_json_object(properties, '$.result_cnt'))              AS result_cnt_ok,
       COUNT(get_json_object(properties, '$.amount'))                  AS amount_ok,
       COUNT(get_json_object(properties, '$.sku_id'))                  AS sku_ok,
       COUNT(get_json_object(properties, '$.bid_price'))               AS bid_ok,
       COUNT(get_json_object(properties, '$.scene'))                   AS scene_ok,
       COUNT(get_json_object(properties, '$.error_type'))              AS errtype_ok
FROM ods_music_event
WHERE dt = '${dt}'
GROUP BY event_name
ORDER BY event_name;

SELECT '=== ⑥ 零结果率 / 出价分档 ===' AS section;
SELECT COUNT(*)                                                        AS search_pv,
       SUM(CASE WHEN get_json_object(properties, '$.result_cnt') = '0'
                THEN 1 ELSE 0 END)                                      AS zero_pv,
       ROUND(SUM(CASE WHEN get_json_object(properties, '$.result_cnt') = '0'
                      THEN 1 ELSE 0 END) / COUNT(*), 4)                 AS zero_rate,
       COUNT(DISTINCT get_json_object(properties, '$.keyword'))         AS keyword_kinds,
       ROUND(AVG(CAST(get_json_object(properties, '$.result_cnt') AS INT)), 2) AS avg_result_cnt
FROM ods_music_event
WHERE dt = '${dt}' AND event_name = 'search';

SELECT get_json_object(properties, '$.bid_price')                      AS bid_price,
       COUNT(*)                                                        AS cnt
FROM ods_music_event
WHERE dt = '${dt}' AND event_name = 'ad_click'
GROUP BY get_json_object(properties, '$.bid_price')
ORDER BY bid_price;

SELECT '=== ⑦ DWD 私有列非空率（11 号脚本重跑后）===' AS section;
SELECT 'dwd_search'  AS tbl, COUNT(*) AS cnt,
       COUNT(keyword) AS keyword_ok, COUNT(result_cnt) AS result_cnt_ok
FROM dwd_search WHERE dt = '${dt}'
UNION ALL
SELECT 'dwd_pay',     COUNT(*), COUNT(sku_id), COUNT(amount)
FROM dwd_pay WHERE dt = '${dt}'
UNION ALL
SELECT 'dwd_ad',      COUNT(*), COUNT(ad_type), COUNT(bid_price)
FROM dwd_ad WHERE dt = '${dt}'
UNION ALL
SELECT 'dwd_interaction', COUNT(*), COUNT(song_id), COUNT(action)
FROM dwd_interaction WHERE dt = '${dt}'
UNION ALL
SELECT 'dwd_session', COUNT(*), COUNT(launch_type), COUNT(stay_duration)
FROM dwd_session WHERE dt = '${dt}'
UNION ALL
SELECT 'dwd_page_view', COUNT(*), COUNT(page), COUNT(stay_duration)
FROM dwd_page_view WHERE dt = '${dt}'
UNION ALL
SELECT 'dwd_play_control', COUNT(*), COUNT(song_id), COUNT(error_type)
FROM dwd_play_control WHERE dt = '${dt}'
UNION ALL
SELECT 'dwd_impression', COUNT(*), COUNT(scene), COUNT(is_click)
FROM dwd_impression WHERE dt = '${dt}'
UNION ALL
SELECT 'dwd_click', COUNT(*), COUNT(target_type), COUNT(page)
FROM dwd_click WHERE dt = '${dt}'
UNION ALL
SELECT 'dwd_skip', COUNT(*), COUNT(song_id), COUNT(skip_type)
FROM dwd_skip WHERE dt = '${dt}';
