-- =============================================================================
-- 作业 B（DIM 接入版）：DWD 层清洗分流 + HBase 维表点查
--
-- 相对 job_b_dwd.sql 的改动（原版保留作「未接 DIM」对照）：
--   ① src_ods 增加 proc_time AS PROCTIME()，作为 LOOKUP JOIN 的时间属性
--   ② 新增 dim_song_hbase（HBase 维表）与 v_dim（一次 LOOKUP JOIN）
--   ③ 只让 dwd_play_end 这一路写维度列，其余 8 路仍读 v_tagged
--   ④ dwd_play_end 增加 song_name / artist_name / genre / album_name
--
-- 为什么只有 play_end 宽表化：它是最核心的事实表（完播率、切歌率、榜单均基于它），
-- 维度退化到它收益最大；其余明细表保留 song_id 外键按需关联，
-- 避免无差别宽表化带来的存储与重复点查开销。
--
-- 提交前必须完成（否则 CREATE TABLE 找不到 hbase-2.2 factory）：
--   161 上 jar 已放入 flink/lib 且 SQL Gateway 已重启；
--   162 上 HBase 6 张表已建；hbase_dim_load.sql 已执行完毕；
--   Doris 已 ALTER TABLE dwd_play_end ADD COLUMN 四个维度列。
-- =============================================================================

-- =============================================================================
-- 作业 B：DWD 层清洗与分流
--   topic ods_music_event ──┬──→ dwd_play_end          （明细）
--                           ├──→ dwd_play_start        （明细）
--                           ├──→ dwd_skip              （明细）
--                           ├──→ dwd_search            （明细）
--                           ├──→ dwd_collect           （明细）
--                           ├──→ dwd_share             （明细）
--                           ├──→ dwd_page_view         （明细）
--                           ├──→ dwd_quality_error     （异常旁路）
--                           ├──→ topic dwd_play_end    （给作业 C/D 做窗口聚合）
--                           └──→ topic dwd_skip        （同上；见下方「播放终止流」说明）
--
-- 为什么 8 个 sink 放在一个作业里是对的：
--   它们全部来自【同一个 source】、共用【同一套判定规则】，属于「一次判定、
--   多路输出」的分流，不是「多层计算压在一起」。拆成 7 个作业会导致同一份
--   数据被 7 个 consumer group 各消费一遍，且公共规则重复判定 7 次。
--
-- 与批式版的关键差异：
--   批式版要扫两遍 ODS（一遍写异常、一遍写明细），因为 Doris 一条 INSERT 只能
--   写一张表。流式版一次消费、9 路输出，判定只做一次，逻辑严格互补：
--     命中规则 → 异常表；rule_name IS NULL → 对应明细表。
--   v10（11-07）：上游统一为 gen_events.py 的 26 类事件、Kafka 单一入口。
--     白名单扩到 26 类，实时侧尚未建明细表的 15 类走 dwd_unrouted 兜底，
--     既不被误判成 event_name_invalid，也保证「入口 = 明细 + 异常 + 兜底」闭合。
--   这消除了批式版「两套 WHERE 条件要人工保证互补」的隐患。
--
-- 为什么每条数据只记一条规则：
--   CASE 自上而下命中即停。一条数据若同时违反多个规则，只记优先级最高的那个。
--   否则「规则命中数之和」会超过「异常数据条数」，召回率统计会做出 >100% 的错误结论。
-- =============================================================================

SET 'pipeline.name' = 'rt_job_b_dwd';
-- 并行度 3 → 2（Doris 内存约束，见 docs/14 第七轮）：
-- 这个作业要同时写 10 张 Doris 表，**并发 Stream Load 数 = 表数 × 并行度**。
-- 并行度 3 时是 30 路并发，每路都占一份 memtable 与写入缓冲，
-- 实测把 WWC163 的 BE 顶到 mem_limit（3G）后整台拒绝服务，所有扫表查询超时。
-- 降到 2 是 20 路，内存压力直接降三分之一。
-- 代价：峰值吞吐从 400 条/s 降到约 260 条/s（演示只需 200，余量仍然够）。
-- 注意：Kafka 源有 3 个分区，并行度 2 时会有 subtask 消费 2 个分区，
-- 不影响正确性（只是单 subtask 负载不均）。
SET 'parallelism.default' = '2';
-- -----------------------------------------------------------------------------
-- 状态容错：RocksDB + HDFS 持久化 checkpoint
-- -----------------------------------------------------------------------------
-- 之前用默认的堆内存状态后端，checkpoint 只留在 JobManager 堆里，REST 查回来
-- external_path 是 <checkpoint-not-externally-addressable>。含义是：JM 一重启，
-- 全部 checkpoint 蒸发，作业只能从 Kafka 位点从头重来。一个不能从故障恢复到
-- 断点的「实时数仓」不算生产可用。
--
-- 为什么是 RocksDB 而不是 HashMap：
--   堆内存后端把每个 key 的 state 对象都放在 JVM 堆上，Top-N / 去重 / 天级窗口
--   的 state 随数据量线性增长，几百万 key 就会把 GC 拖死；RocksDB 把 state 放在
--   堆外 + 本地盘，堆内只有读缓存，且支持增量 checkpoint —— 只上传变化的 SST，
--   不做全量快照。增量快照要求目标文件系统支持原子 rename，HDFS 满足。
--
-- 为什么必须写 HDFS：本地盘会随 TaskManager 一起消失，等于没持久化。
-- -----------------------------------------------------------------------------
SET 'state.backend.type'                                 = 'rocksdb';
SET 'state.backend.incremental'                          = 'true';
SET 'state.checkpoints.dir'                              = 'hdfs://WWC160:9820/flink/checkpoints';
SET 'execution.checkpointing.interval'                   = '30s';
-- -----------------------------------------------------------------------------
-- Doris sink 批量参数（压测驱动，见 docs/14）
-- -----------------------------------------------------------------------------
-- 原值 max-rows=200 / interval=2s：8 个 sink × 2 并行度 = 16 路并发，每 2 秒提交
-- 一个 200 行的小事务。压测实测 B 的写出上限只有 ~120 行/s，400 条/s 时严重积压。
-- 根因是「批太小、事务太多」：每次 Stream Load 都有 label 创建 + txn 提交 + version
-- 发布三笔固定开销，小批量的有效吞吐被固定开销吃掉。
-- 调整：max-rows 200→5000（一批吃满），interval 2s→3s（少提交、多攒）。
-- 代价：DWD 入仓延迟最多增加约 3s（当前 dwd lag 5~6s，可接受）。
-- 注意：sink.enable-2pc=false 保持不变 —— DWD 表是 UNIQUE KEY，靠主键覆盖幂等，
-- 不需要 2PC（2PC 会让每个 checkpoint 都提交事务，反而放大开销）。
-- -----------------------------------------------------------------------------

SET 'execution.checkpointing.min-pause'                  = '10s';
SET 'execution.checkpointing.timeout'                    = '10min';
SET 'execution.checkpointing.max-concurrent-checkpoints' = '1';
-- 取消作业时保留最后一个 checkpoint：配置变更或版本回滚后能从它恢复，
-- 不必重放整个 Kafka topic。
SET 'execution.checkpointing.externalized-checkpoint-retention' = 'RETAIN_ON_CANCELLATION';
-- 故障自愈：TaskManager 掉线后自动重启，10 次以内不惊动人工。
SET 'restart-strategy.type'                              = 'fixed-delay';
SET 'restart-strategy.fixed-delay.attempts'              = '10';
SET 'restart-strategy.fixed-delay.delay'                 = '10s';
-- JM/TM 未重启，启动 classpath 里没有 hbase connector；
-- pipeline.jars 把 jar 附到 JobGraph，作业时由 BlobServer 分发加载。
SET 'pipeline.jars' = 'file:///opt/module/flink/lib/flink-sql-connector-hbase-2.2-1.17.1.jar';

-- -----------------------------------------------------------------------------
-- 0) 源：ODS topic（原始报文）
-- -----------------------------------------------------------------------------
CREATE TABLE src_ods (
    msg STRING,
    proc_time AS PROCTIME()
) WITH (
    'connector'                    = 'kafka',
    'topic'                        = 'ods_music_event',
    'properties.bootstrap.servers' = 'WWC161:9092',
    'properties.group.id'          = 'rt_job_b_dwd_r3',
    -- 位点策略：group-offsets + auto.offset.reset=latest
    --   首次启动（无提交位点）→ 从最新位置开始，不追历史存量；
    --   之后重启 → 从上次提交的位点继续，既不重复也不丢。
    -- 为什么不用 earliest：层间 topic 里存着历次实验的历史消息，
    -- 从最早读会把已被 TRUNCATE 掉的旧数据重新灌进 DWD，导致对账永远不平。
    'scan.startup.mode'            = 'group-offsets',
    'properties.isolation.level'   = 'read_committed',
    'properties.auto.offset.reset' = 'latest',
    'format'                       = 'raw'
);

-- -----------------------------------------------------------------------------
-- 1) 解析视图：公共属性 + 7 类事件私有属性，每个 JSON 只解析一次
-- -----------------------------------------------------------------------------
CREATE TEMPORARY VIEW v_parsed AS
SELECT
    JSON_VALUE(msg, '$.event_id')                            AS event_id,
    JSON_VALUE(msg, '$.event_name')                          AS event_name,
    JSON_VALUE(msg, '$.event_time')                          AS event_time_ms,
    JSON_VALUE(msg, '$.server_time')                         AS server_time_ms,
    JSON_VALUE(msg, '$.user_id')                             AS user_id,
    JSON_VALUE(msg, '$.device_id')                           AS device_id,
    JSON_VALUE(msg, '$.is_login')                            AS is_login,
    JSON_VALUE(msg, '$.is_vip')                              AS is_vip,
    JSON_VALUE(msg, '$.app_version')                         AS app_version,
    JSON_VALUE(msg, '$.os')                                  AS os,
    JSON_VALUE(msg, '$.os_version')                          AS os_version,
    JSON_VALUE(msg, '$.device_model')                        AS device_model,
    JSON_VALUE(msg, '$.network')                             AS network,
    JSON_VALUE(msg, '$.carrier')                             AS carrier,
    JSON_VALUE(msg, '$.ip')                                  AS ip,
    JSON_VALUE(msg, '$.province')                            AS province,
    JSON_VALUE(msg, '$.city')                                AS city,
    JSON_VALUE(msg, '$.session_id')                          AS session_id,
    JSON_VALUE(msg, '$.session_seq')                         AS session_seq,
    JSON_VALUE(msg, '$.is_foreground')                       AS is_foreground,
    JSON_VALUE(msg, '$.channel')                             AS channel,
    JSON_VALUE(msg, '$.page_id')                             AS page_id,
    JSON_QUERY(msg, '$.properties')                          AS properties,

    -- 时间转换：FROM_UNIXTIME 返回 'yyyy-MM-dd HH:mm:ss' 字符串，
    -- 再 TO_TIMESTAMP 转 TIMESTAMP(3)。比 TO_TIMESTAMP_LTZ 更少时区歧义。
    TO_TIMESTAMP(FROM_UNIXTIME(CAST(JSON_VALUE(msg, '$.event_time')  AS BIGINT) / 1000)) AS event_ts,
    TO_TIMESTAMP(FROM_UNIXTIME(CAST(JSON_VALUE(msg, '$.server_time') AS BIGINT) / 1000)) AS server_ts,

    -- play_end
    JSON_VALUE(msg, '$.properties.song_id')                  AS p_song_id,
    CAST(JSON_VALUE(msg, '$.properties.song_duration') AS INT)    AS p_song_duration,
    CAST(JSON_VALUE(msg, '$.properties.play_duration') AS INT)    AS p_play_duration,
    CAST(JSON_VALUE(msg, '$.properties.play_progress') AS DOUBLE) AS p_play_progress,
    JSON_VALUE(msg, '$.properties.end_reason')               AS p_end_reason,
    -- play_start
    JSON_VALUE(msg, '$.properties.playlist_id')              AS p_playlist_id,
    JSON_VALUE(msg, '$.properties.quality')                  AS p_quality,
    JSON_VALUE(msg, '$.properties.play_mode')                AS p_play_mode,
    CAST(JSON_VALUE(msg, '$.properties.song_index') AS INT)  AS p_song_index,
    -- skip
    CAST(JSON_VALUE(msg, '$.properties.played_duration') AS INT)  AS p_played_duration,
    JSON_VALUE(msg, '$.properties.skip_type')                AS p_skip_type,
    -- skip 的歌曲总时长走 $.properties.duration（不是 song_duration）：
    -- 这是 03 号离线脚本定义的老键名，造数器沿用它写 skip 事件。
    -- 用途只有一个 —— 给作业 C/D 的播放终止流算 skip 行的 play_progress，
    -- 让「切歌时的播放进度」和 play_end 的 play_progress 在同一量纲上。
    CAST(JSON_VALUE(msg, '$.properties.duration') AS INT)    AS p_skip_song_duration,
    -- search
    JSON_VALUE(msg, '$.properties.keyword')                  AS p_keyword,
    JSON_VALUE(msg, '$.properties.search_type')              AS p_search_type,
    CAST(JSON_VALUE(msg, '$.properties.result_count') AS INT)     AS p_result_count,
    JSON_VALUE(msg, '$.properties.clicked_song_id')          AS p_clicked_song_id,
    -- collect
    JSON_VALUE(msg, '$.properties.collect_type')             AS p_collect_type,
    -- share
    JSON_VALUE(msg, '$.properties.share_channel')            AS p_share_channel,
    -- 商业化：订阅 / 购买 / 退款（金额来自 SKU 单价，见生成器 gen_music_data.SKUS）
    JSON_VALUE(msg, '$.properties.sku_id')                   AS p_sku_id,
    CAST(JSON_VALUE(msg, '$.properties.amount') AS DOUBLE)   AS p_amount,
    JSON_VALUE(msg, '$.properties.order_id')                 AS p_order_id,
    JSON_VALUE(msg, '$.properties.pay_channel')              AS p_pay_channel,
    CAST(JSON_VALUE(msg, '$.properties.plan_days') AS INT)   AS p_plan_days,
    JSON_VALUE(msg, '$.properties.refund_reason')            AS p_refund_reason,
    -- 客户端质量：播放失败
    JSON_VALUE(msg, '$.properties.error_code')               AS p_error_code,
    JSON_VALUE(msg, '$.properties.error_msg')                AS p_error_msg,
    CAST(JSON_VALUE(msg, '$.properties.play_position') AS INT) AS p_play_position,
    -- page_view
    JSON_VALUE(msg, '$.properties.page_id')                  AS p_view_page_id,
    JSON_VALUE(msg, '$.properties.referrer_page')            AS p_referrer_page,
    CAST(JSON_VALUE(msg, '$.properties.stay_duration') AS INT)    AS p_stay_duration,
    proc_time
FROM src_ods;

-- -----------------------------------------------------------------------------
-- 2) 判定视图：一条数据只产出一个 rule_name（优先级自上而下）
-- -----------------------------------------------------------------------------
-- 优先级设计原则：
--   ① 结构性缺陷（事件名、主键）优先于业务属性缺陷
--   ② 公共规则优先于事件私有规则
--   ③ 时间规则优先于属性规则（时间不可信时，其余属性无从判断）
-- -----------------------------------------------------------------------------
CREATE TEMPORARY VIEW v_tagged AS
SELECT
    *,
    CASE
        -- ① 事件名不在白名单：无法归属任何明细表
        WHEN event_name IS NULL
          OR event_name NOT IN (
                -- 实时侧已建明细表的 11 类
                'play_start','play_end','skip','search','collect','share','page_view',
                'vip_subscribe','order_pay','order_refund','play_error',
                -- v10：离线 26 类里实时暂不建明细表的 15 类，走兜底表
                'impression','click','ad_impression','ad_click','search_click',
                'pause','resume','seek','like','comment','download','follow_artist',
                'playlist_add','app_launch','app_exit')
            THEN 'event_name_invalid'

        -- ①.1 商业化事件私有规则
        -- 订单是「钱」的数据，校验比行为数据更严：没有订单号或金额非正，
        -- 直接进异常表，不允许进 dwd_order —— 否则收入会被一条脏数据带偏，
        -- 而且对账时「Σ订单金额」和「Σ订单数×单价」会莫名其妙对不上。
        WHEN event_name IN ('vip_subscribe','order_pay','order_refund')
             AND COALESCE(p_order_id, '') = ''
            THEN 'order_id_missing'
        WHEN event_name IN ('vip_subscribe','order_pay','order_refund')
             AND (p_amount IS NULL OR p_amount <= 0)
            THEN 'order_amount_invalid'
        WHEN event_name = 'play_error' AND COALESCE(p_error_code, '') = ''
            THEN 'error_code_missing'

        -- ② 公共规则（7 类事件通用）
        WHEN COALESCE(event_id, '') = ''
            THEN 'event_id_missing'
        WHEN COALESCE(device_id, '') = ''
            THEN 'device_id_missing'
        WHEN is_login = 'true' AND COALESCE(user_id, '') = ''
            THEN 'user_id_missing_when_login'
        WHEN event_ts IS NULL
            THEN 'event_time_missing'
        WHEN CAST(event_time_ms AS BIGINT) > UNIX_TIMESTAMP() * 1000 + 300000
            THEN 'event_time_future'
        WHEN CAST(event_time_ms AS BIGINT) < 1700000000000
            THEN 'event_time_too_old'

        -- ③ play_end 私有规则
        WHEN event_name = 'play_end' AND COALESCE(p_song_id, '') = ''
            THEN 'song_id_missing'
        WHEN event_name = 'play_end' AND p_song_duration IS NULL
            THEN 'song_duration_missing'
        WHEN event_name = 'play_end' AND p_play_duration IS NULL
            THEN 'play_duration_missing'
        WHEN event_name = 'play_end' AND p_play_duration > p_song_duration
            THEN 'duration_exceed'
        WHEN event_name = 'play_end' AND (p_play_progress IS NULL OR p_play_progress < 0 OR p_play_progress > 1)
            THEN 'progress_invalid'
        WHEN event_name = 'play_end'
          AND (p_end_reason IS NULL OR p_end_reason NOT IN ('natural','skip','pause','exit','interrupt'))
            THEN 'end_reason_invalid'

        -- ④ play_start 私有规则
        WHEN event_name = 'play_start' AND COALESCE(p_song_id, '') = ''
            THEN 'song_id_missing'

        -- ⑤ skip 私有规则
        WHEN event_name = 'skip' AND COALESCE(p_song_id, '') = ''
            THEN 'song_id_missing'
        WHEN event_name = 'skip' AND p_played_duration IS NULL
            THEN 'played_duration_missing'
        WHEN event_name = 'skip' AND p_played_duration < 0
            THEN 'played_duration_negative'

        -- ⑥ search 私有规则
        WHEN event_name = 'search' AND COALESCE(p_keyword, '') = ''
            THEN 'keyword_missing'
        WHEN event_name = 'search' AND p_result_count < 0
            THEN 'result_count_negative'

        -- ⑦ collect 私有规则
        WHEN event_name = 'collect' AND COALESCE(p_song_id, '') = ''
            THEN 'song_id_missing'
        WHEN event_name = 'collect' AND COALESCE(p_collect_type, '') = ''
            THEN 'collect_type_missing'
        WHEN event_name = 'collect' AND p_collect_type NOT IN ('add','cancel')
            THEN 'collect_type_invalid'

        -- ⑧ share 私有规则
        WHEN event_name = 'share' AND COALESCE(p_song_id, '') = ''
            THEN 'song_id_missing'
        WHEN event_name = 'share' AND COALESCE(p_share_channel, '') = ''
            THEN 'share_channel_missing'

        -- ⑨ page_view 私有规则
        WHEN event_name = 'page_view' AND COALESCE(p_view_page_id, '') = ''
            THEN 'view_page_id_missing'
        WHEN event_name = 'page_view' AND p_stay_duration < 0
            THEN 'stay_duration_negative'

        ELSE NULL
    END AS rule_name,
    CASE
        WHEN event_name IS NULL
          OR event_name NOT IN ('play_start','play_end','skip','search','collect','share','page_view')
            THEN CONCAT('event_name=', COALESCE(event_name, 'NULL'))
        WHEN COALESCE(event_id, '') = ''
            THEN 'event_id 为空'
        WHEN COALESCE(device_id, '') = ''
            THEN 'device_id 为空（设备不可识别）'
        WHEN is_login = 'true' AND COALESCE(user_id, '') = ''
            THEN 'is_login=true 但 user_id 为空（登录态矛盾）'
        WHEN event_ts IS NULL
            THEN CONCAT('event_time=', COALESCE(event_time_ms, 'NULL'), ' 无法转为时间戳')
        WHEN CAST(event_time_ms AS BIGINT) > UNIX_TIMESTAMP() * 1000 + 300000
            THEN CONCAT('event_time 超前 5 分钟以上: ', event_time_ms)
        WHEN CAST(event_time_ms AS BIGINT) < 1700000000000
            THEN CONCAT('event_time 早于 2023: ', event_time_ms)
        WHEN event_name = 'play_end' AND COALESCE(p_song_id, '') = ''
            THEN 'properties.song_id 为空'
        WHEN event_name = 'play_end' AND p_song_duration IS NULL
            THEN 'properties.song_duration 缺失'
        WHEN event_name = 'play_end' AND p_play_duration IS NULL
            THEN 'properties.play_duration 缺失'
        WHEN event_name = 'play_end' AND p_play_duration > p_song_duration
            THEN CONCAT('play_duration=', CAST(p_play_duration AS STRING),
                        ' > song_duration=', CAST(p_song_duration AS STRING))
        WHEN event_name = 'play_end' AND (p_play_progress IS NULL OR p_play_progress < 0 OR p_play_progress > 1)
            THEN CONCAT('play_progress=', COALESCE(CAST(p_play_progress AS STRING), 'NULL'))
        WHEN event_name = 'play_end'
          AND (p_end_reason IS NULL OR p_end_reason NOT IN ('natural','skip','pause','exit','interrupt'))
            THEN CONCAT('end_reason=', COALESCE(p_end_reason, 'NULL'))
        WHEN event_name = 'play_start' AND COALESCE(p_song_id, '') = ''
            THEN 'properties.song_id 为空'
        WHEN event_name = 'skip' AND COALESCE(p_song_id, '') = ''
            THEN 'properties.song_id 为空'
        WHEN event_name = 'skip' AND p_played_duration IS NULL
            THEN 'properties.played_duration 缺失'
        WHEN event_name = 'skip' AND p_played_duration < 0
            THEN CONCAT('played_duration=', CAST(p_played_duration AS STRING))
        WHEN event_name = 'search' AND COALESCE(p_keyword, '') = ''
            THEN 'properties.keyword 为空'
        WHEN event_name = 'search' AND p_result_count < 0
            THEN CONCAT('result_count=', CAST(p_result_count AS STRING))
        WHEN event_name = 'collect' AND COALESCE(p_song_id, '') = ''
            THEN 'properties.song_id 为空'
        WHEN event_name = 'collect' AND COALESCE(p_collect_type, '') = ''
            THEN 'properties.collect_type 缺失'
        WHEN event_name = 'collect' AND p_collect_type NOT IN ('add','cancel')
            THEN CONCAT('collect_type=', p_collect_type)
        WHEN event_name = 'share' AND COALESCE(p_song_id, '') = ''
            THEN 'properties.song_id 为空'
        WHEN event_name = 'share' AND COALESCE(p_share_channel, '') = ''
            THEN 'properties.share_channel 缺失'
        WHEN event_name = 'page_view' AND COALESCE(p_view_page_id, '') = ''
            THEN 'properties.page_id 为空'
        WHEN event_name = 'page_view' AND p_stay_duration < 0
            THEN CONCAT('stay_duration=', CAST(p_stay_duration AS STRING))
        ELSE ''
    END AS detail
FROM v_parsed;

-- -----------------------------------------------------------------------------
-- 2.5) DIM 层：HBase 歌曲维表 + LOOKUP JOIN
-- -----------------------------------------------------------------------------
-- 为什么用 LOOKUP JOIN 而不是 Java UDF：
--   flink-sql-connector-hbase-2.2-1.17.1 已实现 LookupTableSource
--   （HBaseDynamicTableSource#getLookupRuntimeProvider，同步/异步两种实现），
--   「按 rowkey 点查 HBase」本身就是纯 SQL 能力，不必自写 ScalarFunction。
--   ADR-007 早期「connector 不支持 Lookup」的查证结论已被推翻。
--
-- 只让 play_end 这一路走点查，其余 8 路仍读 v_tagged。
--   若把 v_dim 铺到所有 INSERT，同一条事件会被重复点查 9 次。
--
-- RowKey 必须与 hbase_dim_load.sql 的装载表达式逐字一致：
--   CONCAT(SUBSTRING(MD5(id), 1, 2), '_', id)
--   COALESCE 把 song_id 为空的事件兜成空串，避免 NULL 作为 lookup key。
--
-- 缓存：PARTIAL 缓存，压测驱动开启（docs/14）。
--   不开缓存时每条流记录都发一次 HBase Get RPC。400 条/s 入口 = 400 次 RPC/s，
--   HBase 单 RegionServer 在同机混布（Kafka/ZK/HDFS DN）的情况下被同步 RPC 打满，
--   B 的写出能力被钉死在 ~65 行/s —— 这是压测定位到的第一瓶颈，比 Doris flush 参数更根本。
--   维表只有 1486 行、几乎不变，缓存命中率接近 100%：max-rows 给足 20 万（留未来扩容），
--   ttl 10 分钟（维度变更最多 10 分钟后生效，对维表足够）。
--   注意：HBase 只允许按「单一 rowkey」lookup，不能按其它列查。
-- -----------------------------------------------------------------------------
CREATE TABLE dim_song_hbase (
    rowkey STRING,
    cf ROW<song_id STRING, song_name STRING, artist_id STRING, artist_name STRING,
          artist_country STRING, album_id STRING, album_name STRING,
          genre STRING, duration STRING, release_date STRING, mbid STRING>,
    PRIMARY KEY (rowkey) NOT ENFORCED
) WITH (
    'connector'              = 'hbase-2.2',
    'table-name'             = 'dim_song',
    'zookeeper.quorum'       = 'WWC161:2181,WWC162:2181,WWC163:2181',
    'zookeeper.znode.parent'  = '/hbase',
    'lookup.cache.max-rows'   = '200000',
    'lookup.cache.ttl'        = '10 min'
);

CREATE TEMPORARY VIEW v_dim AS
SELECT
    t.*,
    COALESCE(d.cf.song_name,   '') AS dim_song_name,
    COALESCE(d.cf.artist_name, '') AS dim_artist_name,
    COALESCE(d.cf.genre,       '') AS dim_genre,
    COALESCE(d.cf.album_name,  '') AS dim_album_name
FROM v_tagged AS t
LEFT JOIN dim_song_hbase FOR SYSTEM_TIME AS OF t.proc_time AS d
ON CONCAT(SUBSTRING(MD5(COALESCE(t.p_song_id, '')), 1, 2), '_',
          COALESCE(t.p_song_id, '')) = d.rowkey;

-- =============================================================================
-- 3) 九个 sink
-- =============================================================================

-- 3.0 v10 兜底表：白名单内但没有明细表的合法事件，原文保真落地
CREATE TABLE sink_unrouted (
    event_id       STRING,
    event_name     STRING,
    event_ts       TIMESTAMP(3),   -- 与其余 DWD 同型：事件时间统一落 DATETIME
    server_ts      TIMESTAMP(3),   -- 客户端上报时间，用于排查时钟漂移
    user_id        STRING,
    device_id      STRING,
    properties     STRING,         -- 私有属性 JSON 原文，保真不丢
    etl_time       TIMESTAMP(3),
    dt             DATE
) WITH (
    'connector'                         = 'doris',
    'fenodes'                           = 'WWC163:8030',
    'table.identifier'                  = 'music_dw.dwd_unrouted',
    'username'                          = 'root',
    'password'                          = '',
    'sink.label-prefix'                 = 'job_b_unrouted',
    'sink.properties.format'            = 'json',
    'sink.properties.read_json_by_line' = 'true',
    'sink.enable-2pc'                   = 'false',
    'sink.buffer-flush.max-rows'        = '5000',
    'sink.buffer-flush.interval'        = '3s'
);

-- 3.1 异常表
CREATE TABLE sink_err (
    event_id       STRING,
    rule_name      STRING,
    event_name     STRING,
    rule_level     STRING,
    detail         STRING,
    song_id        STRING,
    user_id        STRING,
    device_id      STRING,
    event_ts       TIMESTAMP(3),
    raw_properties STRING,
    etl_time       TIMESTAMP(3),
    dt             DATE
) WITH (
    'connector'                         = 'doris',
    'fenodes'                           = 'WWC163:8030',
    'table.identifier'                  = 'music_dw.dwd_quality_error',
    'username'                          = 'root',
    'password'                          = '',
    'sink.label-prefix'                 = 'job_b_err',
    'sink.properties.format'            = 'json',
    'sink.properties.read_json_by_line' = 'true',
    'sink.enable-2pc'                   = 'false',
    'sink.buffer-flush.max-rows'        = '5000',
    'sink.buffer-flush.interval'        = '3s'
);

-- 3.2 play_end 明细
CREATE TABLE sink_dwd_play_end (
    event_id STRING, event_ts TIMESTAMP(3), server_ts TIMESTAMP(3),
    user_id STRING, device_id STRING, is_login STRING, is_vip STRING,
    app_version STRING, os STRING, os_version STRING, device_model STRING,
    network STRING, carrier STRING, ip STRING, province STRING, city STRING,
    session_id STRING, session_seq STRING, is_foreground STRING, channel STRING,
    page_id STRING, song_id STRING, song_duration INT, play_duration INT,
    play_progress DOUBLE, end_reason STRING, etl_time TIMESTAMP(3), dt DATE,
    song_name STRING, artist_name STRING, genre STRING, album_name STRING
) WITH (
    'connector'                         = 'doris',
    'fenodes'                           = 'WWC163:8030',
    'table.identifier'                  = 'music_dw.dwd_play_end',
    'username'                          = 'root',
    'password'                          = '',
    'sink.label-prefix'                 = 'job_b_play_end',
    'sink.properties.format'            = 'json',
    'sink.properties.read_json_by_line' = 'true',
    'sink.enable-2pc'                   = 'false',
    'sink.buffer-flush.max-rows'        = '5000',
    'sink.buffer-flush.interval'        = '3s'
);

-- 3.3 play_start 明细
CREATE TABLE sink_dwd_play_start (
    event_id STRING, event_ts TIMESTAMP(3), server_ts TIMESTAMP(3),
    user_id STRING, device_id STRING, is_login STRING, is_vip STRING,
    app_version STRING, os STRING, os_version STRING, device_model STRING,
    network STRING, carrier STRING, ip STRING, province STRING, city STRING,
    session_id STRING, session_seq STRING, is_foreground STRING, channel STRING,
    page_id STRING, song_id STRING, playlist_id STRING, quality STRING,
    play_mode STRING, song_index INT, etl_time TIMESTAMP(3), dt DATE
) WITH (
    'connector' = 'doris', 'fenodes' = 'WWC163:8030',
    'table.identifier' = 'music_dw.dwd_play_start', 'username' = 'root', 'password' = '',
    'sink.label-prefix' = 'job_b_play_start',
    'sink.properties.format' = 'json', 'sink.properties.read_json_by_line' = 'true',
    'sink.enable-2pc' = 'false',
    'sink.buffer-flush.max-rows' = '5000', 'sink.buffer-flush.interval' = '3s'
);

-- 3.4 skip 明细
CREATE TABLE sink_dwd_skip (
    event_id STRING, event_ts TIMESTAMP(3), server_ts TIMESTAMP(3),
    user_id STRING, device_id STRING, is_login STRING, is_vip STRING,
    app_version STRING, os STRING, os_version STRING, device_model STRING,
    network STRING, carrier STRING, ip STRING, province STRING, city STRING,
    session_id STRING, session_seq STRING, is_foreground STRING, channel STRING,
    page_id STRING, song_id STRING, played_duration INT, skip_type STRING,
    etl_time TIMESTAMP(3), dt DATE
) WITH (
    'connector' = 'doris', 'fenodes' = 'WWC163:8030',
    'table.identifier' = 'music_dw.dwd_skip', 'username' = 'root', 'password' = '',
    'sink.label-prefix' = 'job_b_skip',
    'sink.properties.format' = 'json', 'sink.properties.read_json_by_line' = 'true',
    'sink.enable-2pc' = 'false',
    'sink.buffer-flush.max-rows' = '5000', 'sink.buffer-flush.interval' = '3s'
);

-- 3.5 search 明细
CREATE TABLE sink_dwd_search (
    event_id STRING, event_ts TIMESTAMP(3), server_ts TIMESTAMP(3),
    user_id STRING, device_id STRING, is_login STRING, is_vip STRING,
    app_version STRING, os STRING, os_version STRING, device_model STRING,
    network STRING, carrier STRING, ip STRING, province STRING, city STRING,
    session_id STRING, session_seq STRING, is_foreground STRING, channel STRING,
    page_id STRING, keyword STRING, search_type STRING, result_count INT,
    clicked_song_id STRING, etl_time TIMESTAMP(3), dt DATE
) WITH (
    'connector' = 'doris', 'fenodes' = 'WWC163:8030',
    'table.identifier' = 'music_dw.dwd_search', 'username' = 'root', 'password' = '',
    'sink.label-prefix' = 'job_b_search',
    'sink.properties.format' = 'json', 'sink.properties.read_json_by_line' = 'true',
    'sink.enable-2pc' = 'false',
    'sink.buffer-flush.max-rows' = '5000', 'sink.buffer-flush.interval' = '3s'
);

-- 3.6 collect 明细
CREATE TABLE sink_dwd_collect (
    event_id STRING, event_ts TIMESTAMP(3), server_ts TIMESTAMP(3),
    user_id STRING, device_id STRING, is_login STRING, is_vip STRING,
    app_version STRING, os STRING, os_version STRING, device_model STRING,
    network STRING, carrier STRING, ip STRING, province STRING, city STRING,
    session_id STRING, session_seq STRING, is_foreground STRING, channel STRING,
    page_id STRING, song_id STRING, collect_type STRING, etl_time TIMESTAMP(3), dt DATE
) WITH (
    'connector' = 'doris', 'fenodes' = 'WWC163:8030',
    'table.identifier' = 'music_dw.dwd_collect', 'username' = 'root', 'password' = '',
    'sink.label-prefix' = 'job_b_collect',
    'sink.properties.format' = 'json', 'sink.properties.read_json_by_line' = 'true',
    'sink.enable-2pc' = 'false',
    'sink.buffer-flush.max-rows' = '5000', 'sink.buffer-flush.interval' = '3s'
);

-- 3.7 share 明细
CREATE TABLE sink_dwd_share (
    event_id STRING, event_ts TIMESTAMP(3), server_ts TIMESTAMP(3),
    user_id STRING, device_id STRING, is_login STRING, is_vip STRING,
    app_version STRING, os STRING, os_version STRING, device_model STRING,
    network STRING, carrier STRING, ip STRING, province STRING, city STRING,
    session_id STRING, session_seq STRING, is_foreground STRING, channel STRING,
    page_id STRING, song_id STRING, share_channel STRING, etl_time TIMESTAMP(3), dt DATE
) WITH (
    'connector' = 'doris', 'fenodes' = 'WWC163:8030',
    'table.identifier' = 'music_dw.dwd_share', 'username' = 'root', 'password' = '',
    'sink.label-prefix' = 'job_b_share',
    'sink.properties.format' = 'json', 'sink.properties.read_json_by_line' = 'true',
    'sink.enable-2pc' = 'false',
    'sink.buffer-flush.max-rows' = '5000', 'sink.buffer-flush.interval' = '3s'
);

-- 3.8 page_view 明细
CREATE TABLE sink_dwd_page_view (
    event_id STRING, event_ts TIMESTAMP(3), server_ts TIMESTAMP(3),
    user_id STRING, device_id STRING, is_login STRING, is_vip STRING,
    app_version STRING, os STRING, os_version STRING, device_model STRING,
    network STRING, carrier STRING, ip STRING, province STRING, city STRING,
    session_id STRING, session_seq STRING, is_foreground STRING, channel STRING,
    page_id STRING, view_page_id STRING, referrer_page STRING, stay_duration INT,
    etl_time TIMESTAMP(3), dt DATE
) WITH (
    'connector' = 'doris', 'fenodes' = 'WWC163:8030',
    'table.identifier' = 'music_dw.dwd_page_view', 'username' = 'root', 'password' = '',
    'sink.label-prefix' = 'job_b_page_view',
    'sink.properties.format' = 'json', 'sink.properties.read_json_by_line' = 'true',
    'sink.enable-2pc' = 'false',
    'sink.buffer-flush.max-rows' = '5000', 'sink.buffer-flush.interval' = '3s'
);

-- 3.9 商业化订单明细（订阅 / 单曲购买 / 退款）
CREATE TABLE sink_dwd_order (
    event_id STRING, event_ts TIMESTAMP(3), server_ts TIMESTAMP(3),
    user_id STRING, device_id STRING, is_login STRING, is_vip STRING,
    app_version STRING, os STRING, os_version STRING, device_model STRING,
    network STRING, carrier STRING, ip STRING, province STRING, city STRING,
    session_id STRING, session_seq STRING, is_foreground STRING, channel STRING,
    page_id STRING,
    event_name STRING, order_id STRING, sku_id STRING, amount DOUBLE,
    pay_channel STRING, plan_days INT, refund_reason STRING, song_id STRING,
    etl_time TIMESTAMP(3), dt DATE
) WITH (
    'connector' = 'doris', 'fenodes' = 'WWC163:8030',
    'table.identifier' = 'music_dw.dwd_order', 'username' = 'root', 'password' = '',
    'sink.label-prefix' = 'job_b_order',
    'sink.properties.format' = 'json', 'sink.properties.read_json_by_line' = 'true',
    'sink.enable-2pc' = 'false',
    'sink.buffer-flush.max-rows' = '5000', 'sink.buffer-flush.interval' = '3s'
);

-- 3.10 客户端播放失败明细
CREATE TABLE sink_dwd_play_error (
    event_id STRING, event_ts TIMESTAMP(3), server_ts TIMESTAMP(3),
    user_id STRING, device_id STRING, is_login STRING, is_vip STRING,
    app_version STRING, os STRING, os_version STRING, device_model STRING,
    network STRING, carrier STRING, ip STRING, province STRING, city STRING,
    session_id STRING, session_seq STRING, is_foreground STRING, channel STRING,
    page_id STRING, song_id STRING,
    error_code STRING, error_msg STRING, play_position INT, network_at_error STRING,
    etl_time TIMESTAMP(3), dt DATE
) WITH (
    'connector' = 'doris', 'fenodes' = 'WWC163:8030',
    'table.identifier' = 'music_dw.dwd_play_error', 'username' = 'root', 'password' = '',
    'sink.label-prefix' = 'job_b_play_error',
    'sink.properties.format' = 'json', 'sink.properties.read_json_by_line' = 'true',
    'sink.enable-2pc' = 'false',
    'sink.buffer-flush.max-rows' = '5000', 'sink.buffer-flush.interval' = '3s'
);

-- 3.11 DWD topic：把清洗后的 play_end 送给作业 C
-- 时间列转成 STRING 再输出，避免 JSON 序列化对 TIMESTAMP 格式的解释差异。
-- 列名必须与 job_c_dws.sql 的 src_dwd_play_end 完全一致（json format 按 key 匹配，
-- 名字差一个字符就读成 NULL）。所以这里用 _str 后缀，作业 C 再用计算列转回 TIMESTAMP。
CREATE TABLE sink_topic_play_end (
    event_id STRING, event_ts_str STRING, server_ts_str STRING,
    user_id STRING, device_id STRING, is_login STRING, is_vip STRING,
    app_version STRING, os STRING, os_version STRING, device_model STRING,
    network STRING, carrier STRING, province STRING, city STRING,
    session_id STRING, channel STRING, page_id STRING,
    song_id STRING, song_duration INT, play_duration INT,
    play_progress DOUBLE, end_reason STRING, etl_time_str STRING
) WITH (
    'connector'                    = 'kafka',
    'topic'                        = 'dwd_play_end',
    'properties.bootstrap.servers' = 'WWC161:9092',
    'format'                       = 'json',
    'sink.partitioner'             = 'default',
    -- ---------------------------------------------------------------------
    -- 端到端精确一次（与作业 A 的 ODS sink 对齐）
    -- ---------------------------------------------------------------------
    -- 原来这里是默认的 at-least-once：B 同时写 Doris(8 张表) 和 Kafka(dwd topic)，
    -- 故障恢复时两边各自重放，Doris 靠 UNIQUE KEY 幂等、Kafka 却会多出几条 ——
    -- 这就是「双写漂移」：报表读 Doris 与下游作业读 Kafka，口径可能差几行。
    -- 加上 2PC 后，Kafka 的写入也只在 checkpoint 完成时才提交。
    -- 注意 transaction.timeout.ms 必须 <= broker 的 transaction.max.timeout.ms
    -- （默认 900000=15min），配大了 InitProducerId 直接失败、数据静默丢失 —— A 踩过。
    'sink.delivery-guarantee'      = 'exactly-once',
    'sink.transactional-id-prefix' = 'job_b_dwd_play_end',
    'properties.transaction.timeout.ms' = '600000'
);

-- 3.12 DWD topic：把 skip 也送给作业 C/D
--
-- 【为什么必须再开一路 topic，而不是把 skip 混进 dwd_play_end】
--   会话模型里，一次起播（play_start）只有两种结局：正常结束（play_end）或切歌（skip）。
--   config/event_weights.json 的权重就是这样配的：play_start 5 条/会话 = play_end 3.5
--   + skip 1.5，并明确写了 skip_rate = skip/(play_end+skip) = 1.5/5 = 30%。
--
--   但作业 C/D 原来只消费 dwd_play_end，`skip_cnt` 又用 `end_reason='skip'` 去数 ——
--   而 play_end 的 end_reason 只会是 natural/exit。两头对不上，切歌率必然恒为 0，
--   完播率也被抬成「73.3%（natural / play_end）」而不是设计值 51.3%（natural / 起播）。
--
--   修法是让 DWS 看到完整的「播放终止流」：play_end ∪ skip。两路各自保持
--   与自己的 Doris 明细表一一对应（topic↔table 同源同过滤条件，对账才有意义），
--   合并动作放在作业 C/D 的 view 里做。
--
--   列只挑聚合用得上的：终止流真正需要的维度是 event_id / 时间 / 歌曲 / 地域 / 设备，
--   skip 特有的 skip_type 只做保留，不参与窗口计算。
CREATE TABLE sink_topic_skip (
    event_id STRING, event_ts_str STRING,
    user_id STRING, device_id STRING, province STRING, song_id STRING,
    played_duration INT, song_duration INT, play_progress DOUBLE,
    skip_type STRING, etl_time_str STRING
) WITH (
    'connector'                    = 'kafka',
    'topic'                        = 'dwd_skip',
    'properties.bootstrap.servers' = 'WWC161:9092',
    'format'                       = 'json',
    'sink.partitioner'             = 'default',
    'sink.delivery-guarantee'      = 'exactly-once',
    'sink.transactional-id-prefix' = 'job_b_dwd_skip',
    'properties.transaction.timeout.ms' = '600000'
);

-- =============================================================================
-- 4) 九路写出：判定一次，出口九个
-- =============================================================================
EXECUTE STATEMENT SET
BEGIN

    -- 异常旁路
    INSERT INTO sink_err
    SELECT
        event_id, rule_name, event_name, 'block', detail,
        p_song_id, user_id, device_id, event_ts, properties,
        LOCALTIMESTAMP, CAST(LOCALTIMESTAMP AS DATE)
    FROM v_tagged
    WHERE rule_name IS NOT NULL;

    -- v10 兜底：26 类白名单中实时侧暂未建明细表的 15 类合法事件，原文落地。
    -- 不是"补实时指标"，而是不让合法埋点被判脏、并让对账恒等式闭合。
    INSERT INTO sink_unrouted
    SELECT
        event_id, event_name, event_ts, server_ts, user_id, device_id,
        properties, LOCALTIMESTAMP, CAST(LOCALTIMESTAMP AS DATE)
    FROM v_tagged
    WHERE rule_name IS NULL
      AND event_name NOT IN ('play_start','play_end','skip','search','collect','share','page_view',
                             'vip_subscribe','order_pay','order_refund','play_error');

    -- play_end：唯一走 HBase 点查的一路，把维度退化进 DWD 宽表
    INSERT INTO sink_dwd_play_end
    SELECT
        event_id, event_ts, server_ts, user_id, device_id, is_login, is_vip,
        app_version, os, os_version, device_model, network, carrier, ip, province, city,
        session_id, session_seq, is_foreground, channel, page_id,
        p_song_id, p_song_duration, p_play_duration, p_play_progress, p_end_reason,
        LOCALTIMESTAMP, CAST(event_ts AS DATE),
        dim_song_name, dim_artist_name, dim_genre, dim_album_name
    FROM v_dim
    WHERE rule_name IS NULL AND event_name = 'play_end';

    -- play_start
    INSERT INTO sink_dwd_play_start
    SELECT
        event_id, event_ts, server_ts, user_id, device_id, is_login, is_vip,
        app_version, os, os_version, device_model, network, carrier, ip, province, city,
        session_id, session_seq, is_foreground, channel, page_id,
        p_song_id, p_playlist_id, p_quality, p_play_mode, p_song_index,
        LOCALTIMESTAMP, CAST(event_ts AS DATE)
    FROM v_tagged
    WHERE rule_name IS NULL AND event_name = 'play_start';

    -- skip
    INSERT INTO sink_dwd_skip
    SELECT
        event_id, event_ts, server_ts, user_id, device_id, is_login, is_vip,
        app_version, os, os_version, device_model, network, carrier, ip, province, city,
        session_id, session_seq, is_foreground, channel, page_id,
        p_song_id, p_played_duration, p_skip_type,
        LOCALTIMESTAMP, CAST(event_ts AS DATE)
    FROM v_tagged
    WHERE rule_name IS NULL AND event_name = 'skip';

    -- search
    INSERT INTO sink_dwd_search
    SELECT
        event_id, event_ts, server_ts, user_id, device_id, is_login, is_vip,
        app_version, os, os_version, device_model, network, carrier, ip, province, city,
        session_id, session_seq, is_foreground, channel, page_id,
        p_keyword, p_search_type, p_result_count, p_clicked_song_id,
        LOCALTIMESTAMP, CAST(event_ts AS DATE)
    FROM v_tagged
    WHERE rule_name IS NULL AND event_name = 'search';

    -- collect
    INSERT INTO sink_dwd_collect
    SELECT
        event_id, event_ts, server_ts, user_id, device_id, is_login, is_vip,
        app_version, os, os_version, device_model, network, carrier, ip, province, city,
        session_id, session_seq, is_foreground, channel, page_id,
        p_song_id, p_collect_type,
        LOCALTIMESTAMP, CAST(event_ts AS DATE)
    FROM v_tagged
    WHERE rule_name IS NULL AND event_name = 'collect';

    -- share
    INSERT INTO sink_dwd_share
    SELECT
        event_id, event_ts, server_ts, user_id, device_id, is_login, is_vip,
        app_version, os, os_version, device_model, network, carrier, ip, province, city,
        session_id, session_seq, is_foreground, channel, page_id,
        p_song_id, p_share_channel,
        LOCALTIMESTAMP, CAST(event_ts AS DATE)
    FROM v_tagged
    WHERE rule_name IS NULL AND event_name = 'share';

    -- page_view
    INSERT INTO sink_dwd_page_view
    SELECT
        event_id, event_ts, server_ts, user_id, device_id, is_login, is_vip,
        app_version, os, os_version, device_model, network, carrier, ip, province, city,
        session_id, session_seq, is_foreground, channel, page_id,
        p_view_page_id, p_referrer_page, p_stay_duration,
        LOCALTIMESTAMP, CAST(event_ts AS DATE)
    FROM v_tagged
    WHERE rule_name IS NULL AND event_name = 'page_view';

    -- 商业化订单（订阅 / 单曲购买 / 退款三合一，用 event_name 区分类型）
    -- 合成一张表而不是三张：三者字段相同（订单号、SKU、金额、渠道），
    -- 拆表只会让下游每次都要 UNION 三张表才能算收入。
    INSERT INTO sink_dwd_order
    SELECT
        event_id, event_ts, server_ts, user_id, device_id, is_login, is_vip,
        app_version, os, os_version, device_model, network, carrier, ip, province, city,
        session_id, session_seq, is_foreground, channel, page_id,
        event_name, p_order_id, p_sku_id, p_amount, p_pay_channel, p_plan_days,
        p_refund_reason, p_song_id,
        LOCALTIMESTAMP, CAST(event_ts AS DATE)
    FROM v_tagged
    WHERE rule_name IS NULL
      AND event_name IN ('vip_subscribe','order_pay','order_refund');

    -- 客户端播放失败明细
    INSERT INTO sink_dwd_play_error
    SELECT
        event_id, event_ts, server_ts, user_id, device_id, is_login, is_vip,
        app_version, os, os_version, device_model, network, carrier, ip, province, city,
        session_id, session_seq, is_foreground, channel, page_id, p_song_id,
        p_error_code, p_error_msg, p_play_position, network,
        LOCALTIMESTAMP, CAST(event_ts AS DATE)
    FROM v_tagged
    WHERE rule_name IS NULL AND event_name = 'play_error';

    -- DWD topic：给作业 C
    -- 注意这里的过滤条件与 dwd_play_end 的 INSERT 完全一致，
    -- 保证「进了 Doris 明细表的数据」与「送进 DWS 的数据」是同一批。
    INSERT INTO sink_topic_play_end
    SELECT
        event_id,
        DATE_FORMAT(event_ts, 'yyyy-MM-dd HH:mm:ss'),
        DATE_FORMAT(server_ts, 'yyyy-MM-dd HH:mm:ss'),
        user_id, device_id, is_login, is_vip,
        app_version, os, os_version, device_model, network, carrier, province, city,
        session_id, channel, page_id,
        p_song_id, p_song_duration, p_play_duration, p_play_progress, p_end_reason,
        DATE_FORMAT(LOCALTIMESTAMP, 'yyyy-MM-dd HH:mm:ss')
    FROM v_tagged
    WHERE rule_name IS NULL AND event_name = 'play_end';

    -- DWD topic：给作业 C/D 补上「播放终止流」的另一半
    -- 过滤条件与 sink_dwd_skip 完全一致，保证 topic 与 dwd_skip 表同源。
    -- play_progress 用 played_duration/song_duration 现算：skip 事件没有 play_progress
    -- 这个键（它是 play_end 的私有属性），但聚合层要拿它算平均播放进度，
    -- 若留 NULL 会把 AVG 拉偏（AVG 会跳过 NULL，等于把切歌样本排除掉）。
    INSERT INTO sink_topic_skip
    SELECT
        event_id,
        DATE_FORMAT(event_ts, 'yyyy-MM-dd HH:mm:ss'),
        user_id, device_id, province, p_song_id,
        p_played_duration, p_skip_song_duration,
        CASE WHEN p_skip_song_duration IS NULL OR p_skip_song_duration <= 0 THEN CAST(0.0 AS DOUBLE)
             ELSE LEAST(CAST(p_played_duration AS DOUBLE) / p_skip_song_duration, 1.0)
        END,
        p_skip_type,
        DATE_FORMAT(LOCALTIMESTAMP, 'yyyy-MM-dd HH:mm:ss')
    FROM v_tagged
    WHERE rule_name IS NULL AND event_name = 'skip';

END;

-- =============================================================================
-- 验证：对账必须严丝合缝
-- =============================================================================
-- 恒等式（v10）：ODS topic 入口 = 10 张明细 + 异常表 + dwd_unrouted 兜底
--
-- SELECT
--   (code/kafka_ods.py offsets() → topic ods_music_event log-end-offset) AS ods_cnt,
--   (SELECT COUNT(*) FROM music_dw.dwd_play_end)  + (SELECT COUNT(*) FROM music_dw.dwd_play_start)
-- + (SELECT COUNT(*) FROM music_dw.dwd_skip)      + (SELECT COUNT(*) FROM music_dw.dwd_search)
-- + (SELECT COUNT(*) FROM music_dw.dwd_collect)   + (SELECT COUNT(*) FROM music_dw.dwd_share)
-- + (SELECT COUNT(*) FROM music_dw.dwd_page_view) AS dwd_cnt,
--   (SELECT COUNT(*) FROM music_dw.dwd_quality_error) AS err_cnt;
--
-- 规则分布（每条数据只记一条，合计 = err_cnt）：
-- SELECT rule_name, COUNT(*) FROM music_dw.dwd_quality_error GROUP BY rule_name ORDER BY 2 DESC;

-- =============================================================================
-- 维度关联验证（DIM 是否真的进了查询路径）
-- =============================================================================
-- SELECT COUNT(*) AS total,
--        SUM(CASE WHEN song_name <> '' THEN 1 ELSE 0 END) AS with_dim,
--        ROUND(100.0 * SUM(CASE WHEN song_name <> '' THEN 1 ELSE 0 END) / COUNT(*), 2) AS hit_pct
-- FROM music_dw.dwd_play_end
-- WHERE event_ts >= NOW() - INTERVAL 10 MINUTE;
--
-- SELECT song_id, song_name, artist_name, genre, album_name
-- FROM music_dw.dwd_play_end
-- WHERE song_name <> '' ORDER BY event_ts DESC LIMIT 10;
--
-- 命中率应为 100%（全部 song_id 都指向真实维表）；
-- 若偏低，先查 HBase 表 RowKey 与装载表达式是否一致。
