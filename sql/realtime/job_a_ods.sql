-- =============================================================================
-- 作业 A：ODS 层落地
--   Kafka(music_events) ──┬──→ topic ods_music_event   （给作业 B 消费）
--                         └──→ doris.ods_music_event   （给质检 SQL 查）
--
-- 为什么这个作业有两个 sink 是合理的：
--   两个出口写的是【同一份数据、同一粒度】，只是介质不同（消息总线 / 分析存储）。
--   不涉及任何转换，所以不构成「把两层的计算压进一个作业」。
--
-- 为什么 ODS topic 输出原始 msg 而不是解析后的结构化 JSON：
--   ODS 层的定义就是「原样归档、不加工」。如果作业 A 先把数据拆成 22 个字段
--   再写回 topic，那 topic 里存的就不是原始报文了，一旦 DWD 解析规则调整，
--   连原件都找不回来。raw 写入让 topic 成为真正的「原始留档」。
--   代价是下游解析两次，但 JSON 解析是纯 CPU 轻量操作，换来的是可回溯性。
--
-- 提交方式（重要）：
--   必须用 remote 模式提交到 161 的 session cluster，否则 sql-client 退出后
--   作业会跟着被取消：
--     bin/sql-client.sh -s sql/realtime/session-remote.yaml -f job_a_ods.sql
-- =============================================================================

SET 'pipeline.name' = 'rt_job_a_ods';
SET 'parallelism.default' = '1';
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
-- checkpoint 间隔 10s → 30s（压测结论，见 docs/14）：
-- A 是唯一带 Kafka 2PC exactly-once sink 的作业，每个 checkpoint 都要提交一次
-- Kafka 事务，而事务提交不能重叠（max-concurrent-checkpoints=1）。实测配 10s 时
-- 实际间隔被提交延迟拉长到约 30s，负载爬坡期还出现过 7 次 checkpoint 中止。
-- 与其配一个达不到的 10s（徒增事务提交压力），不如显式对齐到 30s：
-- 语义不变（精确一次只依赖「已完成 checkpoint」），恢复点粒度仍是 30 秒。
SET 'execution.checkpointing.interval'                   = '30s';
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

-- -----------------------------------------------------------------------------
-- 0) 源：Kafka 原始埋点流
-- -----------------------------------------------------------------------------
-- 用 raw 而不是 json：properties 是嵌套对象，json format 无法把它映射成 STRING。
-- 整条读入后用 JSON_VALUE / JSON_QUERY 拆字段。
-- -----------------------------------------------------------------------------
CREATE TABLE src_kafka (
    msg STRING
) WITH (
    'connector'                    = 'kafka',
    'topic'                        = 'music_events',
    'properties.bootstrap.servers' = 'WWC161:9092',
    'properties.group.id'          = 'rt_job_a_ods',
    -- group-offsets：从消费者组已提交的位点续读。原来写 latest-offset 是图省事，
    -- 但那样每次重启都会跳过停机期间的积压（实测停 10 分钟丢 1.2 万条）。
    -- 该 group 已有提交位点，所以这里等价于「断点续传」；只有全新 group
    -- 才会走 auto.offset.reset 的 latest。
    'scan.startup.mode'            = 'group-offsets',
    'properties.auto.offset.reset' = 'latest',
    'format'                       = 'raw'
);

-- -----------------------------------------------------------------------------
-- 1) sink：ODS topic（原始报文，供作业 B 消费）
-- -----------------------------------------------------------------------------
CREATE TABLE sink_topic_ods (
    msg STRING
) WITH (
    'connector'                    = 'kafka',
    'topic'                        = 'ods_music_event',
    'properties.bootstrap.servers' = 'WWC161:9092',
    'format'                       = 'raw',
    'sink.partitioner'             = 'default',
    -- 端到端精确一次：Kafka sink 走两阶段提交
    --   checkpoint 触发时开启事务并写入，checkpoint 完成后才 commit；
    --   作业失败时 uncommitted 事务被 abort，下游 read_committed 看不到脏数据。
    --   这一步是「重复计数」的根治办法 —— 之前 A 的 checkpoint 重放会往 topic 里
    --   写重复事件，B 原样透传，DWS 的 COUNT(*) 就会虚高。
    --   代价：延迟下界 = checkpoint 间隔（提交才可见），所以 A 的间隔设为 10s。
    'sink.delivery-guarantee'      = 'exactly-once',
    'sink.transactional-id-prefix' = 'rt_job_a_ods',
    -- 事务超时必须 <= broker 的 transaction.max.timeout.ms（默认 900000 = 15min），
    -- 否则 InitProducerId 直接被拒：uncommitted 事务超时会被 broker 自动 abort，
    -- 数据就悄悄丢了。10 分钟给 checkpoint（10s）+ 故障恢复留足余量。
    'properties.transaction.timeout.ms' = '600000'
);

-- -----------------------------------------------------------------------------
-- 2) sink：Doris ODS 表（给质检 SQL 与双轨回溯用）
-- -----------------------------------------------------------------------------

-- -----------------------------------------------------------------------------
-- 3) 单路写出：Kafka topic `ods_music_event` 就是实时 ODS（v10）
--    作业 B 本来就只读这个 topic，Doris music_dw.ods_music_event 是旁写的第二份
--    原始明细。实时侧不分析历史，砍掉它既省存储也消除「两份 ODS」的口径歧义。
--    持久原始层仍是离线 HDFS ODS(ORC)；实时可回溯窗口 = Kafka retention 3 天。
-- -----------------------------------------------------------------------------
EXECUTE STATEMENT SET
BEGIN

    INSERT INTO sink_topic_ods
    SELECT msg FROM src_kafka;


END;

-- -----------------------------------------------------------------------------
-- 验证
-- -----------------------------------------------------------------------------
-- 1. 作业状态：bin/flink list
-- 2. ODS 条数 = topic `ods_music_event` 各分区 log-end-offset 之和：
--    kafka-consumer-groups.sh --bootstrap-server WWC161:9092 --describe --group rt_job_b_dwd_r3
--    （LAG 应为 0；也可用 code/kafka_ods.py 的 offsets() 直接取数）
-- 3. 端到端延迟（实时性的量化证据）：
--    SELECT MAX(etl_time) FROM dwd_play_end;   -- DWD 入仓时间作为 ODS 已消费的代理
-- 4. ODS topic 堆积（应接近 0）：
--    kafka-consumer-groups.sh --bootstrap-server WWC161:9092 --describe --group rt_job_b_dwd
