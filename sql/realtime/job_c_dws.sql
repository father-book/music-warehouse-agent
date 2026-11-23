-- =============================================================================
-- 作业 C：DWS 层窗口聚合
--   topic dwd_play_end ──┬──→ dws_play_overview_1min     （1 分钟全局）
--                        ├──→ dws_song_play_5min         （5 分钟单曲）
--                        └──→ dws_province_play_5min     （5 分钟地域）
--
-- 【这个作业最大的变化：从处理时间改回事件时间】
--
-- 之前那版单作业管道被迫用 PROCTIME，原因是源表直接连着脏流：
--   脏数据里有「事件时间推后 2~30 天」的类型，一旦在源头声明 watermark，
--   水位线会被瞬间推到 30 天后，之后所有正常数据都被判为迟到而丢弃。
--
-- 拆层之后这个问题自然消失了：
--   作业 B 已经在 v_tagged 里用 event_time_future 规则把这类数据拦进异常表，
--   能进 dwd_play_end topic 的数据，事件时间必然落在合法区间内。
--   所以作业 C 可以放心用事件时间 + watermark，窗口语义从
--   「系统在这 1 分钟处理了多少」修正为「用户在这 1 分钟听了多少」——
--   后者才是数仓该有的口径，才经得起「大屏数字和业务方核对」。
--
--   这就是分层的实质价值：把数据的不确定性收敛在边界层（作业 B），
--   让内层（作业 C）可以假设输入是干净的。压在一个作业里做不到这一点，
--   因为源只有一个，watermark 只能在源头声明，而源头注定是脏的。
--
-- 【为什么 watermark 容忍 2 分钟乱序（v10.1 由 5 秒放大）】
--   原来写的是 5 秒，理由是「埋点 server_time 与 Kafka 写入顺序基本一致，
--   乱序只来自多分区并行」。这个假设在真实客户端直连 Kafka 时成立，
--   但在本项目的链路上不成立 —— 一条事件要过三道 Kafka 中转：
--     music_events → (A) → ods_music_event → (B, 含 HBase 点查) → dwd_play_end
--   每一道都是「多 subtask 并发写 + Kafka 粘性分区器」，写出时刻取决于
--   各 subtask 的处理快慢，而不是事件时间。实测（2026-11-08，20 万条/6 分钟）：
--     dwd_play_end topic 相邻逆序 39%，倒回深度 P50=13s、P90=33s、P99=47s、max=67s
--   5 秒容错意味着水位线一过，绝大多数正常事件都被判迟到丢弃：
--   DWS 的 play_cnt 只有明细的 32%，且丢得毫无规律（某些分钟只剩 1/5）。
--
--   2 分钟 = 实测最大倒回（67s）的 1.8 倍，留足抖动余量。
--   代价是窗口晚 2 分钟关闭：大屏「近 1 分钟」实际滞后 2~3 分钟。
--   这个代价必须付 —— 数字完整性远比 2 分钟的时鲜度重要，丢数的实时报表
--   没有任何意义。若将来上游改成客户端直连（只有一道中转），可再调回 30 秒。
-- =============================================================================

SET 'pipeline.name' = 'rt_job_c_dws';
SET 'parallelism.default' = '3';

-- -----------------------------------------------------------------------------
-- 聚合优化：COUNT(DISTINCT) 拆分（Split Distinct）
-- -----------------------------------------------------------------------------
-- 压测基线（docs/14）：400 条/s 时 C 是第二个掉队的算子（dws lag 持续增长不收敛）。
-- 本作业三个聚合全是 TUMBLE 窗口 TVF + COUNT(DISTINCT)，教程性能四件套里
-- MiniBatch / LocalGlobal 只对无界聚合生效（本作业没有无界 GROUP BY，配了也是空转），
-- 唯一命中的是 distinct-agg.split：把「每个窗口一个大 Set」的状态拆成
-- 内层按 (window_start, distinct_key) 预去重 → 外层 COUNT 的两段式。
-- 注意：split 的收益依赖并行度（local 预聚合 → shuffle → global），
-- parallelism=1 时 local=global 收益减半，所以这里同时把并行度提到 2
-- （slot 预算：A1 + B2 + C2 + D1 = 6/6，正好占满）。
--
SET 'table.optimizer.distinct-agg.split.enabled' = 'true';
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
-- 窗口 timer 下沉到 RocksDB（默认 HEAP）。
-- C 是 1/5 分钟 TUMBLE 窗口作业，每个窗口要注册大量清理 timer；
-- HEAP 模式下这些 timer 全在 JVM 堆里，数据量上去后 GC 压力明显。
-- 放到 RocksDB 后堆占用只与活跃 key 有关，长时间运行更稳。
-- 代价：timer 读写多一次序列化，低负载下反而略慢 —— 所以这一项目前拿不到收益，
-- 是为「跑到 800 条/s 时不炸」提前做的准备。
SET 'state.backend.rocksdb.timer-service.factory'        = 'ROCKSDB';
SET 'state.checkpoints.dir'                              = 'hdfs://WWC160:9820/flink/checkpoints';
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

-- 关键：某个分区长时间无数据时，让 watermark 仍能推进，
-- 否则空闲分区会把整个作业的 watermark 卡住，窗口永远不触发。
SET 'table.exec.source.idle-timeout' = '30s';

-- -----------------------------------------------------------------------------
-- 0) 源：DWD topic（已清洗的 play_end 明细）
-- -----------------------------------------------------------------------------
-- 如果这里报「计算列不能作为 watermark 列」，改成直接声明：
--   event_ts TIMESTAMP(3),
-- 并在 WITH 里加 'json.timestamp-format.standard' = 'SQL'
-- -----------------------------------------------------------------------------
-- -----------------------------------------------------------------------------
-- 【计数口径：幂等计数】
--   窗口里的播放量用 COUNT(DISTINCT event_id) 而不是 COUNT(*)。
--   上游是 at-least-once 链路：Doris sink 重试、作业从 checkpoint 恢复后重放，
--   都会让同一事件被处理两次，明细表里就出现两行相同 event_id（DWD 是
--   DUPLICATE 模型，故意保留原始粒度、不做合并）。
--   COUNT(*) 会把两行都算上，播放量虚高；COUNT(DISTINCT event_id) 不管上游重复
--   多少次结果都一样 —— 这是幂等口径，流式数仓里最划算的一道防线。
--   代价：窗口内维护 event_id 去重集合；1/5 分钟窗口只有几十到几百个 ID，可忽略。
-- -----------------------------------------------------------------------------

CREATE TABLE src_dwd_play_end (
    event_id       STRING,
    event_ts_str   STRING,
    server_ts_str  STRING,
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
    province       STRING,
    city           STRING,
    session_id     STRING,
    channel        STRING,
    page_id        STRING,
    song_id        STRING,
    song_duration  INT,
    play_duration  INT,
    play_progress  DOUBLE,
    end_reason     STRING,
    etl_time_str   STRING,
    event_ts       AS TO_TIMESTAMP(event_ts_str),
    WATERMARK FOR event_ts AS event_ts - INTERVAL '2' MINUTE
) WITH (
    'connector'                    = 'kafka',
    'topic'                        = 'dwd_play_end',
    'properties.bootstrap.servers' = 'WWC161:9092',
    'properties.group.id'          = 'rt_job_c_dws_r3',
    -- 首次启动补齐 topic 存量，之后从提交位点续读（同作业 B 的理由）
    'scan.startup.mode'            = 'group-offsets',
    'properties.isolation.level'   = 'read_committed',
    'properties.auto.offset.reset' = 'latest',
    'format'                       = 'json'
);

-- -----------------------------------------------------------------------------
-- 0.2) skip 源：播放终止流的另一半
-- -----------------------------------------------------------------------------
-- 会话模型里一次起播只有两种结局：play_end（唱完/中途退出）或 skip（切歌）。
-- 作业 B 之前只把 play_end 送过来，于是本作业的 skip_cnt（end_reason='skip'）
-- 永远是 0，切歌率恒为 0；完播率也被算成 natural/play_end = 73.3%，
-- 而设计口径是 natural/起播 = 51.3%（见 config/event_weights.json 的 _derived）。
-- 现在把 dwd_skip 一起读进来，用 v_play_term 合成完整终止流。
CREATE TABLE src_dwd_skip (
    event_id        STRING,
    event_ts_str    STRING,
    user_id         STRING,
    device_id       STRING,
    province        STRING,
    song_id         STRING,
    played_duration INT,
    song_duration   INT,
    play_progress   DOUBLE,
    skip_type       STRING,
    etl_time_str    STRING,
    event_ts        AS TO_TIMESTAMP(event_ts_str),
    WATERMARK FOR event_ts AS event_ts - INTERVAL '2' MINUTE
) WITH (
    'connector'                    = 'kafka',
    'topic'                        = 'dwd_skip',
    'properties.bootstrap.servers' = 'WWC161:9092',
    'properties.group.id'          = 'rt_job_c_dws_r3',
    'scan.startup.mode'            = 'group-offsets',
    'properties.isolation.level'   = 'read_committed',
    'properties.auto.offset.reset' = 'latest',
    'format'                       = 'json'
);

-- 播放终止流：列裁剪到聚合真正用到的 9 个，两路 schema 对齐（UNION ALL 要求逐列同类型）。
-- event_ts 是两路都声明的 ROWTIME，Flink 会沿 UNION 传播时间属性，
-- 下游 TUMBLE/HOP 仍按事件时间开窗；水位线取两路较小值。
CREATE TEMPORARY VIEW v_play_term AS
SELECT event_id, event_ts, end_reason, play_duration, play_progress,
       device_id, user_id, song_id, province
FROM src_dwd_play_end
UNION ALL
SELECT event_id, event_ts, 'skip' AS end_reason,
       played_duration AS play_duration, play_progress,
       device_id, user_id, song_id, province
FROM src_dwd_skip;

-- -----------------------------------------------------------------------------
-- 1) 三个 sink
-- -----------------------------------------------------------------------------
CREATE TABLE sink_dws_overview (
    dt DATE, window_start TIMESTAMP(3), window_end TIMESTAMP(3),
    play_cnt BIGINT, finish_cnt BIGINT, skip_cnt BIGINT,
    pause_cnt BIGINT, exit_cnt BIGINT, interrupt_cnt BIGINT,
    finish_rate DOUBLE, skip_rate DOUBLE,
    avg_play_duration DOUBLE, avg_play_progress DOUBLE,
    uv BIGINT, login_uv BIGINT
) WITH (
    'connector' = 'doris', 'fenodes' = 'WWC163:8030',
    'table.identifier' = 'music_dw.dws_play_overview_1min',
    'username' = 'root', 'password' = '',
    'sink.label-prefix' = 'job_c_overview',
    'sink.properties.format' = 'json', 'sink.properties.read_json_by_line' = 'true',
    'sink.enable-2pc' = 'false',
    'sink.buffer-flush.max-rows' = '200', 'sink.buffer-flush.interval' = '1s'
);

CREATE TABLE sink_dws_song (
    dt DATE, window_start TIMESTAMP(3), song_id STRING,
    play_cnt BIGINT, finish_cnt BIGINT, skip_cnt BIGINT,
    skip_rate DOUBLE, avg_play_progress DOUBLE, uv BIGINT
) WITH (
    'connector' = 'doris', 'fenodes' = 'WWC163:8030',
    'table.identifier' = 'music_dw.dws_song_play_5min',
    'username' = 'root', 'password' = '',
    'sink.label-prefix' = 'job_c_song',
    'sink.properties.format' = 'json', 'sink.properties.read_json_by_line' = 'true',
    'sink.enable-2pc' = 'false',
    'sink.buffer-flush.max-rows' = '500', 'sink.buffer-flush.interval' = '1s'
);

CREATE TABLE sink_dws_province (
    dt DATE, window_start TIMESTAMP(3), province STRING,
    play_cnt BIGINT, finish_cnt BIGINT, skip_cnt BIGINT,
    skip_rate DOUBLE, uv BIGINT
) WITH (
    'connector' = 'doris', 'fenodes' = 'WWC163:8030',
    'table.identifier' = 'music_dw.dws_province_play_5min',
    'username' = 'root', 'password' = '',
    'sink.label-prefix' = 'job_c_province',
    'sink.properties.format' = 'json', 'sink.properties.read_json_by_line' = 'true',
    'sink.enable-2pc' = 'false',
    'sink.buffer-flush.max-rows' = '500', 'sink.buffer-flush.interval' = '1s'
);

-- -----------------------------------------------------------------------------
-- 2) 三路窗口聚合
-- -----------------------------------------------------------------------------
EXECUTE STATEMENT SET
BEGIN

    -- 2.1 全局播放总览（1 分钟）
    INSERT INTO sink_dws_overview
    SELECT
        CAST(TUMBLE_START(event_ts, INTERVAL '1' MINUTE) AS DATE)          AS dt,
        TUMBLE_START(event_ts, INTERVAL '1' MINUTE)                        AS window_start,
        TUMBLE_END(event_ts, INTERVAL '1' MINUTE)                          AS window_end,
        COUNT(DISTINCT event_id)                                                          AS play_cnt,
        COUNT(DISTINCT CASE WHEN end_reason = 'natural' THEN event_id END)         AS finish_cnt,
        COUNT(DISTINCT CASE WHEN end_reason = 'skip' THEN event_id END)         AS skip_cnt,
        SUM(CASE WHEN end_reason = 'pause'     THEN 1 ELSE 0 END)         AS pause_cnt,
        SUM(CASE WHEN end_reason = 'exit'      THEN 1 ELSE 0 END)         AS exit_cnt,
        SUM(CASE WHEN end_reason = 'interrupt' THEN 1 ELSE 0 END)         AS interrupt_cnt,
        ROUND(COUNT(DISTINCT CASE WHEN end_reason = 'natural' THEN event_id END) * 100.0 / COUNT(DISTINCT event_id), 2),
        ROUND(COUNT(DISTINCT CASE WHEN end_reason = 'skip' THEN event_id END) * 100.0 / COUNT(DISTINCT event_id), 2),
        ROUND(AVG(CAST(play_duration AS DOUBLE)), 1),
        ROUND(AVG(play_progress), 4),
        COUNT(DISTINCT device_id),
        COUNT(DISTINCT CASE WHEN COALESCE(user_id, '') <> '' THEN user_id END)
    FROM v_play_term
    GROUP BY TUMBLE(event_ts, INTERVAL '1' MINUTE);

    -- 2.2 单曲热度（5 分钟）
    INSERT INTO sink_dws_song
    SELECT
        CAST(TUMBLE_START(event_ts, INTERVAL '5' MINUTE) AS DATE)           AS dt,
        TUMBLE_START(event_ts, INTERVAL '5' MINUTE)                         AS window_start,
        song_id,
        COUNT(DISTINCT event_id)                                                          AS play_cnt,
        COUNT(DISTINCT CASE WHEN end_reason = 'natural' THEN event_id END)           AS finish_cnt,
        COUNT(DISTINCT CASE WHEN end_reason = 'skip' THEN event_id END)           AS skip_cnt,
        ROUND(COUNT(DISTINCT CASE WHEN end_reason = 'skip' THEN event_id END) * 100.0 / COUNT(DISTINCT event_id), 2),
        ROUND(AVG(play_progress), 4),
        COUNT(DISTINCT device_id)
    FROM v_play_term
    WHERE COALESCE(song_id, '') <> ''
    GROUP BY TUMBLE(event_ts, INTERVAL '5' MINUTE), song_id;

    -- 2.3 地域分布（5 分钟）
    INSERT INTO sink_dws_province
    SELECT
        CAST(TUMBLE_START(event_ts, INTERVAL '5' MINUTE) AS DATE)           AS dt,
        TUMBLE_START(event_ts, INTERVAL '5' MINUTE)                         AS window_start,
        province,
        COUNT(DISTINCT event_id)                                                          AS play_cnt,
        COUNT(DISTINCT CASE WHEN end_reason = 'natural' THEN event_id END)           AS finish_cnt,
        COUNT(DISTINCT CASE WHEN end_reason = 'skip' THEN event_id END)           AS skip_cnt,
        ROUND(COUNT(DISTINCT CASE WHEN end_reason = 'skip' THEN event_id END) * 100.0 / COUNT(DISTINCT event_id), 2),
        COUNT(DISTINCT device_id)
    FROM v_play_term
    WHERE COALESCE(province, '') <> ''
    GROUP BY TUMBLE(event_ts, INTERVAL '5' MINUTE), province;

END;

-- =============================================================================
-- 验证
-- =============================================================================
-- 1. 窗口是否在推进（等 1~2 分钟后再查）：
--    SELECT * FROM music_dw.dws_play_overview_1min ORDER BY window_start DESC LIMIT 10;
--
-- 2. 与明细层对账（同一时间范围内，DWS 条数应等于 DWD 条数）：
--    SELECT SUM(play_cnt) FROM music_dw.dws_play_overview_1min
--    WHERE window_start >= NOW() - INTERVAL '30' MINUTE;
--    SELECT COUNT(*) FROM music_dw.dwd_play_end
--    WHERE event_ts >= NOW() - INTERVAL '30' MINUTE;
--
-- 3. 完播率是否落在合理区间（60%~70%）：
--    SELECT ROUND(SUM(finish_cnt)*100.0/SUM(play_cnt), 2) AS finish_rate_pct,
--           ROUND(SUM(skip_cnt)*100.0/SUM(play_cnt), 2)   AS skip_rate_pct
--    FROM music_dw.dws_play_overview_1min;
