-- =============================================================================
-- 作业 D：ADS 层结果表
--   topic dwd_play_end ──┬──→ ads_song_rank_10min     （10 分钟滚动热榜 Top20）
--                        └──→ ads_province_10min      （10 分钟滚动地域分布，大屏地图）
--
-- 【注意：日累计不在这里】
--   ads_realtime_overview（今日累计 KPI）与 ads_commercial_daily（商业化日累计）
--   由 `code/collect_ops_metrics.py` 每分钟从 dwd_play_end / dwd_order 微批重算，
--   不是本作业的输出（ADR-021：日累计必须可回溯，不能挂在流状态上）。
--   本文档早前版本把 overview 画在本作业下，那是 ADR-021 之前的方案。
--
-- 【为什么要独立成作业，而不是并进作业 C】
--   作业 C 和作业 D 都消费 dwd_play_end，看起来像重复消费，但按切分判据第二条
--   （同源、计算强度与 SLA 差异大 → 拆）：
--     - 作业 C 是无状态窗口聚合，checkpoint 在 MB 级，要求秒级延迟
--     - 作业 D 有全天累计窗口（CUMULATE 1 天）、窗口 Top-N、双流 join，
--       state 大一到两个数量级，30 秒级延迟即可
--   并在一起会让 D 的 state 拖慢 C 的 checkpoint，且 C 的延迟被 D 的天级窗口绑架。
--
-- 【三张表为什么要三种不同的窗口形态】
--   ads_realtime_overview 是「今日累计」——不是窗口指标，是单调累加值。
--     用 CUMULATE（累积窗口）：每天 0 点起算，每 30 秒输出一次"截至目前的累计"。
--     Doris 侧 UNIQUE KEY(stat_date) 覆盖写，所以只会留最新那一行。
--     这里不能用 TUMBLE：TUMBLE 每 30 秒只给 30 秒的量，不是累计。
--
--   ads_song_rank_10min 是「近 10 分钟滚动 Top20」——窗口聚合 + 窗口内排名。
--     用 HOP(1 分钟滑动, 10 分钟窗口) + ROW_NUMBER，Flink 会识别成 Window Top-N，
--     只保留每个窗口的前 20 名，不会把全部歌曲留在 state 里。
--     为什么不是 TUMBLE 1 小时：整点过后才吐一次，演示时榜单长时间不动，像没在跑；
--     为什么不是 TUMBLE 10 分钟：仍是每 10 分钟一跳（00/10/20…），不够实时。
--     HOP 每分钟关闭一个窗口，窗口内始终覆盖最近 10 分钟，每分钟刷新一次；
--     代价是同时维护 10 个重叠窗口的聚合状态——1486 首歌、几百条/秒，可忽略。
--     注意 HOP 的语义是「窗口结束才输出」，所以大屏看到的最新窗口最多滞后
--     1 分钟 + watermark（5 秒），而不是 1 小时。
--
--   ads_province_10min 是「各省占同窗口全国播放量的比例」——
--     需要"分省聚合"和"全国合计"两个粒度，用同一窗口的两次聚合 join 起来算占比。
--     窗口用 HOP(1 分钟滑动, 10 分钟窗口)，和榜单同一套节奏：
--     原来是 TUMBLE 1 小时，地图几十分钟不变色，和榜单改之前是同一个问题。
--     Flink 的 OVER (PARTITION BY window_start) 在流上要求有界 ORDER BY，
--     这里绕开它，用 join 实现，行为更可预期。
--
-- 【相对原批式脚本的一处口径修正】
--   原 SQL 里榜单和地域的 uv 是 SUM(dws.uv)，即"各窗口去重数之和"，
--   同一用户跨窗口会被重复计数，是近似值。
--   流式版直接在窗口内 COUNT(DISTINCT device_id)，是精确值。
-- =============================================================================

SET 'pipeline.name' = 'rt_job_d_ads';
SET 'parallelism.default' = '3';
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
-- 窗口 timer 下沉到 RocksDB（同 C）：D 是 10 分钟 HOP + Window Top-N，
-- 每个窗口既要注册清理 timer，还要维护 rank 缓存，堆压力比 C 更大。
SET 'state.backend.rocksdb.timer-service.factory'        = 'ROCKSDB';
SET 'state.checkpoints.dir'                              = 'hdfs://WWC160:9820/flink/checkpoints';
-- D 的状态是 10 分钟窗口 Top-N，比 A/B/C 大一个量级；30s 一次会让
-- checkpoint 与重放互相拖累（实测一次要 30~50s），放宽到 60s。
SET 'execution.checkpointing.interval'                   = '60s';
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
SET 'table.exec.source.idle-timeout' = '30s';

-- -----------------------------------------------------------------------------
-- 0) 源：清洗后的播放明细（与作业 C 同 topic，不同 group，各自独立位点）
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
    -- 2 分钟容错，理由与实测数据见 job_c_dws.sql 文件头：
    -- 链路上有两道 Kafka 中转 + HBase 点查，实测倒回深度 P90=33s、max=67s，
    -- 5 秒容错会把大部分正常事件判成迟到丢掉（DWS 只剩 32%）。
    WATERMARK FOR event_ts AS event_ts - INTERVAL '2' MINUTE
) WITH (
    'connector'                    = 'kafka',
    'topic'                        = 'dwd_play_end',
    'properties.bootstrap.servers' = 'WWC161:9092',
    'properties.group.id'          = 'rt_job_d_ads_r4',
    -- 位点策略：group-offsets + auto.offset.reset=latest（与 A/B/C 一致）。
    -- 曾经这里必须是 earliest：那时「今日累计」是 CUMULATE(1d) 的流状态，
    -- 重启后状态为空，只能靠重放整条 topic 把当天数据重新累一遍，
    -- 否则大屏顶部「今日播放量」会突然掉一个数量级。
    -- 现在日累计改由采集器每分钟从 dwd_play_end 聚合（code/collect_ops_metrics.py），
    -- D 不再持有日级状态 → 位点续读即可，重启从秒级恢复，没有重放窗口。
    --
    -- 【2026-11-08 修】下面原来多写了一条 auto.offset.reset=earliest，
    -- 把上一行 latest 覆盖掉了（Flink 按 WITH 里的书写顺序取后者）：
    -- 配置文件看着是「续读」，实际跑的是「从最早重放」，与注释完全相反。
    -- 重复项已删除，只留 latest 一条。
    'scan.startup.mode'            = 'group-offsets',
    'properties.auto.offset.reset' = 'latest',
    'properties.isolation.level'   = 'read_committed',
    'format'                       = 'json'
);

-- skip 源：与作业 C 同口径，把「播放终止流」的另一半补齐。
-- 不加这一路，热榜的 skip_rate 恒为 0（play_end 的 end_reason 只有 natural/exit），
-- 榜单会呈现「零切歌」的假象。
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
    'properties.group.id'          = 'rt_job_d_ads_r4',
    'scan.startup.mode'            = 'group-offsets',
    'properties.auto.offset.reset' = 'latest',
    'properties.isolation.level'   = 'read_committed',
    'format'                       = 'json'
);

-- 播放终止流 = play_end ∪ skip（定义见 job_c_dws.sql 同段注释）
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
-- 1) 两个 sink
-- -----------------------------------------------------------------------------
CREATE TABLE sink_ads_song_rank (
    window_start TIMESTAMP(3), rank_no INT, song_id STRING,
    play_cnt BIGINT, uv BIGINT, skip_rate DOUBLE, updated_at TIMESTAMP(3)
) WITH (
    'connector' = 'doris', 'fenodes' = 'WWC163:8030',
    'table.identifier' = 'music_dw.ads_song_rank_10min',
    'username' = 'root', 'password' = '',
    'sink.label-prefix' = 'job_d_song_rank',
    'sink.properties.format' = 'json', 'sink.properties.read_json_by_line' = 'true',
    'sink.enable-2pc' = 'false',
    'sink.buffer-flush.max-rows' = '100', 'sink.buffer-flush.interval' = '5s'
);

CREATE TABLE sink_ads_province (
    window_start TIMESTAMP(3), province STRING, play_cnt BIGINT, uv BIGINT,
    play_share DOUBLE, updated_at TIMESTAMP(3)
) WITH (
    'connector' = 'doris', 'fenodes' = 'WWC163:8030',
    'table.identifier' = 'music_dw.ads_province_10min',
    'username' = 'root', 'password' = '',
    'sink.label-prefix' = 'job_d_province',
    'sink.properties.format' = 'json', 'sink.properties.read_json_by_line' = 'true',
    'sink.enable-2pc' = 'false',
    'sink.buffer-flush.max-rows' = '100', 'sink.buffer-flush.interval' = '5s'
);

-- -----------------------------------------------------------------------------
-- 2) 地域占比的两个中间视图（同一 10 分钟窗口的两种粒度）
-- -----------------------------------------------------------------------------
-- 注意：这里不要设 table.exec.state.ttl。它会影响 CUMULATE 的天级窗口状态，
-- 导致累计值被中途清空。join 的 key 是 window_start（每分钟一个），
-- 旧窗口不再来数据，state 事实上不增长，无需 TTL。
-- -----------------------------------------------------------------------------
CREATE TEMPORARY VIEW v_prov_10min AS
SELECT
    HOP_START(event_ts, INTERVAL '1' MINUTE, INTERVAL '10' MINUTE) AS window_start,
    province,
    COUNT(DISTINCT event_id)                                     AS play_cnt,
    COUNT(DISTINCT device_id)                    AS uv
FROM v_play_term
WHERE COALESCE(province, '') <> ''
GROUP BY HOP(event_ts, INTERVAL '1' MINUTE, INTERVAL '10' MINUTE), province;

CREATE TEMPORARY VIEW v_10min_total AS
-- 【2026-11-08 修】原先是「窗口内全局 COUNT(DISTINCT event_id)」，而 v_prov_10min
-- 是「按省 COUNT(DISTINCT event_id)」。造数器注入的重复 event_id 会带不同省份，
-- 同一个 ID 在省级各算一次、全国只算一次 → 各省 play_share 之和能到 180%
-- （实测 11-08 14:06 窗口：各省合计 612，分母只有 340）。
-- 正确做法是分母 = Σ(各省去重值)，即对省份视图再求一次和，保证 Σshare = 100%。
SELECT
    window_start,
    SUM(pc) AS total_cnt
FROM (
    SELECT
        HOP_START(event_ts, INTERVAL '1' MINUTE, INTERVAL '10' MINUTE) AS window_start,
        province,
        COUNT(DISTINCT event_id)                                     AS pc
    FROM v_play_term
    WHERE COALESCE(province, '') <> ''
    GROUP BY HOP(event_ts, INTERVAL '1' MINUTE, INTERVAL '10' MINUTE), province
)
GROUP BY window_start;

-- -----------------------------------------------------------------------------
-- 3) 两路写出
-- -----------------------------------------------------------------------------
EXECUTE STATEMENT SET
BEGIN


    -- 3.2 近 10 分钟滚动热榜 Top20（HOP 窗口聚合 + 窗口内排名，Flink 识别为 Window Top-N）
    --     HOP(滑动 1 分钟, 窗口 10 分钟)：每分钟关闭一个窗口，窗口内覆盖最近 10 分钟。
    --     Top-N 的 N 由 WHERE rn <= 20 控制；改这里要同步改大屏取数与文档口径。
    INSERT INTO sink_ads_song_rank
    SELECT
        window_start,
        CAST(rn AS INT) AS rank_no,
        song_id,
        play_cnt,
        uv,
        skip_rate,
        LOCALTIMESTAMP
    FROM (
        SELECT
            window_start, song_id, play_cnt, uv, skip_rate,
            ROW_NUMBER() OVER (PARTITION BY window_start ORDER BY play_cnt DESC, song_id) AS rn
        FROM (
            SELECT
                HOP_START(event_ts, INTERVAL '1' MINUTE, INTERVAL '10' MINUTE) AS window_start,
                song_id,
                COUNT(DISTINCT event_id)                                 AS play_cnt,
                COUNT(DISTINCT device_id)                AS uv,
                ROUND(COUNT(DISTINCT CASE WHEN end_reason = 'skip' THEN event_id END) * 100.0 / COUNT(DISTINCT event_id), 2) AS skip_rate
            FROM v_play_term
            WHERE COALESCE(song_id, '') <> ''
            GROUP BY HOP(event_ts, INTERVAL '1' MINUTE, INTERVAL '10' MINUTE), song_id
        )
    )
    WHERE rn <= 20;

    -- 3.3 近 10 分钟地域分布（分省播放量 ÷ 同窗口全国播放量）
    INSERT INTO sink_ads_province
    SELECT
        p.window_start,
        p.province,
        p.play_cnt,
        p.uv,
        ROUND(p.play_cnt * 100.0 / t.total_cnt, 2)   AS play_share,
        LOCALTIMESTAMP
    FROM v_prov_10min p
    JOIN v_10min_total t ON p.window_start = t.window_start;

END;

-- =============================================================================
-- 验证
-- =============================================================================
-- 1. 三张表是否在增长（等 1~2 分钟）：
--    SELECT * FROM music_dw.ads_realtime_overview\G
--    SELECT MAX(window_start), COUNT(*) FROM music_dw.ads_song_rank_10min;
--    -- 每分钟应新增一个窗口、20 行；rank_no 连续 1~20
--    SELECT * FROM music_dw.ads_song_rank_10min
--    WHERE window_start = (SELECT MAX(window_start) FROM music_dw.ads_song_rank_10min)
--    ORDER BY rank_no;
--
-- 2. 地域占比之和应接近 100%：
--    SELECT window_start, ROUND(SUM(play_share), 2) FROM music_dw.ads_province_10min
--    GROUP BY window_start ORDER BY window_start DESC LIMIT 5;
--
-- 3. 总览与明细对账（口径必须一致）：
--    SELECT SUM(total_play) FROM music_dw.ads_realtime_overview;
--    SELECT COUNT(*) FROM music_dw.dwd_play_end WHERE dt = CURDATE();
