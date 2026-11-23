-- =============================================================================
-- 运维监控表（不参与业务口径，只服务可观测性与答辩演示）
-- 执行位置：任意能连 Doris 9030 的机器
-- 执行方式：mysql -hWWC163 -P9030 -uroot < doris_ops_tables.sql
-- =============================================================================
-- 设计要点：
--   1. 三张表共用 stat_minute（分钟粒度）作为主键前缀，采集脚本一分钟写一轮。
--   2. 全部 UNIQUE KEY：采集脚本可以安全重跑，同一分钟重复采集只覆盖不追加。
--   3. 与业务表分名前缀（ops_），避免被误当成 ADS 指标参与对账。
--   4. 只保留 30 天：监控数据的价值随时间衰减，且量级远大于业务表。
-- =============================================================================

USE music_dw;

-- -----------------------------------------------------------------------------
-- 1) 链路延迟与吞吐（每分钟一行）
-- -----------------------------------------------------------------------------
-- 这是判断「链路是不是还活着」的唯一事实来源。没有它，只能手工查 MAX(etl_time)。
-- 延迟口径统一为「当前墙钟 − 该层最新数据时间」，单位秒；
-- 某层完全没数据时写 NULL，不要写 -1（0 和 NULL 的语义完全不同）。
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ops_pipeline_latency (
    stat_minute   DATETIME     NOT NULL COMMENT '统计分钟（截断到分）',
    ods_lag_s     INT          NULL     COMMENT 'ODS 落地延迟 秒 = now - max(ods.etl_time)',
    dwd_lag_s     INT          NULL     COMMENT 'DWD 落地延迟 秒',
    dws_lag_s     INT          NULL     COMMENT 'DWS 窗口滞后 秒 = now - (max(window_start)+窗口长度)',
    ads_lag_s     INT          NULL     COMMENT 'ADS 窗口滞后 秒',
    input_rate    DOUBLE       NULL     COMMENT '入口速率 条/s（近 1 分钟 ODS 新增 / 60）',
    dwd_rows_1m   BIGINT       NULL     COMMENT '近 1 分钟 DWD 七表明细新增合计',
    kafka_lag_rows BIGINT      NULL     COMMENT 'Kafka 消费积压行数（B 的 records-lag-max 各 subtask 求和）。dwd_lag 检测不到积压，用它兜底',
    updated_at    DATETIME     NULL
) ENGINE=OLAP
UNIQUE KEY(stat_minute)
COMMENT 'OPS-链路延迟与吞吐'
PARTITION BY RANGE(stat_minute) ()
DISTRIBUTED BY HASH(stat_minute) BUCKETS 1
PROPERTIES (
    "replication_allocation" = "tag.location.default: 1",
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "DAY",
    "dynamic_partition.start" = "-30",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.buckets" = "1"
);

-- -----------------------------------------------------------------------------
-- 2) 作业健康（每分钟 × 每个作业一行）
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ops_job_health (
    stat_minute        DATETIME     NOT NULL COMMENT '统计分钟',
    job_name           VARCHAR(64)  NOT NULL COMMENT '作业名，如 rt_job_b_dwd',
    job_id             VARCHAR(64)  NULL     COMMENT 'Flink job id（32 位十六进制）',
    state              VARCHAR(16)  NULL     COMMENT 'RUNNING / FAILED / CANCELED / FINISHED',
    run_seconds        BIGINT       NULL     COMMENT '已运行秒数 = now - start-time',
    ckpt_size_mb       DOUBLE       NULL     COMMENT '最近一次成功 checkpoint 大小 MB',
    ckpt_age_s         INT          NULL     COMMENT '距最近一次成功 checkpoint 的秒数',
    ckpt_failed        INT          NULL     COMMENT '累计失败 checkpoint 次数',
    updated_at         DATETIME     NULL
) ENGINE=OLAP
UNIQUE KEY(stat_minute, job_name)
COMMENT 'OPS-作业健康'
PARTITION BY RANGE(stat_minute) ()
DISTRIBUTED BY HASH(stat_minute) BUCKETS 1
PROPERTIES (
    "replication_allocation" = "tag.location.default: 1",
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "DAY",
    "dynamic_partition.start" = "-30",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.buckets" = "1"
);

-- -----------------------------------------------------------------------------
-- 3) 数据质量（每分钟一行）
-- -----------------------------------------------------------------------------
-- 异常率 = 近 1 分钟进异常表的事件 / 近 1 分钟进入 DWD 的事件（含异常）。
-- 生成器按 3% 左右注入脏数据，实际值长期在 2~4% 之间；突然跳变说明规则或链路有问题。
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ops_data_quality (
    stat_minute    DATETIME     NOT NULL COMMENT '统计分钟',
    events_1m      BIGINT       NULL     COMMENT '近 1 分钟进入 DWD 的事件数（含被拦下的）',
    errors_1m      BIGINT       NULL     COMMENT '其中被质检规则拦下的条数',
    error_rate     DOUBLE       NULL     COMMENT '异常率 %',
    top_rule       VARCHAR(64)  NULL     COMMENT '近 1 分钟命中次数最多的规则',
    top_rule_cnt   BIGINT       NULL,
    updated_at     DATETIME     NULL
) ENGINE=OLAP
UNIQUE KEY(stat_minute)
COMMENT 'OPS-数据质量'
PARTITION BY RANGE(stat_minute) ()
DISTRIBUTED BY HASH(stat_minute) BUCKETS 1
PROPERTIES (
    "replication_allocation" = "tag.location.default: 1",
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "DAY",
    "dynamic_partition.start" = "-30",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.buckets" = "1"
);

-- -----------------------------------------------------------------------------
-- 验证
-- -----------------------------------------------------------------------------
-- SHOW TABLES LIKE 'ops_%';
-- SELECT * FROM ops_pipeline_latency ORDER BY stat_minute DESC LIMIT 10;
-- SELECT job_name, state, ckpt_age_s FROM ops_job_health
--   WHERE stat_minute = (SELECT MAX(stat_minute) FROM ops_job_health) ORDER BY job_name;


-- 会话指标（近 10 分钟微批）
-- 为什么放 ops 而不是 ads：会话是「行为归并」类指标，口径依赖 session_id 的粘性，
-- 与链路健康（延迟/积压/质量）同属「观察系统状态」的范畴，放一起便于统一维护。
CREATE TABLE IF NOT EXISTS ads_session_1min (
    stat_minute      DATETIME     NOT NULL COMMENT '统计分钟',
    session_cnt      BIGINT       NULL     COMMENT '近10分钟会话数',
    avg_evts_per_sess DOUBLE      NULL     COMMENT '平均每会话事件数',
    bounce_sess_cnt  BIGINT       NULL     COMMENT '跳出会话数（仅1条事件）',
    bounce_rate      DOUBLE       NULL     COMMENT '跳出率 %',
    updated_at       DATETIME     NULL
) ENGINE=OLAP
UNIQUE KEY(stat_minute)
COMMENT 'ADS-会话指标（近10分钟滚动，微批）'
DISTRIBUTED BY HASH(stat_minute) BUCKETS 1
PROPERTIES ("replication_allocation" = "tag.location.default: 1");
