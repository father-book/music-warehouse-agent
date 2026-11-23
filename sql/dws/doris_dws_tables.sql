-- =============================================================================
-- DWS 层建表：三个聚合表（1 分钟全局 / 5 分钟单曲 / 5 分钟地域）
-- 执行：python code/run_sql.py sql/dws/doris_dws_tables.sql
-- =============================================================================
-- 模型选择：UNIQUE KEY（按窗口覆盖写）
--   同一窗口重算时，同 key 的新行会覆盖旧行，天然支持「回溯重跑」。
--   若用 Duplicate，重跑会累加出重复数据；若用 Aggregate，无法回退旧值。
--
-- 分区：按 dt 天分区，动态保留 365 天。
-- 分桶：按 window_start 哈希，避免热点集中在少数 tablet。
-- =============================================================================

USE music_dw;

-- -----------------------------------------------------------------------------
-- 1) 播放总览（1 分钟粒度，全局无维度）
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dws_play_overview_1min (
    dt                DATE         NOT NULL COMMENT '日期，分区键',
    window_start      DATETIME     NOT NULL COMMENT '窗口开始',
    window_end        DATETIME     NULL     COMMENT '窗口结束',
    play_cnt          BIGINT       NULL     COMMENT '播放结束事件数',
    finish_cnt        BIGINT       NULL     COMMENT '自然结束（完播）',
    skip_cnt          BIGINT       NULL     COMMENT '切歌',
    pause_cnt         BIGINT       NULL     COMMENT '暂停',
    exit_cnt          BIGINT       NULL     COMMENT '退出',
    interrupt_cnt     BIGINT       NULL     COMMENT '中断（来电等）',
    finish_rate       DOUBLE       NULL     COMMENT '完播率 = finish/play',
    skip_rate         DOUBLE       NULL     COMMENT '切歌率 = skip/play',
    avg_play_duration DOUBLE       NULL     COMMENT '平均播放时长（秒）',
    avg_play_progress DOUBLE       NULL     COMMENT '平均播放进度 0~1',
    uv                BIGINT       NULL     COMMENT '去重设备数',
    login_uv          BIGINT       NULL     COMMENT '去重登录用户数'
) ENGINE=OLAP
UNIQUE KEY(dt, window_start)
COMMENT 'DWS-播放总览-1分钟'
PARTITION BY RANGE(dt) ()
DISTRIBUTED BY HASH(window_start) BUCKETS 4
PROPERTIES (
    "replication_allocation" = "tag.location.default: 1",
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "DAY",
    "dynamic_partition.start" = "-365",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.buckets" = "4"
);

-- -----------------------------------------------------------------------------
-- 2) 单曲热度（5 分钟粒度）
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dws_song_play_5min (
    dt                DATE         NOT NULL COMMENT '日期，分区键',
    window_start      DATETIME     NOT NULL COMMENT '窗口开始',
    song_id           VARCHAR(32)  NOT NULL COMMENT '歌曲ID',
    play_cnt          BIGINT       NULL,
    finish_cnt        BIGINT       NULL,
    skip_cnt          BIGINT       NULL,
    skip_rate         DOUBLE       NULL,
    avg_play_progress DOUBLE       NULL,
    uv                BIGINT       NULL     COMMENT '去重设备数'
) ENGINE=OLAP
UNIQUE KEY(dt, window_start, song_id)
COMMENT 'DWS-单曲热度-5分钟'
PARTITION BY RANGE(dt) ()
DISTRIBUTED BY HASH(song_id) BUCKETS 8
PROPERTIES (
    "replication_allocation" = "tag.location.default: 1",
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "DAY",
    "dynamic_partition.start" = "-365",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.buckets" = "8"
);

-- -----------------------------------------------------------------------------
-- 3) 地域播放分布（5 分钟粒度）
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dws_province_play_5min (
    dt                DATE         NOT NULL COMMENT '日期，分区键',
    window_start      DATETIME     NOT NULL COMMENT '窗口开始',
    province          VARCHAR(32)  NOT NULL COMMENT '省份',
    play_cnt          BIGINT       NULL,
    finish_cnt        BIGINT       NULL,
    skip_cnt          BIGINT       NULL,
    skip_rate         DOUBLE       NULL,
    uv                BIGINT       NULL
) ENGINE=OLAP
UNIQUE KEY(dt, window_start, province)
COMMENT 'DWS-地域播放-5分钟'
PARTITION BY RANGE(dt) ()
DISTRIBUTED BY HASH(province) BUCKETS 4
PROPERTIES (
    "replication_allocation" = "tag.location.default: 1",
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "DAY",
    "dynamic_partition.start" = "-365",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.buckets" = "4"
);
