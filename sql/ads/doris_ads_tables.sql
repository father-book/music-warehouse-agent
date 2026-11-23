-- =============================================================================
-- ADS 层建表：面向大屏的三张结果表
-- 执行：python code/run_sql.py sql/ads/doris_ads_tables.sql
-- =============================================================================
-- 定位：ADS 不是"再聚合一次"，而是「大屏一次查询就能出图」的成品表。
--   大屏绝不查 DWD 明细，也不做多表 JOIN —— 那是 DWS/ADS 该干的活。
--
-- 模型：UNIQUE KEY，按统计周期覆盖写，重算幂等。
-- 更新方式：先 TRUNCATE 再 INSERT（批式重算），数据量小，成本可忽略。
-- =============================================================================

USE music_dw;

-- -----------------------------------------------------------------------------
-- 1) 实时总览：今日累计（大屏顶部四个数字）
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ads_realtime_overview (
    stat_date         DATE         NOT NULL COMMENT '统计日期',
    total_play        BIGINT       NULL     COMMENT '累计播放量',
    total_uv          BIGINT       NULL     COMMENT '累计去重设备数（不可由 DWS 相加，须回明细去重）',
    login_uv          BIGINT       NULL     COMMENT '累计去重登录用户数',
    finish_rate       DOUBLE       NULL     COMMENT '完播率 %',
    skip_rate         DOUBLE       NULL     COMMENT '切歌率 %',
    avg_play_duration DOUBLE       NULL     COMMENT '平均播放时长（秒）',
    active_minutes    INT          NULL     COMMENT '有数据的分钟数',
    last_event_ts     DATETIME     NULL     COMMENT '最后一条事件时间',
    updated_at        DATETIME     NULL     COMMENT '本行重算时间'
) ENGINE=OLAP
UNIQUE KEY(stat_date)
COMMENT 'ADS-实时总览'
PARTITION BY RANGE(stat_date) ()
DISTRIBUTED BY HASH(stat_date) BUCKETS 1
PROPERTIES (
    "replication_allocation" = "tag.location.default: 1",
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "DAY",
    "dynamic_partition.start" = "-365",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.buckets" = "1"
);

-- -----------------------------------------------------------------------------
-- 2) 10 分钟滚动热榜 Top20（大屏右侧榜单）
-- -----------------------------------------------------------------------------
-- 为什么不是「小时榜」：
--   TUMBLE 1 小时只在整点过后才吐一次结果，答辩演示时榜单会长时间纹丝不动，
--   大屏看着像没在跑。改成 10 分钟窗口后，每分钟都在刷新。
-- 为什么用 HOP 而不是 TUMBLE 10 分钟：
--   TUMBLE 10 分钟每 10 分钟才出一次（00/10/20…），仍然不够实时；
--   HOP(1 分钟滑动, 10 分钟窗口) 每分钟关闭一个窗口，窗口内滚动覆盖最近 10 分钟，
--   是真正的「近 10 分钟滚动榜」，代价是同时存在 10 个窗口的状态（几千行，可忽略）。
-- 行数估算：每窗口 20 行 × 1440 分钟 = 2.9 万行/天，故只保留 7 天。
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ads_song_rank_10min (
    window_start DATETIME     NOT NULL COMMENT '10 分钟窗口起点',
    rank_no      INT          NOT NULL COMMENT '窗口内排名 1~20',
    song_id      VARCHAR(32)  NOT NULL,
    play_cnt     BIGINT       NULL     COMMENT '窗口内播放次数',
    uv           BIGINT       NULL     COMMENT '窗口内该歌去重设备数',
    skip_rate    DOUBLE       NULL     COMMENT '窗口内切歌率 %',
    updated_at   DATETIME     NULL
) ENGINE=OLAP
UNIQUE KEY(window_start, rank_no)
COMMENT 'ADS-10分钟滚动热榜Top20'
PARTITION BY RANGE(window_start) ()
DISTRIBUTED BY HASH(window_start) BUCKETS 1
PROPERTIES (
    "replication_allocation" = "tag.location.default: 1",
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "DAY",
    "dynamic_partition.start" = "-7",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.buckets" = "1"
);

-- -----------------------------------------------------------------------------
-- 3) 10 分钟滚动地域分布（大屏地图）
-- -----------------------------------------------------------------------------
-- 为什么不是「小时地域」：TUMBLE 1 小时要等整点才吐结果，演示时地图几十分钟
-- 不变色，和榜单原来的问题一模一样。改成 HOP 后每分钟关闭一个窗口，
-- 窗口内覆盖最近 10 分钟，地图每分钟变一次色。
-- 口径：分省播放量 ÷ 同窗口全国播放量（各省 play_share 之和 = 100）。
-- 行数估算：每窗口 ~34 行 × 1440 分钟 ≈ 4.9 万行/天，故只保留 7 天。
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ads_province_10min (
    window_start DATETIME     NOT NULL COMMENT '10 分钟窗口起点',
    province     VARCHAR(32)  NOT NULL,
    play_cnt     BIGINT       NULL,
    uv           BIGINT       NULL,
    play_share   DOUBLE       NULL     COMMENT '占同窗口全国播放量比例 %',
    updated_at   DATETIME     NULL
) ENGINE=OLAP
UNIQUE KEY(window_start, province)
COMMENT 'ADS-10分钟滚动地域分布'
PARTITION BY RANGE(window_start) ()
DISTRIBUTED BY HASH(window_start) BUCKETS 1
PROPERTIES (
    "replication_allocation" = "tag.location.default: 1",
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "DAY",
    "dynamic_partition.start" = "-7",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.buckets" = "1"
);
