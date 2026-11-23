-- =============================================================================
-- ADS 层聚合：从 DWS / DWD 汇总出大屏可直接查询的结果
-- 执行：python code/run_sql.py sql/ads/doris_ads_aggregate.sql
-- =============================================================================
-- 口径说明（重要）：
--   1. 累计 UV 必须回明细去重。DWS 每窗口的 uv 是窗口内去重的，
--      跨窗口相加会把同一用户重复计算。这是「不可加指标」的典型陷阱。
--   2. 榜单/地域表的 uv 采用「各窗口 UV 之和」作为近似值，字段注释已标注。
--      需要精确值时应从 dwd_play_end 单独 COUNT(DISTINCT device_id)。
--   3. 先 TRUNCATE 再 INSERT：均为按周期覆盖的批式重算，重跑幂等。
-- =============================================================================

USE music_dw;

-- -----------------------------------------------------------------------------
-- 1) 实时总览：今日累计
-- -----------------------------------------------------------------------------
TRUNCATE TABLE ads_realtime_overview;

INSERT INTO ads_realtime_overview
SELECT
    t.dt                                                        AS stat_date,
    COUNT(*)                                                    AS total_play,
    COUNT(DISTINCT t.device_id)                                 AS total_uv,
    COUNT(DISTINCT CASE WHEN t.user_id <> '' THEN t.user_id END) AS login_uv,
    ROUND(SUM(CASE WHEN t.end_reason = 'natural' THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) AS finish_rate,
    ROUND(SUM(CASE WHEN t.end_reason = 'skip'    THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) AS skip_rate,
    ROUND(AVG(t.play_duration), 1)                              AS avg_play_duration,
    COUNT(DISTINCT FLOOR(UNIX_TIMESTAMP(t.event_ts) / 60))      AS active_minutes,
    MAX(t.event_ts)                                             AS last_event_ts,
    NOW()                                                       AS updated_at
FROM (
    -- 播放终止流 = play_end ∪ skip（口径同作业 C / 批式 DWS）
    SELECT dt, event_ts, device_id, user_id, end_reason, play_duration
    FROM dwd_play_end
    UNION ALL
    SELECT dt, event_ts, device_id, user_id, 'skip' AS end_reason,
           played_duration AS play_duration
    FROM dwd_skip
) t
GROUP BY t.dt;

-- -----------------------------------------------------------------------------
-- 2) 10 分钟滚动热榜 Top20（与流式作业 D 的 HOP 窗口口径对齐）
--    数据源是 5 分钟粒度的 DWS，按 10 分钟对齐后聚合，再在窗口内排名。
-- -----------------------------------------------------------------------------
TRUNCATE TABLE ads_song_rank_10min;

INSERT INTO ads_song_rank_10min
SELECT window_start, rank_no, song_id, play_cnt, uv, skip_rate, NOW()
FROM (
    SELECT
        h.window_start,
        ROW_NUMBER() OVER (PARTITION BY h.window_start ORDER BY h.play_cnt DESC, h.song_id) AS rank_no,
        h.song_id,
        h.play_cnt,
        h.uv,
        h.skip_rate
    FROM (
        SELECT
            FROM_UNIXTIME(FLOOR(UNIX_TIMESTAMP(window_start) / 600) * 600) AS window_start,
            song_id,
            SUM(play_cnt)                             AS play_cnt,
            SUM(uv)                                   AS uv,
            ROUND(SUM(skip_cnt) / SUM(play_cnt) * 100, 2) AS skip_rate
        FROM dws_song_play_5min
        GROUP BY 1, 2
    ) h
) r
WHERE r.rank_no <= 20;

-- -----------------------------------------------------------------------------
-- 3) 10 分钟滚动地域分布（与流式作业 D 的 HOP 10 分钟窗口口径对齐）
-- -----------------------------------------------------------------------------
TRUNCATE TABLE ads_province_10min;

INSERT INTO ads_province_10min
SELECT
    p.window_start,
    p.province,
    p.play_cnt,
    p.uv,
    ROUND(p.play_cnt / SUM(p.play_cnt) OVER (PARTITION BY p.window_start) * 100, 2) AS play_share,
    NOW()
FROM (
    SELECT
        FROM_UNIXTIME(FLOOR(UNIX_TIMESTAMP(window_start) / 600) * 600) AS window_start,
        province,
        SUM(play_cnt) AS play_cnt,
        SUM(uv)       AS uv
    FROM dws_province_play_5min
    GROUP BY 1, 2
) p;

-- =============================================================================
-- 验证
-- =============================================================================
-- 1. 总览（大屏顶部数字）
--    SELECT * FROM ads_realtime_overview\G
--
-- 2. 最新 10 分钟热榜（HOP 10min/1min 滑动窗口）
--    SELECT rank_no, song_id, play_cnt, skip_rate
--    FROM ads_song_rank_10min
--    WHERE window_start = (SELECT MAX(window_start) FROM ads_song_rank_10min)
--    ORDER BY rank_no;
--
-- 3. 地域占比之和应为 100%
--    SELECT window_start, SUM(play_share) FROM ads_province_10min GROUP BY window_start;
