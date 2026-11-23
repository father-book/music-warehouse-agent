-- =============================================================================
-- DWS 层聚合：从 dwd_play_end 汇总到三个窗口表
-- 执行：python code/run_sql.py sql/dws/doris_dws_aggregate.sql
-- =============================================================================
-- 窗口定义：
--   1 分钟窗口：FLOOR(ts/60)*60
--   5 分钟窗口：FLOOR(ts/300)*300（对齐到 00/05/10…）
-- 聚合口径：
--   uv       = COUNT(DISTINCT device_id)  —— 设备级去重，未登录用户也计入
--   login_uv = COUNT(DISTINCT user_id)    —— 仅登录用户
--   完播率/切歌率分母都是 play_cnt，不用 (play+skip)，因为 skip 本身是
--   play_end 的一种 end_reason，已包含在 play_cnt 中（口径见 docs/04）。
-- =============================================================================

USE music_dw;

-- -----------------------------------------------------------------------------
-- 1) 播放总览（1 分钟）
-- -----------------------------------------------------------------------------
INSERT INTO dws_play_overview_1min
SELECT
    t.dt,
    t.window_start,
    DATE_ADD(t.window_start, INTERVAL 1 MINUTE)          AS window_end,
    -- 计数一律 COUNT(DISTINCT event_id)：造数器按 0.6% 故意发重复 event_id
    -- （模拟客户端重传），dwd 明细是 DUPLICATE KEY 重复行并存。用 COUNT(*)
    -- 会比实时作业 C（COUNT DISTINCT）大 0.6%，批流两条路径就对不上账了。
    COUNT(DISTINCT t.event_id)                           AS play_cnt,
    COUNT(DISTINCT CASE WHEN t.end_reason = 'natural'   THEN t.event_id END) AS finish_cnt,
    COUNT(DISTINCT CASE WHEN t.end_reason = 'skip'      THEN t.event_id END) AS skip_cnt,
    COUNT(DISTINCT CASE WHEN t.end_reason = 'pause'     THEN t.event_id END) AS pause_cnt,
    COUNT(DISTINCT CASE WHEN t.end_reason = 'exit'      THEN t.event_id END) AS exit_cnt,
    COUNT(DISTINCT CASE WHEN t.end_reason = 'interrupt' THEN t.event_id END) AS interrupt_cnt,
    ROUND(COUNT(DISTINCT CASE WHEN t.end_reason = 'natural' THEN t.event_id END)
          * 100.0 / NULLIF(COUNT(DISTINCT t.event_id), 0), 2) AS finish_rate,
    ROUND(COUNT(DISTINCT CASE WHEN t.end_reason = 'skip' THEN t.event_id END)
          * 100.0 / NULLIF(COUNT(DISTINCT t.event_id), 0), 2) AS skip_rate,
    ROUND(AVG(t.play_duration), 1)                       AS avg_play_duration,
    ROUND(AVG(t.play_progress), 4)                       AS avg_play_progress,
    COUNT(DISTINCT t.device_id)                          AS uv,
    COUNT(DISTINCT CASE WHEN t.user_id <> '' THEN t.user_id END) AS login_uv
FROM (
    -- 播放终止流 = play_end ∪ skip：一次起播只有这两种结局（见 config/event_weights.json
    -- 的 _derived.skip_rate = skip/(play_end+skip)）。只取 dwd_play_end 会让
    -- skip_cnt 恒为 0、完播率被抬高，批式重算与实时作业 C 也会随口径分叉。
    SELECT dt,
           FROM_UNIXTIME(FLOOR(UNIX_TIMESTAMP(event_ts) / 60) * 60) AS window_start,
           event_id, end_reason, play_duration, play_progress, device_id, user_id
    FROM dwd_play_end
    UNION ALL
    SELECT dt,
           FROM_UNIXTIME(FLOOR(UNIX_TIMESTAMP(event_ts) / 60) * 60) AS window_start,
           event_id, 'skip' AS end_reason,
           played_duration AS play_duration,
           -- dwd_skip 没有 song_duration 列（只有 played_duration），算不出进度。
           -- 实时侧 job_c_dws.sql 与 collect_ops_metrics.py 也都是 NULL，
           -- 三处必须一致，否则批流两边的 avg_play_progress 会分叉。
           CAST(NULL AS DOUBLE) AS play_progress,
           device_id, user_id
    FROM dwd_skip
) t
GROUP BY t.dt, t.window_start;

-- -----------------------------------------------------------------------------
-- 2) 单曲热度（5 分钟）
-- -----------------------------------------------------------------------------
INSERT INTO dws_song_play_5min
SELECT
    t.dt,
    t.window_start,
    t.song_id,
    COUNT(DISTINCT t.event_id)                           AS play_cnt,
    COUNT(DISTINCT CASE WHEN t.end_reason = 'natural' THEN t.event_id END) AS finish_cnt,
    COUNT(DISTINCT CASE WHEN t.end_reason = 'skip'    THEN t.event_id END) AS skip_cnt,
    ROUND(COUNT(DISTINCT CASE WHEN t.end_reason = 'skip' THEN t.event_id END)
          * 100.0 / NULLIF(COUNT(DISTINCT t.event_id), 0), 2) AS skip_rate,
    ROUND(AVG(t.play_progress), 4)                       AS avg_play_progress,
    COUNT(DISTINCT t.device_id)                          AS uv
FROM (
    -- 同 1) 的口径：skip 也是播放终止的一种，必须并入
    SELECT dt,
           FROM_UNIXTIME(FLOOR(UNIX_TIMESTAMP(event_ts) / 300) * 300) AS window_start,
           song_id, event_id, end_reason, play_progress, device_id
    FROM dwd_play_end
    UNION ALL
    SELECT dt,
           FROM_UNIXTIME(FLOOR(UNIX_TIMESTAMP(event_ts) / 300) * 300) AS window_start,
           song_id, event_id, 'skip' AS end_reason,
           -- 同 1) 段：dwd_skip 没有 song_duration，进度只能记 NULL
           CAST(NULL AS DOUBLE) AS play_progress,
           device_id
    FROM dwd_skip
) t
GROUP BY t.dt, t.window_start, t.song_id;

-- -----------------------------------------------------------------------------
-- 3) 地域分布（5 分钟）
-- -----------------------------------------------------------------------------
INSERT INTO dws_province_play_5min
SELECT
    t.dt,
    t.window_start,
    t.province,
    COUNT(DISTINCT t.event_id)                           AS play_cnt,
    COUNT(DISTINCT CASE WHEN t.end_reason = 'natural' THEN t.event_id END) AS finish_cnt,
    COUNT(DISTINCT CASE WHEN t.end_reason = 'skip'    THEN t.event_id END) AS skip_cnt,
    ROUND(COUNT(DISTINCT CASE WHEN t.end_reason = 'skip' THEN t.event_id END)
          * 100.0 / NULLIF(COUNT(DISTINCT t.event_id), 0), 2) AS skip_rate,
    COUNT(DISTINCT t.device_id)                          AS uv
FROM (
    -- 同 1) 的口径：skip 也是播放终止的一种，必须并入
    SELECT dt,
           FROM_UNIXTIME(FLOOR(UNIX_TIMESTAMP(event_ts) / 300) * 300) AS window_start,
           COALESCE(NULLIF(province, ''), '未知') AS province,
           event_id, end_reason, device_id
    FROM dwd_play_end
    UNION ALL
    SELECT dt,
           FROM_UNIXTIME(FLOOR(UNIX_TIMESTAMP(event_ts) / 300) * 300) AS window_start,
           COALESCE(NULLIF(province, ''), '未知') AS province,
           event_id, 'skip' AS end_reason, device_id
    FROM dwd_skip
) t
GROUP BY t.dt, t.window_start, t.province;

-- =============================================================================
-- 验证
-- =============================================================================
-- 1. 窗口数一致性：1 分钟表行数应远多于 5 分钟表
--    SELECT (SELECT COUNT(*) FROM dws_play_overview_1min)   AS m1_cnt,
--           (SELECT COUNT(*) FROM dws_song_play_5min)       AS m5_song_cnt,
--           (SELECT COUNT(*) FROM dws_province_play_5min)   AS m5_prov_cnt;
--
-- 2. 总量对账：DWS 的 play_cnt 之和应等于 DWD 行数（窗口不重叠时）
--    SELECT SUM(play_cnt) FROM dws_play_overview_1min;
--    SELECT COUNT(*) FROM dwd_play_end;
--
-- 3. 最近 10 分钟趋势
--    SELECT window_start, play_cnt, uv, finish_rate, skip_rate
--    FROM dws_play_overview_1min
--    ORDER BY window_start DESC LIMIT 10;
--
-- 4. 实时热榜 Top 10
--    SELECT song_id, SUM(play_cnt) AS plays
--    FROM dws_song_play_5min
--    GROUP BY song_id ORDER BY plays DESC LIMIT 10;
