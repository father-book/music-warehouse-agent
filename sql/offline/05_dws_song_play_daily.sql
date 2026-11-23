-- =============================================================================
-- 05_dws_song_play_daily.sql —— DWS：歌曲日粒度播放/完播聚合
-- 执行：hive --hivevar start_dt=2026-10-05 --hivevar end_dt=2026-11-03 -f 05_dws_song_play_daily.sql
--
-- 粒度：dt × song_id（一首歌一天一行）
--   30 天 × 1486 首 ≈ 4.46 万行 —— 这是 DWS 的价值：从 3000 万行压到 4.46 万行，
--   下游 ADS 查「Top 歌曲」「完播率分布」直接扫这张，不再碰明细。
--
-- 指标口径（答辩必问，先写死在这里）：
--   play_start_cnt  起播次数                  ← dwd_play_start
--   play_end_cnt    结束次数（含播完+中断）    ← dwd_play_end
--   complete_cnt    完播次数（progress ≥ 0.9） ← dwd_play_end
--   complete_rate   完播率 = complete_cnt / play_start_cnt
--   avg_progress    平均播放进度
--
--   ⚠ 分母是「起播次数」，不是「结束次数」：
--     跳过的事件不产生 play_end，用结束次数作分母会把跳过的人剔除掉，
--     完播率虚高。本数据里起播 750 万、结束 525 万，两者差 43%。
--
-- 为什么用 FULL OUTER JOIN：
--   存在「只有起播、没有结束」的歌（用户中途退出 App，没写结束事件）。
--   INNER JOIN 会把这些歌整首丢掉，导致长尾歌曲的完播率被系统性高估。
--   FULL OUTER JOIN + NVL 保住它们，完播率算出来是 0 —— 这才是真实的。
-- =============================================================================

SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;
SET hive.exec.max.dynamic.partitions = 200;
SET hive.exec.max.dynamic.partitions.pernode = 100;

CREATE EXTERNAL TABLE IF NOT EXISTS music_dw.dws_song_play_daily
(
    song_id            STRING  COMMENT '歌曲ID',
    play_start_cnt     BIGINT  COMMENT '起播次数（完播率分母）',
    play_end_cnt       BIGINT  COMMENT '结束次数（含播完与中断）',
    complete_cnt       BIGINT  COMMENT '完播次数（progress >= 0.9）',
    complete_rate      DOUBLE  COMMENT '完播率 = complete_cnt / play_start_cnt',
    skip_play_cnt      BIGINT  COMMENT '未播完次数 = 起播 - 完播',
    avg_progress       DOUBLE  COMMENT '平均播放进度',
    avg_play_duration  DOUBLE  COMMENT '平均实际播放时长（秒）'
)
COMMENT 'DWS-歌曲日粒度播放与完播指标'
PARTITIONED BY (dt STRING COMMENT '统计日期 yyyy-MM-dd')
STORED AS ORC
LOCATION 'hdfs://WWC160:9820/warehouse/music_dw/dws_song_play_daily'
TBLPROPERTIES ('orc.compress' = 'SNAPPY');


INSERT OVERWRITE TABLE music_dw.dws_song_play_daily PARTITION (dt)
SELECT
    COALESCE(s.song_id, e.song_id)                                  AS song_id,
    NVL(s.start_cnt, 0)                                             AS play_start_cnt,
    NVL(e.end_cnt, 0)                                               AS play_end_cnt,
    NVL(e.complete_cnt, 0)                                          AS complete_cnt,
    ROUND(NVL(e.complete_cnt, 0) / NULLIF(s.start_cnt, 0), 4)       AS complete_rate,
    NVL(s.start_cnt, 0) - NVL(e.complete_cnt, 0)                    AS skip_play_cnt,
    ROUND(e.avg_progress, 4)                                        AS avg_progress,
    ROUND(e.avg_play_duration, 2)                                   AS avg_play_duration,
    COALESCE(s.dt, e.dt)                                            AS dt
FROM (
    SELECT dt,
           song_id,
           COUNT(*) AS start_cnt
    FROM music_dw.dwd_play_start
    WHERE dt BETWEEN '${start_dt}' AND '${end_dt}'
      AND song_id IS NOT NULL
    GROUP BY dt, song_id
) s
FULL OUTER JOIN (
    SELECT dt,
           song_id,
           COUNT(*)                                AS end_cnt,
           SUM(IF(play_progress >= 0.9, 1, 0))     AS complete_cnt,
           AVG(play_progress)                      AS avg_progress,
           AVG(play_duration)                      AS avg_play_duration
    FROM music_dw.dwd_play_end
    WHERE dt BETWEEN '${start_dt}' AND '${end_dt}'
      AND song_id IS NOT NULL
    GROUP BY dt, song_id
) e
ON s.dt = e.dt AND s.song_id = e.song_id
DISTRIBUTE BY dt;


-- ---------------------------------------------------------------------------
-- 验证 1：整体完播率（加权口径，不是平均单曲完播率）
--   注意区分：SUM(complete)/SUM(start) 是「加权完播率」，
--             AVG(complete_rate) 是「单曲完播率的平均」，两者不等价。
--   报表用前者（反映整体播放体验），按歌曲分析用后者。
-- ---------------------------------------------------------------------------
SELECT dt,
       COUNT(*)                                                   AS song_cnt,
       SUM(play_start_cnt)                                        AS start_total,
       SUM(complete_cnt)                                          AS complete_total,
       ROUND(SUM(complete_cnt) / SUM(play_start_cnt), 4)          AS weighted_complete_rate,
       ROUND(AVG(complete_rate), 4)                               AS avg_song_complete_rate,
       -- 【2026-11-09 修正】原来是 AVG(avg_progress)，即「每首歌平均进度的平均」。
       -- 上面两列本来就是「加权 vs 等权」的对照，唯独这一列只给了等权版，
       -- 于是它被当成大盘进度用进了 ads_daily_overview，让长尾歌曲（起播 1~2 次）
       -- 和热门歌曲等权。按结束次数加权后才等于全体结束事件的进度均值。
       ROUND(SUM(avg_progress * play_end_cnt) / NULLIF(SUM(play_end_cnt), 0), 4)
                                                                  AS weighted_avg_progress
FROM music_dw.dws_song_play_daily
WHERE dt BETWEEN '${start_dt}' AND '${end_dt}'
GROUP BY dt ORDER BY dt;
