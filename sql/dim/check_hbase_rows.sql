-- =============================================================================
-- DIM 层行级对账：把 HBase 里的 dim_song 全量读回 Doris，与镜像表逐行比对
--
-- 为什么需要它：
--   作业 B 的维度命中率实测 98.39%，未命中的 song_id 在 Doris 镜像表里都存在。
--   要判断问题出在「装载丢行」还是「lookup 偶发失败」，必须看 HBase 里到底有什么。
--   HBase 的 scan 结果没法用 Gateway 的 SELECT 取回（网关取结果集有 bug），
--   所以用一次 bounded 作业把 HBase 落到 Doris，再用 SQL 做集合差。
--
-- 输出表：music_dw.dim_song_hbase_check（DUPLICATE 模型，每次重跑前先 TRUNCATE）
-- 用法：python code/flink_gateway.py sql/dim/check_hbase_rows.sql
--
-- 注意：Doris sink 的 Stream Load label 由 (prefix, db, table, checkpointId, subtask)
--   拼成，bounded 作业的 checkpointId 不递增，**同一作业重复提交必报
--   LABEL_ALREADY_EXISTS**。重跑前请给 sink 加一个唯一的
--   'sink.label-prefix'（例如带时间戳），或每次换一张目标表。
-- =============================================================================

SET 'parallelism.default' = '1';
SET 'pipeline.jars' = 'file:///opt/module/flink/lib/flink-sql-connector-hbase-2.2-1.17.1.jar';

CREATE TABLE hb_src_song (
    rowkey STRING,
    cf ROW<song_id STRING, song_name STRING, artist_id STRING, artist_name STRING,
          artist_country STRING, album_id STRING, album_name STRING,
          genre STRING, duration STRING, release_date STRING, mbid STRING>,
    PRIMARY KEY (rowkey) NOT ENFORCED
) WITH (
    'connector' = 'hbase-2.2',
    'table-name' = 'dim_song',
    'zookeeper.quorum' = 'WWC161:2181,WWC162:2181,WWC163:2181',
    'zookeeper.znode.parent' = '/hbase'
);

CREATE TABLE sink_dim_song_check (
    song_id STRING, song_name STRING, artist_name STRING, genre STRING, album_name STRING
) WITH (
    'connector' = 'doris',
    'fenodes' = 'WWC163:8030',
    'table.identifier' = 'music_dw.dim_song_hbase_check',
    'username' = 'root',
    'password' = ''
);

INSERT INTO sink_dim_song_check
SELECT cf.song_id, cf.song_name, cf.artist_name, cf.genre, cf.album_name
FROM hb_src_song;
