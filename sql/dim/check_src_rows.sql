-- =============================================================================
-- 定位装载丢行的位置：Doris 镜像表 → Doris 对账表（不经过 HBase）
--   若结果也是 1424 行 → 丢在 Doris connector 的 source 侧
--   若是 1486 行     → 丢在 HBase sink 侧
-- 用法：python code/flink_gateway.py sql/dim/check_src_rows.sql
-- 注意：bounded 作业重复提交会撞 Doris 的 LABEL_ALREADY_EXISTS，
--       重跑前给 sink 换一个唯一的 'sink.label-prefix' 或换目标表。
-- =============================================================================
SET 'parallelism.default' = '1';

CREATE TABLE src_dim_song_probe (
    song_id STRING, song_name STRING, artist_id STRING, album_id STRING,
    genre STRING, duration INT, release_date DATE
) WITH (
    'connector' = 'doris',
    'fenodes' = 'WWC163:8030',
    'table.identifier' = 'music_dw.dim_song',
    'username' = 'root',
    'password' = ''
);

CREATE TABLE sink_dim_song_src_check (
    song_id STRING, song_name STRING, artist_id STRING, album_id STRING,
    genre STRING, duration INT, release_date DATE
) WITH (
    'connector' = 'doris',
    'fenodes' = 'WWC163:8030',
    'table.identifier' = 'music_dw.dim_song_src_check',
    'username' = 'root',
    'password' = ''
);

INSERT INTO sink_dim_song_src_check
SELECT song_id, song_name, artist_id, album_id, genre, duration, release_date
FROM src_dim_song_probe;
