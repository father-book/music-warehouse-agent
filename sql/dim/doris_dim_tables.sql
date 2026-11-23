-- =============================================================================
-- DIM 层建表（Doris）
--
-- 【定位说明，避免误解】
--   按 ADR-007，DIM 层最终应落在 HBase，用 UDF 点查做维表关联。
--   本文件建的是 Doris 侧的**镜像表**，用途有三个：
--     1. HBase 尚未部署完成前的过渡查询层（大屏要显示歌名，不能只有 song_id）
--     2. 维表数据的校验基准（CSV 导入后先在 Doris 里核对条数与字段）
--     3. 对比实验：Doris Lookup Join 与 HBase UDF 点查的性能、实现成本差异
--
--   维表写入后基本不变，属于典型的「一次装载、长期点查」型数据，
--   因此用 UNIQUE KEY 模型（同主键覆盖），不分动态分区（维表不按时间切分）。
--
-- 执行：python code/run_sql.py sql/dim/doris_dim_tables.sql
-- =============================================================================

USE music_dw;

-- -----------------------------------------------------------------------------
-- 1) 歌曲维表（真实歌曲，来自 MusicBrainz）
-- -----------------------------------------------------------------------------
DROP TABLE IF EXISTS dim_song;
CREATE TABLE dim_song (
    song_id       VARCHAR(16)  NOT NULL COMMENT '歌曲ID',
    song_name     VARCHAR(128) NULL     COMMENT '歌名',
    artist_id     VARCHAR(16)  NULL     COMMENT '歌手ID',
    album_id      VARCHAR(16)  NULL     COMMENT '专辑ID',
    genre         VARCHAR(32)  NULL     COMMENT '曲风',
    duration      INT          NULL     COMMENT '时长（秒）',
    release_date  DATE         NULL     COMMENT '发行日期'
) ENGINE=OLAP
UNIQUE KEY(song_id)
COMMENT 'DIM-歌曲维度（真实数据）'
DISTRIBUTED BY HASH(song_id) BUCKETS 4
PROPERTIES ("replication_allocation" = "tag.location.default: 1");

-- -----------------------------------------------------------------------------
-- 2) 歌手维表
-- -----------------------------------------------------------------------------
DROP TABLE IF EXISTS dim_artist;
CREATE TABLE dim_artist (
    artist_id    VARCHAR(16)  NOT NULL,
    artist_name  VARCHAR(64)  NULL COMMENT '歌手名',
    country      VARCHAR(32)  NULL COMMENT '国家/地区',
    genre        VARCHAR(32)  NULL COMMENT '主要风格',
    mbid         VARCHAR(64)  NULL COMMENT 'MusicBrainz ID，可回溯核对',
    type         VARCHAR(16)  NULL COMMENT 'Person / Group'
) ENGINE=OLAP
UNIQUE KEY(artist_id)
COMMENT 'DIM-歌手维度（真实数据）'
DISTRIBUTED BY HASH(artist_id) BUCKETS 2
PROPERTIES ("replication_allocation" = "tag.location.default: 1");

-- -----------------------------------------------------------------------------
-- 3) 专辑维表
-- -----------------------------------------------------------------------------
DROP TABLE IF EXISTS dim_album;
CREATE TABLE dim_album (
    album_id      VARCHAR(16)  NOT NULL,
    album_name    VARCHAR(128) NULL,
    artist_id     VARCHAR(16)  NULL,
    release_date  DATE         NULL
) ENGINE=OLAP
UNIQUE KEY(album_id)
COMMENT 'DIM-专辑维度（真实数据）'
DISTRIBUTED BY HASH(album_id) BUCKETS 2
PROPERTIES ("replication_allocation" = "tag.location.default: 1");

-- -----------------------------------------------------------------------------
-- 4) 歌单维表（歌单本身虚构，曲目为真实歌曲）
-- -----------------------------------------------------------------------------
DROP TABLE IF EXISTS dim_playlist;
CREATE TABLE dim_playlist (
    playlist_id      VARCHAR(16)  NOT NULL,
    playlist_name    VARCHAR(64)  NULL,
    creator_user_id  VARCHAR(16)  NULL,
    genre            VARCHAR(32)  NULL
) ENGINE=OLAP
UNIQUE KEY(playlist_id)
COMMENT 'DIM-歌单维度（歌单虚构，曲目真实）'
DISTRIBUTED BY HASH(playlist_id) BUCKETS 2
PROPERTIES ("replication_allocation" = "tag.location.default: 1");

-- -----------------------------------------------------------------------------
-- 5) 歌单-歌曲关系表（多对多桥接表）
-- -----------------------------------------------------------------------------
DROP TABLE IF EXISTS dim_playlist_song;
CREATE TABLE dim_playlist_song (
    playlist_id  VARCHAR(16) NOT NULL,
    song_id      VARCHAR(16) NOT NULL,
    seq          INT         NULL COMMENT '歌单内顺序'
) ENGINE=OLAP
UNIQUE KEY(playlist_id, song_id)
COMMENT 'DIM-歌单歌曲关系'
DISTRIBUTED BY HASH(playlist_id) BUCKETS 2
PROPERTIES ("replication_allocation" = "tag.location.default: 1");

-- -----------------------------------------------------------------------------
-- 6) 用户维表（虚构，本来就是用户画像数据）
-- -----------------------------------------------------------------------------
DROP TABLE IF EXISTS dim_user;
CREATE TABLE dim_user (
    user_id        VARCHAR(16) NOT NULL,
    nickname       VARCHAR(64) NULL,
    age            INT         NULL,
    gender         CHAR(1)     NULL COMMENT 'M / F',
    city           VARCHAR(32) NULL,
    register_time  DATETIME    NULL,
    vip_level      INT         NULL COMMENT '0 普通 / 1-3 会员等级'
) ENGINE=OLAP
UNIQUE KEY(user_id)
COMMENT 'DIM-用户维度'
DISTRIBUTED BY HASH(user_id) BUCKETS 4
PROPERTIES ("replication_allocation" = "tag.location.default: 1");

-- =============================================================================
-- 验证
-- =============================================================================
-- SELECT COUNT(*) FROM dim_song;
-- SELECT s.song_id, s.song_name, a.artist_name, al.album_name
-- FROM dim_song s
-- LEFT JOIN dim_artist a ON s.artist_id = a.artist_id
-- LEFT JOIN dim_album  al ON s.album_id = al.album_id
-- LIMIT 10;
