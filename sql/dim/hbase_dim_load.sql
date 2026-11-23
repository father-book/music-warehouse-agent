-- =============================================================================
-- DIM 层装载：Doris 镜像表 → HBase（一次性 bounded 作业，纯 Flink SQL）
--
-- 为什么这么装：
--   ADR-007 定的「维表入 HBase 用 Flink SQL 的 hbase-2.2 sink」不变，
--   但源不用传 CSV 文件——直接读已经校验过的 Doris 镜像表，
--   同一份数据两种存储，装载完可直接对账，这正是论文里「双存储对照」的素材。
--
-- 为什么 dim_song 要预关联歌手/专辑：
--   流式侧只需要「song_id → 歌名 + 歌手 + 曲风 + 专辑」这一次点查。
--   若把 artist 单独放一张表，作业 B 就得做两级 LOOKUP JOIN（song→artist_id→artist），
--   每条事件两次 HBase RPC、且 state 更大。维表是一次装载、长期不变的，
--   在装载阶段把常用维度打平进同一行（HBase 稀疏列天然适合宽行），
--   是「用装载成本换查询路径」的典型取舍。
--
-- RowKey：md5(业务ID) 前 2 位 + "_" + 业务ID
--   顺序 ID 直接做 rowkey 会让写入集中在一个 region；
--   散列前缀把连续 ID 打散到 256 个桶，规避写热点。
--   作业 B 的 LOOKUP JOIN 用完全相同的表达式还原 rowkey，两处必须逐字一致。
--
-- 用法（Windows 提交到 161 的 SQL Gateway）：
--   python code/flink_gateway.py sql/dim/hbase_dim_load.sql
--
-- 前置（缺一不可）：
--   1) 161 已把 flink-sql-connector-hbase-2.2-1.17.1.jar 放进 /opt/module/flink/lib/
--   2) 161 已重启 SQL Gateway（让 hbase-2.2 factory 进入 Gateway classpath）
--   3) 162 已用 sql/dim/hbase_create_tables.txt 建好 6 张物理表
--   4) Doris 镜像表已有数据（1486/50/230/600/16440/3000 行）
-- =============================================================================

-- 让本作业自带 connector jar。JM/TM 没有重启，启动 classpath 里没有它，
-- 但 pipeline.jars 会把 jar 附到 JobGraph，JM/TM 通过 BlobServer 加载。
SET 'pipeline.jars' = 'file:///opt/module/flink/lib/flink-sql-connector-hbase-2.2-1.17.1.jar';
SET 'parallelism.default' = '1';
SET 'execution.checkpointing.interval' = '10s';

-- =============================================================================
-- 1) Doris 镜像表（source，读一次快照，作业读完后 FINISHED）
-- =============================================================================
CREATE TABLE src_dim_song (
    song_id STRING, song_name STRING, artist_id STRING, album_id STRING,
    genre STRING, duration INT, release_date DATE
) WITH (
    'connector' = 'doris',
    'fenodes' = 'WWC163:8030',
    'table.identifier' = 'music_dw.dim_song',
    'username' = 'root',
    'password' = ''
);

CREATE TABLE src_dim_artist (
    artist_id STRING, artist_name STRING, country STRING,
    genre STRING, mbid STRING, type STRING
) WITH (
    'connector' = 'doris',
    'fenodes' = 'WWC163:8030',
    'table.identifier' = 'music_dw.dim_artist',
    'username' = 'root',
    'password' = ''
);

CREATE TABLE src_dim_album (
    album_id STRING, album_name STRING, artist_id STRING, release_date DATE
) WITH (
    'connector' = 'doris',
    'fenodes' = 'WWC163:8030',
    'table.identifier' = 'music_dw.dim_album',
    'username' = 'root',
    'password' = ''
);

CREATE TABLE src_dim_playlist (
    playlist_id STRING, playlist_name STRING, creator_user_id STRING, genre STRING
) WITH (
    'connector' = 'doris',
    'fenodes' = 'WWC163:8030',
    'table.identifier' = 'music_dw.dim_playlist',
    'username' = 'root',
    'password' = ''
);

CREATE TABLE src_dim_playlist_song (
    playlist_id STRING, song_id STRING, seq INT
) WITH (
    'connector' = 'doris',
    'fenodes' = 'WWC163:8030',
    'table.identifier' = 'music_dw.dim_playlist_song',
    'username' = 'root',
    'password' = ''
);

CREATE TABLE src_dim_user (
    user_id STRING, nickname STRING, age INT, gender STRING, city STRING,
    register_time TIMESTAMP(3), vip_level INT
) WITH (
    'connector' = 'doris',
    'fenodes' = 'WWC163:8030',
    'table.identifier' = 'music_dw.dim_user',
    'username' = 'root',
    'password' = ''
);

-- =============================================================================
-- 2) HBase 维表（sink）
--    HBase 只存 byte[]，所有字段统一 STRING；空值在 SELECT 里 COALESCE 成 ''。
--    ROW<...> 的外层字段名 cf 就是列族名，内部字段名就是 qualifier，
--    PRIMARY KEY (rowkey) NOT ENFORCED 是 hbase-2.2 的硬性要求。
-- =============================================================================
CREATE TABLE hb_dim_song (
    rowkey STRING,
    cf ROW<song_id STRING, song_name STRING, artist_id STRING, artist_name STRING,
          artist_country STRING, album_id STRING, album_name STRING,
          genre STRING, duration STRING, release_date STRING, mbid STRING>,
    PRIMARY KEY (rowkey) NOT ENFORCED
) WITH (
    'connector' = 'hbase-2.2',
    'table-name' = 'dim_song',
    'zookeeper.quorum' = 'WWC161:2181,WWC162:2181,WWC163:2181',
    'zookeeper.znode.parent' = '/hbase',
    'sink.buffer-flush.max-rows' = '1',
    'sink.buffer-flush.interval' = '1s'
);

CREATE TABLE hb_dim_artist (
    rowkey STRING,
    cf ROW<artist_id STRING, artist_name STRING, country STRING,
          genre STRING, mbid STRING, type STRING>,
    PRIMARY KEY (rowkey) NOT ENFORCED
) WITH (
    'connector' = 'hbase-2.2',
    'table-name' = 'dim_artist',
    'zookeeper.quorum' = 'WWC161:2181,WWC162:2181,WWC163:2181',
    'zookeeper.znode.parent' = '/hbase',
    'sink.buffer-flush.max-rows' = '1',
    'sink.buffer-flush.interval' = '1s'
);

CREATE TABLE hb_dim_album (
    rowkey STRING,
    cf ROW<album_id STRING, album_name STRING, artist_id STRING, release_date STRING>,
    PRIMARY KEY (rowkey) NOT ENFORCED
) WITH (
    'connector' = 'hbase-2.2',
    'table-name' = 'dim_album',
    'zookeeper.quorum' = 'WWC161:2181,WWC162:2181,WWC163:2181',
    'zookeeper.znode.parent' = '/hbase',
    'sink.buffer-flush.max-rows' = '1',
    'sink.buffer-flush.interval' = '1s'
);

CREATE TABLE hb_dim_playlist (
    rowkey STRING,
    cf ROW<playlist_id STRING, playlist_name STRING, creator_user_id STRING, genre STRING>,
    PRIMARY KEY (rowkey) NOT ENFORCED
) WITH (
    'connector' = 'hbase-2.2',
    'table-name' = 'dim_playlist',
    'zookeeper.quorum' = 'WWC161:2181,WWC162:2181,WWC163:2181',
    'zookeeper.znode.parent' = '/hbase',
    'sink.buffer-flush.max-rows' = '1',
    'sink.buffer-flush.interval' = '1s'
);

CREATE TABLE hb_dim_playlist_song (
    rowkey STRING,
    cf ROW<playlist_id STRING, song_id STRING, seq STRING>,
    PRIMARY KEY (rowkey) NOT ENFORCED
) WITH (
    'connector' = 'hbase-2.2',
    'table-name' = 'dim_playlist_song',
    'zookeeper.quorum' = 'WWC161:2181,WWC162:2181,WWC163:2181',
    'zookeeper.znode.parent' = '/hbase',
    'sink.buffer-flush.max-rows' = '1',
    'sink.buffer-flush.interval' = '1s'
);

CREATE TABLE hb_dim_user (
    rowkey STRING,
    cf ROW<user_id STRING, nickname STRING, age STRING, gender STRING,
          city STRING, register_time STRING, vip_level STRING>,
    PRIMARY KEY (rowkey) NOT ENFORCED
) WITH (
    'connector' = 'hbase-2.2',
    'table-name' = 'dim_user',
    'zookeeper.quorum' = 'WWC161:2181,WWC162:2181,WWC163:2181',
    'zookeeper.znode.parent' = '/hbase',
    'sink.buffer-flush.max-rows' = '1',
    'sink.buffer-flush.interval' = '1s'
);

-- =============================================================================
-- 3) 六路装载
--    一个 STATEMENT SET 一个作业，只占 1 个 slot，跑完自动 FINISHED。
-- =============================================================================
EXECUTE STATEMENT SET
BEGIN

    -- 歌曲：装载时预关联歌手与专辑，把流式侧常用的维度打平进一行
    INSERT INTO hb_dim_song
    SELECT
        CONCAT(SUBSTRING(MD5(s.song_id), 1, 2), '_', s.song_id),
        ROW(
            COALESCE(s.song_id, ''),
            COALESCE(s.song_name, ''),
            COALESCE(s.artist_id, ''),
            COALESCE(a.artist_name, ''),
            COALESCE(a.country, ''),
            COALESCE(s.album_id, ''),
            COALESCE(al.album_name, ''),
            COALESCE(s.genre, ''),
            COALESCE(CAST(s.duration AS STRING), ''),
            COALESCE(CAST(s.release_date AS STRING), ''),
            COALESCE(a.mbid, '')
        )
    FROM src_dim_song s
    LEFT JOIN src_dim_artist a  ON s.artist_id = a.artist_id
    LEFT JOIN src_dim_album  al ON s.album_id  = al.album_id;

    INSERT INTO hb_dim_artist
    SELECT
        CONCAT(SUBSTRING(MD5(a.artist_id), 1, 2), '_', a.artist_id),
        ROW(
            COALESCE(a.artist_id, ''),
            COALESCE(a.artist_name, ''),
            COALESCE(a.country, ''),
            COALESCE(a.genre, ''),
            COALESCE(a.mbid, ''),
            COALESCE(a.type, '')
        )
    FROM src_dim_artist a;

    INSERT INTO hb_dim_album
    SELECT
        CONCAT(SUBSTRING(MD5(al.album_id), 1, 2), '_', al.album_id),
        ROW(
            COALESCE(al.album_id, ''),
            COALESCE(al.album_name, ''),
            COALESCE(al.artist_id, ''),
            COALESCE(CAST(al.release_date AS STRING), '')
        )
    FROM src_dim_album al;

    INSERT INTO hb_dim_playlist
    SELECT
        CONCAT(SUBSTRING(MD5(p.playlist_id), 1, 2), '_', p.playlist_id),
        ROW(
            COALESCE(p.playlist_id, ''),
            COALESCE(p.playlist_name, ''),
            COALESCE(p.creator_user_id, ''),
            COALESCE(p.genre, '')
        )
    FROM src_dim_playlist p;

    -- 歌单-歌曲是复合主键，用 '|' 拼成单一 rowkey 再散列
    INSERT INTO hb_dim_playlist_song
    SELECT
        CONCAT(SUBSTRING(MD5(CONCAT(ps.playlist_id, '|', ps.song_id)), 1, 2),
               '_', ps.playlist_id, '|', ps.song_id),
        ROW(
            COALESCE(ps.playlist_id, ''),
            COALESCE(ps.song_id, ''),
            COALESCE(CAST(ps.seq AS STRING), '')
        )
    FROM src_dim_playlist_song ps;

    INSERT INTO hb_dim_user
    SELECT
        CONCAT(SUBSTRING(MD5(u.user_id), 1, 2), '_', u.user_id),
        ROW(
            COALESCE(u.user_id, ''),
            COALESCE(u.nickname, ''),
            COALESCE(CAST(u.age AS STRING), ''),
            COALESCE(u.gender, ''),
            COALESCE(u.city, ''),
            COALESCE(CAST(u.register_time AS STRING), ''),
            COALESCE(CAST(u.vip_level AS STRING), '')
        )
    FROM src_dim_user u;

END;
