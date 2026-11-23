SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;

SELECT 'SONG_PREFIX' AS k, SUBSTR(song_id,1,1) AS p, COUNT(*) AS n
FROM music_dw.dim_song GROUP BY SUBSTR(song_id,1,1);

SELECT 'SONG_JOIN' AS k,
       COUNT(*) AS fact_rows,
       COUNT(d.song_id) AS hit_rows
FROM music_dw.dws_song_play_daily s
LEFT JOIN music_dw.dim_song d ON s.song_id = d.song_id;

SELECT 'SONG_DISTINCT' AS k,
       COUNT(DISTINCT song_id) AS fact_songs
FROM music_dw.dws_song_play_daily;
