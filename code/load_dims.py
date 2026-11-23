#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""把真实维表 CSV 装载进 Doris 的 DIM 层。

做四件事：
    1. 清洗 dim_song：未来发行日期置空、异常时长修正
    2. 重建 dim_playlist_song：旧关系表引用的是虚构歌曲 ID（S000001–S005000），
       与新的 1486 首真实歌曲对不上，按曲风重新配对
    3. 执行 sql/dim/doris_dim_tables.sql 建表
    4. 批量导入 6 张维表并验证

用法：
    python code/load_dims.py
    python code/load_dims.py --dry-run     # 只清洗和打印，不写库
"""

import argparse
import csv
import os
import random
import subprocess
import sys
from collections import Counter, defaultdict
from datetime import date

import pymysql

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DIM = os.path.join(BASE_DIR, "data", "dim")
DB = dict(host="192.168.128.163", port=9030, user="root", password="",
          database="music_dw", charset="utf8mb4", autocommit=True)
TODAY = date.today()


def read_csv(name):
    p = os.path.join(DIM, name + ".csv")
    if not os.path.exists(p):
        return []
    with open(p, encoding="utf-8-sig") as f:
        return list(csv.DictReader(f))


def write_csv(name, rows, cols):
    p = os.path.join(DIM, name + ".csv")
    with open(p, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=cols, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)


def clean_date(s):
    """返回合法的 yyyy-MM-dd，否则空串。未来日期一律置空。"""
    s = (s or "").strip()
    if not s:
        return ""
    parts = s.split("-")
    try:
        y = int(parts[0])
        m = int(parts[1]) if len(parts) > 1 else 1
        d = int(parts[2]) if len(parts) > 2 else 1
        dt = date(y, m, d)
    except (ValueError, IndexError):
        return ""
    if dt > TODAY:
        return ""
    if y < 1950:
        return ""
    return dt.isoformat()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    songs = read_csv("dim_song")
    artists = read_csv("dim_artist")
    albums = read_csv("dim_album")
    playlists = read_csv("dim_playlist")
    users = read_csv("dim_user")

    print(f"读入：{len(songs)} 歌 / {len(artists)} 歌手 / {len(albums)} 专辑 / "
          f"{len(playlists)} 歌单 / {len(users)} 用户")

    # --- 1. 清洗歌曲 ---
    fixed_date = fixed_dur = 0
    for s in songs:
        nd = clean_date(s.get("release_date"))
        if nd != (s.get("release_date") or ""):
            fixed_date += 1
        s["release_date"] = nd
        try:
            d = int(float(s.get("duration") or 0))
        except ValueError:
            d = 0
        if d < 45 or d > 900:
            d = 200
            fixed_dur += 1
        s["duration"] = d
    print(f"清洗：修正日期 {fixed_date} 条，修正时长 {fixed_dur} 条")

    # --- 2. 重建歌单-歌曲关系 ---
    # 旧关系表指向 S000001–S005000 的虚构歌曲，新数据只有 1486 首，必须重建
    by_genre = defaultdict(list)
    for s in songs:
        by_genre[s["genre"]].append(s["song_id"])
    rng = random.Random(20261101)
    pairs = []
    for pl in playlists:
        g = pl.get("genre") or "流行"
        pool = by_genre.get(g) or [s["song_id"] for s in songs]
        n = min(len(pool), rng.randint(15, 40))
        picked = rng.sample(pool, n)
        for i, sid in enumerate(picked, 1):
            pairs.append({"playlist_id": pl["playlist_id"],
                          "song_id": sid, "seq": i})
    print(f"重建歌单关系：{len(playlists)} 个歌单 / {len(pairs)} 条记录")

    write_csv("dim_song", songs,
              ["song_id", "song_name", "artist_id", "album_id", "genre",
               "duration", "release_date"])
    write_csv("dim_playlist_song", pairs, ["playlist_id", "song_id", "seq"])

    if args.dry_run:
        print("\n--dry-run，未写数据库")
        for s in songs[:5]:
            print("  ", s)
        return

    # --- 3. 建表 ---
    ddl = os.path.join(BASE_DIR, "sql", "dim", "doris_dim_tables.sql")
    print(f"\n建表：{ddl}")
    r = subprocess.run([sys.executable,
                        os.path.join(BASE_DIR, "code", "run_sql.py"), ddl],
                       capture_output=True, text=True, encoding="utf-8", errors="replace")
    print((r.stdout or "")[-800:])
    if r.returncode != 0:
        print((r.stderr or "")[-800:])
        return

    # --- 4. 导入 ---
    conn = pymysql.connect(**DB)
    cur = conn.cursor()

    def load(table, rows, cols, batch=500):
        if not rows:
            print(f"  {table}: 无数据，跳过")
            return
        cur.execute(f"TRUNCATE TABLE {table}")
        sql = (f"INSERT INTO {table} ({','.join(cols)}) "
               f"VALUES ({','.join(['%s'] * len(cols))})")
        vals = [tuple((r.get(c) if r.get(c) not in ("", None) else None) for c in cols)
                for r in rows]
        for i in range(0, len(vals), batch):
            cur.executemany(sql, vals[i:i + batch])
        cur.execute(f"SELECT COUNT(*) FROM {table}")
        print(f"  {table}: 写入 {cur.fetchone()[0]} 行")

    print("\n导入：")
    load("dim_artist", artists,
         ["artist_id", "artist_name", "country", "genre", "mbid", "type"])
    load("dim_album", albums,
         ["album_id", "album_name", "artist_id", "release_date"])
    load("dim_song", songs,
         ["song_id", "song_name", "artist_id", "album_id", "genre",
          "duration", "release_date"])
    load("dim_playlist", playlists,
         ["playlist_id", "playlist_name", "creator_user_id", "genre"])
    load("dim_playlist_song", pairs, ["playlist_id", "song_id", "seq"])
    load("dim_user", users,
         ["user_id", "nickname", "age", "gender", "city", "register_time", "vip_level"])

    # --- 5. 验证 ---
    print("\n验证（歌曲 + 歌手 + 专辑 关联抽样）：")
    cur.execute("""
        SELECT s.song_id, s.song_name, a.artist_name, al.album_name, s.duration
        FROM dim_song s
        LEFT JOIN dim_artist a ON s.artist_id = a.artist_id
        LEFT JOIN dim_album  al ON s.album_id = al.album_id
        ORDER BY s.song_id LIMIT 8
    """)
    for row in cur.fetchall():
        print("  ", " | ".join(str(x) for x in row))

    cur.execute("""SELECT COUNT(*) FROM dim_song s
                   LEFT JOIN dim_artist a ON s.artist_id = a.artist_id
                   WHERE a.artist_id IS NULL""")
    orphan = cur.fetchone()[0]
    print(f"\n  孤儿歌曲（找不到歌手）：{orphan}")
    conn.close()
    print("完成")


if __name__ == "__main__":
    main()
