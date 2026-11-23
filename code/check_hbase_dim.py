#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""DIM 层 HBase 落地后的端到端验证（Windows 侧执行）。

验证四件事：
    1. Doris 镜像表 6 张的行数（装载基准）
    2. dwd_play_end 是否已有 4 个维度列
    3. 维度命中率 + 抽样（证明 HBase LOOKUP JOIN 真的进了查询路径）
    4. Flink 作业状态 + 大屏榜单是否显示真实歌名

用法：
    python code/check_hbase_dim.py
"""
import json
import urllib.request

import pymysql

DB = dict(host="192.168.128.163", port=9030, user="root", password="",
          database="music_dw", charset="utf8mb4")
MIRROR = ["dim_song", "dim_artist", "dim_album",
          "dim_playlist", "dim_playlist_song", "dim_user"]
DIM_COLS = ["song_name", "artist_name", "genre", "album_name"]
_OPENER = urllib.request.build_opener(urllib.request.ProxyHandler({}))


def doris():
    conn = pymysql.connect(**DB)
    cur = conn.cursor(pymysql.cursors.DictCursor)

    print("=== 1. Doris 镜像表行数（HBase 装载的对账基准） ===")
    for t in MIRROR:
        cur.execute("SELECT COUNT(*) AS c FROM " + t)
        print("  %-20s %6d" % (t, cur.fetchone()["c"]))

    print("\n=== 2. dwd_play_end 维度列 ===")
    cur.execute("DESC dwd_play_end")
    cols = [r["Field"] for r in cur.fetchall()]
    for c in DIM_COLS:
        print("  %-14s %s" % (c, "✓" if c in cols else "✗ 缺失（先跑 add_dim_columns.py）"))

    if all(c in cols for c in DIM_COLS):
        print("\n=== 3. 维度命中率（近 10 分钟） ===")
        cur.execute("""
            SELECT COUNT(*) AS total,
                   SUM(CASE WHEN song_name <> '' THEN 1 ELSE 0 END) AS hit
            FROM dwd_play_end
            WHERE event_ts >= NOW() - INTERVAL 10 MINUTE
        """)
        r = cur.fetchone()
        total, hit = r["total"] or 0, r["hit"] or 0
        pct = round(100.0 * hit / total, 2) if total else 0
        print("  total=%d  hit=%d  hit_rate=%.2f%%" % (total, hit, pct))

        print("\n  抽样（真实歌名/歌手）：")
        cur.execute("""
            SELECT song_id, song_name, artist_name, genre, album_name
            FROM dwd_play_end
            WHERE song_name <> '' ORDER BY event_ts DESC LIMIT 8
        """)
        for x in cur.fetchall():
            print("   ", " | ".join(str(v) for v in x.values()))
    conn.close()


def flink_jobs():
    print("\n=== 4. Flink 作业状态 ===")
    try:
        with _OPENER.open("http://192.168.128.161:8081/jobs/overview", timeout=5) as r:
            d = json.load(r)
        for j in d.get("jobs", []):
            print("  %-14s %-10s %s" % (j["jid"][:12], j["state"], j.get("name", "")[:50]))
    except Exception as e:
        print("  ✗ 读 Flink REST 失败：", e)


def dashboard():
    print("\n=== 5. 大屏榜单 ===")
    try:
        with _OPENER.open("http://127.0.0.1:5000/api/all", timeout=5) as r:
            d = json.load(r)
        for x in d.get("rank", [])[:5]:
            print("  %-10s %-20s %-12s play=%s" % (
                x.get("song_id"), x.get("song_name"),
                x.get("artist_name"), x.get("play_cnt")))
        if not d.get("rank"):
            print("  （榜单为空，可能当前无近 10 分钟数据）")
    except Exception as e:
        print("  ✗ 大屏未启动或不可达：", e)


if __name__ == "__main__":
    try:
        doris()
    except Exception as e:
        print("Doris 检查失败：", type(e).__name__, e)
    flink_jobs()
    dashboard()
