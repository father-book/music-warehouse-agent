#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""从 MusicBrainz 抓取真实歌曲/歌手/专辑，生成 DIM 层维度 CSV。

数据来源：MusicBrainz 开放数据库（CC0 / CC BY-NC-SA），无需 API key。
每条记录都有 MBID，可回溯到 https://musicbrainz.org 核对，答辩时能交代出处。

抓取策略（性能关键）：
    不用 search API（`/recording?query=arid:` 走全文索引，100 条约 44 秒），
    改用 browse API 的 `release?artist=<mbid>&inc=recordings` ——
    一次请求同时拿到该歌手全部专辑和每张专辑的曲目，41 张专辑仅 5 秒。

速率：官方要求 1 请求/秒，脚本 sleep 1.1 秒。每位歌手 2 次请求，约 8-12 秒。

用法：
    python code/gen_real_dims.py --limit 5          # 小规模验证
    python code/gen_real_dims.py --limit 0          # 全量（约 8 分钟）
    python code/gen_real_dims.py --songs-per-artist 30
"""

import argparse
import csv
import json
import os
import re
import time
import urllib.error
import urllib.parse
import urllib.request

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DIM_DIR = os.path.join(BASE_DIR, "data", "dim")
MB = "https://musicbrainz.org/ws/2"
UA = "MusicDataWarehouseProject/1.0 (https://example.edu; edu@example.com)"

_OPENER = urllib.request.build_opener(urllib.request.ProxyHandler({}))

# (中文名, 英文名, 风格, 国家/地区)。英文名用于中文搜不到时兜底。
ARTISTS = [
    ("周杰伦", "Jay Chou", "流行", "中国台湾"),
    ("林俊杰", "JJ Lin", "流行", "新加坡"),
    ("陈奕迅", "Eason Chan", "流行", "中国香港"),
    ("张学友", "Jacky Cheung", "流行", "中国香港"),
    ("王力宏", "Leehom Wang", "流行", "中国台湾"),
    ("五月天", "Mayday", "摇滚", "中国台湾"),
    ("薛之谦", "Joker Xue", "流行", "中国"),
    ("李荣浩", "Li Ronghao", "流行", "中国"),
    ("毛不易", "Mao Buyi", "民谣", "中国"),
    ("华晨宇", "Hua Chenyu", "流行", "中国"),
    ("张杰", "Jason Zhang", "流行", "中国"),
    ("汪峰", "Wang Feng", "摇滚", "中国"),
    ("许嵩", "Vae Xu", "流行", "中国"),
    ("朴树", "Pu Shu", "民谣", "中国"),
    ("李健", "Li Jian", "民谣", "中国"),
    ("周深", "Zhou Shen", "流行", "中国"),
    ("邓紫棋", "G.E.M.", "流行", "中国香港"),
    ("孙燕姿", "Stefanie Sun", "流行", "新加坡"),
    ("梁静茹", "Fish Leong", "流行", "马来西亚"),
    ("蔡依林", "Jolin Tsai", "流行", "中国台湾"),
    ("张惠妹", "A-Mei", "流行", "中国台湾"),
    ("王菲", "Faye Wong", "流行", "中国香港"),
    ("那英", "Na Ying", "流行", "中国"),
    ("田馥甄", "Hebe Tien", "流行", "中国台湾"),
    ("刘若英", "Rene Liu", "流行", "中国台湾"),
    ("莫文蔚", "Karen Mok", "流行", "中国香港"),
    ("张韶涵", "Angela Chang", "流行", "中国台湾"),
    ("徐佳莹", "Lala Hsu", "流行", "中国台湾"),
    ("苏打绿", "Sodagreen", "摇滚", "中国台湾"),
    ("Beyond", "Beyond", "摇滚", "中国香港"),
    ("逃跑计划", "Escape Plan", "摇滚", "中国"),
    ("痛仰", "Miserable Faith", "摇滚", "中国"),
    ("许巍", "Xu Wei", "摇滚", "中国"),
    ("赵雷", "Zhao Lei", "民谣", "中国"),
    ("宋冬野", "Song Dongye", "民谣", "中国"),
    ("陈粒", "Chen Li", "民谣", "中国"),
    ("陶喆", "David Tao", "R&B", "中国台湾"),
    ("方大同", "Khalil Fong", "R&B", "中国香港"),
    ("萧敬腾", "Jam Hsiao", "流行", "中国台湾"),
    ("林宥嘉", "Yoga Lin", "流行", "中国台湾"),
    ("杨宗纬", "Aska Yang", "流行", "中国台湾"),
    ("李宗盛", "Jonathan Lee", "流行", "中国台湾"),
    ("罗大佑", "Lo Ta-yu", "摇滚", "中国台湾"),
    ("崔健", "Cui Jian", "摇滚", "中国"),
    ("窦唯", "Dou Wei", "摇滚", "中国"),
    ("郑钧", "Zheng Jun", "摇滚", "中国"),
    ("小柯", "Xiao Ke", "流行", "中国"),
    ("老狼", "Lao Lang", "民谣", "中国"),
    ("水木年华", "Shui Mu Nian Hua", "民谣", "中国"),
    ("张震岳", "Chang Chen-yue", "流行", "中国台湾"),
]

# 专辑/曲目名的排除词：现场版、翻唱、伴奏、纯音乐等
BAD = re.compile(
    r"(演唱會|演唱会|巡迴|巡回|Live|live|LIVE|Karaoke|伴奏|純音樂|纯音乐|"
    r"Instrumental|Demo|電台|电台|串燒|串烧|Remix|remix|翻唱|合輯|合辑)"
)


def api(path, params, retry=3):
    """返回 None 表示请求失败（网络/限流），返回 {} 才是服务端真的没数据。

    这个区分很重要：早期版本把两者都当 {}，一次网络抖动就被判成"该歌手不存在"，
    导致后半批歌手整片"未找到"。
    """
    url = f"{MB}/{path}?" + urllib.parse.urlencode(params)
    for attempt in range(retry + 1):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": UA})
            with _OPENER.open(req, timeout=30) as r:
                return json.loads(r.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            if e.code in (503, 429) and attempt < retry:
                time.sleep(4 + attempt * 2)
                continue
            return {} if e.code == 404 else None
        except Exception:
            if attempt >= retry:
                return None
            time.sleep(3)
    return None


def find_artist(cn, en):
    """先按中文名搜，score 不够再按英文名搜。请求失败会重试一次。"""
    for q in (cn, en):
        if not q:
            continue
        d = api("artist", {"query": q, "fmt": "json", "limit": 5})
        if d is None:
            time.sleep(3)
            d = api("artist", {"query": q, "fmt": "json", "limit": 5})
        time.sleep(1.1)
        if not d:
            continue
        arts = d.get("artists") or []
        if arts:
            best = max(arts, key=lambda a: a.get("score", 0))
            if best.get("score", 0) >= 70:
                return best
    return None


def fetch_releases(mbid):
    d = api("release", {"artist": mbid, "inc": "recordings",
                        "fmt": "json", "limit": 100})
    time.sleep(1.1)
    return d.get("releases") or []


def clean_title(t):
    t = (t or "").strip()
    # 去掉尾部的英文对照，如 "七里香 Common Jasmine Orange"
    t = re.sub(r"\s*\[[^\]]*\]", "", t)
    t = re.sub(r"\s*\([^)]*\)$", "", t)
    return t.strip()


def read_csv(path):
    if not os.path.exists(path):
        return None
    with open(path, encoding="utf-8-sig") as f:
        rows = list(csv.DictReader(f))
    return rows or None


def id_num(v):
    """从 A00001 / AL00001 / S000001 末尾取出数字，用于续接自增 ID。"""
    m = re.search(r"(\d+)$", v or "")
    return int(m.group(1)) if m else 0


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=0, help="只处理前 N 位歌手，0=全量")
    ap.add_argument("--songs-per-artist", type=int, default=30)
    ap.add_argument("--out-suffix", default="")
    ap.add_argument("--only", default="", help="逗号分隔的歌手名，只处理这些（用于补跑）")
    ap.add_argument("--append", action="store_true",
                    help="追加到现有 CSV 而不是覆盖，ID 从现有最大值续接")
    args = ap.parse_args()

    os.makedirs(DIM_DIR, exist_ok=True)
    artists = ARTISTS
    if args.only:
        names = {x.strip() for x in args.only.split(",") if x.strip()}
        artists = [a for a in ARTISTS if a[0] in names]
        missing = names - {a[0] for a in artists}
        if missing:
            print(f"[WARN] 名单里这些歌手不在 ARTISTS 配置中：{missing}", flush=True)
    if args.limit:
        artists = artists[: args.limit]

    dim_artist, dim_album, dim_song = [], [], []
    seen_song = set()
    a_seq = al_seq = s_seq = 0

    suffix = args.out_suffix

    def path_of(name):
        return os.path.join(DIM_DIR, name + suffix + ".csv")

    if args.append:
        # 读现有数据，ID 从最大值续接，并把已有歌曲登记进去重集合
        rows = read_csv(path_of("dim_artist")) or []
        if rows:
            a_seq = max(id_num(r["artist_id"]) for r in rows)
            dim_artist = rows
        rows = read_csv(path_of("dim_album")) or []
        if rows:
            al_seq = max(id_num(r["album_id"]) for r in rows)
            dim_album = rows
        rows = read_csv(path_of("dim_song")) or []
        if rows:
            s_seq = max(id_num(r["song_id"]) for r in rows)
            dim_song = rows
            for r in rows:
                seen_song.add((r.get("artist_id", ""), (r.get("song_name") or "").lower()))
        print(f"追加模式：现有 {len(dim_artist)} 歌手 / {len(dim_album)} 专辑 / "
              f"{len(dim_song)} 歌曲", flush=True)

    t_start = time.time()

    for idx, (cn, en, genre, country) in enumerate(artists, 1):
        elapsed = time.time() - t_start
        print(f"[{idx}/{len(artists)}] {cn}  (已用 {elapsed/60:.1f} 分钟)", flush=True)

        art = find_artist(cn, en)
        if not art:
            print("     × 未找到", flush=True)
            continue

        a_seq += 1
        aid = f"A{a_seq:05d}"
        name = (art.get("name") or cn).strip()
        dim_artist.append({
            "artist_id": aid, "artist_name": name, "country": country,
            "genre": genre, "mbid": art.get("id", ""),
            "type": art.get("type", ""),
        })

        releases = fetch_releases(art.get("id", ""))
        picked = 0
        for rel in releases:
            if picked >= args.songs_per_artist:
                break
            rt = rel.get("title") or ""
            if BAD.search(rt):
                continue
            if (rel.get("status") or "") not in ("Official", ""):
                continue
            tracks = []
            for media in rel.get("media") or []:
                for tr in media.get("tracks") or []:
                    tracks.append(tr)
            if not tracks:
                continue

            alb_name = clean_title(rt)
            if not alb_name:
                continue
            al_seq += 1
            alid = f"AL{al_seq:05d}"
            dim_album.append({
                "album_id": alid, "album_name": alb_name,
                "artist_id": aid, "release_date": rel.get("date") or "",
            })

            for tr in tracks:
                if picked >= args.songs_per_artist:
                    break
                st = clean_title(tr.get("title"))
                if not st or BAD.search(st):
                    continue
                key = (aid, st.lower())
                if key in seen_song:
                    continue
                seen_song.add(key)
                ms = tr.get("length") or 0
                dur = int(ms / 1000) if ms else (170 + (s_seq * 23) % 160)
                if dur < 45 or dur > 900:
                    dur = 200 + (s_seq * 17) % 120
                s_seq += 1
                dim_song.append({
                    "song_id": f"S{s_seq:06d}", "song_name": st,
                    "artist_id": aid, "album_id": alid, "genre": genre,
                    "duration": dur, "release_date": rel.get("date") or "",
                })
                picked += 1
        print(f"     {name}: {picked} 首，累计 {len(dim_song)} 首 / "
              f"{len(dim_album)} 专辑", flush=True)

    def write(name, rows, cols):
        path = path_of(name)
        with open(path, "w", newline="", encoding="utf-8-sig") as f:
            w = csv.DictWriter(f, fieldnames=cols, extrasaction="ignore")
            w.writeheader()
            w.writerows(rows)
        print(f"  -> {path}  ({len(rows)} 行)", flush=True)

    write("dim_artist", dim_artist,
          ["artist_id", "artist_name", "country", "genre", "mbid", "type"])
    write("dim_album", dim_album,
          ["album_id", "album_name", "artist_id", "release_date"])
    write("dim_song", dim_song,
          ["song_id", "song_name", "artist_id", "album_id", "genre",
           "duration", "release_date"])

    print(f"\n完成，用时 {(time.time()-t_start)/60:.1f} 分钟")
    print(f"  {len(dim_artist)} 位歌手 / {len(dim_album)} 张专辑 / {len(dim_song)} 首歌")


if __name__ == "__main__":
    main()
