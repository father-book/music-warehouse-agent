#!/usr/bin/env python3
"""实时链路窗口探针（仅用于验证，不作为数据源）
==================================================

背景
----
`gen_events.py` 生成的是**历史事件时间**（按 --start 指定的那一天铺开）。
Flink 的窗口算子按事件时间推进水位线，迟到超过 allowed lateness 的数据会被丢弃
（这是正确行为，不是 bug）。所以用历史事件去验证 `ads_realtime_overview`
（CUMULATE 30s/1d）、`ads_song_rank_10min`（HOP 1min/10min）永远看不到产出。

本脚本的作用：从 `music_events` 取最近若干条真实消息，**只改写 event_time /
server_time 为最近 N 秒**，再原样发回同一个 topic，让 4 个作业按正常路径跑一遍。

边界
----
* 它不造新数据、不改 event_id 语义（会加 `probe-` 前缀避免撞离线 landing 的
  去重键），只为触发窗口。
* **会改写歌曲 ID**：把 `sg_00042` 归一成维表口径 `S000043`。`dim_song` 用的是
  `S%06d`，历史批次里 `sg_` 口径的消息混进来会让维表 join 全落空
  （歌名空串、曲风「未知」）。`--keep-song-id` 可关掉这个改写。
* 数据仍会经 Flume 落到 landing → 验证完必须清 landing 当日目录，
  否则下次跑 landing step 会把这些探针数据带进离线 ODS。
* 生产上不会有这种脚本；答辩要说明它是「窗口验证脚手架」。

用法
----
    python3 rt_probe.py --count 3000 --span 300
"""
import argparse
import json
import random
import re
import time
import uuid

from kafka import KafkaConsumer, KafkaProducer, TopicPartition

BOOTSTRAP = "192.168.128.161:9092"
TOPIC = "music_events"

# 旧编号空间 sg_00000~sg_01485 → 维表口径 S000001~S001486。
# 为什么必须归一：dim_song / dim_artist 全部是 `S%06d`，job_b 按 song_id 去 join
# 维表取歌名/歌手/曲风。`sg_` 这套自造 ID 一个都 join 不上，结果就是
# song_name / artist_name / genre 全空串，大屏退回显示 ID、曲风一律「未知」。
# 换算关系：sg_ 的序号从 0 开始，S 的编号从 1 开始，且补零位数 5 → 6。
_SG_PREFIX = re.compile(r"^sg_0*(\d+)$")
# properties 里所有指向歌曲的键，都要跟着换空间
_SONG_KEYS = ("song_id", "result_id", "clicked_song_id")


def norm_song_id(sid):
    """sg_00042 → S000043；已经是 S 开头或不是字符串的原样返回。"""
    if not isinstance(sid, str):
        return sid
    m = _SG_PREFIX.match(sid)
    if not m:
        return sid
    return "S%06d" % (int(m.group(1)) + 1)


def normalize_props(ev):
    """把 properties 里 sg_ 口径的歌曲 ID 换到维表口径，并统一成 JSON 对象。"""
    p = ev.get("properties")
    if isinstance(p, str):
        try:
            p = json.loads(p)
        except Exception:
            return
    if not isinstance(p, dict):
        return
    touched = False
    for k in _SONG_KEYS:
        if k in p:
            new = norm_song_id(p[k])
            if new != p[k]:
                p[k] = new
                touched = True
    ev["properties"] = p
    return touched


def sample_events(count, rng):
    """跨分区随机抽 offset，而不是取尾部。

    踩过一次坑：topic 尾部那一段几乎全是 page_view（会话收尾事件集中在末尾），
    取尾部等于只喂 `dwd_page_view`，播放窗口一个事件都收不到。
    随机 offset 才能覆盖 26 类事件。
    """
    c = KafkaConsumer(bootstrap_servers=BOOTSTRAP, enable_auto_commit=False,
                      consumer_timeout_ms=5000,
                      value_deserializer=lambda b: json.loads(b.decode("utf-8")))
    tps = [TopicPartition(TOPIC, p) for p in sorted(c.partitions_for_topic(TOPIC) or [])]
    c.assign(tps)
    ends = c.end_offsets(tps)
    begins = c.beginning_offsets(tps)
    per = max(1, count // max(1, len(tps)))

    out = []
    for tp in tps:
        lo, hi = begins[tp], ends[tp]
        if hi - lo <= per:
            picks = list(range(lo, hi))
        else:
            picks = rng.sample(range(lo, hi), per)
        picks.sort()
        # 同一段 offset 连续读，避免随机 seek 一千次的网络往返
        chunk = 400
        for i in range(0, len(picks), chunk):
            part = picks[i:i + chunk]
            keep = set(part)
            c.seek(tp, part[0])
            want = part[-1] - part[0] + 1
            got = 0
            while got < want:
                batch = c.poll(timeout_ms=1500).get(tp, [])
                if not batch:
                    break
                for m in batch:
                    got += 1
                    if m.offset in keep:
                        out.append(m.value)
                    if got >= want:
                        break
    c.close()
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--count", type=int, default=3000, help="探针消息条数")
    ap.add_argument("--span", type=int, default=300, help="事件时间回拨的上限（秒）")
    ap.add_argument("--seed", type=int, default=20261107)
    ap.add_argument("--keep-song-id", action="store_true",
                    help="不改写歌曲 ID（默认会把 sg_ 口径归一成维表的 S 编号）")
    args = ap.parse_args()

    rng = random.Random(args.seed)
    evs = sample_events(args.count, rng)
    print(f"取到 {len(evs)} 条样本，改写时间后回发 {TOPIC}")

    # 同一条样本里 event_id 要换掉：否则离线 landing 的 event_id 去重会把它当重复丢弃，
    # 探针就白发了。用确定前缀 + uuid4 保证唯一且可识别。
    prod = KafkaProducer(bootstrap_servers=BOOTSTRAP,
                         value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
                         acks=1, linger_ms=20)
    now_ms = int(time.time() * 1000)
    n = 0
    fixed = 0
    for e in evs:
        back = rng.randint(0, args.span) * 1000
        t = now_ms - back
        e["event_id"] = "probe-" + uuid.uuid4().hex
        e["event_time"] = str(t)
        e["server_time"] = str(t + rng.randint(0, 3000))
        if not args.keep_song_id and normalize_props(e):
            fixed += 1
        prod.send(TOPIC, e)
        n += 1
    prod.flush()
    prod.close()
    print(f"== 探针发送完成：{n} 条，事件时间分布在最近 {args.span} 秒 ==")
    if args.keep_song_id:
        print("   歌曲 ID 未改写（--keep-song-id）")
    else:
        print(f"   歌曲 ID 归一化 {fixed} 条（sg_ → S），其余本就在维表口径内")


if __name__ == "__main__":
    main()
