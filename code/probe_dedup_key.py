#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""probe_dedup_key.py —— 找出真正唯一的去重键

已确认（2026-11-08）：
  Kafka 里同一 event_id 会有多条语义不同的消息 —— event_time / server_time 100% 不同，
  user_id 17.8% 不同。event_id 由"时间槽 + 序号"构成，不含用户，多批重灌必然撞号。
  因此 `dropDuplicates(["event_id"])` 会随机丢掉真实事件。

本脚本比较四种候选键在真实数据上的重复率，选出既唯一又不丢事件的组合：
  K1 event_id
  K2 (event_id, event_time)
  K3 (event_id, event_time, user_id)
  K4 (user_id, session_id, session_seq, event_name)

判定标准：重复率越低越唯一；同时 K3/K4 若为 0，则它们不会误删真实事件。
只读，不落盘。
"""
import sys
import zlib

import json
from kafka import KafkaConsumer, TopicPartition

BOOT = "192.168.128.161:9092"
TOPIC = "music_events"


def fp(*parts):
    h = 0
    for p in parts:
        h = zlib.crc32(((p if isinstance(p, str) else str(p)) + "|").encode("utf-8"), h)
    return h


def make(gid):
    c = KafkaConsumer(bootstrap_servers=BOOT, enable_auto_commit=False,
                      group_id=gid, max_poll_records=5000,
                      auto_offset_reset="earliest", request_timeout_ms=60000,
                      fetch_max_bytes=52428800)
    tps = [TopicPartition(TOPIC, p) for p in sorted(c.partitions_for_topic(TOPIC) or [])]
    c.assign(tps)
    ends = c.end_offsets(tps)
    for tp in tps:
        c.seek(tp, 0)
    return c, tps, ends


# 内存：161 可用约 1.1G。四个集合各存 240 万个 32 位 int ≈ 4 × 90MB，
# 偏紧，所以拆两趟：第一趟算 K1/K2，第二趟算 K3/K4。
def scan(which):
    c, tps, ends = make("probe_key_%s" % which)
    s1 = set()
    s2 = set()
    n = 0
    while True:
        b = c.poll(timeout_ms=3000, max_records=5000)
        if not b:
            if all(c.position(tp) >= ends[tp] for tp in tps):
                break
            continue
        for tp, recs in b.items():
            for r in recs:
                n += 1
                try:
                    o = json.loads(r.value.decode("utf-8"))
                except Exception:
                    continue
                eid = o.get("event_id")
                if not eid:
                    continue
                et = o.get("event_time")
                uid = o.get("user_id")
                sid = o.get("session_id")
                sq = o.get("session_seq")
                nm = o.get("event_name")
                if which == "a":
                    s1.add(fp(eid))
                    s2.add(fp(eid, et))
                else:
                    s1.add(fp(eid, et, uid))
                    s2.add(fp(uid, sid, sq, nm))
        if n % 500000 < 5000:
            sys.stderr.write("  已读 %d\n" % n)
            sys.stderr.flush()
    c.close()
    return n, len(s1), len(s2)


n, k1, k2 = scan("a")
print("第一趟：%d 条" % n)
print("  K1 event_id                唯一 %d  重复率 %.2f%%" % (k1, 100.0 * (n - k1) / n))
print("  K2 (event_id, event_time)  唯一 %d  重复率 %.2f%%" % (k2, 100.0 * (n - k2) / n))

n2, k3, k4 = scan("b")
print("第二趟：%d 条" % n2)
print("  K3 (event_id, event_time, user_id)              唯一 %d  重复率 %.2f%%"
      % (k3, 100.0 * (n2 - k3) / n2))
print("  K4 (user_id, session_id, session_seq, event_name) 唯一 %d  重复率 %.2f%%"
      % (k4, 100.0 * (n2 - k4) / n2))

print("\n【结论】")
if n2 - k3 == 0:
    print("  K3 完全唯一 → 可作为落盘去重键，且不会误删真实事件")
elif (n2 - k3) < 0.01 * n2:
    print("  K3 近似唯一（重复 %.3f%%），可作去重键，残余需人工核对" % (100.0 * (n2 - k3) / n2))
else:
    print("  K3 仍有 %.2f%% 重复，说明还存在同 id 同时刻同用户的重复上报，需再下钻"
          % (100.0 * (n2 - k3) / n2))
