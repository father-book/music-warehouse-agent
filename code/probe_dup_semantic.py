#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""probe_dup_semantic.py —— 重复消息是"字节不同"还是"语义不同"

矛盾现场：
  · probe_cross_dedup 的 Q2：抽样 30 万条，重复 90,543 对，
    按 crc32(raw) 比较只有 347 对相同、90,196 对不同（99.6%）
  · probe_dup_diff：抽样 5 万条，106 个重复组，逐字段比较**全部相同**

两个结果不可能同时成立，除非 crc32(raw) 把"语义相同、序列化不同"的消息
判成了不同。最常见原因就是 JSON 里 key 的顺序不同。

本脚本对同一批重复对同时算三种指纹：
  A. raw                —— 原始字节
  B. json(sort_keys)    —— 规范化后字节
  C. 业务关键字段元组    —— event_name, event_time, user_id, server_time
若 B 相同而 A 不同 → 纯序列化差异，dropDuplicates 安全
若 C 不同          → 真的是不同事件撞了同一个 event_id，必须先定主键

内存：161 可用约 1.1G，这里每条只留三个整数指纹（约 50MB），
不缓存 raw / dict；只对前 3 个有差异的样本临时保留原文用于打印。
"""
import json
import zlib
from collections import Counter

from kafka import KafkaConsumer, TopicPartition

BOOT = "192.168.128.161:9092"
TOPIC = "music_events"
TAIL = 300000

c = KafkaConsumer(bootstrap_servers=BOOT, enable_auto_commit=False,
                  group_id="probe_dup_semantic", max_poll_records=5000,
                  auto_offset_reset="earliest", request_timeout_ms=60000)
tp = TopicPartition(TOPIC, 2)
c.assign([tp])
end = c.end_offsets([tp])[tp]
c.seek(tp, max(0, end - TAIL))

seen = {}           # eid -> (raw_crc, norm_crc, keytup)
raw_same = raw_diff = 0
norm_same = norm_diff = 0
key_same = key_diff = 0
diff_fields = Counter()
samples = []
got = 0


def keytup(o):
    return (o.get("event_name"), str(o.get("event_time")),
            o.get("user_id"), str(o.get("server_time")))


while got < TAIL:
    b = c.poll(timeout_ms=3000, max_records=5000)
    if not b:
        break
    for _, recs in b.items():
        for r in recs:
            got += 1
            raw = r.value
            try:
                o = json.loads(raw.decode("utf-8"))
            except Exception:
                continue
            eid = o.get("event_id")
            if not eid:
                continue
            rc = zlib.crc32(raw)
            nc = zlib.crc32(json.dumps(o, sort_keys=True, ensure_ascii=False).encode("utf-8"))
            kt = keytup(o)
            prev = seen.get(eid)
            if prev is None:
                seen[eid] = (rc, nc, kt)
                continue
            prc, pnc, pkt = prev
            if prc == rc:
                raw_same += 1
            else:
                raw_diff += 1
            if pnc == nc:
                norm_same += 1
            else:
                norm_diff += 1
                if len(samples) < 3:
                    samples.append((eid, raw, o))
            if pkt == kt:
                key_same += 1
            else:
                key_diff += 1
                for i, name in enumerate(("event_name", "event_time", "user_id", "server_time")):
                    if str(pkt[i]) != str(kt[i]):
                        diff_fields[name] += 1
c.close()

print("抽样 %d 条，唯一 id %d，重复条数 %d" % (got, len(seen), raw_same + raw_diff))
print("\n【A】raw 字节比较        相同 %d / 不同 %d" % (raw_same, raw_diff))
print("【B】规范化 JSON 比较    相同 %d / 不同 %d" % (norm_same, norm_diff))
print("【C】业务关键字段比较    相同 %d / 不同 %d" % (key_same, key_diff))

print("\n【判定】")
if norm_diff == 0 and key_diff == 0:
    print("  A 不同而 B/C 全同 → 纯序列化差异（key 顺序等）")
    print("  → dropDuplicates(['event_id']) 安全，结果可复现")
elif key_diff == 0:
    print("  B 有差异但 C 全同 → 差异在非关键字段，须确认是否影响下游口径")
else:
    print("  !! C 有 %d 对不同 → 同 event_id 指向不同事件，event_id 不能当主键" % key_diff)
    for k, v in diff_fields.most_common(10):
        print("     %-14s %d 对" % (k, v))

for eid, raw, o in samples:
    print("\n" + "-" * 80)
    print("event_id = %s 的重点字段：" % eid)
    for k in ("event_name", "event_time", "user_id", "server_time", "dt", "etl_time"):
        print("  %-12s %s" % (k, str(o.get(k))[:80]))
    print("  raw 前 100 字节: %s" % raw[:100])
