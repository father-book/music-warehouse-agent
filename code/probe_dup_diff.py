#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""probe_dup_diff.py —— 同一个 event_id 的多条消息，到底差在哪个字段

为什么必须查：
  抽样 30 万条发现重复 90,543 对，其中只有 347 对逐字节相同，
  99.6% 是"同 event_id、内容不同"。

  这直接决定两件事：
    1) ODS 里 `dropDuplicates(["event_id"])` 是否合法 —— 它随机留一条，
       如果各条内容不同，结果不可复现，重跑一次数就变。
    2) 若要按规则选版本，选哪条：server_time/etl_time 最大？event_time 最小？

做法：取 partition 2 末 5 万条，找重复 event_id，打印前 3 组的逐字段差异。
"""
import json
import time
from collections import defaultdict

from kafka import KafkaConsumer, TopicPartition

BOOT = "192.168.128.161:9092"
TOPIC = "music_events"
TAIL = 50000

c = KafkaConsumer(bootstrap_servers=BOOT, enable_auto_commit=False,
                  group_id="probe_dup_diff", max_poll_records=5000,
                  auto_offset_reset="earliest", request_timeout_ms=60000)
tp = TopicPartition(TOPIC, 2)
c.assign([tp])
end = c.end_offsets([tp])[tp]
c.seek(tp, max(0, end - TAIL))

groups = defaultdict(list)
got = 0
while got < TAIL:
    b = c.poll(timeout_ms=3000, max_records=5000)
    if not b:
        break
    for _, recs in b.items():
        for r in recs:
            got += 1
            try:
                o = json.loads(r.value.decode("utf-8"))
            except Exception:
                continue
            eid = o.get("event_id")
            if eid:
                groups[eid].append(o)
c.close()

dups = [(k, v) for k, v in groups.items() if len(v) > 1]
print("抽样 %d 条，唯一 event_id %d，重复组 %d" % (got, len(groups), len(dups)))
cnt = {}
for _, v in dups:
    cnt[len(v)] = cnt.get(len(v), 0) + 1
print("重复组内条数分布：%s" % dict(sorted(cnt.items())))

if not dups:
    print("没有重复")
    raise SystemExit

print("\n" + "=" * 90)
for eid, rows in dups[:3]:
    print("\nevent_id = %s   出现 %d 次" % (eid, len(rows)))
    keys = set()
    for r in rows:
        keys |= set(r.keys())
    for k in sorted(keys):
        vals = [r.get(k, "<缺失>") for r in rows]
        same = len(set(map(str, vals))) == 1
        mark = "  " if same else "**"
        show = vals if not same else [vals[0]]
        s = " | ".join(str(x)[:46] for x in show)
        print("  %s %-14s %s" % (mark, k, s))

# 统计：差异最多的字段是哪些
from collections import Counter
field_diff = Counter()
for eid, rows in dups:
    keys = set()
    for r in rows:
        keys |= set(r.keys())
    for k in keys:
        if len(set(str(r.get(k)) for r in rows)) > 1:
            field_diff[k] += 1
print("\n" + "=" * 90)
print("重复组里发生差异的字段频次（共 %d 组）：" % len(dups))
for k, v in field_diff.most_common(20):
    print("  %-16s %6d 组 (%.1f%%)" % (k, v, 100.0 * v / len(dups)))
