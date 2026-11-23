#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""probe_kafka_dt_cross.py —— 一次性探测：Kafka 里 dt 与 event_time 构成什么关系

重放前必须回答一个问题：
  dt 字段（事件业务日期）与 event_time（事件发生时间）是否一致？
若不一致，按 dt 分流会把"A 日的事件"写进"B 日的 landing 分区"，
而离线 ODS 的 dt 分区又是下游 Hive 的全部过滤条件 —— 一个不一致会污染整条链路。

输出：
  1) (dt × event_time 日期) 交叉表
  2) 每个 dt 内 event_time 的小时分布（看是不是"一口气灌完"的快照）
  3) 每个 dt 的事件类型全分布
不写任何文件，只读。
"""
import json
import sys
import time
from collections import Counter, defaultdict

from kafka import KafkaConsumer, TopicPartition

BOOT = "192.168.128.161:9092"
TOPIC = "music_events"
GROUP = "probe_dt_cross"

c = KafkaConsumer(bootstrap_servers=BOOT, enable_auto_commit=False,
                  group_id=GROUP, max_poll_records=5000,
                  auto_offset_reset="earliest", request_timeout_ms=60000,
                  fetch_max_bytes=52428800)
parts = sorted(c.partitions_for_topic(TOPIC) or [])
tps = [TopicPartition(TOPIC, p) for p in parts]
c.assign(tps)
ends = c.end_offsets(tps)
for tp in tps:
    c.seek(tp, 0)

cross = defaultdict(Counter)      # dt -> {etime_date: n}
hours = defaultdict(Counter)      # dt -> {hour: n}
names = defaultdict(Counter)      # dt -> {event_name: n}
bad = 0
n = 0
t0 = time.time()


def dstr(ms):
    return time.strftime("%Y-%m-%d", time.localtime(ms / 1000.0))


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
                bad += 1
                continue
            dt = o.get("dt") or "?"
            if len(dt) == 8 and dt.isdigit():
                dt = "%s-%s-%s" % (dt[:4], dt[4:6], dt[6:])
            et = o.get("event_time")
            try:
                ms = int(et)
                cross[dt][dstr(ms)] += 1
                hours[dt][time.localtime(ms / 1000.0).tm_hour] += 1
            except (TypeError, ValueError):
                cross[dt]["无效时间"] += 1
            names[dt][o.get("event_name") or "?"] += 1
    if n % 500000 < 5000:
        sys.stderr.write("  已读 %d\n" % n)
        sys.stderr.flush()

c.close()
print("总条数 %d，坏 JSON %d，用时 %.1f 分钟" % (n, bad, (time.time() - t0) / 60.0))

print("\n【一】dt × event_time 日期 交叉表（行=消息里的 dt 字段，列=真实事件发生日）")
alldates = sorted({d for v in cross.values() for d in v})
w = max(12, max(len(x) for x in alldates) + 2)
print("%-12s %s %10s" % ("dt \\ etime", "".join("%*s" % (w, x) for x in alldates), "合计"))
for dt in sorted(cross):
    tot = sum(cross[dt].values())
    row = "".join("%*s" % (w, cross[dt].get(x, 0) or "-") for x in alldates)
    print("%-12s %s %10d" % (dt, row, tot))

print("\n【二】每个 dt 的 event_time 小时分布（0~23 点）")
for dt in sorted(hours):
    hh = hours[dt]
    span = sum(1 for h in hh if hh[h] > 0)
    top = ",".join("%02d点:%d" % (h, hh[h]) for h in sorted(hh)[:6])
    print("%-12s 覆盖 %2d 个小时  %s%s" % (dt, span, top, " ..." if span > 6 else ""))

print("\n【三】每个 dt 的事件类型全分布")
for dt in sorted(names):
    print("%-12s %s" % (dt, dict(names[dt].most_common())))
