#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""probe_cross_dedup.py —— 跨日数据是否与同日桶重叠（低内存版，两遍扫描）

背景（2026-11-08 全量扫描）：
    dt=11-04 的 1,023,569 条里 23,713 条 event_time 落在 11-07
    dt=11-05 里 2,472 条、dt=11-06 里 4,544 条同样落在 11-07
    合计 30,729 条 dt 与 event_time 不同日

要回答：
  Q1 这 30,729 条的 event_id 是否已存在于 event_time 那天的桶里？
     重叠高 → 重灌副本，重放时丢弃，保留事件时间当天的版本
     不重叠 → 独立事件，只是 dt 标错，必须保留

  Q2 桶内 46%~56% 的重复，是否"整条消息逐字节相同"？
     相同  → 传输层重放，按 event_id 去重安全
     不同  → 同 id 多次上报，去重前必须定版本规则

内存设计：161 上 swap 已用 1.9G、可用约 1.1G，
  这里只保存 3 万个跨日 id（约 2MB），不用任何全量集合。
"""
import json
import sys
import time
import zlib
from collections import Counter

from kafka import KafkaConsumer, TopicPartition

BOOT = "192.168.128.161:9092"
TOPIC = "music_events"


def make_consumer(gid):
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


def dstr(ms):
    return time.strftime("%Y-%m-%d", time.localtime(ms / 1000.0))


# ── 第 1 遍：收集跨日单元格的 id 指纹 ──────────────────────────────────────
print("第 1 遍：收集 dt != event_time 的 event_id ……")
c, tps, ends = make_consumer("probe_cross_p1")
cross_ids = {}          # (dt, ed) -> {id_fp: 1}
cell_rows = Counter()   # (dt, ed) -> 行数
n = 0
ifp_of = {}             # 只为跨日的 3 万条记录 id 指纹，极轻
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
            dt = o.get("dt") or "?"
            if len(dt) == 8 and dt.isdigit():
                dt = "%s-%s-%s" % (dt[:4], dt[4:6], dt[6:])
            try:
                ed = dstr(int(o.get("event_time")))
            except (TypeError, ValueError):
                ed = "?"
            cell_rows[(dt, ed)] += 1
            if dt != ed:
                h = zlib.crc32(eid.encode("utf-8"))
                cross_ids.setdefault((dt, ed), set()).add(h)
c.close()
print("  扫完 %d 条；跨日单元格 %d 个，唯一 id 合计 %d"
      % (n, len(cross_ids), sum(len(v) for v in cross_ids.values())))

# ── 第 2 遍：统计这些 id 在同日桶里出现多少次 ───────────────────────────────
print("第 2 遍：检查跨日 id 是否也出现在 dt==event_time 的桶里 ……")
hit = Counter()
c, tps, ends = make_consumer("probe_cross_p2")
# 按 event_time 日期建索引，避免每条消息都遍历所有跨日单元格
by_ed = {}
for (cdt, ced), s in cross_ids.items():
    by_ed.setdefault(ced, []).append((cdt, s))
n2 = 0
while True:
    b = c.poll(timeout_ms=3000, max_records=5000)
    if not b:
        if all(c.position(tp) >= ends[tp] for tp in tps):
            break
        continue
    for tp, recs in b.items():
        for r in recs:
            n2 += 1
            try:
                o = json.loads(r.value.decode("utf-8"))
            except Exception:
                continue
            eid = o.get("event_id")
            if not eid:
                continue
            dt = o.get("dt") or "?"
            if len(dt) == 8 and dt.isdigit():
                dt = "%s-%s-%s" % (dt[:4], dt[4:6], dt[6:])
            try:
                ed = dstr(int(o.get("event_time")))
            except (TypeError, ValueError):
                continue
            if dt != ed:
                continue
            buckets = by_ed.get(ed)
            if not buckets:
                continue
            h = zlib.crc32(eid.encode("utf-8"))
            for cdt, s in buckets:
                if h in s:
                    hit[(cdt, ed)] += 1
c.close()

# ── Q2：桶内重复是否逐字节相同（抽样 partition 2 末段 30 万条）──────────────
print("第 3 步：抽样检查重复是否整条消息相同 ……")
c = KafkaConsumer(bootstrap_servers=BOOT, enable_auto_commit=False,
                  group_id="probe_dup_bytes", max_poll_records=5000,
                  auto_offset_reset="earliest", request_timeout_ms=60000)
tp = TopicPartition(TOPIC, 2)
c.assign([tp])
end = c.end_offsets([tp])[tp]
start = max(0, end - 300000)
c.seek(tp, start)
seen = {}
dup_same = dup_diff = dup_n = 0
got = 0
while got < 300000:
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
            fp = zlib.crc32(raw)
            prev = seen.get(eid)
            if prev is None:
                seen[eid] = fp
            else:
                dup_n += 1
                if prev == fp:
                    dup_same += 1
                else:
                    dup_diff += 1
c.close()

# ── 输出 ──────────────────────────────────────────────────────────────────
print("\n" + "=" * 88)
print("【Q1】跨日数据 vs 同日桶")
print("%-12s %-12s %8s %8s %8s %s" % ("dt字段", "event_time", "唯一id", "同日命中", "命中率", "判定"))
for (cdt, ced), s in sorted(cross_ids.items()):
    h = hit.get((cdt, ced), 0)
    rate = 100.0 * h / len(s) if s else 0
    verdict = "重灌副本" if rate > 80 else ("部分重叠" if rate > 5 else "独立事件")
    print("%-12s %-12s %8d %8d %7.1f%% %s" % (cdt, ced, len(s), h, rate, verdict))

print("\n【Q2】重复的字节一致性（partition 2 末 30 万条抽样）")
print("  抽样 %d 条，其中重复 %d 对：逐字节相同 %d，内容不同 %d"
      % (got, dup_n, dup_same, dup_diff))
print("  → %s" % ("重复是整条消息重放，按 event_id 去重安全"
                  if dup_diff == 0 else
                  "存在同 id 不同内容，去重前必须先定版本规则"))

print("\n【Q3】各单元格行数（dt × event_time）")
for k in sorted(cell_rows):
    tag = "  <-跨日" if k[0] != k[1] else ""
    print("  dt=%-11s etime=%-11s %9d%s" % (k[0], k[1], cell_rows[k], tag))
