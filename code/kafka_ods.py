#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Kafka-as-ODS 读取层（v10 / 2026-11-07）。

docs/17 §3.2 的「Kafka 唯一入口」在 v10 补完最后一步：实时侧不再落
Doris `music_dw.ods_music_event`，topic `ods_music_event` **本身就是 ODS**。
过去运维/对账脚本查那张表拿「ODS 行数 / 入仓时间 / 会话明细」，
现在统一从这里取：累计条数用 log-end-offset，近期明细用时间窗口样本。

为什么这么改：
  job_a 原本双路写出（Kafka topic + Doris 表），job_b 却只读 topic，
  那张 Doris 表是「第二份原始明细」，既占存储又制造「两份 ODS」的口径歧义。
  实时侧本来不分析历史（用户 11-07 明确），所以原始层用 Kafka 足够。

边界（答辩要讲清楚）：
  · `music_events` / `ods_music_event` 的 retention.ms = 259200000（3 天），
    所以实时 ODS 的可回溯/重放窗口是 3 天；持久原始层是离线 HDFS ODS(ORC)。
  · `window_events()` 是「近 N 分钟」滚动指标的样本读取，受 max_messages
    与读取时限约束，不用于全量精确对账；全量对账用 offset 差 + DWD 各表增量。
  · 依赖 kafka-python，在 161 上执行（本机 Windows 未装也能静态检查）。

用法：
    from kafka_ods import offsets, window_events
    end, consumed, lag = offsets(group="rt_job_b_dwd_r3")
    evs = window_events(600)          # 近 10 分钟原始事件
"""
import json
import time

BOOTSTRAP = "192.168.128.161:9092"
RAW_TOPIC = "music_events"          # 造数器唯一入口（docs/17 §3.2）
ODS_TOPIC = "ods_music_event"       # 实时 ODS = 这个 topic
ODS_GROUP = "rt_job_b_dwd_r3"       # job_b 消费组
RETENTION_MS = 259200000            # 3 天，与 topic 动态配置一致


def _partitions(c, topic):
    from kafka import TopicPartition
    ps = c.partitions_for_topic(topic) or []
    return [TopicPartition(topic, p) for p in sorted(ps)]


def offsets(topic=ODS_TOPIC, group=None):
    """返回 (log_end_total, committed_total, lag)。

    log_end_total = topic 累计写入条数（即 ODS 累计行数）
    committed_total = 消费组已提交位点之和（已被 job_b 读走）
    lag = 两者之差，理想为 0；group=None 时后两项为 None。
    """
    from kafka import KafkaConsumer
    # committed() 要求 group_id；只查 log-end-offset 时给一个只读探针组名，
    # 不订阅、不提交，不会影响 rt_job_b_dwd_r3 的真实位点。
    c = KafkaConsumer(bootstrap_servers=BOOTSTRAP, enable_auto_commit=False,
                      consumer_timeout_ms=3000,
                      group_id=group or "kafka_ods_probe")
    try:
        tps = _partitions(c, topic)
        if not tps:
            return 0, None, None
        end = c.end_offsets(tps)
        total_end = sum(end.values())
        if not group:
            return total_end, None, None
        total_com = 0
        total_lag = 0
        for tp in tps:
            com = c.committed(tp)
            com = com if com is not None else 0
            total_com += com
            total_lag += max(end[tp] - com, 0)
        return total_end, total_com, total_lag
    finally:
        c.close()


def window_events(seconds=600, topic=ODS_TOPIC, max_messages=200000,
                  read_seconds=20):
    """读最近 `seconds` 秒的原始事件，返回 list[dict]（已 json.loads）。

    用 offsets_for_times 定位起点，避免把 3 天 retention 全量扫一遍；
    read_seconds 是读取时限，防止常驻采集器被慢 topic 拖住。
    """
    from kafka import KafkaConsumer
    c = KafkaConsumer(bootstrap_servers=BOOTSTRAP, enable_auto_commit=False,
                      consumer_timeout_ms=3000, auto_offset_reset="latest")
    try:
        tps = _partitions(c, topic)
        if not tps:
            return []
        since_ms = int(time.time() * 1000) - seconds * 1000
        starts = c.offsets_for_times({tp: since_ms for tp in tps})
        ends = c.end_offsets(tps)
        c.assign(tps)
        for tp in tps:
            o = starts.get(tp)
            c.seek(tp, o.offset if o is not None else ends[tp])
        out = []
        deadline = time.time() + read_seconds
        while time.time() < deadline and len(out) < max_messages:
            batch = c.poll(timeout_ms=1000)
            if not batch:
                # 已追到各分区末尾就可以提前收工
                if all(c.position(tp) >= ends[tp] for tp in tps):
                    break
                continue
            for _, recs in batch.items():
                for r in recs:
                    try:
                        out.append(json.loads(r.value.decode("utf-8")))
                    except Exception:
                        pass
            if all(c.position(tp) >= ends[tp] for tp in tps):
                break
        return out
    finally:
        c.close()


def session_stats(events, tail_grace_s=60):
    """近窗口会话指标：会话数 / 平均每会话事件数 / 单事件会话数 / 跳出率。

    等价于原 SQL 里 `GROUP BY session_id` 的那段，只是数据源从
    Doris ODS 表换成 Kafka topic 的窗口样本。

    【为什么先按 event_id 去重】
        Kafka 是 at-least-once，同一批消息可能被重复读到；Flink 作业重启、
        消费位点回退时更明显。不去重的话每个会话都被多算一份 ——
        「人均事件数」直接翻倍、「跳出率」被压到 0（没有会话只剩 1 条了）。
        实测 2026-11-08：造数器设计 55.7 条/会话，面板上显示 112.35。

    【为什么剔除「还在进行」的会话】
        这是滚动窗口统计会话的固有问题：一个会话跨过窗口右边界时，
        窗口里只截到它的前半段，事件数变少，很容易被误判成「跳出会话」。
        实测同一批数据，跳出率在 32.52% 和 0.00% 之间来回跳 ——
        跳的不是业务，是边界切在哪儿。
        所以只统计「最后一条事件距窗口末尾超过 tail_grace_s 秒」的会话，
        也就是基本已经结束的那些；窗口里刚发生的会话下一轮再算。
    """
    seen = set()
    counts = {}
    last_ms = {}
    dup = 0
    for ev in events:
        sid = ev.get("session_id") or ""
        if not sid:
            continue
        eid = ev.get("event_id")
        if eid:
            if eid in seen:
                dup += 1
                continue
            seen.add(eid)
        counts[sid] = counts.get(sid, 0) + 1
        try:
            ts = int(ev.get("event_time") or 0)
        except (TypeError, ValueError):
            ts = 0
        if ts > last_ms.get(sid, 0):
            last_ms[sid] = ts

    if tail_grace_s and last_ms:
        # 基准取「窗口内最晚的事件时间」，不是 now。
        # 用 now 做基准有个死穴：数据一停流，now 就永远跑在事件时间前面，
        # 所有会话都被判成「还没结束」而全部剔除 —— 实测某轮会话数直接掉到 0。
        # 以窗口自己的最晚事件为锚，停流多久都能算出「最后那批会话」的跳出率。
        ref = max(last_ms.values())
        cut = ref - tail_grace_s * 1000
        counts = {k: v for k, v in counts.items() if not last_ms.get(k) or last_ms[k] <= cut}

    n = len(counts)
    if n == 0:
        return 0, 0.0, 0, 0.0
    bounce = sum(1 for c in counts.values() if c == 1)
    return n, round(sum(counts.values()) * 1.0 / n, 2), bounce, \
        round(bounce * 100.0 / n, 2)

def tail_events(n=800, topic=ODS_TOPIC):
    """取 topic 末尾 n 条消息（不按时间，按条数）。

    用途：对账抽样。`window_events()` 按时间窗取，灌数节奏一变就取不到；
    而「最近 N 条」在任何时刻都有意义，适合验证
    「进入 ODS 的 event_id 是否都能在 DWD 并集里找到」。
    """
    from kafka import KafkaConsumer
    c = KafkaConsumer(bootstrap_servers=BOOTSTRAP, enable_auto_commit=False,
                      consumer_timeout_ms=3000, auto_offset_reset="latest")
    try:
        tps = _partitions(c, topic)
        if not tps:
            return []
        ends = c.end_offsets(tps)
        per = max(1, n // max(1, len(tps)))
        out = []
        for tp in tps:
            begin = max(0, ends[tp] - per)
            if ends[tp] <= begin:
                continue
            c.assign([tp])
            c.seek(tp, begin)
            got = 0
            while got < per:
                batch = c.poll(timeout_ms=1200).get(tp, [])
                if not batch:
                    break
                for r in batch:
                    try:
                        out.append(json.loads(r.value.decode("utf-8")))
                    except Exception:
                        pass
                    got += 1
                    if got >= per:
                        break
        return out
    finally:
        c.close()
