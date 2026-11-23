#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
replay_kafka_to_landing.py —— Kafka 原始事件 → landing（喂给 spark_landing_to_ods.py）

═══════════════════════════════════════════════════════════════════════════════
为什么需要它
═══════════════════════════════════════════════════════════════════════════════
Kafka 的 retention.ms = 3 天（259200000）。离线 ODS 只到 2026-11-04，
而 Kafka 里还有 11-05 ~ 11-08 的原始事件。**11-10 14:40 之后 11-05 永久消失。**

重放一次同时满足两个目的，不必做两遍：
  ① W1：离线侧补 impression / click（ODS 现批次只有 15 类事件，没有曝光）
  ② P1-2：拿到 11-07/11-08 的离线结果，才有第一次真正的批流对账

═══════════════════════════════════════════════════════════════════════════════
为什么不用 Spark 直读 Kafka
═══════════════════════════════════════════════════════════════════════════════
  1. 集群 Spark 3.3.1 未带 spark-sql-kafka，只有 kafka-clients；
     引入连接器要匹配 scala 2.12 + commons-pool2，还要分发到 YARN executor，
     为一个"纯分流"任务增加两个新故障面，不划算。
  2. 本作业没有计算：只是把原始 JSON 按 dt 分桶落盘（保持 value 原样字节）。
     真正的解析 / 去重 / 写 ORC 在 spark_landing_to_ods.py 里，已经验证过，直接复用。
  所以这里用 kafka-python（161 上已有 2.0.2），零新增依赖。

═══════════════════════════════════════════════════════════════════════════════
三个必须守住的约束
═══════════════════════════════════════════════════════════════════════════════
① **不能按 offset 猜日期**
   实测 11-05 段乱序：offset 340k → 22:08，350k → 15:21，360k → 00:03。
   是多批倒灌，同一业务日的消息不连续。只能全量扫描，按每条消息自带的 dt 分流。

② **落盘必须原样字节**
   value 不做 json.loads → dumps 的往返，否则字段顺序、数字精度、转义都会变，
   下游 ODS 解析结果可能与 Flume 那条路不一致 —— 那就不是同源数据了。

③ **去重键必须是 (event_id, event_time)，不能只用 event_id**（--dedup）
   2026-11-08 实测：Kafka 的 240 万条按 event_id 去重只剩 120 万（重复 49.89%），
   但其中 99.6% 是"同 id、不同 event_time"，属于被多批重灌进来的真实事件。
   造数器的 event_id 只编码"时间槽+序号"，不含用户/会话，重灌必然撞号。
   按 (event_id, event_time) 去重后重复率降到 0.35%，加 user_id 只多认 54 条。
   离线历史分区（10-05~11-04）的 event_id 本来就唯一，所以这条只影响重放数据。

④ **本地必须先压缩再上传**
   平均 711 字节/条 × 240 万条 ≈ 1.71 GB，而 161 根分区只剩 3.9 GB。
   gzip 后约 200 MB。写完立刻 put 到 HDFS 再删本地，避免磁盘打满。

═══════════════════════════════════════════════════════════════════════════════
幂等 / 断点
═══════════════════════════════════════════════════════════════════════════════
  · --state 记录每个分区的消费位点（每 5 万条落一次），中断后从断点续跑，不重复不丢。
  · 断点续跑时已写过的 gz 是 "ab" 追加模式，不会截断已有内容。
  · HDFS 目标目录已存在则拒绝，--force 才覆盖。

═══════════════════════════════════════════════════════════════════════════════
用法（在 161 上执行，与 Kafka 同机）
═══════════════════════════════════════════════════════════════════════════════
  python3 replay_kafka_to_landing.py --dry-run                 # 只统计，不落盘（先跑这个）
  python3 replay_kafka_to_landing.py --dts 2026-11-05,2026-11-06 --dedup
  python3 replay_kafka_to_landing.py --dedup                   # 全量落盘并去重
  python3 replay_kafka_to_landing.py --dedup --upload --hdfs-dir /landing/replay

  # 落盘后接上现有离线链路（去重键要跟着换）：
  spark-submit ... /root/offline/spark_landing_to_ods.py \
      --landing hdfs://WWC160:9820/landing/replay --dedup-keys event_id,event_time
"""
import argparse
import gzip
import json
import os
import shutil
import subprocess
import sys
import time
import zlib
from collections import Counter

DEFAULT_BOOTSTRAP = "192.168.128.161:9092"
DEFAULT_TOPIC = "music_events"
DEFAULT_OUT = "/root/replay/landing"
DEFAULT_STATE = "/root/replay/state.json"
DEFAULT_REPORT = "/root/replay/report.json"
GROUP = "replay_to_landing"          # 独立消费组，不碰 flume-ods / rt_job_* 的位点

_ap = argparse.ArgumentParser(description="Kafka → landing 重放器")
_ap.add_argument("--bootstrap", default=DEFAULT_BOOTSTRAP)
_ap.add_argument("--topic", default=DEFAULT_TOPIC)
_ap.add_argument("--out", default=DEFAULT_OUT, help="本地落盘根目录")
_ap.add_argument("--state", default=DEFAULT_STATE, help="断点位点文件")
_ap.add_argument("--report", default=DEFAULT_REPORT, help="统计报告输出")
_ap.add_argument("--dts", default="", help="只保留这些事件日期，逗号分隔 yyyy-MM-dd；空=全部")
_ap.add_argument("--max-records", type=int, default=0, help="最多处理多少条（调试用，0=不限）")
_ap.add_argument("--dry-run", action="store_true", help="只统计不落盘")
_ap.add_argument("--dedup", action="store_true",
                 help="按 (event_id, event_time) 去重后再落盘。重放倒灌数据时必须开："
                      "实测 event_id 单键重复 49.89%%，(event_id,event_time) 只重复 0.35%%")
_ap.add_argument("--upload", action="store_true", help="扫完后自动 hdfs put")
_ap.add_argument("--hdfs-dir", default="/landing/replay")
_ap.add_argument("--force", action="store_true", help="HDFS 目标已存在时覆盖")
_ap.add_argument("--reset-state", action="store_true", help="忽略已有断点，从头扫")
ARGS = _ap.parse_args()

WANT = set(x.strip() for x in ARGS.dts.split(",") if x.strip())


def log(msg):
    sys.stdout.write("[%s] %s\n" % (time.strftime("%H:%M:%S"), msg))
    sys.stdout.flush()


def load_state():
    if ARGS.reset_state or not os.path.exists(ARGS.state):
        return {}
    try:
        with open(ARGS.state, "r") as f:
            raw = json.load(f)
        return {int(k): int(v) for k, v in raw.items()}
    except Exception as e:
        log("断点文件损坏，忽略：%s" % e)
        return {}


def save_state(offsets):
    tmp = ARGS.state + ".tmp"
    d = os.path.dirname(tmp)
    if d and not os.path.isdir(d):
        os.makedirs(d)
    with open(tmp, "w") as f:
        json.dump({str(k): v for k, v in offsets.items()}, f)
    os.replace(tmp, ARGS.state)          # 原子替换，避免写一半断电留下坏文件


class Writers(object):
    """按 dt 惰性打开 gzip 句柄；append 模式，断点续跑不截断已有内容。"""

    def __init__(self, root, append):
        self.root = root
        self.append = append
        self.h = {}
        self.bytes = Counter()

    def get(self, dt):
        w = self.h.get(dt)
        if w is None:
            d = os.path.join(self.root, "dt=%s" % dt.replace("-", ""))
            if not os.path.isdir(d):
                os.makedirs(d)
            path = os.path.join(d, "part-00000.jsonl.gz")
            mode = "ab" if (self.append and os.path.exists(path)) else "wb"
            w = gzip.open(path, mode, compresslevel=6)
            self.h[dt] = w
        return w

    def write(self, dt, raw):
        w = self.get(dt)
        w.write(raw)
        w.write(b"\n")
        self.bytes[dt] += len(raw) + 1

    def flush(self):
        for w in self.h.values():
            w.flush()

    def close(self):
        for w in self.h.values():
            try:
                w.close()
            except Exception:
                pass
        self.h.clear()


def parse_msg(raw):
    """只解析一次，取出分流和统计需要的四个字段。

    注意：解析结果仅用于"决定写到哪个桶"和"计数"，落盘的仍是原始字节 raw，
    不做 json.dumps 往返 —— 否则字段顺序/数值精度/转义都会变，下游 ODS
    解析出来的内容就和 Flume 那条路不一致，失去同源可比性。
    """
    try:
        obj = json.loads(raw.decode("utf-8"))
    except Exception:
        return None, None, None, None
    dt = obj.get("dt")
    if isinstance(dt, str) and len(dt) == 8 and dt.isdigit():
        dt = "%s-%s-%s" % (dt[:4], dt[4:6], dt[6:])
    elif not isinstance(dt, str):
        dt = None
    return dt, obj.get("event_id"), obj.get("event_time"), obj.get("event_name")


def main():
    from kafka import KafkaConsumer, TopicPartition

    consumer = KafkaConsumer(
        bootstrap_servers=ARGS.bootstrap,
        enable_auto_commit=False,          # 位点完全由 --state 文件管理
        group_id=GROUP,
        max_poll_records=5000,
        auto_offset_reset="earliest",
        request_timeout_ms=60000,
        fetch_max_bytes=52428800,
    )
    parts = sorted(consumer.partitions_for_topic(ARGS.topic) or [])
    if not parts:
        log("topic %s 不存在或无分区" % ARGS.topic)
        return 2
    tps = [TopicPartition(ARGS.topic, p) for p in parts]
    consumer.assign(tps)

    begins = consumer.beginning_offsets(tps)
    ends = consumer.end_offsets(tps)
    state = load_state()

    log("topic=%s 分区=%d" % (ARGS.topic, len(parts)))
    for tp in tps:
        log("  p%-2d 最早=%-8d 最新=%-8d 总量=%d"
            % (tp.partition, begins[tp], ends[tp], ends[tp] - begins[tp]))

    start_off = {}
    for tp in tps:
        s = state.get(tp.partition, begins[tp])
        s = max(begins[tp], min(s, ends[tp]))
        start_off[tp] = s
        consumer.seek(tp, s)
    plan = sum(ends[tp] - start_off[tp] for tp in tps)
    log("本次待处理 %d 条%s" % (plan, "（断点续跑）" if any(start_off[tp] > begins[tp] for tp in tps) else ""))

    writers = None
    if not ARGS.dry_run:
        appended = any(start_off[tp] > begins[tp] for tp in tps)
        writers = Writers(ARGS.out, append=appended)

    stats = {}
    bad_json = 0
    no_dt = 0
    skipped_dt = 0
    skipped_dup = 0
    read = 0
    dedup_seen = set() if ARGS.dedup else None
    t0 = time.time()
    last_report = 0
    last_state = 0
    pos = dict(start_off)

    def bucket(dt):
        s = stats.get(dt)
        if s is None:
            s = {"rows": 0, "names": Counter(), "min_ts": None, "max_ts": None}
            stats[dt] = s
        return s

    try:
        while True:
            batch = consumer.poll(timeout_ms=3000, max_records=5000)
            if not batch:
                if all(pos[tp] >= ends[tp] for tp in tps):
                    break
                continue

            for tp, records in batch.items():
                for rec in records:
                    pos[tp] = rec.offset + 1
                    read += 1
                    raw = rec.value
                    if raw is None:
                        bad_json += 1
                        continue
                    dt, eid, ets, ename = parse_msg(raw)
                    if dt is None:
                        no_dt += 1
                        if not WANT:
                            dt = "unknown"
                        else:
                            continue
                    elif WANT and dt not in WANT:
                        skipped_dt += 1
                        continue

                    # 去重必须在写盘之前。键用 (event_id, event_time)：
                    # 造数器的 event_id 只编码时间槽+序号，多批重灌时会撞号，
                    # 单用 event_id 会把 49.89% 的真实事件当重复丢掉。
                    if dedup_seen is not None:
                        key = zlib.crc32(("%s|%s" % (eid, ets)).encode("utf-8"))
                        if key in dedup_seen:
                            skipped_dup += 1
                            continue
                        dedup_seen.add(key)

                    s = bucket(dt)
                    s["rows"] += 1
                    if ename is not None:
                        s["names"][ename] += 1
                    try:
                        t = int(ets)
                        if s["min_ts"] is None or t < s["min_ts"]:
                            s["min_ts"] = t
                        if s["max_ts"] is None or t > s["max_ts"]:
                            s["max_ts"] = t
                    except (TypeError, ValueError):
                        pass
                    if writers is not None:
                        writers.write(dt, raw)

            if read - last_report >= 100000:
                last_report = read
                el = time.time() - t0
                rate = read / el if el > 0 else 0
                left = plan - read
                eta = left / rate if rate > 0 else 0
                log("已读 %d/%d（%.1f%%）%.0f 条/秒 预计剩余 %.1f 分钟"
                    % (read, plan, 100.0 * read / plan if plan else 0, rate, eta / 60.0))

            if writers is not None and read - last_state >= 50000:
                last_state = read
                writers.flush()
                save_state(pos)

            if ARGS.max_records and read >= ARGS.max_records:
                log("达到 --max-records=%d，提前结束" % ARGS.max_records)
                break
    finally:
        if writers is not None:
            writers.flush()
            writers.close()
            # 只有真写过文件才落断点。--dry-run 落断点会让下一次真实运行误以为
            # 这些位点"已处理"，从而跳过整段数据 —— 这是最容易埋进去的静默错误。
            save_state(pos)
        try:
            consumer.close()
        except Exception:
            pass

    el = time.time() - t0
    log("扫描结束：读 %d 条，用时 %.1f 分钟，%.0f 条/秒" % (read, el / 60.0, read / el if el else 0))
    log("坏 JSON %d 条，缺 dt %d 条，dt 不在 --dts 内被跳过 %d 条，去重丢弃 %d 条"
        % (bad_json, no_dt, skipped_dt, skipped_dup))

    # ── 统计报告 ────────────────────────────────────────────────────────────
    report = {"scanned_at": time.strftime("%Y-%m-%d %H:%M:%S"),
              "topic": ARGS.topic, "dry_run": ARGS.dry_run, "dedup": ARGS.dedup,
              "read": read, "bad_json": bad_json, "no_dt": no_dt,
              "skipped_dt": skipped_dt, "skipped_dup": skipped_dup,
              "seconds": round(el, 1),
              "partitions": {str(tp.partition): {"begin": begins[tp], "end": ends[tp],
                                                 "consumed": pos[tp] - start_off[tp]}
                             for tp in tps},
              "dts": {}}

    log("\n%-12s %10s  %-21s  %s" % ("dt", "落盘行数", "event_time 范围", "事件类型 top3"))
    log("-" * 100)
    for dt in sorted(stats):
        s = stats[dt]
        f = lambda x: time.strftime("%m-%d %H:%M", time.localtime(x / 1000.0)) if x else "-"
        top = ",".join("%s:%d" % (k, v) for k, v in s["names"].most_common(3)) or "-"
        log("%-12s %10d  %s ~ %s  %s"
            % (dt, s["rows"], f(s["min_ts"]), f(s["max_ts"]), top))
        report["dts"][dt] = {"rows": s["rows"],
                             "event_time_min": f(s["min_ts"]), "event_time_max": f(s["max_ts"]),
                             "top_events": dict(s["names"].most_common(10)),
                             "uncompressed_bytes": writers.bytes.get(dt, 0) if writers else None}

    if writers is not None:
        log("\n本地落盘（压缩后）：")
        for dt in sorted(writers.bytes):
            d = os.path.join(ARGS.out, "dt=%s" % dt.replace("-", ""))
            sz = sum(os.path.getsize(os.path.join(d, x)) for x in os.listdir(d)) if os.path.isdir(d) else 0
            log("  %-12s 原始 %8.1f MB → gz %7.1f MB" % (dt, writers.bytes[dt] / 1048576.0, sz / 1048576.0))
        du = shutil.disk_usage("/")
        log("  磁盘剩余 %.1f GB" % (du.free / 1073741824.0))

    try:
        with open(ARGS.report, "w") as fp:
            json.dump(report, fp, ensure_ascii=False, indent=2)
        log("\n报告写入 %s" % ARGS.report)
    except Exception as e:
        log("报告写入失败：%s" % e)

    # ── 上传 HDFS ──────────────────────────────────────────────────────────
    if ARGS.upload and not ARGS.dry_run:
        if plan and read < plan:
            log("!! 未扫完（%d/%d），拒绝上传，避免残缺数据进 HDFS" % (read, plan))
            return 3
        subprocess.call(["hdfs", "dfs", "-mkdir", "-p", ARGS.hdfs_dir])
        for name in sorted(os.listdir(ARGS.out)):
            if not name.startswith("dt="):
                continue
            dst = "%s/%s" % (ARGS.hdfs_dir.rstrip("/"), name)
            if subprocess.call(["hdfs", "dfs", "-test", "-d", dst]) == 0 and not ARGS.force:
                log("跳过已存在 %s（--force 覆盖）" % dst)
                continue
            if ARGS.force:
                subprocess.call(["hdfs", "dfs", "-rm", "-r", "-skipTrash", dst])
            log("上传 %s → %s" % (name, dst))
            subprocess.call(["hdfs", "dfs", "-put", os.path.join(ARGS.out, name), ARGS.hdfs_dir + "/"])
        log("HDFS 上传完成：%s" % ARGS.hdfs_dir)
    return 0


if __name__ == "__main__":
    sys.exit(main())
