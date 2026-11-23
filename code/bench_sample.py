#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""压测采样：以固定频率采集「入口速率 → 各层延迟 → 消费积压」，输出 TSV。

用途：为「速率—延迟曲线」提供数据点。压测时按档位提高生成器速率，
本脚本每 --interval 秒采一次，把 (入口速率, dwd延迟, kafka积压, B写出速率) 写到 stdout，
重定向到文件后用任意工具画曲线（见 docs/14 的曲线绘制说明）。

为什么单独写一个、不复用 collect_ops_metrics.py：
  采集器 60 秒一轮 + 写 Doris，是「健康监控」口径；压测要找延迟拐点，
  需要 15 秒级别的采样密度，且要实时看到裸值、不经过 Doris 的 upsert 覆盖。

关键口径（必须和采集器一致，否则曲线对不上大屏）：
  · 入口速率 = 数 ODS 的 etl_time 近 60 秒行数，而不是用生成器名义 --rate
    （生成器实测有 ± 波动，且双进程翻倍时名义值不可信）
  · DWD 延迟 = now - max(dwd_play_end.etl_time)，衡量「明细从进入到落库」的真实时延
  · kafka 积压 = B 消费组的 end_offsets - committed_offsets（滞后行数，最敏感的信号）
  · B 写出速率 = 数 9 张 DWD 表近 60 秒合计，衡量「下游真实吞吐」

用法：
  python code/bench_sample.py --interval 15 --duration 600 > bench_20261103.tsv
  第一列是采集时刻，之后是 tab 分隔的数值，方便 Excel/pandas 直接读。

压测流程（配合生成器多档位）：
  1. 起本脚本（--duration 覆盖全程）
  2. 分档灌数：50 → 100 → 200 → 400 → 800 条/s，每档跑 3~5 分钟
  3. 每档之间停 2 分钟，让积压排空、延迟回到基线
  4. 结束后把 tsv 画成「X=入口速率, Y=dwd延迟/kafka积压」的双轴曲线
"""

import argparse
import sys
import time
from datetime import datetime

import pymysql
from kafka import KafkaConsumer
from kafka.structs import TopicPartition

DORIS = dict(host="192.168.128.163", port=9030, user="root", password="",
             database="music_dw", charset="utf8mb4", connect_timeout=5, read_timeout=30)

DWD_TABLES = ["dwd_play_end", "dwd_play_start", "dwd_skip", "dwd_search",
              "dwd_collect", "dwd_share", "dwd_page_view",
              "dwd_order", "dwd_play_error"]


def kafka_lag():
    """B 消费组的滞后行数。每次新建 consumer 代价高，但 15 秒一次可接受。"""
    try:
        c = KafkaConsumer(group_id="rt_job_b_dwd_r3",
                          bootstrap_servers=["192.168.128.161:9092"],
                          enable_auto_commit=False, consumer_timeout_ms=2000,
                          request_timeout_ms=8000)
        parts = [TopicPartition("ods_music_event", p)
                 for p in (c.partitions_for_topic("ods_music_event") or [])]
        end = c.end_offsets(parts)
        total = 0
        for tp in parts:
            com = c.committed(tp)
            if com is not None:
                total += end[tp] - com
        c.close(autocommit=False, timeout_ms=1000)
        return total
    except Exception:
        return None


def sample(cur):
    """采一次，返回 dict。"""
    # v10：ODS = Kafka topic，入仓时间用 DWD 代理；入口速率用 topic 窗口样本
    cur.execute("SELECT MAX(etl_time) FROM dwd_play_end")
    dwd_max = cur.fetchone()[0]

    try:
        import os, sys
        sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
        from kafka_ods import window_events
        ods_1m = len(window_events(60, max_messages=50000))
    except Exception:
        ods_1m = 0

    parts = [f"SELECT COUNT(*) FROM {t} WHERE etl_time >= NOW() - INTERVAL 60 SECOND"
             for t in DWD_TABLES]
    cur.execute(" UNION ALL ".join(parts))
    dwd_1m = sum(r[0] for r in cur.fetchall())

    now = datetime.now()
    dwd_lag = max(0, int((now - dwd_max).total_seconds())) if dwd_max else None

    return {
        "ts": now.strftime("%H:%M:%S"),
        "ods_1m": ods_1m,              # 入口速率（条/分）
        "dwd_1m": dwd_1m,              # B 写出速率（条/分）
        "dwd_lag": dwd_lag,            # DWD 延迟（秒）
        "lag": kafka_lag(),            # B 消费积压（行）
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--interval", type=int, default=15, help="采样间隔秒，默认 15")
    ap.add_argument("--duration", type=int, default=600, help="总时长秒，默认 600")
    args = ap.parse_args()

    header = "ts\tods_1m\tdwd_1m\tdwd_lag_s\tkafka_lag"
    print(header)

    conn = pymysql.connect(**DORIS)
    cur = conn.cursor()
    start = time.time()
    try:
        while time.time() - start < args.duration:
            s = sample(cur)
            print("%s\t%s\t%s\t%s\t%s" % (
                s["ts"], s["ods_1m"], s["dwd_1m"],
                s["dwd_lag"] if s["dwd_lag"] is not None else "",
                s["lag"] if s["lag"] is not None else ""))
            sys.stdout.flush()
            time.sleep(args.interval)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
