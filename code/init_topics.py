#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""创建实时链路需要的两个中间 topic（在 Windows 宿主机上运行）。

与 00_init_topics.sh 功能相同，只是不用把脚本传到虚拟机上。

    music_events     原始埋点      —— 生成器直接写入
    ods_music_event  ODS 层        —— 作业 A 写，作业 B 读
    dwd_play_end     DWD 层        —— 作业 B 写，作业 C 读

v10：Kafka `ods_music_event` 就是实时 ODS（Doris ODS 表已砍），保留 3 天；
实时侧不分析历史，可回溯/重放窗口就是 3 天；持久原始层是离线 HDFS ODS(ORC)。

用法：
    python code/init_topics.py
    python code/init_topics.py                # rf=1 / 保留 3 天，与集群一致
"""

import argparse
import sys

BOOTSTRAP = ["192.168.128.161:9092"]  # v10：Kafka 收敛为单 broker（docs/17 §3.6.1）

TOPICS = [
    ("ods_music_event", 3),
    ("dwd_play_end", 3),
]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--partitions", type=int, default=3)
    ap.add_argument("--replication", type=int, default=1)
    ap.add_argument("--retention-ms", type=int, default=3 * 24 * 3600 * 1000)
    args = ap.parse_args()

    try:
        from kafka import KafkaAdminClient
        from kafka.admin import NewTopic
        from kafka.errors import TopicAlreadyExistsError
    except ImportError:
        print("未安装 kafka-python：pip install kafka-python")
        return 1

    try:
        admin = KafkaAdminClient(bootstrap_servers=BOOTSTRAP, request_timeout_ms=10000)
    except Exception as e:
        print(f"无法连接 Kafka: {e}")
        return 1

    existing = set(admin.list_topics())
    print(f"已有 topic: {sorted(existing)}\n")

    created = []
    for name, partitions in TOPICS:
        if name in existing:
            print(f"  [SKIP] {name} 已存在")
            continue
        try:
            admin.create_topics(
                new_topics=[NewTopic(
                    name=name,
                    num_partitions=args.partitions or partitions,
                    replication_factor=args.replication,
                    topic_configs={"retention.ms": str(args.retention_ms)},
                )],
                validate_only=False,
            )
            print(f"  [OK]   {name}  partitions={args.partitions}  rf={args.replication}")
            created.append(name)
        except TopicAlreadyExistsError:
            print(f"  [SKIP] {name} 已存在（并发创建）")
        except Exception as e:
            print(f"  [FAIL] {name}: {e}")
            if "replication factor" in str(e).lower():
                print("         broker 数量不足 3 个，用 --replication 1 重试")
            admin.close()
            return 1

    admin.close()

    if created:
        print(f"\n已创建 {len(created)} 个 topic，保留 {args.retention_ms // 86400000} 天")
    else:
        print("\n无新增 topic")
    print("下一步：bash sql/realtime/run_jobs.sh（在 161 上执行）")
    return 0


if __name__ == "__main__":
    sys.exit(main())
