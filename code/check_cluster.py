#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
集群巡检脚本（在 Windows 宿主机上运行）
====================================
一次性检查：服务端口、Doris 状态与数据量、Kafka 主题与积压。

用法：
    D:\\魏文铖\\python\\python.exe code/check_cluster.py
    D:\\魏文铖\\python\\python.exe code/check_cluster.py --topic music_events

退出码：0 全部正常，1 有服务未启动。
"""

import argparse
import socket
import sys

HOSTS = {
    "160": "192.168.128.160",
    "161": "192.168.128.161",
    "162": "192.168.128.162",
    "163": "192.168.128.163",
}

# (显示名, 主机, 端口, 是否必需)
SERVICES = [
    ("NameNode        160", "160", 9820, True),
    ("ResourceManager 160", "160", 8032, True),
    ("HiveMetastore   160", "160", 9083, True),
    ("ZooKeeper       161", "161", 2181, True),
    ("ZooKeeper       162", "162", 2181, True),
    ("ZooKeeper       163", "163", 2181, True),
    ("Kafka           161", "161", 9092, True),
    ("Kafka           162", "162", 9092, True),
    ("Kafka           163", "163", 9092, True),
    ("Flink           161", "161", 8081, True),
    ("DorisFE         163", "163", 9030, True),
    ("DorisBE         163", "163", 8043, True),
]


def check_ports():
    ok = True
    print("=" * 56)
    print("服务端口")
    print("=" * 56)
    for name, host_key, port, required in SERVICES:
        host = HOSTS[host_key]
        s = socket.socket()
        s.settimeout(2)
        try:
            s.connect((host, port))
            print(f"  [OK]   {name}  {host}:{port}")
        except Exception:
            print(f"  [DOWN] {name}  {host}:{port}")
            if required:
                ok = False
        finally:
            s.close()
    return ok


def check_doris():
    print("=" * 56)
    print("Doris")
    print("=" * 56)
    try:
        import pymysql
    except ImportError:
        print("  [SKIP] 未安装 pymysql")
        return True

    try:
        conn = pymysql.connect(host=HOSTS["163"], port=9030, user="root",
                               password="", charset="utf8mb4", connect_timeout=5)
        cur = conn.cursor()

        cur.execute("SHOW BACKENDS")
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()
        i_alive, i_tablet = cols.index("Alive"), cols.index("TabletNum")
        print(f"  BE: alive={[r[i_alive] for r in rows]}  tablets={[r[i_tablet] for r in rows]}")

        cur.execute("SHOW TABLES FROM music_dw")
        tables = [r[0] for r in cur.fetchall()]
        print(f"  表: {tables if tables else '(空)'}")

        # v10：实时 ODS 是 Kafka topic，不再是 Doris 表。
        # 打印 topic 累计条数与 job_b 消费组 LAG，作为入口健康度证据。
        try:
            import os, sys
            sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
            from kafka_ods import offsets, ODS_GROUP
            end, consumed, lag = offsets(group=ODS_GROUP)
            print(f"  ODS topic ods_music_event: 累计 {end} 条 / "
                  f"已消费 {consumed} 条 / LAG {lag}（保留期 3 天）")
        except Exception as e:
            print(f"  ODS topic 位点读取跳过: {e}")
        if "dwd_unrouted" in tables:
            cur.execute("SELECT COUNT(*) FROM music_dw.dwd_unrouted")
            print(f"  dwd_unrouted 兜底行数: {cur.fetchone()[0]}")

        conn.close()
        return True
    except Exception as e:
        print(f"  [ERR] {e}")
        return False


def check_kafka(topic):
    print("=" * 56)
    print("Kafka")
    print("=" * 56)
    try:
        from kafka import KafkaConsumer, KafkaAdminClient
        from kafka.structs import TopicPartition
    except ImportError:
        print("  [SKIP] 未安装 kafka-python")
        return True

    bootstrap = f"{HOSTS['161']}:9092"
    try:
        admin = KafkaAdminClient(bootstrap_servers=bootstrap, request_timeout_ms=8000)
        topics = sorted(admin.list_topics())
        print(f"  主题: {topics}")
        admin.close()
    except Exception as e:
        print(f"  [ERR] {e}")
        return False

    if topic and topic in topics:
        try:
            consumer = KafkaConsumer(bootstrap_servers=bootstrap, request_timeout_ms=8000)
            parts = consumer.partitions_for_topic(topic)
            tps = [TopicPartition(topic, p) for p in sorted(parts)]
            ends = consumer.end_offsets(tps)
            total = sum(ends.values())
            detail = ", ".join(f"p{tp.partition}={ends[tp]}" for tp in tps)
            print(f"  {topic} 总消息数: {total}  ({detail})")
            consumer.close()
        except Exception as e:
            print(f"  [ERR] 读取 {topic} offset 失败: {e}")
    return True


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--topic", default="music_events")
    args = ap.parse_args()

    ports_ok = check_ports()
    check_doris()
    check_kafka(args.topic)
    print("=" * 56)
    print("巡检完成" if ports_ok else "有服务未启动，见上方 [DOWN]")
    sys.exit(0 if ports_ok else 1)


if __name__ == "__main__":
    main()
