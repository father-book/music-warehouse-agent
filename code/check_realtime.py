#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""实时链路就绪检查（在 Windows 宿主机上运行）。

提交三个作业之前跑一遍，把「环境没准备好」和「SQL 写错了」两类问题分开。
否则作业提交后报的错会混在一起，很难判断是环境还是语法。

检查项：
    1. Flink session cluster 是否可达，slot 够不够（三个作业共需 5 个）
    2. 是否已有同名作业在跑（避免重复提交导致数据翻倍）
    3. 是否还有旧的单作业管道在跑（group.id 不同，会与作业 A 抢同一个 topic）
    4. Kafka 三个 topic 是否齐备
    5. Doris 15 张表是否齐备
    6. 已有数据时，做一次分层对账

用法：
    python code/check_realtime.py
    python code/check_realtime.py --host 192.168.128.161

退出码：0 全部就绪，1 有阻塞项。
"""

import argparse
import json
import socket
import sys
import urllib.request

DEFAULT_HOST = "192.168.128.161"
FLINK_PORT = 8081
KAFKA_BOOTSTRAP = "192.168.128.161:9092"
DORIS_HOST = "192.168.128.163"
DORIS_PORT = 9030

NEED_SLOTS = 5

TOPICS = ["music_events", "ods_music_event", "dwd_play_end"]

TABLES = {
    # v10：ODS 就是 Kafka topic `ods_music_event`，不再是 Doris 表。
    # 累计条数用 log-end-offset，窗口明细用 code/kafka_ods.py 取。
    "ODS": [],
    "DWD": ["dwd_play_end", "dwd_play_start", "dwd_skip", "dwd_search",
            "dwd_collect", "dwd_share", "dwd_page_view",
            # 商业化 + 客户端质量明细。对账恒等式必须把它们算进去，
            # 否则「ODS = 明细 + 异常」会因为漏掉两类事件而显示「丢了 N 万条」。
            "dwd_order", "dwd_play_error", "dwd_quality_error",
            # v10：26 类事件里实时暂未建明细表的 15 类，原文落这里，
            # 恒等式必须把它算进去，否则会误报「丢了 N 条」。
            "dwd_unrouted"],
    "DWS": ["dws_play_overview_1min", "dws_song_play_5min", "dws_province_play_5min"],
    "ADS": ["ads_realtime_overview", "ads_song_rank_10min", "ads_province_10min",
            "ads_commercial_daily", "ads_sku_daily", "ads_client_quality_1min"],
}

EXPECTED_JOBS = ["rt_job_a_ods", "rt_job_b_dwd", "rt_job_c_dws", "rt_job_d_ads"]
LEGACY_JOBS = ["insert-into_default_catalog", "music_realtime_pipeline"]

blockers = []
running_names = []


def hr(title):
    print("=" * 60)
    print(title)
    print("=" * 60)


def fetch_json(url, timeout=5):
    with urllib.request.urlopen(url, timeout=timeout) as r:
        return json.loads(r.read().decode("utf-8"))


def check_flink(host):
    hr(f"Flink 集群 {host}:{FLINK_PORT}")
    base = f"http://{host}:{FLINK_PORT}"
    try:
        ov = fetch_json(f"{base}/overview")
    except Exception as e:
        print(f"  [DOWN] 无法连接 Flink REST: {e}")
        blockers.append("Flink session cluster 未启动（在 161 上执行 start-cluster.sh）")
        return

    total = ov.get("slots-total", 0)
    free = ov.get("slots-available", 0)
    tms = ov.get("taskmanagers", 0)
    print(f"  TaskManager: {tms}   slot 总数 {total}   空闲 {free}   需要 {NEED_SLOTS}")
    if total < NEED_SLOTS:
        blockers.append(
            f"slot 只有 {total} 个，四个作业要 {NEED_SLOTS} 个。"
            f"改 flink-conf.yaml 的 taskmanager.numberOfTaskSlots 后重启集群"
        )
    elif free < NEED_SLOTS:
        # 全新提交需要 free >= NEED_SLOTS；但作业已在跑时 free 变小是正常的，
        # 这里不下结论，等读完作业列表在结论段区分。
        print(f"  [INFO] 空闲 slot {free} 个（全新提交需 {NEED_SLOTS} 个；若作业已在跑属正常）")

    try:
        jobs = fetch_json(f"{base}/jobs/overview").get("jobs", [])
    except Exception as e:
        print(f"  [ERR] 读取作业列表失败: {e}")
        return

    running = [j for j in jobs if j.get("state") == "RUNNING"]
    if not running:
        print("  当前没有运行中的作业")
    for j in running:
        print(f"  RUNNING  {j.get('name')}  ({j.get('jid')[:8]})")

    names = [j.get("name", "") for j in running]
    running_names.extend(names)
    for exp in EXPECTED_JOBS:
        if exp in names:
            print(f"  [OK  ] {exp} 已在运行")
    for legacy in LEGACY_JOBS:
        if any(legacy in n for n in names):
            blockers.append(
                f"旧的单作业管道（{legacy}）还在跑，它会与作业 A 抢同一个 topic "
                f"并重复写 ODS。先 flink cancel 再提交新作业"
            )


def check_kafka():
    hr("Kafka topic")
    try:
        from kafka import KafkaAdminClient
    except ImportError:
        print("  [SKIP] 未安装 kafka-python（pip install kafka-python）")
        return

    try:
        admin = KafkaAdminClient(bootstrap_servers=KAFKA_BOOTSTRAP, request_timeout_ms=8000)
        topics = set(admin.list_topics())
        admin.close()
    except Exception as e:
        print(f"  [DOWN] 无法连接 Kafka: {e}")
        blockers.append("Kafka 未启动或不可达")
        return

    for t in TOPICS:
        if t in topics:
            print(f"  [OK]   {t}")
        else:
            print(f"  [MISS] {t}")
            if t != "music_events":
                blockers.append(f"topic {t} 不存在，先跑 00_init_topics.sh")
            else:
                blockers.append("topic music_events 不存在，生成器还没发过数据")


def check_doris():
    hr("Doris 表")
    try:
        import pymysql
    except ImportError:
        print("  [SKIP] 未安装 pymysql")
        return

    try:
        conn = pymysql.connect(host=DORIS_HOST, port=DORIS_PORT, user="root", password="",
                               database="music_dw", charset="utf8mb4", connect_timeout=5)
        cur = conn.cursor()
    except Exception as e:
        print(f"  [DOWN] 无法连接 Doris: {e}")
        blockers.append("Doris FE 不可达")
        return

    cur.execute("SHOW TABLES")
    existing = {r[0] for r in cur.fetchall()}

    missing = []
    for layer, tables in TABLES.items():
        for t in tables:
            mark = "OK  " if t in existing else "MISS"
            if t not in existing:
                missing.append(t)
            print(f"  [{mark}] {layer:4s} {t}")

    if missing:
        blockers.append(f"Doris 缺 {len(missing)} 张表: {missing}")
        conn.close()
        return

    # 数据量
    #
    # 说明：ODS 和 DWD 都是 DUPLICATE 模型（保留原始上报，不做合并），
    # 上游埋点有约 0.6% 的「客户端重报」（生成器按 RATES["duplicate"]=0.006 注入），
    # 所以两边行数天然大于唯一 event_id 数。不能用 "DWD + 异常 == ODS 行数"
    # 这种减法对账，改用 event_id 反连接：只要 ODS 里 30 秒前落地的事件都能在
    # DWD/异常表里找到，就说明没有规则性丢数。
    #
    # 注意：早期版本这里写的是「A 的 checkpoint 重放导致重复、DWD 是 UNIQUE 表会
    # 合并」——两处都不对。重放重复已由 A 的 Kafka 两阶段提交消除；DWD 始终是
    # DUPLICATE。重复的稳定来源是注入脏数据，不是系统缺陷。
    hr("数据量与分层对账")
    # v10：ODS = Kafka topic，用 offset 对账，不再查 Doris 表。
    try:
        from kafka_ods import offsets, window_events, ODS_GROUP
        ods_end, ods_consumed, ods_lag = offsets(group=ODS_GROUP)
    except Exception as e:
        print(f"  [WARN] Kafka ODS offset 读取失败：{e}")
        ods_end = ods_consumed = ods_lag = None

    dwd_tables = [t for t in TABLES["DWD"] if t != "dwd_quality_error"]
    dwd = 0
    for t in dwd_tables:
        cur.execute(f"SELECT COUNT(*) FROM {t}")
        dwd += cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM dwd_quality_error")
    err = cur.fetchone()[0]

    if ods_end is not None:
        print(f"  ODS(Kafka topic ods_music_event) 现有 {ods_end} 条，"
              f"job_b 已消费 {ods_consumed} 条，LAG {ods_lag}")
        print("  保留期 3 天，实时侧不分析历史；持久原始层是离线 HDFS ODS(ORC)")
    print(f"  {len(dwd_tables)} 张明细历史累计 {dwd}    异常 {err}    "
          f"合计 {dwd + err}")

    # ⚠️ 不能用「累计 offset vs 表历史总量」做恒等式：
    #   topic 保留 3 天且被重建过（最早 offset 从 0 起），而 DWD 表保留全部历史，
    #   两者基数不同源，相减会得到 -36 万这种毫无意义的差值（v10 首版就踩了）。
    # 也不按时间窗做数量对账：灌数节奏一变（批量灌 vs 静默）两边窗口就不对齐。
    # 真正稳的验证是「按 event_id 抽查」：进了 ODS 的消息，是否都在 DWD 并集里。
    try:
        from kafka_ods import tail_events
        evs = tail_events(800)
        ids = [e.get("event_id") for e in evs if e.get("event_id")]
        uniq = list(dict.fromkeys(ids))
        if uniq:
            hit = 0
            for i in range(0, len(uniq), 200):
                chunk = uniq[i:i + 200]
                ph = ",".join(["%s"] * len(chunk))
                union = " UNION ALL ".join(
                    f"SELECT event_id FROM {t}" for t in dwd_tables
                    + ["dwd_quality_error", "dwd_unrouted"])
                cur.execute(f"SELECT COUNT(DISTINCT event_id) FROM ({union}) u "
                            f"WHERE event_id IN ({ph})", chunk)
                hit += cur.fetchone()[0]
            pct = hit * 100.0 / len(uniq)
            flag = "OK  " if pct >= 98 else "WARN"
            print(f"  [{flag}] 抽样对账：ODS topic 最近 {len(evs)} 条（去重 {len(uniq)} 个 event_id），"
                  f"在 DWD 并集命中 {hit}（{pct:.1f}%）")
            if pct < 98:
                print("         未被命中部分通常在途（LAG）或迟到丢弃，"
                      "持续偏低要查 job_b 规则")
        else:
            print("  ODS topic 最近无消息，跳过抽样对账")
    except Exception as e:
        print(f"  [WARN] 抽样对账跳过：{e}")

    # Flink 的 offset 提交发生在 checkpoint 边界，每个分区可能滞后 1~2 条；
    # LAG <= 分区数 × 2 视为追平，不再对 3 条报 WARN。
    if ods_lag is not None:
        tol = max(10, 3 * 2)
        print(f"  [OK  ] job_b 已追平 ODS topic，无积压" if ods_lag <= tol
              else f"  [WARN] job_b 仍有 {ods_lag} 条积压")

    # 抽样复核：近 60 秒窗口的 event_id 是否都能在 DWD 并集里找到
    # （替代原来「ODS 表 LEFT JOIN DWD」的反连接，ODS 表砍掉后只能这么验）
    try:
        from kafka_ods import window_events
        evs = window_events(60, max_messages=50000)
        ids = list({e.get("event_id") for e in evs if e.get("event_id")})
        if ids:
            sample = ids[:200]
            ph = ",".join(["%s"] * len(sample))
            union = " UNION ALL ".join(f"SELECT event_id FROM {t}"
                                       for t in dwd_tables + ["dwd_quality_error"])
            cur.execute(f"SELECT COUNT(DISTINCT event_id) FROM ({union}) u "
                        f"WHERE event_id IN ({ph})", sample)
            hit = cur.fetchone()[0]
            print(f"  窗口抽样：近 60s {len(ids)} 个 event_id，抽 {len(sample)} 个，"
                  f"在 DWD 命中 {hit}")
    except Exception as e:
        print(f"  [WARN] 窗口抽样跳过：{e}")

    for t in TABLES["DWS"]:
        cur.execute(f"SELECT COUNT(*) FROM {t}")
        print(f"  {t}: {cur.fetchone()[0]} 行")

    # ADS 新鲜度：作业 D 在跑的话 updated_at 应该一直在刷新（30 秒一个 CUMULATE 步长）
    cur.execute("SELECT MAX(updated_at), "
                "TIMESTAMPDIFF(SECOND, MAX(updated_at), NOW()) FROM ads_realtime_overview")
    row = cur.fetchone()
    if row and row[0]:
        age = row[1]
        stale = age is not None and age > 120
        print(f"  [{'WARN' if stale else 'OK  '}] ads_realtime_overview 最后更新 "
              f"{row[0]}（{age} 秒前）")
        if stale:
            # 注意：这张表不是 Flink 直写，而是 collect_ops_metrics.py 每分钟
            # 从 dwd_play_end 重算日累计（重启可回溯，不受流状态影响）。
            # 所以「没刷新」要查采集脚本，不要去看作业 D。
            print("         超过 2 分钟没刷新：该表由 collect_ops_metrics.py 微批写入，"
                  "请确认采集进程在跑")
    else:
        print("  ads_realtime_overview 还是空的（collect_ops_metrics.py 尚未跑过）")

    conn.close()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default=DEFAULT_HOST, help="Flink JobManager 地址")
    args = ap.parse_args()

    check_flink(args.host)
    check_kafka()
    check_doris()

    hr("结论")
    if blockers:
        for i, b in enumerate(blockers, 1):
            print(f"  {i}. {b}")
        print("\n有阻塞项，先解决再提交作业。")
        return 1

    alive = [j for j in EXPECTED_JOBS if j in running_names]
    if len(alive) == len(EXPECTED_JOBS):
        print("  四个作业都在运行，链路在线；不要再提交，重复提交会让数据翻倍。")
        print("  看大屏：python dashboard/app.py --port 5000")
        return 0

    if alive:
        missing = [j for j in EXPECTED_JOBS if j not in running_names]
        print(f"  已运行 {len(alive)} 个：{', '.join(alive)}")
        print(f"  未运行 {len(missing)} 个：{', '.join(missing)}")
        print("  补提缺的那个：python code/redeploy_realtime.py --only a|b|c|d")
        return 0

    print("  环境就绪，可以提交：bash sql/realtime/run_jobs.sh")
    return 0


if __name__ == "__main__":
    sys.exit(main())
