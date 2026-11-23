#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""实时链路运维指标采集：每分钟把「延迟 / 作业健康 / 数据质量」写进 Doris。

为什么需要它：
    四个作业跑起来之后，「链路是否健康」此前只能靠人工连 Doris 查 MAX(etl_time)。
    作业半夜挂了、延迟悄悄涨到几分钟、Kafka 侧积压——都不会有任何提示。
    这个脚本把判断依据固化成三张 ops_ 表，大屏和巡检都直接读表，不各算各的。

采集内容：
    ops_pipeline_latency  各层落地延迟、入口速率、DWD 增量
    ops_job_health        每个作业的状态、运行时长、checkpoint 大小与新鲜度
    ops_data_quality      每分钟事件量、异常率、命中最多的规则

用法：
    python code/collect_ops_metrics.py              # 循环，默认 60 秒一轮
    python code/collect_ops_metrics.py --once       # 只跑一轮（验证用）
    python code/collect_ops_metrics.py --interval 30

设计约束：
    1. 三张表都是 UNIQUE KEY，同一分钟重复采集只会覆盖，不会追加 —— 脚本可以随便重跑。
    2. Flink REST 不可达时写 NULL 而不是让脚本崩：链路挂了正是最需要留下记录的时候。
    3. 某层完全没有数据时延迟写 NULL，不写 -1 或 0 —— 0 秒和「没数据」是两件事。
"""
import argparse
import json
import sys
import time
import traceback
import urllib.request
import urllib.parse
from datetime import datetime, timedelta

import pymysql

# v10：ODS 改为 Kafka topic，窗口/位点类读数走共享读取层
try:
    from kafka_ods import offsets, window_events, session_stats, ODS_GROUP
except ImportError:                       # 允许脱离 code/ 目录单独跑
    import os, sys
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    from kafka_ods import offsets, window_events, session_stats, ODS_GROUP

DORIS = dict(host="192.168.128.163", port=9030, user="root", password="",
             database="music_dw", charset="utf8mb4")
FLINK = "http://192.168.128.161:8081"

# 七张明细表：算「近 1 分钟进 DWD 多少条」用
DWD_TABLES = ["dwd_play_end", "dwd_play_start", "dwd_skip", "dwd_search",
              "dwd_collect", "dwd_share", "dwd_page_view",
              # 商业化与客户端质量明细：计入「近 1 分钟 DWD 新增」，
              # 否则入口速率会漏掉这部分事件，吞吐指标系统性偏低。
              "dwd_order", "dwd_play_error", "dwd_unrouted"]

EXPECTED_JOBS = ["rt_job_a_ods", "rt_job_b_dwd", "rt_job_c_dws", "rt_job_d_ads"]


def now_minute():
    """截断到分钟，作为三张表的主键。"""
    return datetime.now().replace(second=0, microsecond=0)


def fetch_json(path, timeout=6):
    """访问 Flink REST。失败返回 None，由调用方决定写 NULL 还是跳过。"""
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    try:
        with opener.open(FLINK + path, timeout=timeout) as r:
            return json.loads(r.read().decode("utf-8"))
    except Exception:
        return None


def collect_kafka_lag():
    """Kafka source 的真实积压行数：各分区 (末端 offset - 已提交 offset) 之和。

    为什么必须有这个指标：dwd_lag = now - max(etl_time)，它的语义是「最新一行数据多旧」。
    只要 B 还在写（哪怕写的是几分钟前的旧数据），max(etl_time) 就一直被刷新，
    dwd_lag 永远显示「健康」。实测 B 积压 6 万行时，dwd_lag 全程 11~15s。
    真正的积压只能看 consumer lag。

    实现选择：不用 Flink REST 的 records-lag-max —— 实测该 gauge 恒为 NaN
    （subtasks 聚合接口对 gauge 只返回 min/max/avg，按 subtask 取 value 也是 NaN）。
    改用 kafka-python 直接算：committed() 走 OffsetFetch、end_offsets() 走 ListOffsets，
    **不 subscribe、不 poll**，因此不会加入消费组，也就不会触发 rebalance 干扰 Flink。
    """
    try:
        from kafka import KafkaConsumer
        from kafka.structs import TopicPartition
    except ImportError:
        return None
    topic, group = "ods_music_event", "rt_job_b_dwd_r3"
    c = None
    try:
        c = KafkaConsumer(group_id=group, bootstrap_servers=["192.168.128.161:9092"],
                          enable_auto_commit=False, consumer_timeout_ms=2000,
                          # 踩坑：request_timeout_ms 必须 > session_timeout_ms（默认 10000），
                          # 原来写 5000 会直接抛 KafkaConfigurationError，被下面 except 吞成 None，
                          # 表现为「Kafka 积压 None」而不是报错。
                          session_timeout_ms=6000, request_timeout_ms=15000)
        parts = [TopicPartition(topic, p) for p in (c.partitions_for_topic(topic) or [])]
        if not parts:
            return None
        # 注意：kafka-python 2.x 的 committed() 只接受单个 TopicPartition，
        # 传列表会抛 TypeError('partition must be a TopicPartition namedtuple')。
        committed = {tp: c.committed(tp) for tp in parts}
        end = c.end_offsets(parts)
        total = 0
        for tp in parts:
            begin = committed.get(tp) or 0
            total += max(0, end.get(tp, 0) - begin)
        return total
    except Exception:
        return None
    finally:
        if c is not None:
            try:
                c.close(autocommit=False, timeout_ms=1000)
            except Exception:
                pass


def collect_latency(cur, minute):
    """各层延迟与吞吐。"""
    # v10：ODS 是 Kafka topic，没有 etl_time 列。
    # 用 job_b 最早落地的一张明细表的入仓时间，作为「ODS 已被消费」的时间代理；
    # topic 积压条数仍由 collect_kafka_lag() 单独写入 kafka_lag_rows。
    cur.execute("SELECT MAX(etl_time) FROM dwd_play_end")
    dwd_max = cur.fetchone()[0]
    ods_max = dwd_max

    # DWS 1 分钟窗口：窗口结束后才算产出，所以滞后 = now - (窗口起点 + 60s)
    cur.execute("SELECT MAX(window_start) FROM dws_play_overview_1min")
    dws_max = cur.fetchone()[0]

    # ADS 榜单 10 分钟窗口
    cur.execute("SELECT MAX(window_start) FROM ads_song_rank_10min")
    ads_max = cur.fetchone()[0]

    def lag(ts, offset_s=0):
        if ts is None:
            return None
        return max(0, int((datetime.now() - ts).total_seconds()) - offset_s)

    # 近 1 分钟 DWD 增量（七表合计）
    parts = [f"SELECT COUNT(*) FROM {t} WHERE etl_time >= NOW() - INTERVAL 60 SECOND"
             for t in DWD_TABLES]
    cur.execute(" UNION ALL ".join(parts))
    dwd_1m = sum(r[0] for r in cur.fetchall())

    row = (minute,
           lag(ods_max),
           lag(dwd_max),
           lag(dws_max, 60),
           lag(ads_max, 600),
           round(dwd_1m / 60.0, 2),
           dwd_1m,
           collect_kafka_lag(),
           datetime.now())
    cur.execute("""INSERT INTO ops_pipeline_latency
                   (stat_minute, ods_lag_s, dwd_lag_s, dws_lag_s, ads_lag_s,
                    input_rate, dwd_rows_1m, kafka_lag_rows, updated_at)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)""", row)
    return dict(zip(["ods_lag_s", "dwd_lag_s", "dws_lag_s", "ads_lag_s",
                     "input_rate", "dwd_rows_1m", "kafka_lag_rows"], row[1:8]))


def collect_jobs(cur, minute):
    """作业状态 + checkpoint 健康度。"""
    ov = fetch_json("/jobs/overview")
    if ov is None:
        # Flink 不可达：仍然留一行，state 写 DOWN，让大屏能看出来
        for name in EXPECTED_JOBS:
            cur.execute("""INSERT INTO ops_job_health
                           (stat_minute, job_name, job_id, state, updated_at)
                           VALUES (%s,%s,%s,%s,%s)""",
                        (minute, name, None, "DOWN", datetime.now()))
        return {"flink": "DOWN"}

    running = {j.get("name"): j for j in ov.get("jobs", [])
               if j.get("state") == "RUNNING"}
    written = 0
    for name in EXPECTED_JOBS:
        j = running.get(name)
        if j is None:
            cur.execute("""INSERT INTO ops_job_health
                           (stat_minute, job_name, job_id, state, updated_at)
                           VALUES (%s,%s,%s,%s,%s)""",
                        (minute, name, None, "MISSING", datetime.now()))
            written += 1
            continue

        jid = j["jid"]
        run_s = None
        if j.get("start-time", -1) > 0:
            run_s = int((time.time() * 1000 - j["start-time"]) / 1000)

        # checkpoint 详情：失败时静默降级为 NULL
        size_mb = age_s = failed = None
        ck = fetch_json(f"/jobs/{jid}/checkpoints")
        if ck:
            counts = ck.get("counts") or {}
            failed = counts.get("failed")
            latest = ((ck.get("latest") or {}).get("completed") or {})
            if latest:
                # checkpointed_size 含增量与元数据，比 state_size 更接近真实落盘量
                size = latest.get("checkpointed_size") or latest.get("state_size")
                if size:
                    size_mb = round(size / 1024.0 / 1024.0, 2)
                ts = latest.get("trigger_timestamp")
                if ts:
                    age_s = int(time.time() - ts / 1000.0)

        cur.execute("""INSERT INTO ops_job_health
                       (stat_minute, job_name, job_id, state, run_seconds,
                        ckpt_size_mb, ckpt_age_s, ckpt_failed, updated_at)
                       VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                    (minute, name, jid, "RUNNING", run_s,
                     size_mb, age_s, failed, datetime.now()))
        written += 1
    return {"flink": "OK", "running": len(running), "rows": written}


def collect_quality(cur, minute):
    """近 1 分钟事件量、异常率、命中最多的规则。"""
    parts = [f"SELECT COUNT(*) FROM {t} WHERE etl_time >= NOW() - INTERVAL 60 SECOND"
             for t in DWD_TABLES]
    cur.execute(" UNION ALL ".join(parts))
    detail = sum(r[0] for r in cur.fetchall())

    cur.execute("""SELECT COUNT(*) FROM dwd_quality_error
                   WHERE etl_time >= NOW() - INTERVAL 60 SECOND""")
    err = cur.fetchone()[0]

    cur.execute("""SELECT rule_name, COUNT(*) c FROM dwd_quality_error
                   WHERE etl_time >= NOW() - INTERVAL 60 SECOND
                   GROUP BY rule_name ORDER BY c DESC LIMIT 1""")
    top = cur.fetchone()

    # 分母要把被拦下的异常也算进去，否则「异常率」会永远偏小
    total = detail + err
    rate = round(err * 100.0 / total, 2) if total else None

    cur.execute("""INSERT INTO ops_data_quality
                   (stat_minute, events_1m, errors_1m, error_rate,
                    top_rule, top_rule_cnt, updated_at)
                   VALUES (%s,%s,%s,%s,%s,%s,%s)""",
                (minute, total, err, rate,
                 top[0] if top else None, top[1] if top else None, datetime.now()))
    return {"events_1m": total, "errors_1m": err, "error_rate": rate}


def refresh_overview(cur):
    """日累计 KPI：从明细表重算，1 分钟一次微批。

    为什么不让 Flink 用 CUMULATE(1d) 算：
        日级累计是「必须单调递增」的 KPI，而流状态在重启后无法回溯 —— 作业一重启
        状态就从 0 开始，只能靠重放整条 Kafka topic 来重建（12 万条要 8~10 分钟，
        重放期间 ADS 严重滞后）。明细表则永远可以回溯重算，结果必然正确。
        所以判断是：日累计依赖明细，不依赖流状态；秒级指标才走流。

    代价与取舍：
        这条语句每分钟扫一次当天分区（当前几万行，毫秒级）。
        数据量真的上到千万级日增时，应该改成读 DWS 的 1 分钟表做增量累加，
        但那时 UV 就不能相加了（去重不可加），得靠 Doris 的 BITMAP。
    """
    cur.execute("""
        INSERT INTO ads_realtime_overview
        SELECT CURDATE(),
               COUNT(DISTINCT event_id),
               COUNT(DISTINCT device_id),
               COUNT(DISTINCT CASE WHEN COALESCE(user_id, '') <> '' THEN user_id END),
               ROUND(COUNT(DISTINCT CASE WHEN end_reason = 'natural' THEN event_id END)
                     * 100.0 / NULLIF(COUNT(DISTINCT event_id), 0), 2),
               ROUND(COUNT(DISTINCT CASE WHEN end_reason = 'skip' THEN event_id END)
                     * 100.0 / NULLIF(COUNT(DISTINCT event_id), 0), 2),
               ROUND(AVG(CAST(play_duration AS DOUBLE)), 1),
               COUNT(DISTINCT DATE_FORMAT(event_ts, '%Y-%m-%d %H:%i')),
               MAX(event_ts),
               NOW()
        FROM (
            -- 播放终止流 = play_end ∪ skip（口径同 job_c_dws / doris_ads_aggregate）
            -- 一次起播只有两种结局：正常结束或切歌。只数 dwd_play_end 会让
            -- skip_rate 恒为 0、完播率被抬成 73%（应为 51% 左右）。
            SELECT event_id, device_id, user_id, end_reason, play_duration, event_ts
            FROM dwd_play_end WHERE dt = CURDATE()
            UNION ALL
            SELECT event_id, device_id, user_id, 'skip' AS end_reason,
                   played_duration AS play_duration, event_ts
            FROM dwd_skip WHERE dt = CURDATE()
        ) t
    """)
    return cur.rowcount


def refresh_commercial(cur):
    """商业化日累计：从 dwd_order 重算（微批，1 分钟一次）。

    商业化口径和行为数据最大的区别是「必须自洽」——五个指标互相能验证：
        收入   = Σ 已支付订单金额（订阅 + 单曲/专辑购买）
        退款   = Σ 退款金额
        净收入 = 收入 − 退款
        付费用户 = 当日有支付订单的去重 user_id
        付费率 = 付费用户 / 当日活跃用户
        ARPPU  = 净收入 / 付费用户
    因为订单金额取自 SKU 单价（生成器里写死），所以「收入」与
    「Σ 订单数 × 单价」必然一致 —— 这是能经得起追问的地方。
    """
    cur.execute("""
        INSERT INTO ads_commercial_daily
        SELECT CURDATE(), o.*, NOW() FROM (
            SELECT
               COUNT(DISTINCT CASE WHEN event_name IN ('vip_subscribe','order_pay') THEN order_id END),
               COUNT(DISTINCT CASE WHEN event_name IN ('vip_subscribe','order_pay') THEN user_id END),
               COALESCE((SELECT COUNT(DISTINCT user_id) FROM dwd_play_end
                          WHERE dt = CURDATE() AND COALESCE(user_id,'') <> ''), 0),
               ROUND(COUNT(DISTINCT CASE WHEN event_name IN ('vip_subscribe','order_pay') THEN user_id END) * 100.0
                     / NULLIF((SELECT COUNT(DISTINCT user_id) FROM dwd_play_end
                                WHERE dt = CURDATE() AND COALESCE(user_id,'') <> ''), 0), 2),
               ROUND(SUM(CASE WHEN event_name IN ('vip_subscribe','order_pay') THEN amount ELSE 0 END), 2),
               ROUND(SUM(CASE WHEN event_name = 'order_refund' THEN amount ELSE 0 END), 2),
               ROUND(SUM(CASE WHEN event_name IN ('vip_subscribe','order_pay') THEN amount
                              WHEN event_name = 'order_refund' THEN -amount ELSE 0 END), 2),
               COUNT(DISTINCT CASE WHEN event_name = 'order_refund' THEN order_id END),
               COUNT(DISTINCT CASE WHEN event_name = 'vip_subscribe' THEN order_id END),
               COUNT(DISTINCT CASE WHEN event_name = 'order_pay' THEN order_id END),
               ROUND(SUM(CASE WHEN event_name IN ('vip_subscribe','order_pay') THEN amount
                              WHEN event_name = 'order_refund' THEN -amount ELSE 0 END)
                     / NULLIF(COUNT(DISTINCT CASE WHEN event_name IN ('vip_subscribe','order_pay')
                                                 THEN user_id END), 0), 2)
            FROM (
                -- 先按订单号去重再聚合：
                -- 埋点会有客户端重报（同一笔订单被上报两次，event_id 不同、order_id 相同），
                -- 直接 SUM(amount) 会把收入算多。实测 289 笔订单里就有 1 笔重复，
                -- 导致季卡收入多出 ¥40 —— 商业化指标必须先按业务主键去重，这不是洁癖问题。
                SELECT order_id, user_id, event_name, sku_id, MAX(amount) AS amount
                FROM dwd_order
                WHERE dt = CURDATE()
                GROUP BY order_id, user_id, event_name, sku_id
            ) o1
        ) o
    """)
    # SKU 构成单独一张表：一个 SKU 一行，大屏可以画条形图
    cur.execute("""
        INSERT INTO ads_sku_daily
        SELECT CURDATE(), sku_id, COUNT(*) AS order_cnt, ROUND(SUM(amount), 2), NOW()
        FROM (
            SELECT order_id, sku_id, MAX(amount) AS amount
            FROM dwd_order
            WHERE dt = CURDATE() AND event_name IN ('vip_subscribe','order_pay')
            GROUP BY order_id, sku_id
        ) t
        GROUP BY sku_id
    """)
    return True


def refresh_quality(cur):
    """客户端播放质量（分钟级）：播放失败率 + 最高频错误码。

    口径：失败率 = 失败数 / (播放开始数 + 失败数)。
    分母用「播放开始」而不是「播放结束」——失败发生在开始阶段，
    用 play_end 做分母会把还没播完的请求排除掉，失败率被系统性高估。
    """
    # 踩坑：原来把「取最高频错误码」写成 (SELECT error_code ... GROUP BY ... ORDER BY ... LIMIT 1)
    # 直接放在 SELECT 列表里，Doris 会按标量子查询执行并报
    # 1105 Expected EQ 1 to be returned by expression (SCALARSUBQUERY)。
    # 改成派生表 + ROW_NUMBER() 取 top1，再 CROSS JOIN 汇总行，语义不变且只扫一遍。
    cur.execute("""
        INSERT INTO ads_client_quality_1min
        SELECT CAST(DATE_FORMAT(NOW(), '%Y-%m-%d %H:%i:00') AS DATETIME),
               s.cnt,
               e.cnt,
               ROUND(e.cnt * 100.0 / NULLIF(s.cnt + e.cnt, 0), 2),
               e.top_code,
               COALESCE(e.top_cnt, 0),
               NOW()
        FROM (SELECT COUNT(*) AS cnt FROM dwd_play_start
               WHERE dt = CURDATE() AND event_ts >= NOW() - INTERVAL 1 MINUTE) s
        CROSS JOIN (
            SELECT COUNT(*) AS cnt,
                   MAX(CASE WHEN rn = 1 THEN error_code END) AS top_code,
                   MAX(CASE WHEN rn = 1 THEN c END)          AS top_cnt
            FROM (
                SELECT error_code, COUNT(*) AS c,
                       ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC, error_code) AS rn
                FROM dwd_play_error
                WHERE dt = CURDATE() AND event_ts >= NOW() - INTERVAL 1 MINUTE
                GROUP BY error_code
            ) r
        ) e
    """)
    return True


def refresh_session(cur):
    """会话指标（近 10 分钟滚动）。

    跳出率 = 只有 1 条事件的会话 / 总会话数。
    「跳出」指用户打开 App 只产生一条行为就走了，是衡量首屏粘性的关键指标；
    真实音乐 App 的跳出率通常 20%~40%（很多人点开听一首就退出）。

    为什么用 ODS 而不是 DWD（v10 后改从 Kafka topic 取窗口）：
      session_id 是原始流里就有的字段，会话归并不依赖清洗；
      DWD 已按事件类型分流到多张表，归并会话反而要 UNION。
      v10 砍掉 Doris ODS 表后，直接从 `ods_music_event` topic 读近 10 分钟窗口，
      语义完全等价，还少一层落表。
    为什么是「近 10 分钟」而非「今日」：
      大屏是实时视图，今日累计已有 KPI 卡；且会话是滚动概念，
      今日累计里混着旧代码（每条事件一个 session）的脏数据，用近期窗口才干净。
    """
    # v10：数据源从 Doris ODS 表换成 Kafka topic 的近 10 分钟窗口样本。
    # 会话归并本来就依赖「未分流的原始流」，Kafka-as-ODS 反而更贴合语义。
    try:
        evs = window_events(600)
        n, avg_c, bounce, bounce_rate = session_stats(evs)
        minute_ts = datetime.now().replace(second=0, microsecond=0)
        cur.execute("""INSERT INTO ads_session_1min
                       (stat_minute, session_cnt, avg_evts_per_sess,
                        bounce_sess_cnt, bounce_rate, updated_at)
                       VALUES (%s,%s,%s,%s,%s,NOW())""",
                    (minute_ts, n, avg_c, bounce, bounce_rate))
        return True
    except Exception as e:
        print(f"  [WARN] 会话指标采集失败（Kafka ODS 窗口）：{e}")
        return False


def refresh_dws_tail(cur, lo_override=None):
    """尾窗回补：把 Flink 还没闭合的最后几个 DWS 窗口，用明细重算一遍。

    【为什么要补】
        TUMBLE 窗口只在 watermark 越过 window_end 时才输出，而 watermark =
        max(event_ts) − 2 分钟。于是「最近 2 分钟的 1 分钟窗口」「最近 7 分钟的
        5 分钟窗口」永远是空的；数据一旦停流，这些窗口就永久不闭合。
        实测（2026-11-08）：5 分钟单曲表 18,209 条 / 明细 19,602 条 = 92.9%，
        缺口 1,393 条全部落在尾部未闭合窗口；已闭合窗口逐窗对账差 0
        （11:00 / 11:05 / 12:20 / 12:25 / 12:30 全部精确相等）。
        所以「覆盖率偏低」不是随机丢数，是尾窗没收敛。

    【怎么做】
        每分钟用 SQL 从 dwd_play_end ∪ dwd_skip 重算最近几个窗口，照抄 Flink 的
        分组口径（GROUP BY window_start, song_id，COUNT DISTINCT event_id，
        左闭右开 [w, w+5min)），以 UNIQUE KEY 覆盖写回。
        · Flink 之后真闭窗时会写入同样值的窗口 → 多覆盖一次，结果相同，无害；
        · 数据停流时 Flink 不再 emit → 这里给出「截至最后一刻」的值，不再永远空着。

    【回补范围：只补未闭合的窗口】
        1 分钟窗口在 watermark 越过 w+1 时闭窗，5 分钟窗口在越过 w+5 时闭窗，
        而 watermark = anchor − 2 分钟。所以「未闭合」= w > anchor − 2 − len：
        1 分钟只剩 2~3 个窗口，5 分钟只剩 1 个。
        早先的写法是「最近 6 / 15 分钟照单全收」，会把已闭合窗口也重算一遍，
        用明细表当前快照覆盖 Flink 算准的值 —— 在途行还没落表时就会写小，
        详见上面 WM_LAG_MIN 那段注释。

    【一个已知的口径差，别当成 bug】
        dwd_skip 没有 play_progress 列（只有 played_duration），而 Flink 侧 skip 的
        进度是 played_duration/duration 现算的，这里没有 duration 可除，只能给 NULL。
        于是 avg_play_progress 的回补值与 Flink 值略有出入。这一列不上大屏、也不参与
        任何比率计算，Flink 闭窗后会被准确值覆盖，因此不额外造分母硬凑。
    """
    # 5 分钟窗口对齐用的表达式：先取秒级时间戳，向下取整到 300 秒，再转回 DATETIME。
    # Doris 的 date_trunc 不支持 5 分钟粒度，只能这样算。
    W300 = ("CAST(FROM_UNIXTIME(FLOOR(UNIX_TIMESTAMP(event_ts) / 300) * 300) "
            "AS DATETIME)")
    W60 = ("CAST(FROM_UNIXTIME(FLOOR(UNIX_TIMESTAMP(event_ts) / 60) * 60) "
           "AS DATETIME)")

    # 播放终止流 = play_end ∪ skip，与 job_c_dws / doris_ads_aggregate 同口径。
    # skip 行的 end_reason 补成 'skip'、play_progress 为 NULL（见 docstring 末段）。
    def term_union(w_expr, extra_cols):
        return f"""
            SELECT event_id, device_id, user_id, song_id, end_reason,
                   play_duration, play_progress, province,
                   {w_expr} AS w, event_ts {extra_cols}
            FROM dwd_play_end WHERE dt = CURDATE()
            UNION ALL
            SELECT event_id, device_id, user_id, song_id, 'skip' AS end_reason,
                   played_duration AS play_duration, NULL AS play_progress, province,
                   {w_expr} AS w, event_ts {extra_cols}
            FROM dwd_skip WHERE dt = CURDATE()
        """

    # 回补的锚点必须是「明细里最新的事件时间」，不能用 NOW()。
    # 用 NOW() 的话，数据一旦停流超过回补范围，尾窗就又永远补不到了 ——
    # 而「数据停流导致尾窗永空」正是这个函数要解决的问题本身。
    # 锚定 max(event_ts) 后：数据在流时补最近几分钟，停流后补最后几分钟，
    # 两种情况下尾窗都有值；当天没有数据时直接跳过（子查询返回 NULL）。
    cur.execute("""
        SELECT MAX(mx) FROM (
            SELECT MAX(event_ts) AS mx FROM dwd_play_end WHERE dt = CURDATE()
            UNION ALL
            SELECT MAX(event_ts) AS mx FROM dwd_skip     WHERE dt = CURDATE()
        ) t
    """)
    row = cur.fetchone()
    anchor = row[0] if row else None
    if anchor is None:
        return 0

    # 下限必须对齐到窗口边界，不能直接用 anchor − N 分钟。
    # 反例（实测踩到）：anchor=13:23、5 分钟表取 15 分钟 → 下限落到 13:08，
    # 把 13:05~13:10 这个窗口从中间切断，只数到 13:08~13:10 的 377 条，
    # 覆盖掉 Flink 已算好的 451 条 —— 补尾窗反而把已闭合的好窗口写坏了。
    #
    # 【更根本的问题：只能补「未闭合」窗口】
    # 上面只是把下限对齐了边界，范围仍是「最近 6 / 15 分钟」——
    # 把**已经闭合**的窗口也重算了一遍。闭合窗口的正确值本来就由 Flink 给出，
    # 我们再拿明细快照覆盖一次，用的是「此刻表里有什么」而不是「窗口里最终有什么」：
    # job_b 写明细 → job_c 读明细之间有秒级延迟，在途行还没落表，
    # 重算值就会偏小。写入又是 UNIQUE KEY 覆盖，谁后写谁赢 ——
    # 只要 Flink 先闭窗、我们后覆盖，偏小的值就留在表里，且永久不会自愈
    # （Flink 不会为一个已闭合窗口再 emit 一次）。
    #
    # 正确做法：**回补只碰 Flink 永远不会 emit 的窗口**。
    # 窗口 [w, w+len) 在 watermark >= w+len 时闭窗，watermark = anchor − 2 分钟，
    # 所以已闭合 ⇔ w + len <= anchor − 2，未闭合 ⇔ w > anchor − 2 − len。
    # 取「严格大于 anchor−2−len 的最小 len 边界」作为起点：
    # 既不覆盖 Flink 算准的值，又恰好覆盖那批永远空着的尾窗。
    # 数据停流时 anchor 不再前进，这几个窗口就由这里给出「截至最后一刻」的值。
    #
    # 【对账提示：明细要按 event_id 去重，别用 COUNT(*)】
    # 造数器会按 0.6% 的概率故意发重复 event_id（模拟客户端重传），
    # 明细表是 DUPLICATE KEY，重复行会并存；而 DWS 一律 COUNT(DISTINCT event_id)。
    # 实测 14:20 窗口 play_end 515 行 / 510 个去重 ID，skip 193 行 / 192 个，
    # 合计 708 行 vs 702 条 —— 差的那 6 条不是丢数，是重复消息。
    # 直接用 COUNT(*) 对账会看到「DWS 只有明细的 99.2%」这种假缺口。
    WM_LAG_MIN = 2          # 与 job_c_dws.sql 的 watermark 延迟保持一致

    def _open_win_start(anchor_ts, minutes):
        """Flink 还没闭合的最小窗口起点（严格大于 anchor − WM_LAG − len 的 len 边界）。"""
        t = anchor_ts - timedelta(minutes=WM_LAG_MIN + minutes)
        base = t.replace(second=0, microsecond=0)
        rem = base.minute % minutes
        # rem == 0 说明 base 正好压在边界上，而「已闭合」是 <=，所以还得再加一格
        return base + timedelta(minutes=(minutes - rem) if rem else minutes)

    lo1 = _open_win_start(anchor, 1)
    lo5 = _open_win_start(anchor, 5)

    # ── 回补范围：把「未闭合窗口」和「迟到修正」合成一段 ──────────────────
    # A 段（必须保留）：Flink 永远不 emit 的未闭合窗口。anchor 静止（停流）时
    #   靠它给出「截至最后一刻」的值，否则尾窗永远空着。
    # B 段（新增）：最近 LOOKBACK_MIN 分钟内的**已闭合**窗口，用来吸收迟到数据。
    #
    # 【B 段为什么"只增不减"就安全 —— 这是不牺牲实时性的关键】
    # 迟到数据（分批灌数时后一批的时间戳更早）只会让窗口计数变大；
    # 而这里读明细时可能有在途行没落表（job_b 写 → job_c 读有秒级延迟），
    # 重算值可能偏小。UNIQUE KEY 是覆盖写、谁后写谁赢，一旦写小就永久留下
    # （Flink 不会为已闭合窗口再 emit）。所以判据定成：
    #     重算值 > DWS 现值  → 写（迟到数据，必须补）
    #     重算值 <= DWS 现值 → 跳过（要么一致，要么快照偏小 / 明细被清理过）
    # 这样迟到数据在一个采集周期内自动收敛，**不需要调大 watermark**，
    # 首屏延迟一点没变。也就不再需要事后人工 --repair-from。
    #
    # 【对账提示】明细必须 COUNT(DISTINCT event_id)：
    # 造数器按 0.6% 故意发重复 event_id（模拟重传），明细是 DUPLICATE KEY 会并存。
    LOOKBACK_MIN = 45          # > 会话跨度 25 分钟 + 采集/写入延迟余量
    force = lo_override is not None

    def _floor_to(d, minutes):
        base = d.replace(second=0, microsecond=0)
        return base - timedelta(minutes=base.minute % minutes)

    lo1 = min(_open_win_start(anchor, 1),
              _floor_to(anchor - timedelta(minutes=LOOKBACK_MIN), 1))
    lo5 = min(_open_win_start(anchor, 5),
              _floor_to(anchor - timedelta(minutes=LOOKBACK_MIN), 5))

    # lo_override：人工修数入口（--repair-from），force=True 允许写小 ——
    # 明细被清理过的场景需要把 DWS 拉回真值。日常轮询永远 force=False。
    if lo_override is not None:
        lo1 = lo_override
        lo5 = _floor_to(lo_override, 5)

    updated = 0

    # 2.1 全局 1 分钟总览
    cur.execute(f"""
        SELECT CAST(w AS DATE),
               w,
               DATE_ADD(w, INTERVAL 1 MINUTE),
               COUNT(DISTINCT event_id),
               COUNT(DISTINCT CASE WHEN end_reason = 'natural' THEN event_id END),
               COUNT(DISTINCT CASE WHEN end_reason = 'skip'    THEN event_id END),
               SUM(CASE WHEN end_reason = 'pause'     THEN 1 ELSE 0 END),
               SUM(CASE WHEN end_reason = 'exit'      THEN 1 ELSE 0 END),
               SUM(CASE WHEN end_reason = 'interrupt' THEN 1 ELSE 0 END),
               ROUND(COUNT(DISTINCT CASE WHEN end_reason = 'natural' THEN event_id END)
                     * 100.0 / NULLIF(COUNT(DISTINCT event_id), 0), 2),
               ROUND(COUNT(DISTINCT CASE WHEN end_reason = 'skip' THEN event_id END)
                     * 100.0 / NULLIF(COUNT(DISTINCT event_id), 0), 2),
               ROUND(AVG(CAST(play_duration AS DOUBLE)), 1),
               ROUND(AVG(play_progress), 4),
               COUNT(DISTINCT device_id),
               COUNT(DISTINCT CASE WHEN COALESCE(user_id, '') <> '' THEN user_id END)
        FROM ({term_union(W60, "")}) t
        WHERE event_ts >= %s
        GROUP BY w
    """, (lo1,))
    fresh = cur.fetchall()
    if fresh:
        cur.execute("""SELECT window_start, play_cnt FROM dws_play_overview_1min
                       WHERE window_start >= %s""", (lo1,))
        old = {r[0]: (r[1] or 0) for r in cur.fetchall()}
        todo = [r for r in fresh if force or r[3] > old.get(r[1], 0)]
        if todo:
            cur.executemany("""
                INSERT INTO dws_play_overview_1min
                    (dt, window_start, window_end, play_cnt, finish_cnt, skip_cnt,
                     pause_cnt, exit_cnt, interrupt_cnt, finish_rate, skip_rate,
                     avg_play_duration, avg_play_progress, uv, login_uv)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """, todo)
        updated += len(todo)

    # 2.2 单曲 5 分钟
    cur.execute(f"""
        SELECT CAST(w AS DATE), w, song_id,
               COUNT(DISTINCT event_id),
               COUNT(DISTINCT CASE WHEN end_reason = 'natural' THEN event_id END),
               COUNT(DISTINCT CASE WHEN end_reason = 'skip'    THEN event_id END),
               ROUND(COUNT(DISTINCT CASE WHEN end_reason = 'skip' THEN event_id END)
                     * 100.0 / NULLIF(COUNT(DISTINCT event_id), 0), 2),
               ROUND(AVG(play_progress), 4),
               COUNT(DISTINCT device_id)
        FROM ({term_union(W300, "")}) t
        WHERE COALESCE(song_id, '') <> ''
          AND event_ts >= %s
        GROUP BY w, song_id
    """, (lo5,))
    fresh = cur.fetchall()
    if fresh:
        cur.execute("""SELECT window_start, song_id, play_cnt FROM dws_song_play_5min
                       WHERE window_start >= %s""", (lo5,))
        old = {(r[0], r[1]): (r[2] or 0) for r in cur.fetchall()}
        todo = [r for r in fresh if force or r[3] > old.get((r[1], r[2]), 0)]
        if todo:
            cur.executemany("""
                INSERT INTO dws_song_play_5min
                    (dt, window_start, song_id, play_cnt, finish_cnt, skip_cnt,
                     skip_rate, avg_play_progress, uv)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """, todo)
        updated += len(todo)

    # 2.3 地域 5 分钟
    cur.execute(f"""
        SELECT CAST(w AS DATE), w, province,
               COUNT(DISTINCT event_id),
               COUNT(DISTINCT CASE WHEN end_reason = 'natural' THEN event_id END),
               COUNT(DISTINCT CASE WHEN end_reason = 'skip'    THEN event_id END),
               ROUND(COUNT(DISTINCT CASE WHEN end_reason = 'skip' THEN event_id END)
                     * 100.0 / NULLIF(COUNT(DISTINCT event_id), 0), 2),
               COUNT(DISTINCT device_id)
        FROM ({term_union(W300, "")}) t
        WHERE COALESCE(province, '') <> ''
          AND event_ts >= %s
        GROUP BY w, province
    """, (lo5,))
    fresh = cur.fetchall()
    if fresh:
        cur.execute("""SELECT window_start, province, play_cnt FROM dws_province_play_5min
                       WHERE window_start >= %s""", (lo5,))
        old = {(r[0], r[1]): (r[2] or 0) for r in cur.fetchall()}
        todo = [r for r in fresh if force or r[3] > old.get((r[1], r[2]), 0)]
        if todo:
            cur.executemany("""
                INSERT INTO dws_province_play_5min
                    (dt, window_start, province, play_cnt, finish_cnt, skip_cnt,
                     skip_rate, uv)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
            """, todo)
        updated += len(todo)

    return updated


def one_round(verbose=True):
    minute = now_minute()
    conn = pymysql.connect(**DORIS)
    lat = jobs = qual = None
    try:
        cur = conn.cursor()
        # 每一步独立 try + commit：
        # Doris 在内存压力下会 cancel 正在跑的 load（MEM_LIMIT_EXCEEDED），
        # 之前四个步骤共用一个事务，任何一步失败就把整轮三张表的数据全丢了 ——
        # 163 内存吃紧那两分钟，大屏直接看不到任何新指标。指标采集必须"能写多少写多少"。
        for name, fn in (("latency", lambda: collect_latency(cur, minute)),
                         ("jobs",    lambda: collect_jobs(cur, minute)),
                         ("quality", lambda: collect_quality(cur, minute)),
                         ("overview", lambda: refresh_overview(cur)),
                         # 商业化与客户端质量：同样是「明细重算 + UNIQUE KEY 覆盖」，
                         # 与 overview 一致，重跑不会翻倍。
                         ("commercial", lambda: refresh_commercial(cur)),
                         ("client_quality", lambda: refresh_quality(cur)),
                         ("session", lambda: refresh_session(cur)),
                         # 尾窗回补：Flink 的 TUMBLE 要等 watermark 越过窗口末尾才输出，
                         # 最近 2 分钟（1min 窗）/ 7 分钟（5min 窗）永远是空的。
                         # 这里用明细重算补齐，避免「覆盖率 93%」被误读成丢数。
                         ("dws_tail", lambda: refresh_dws_tail(cur))):
            try:
                r = fn()
                conn.commit()
                if name == "latency":
                    lat = r
                elif name == "jobs":
                    jobs = r
                elif name == "quality":
                    qual = r
            except Exception as e:
                try:
                    conn.rollback()
                except Exception:
                    pass
                print(f"  [WARN] {name} 写入失败：{str(e)[:140]}")
    finally:
        conn.close()

    if verbose and lat:
        print(f"[{datetime.now():%H:%M:%S}] {minute:%Y-%m-%d %H:%M}")
        print(f"  延迟  ODS {lat['ods_lag_s']}s / DWD {lat['dwd_lag_s']}s / "
              f"DWS {lat['dws_lag_s']}s / ADS {lat['ads_lag_s']}s")
        print(f"  吞吐  {lat['input_rate']} 条/s（近 1 分钟 DWD {lat['dwd_rows_1m']} 行）"
              f"  Kafka 积压 {lat.get('kafka_lag_rows')} 行")
        if qual:
            print(f"  质量  事件 {qual['events_1m']}  异常 {qual['errors_1m']}  "
                  f"异常率 {qual['error_rate']}%")
        if jobs:
            print(f"  作业  Flink {jobs.get('flink')}  运行 {jobs.get('running', 0)} 个")
    return lat, jobs, qual


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--once", action="store_true", help="只采一轮")
    ap.add_argument("--interval", type=int, default=60, help="循环间隔秒数")
    ap.add_argument("--repair-from", metavar="'YYYY-MM-DD HH:MM'", default=None,
                help="强制校正：从该时刻起用明细重算 DWS 窗口并**允许写小**。"
                     "日常轮询已经会自动追平，只有「明细被清理过」才需要它")
    args = ap.parse_args()

    if args.repair_from:
        # 人工修数入口：日常轮询只补未闭合窗口（Flink 永不会 emit 的那几个），
        # 但「分批倒序灌数」会让一批早时间事件落在已推进的 watermark 之后，
        # 被 allowed lateness=0 丢掉 —— 那些窗口 DWS 永久偏小，Flink 也不会再 emit。
        # 停流后用明细终态重算一遍即可对齐；这里给个显式开关，避免每次手写脚本。
        lo = datetime.strptime(args.repair_from, "%Y-%m-%d %H:%M")
        conn = pymysql.connect(**DORIS)
        try:
            cur = conn.cursor()
            n = refresh_dws_tail(cur, lo_override=lo)
            conn.commit()
            print("校正完成：从 %s 起重算，写回 %d 行（1min 总览 / 5min 单曲 / 5min 地域）" % (lo, n))
        finally:
            conn.close()
        return 0

    if args.once:
        one_round()
        return 0

    print(f"开始采集，每 {args.interval} 秒一轮，Ctrl+C 退出")
    while True:
        try:
            one_round()
        except KeyboardInterrupt:
            print("\n已停止")
            return 0
        except Exception:
            # 单轮失败不能让采集循环退出：链路异常时更需要连续记录
            print(f"[{datetime.now():%H:%M:%S}] 本轮采集失败：")
            traceback.print_exc()
        time.sleep(args.interval)


if __name__ == "__main__":
    sys.exit(main())
