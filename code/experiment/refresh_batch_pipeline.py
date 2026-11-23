"""【实验脚本，不在生产链路中】批式对照组：DWD → DWS → ADS 全量重算。

生产链路的四层全部由 Flink 作业实时写入，没有任何外部脚本参与计算：

    作业 A → ODS      作业 B → DWD + 异常表
    作业 C → DWS      作业 D → ADS

大屏（dashboard/app.py）只读 Doris，不触发任何计算。

本脚本唯一的用途是论文第四章的「离线轨 vs 实时轨」双轨对比实验：
同一批数据分别走批式重算与流式管道，比对两边的指标结果是否一致。
它是一次性实验工具，不是数据管道的一环。

使用前提：**必须先停掉 Flink 作业**。脚本会 TRUNCATE DWD/DWS，
作业在跑的时候执行会把流式写入的数据清空。

用法：
    # 1. 停 Flink 作业
    bash sql/realtime/stop_jobs.sh
    # 2. 跑批式全量
    python code/experiment/refresh_batch_pipeline.py --once
    # 3. 记录结果后清空，再重启 Flink 作业做实时轨
    bash sql/realtime/run_jobs.sh

设计说明：
    1. 每轮采用「清空 + 全量重算」，保证幂等，重跑不会累积重复数据。
    2. 三层顺序不可颠倒：DWS 依赖 DWD，ADS 依赖 DWS。
    3. 重算期间目标表会短暂为空。
"""
import argparse
import os
import sys
import time
from datetime import datetime

import pymysql

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

DORIS = dict(host="192.168.128.163", port=9030, user="root", password="",
             database="music_dw", charset="utf8mb4", autocommit=True)

DWD_TABLES = ["dwd_play_end", "dwd_play_start", "dwd_skip", "dwd_search",
              "dwd_collect", "dwd_share", "dwd_page_view", "dwd_quality_error"]
DWS_TABLES = ["dws_play_overview_1min", "dws_song_play_5min", "dws_province_play_5min"]
ADS_TABLES = ["ads_realtime_overview", "ads_song_rank_10min", "ads_province_10min"]

SQL_DWD = ["sql/dwd/doris_dwd_play_end_clean.sql", "sql/dwd/generated_clean.sql"]
SQL_DWS = ["sql/dws/doris_dws_aggregate.sql"]
SQL_ADS = ["sql/ads/doris_ads_aggregate.sql"]

# 重算时禁止插入的语句（SQL 文件里的 TRUNCATE 交给脚本统一控制）
SKIP_KEYWORDS = ("TRUNCATE",)

MODES = ("ads", "all")


def split_statements(sql: str):
    lines = [ln for ln in sql.split("\n") if not ln.strip().startswith("--")]
    body = "\n".join(lines)
    return [s.strip() for s in body.split(";") if s.strip()]


def run_sql_file(cur, rel_path):
    path = os.path.join(BASE_DIR, rel_path)
    if not os.path.exists(path):
        raise FileNotFoundError(path)
    stmts = split_statements(open(path, encoding="utf-8").read())
    n = 0
    for stmt in stmts:
        if not stmt or stmt.lstrip().upper().startswith(SKIP_KEYWORDS):
            continue
        if stmt.upper().startswith("USE "):
            continue
        cur.execute(stmt)
        n += 1
    return n


def table_count(cur, table):
    cur.execute(f"SELECT COUNT(*) FROM {table}")
    return cur.fetchone()[0]


def refresh_once(verbose=True, mode="ads"):
    """重算一轮。

    mode="ads"  只重算 ADS（Flink 负责 DWD/DWS）
    mode="all"  批式对照组，DWD + DWS + ADS 全量重算
    """
    if mode not in MODES:
        raise ValueError(f"mode 必须是 {MODES} 之一，收到 {mode}")

    t_start = time.time()
    conn = pymysql.connect(**DORIS)
    cur = conn.cursor()

    # v10：实时 ODS 是 Kafka topic，累计条数取 log-end-offset 之和
    import os, sys
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from kafka_ods import offsets
    ods_cnt = offsets()[0]
    dwd_cnt = err_cnt = dws_cnt = 0
    t_dwd = t_dws = 0.0

    if mode == "all":
        # --- DWD ---
        t0 = time.time()
        for t in DWD_TABLES:
            cur.execute(f"TRUNCATE TABLE {t}")
        for f in SQL_DWD:
            run_sql_file(cur, f)
        dwd_cnt = sum(table_count(cur, t) for t in DWD_TABLES[:-1])
        err_cnt = table_count(cur, "dwd_quality_error")
        t_dwd = time.time() - t0

        # --- DWS ---
        t0 = time.time()
        for t in DWS_TABLES:
            cur.execute(f"TRUNCATE TABLE {t}")
        for f in SQL_DWS:
            run_sql_file(cur, f)
        dws_cnt = sum(table_count(cur, t) for t in DWS_TABLES)
        t_dws = time.time() - t0

    # --- ADS ---
    t0 = time.time()
    for t in ADS_TABLES:
        cur.execute(f"TRUNCATE TABLE {t}")
    for f in SQL_ADS:
        run_sql_file(cur, f)
    t_ads = time.time() - t0

    conn.close()

    if verbose:
        ts = datetime.now().strftime("%H:%M:%S")
        print(f"[{ts}] mode={mode}  ODS {ods_cnt:>7,} 行")
        if mode == "all":
            print(f"         DWD {dwd_cnt:>7,} 行 + 异常 {err_cnt:>4,} 条   耗时 {t_dwd:>4.1f}s")
            print(f"         DWS {dws_cnt:>7,} 行                    耗时 {t_dws:>4.1f}s")
        else:
            print(f"         DWD/DWS 由 Flink 作业实时写入，本轮跳过")
        print(f"         ADS 3 张表已更新                    耗时 {t_ads:>4.1f}s")
        print(f"         合计 {time.time() - t_start:.1f}s\n")

    return {"ods": ods_cnt, "dwd": dwd_cnt, "err": err_cnt,
            "dws": dws_cnt, "elapsed": time.time() - t_start}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--once", action="store_true", help="只跑一轮")
    ap.add_argument("--interval", type=int, default=20, help="循环间隔（秒）")
    ap.add_argument("--mode", choices=MODES, default="all",
                    help="all=全量重算（实验默认）；ads=只算 ADS")
    args = ap.parse_args()

    if args.once:
        refresh_once(mode=args.mode)
        return

    print(f"每 {args.interval} 秒重算一次（mode={args.mode}），Ctrl+C 停止\n")
    round_no = 0
    try:
        while True:
            round_no += 1
            try:
                refresh_once(mode=args.mode)
            except Exception as e:
                print(f"  第 {round_no} 轮失败: {e}")
            time.sleep(args.interval)
    except KeyboardInterrupt:
        print(f"\n已停止，共执行 {round_no} 轮")


if __name__ == "__main__":
    main()
