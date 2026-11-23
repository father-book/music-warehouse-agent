#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""DWS 逐窗对账：DWS 聚合值 vs DWD 明细（在 Windows 宿主机上运行）。

【为什么需要这个脚本】
    对账最容易踩的坑是口径不一致。造数器按 0.6% 故意发重复 event_id
    （模拟客户端重传），明细表是 DUPLICATE KEY，重复行并存；
    而 job_c_dws.sql 里用的是 COUNT(DISTINCT event_id)。
    如果对账 SQL 图省事写 COUNT(*)，就会看到「DWS 只有明细的 99.2%」这种
    假缺口，然后跑去调 watermark、改窗口，越改越错。
    —— 2026-11-08 实测踩过，差 6 条（702 vs 708）。

【对账口径】
    播放终止流 = dwd_play_end ∪ dwd_skip（`v_play_term` 口径）
    计数一律 COUNT(DISTINCT event_id)
    窗口边界用 FROM_UNIXTIME(FLOOR(UNIX_TIMESTAMP(ts)/len)*len) 对齐，
    与 job_c_dws.sql 完全一致。

【去重粒度也必须一致 —— 第二次踩的坑】
    DWS 是 GROUP BY (窗口, song_id / province) 之后才 COUNT(DISTINCT event_id)。
    造数器注入的重复 event_id **会带上不同的省份**，所以「全局按 event_id 去重」
    比「各省去重之和」少一大截（实测 11-07 22:35：13,773 vs 16,176，差 2,403）。
    对账必须先按 (窗口, 维度) 去重再求和，否则又是一个假缺口。

【判读】
    DWS < 明细：可能是 Flink 迟到丢弃（分批倒序灌数，allowed lateness=0），
                也可能尾窗还没闭合 —— 用 --repair-from 重算即可，见
                collect_ops_metrics.py 的说明。
    DWS > 明细：明细被清理过（如 sg_ 历史脏数据清理），DWS 是当时快照。
    两者都该为 0。

用法：
    python code/check_dws_consistency.py                       # 今天
    python code/check_dws_consistency.py --date 2026-11-08
    python code/check_dws_consistency.py --from "2026-11-08 13:00" --to "2026-11-08 15:00"
    python code/check_dws_consistency.py --date 2026-11-08 -v  # 打印每个窗口

退出码：0 全部对齐，1 存在差异，2 明细已清空无法对账。
"""

import argparse
import sys
from datetime import datetime, timedelta

import pymysql

DORIS = dict(host="192.168.128.163", port=9030, user="root", password="",
             database="music_dw", charset="utf8mb4")

# 播放终止流：skip 也算一次播放终止，必须并入（与 job_c_dws.sql 的 v_play_term 一致）
TERM_UNION = """
    SELECT event_id, event_ts, device_id, province, song_id, end_reason
    FROM dwd_play_end WHERE event_ts >= %(lo)s AND event_ts < %(hi)s
    UNION ALL
    SELECT event_id, event_ts, device_id, province, song_id, 'skip' AS end_reason
    FROM dwd_skip     WHERE event_ts >= %(lo)s AND event_ts < %(hi)s
"""


def q_overview(cur, lo, hi):
    """1 分钟总览：DWS.play_cnt vs 明细去重计数。"""
    cur.execute(f"""
        SELECT d.window_start, d.play_cnt dws, x.cnt dwd
        FROM dws_play_overview_1min d
        JOIN (
            SELECT CAST(FROM_UNIXTIME(FLOOR(UNIX_TIMESTAMP(event_ts)/60)*60) AS DATETIME) w,
                   COUNT(DISTINCT event_id) cnt
            FROM ({TERM_UNION}) t
            GROUP BY w
        ) x ON d.window_start = x.w
        WHERE d.window_start >= %(lo)s AND d.window_start < %(hi)s
        ORDER BY d.window_start
    """, dict(lo=lo, hi=hi))
    return cur.fetchall()


def q_overview_missing(cur, lo, hi):
    """明细有数据、DWS 没有对应窗口 —— 这类不是「差异」是「缺失」，更严重。"""
    cur.execute(f"""
        SELECT x.w, x.cnt
        FROM (
            SELECT CAST(FROM_UNIXTIME(FLOOR(UNIX_TIMESTAMP(event_ts)/60)*60) AS DATETIME) w,
                   COUNT(DISTINCT event_id) cnt
            FROM ({TERM_UNION}) t
            GROUP BY w
        ) x
        LEFT JOIN dws_play_overview_1min d ON d.window_start = x.w
        WHERE x.w >= %(lo)s AND x.w < %(hi)s AND d.window_start IS NULL
        ORDER BY x.w
    """, dict(lo=lo, hi=hi))
    return cur.fetchall()


def q_song_5min(cur, lo, hi):
    """5 分钟单曲：DWS 各歌合计 vs 明细去重计数。

    注意去重粒度必须与 DWS 的 GROUP BY 一致：DWS 是 GROUP BY (窗口, song_id)
    之后 COUNT(DISTINCT event_id)，所以同一 event_id 若出现在两个 song_id 下
    会被算两次。对账也必须先按 (窗口, song_id) 去重再求和，否则假缺口。
    """
    cur.execute(f"""
        SELECT s.win_start, s.dws, x.cnt dwd
        FROM (
            SELECT window_start win_start, SUM(play_cnt) dws
            FROM dws_song_play_5min
            WHERE window_start >= %(lo)s AND window_start < %(hi)s
            GROUP BY window_start
        ) s
        JOIN (
            SELECT y.w, SUM(y.c) cnt FROM (
                SELECT CAST(FROM_UNIXTIME(FLOOR(UNIX_TIMESTAMP(event_ts)/300)*300) AS DATETIME) w,
                       song_id, COUNT(DISTINCT event_id) c
                FROM ({TERM_UNION}) t
                GROUP BY w, song_id
            ) y GROUP BY y.w
        ) x ON s.win_start = x.w
        ORDER BY s.win_start
    """, dict(lo=lo, hi=hi))
    return cur.fetchall()


def q_prov_5min(cur, lo, hi):
    """5 分钟地域：DWS 各省合计 vs 明细去重计数。

    同 q_song_5min：DWS 是 GROUP BY (窗口, province) 后 COUNT(DISTINCT event_id)。
    造数器注入的重复 event_id 会带上**不同的省份**，所以全局去重比各省去重
    之和少一大截（实测 11-07 22:35：13,773 vs 16,176，差 2,403）。
    这不是丢数，是去重粒度不对 —— 必须按 (窗口, province) 去重。
    """
    cur.execute(f"""
        SELECT p.win_start, p.dws, x.cnt dwd
        FROM (
            SELECT window_start win_start, SUM(play_cnt) dws
            FROM dws_province_play_5min
            WHERE window_start >= %(lo)s AND window_start < %(hi)s
            GROUP BY window_start
        ) p
        JOIN (
            SELECT y.w, SUM(y.c) cnt FROM (
                SELECT CAST(FROM_UNIXTIME(FLOOR(UNIX_TIMESTAMP(event_ts)/300)*300) AS DATETIME) w,
                       COALESCE(NULLIF(province, ''), '未知') pv, COUNT(DISTINCT event_id) c
                FROM ({TERM_UNION}) t
                GROUP BY w, pv
            ) y GROUP BY y.w
        ) x ON p.win_start = x.w
        ORDER BY p.win_start
    """, dict(lo=lo, hi=hi))
    return cur.fetchall()


def range_ctx(cur, lo, hi):
    """区间两侧的原始行数 —— 用来识别「明细已被清空，JOIN 不上所以显示 0 差异」的假象。

    实例：2026-11-06 清理 sg_ 历史脏数据时把明细删了，DWS 里那 1325 个窗口还在。
    对账 INNER JOIN 结果为空，若只报「差异 0 个」，会被误读成「全对齐」。
    """
    cur.execute(f"SELECT COUNT(*) FROM ({TERM_UNION}) t", dict(lo=lo, hi=hi))
    dwd_n = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM dws_play_overview_1min "
                "WHERE window_start >= %s AND window_start < %s", (lo, hi))
    dws_n = cur.fetchone()[0]
    return dws_n, dwd_n


def judge(name, rows, verbose=False):
    """打印一项对账结果，返回差异窗口数。"""
    bad = [r for r in rows if r[1] != r[2]]
    print("\n--- %s：%d 个窗口，差异 %d 个 ---" % (name, len(rows), len(bad)))
    if not rows:
        print("    （区间内 DWS 无数据，检查时间段或看 --date）")
    if bad:
        for r in bad[:20]:
            print("    %s  dws=%-8s dwd=%-8s 差=%+d  %s"
                  % (r[0].strftime("%m-%d %H:%M"), r[1], r[2], r[1] - r[2],
                     "DWS 偏小" if r[1] < r[2] else "DWS 偏大"))
        if len(bad) > 20:
            print("    ... 另有 %d 个" % (len(bad) - 20))
    elif verbose:
        for r in rows:
            print("    %s  dws=%-8s dwd=%-8s OK" % (r[0].strftime("%m-%d %H:%M"), r[1], r[2]))
    return len(bad)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default=None, help="统计日期 yyyy-MM-dd，默认今天")
    ap.add_argument("--from", dest="lo", default=None, help="起始时间 'yyyy-MM-dd HH:MM'")
    ap.add_argument("--to", dest="hi", default=None, help="结束时间 'yyyy-MM-dd HH:MM'")
    ap.add_argument("--host", default=DORIS["host"])
    ap.add_argument("-v", "--verbose", action="store_true", help="逐窗打印（含对齐的）")
    args = ap.parse_args()

    if args.lo:
        lo = datetime.strptime(args.lo, "%Y-%m-%d %H:%M")
    else:
        d = datetime.strptime(args.date, "%Y-%m-%d") if args.date else datetime.now().replace(
            hour=0, minute=0, second=0, microsecond=0)
        lo = d
    if args.hi:
        hi = datetime.strptime(args.hi, "%Y-%m-%d %H:%M")
    else:
        hi = lo + timedelta(days=1)

    cfg = dict(DORIS)
    cfg["host"] = args.host
    conn = pymysql.connect(**cfg)
    try:
        cur = conn.cursor()
        print("=" * 74)
        print("DWS 逐窗对账  区间 [%s, %s)  口径 COUNT(DISTINCT event_id)" % (lo, hi))
        print("=" * 74)

        dws_n, dwd_n = range_ctx(cur, lo, hi)
        print("区间数据量：DWS 窗口 %d 个，明细终止事件 %d 条" % (dws_n, dwd_n))
        if dwd_n == 0 and dws_n > 0:
            print("\n!! 明细为空但 DWS 有数据 —— 明细被清理过（如 sg_ 脏数据清理），")
            print("   DWS 保留的是当时快照，本区间**无法对账**，不要当成「全部对齐」。")
            return 2
        if dwd_n == 0 and dws_n == 0:
            print("\n区间两侧都没有数据，检查时间范围或是否已灌数。")
            return 0

        n1 = judge("1min 总览  dws_play_overview_1min", q_overview(cur, lo, hi), args.verbose)

        miss = q_overview_missing(cur, lo, hi)
        if miss:
            print("\n--- 缺失窗口（明细有、DWS 无）：%d 个 ---" % len(miss))
            for r in miss[:20]:
                print("    %s  明细 %s 条" % (r[0].strftime("%m-%d %H:%M"), r[1]))

        n5 = judge("5min 单曲  dws_song_play_5min", q_song_5min(cur, lo, hi), args.verbose)
        np_ = judge("5min 地域  dws_province_play_5min", q_prov_5min(cur, lo, hi), args.verbose)

        total = n1 + n5 + np_ + len(miss)
        print("\n" + "=" * 74)
        if total == 0:
            print("结论：全部对齐（差异 0，缺失 0）")
        else:
            print("结论：差异 %d 个（1min %d / 5min 单曲 %d / 5min 地域 %d / 缺失 %d）"
                  % (total, n1, n5, np_, len(miss)))
            print("  偏小 → 多半是 Flink 迟到丢弃或尾窗未闭合，停流后重算：")
            print("         python code/collect_ops_metrics.py --repair-from 'YYYY-MM-DD HH:MM'")
            print("  偏大 → 明细被清理过（DWS 是当时快照）")
        print("=" * 74)
        return 0 if total == 0 else 1
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
