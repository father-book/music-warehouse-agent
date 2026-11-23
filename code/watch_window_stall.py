#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""窗口停滞告警：检测实时链路的窗口是否停止推进，并区分「断流」「追平」「卡死」。

背景（为什么要有这个脚本）：
  昨天两次误判「系统挂了」，根因都是没有区分三种本质不同的情况 ——
    1. 卡死（真故障）：作业 RUNNING、窗口长时间不推进。昨天 Doris BE 被内存顶满、
       D 的 sink 写不动，就是这类，必须立即告警。
    2. 断流（正常）：生成器停了、没有新数据进来，watermark 停在最后一条事件时间，
       窗口自然不再关闭。事件时间语义的正常行为，不该告警。
    3. 追平（正常）：作业刚重启、在追停机期间的空窗期。窗口会高速向前跳
       （一跳十几小时），此时「窗口落后」但「窗口在动」，不是卡死。

  区分三者的关键信号，是**窗口是否在移动**，而不是**窗口落后多少**：
    · 窗口在动（两次采样 MAX(window_start) 变化）→ 不管落后多少，都是追平，正常。
    · 窗口不动 且 DWD 还在写 → 卡死（数据进得来但算不动），告警。
    · 窗口不动 且 DWD 也停了 → 断流（没有输入），提示即可。

  所以本脚本必须用 --loop 常驻、做两次采样才能判断「窗口是否在动」。
  单次调用（--loop 0）只能报一个快照，无法区分追平和卡死 —— 只做巡检用。

用法：
  python code/watch_window_stall.py --loop 60      # 常驻：每 60 秒采样，连续两次不动才告警
  python code/watch_window_stall.py --loop 60 --stall 2   # 连续 2 次采样不动即告警
  python code/watch_window_stall.py                # 单次快照（巡检）
"""

import argparse
import time
from datetime import datetime

import pymysql

DORIS = dict(host="192.168.128.163", port=9030, user="root", password="",
             database="music_dw", charset="utf8mb4", connect_timeout=5, read_timeout=30)


def snapshot():
    """取一次窗口快照。返回 dict 或 None（Doris 连不上）。"""
    try:
        conn = pymysql.connect(**DORIS)
    except Exception as e:
        return {"error": str(e)[:80]}
    cur = conn.cursor()
    try:
        cur.execute("SELECT MAX(window_start) FROM ads_song_rank_10min")
        ads_max = cur.fetchone()[0]
        cur.execute("SELECT MAX(window_start) FROM dws_play_overview_1min")
        dws_max = cur.fetchone()[0]
        cur.execute("""SELECT COUNT(*) FROM dwd_play_end
                       WHERE etl_time >= NOW() - INTERVAL 2 MINUTE""")
        dwd_recent = cur.fetchone()[0]
        return {"ads": ads_max, "dws": dws_max, "dwd_recent": dwd_recent,
                "now": datetime.now()}
    finally:
        conn.close()


def classify(prev, cur, stall_rounds):
    """基于前后两次快照分类。prev 可为 None（首次采样，无法判断）。"""
    if cur.get("error"):
        return "WARN", f"Doris 连不上：{cur['error']}"

    ads, dws = cur["ads"], cur["dws"]
    dwd_recent = cur["dwd_recent"]
    lag = (cur["now"] - ads).total_seconds() if ads else None

    lines = [
        f"ADS {ads}",
        f"DWS {dws}",
        f"近2分钟DWD写入 {dwd_recent} 条",
    ]
    if lag is not None:
        lines.append(f"ADS滞后 {int(lag)}s")

    if ads is None:
        return "WARN", "ADS 无数据，作业 D 可能没跑起来 | " + " | ".join(lines)

    # 首次采样或历史空窗：无法判断是否在动，先观察
    if prev is None or prev.get("ads") != ads or prev.get("dws") != dws:
        # 窗口在动（或首次采样）→ 追平/正常
        return "OK", "窗口在推进 | " + " | ".join(lines)

    # 窗口连续两轮没动
    if dwd_recent > 0:
        return "WARN", ("窗口停滞但 DWD 仍在写 %d 条/2min —— 数据进得来算不动，"
                        "疑似卡死（BE/sink/watermark） | " % dwd_recent) + " | ".join(lines)
    return "IDLE", "窗口停滞且无输入，断流（生成器停了）而非卡死 | " + " | ".join(lines)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--loop", type=int, default=0, help="采样间隔秒，>0 则常驻")
    ap.add_argument("--stall", type=int, default=2, help="连续多少轮窗口不动才算停滞，默认 2")
    args = ap.parse_args()

    prev = None
    while True:
        cur = snapshot()
        if args.loop <= 0:
            # 单次快照：只能报状态，不能判断「是否在动」
            level, msg = classify(None, cur, args.stall)
            if level == "OK":
                # 单次模式下 OK 语义不准确，改为「快照」
                level = "IDLE"
            tag = {"OK": "  [OK]  ", "WARN": "  [告警] ", "IDLE": "  [巡检] "}[level]
            print(f"[{cur.get('now', datetime.now()):%H:%M:%S}]{tag}{msg}")
            break

        level, msg = classify(prev, cur, args.stall)
        tag = {"OK": "  [OK]  ", "WARN": "  [告警] ", "IDLE": "  [空闲] "}[level]
        print(f"[{cur.get('now', datetime.now()):%H:%M:%S}]{tag}{msg}")
        prev = cur
        time.sleep(args.loop)


if __name__ == "__main__":
    main()
