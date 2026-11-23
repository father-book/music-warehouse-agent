#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""把 bench_sample.py 采集的 TSV 画成压测曲线。

产出两张图（放 docs/ 或 dist/）：
  1. 时间序列：入口速率 / B 写出 / kafka 积压 随时间演变（能看出各档爬坡、稳态、排空）
  2. 速率-延迟散点：X=入口速率，Y=DWD延迟，点大小=积压，标出拐点

用法：
  python code/bench_plot.py bench_20261103.tsv docs/bench_curve.png
"""

import csv
import sys

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

# 中文字体：matplotlib 默认不含 CJK 字形，不设则中文全变方块
plt.rcParams["font.sans-serif"] = ["Microsoft YaHei", "SimHei", "Arial"]
plt.rcParams["axes.unicode_minus"] = False


def load(path):
    rows = []
    with open(path) as f:
        r = csv.DictReader(f, delimiter="\t")
        for row in r:
            try:
                rows.append({
                    "i": len(rows),
                    "ts": row["ts"],
                    "ods": int(row["ods_1m"]) / 60.0,
                    "dwd": int(row["dwd_1m"]) / 60.0,
                    "lag": int(row["dwd_lag_s"]),
                    "kafka": int(row["kafka_lag"]),
                })
            except (ValueError, KeyError):
                continue
    return rows


def main():
    path = sys.argv[1] if len(sys.argv) > 1 else "bench_20261103.tsv"
    out = sys.argv[2] if len(sys.argv) > 2 else "docs/bench_curve.png"
    rows = load(path)

    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 9), sharex=False)
    fig.suptitle("实时链路压测：速率 - 延迟 - 积压（%d 个采样点）" % len(rows),
                 fontsize=14, fontweight="bold")

    x = [r["i"] for r in rows]

    # 图1：时间序列
    ax1.plot(x, [r["ods"] for r in rows], label="入口速率 (条/s)", color="#dc2626", lw=1.6)
    ax1.plot(x, [r["dwd"] for r in rows], label="B 写出 (条/s)", color="#2563eb", lw=1.4, alpha=0.9)
    ax1.set_ylabel("速率 (条/s)", fontsize=11)
    ax1.legend(loc="upper left", fontsize=9)
    ax1.grid(alpha=0.25)
    ax1.set_title("各档位速率变化（爬坡 → 稳态 → 排空）", fontsize=11)

    ax1b = ax1.twinx()
    ax1b.plot(x, [r["kafka"] for r in rows], label="Kafka 积压 (行)", color="#f59e0b", lw=1.0, alpha=0.7)
    ax1b.set_ylabel("Kafka 积压 (行)", fontsize=11, color="#f59e0b")
    ax1b.tick_params(axis="y", labelcolor="#f59e0b")

    # 图2：速率-延迟散点，点大小=积压
    pts = [r for r in rows if r["ods"] > 0 and r["lag"] < 200]  # 剔除断流期（lag 是停机残留）
    if pts:
        ods = [r["ods"] for r in pts]
        lag = [r["lag"] for r in pts]
        size = [max(10, r["kafka"] / 2000) for r in pts]
        sc = ax2.scatter(ods, lag, s=size, c=ods, cmap="Reds", alpha=0.7, edgecolors="grey", linewidths=0.4)
        ax2.set_xlabel("入口速率 (条/s)", fontsize=11)
        ax2.set_ylabel("DWD 延迟 (s)", fontsize=11)
        ax2.set_title("速率-延迟关系（点大小 = Kafka 积压；延迟被 10min 窗口平滑，拐点在积压曲线上更明显）", fontsize=10)
        ax2.grid(alpha=0.25)
        cbar = fig.colorbar(sc, ax=ax2)
        cbar.set_label("入口速率 (条/s)", fontsize=9)

    plt.tight_layout()
    plt.savefig(out, dpi=130)
    print("已保存:", out)


if __name__ == "__main__":
    main()
