#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Flink 作业运维命令行：查看 / 取消 / savepoint / checkpoint 体检。

为什么单独抽一个工具：
    之前每次排查都是在命令行里临时拼 urllib 脚本，参数名（比如 savepoint 的
    `target-directory` 是连字符，不是驼峰）踩一次坑忘一次。这些操作在答辩现场
    也可能被要求演示，抽成固定命令比翻聊天记录可靠。

用法：
    python code/flink_admin.py ls                       # 列出作业 + 空闲 slot
    python code/flink_admin.py ckpt rt_job_d_ads        # 看 checkpoint 是否落盘
    python code/flink_admin.py cancel rt_job_d_ads      # 取消（保留 checkpoint）
    python code/flink_admin.py savepoint rt_job_d_ads   # 触发 savepoint 到 HDFS
    python code/flink_admin.py latest-ckpt rt_job_d_ads # 打印最新 checkpoint 的 HDFS 路径
"""

import argparse
import json
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import flink_gateway as fg  # noqa: E402

FM = "http://192.168.128.161:8081"
SAVEPOINT_DIR = "hdfs://WWC160:9820/flink/savepoints"
CHECKPOINT_DIR = "hdfs://WWC160:9820/flink/checkpoints"


def get(path, timeout=15):
    code, d = fg.call(FM, "GET", path, timeout=timeout)
    if code != 200:
        raise RuntimeError("GET %s -> %s %s" % (path, code, str(d)[:200]))
    return d


def jobs():
    return get("/jobs/overview").get("jobs", [])


def find(name=None, running_only=False):
    out = []
    for j in jobs():
        if name and j.get("name") != name:
            continue
        if running_only and j.get("state") != "RUNNING":
            continue
        out.append(j)
    return out


def one(name, running_only=True):
    js = find(name, running_only)
    if not js:
        raise SystemExit("找不到作业 %s%s" % (name, "（RUNNING）" if running_only else ""))
    if len(js) > 1:
        print("! 同名作业有 %d 个，取第一个：%s" % (len(js), js[0]["jid"][:12]))
    return js[0]


def cmd_ls(args):
    ov = get("/overview")
    print("集群：TM %s 个，slot 共 %s，空闲 %s，运行中作业 %s，失败 %s" % (
        ov.get("taskmanagers"), ov.get("slots-total"), ov.get("slots-available"),
        ov.get("jobs-running"), ov.get("jobs-failed")))
    print()
    for j in sorted(jobs(), key=lambda x: (x.get("state") != "RUNNING", x.get("name") or "")):
        dur = ""
        start = j.get("start-time")
        if start and start > 0:
            end = j.get("end-time")
            # 运行中的作业 end-time 是 -1，不是 None，不能直接 or
            if not end or end < 0:
                end = int(time.time() * 1000)
            dur = "%6.0fs" % ((end - start) / 1000.0)
        print("  %-12s %-10s %s %s" % (j["jid"][:12], j["state"], dur,
                                       (j.get("name") or "")[:48]))


def cmd_ckpt(args):
    j = one(args.name)
    d = get("/jobs/%s/checkpoints" % j["jid"])
    cnt = d.get("counts") or {}
    latest = (d.get("latest") or {}).get("completed") or {}
    print("作业 %s（%s）" % (j.get("name"), j["jid"][:12]))
    print("  checkpoint：总 %s / 完成 %s / 失败 %s / 恢复次数 %s" % (
        cnt.get("total"), cnt.get("completed"), cnt.get("failed"), cnt.get("restored")))
    if not latest:
        print("  （还没有完成的 checkpoint）")
        return
    print("  #%s  state %.2f MB  checkpointed %.2f MB  耗时 %sms" % (
        latest.get("id"), (latest.get("state_size") or 0) / 1048576.0,
        (latest.get("checkpointed_size") or 0) / 1048576.0,
        latest.get("end_to_end_duration")))
    path = latest.get("external_path")
    persisted = bool(path) and "not-externally-addressable" not in str(path)
    print("  落盘：%s" % ("是" if persisted else "否（只在 JM 堆内存里）"))
    print("  路径：%s" % path)
    if not persisted:
        print("\n  [问题] checkpoint 没有外部路径，JM 重启即丢失，作业无法断点恢复。")


def cmd_latest_ckpt(args):
    j = one(args.name)
    d = get("/jobs/%s/checkpoints" % j["jid"])
    latest = (d.get("latest") or {}).get("completed") or {}
    path = latest.get("external_path")
    if not path or "not-externally-addressable" in str(path):
        raise SystemExit("最新 checkpoint 没有落盘，无法用于恢复")
    print(path)


def cmd_cancel(args):
    j = one(args.name)
    print("取消 %s（%s）" % (j.get("name"), j["jid"][:12]))
    code, d = fg.call(FM, "PATCH", "/jobs/%s?mode=cancel" % j["jid"], timeout=20)
    print("  ->", code)
    deadline = time.time() + 60
    while time.time() < deadline:
        time.sleep(2)
        st = [x.get("state") for x in find(args.name) if x["jid"] == j["jid"]]
        if not st or st[0] in ("CANCELED", "FAILED", "FINISHED"):
            print("  已停止：%s" % (st[0] if st else "已从列表移除"))
            return
    print("  ! 等待停止超时")


def cmd_savepoint(args):
    j = one(args.name)
    target = args.dir or SAVEPOINT_DIR
    body = {"target-directory": target, "cancel-job": False}
    print("给 %s 触发 savepoint -> %s" % (j.get("name"), target))
    code, d = fg.call(FM, "POST", "/jobs/%s/savepoints" % j["jid"], body, timeout=25)
    if code not in (200, 202):
        raise SystemExit("触发失败：%s %s" % (code, str(d)[:300]))
    tid = d.get("request-id")
    deadline = time.time() + 180
    while time.time() < deadline:
        time.sleep(3)
        s = get("/jobs/%s/savepoints/%s" % (j["jid"], tid))
        st = (s.get("status") or {}).get("id")
        if st and st != "IN_PROGRESS":
            loc = (s.get("operation") or {}).get("location")
            print("  %s" % st)
            if loc:
                print("  %s" % loc)
            return
    print("  ! 等待超时（任务可能仍在进行）")


def main():
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)

    sub.add_parser("ls")
    p = sub.add_parser("ckpt"); p.add_argument("name")
    p = sub.add_parser("latest-ckpt"); p.add_argument("name")
    p = sub.add_parser("cancel"); p.add_argument("name")
    p = sub.add_parser("savepoint"); p.add_argument("name"); p.add_argument("--dir")

    args = ap.parse_args()
    {"ls": cmd_ls, "ckpt": cmd_ckpt, "latest-ckpt": cmd_latest_ckpt,
     "cancel": cmd_cancel, "savepoint": cmd_savepoint}[args.cmd](args)


if __name__ == "__main__":
    main()
