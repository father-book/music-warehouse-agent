#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""集群重启后按序重建实时链路：HBase 维表装载 → A/B/C/D 四个作业。

为什么需要这个脚本：
    Flink 集群重启（加 HADOOP_CLASSPATH）后所有作业都没了，需要按依赖顺序重提。
    A/B/C/D 不是随便并行提交的——装载作业要先跑完释放 slot，且 B 依赖 HBase 表
    已经有数据，否则 dwd_play_end 的维度列会全是空串。

顺序：
    1. hbase_dim_load.sql  —— bounded，必须等 FINISHED
    2. job_a_ods.sql       —— 1 slot，music_events → ODS topic + Doris
    3. job_b_dwd_dim.sql   —— 2 slot，ODS → 7 明细 + 异常 + dwd_play_end（含 HBase 点查）
    4. job_c_dws.sql       —— 1 slot，dwd_play_end → 3 张窗口表
    5. job_d_ads.sql       —— 1 slot，dwd_play_end → 3 张 ADS 表

用法：
    python code/redeploy_realtime.py              # 全流程
    python code/redeploy_realtime.py --skip-load  # 维表已装载，只重提四个作业
    python code/redeploy_realtime.py --only a b   # 只提指定作业（调试用）
"""

import argparse
import json
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import flink_gateway as fg  # noqa: E402

BASE = "http://192.168.128.161:8083"
FM = "http://192.168.128.161:8081"
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

LOAD_SQL = os.path.join(ROOT, "sql", "dim", "hbase_dim_load.sql")
JOBS = {
    "a": (os.path.join(ROOT, "sql", "realtime", "job_a_ods.sql"), 1),
    "b": (os.path.join(ROOT, "sql", "realtime", "job_b_dwd_dim.sql"), 2),
    "c": (os.path.join(ROOT, "sql", "realtime", "job_c_dws.sql"), 1),
    "d": (os.path.join(ROOT, "sql", "realtime", "job_d_ads.sql"), 1),
}


def fm_get(path):
    return fg.call(FM, "GET", path)


def job_ids():
    code, d = fm_get("/jobs")
    if code != 200:
        return set()
    return {j["id"] for j in d.get("jobs", [])}


def job_states():
    code, d = fm_get("/jobs/overview")
    if code != 200:
        return []
    return d.get("jobs", [])


def new_session():
    code, s = fg.call(BASE, "POST", "/v1/sessions", {})
    if "sessionHandle" not in s:
        raise RuntimeError("创建 Gateway 会话失败: %s" % s)
    return s["sessionHandle"]


def run_stmt(sh, stmt, timeout=180):
    """执行单条语句；返回 (status, 错误摘要)。"""
    code, op = fg.call(BASE, "POST", "/v1/sessions/%s/statements" % sh,
                       {"statement": stmt})
    if "operationHandle" not in op:
        return "SUBMIT_FAIL", json.dumps(op, ensure_ascii=False)[:400]
    oh = op["operationHandle"]
    deadline = time.time() + timeout
    status = "PENDING"
    while time.time() < deadline:
        time.sleep(0.4)
        c2, st = fg.call(BASE, "GET",
                         "/v1/sessions/%s/operations/%s/status" % (sh, oh))
        status = st.get("status", "?")
        if status in ("FINISHED", "ERROR", "CANCELED"):
            break
    detail = ""
    if status != "FINISHED":
        c3, res = fg.call(BASE, "GET",
                          "/v1/sessions/%s/operations/%s/result/0" % (sh, oh))
        err = "\n".join(res.get("errors") or [])
        for line in err.splitlines():
            if line.startswith("Caused by"):
                detail = line[:400]
                break
        detail = detail or err[:400]
    return status, detail


def submit(path, label, expect_bounded=False, wait=600, restore=None):
    """提交一个 SQL 文件，返回新出现的 job id 列表。

    restore：checkpoint / savepoint 的 HDFS 路径。指定后会在第一条 CREATE TABLE
    之前插入 `SET 'execution.savepoint.path'`，让作业从该状态恢复而不是重新开始。
    这就是「配置变更不丢状态」的做法：先 cancel（RETAIN_ON_CANCELLATION 会保留
    最后一次 checkpoint），改配置，再带路径重提。
    """
    sql = fg.strip_comments(open(path, encoding="utf-8").read())
    if restore:
        # 插在第一条 CREATE TABLE 之前，保证 SET 先生效
        idx = sql.find("CREATE TABLE")
        sql = (sql[:idx]
               + "SET 'execution.savepoint.path' = '%s';\n\n" % restore
               + sql[idx:])
        print("  ↺ 从状态恢复：%s" % restore)
    stmts = fg.split_statements(sql)
    before = job_ids()
    sh = new_session()
    print("  %s：%d 条语句" % (label, len(stmts)))

    for idx, stmt in enumerate(stmts, 1):
        status, detail = run_stmt(sh, stmt, timeout=300)
        head = " ".join(stmt.split())[:64]
        if status in ("ERROR", "SUBMIT_FAIL", "CANCELED"):
            print("    [%d/%d] %-64s ✗ %s" % (idx, len(stmts), head, status))
            if detail:
                print("        %s" % detail)
            return None
        if "INSERT INTO" in stmt.upper() or "STATEMENT SET" in stmt.upper():
            print("    [%d/%d] %-64s ✓ 已提交" % (idx, len(stmts), head))
        else:
            print("    [%d/%d] %-64s ✓" % (idx, len(stmts), head))

    # 找出本次新提交的作业
    time.sleep(2)
    created = job_ids() - before
    if not created:
        print("    ! 没有发现新作业，检查 SQL 是否包含 INSERT")
        return []

    if expect_bounded:
        deadline = time.time() + wait
        while time.time() < deadline:
            states = {j["jid"]: j["state"] for j in job_states()}
            cur = [states.get(j) for j in created if states.get(j)]
            if all(s == "FINISHED" for s in cur) and cur:
                print("    装载作业已 FINISHED（%s）" % ", ".join(j[:8] for j in created))
                return list(created)
            if any(s in ("FAILED", "CANCELED") for s in cur):
                print("    ✗ 作业失败：%s" % cur)
                for j in created:
                    c, e = fm_get("/jobs/%s/exceptions" % j)
                    if c == 200 and e.get("all-exceptions"):
                        print("      %s" % e["all-exceptions"][0].get("exception", "")[:300])
                return None
            time.sleep(3)
        print("    ! 等待超时，作业仍在运行：%s" % created)
        return list(created)

    # 流式作业：等它进入 RUNNING
    deadline = time.time() + 120
    while time.time() < deadline:
        states = {j["jid"]: j["state"] for j in job_states()}
        cur = {j: states.get(j) for j in created}
        seen = [s for s in cur.values() if s]
        if seen and all(s == "RUNNING" for s in seen) and len(seen) == len(created):
            print("    作业 RUNNING：%s" % ", ".join(j[:8] for j in created))
            return list(created)
        if any(s in ("FAILED", "CANCELED") for s in cur.values()):
            print("    ✗ 作业失败：%s" % cur)
            for j in created:
                c, e = fm_get("/jobs/%s/exceptions" % j)
                if c == 200 and e.get("all-exceptions"):
                    print("      %s" % e["all-exceptions"][0].get("exception", "")[:400])
            return None
        time.sleep(2)
    print("    ! 等待 RUNNING 超时：%s" % created)
    return list(created)


def check_slots():
    c, d = fm_get("/overview")
    if c != 200:
        print("无法读取 Flink /overview：", d)
        return None
    print("集群：%d 个 TM，%d slots，空闲 %d，运行中作业 %d" % (
        d.get("taskmanagers"), d.get("slots-total"),
        d.get("slots-available"), d.get("jobs-running")))
    return d


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--skip-load", action="store_true", help="跳过 HBase 维表装载")
    ap.add_argument("--only", nargs="*", choices=list(JOBS), help="只提交指定作业")
    ap.add_argument("--restore", help="从该 checkpoint/savepoint 路径恢复（仅配合 --only 单个作业）")
    args = ap.parse_args()

    code, info = fg.call(BASE, "GET", "/v1/info")
    if code != 200:
        print("SQL Gateway 不可达：", info)
        return 1
    print("SQL Gateway：Flink %s\n" % info.get("version"))
    ov = check_slots()
    if ov is None:
        return 1

    if not args.skip_load and not args.only:
        print("\n[1/5] 装载 HBase 维表（bounded，会等它跑完）")
        if submit(LOAD_SQL, "hbase_dim_load.sql", expect_bounded=True) is None:
            print("\n装载失败，后续作业不提交（DIM 为空时 B 的维度列全是空串）")
            return 1
        check_slots()

    todo = args.only or ["a", "b", "c", "d"]
    for i, key in enumerate(todo, 1):
        path, slots = JOBS[key]
        print("\n[%d/%d] 作业 %s（需要 %d slot）" % (i + (0 if args.only else 1),
                                                  len(todo) + (0 if args.only else 1),
                                                  key.upper(), slots))
        restore = args.restore if (args.restore and len(todo) == 1) else None
        if submit(path, os.path.basename(path), restore=restore) is None:
            print("\n作业 %s 提交失败，后续不提交" % key.upper())
            return 1
        check_slots()

    print("\n=== 最终状态 ===")
    for j in job_states():
        print("  %-14s %-10s %s" % (j["jid"][:12], j["state"], j.get("name", "")[:60]))
    print("\n下一步：python code/check_hbase_dim.py")
    return 0


if __name__ == "__main__":
    sys.exit(main())
