#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""通过 Flink SQL Gateway 的 REST 接口提交 SQL 文件。

为什么不用 sql-client：
    Flink 1.17 的 embedded 模式无法指定「提交到哪个集群」，且 execution.attached
    默认为 true，-f 跑到 INSERT 会一直等流作业结束、永不返回。
    gateway 模式（sql-client.sh gateway --endpoint ...）能解决，但从 Windows 侧
    还要先登录 161 才能执行。直接走 REST 就没有这个限制，且能读到每条语句的
    结构化报错。

用法：
    python code/flink_gateway.py sql/realtime/job_a_ods.sql
    python code/flink_gateway.py sql/realtime/job_b_dwd.sql --host 192.168.128.161

前置：161 上已执行 bin/sql-gateway.sh start，且配置了
    sql-gateway.endpoint.rest.address / bind-address / port
"""

import argparse
import json
import re
import sys
import time
import urllib.error
import urllib.request

DEFAULT_HOST = "192.168.128.161"
DEFAULT_PORT = 8083

# 这三个环境变量会把本机对内网的请求也导向代理，必须显式绕过
_OPENER = urllib.request.build_opener(urllib.request.ProxyHandler({}))


def call(base, method, path, body=None, timeout=60):
    data = json.dumps(body).encode("utf-8") if body is not None else None
    req = urllib.request.Request(
        base + path, data=data, method=method,
        headers={"Content-Type": "application/json"},
    )
    try:
        with _OPENER.open(req, timeout=timeout) as resp:
            txt = resp.read().decode("utf-8")
            return resp.status, (json.loads(txt) if txt.strip() else {})
    except urllib.error.HTTPError as e:
        txt = e.read().decode("utf-8", errors="replace")
        try:
            return e.code, json.loads(txt)
        except Exception:
            return e.code, {"raw": txt}
    except urllib.error.URLError as e:
        return 0, {"error": str(e.reason)}


def strip_comments(sql):
    out = []
    for line in sql.split("\n"):
        idx = line.find("--")
        if idx >= 0:
            line = line[:idx]
        out.append(line)
    return "\n".join(out)


def split_statements(sql):
    """按分号拆语句，但 EXECUTE STATEMENT SET BEGIN ... END; 要整体保留。

    那段内部有多个 INSERT，各自带分号。按分号硬拆会把一条复合语句切成碎片，
    gateway 会报语法错误。
    """
    blocks = []

    def keep(m):
        blocks.append(m.group(0))
        return f"/*BLOCK{len(blocks) - 1}*/SELECT 1;"

    masked = re.sub(
        r"EXECUTE\s+STATEMENT\s+SET\s+BEGIN.*?\n\s*END\s*;",
        keep, sql, flags=re.S | re.I,
    )

    stmts, buf, in_str = [], "", False
    i = 0
    while i < len(masked):
        ch = masked[i]
        if ch == "'":
            # 处理 '' 转义
            if in_str and i + 1 < len(masked) and masked[i + 1] == "'":
                buf += "''"
                i += 2
                continue
            in_str = not in_str
            buf += ch
        elif ch == ";" and not in_str:
            buf += ch
            if buf.strip():
                stmts.append(buf.strip())
            buf = ""
        else:
            buf += ch
        i += 1
    if buf.strip():
        stmts.append(buf.strip())

    # 还原被掩码的 STATEMENT SET
    restored = []
    for s in stmts:
        m = re.match(r"^/\*BLOCK(\d+)\*/SELECT 1;$", s.strip())
        if m:
            restored.append(blocks[int(m.group(1))].strip())
        else:
            restored.append(s)
    return restored


def is_long_running(stmt):
    up = stmt.upper()
    return "INSERT INTO" in up or "EXECUTE STATEMENT SET" in up


def describe(stmt):
    s = " ".join(stmt.split())
    if len(s) > 78:
        s = s[:75] + "..."
    return s


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("file", help="SQL 文件路径")
    ap.add_argument("--host", default=DEFAULT_HOST)
    ap.add_argument("--port", type=int, default=DEFAULT_PORT)
    ap.add_argument("--timeout", type=int, default=120, help="单条语句等待上限（秒）")
    args = ap.parse_args()

    base = f"http://{args.host}:{args.port}"

    code, info = call(base, "GET", "/v1/info")
    if code != 200:
        print(f"无法连接 SQL Gateway {base}：{info}")
        print("确认 161 上 bin/sql-gateway.sh start 已执行，且配置了 "
              "sql-gateway.endpoint.rest.address")
        return 1
    print(f"已连接 SQL Gateway：Flink {info.get('version')}")

    code, sess = call(base, "POST", "/v1/sessions", {})
    if code not in (200, 201) or "sessionHandle" not in sess:
        print(f"创建会话失败：{sess}")
        return 1
    sh = sess["sessionHandle"]
    print(f"会话 {sh}\n")

    sql = strip_comments(open(args.file, encoding="utf-8").read())
    stmts = split_statements(sql)
    print(f"{args.file}：{len(stmts)} 条语句\n")
    print("=" * 70)

    failed = 0
    for idx, stmt in enumerate(stmts, 1):
        print(f"[{idx}/{len(stmts)}] {describe(stmt)}")
        code, op = call(base, "POST", f"/v1/sessions/{sh}/statements",
                        {"statement": stmt})
        if code not in (200, 201) or "operationHandle" not in op:
            print(f"    ✗ 提交失败 ({code}): {json.dumps(op, ensure_ascii=False)[:600]}")
            failed += 1
            break

        oh = op["operationHandle"]
        deadline = time.time() + args.timeout
        status = "PENDING"
        detail = None
        while time.time() < deadline:
            time.sleep(0.4)
            c2, st = call(base, "GET", f"/v1/sessions/{sh}/operations/{oh}/status")
            status = st.get("status", "?")
            if status in ("FINISHED",):
                break
            if status == "ERROR":
                detail = st.get("exception", st)
                break
            # 长时作业进入 RUNNING 后不会 FINISHED，但「RUNNING」只代表语句在执行，
            # 作业仍可能随后提交失败。必须再多观察一段时间，否则会把失败当成功。
            if status == "RUNNING" and is_long_running(stmt):
                settle = time.time() + 15
                while time.time() < settle:
                    time.sleep(1)
                    c3, st2 = call(base, "GET",
                                   f"/v1/sessions/{sh}/operations/{oh}/status")
                    if st2.get("status") == "ERROR":
                        status = "ERROR"
                        detail = st2.get("exception", st2)
                        break
                break

        if status == "FINISHED":
            print("    ✓ 完成")
        elif status == "ERROR":
            failed += 1
            print("    ✗ 执行失败")
            # gateway 的 status 接口在部分版本里不带异常体，改从 result 接口取
            c4, res = call(base, "GET",
                           f"/v1/sessions/{sh}/operations/{oh}/result/0")
            err = res.get("results", {}).get("error") if isinstance(res, dict) else None
            if err:
                for key in ("root-cause", "message", "stack-trace"):
                    val = err.get(key)
                    if val:
                        print(f"      [{key}]")
                        for line in str(val).split("\n")[:6]:
                            print(f"        {line.strip()[:200]}")
            if not err and isinstance(detail, dict):
                for key in ("root-cause", "message", "stack-trace"):
                    val = detail.get(key)
                    if val:
                        print(f"      [{key}] {str(val)[:300]}")
                if not any(detail.get(k) for k in ("root-cause", "message", "stack-trace")):
                    print(f"      原始: {json.dumps(detail, ensure_ascii=False)[:800]}")
            if not err and not detail:
                print(f"      result 原始: {json.dumps(res, ensure_ascii=False)[:1000]}")
            break
        elif status == "RUNNING" and is_long_running(stmt):
            print("    ✓ 已提交，作业运行中")
        else:
            failed += 1
            print(f"    ✗ 超时未完成，最后状态 {status}")
            break

    print("=" * 70)
    if failed:
        print("提交中断，后面的语句未执行")
        return 1
    print("全部语句执行完毕。检查作业状态：")
    print("    python code/check_realtime.py")
    return 0


if __name__ == "__main__":
    sys.exit(main())
