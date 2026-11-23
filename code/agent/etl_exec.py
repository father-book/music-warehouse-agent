"""ETL 执行通道：把校验过的建表 SQL 真正跑到 161 的 Hive 引擎上。

为什么执行走 SSH + Hive 而不是 Doris：
  Doris 2.x 不允许向外部 catalog（hive_catalog）写数据（2026-11-11 实测
  NativeInsertStmt 限制），而护栏放行的 SQL 是两段式 `music_dw.dev_*` 的 Hive
  方言——它本来就是给 Hive 引擎准备的。

三道边界（缺一不可）：
  1. 角色：仅 data_engineer（webapp 端点里卡）
  2. 语句：执行前用 etlguard.validate 重验一遍，四项 + EXPLAIN 全过才放行
     ——不信任已入库记录，历史记录也可能是旧版本护栏放行的
  3. 机器：只连 config.etl_exec.host 白名单里那台；SQL 先经 ssh 落到远端临时
     文件，再 `hive -f` 同步执行（stdin 方式对 CTAS 会假成功，见 run_hive）

执行后两件收尾：
  · Doris 侧 REFRESH TABLE：外部 catalog 的元数据不是实时同步的，2026-11-11
    实测不刷则 DESCRIBE 仍看到旧结构
  · agent_ro 回读行数：证明新建的表在问数账号下**可见可查**，否则"建好了但
    查不到"会在下游变成第二个坑
"""
from __future__ import annotations

import shutil
import subprocess
import time

import etlguard

# Hive CLI 无 -e/-f 时从 stdin 逐条读命令；-S 只省掉"OK/Time taken"这类噪音，
# INFO 日志仍走 stdout，所以判定成功看 FAILED 行而不是看输出是否为空。
_ERROR_MARKS = ("FAILED:", "ParseException", "SemanticException",
                "Execution Error", "Error while compiling")
_NOISE = ("INFO ", "WARN ", "SLF4J", "log4j", "Hive-on-MR is deprecated")


def _clean(text: str, limit: int = 4000) -> str:
    lines = [ln for ln in (text or "").splitlines()
             if ln.strip() and not any(n in ln for n in _NOISE)]
    out = "\n".join(lines)
    return out[-limit:]


def exec_conf(cfg: dict) -> dict:
    c = (cfg.get("etl_exec") or {})
    return {"enabled": bool(c.get("enabled", False)),
            "host": str(c.get("host") or ""),
            "user": str(c.get("user") or "root"),
            "hive_bin": str(c.get("hive_bin") or "/opt/module/hive/bin/hive"),
            "timeout_s": int(c.get("timeout_s") or 600)}


def run_hive(sql: str, cfg: dict) -> dict:
    """把 SQL 经 stdin 落到目标机器的临时文件，再用 `hive -f` 同步执行。

    为什么不用 `hive` 直读 stdin：2026-11-11 实测，stdin 管道模式下 CTAS 的
    Spark 作业会被提前收尾——CLI 返回 0、无 FAILED 行，但表根本没建出来
    （DROP 却生效了）。`-f 文件` 是同步等待作业完成的，也是手工验证走通的路径。
    """
    c = exec_conf(cfg)
    if not c["enabled"] or not c["host"]:
        return {"ok": False, "error": "执行通道未启用（config.etl_exec.enabled=false）"}
    ssh = shutil.which("ssh")
    if not ssh:
        return {"ok": False, "error": "本机找不到 ssh 客户端"}
    remote = "/tmp/etl_exec_%d.sql" % int(time.time() * 1000)
    cmd = ("cat > %s && %s -S -f %s; rc=$?; rm -f %s; exit $rc"
           % (remote, c["hive_bin"], remote, remote))
    t0 = time.perf_counter()
    try:
        p = subprocess.run(
            [ssh, "-o", "BatchMode=yes", "-o", "ConnectTimeout=8",
             f"{c['user']}@{c['host']}", cmd],
            input=sql, capture_output=True, text=True, encoding="utf-8",
            errors="replace", timeout=c["timeout_s"])
    except subprocess.TimeoutExpired:
        return {"ok": False, "seconds": round(time.perf_counter() - t0, 1),
                "error": f"执行超时（>{c['timeout_s']}s），作业可能仍在集群上跑"}
    except Exception as e:
        return {"ok": False, "error": "ssh 调用失败：%s" % str(e)[:180]}
    bag = (p.stdout or "") + "\n" + (p.stderr or "")
    bad = [ln for ln in bag.splitlines() if any(m in ln for m in _ERROR_MARKS)]
    ok = p.returncode == 0 and not bad
    return {"ok": ok, "seconds": round(time.perf_counter() - t0, 1),
            "output_tail": _clean(bag),
            "error": ("" if ok else (bad[0][:300] if bad
                                     else f"hive 退出码 {p.returncode}"))}


def _admin_conn(cfg: dict):
    import pymysql
    c = cfg.get("doris_admin") or {}
    return pymysql.connect(host=c.get("host") or "192.168.128.163",
                           port=int(c.get("port") or 9030),
                           user=c.get("user") or "root",
                           password=c.get("password") or "",
                           connect_timeout=5, read_timeout=120)


def refresh_metadata(target: str, cfg: dict) -> dict:
    """Doris 侧刷新外部 catalog 元数据。

    两种刷新不是可互换的（2026-11-11 实测）：
      · 表结构变了（列增删）→ REFRESH TABLE 有效
      · 表是**新建**的 → REFRESH TABLE 直接报 "Table does not exist"，必须先
        REFRESH CATALOG hive_catalog 让 FE 重新拉一遍 metastore
    所以按"先精确、后整库"的顺序试，REFRESH CATALOG 是重操作，只在需要时用。
    """
    t = target.split(".")[-1]
    try:
        conn = _admin_conn(cfg)
        with conn.cursor() as cur:
            cur.execute("SWITCH hive_catalog")
            table_ok = True
            try:
                cur.execute(f"REFRESH TABLE music_dw.{t}")
            except Exception:
                table_ok = False
            if not table_ok:
                cur.execute("REFRESH CATALOG hive_catalog")
        conn.close()
        return {"refreshed": True,
                "refresh_mode": "table" if table_ok else "catalog",
                "refresh_error": ""}
    except Exception as e:
        return {"refreshed": False, "refresh_error": str(e)[:200]}


def verify_table(target: str, cfg: dict) -> dict:
    """用问数账号回读：行数 + dt 范围。建好但 agent_ro 查不到 = 没真的交付。"""
    import db
    t = target.split(".")[-1]
    cols = [c["name"] for c in describe_table(t, cfg)]
    has_dt = any(c.lower() == "dt" for c in cols)
    sql = ("SELECT COUNT(*)%s FROM %s"
           % (", MIN(dt), MAX(dt)" if has_dt else "", t))
    r = db.run_sql(sql, cfg)
    if r.get("error"):
        return {"visible": False, "verify_error": str(r["error"])[:220]}
    row = (r.get("rows") or [[None]])[0]
    out = {"visible": True, "rows": int(row[0] or 0)}
    if has_dt:
        out["dt_min"] = str(row[1]) if row[1] is not None else ""
        out["dt_max"] = str(row[2]) if row[2] is not None else ""
    return out


def describe_table(target: str, cfg: dict) -> list[dict]:
    """DESCRIBE 目标表，返回 [{name, type}]。走 agent_ro 的连接池。

    注意 Doris 的 DESCRIBE 列序是 Field/Type/Null/Key/Default/Extra —— **没有
    comment 列**。2026-11-11 踩过：把第 3 列当注释，DDL 里生成了
    `COMMENT 'Yes'`（那是 Null 标记）。这里只取前两列，其余不用。
    """
    import db
    t = target.split(".")[-1]
    r = db.run_sql("DESCRIBE %s" % t, cfg)
    if r.get("error"):
        return []
    out = []
    for row in r.get("rows") or []:
        if row and row[0]:
            out.append({"name": str(row[0]),
                        "type": str(row[1]).lower() if len(row) > 1 else ""})
    return out


def execute_ctas(sql: str, cfg: dict) -> dict:
    """重验 → 执行（DROP + CTAS）→ 刷新元数据 → 回读验证。"""
    checks = etlguard.validate(sql, cfg)
    ok_static = all(checks.get(k) for k in
                    ("stmt_ok", "target_ok", "tables_ok", "danger_ok",
                     "catalog_ok", "explain_ok"))
    if not ok_static:
        return {"ok": False, "stage": "validate", "checks": checks,
                "error": "执行前重验未通过：%s" % (checks.get("explain_error")
                                                or "见检查项")}
    target = checks.get("target") or ""
    if not target:
        return {"ok": False, "stage": "validate", "error": "识别不出目标表"}
    t = target.split(".")[-1]
    # DROP 由系统补，不来自模型——护栏禁止生成物自带 DROP，但"重建"语义要求它。
    # 两条语句一次喂给 hive CLI，它按行逐条执行。
    script = f"DROP TABLE IF EXISTS music_dw.{t};\n{sql.rstrip(';')};\n"
    r = run_hive(script, cfg)
    if not r.get("ok"):
        return {"ok": False, "stage": "execute", "target": target,
                "seconds": r.get("seconds"), "error": r.get("error"),
                "output_tail": r.get("output_tail", "")}
    mr = refresh_metadata(target, cfg)
    vr = verify_table(target, cfg)
    return {"ok": bool(vr.get("visible")), "stage": "verify", "target": target,
            "seconds": r.get("seconds"), "output_tail": r.get("output_tail", ""),
            **mr, **vr,
            "error": "" if vr.get("visible") else (vr.get("verify_error")
                                                   or "表已创建但问数账号读不到")}
