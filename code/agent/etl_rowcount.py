"""rowcount.tsv 的新鲜度维护 —— 让"声明行数"跟着数据走。

问题：`agent/rowcount.tsv` 是 sem_check 用来核对 tables.yaml 里 `rows:` 的实测值。
它是**某一刻**的快照：dev 表每天被 INSERT OVERWRITE 重刷，行数变了，快照就旧了；
sem_check 于是报"行数不一致"，人开始习惯性忽略这条告警——一条天天亮红灯的
检查等于没有检查。

两个入口，都用同一段写入逻辑：
  · 执行完建表（随时）→ `upsert({表: 行数})`，只回写刚跑过的那张表
  · 每日调度跑了一阵（或体检时）→ `refresh(cfg)`，用问数账号把**所有 dev 表**
    实测一遍，列出漂移量再回写

为什么实测走 Doris 而不是 Hive：hive CLI 一次会话启动 Spark 要 60s
（2026-11-11 实测），Doris 从 hive_catalog 读同一份数据 0.2s 出结果。
调度脚本在集群上跑、仓库在本机，中间没有回调通道——所以"随调度刷新"落成
**仓库侧的校准动作**，而不是让调度脚本去改仓库文件（那会把生产调度和开发机
的目录结构绑死）。
"""
from __future__ import annotations

import os
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.abspath(os.path.join(HERE, "..", ".."))
ROWCOUNT_TSV = os.path.join(ROOT, "agent", "rowcount.tsv")
TABLES_YAML = os.path.join(ROOT, "agent", "semantic", "tables.yaml")
GEN_TABLES = os.path.join(ROOT, "agent", "semantic", "gen_tables.py")
SEM_CHECK = os.path.join(ROOT, "agent", "semantic", "sem_check.py")
_CHUNK = 6          # 一条 SQL 里最多塞几张表，出错时还要能定位到具体表


def _read(path: str) -> str:
    try:
        with open(path, encoding="utf-8") as f:
            return f.read()
    except FileNotFoundError:
        return ""


def parse_tsv() -> dict[str, int]:
    out = {}
    for line in _read(ROWCOUNT_TSV).splitlines():
        p = line.strip().split("\t")
        if len(p) == 2 and p[1].isdigit():
            out[p[0]] = int(p[1])
    return out


def dev_tables(cfg: dict) -> list[str]:
    """要校准的表：语义层 layer=dev ∪ rowcount.tsv 里的 dev_ 行。

    为什么要并上 tsv：刚执行完还没登记语义层的表只会出现在快照里
    （执行路径会 upsert 一行），只读语义层会把它漏掉——而它恰恰是最容易
    过期的那张（新表、还在调口径）。
    """
    names: set[str] = set()
    try:
        import semantic
        tabs = semantic.all_tables(cfg) or {}
        names |= {t for t, v in tabs.items() if (v.get("layer") or "") == "dev"}
    except Exception:
        pass
    names |= {t for t in parse_tsv() if t.startswith("dev_")}
    return sorted(names)


def measure(cfg: dict, tables: list[str] | None = None) -> dict:
    """实测行数。先按块 UNION ALL，某块失败就逐表重试以定位是哪张表读不到。"""
    import db
    tables = list(tables or dev_tables(cfg))
    if not tables:
        return {"ok": True, "rows": {}, "failed": {}}
    got: dict[str, int] = {}
    failed: dict[str, str] = {}
    for i in range(0, len(tables), _CHUNK):
        chunk = tables[i:i + _CHUNK]
        sql = " UNION ALL ".join(
            "SELECT '%s' AS t, COUNT(*) AS n FROM %s" % (t, t) for t in chunk)
        r = db.run_sql(sql, cfg)
        if not r.get("error"):
            for row in (r.get("rows") or []):
                if len(row) >= 2 and str(row[1]).isdigit():
                    got[str(row[0])] = int(row[1])
            continue
        for t in chunk:                  # 逐表重试：一张表坏不该让整块失真
            r1 = db.run_sql("SELECT COUNT(*) FROM %s" % t, cfg)
            if r1.get("error"):
                failed[t] = str(r1["error"])[:160]
            else:
                got[t] = int((r1.get("rows") or [[0]])[0][0] or 0)
    return {"ok": True, "rows": got, "failed": failed}


def _yaml_rows() -> dict[str, int]:
    """从 tables.yaml 粗读每张表的 rows: 声明值（按两空格缩进的表名分块）。"""
    out, cur = {}, None
    for line in _read(TABLES_YAML).splitlines():
        if len(line) > 3 and line.startswith("  ") and not line.startswith("   ") \
                and line.rstrip().endswith(":") and not line.strip().startswith("-"):
            cur = line.strip().rstrip(":")
        elif cur and line.lstrip().startswith("rows:"):
            val = line.split(":", 1)[1].strip()
            if val.isdigit():
                out[cur] = int(val)
            cur = None
    return out


def plan(measured: dict[str, int]) -> list[dict]:
    """三方对照：语义层声明 / 行数快照 / 实测。"""
    tsv = parse_tsv()
    yml = _yaml_rows()
    out = []
    for t, n in sorted(measured.items()):
        declared = yml.get(t)
        snap = tsv.get(t)
        if declared is None:
            status = "未登记"            # 只在快照里（新表，还没进语义层）
        elif snap is None:
            status = "无快照"
        elif declared == n and snap == n:
            status = "一致"
        elif snap == n:
            status = "声明过期"          # 数据没变，tables.yaml 的 rows: 旧了
        else:
            status = "快照过期"          # 数据变了，快照要刷新
        out.append({"table": t, "measured": n, "declared": declared,
                    "snapshot": snap, "delta": None if snap is None else n - snap,
                    "status": status})
    return out


def _write_tsv(measured: dict[str, int]) -> int:
    lines = [ln for ln in _read(ROWCOUNT_TSV).splitlines() if ln.strip()]
    seen, out = set(), []
    for ln in lines:
        name = ln.split("\t")[0]
        if name in measured:
            if name in seen:
                continue
            seen.add(name)
            out.append("%s\t%d" % (name, measured[name]))
        else:
            out.append(ln)
    for name in sorted(measured):
        if name not in seen:
            out.append("%s\t%d" % (name, measured[name]))
    with open(ROWCOUNT_TSV, "w", encoding="utf-8") as f:
        f.write("\n".join(out) + "\n")
    return len(measured)


def _write_yaml_rows(measured: dict[str, int]) -> list[str]:
    """只改 `rows:` 那一行，其余字节原样保留（tables.yaml 有大量人工注释与顺序）。"""
    lines = _read(TABLES_YAML).splitlines()
    cur, changed = None, []
    for i, line in enumerate(lines):
        if len(line) > 3 and line.startswith("  ") and not line.startswith("   ") \
                and line.rstrip().endswith(":") and not line.strip().startswith("-"):
            cur = line.strip().rstrip(":")
            continue
        if cur and line.lstrip().startswith("rows:") and cur in measured:
            indent = line[:len(line) - len(line.lstrip())]
            old = line.split(":", 1)[1].strip()
            new = str(measured[cur])
            if old != new:
                lines[i] = "%srows: %s" % (indent, new)
                changed.append(cur)
            cur = None
    if changed:
        with open(TABLES_YAML, "w", encoding="utf-8") as f:
            f.write("\n".join(lines) + ("\n" if lines else ""))
    return changed


def upsert(measured: dict[str, int], regen: bool = True) -> dict:
    """回写快照 + 语义层声明，再重建 tables.json（问数提示词里的行数也靠它）。"""
    if not measured:
        return {"ok": True, "tables": 0, "yaml": [], "regen": None}
    n = _write_tsv(measured)
    changed = _write_yaml_rows(measured)
    reg = None
    if regen:
        reg = _run(GEN_TABLES)
        try:
            import semantic
            semantic.clear_cache()
        except Exception:
            pass
    return {"ok": bool(reg is None or reg.get("ok")), "tables": n,
            "yaml": changed, "regen": reg}


def refresh(cfg: dict, check: bool = True) -> dict:
    """全量校准：实测 → 对照 → 回写 → gen_tables（可选 sem_check）。"""
    m = measure(cfg)
    rows = m.get("rows") or {}
    drift = plan(rows)
    upd = upsert(rows)
    sem = _run(SEM_CHECK) if (check and upd.get("ok")) else None
    return {"ok": bool(upd.get("ok") and (sem is None or sem.get("ok"))),
            "measured": rows, "failed": m.get("failed") or {},
            "drift": drift, "changed_yaml": upd.get("yaml") or [],
            "regen": upd.get("regen"), "semcheck": sem}


def _run(script: str) -> dict:
    try:
        p = subprocess.run([sys.executable, script], capture_output=True,
                           text=True, encoding="utf-8", errors="replace",
                           timeout=180)
        out = (p.stdout or "") + (p.stderr or "")
        tail = "\n".join(ln for ln in out.splitlines() if ln.strip())[-1600:]
        return {"ok": p.returncode == 0, "tail": tail}
    except Exception as e:
        return {"ok": False, "tail": "执行 %s 失败：%s"
                % (os.path.basename(script), str(e)[:160])}
