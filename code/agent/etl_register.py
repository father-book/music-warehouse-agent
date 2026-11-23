"""ETL 生成物 → 语义层登记（半自动：出草案 → 机检 → 人工确认入库）。

为什么不让 agent 直接写语义层：
  语义层是问数链路的口径权威。它一旦被模型自动写入，"护栏"就变成了"模型自己
  给自己发通行证"。2026-11-11 补省份销售表那次，登记是五步手工活（DDL 文件、
  tables.yaml、metrics、glossary、rowcount.tsv），漏一步下游就查不到。这里把
  其中**机械的四步**自动化，把**需要人判断的一步**（口径是否被正确表达、要不要
  新增指标/词条）留在草案里显式提示。

登记一个 dev 表需要落四个地方（与手工流程完全一致，只是由代码写）：
  1. sql/offline/15_dev_tables.sql   —— CREATE EXTERNAL TABLE，tables.json 的列结构来源
  2. agent/semantic/tables.yaml      —— 策展知识（layer/name_cn/grain/caveats）
  3. agent/rowcount.tsv              —— 实测行数，sem_check 用它比对声明值
  4. gen_tables.py + sem_check.py    —— 重新生成 tables.json 并做一致性检查

每次写入前都重建草案并复核冲突，不信任前端回传内容。
"""
from __future__ import annotations

import datetime as _dt
import os
import re
import subprocess
import sys

import etl_exec
import etl_rowcount

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.abspath(os.path.join(HERE, "..", ".."))
SEM_DIR = os.path.join(ROOT, "agent", "semantic")
TABLES_YAML = os.path.join(SEM_DIR, "tables.yaml")
DDL_FILE = os.path.join(ROOT, "sql", "offline", "15_dev_tables.sql")
GEN_TABLES = os.path.join(SEM_DIR, "gen_tables.py")
SEM_CHECK = os.path.join(SEM_DIR, "sem_check.py")

_NAME_RE = re.compile(r"^dev_[a-z][a-z0-9_]{2,60}$")
_TS_TYPE = {"128": "STRING"}


def _read(path: str) -> str:
    try:
        with open(path, encoding="utf-8") as f:
            return f.read()
    except FileNotFoundError:
        return ""


def _yaml_tables() -> dict:
    """读 tables.yaml 的 tables 段（只读，用于冲突检查）。"""
    try:
        import yaml
        d = yaml.safe_load(_read(TABLES_YAML)) or {}
        return d.get("tables") or {}
    except Exception:
        return {}


def _ddl_has(name: str) -> bool:
    return re.search(r"CREATE\s+EXTERNAL\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?"
                     r"(?:music_dw\.)?%s\b" % re.escape(name),
                     _read(DDL_FILE), re.I) is not None


def hive_type(doris_type: str) -> str:
    """Doris DESCRIBE 的类型 → Hive DDL 类型。认不出的按 STRING 兜底。"""
    t = (doris_type or "").lower().strip()
    if t.startswith("varchar") or t.startswith("char") or t.startswith("text"):
        return "STRING"
    if t.startswith("decimalv3"):
        m = re.search(r"\((\d+)\s*,\s*(\d+)\)", t)
        return "DECIMAL(%s,%s)" % (m.group(1), m.group(2)) if m else "DECIMAL(18,2)"
    if t.startswith("decimal"):
        return t.replace("decimalv3", "decimal").upper()
    if t in ("bigint", "int", "smallint", "tinyint", "double", "float", "boolean"):
        return t.upper()
    if t in ("date", "datetime"):
        return t.upper()
    return "STRING"


def draft(script: dict, cfg: dict) -> dict:
    """生成登记草案：列结构取实测表，行数/时间范围取实测值，冲突就地检查。"""
    checks_in = script.get("checks") or {}
    if isinstance(checks_in, str):
        import json
        try:
            checks_in = json.loads(checks_in)
        except ValueError:
            checks_in = {}
    target = (script.get("target_table") or checks_in.get("target") or "").strip()
    t = target.split(".")[-1]
    if not t:
        return {"ok": False, "error": "这条记录没有目标表，无法登记"}
    cols = etl_exec.describe_table(t, cfg)
    if not cols:
        return {"ok": False, "error": "读不到表结构：先执行建表（问数账号看不到这张表）"}
    stat = etl_exec.verify_table(t, cfg)
    if not stat.get("visible"):
        return {"ok": False, "error": "表结构可读但数据读不到：%s"
                                     % (stat.get("verify_error") or "未知原因")}

    rows = int(stat.get("rows") or 0)
    dt_min, dt_max = stat.get("dt_min", ""), stat.get("dt_max", "")
    existing = _yaml_tables()
    has_dt = any(c["name"].lower() == "dt" for c in cols)
    # 粒度只由**字符串维度列**构成；decimal/bigint 是度量，进了粒度会把
    # "每省每天 Top20" 错写成"每省每天每金额 Top20"（2026-11-11 踩过）。
    dims = [c["name"] for c in cols
            if c["name"].lower() != "dt"
            and hive_type(c.get("type")) == "STRING"][:2]
    grain = ("天" if has_dt else "")
    grain = (grain + " × " + " × ".join(dims)) if dims else (grain or "—")
    title = (script.get("title") or t).strip()
    req = (script.get("requirement") or "").strip()
    today = _dt.date.today().isoformat()

    caveats = [
        "由 Agent ETL 流程生成、etlguard 校验、执行后回读（%s，%d 行）" % (today, rows),
    ]
    if dt_min and dt_max:
        caveats.append("数据窗口 %s ~ %s；源表更新后由每日调度脚本重刷" % (dt_min, dt_max))
    if req:
        caveats.append("业务口径来源：%s" % req[:160])
    caveats.append("列结构由执行后 DESCRIBE 实测生成；口径若要成为命名指标，"
                   "需在 metrics.yaml 补指标定义（会进入口径评测）")

    yaml_text = _yaml_entry(t, title, grain, dims, has_dt, rows, caveats)
    ddl_text = _ddl_entry(t, title, cols)
    checks = [
        {"name": "目标表已执行且问数账号可读", "ok": True,
         "detail": "实测 %d 行" % rows},
        {"name": "表名前缀合规", "ok": bool(_NAME_RE.match(t)),
         "detail": t},
        {"name": "语义层无同名表", "ok": t not in existing,
         "detail": "已登记" if t in existing else "无冲突"},
        {"name": "DDL 文件无同名定义", "ok": not _ddl_has(t),
         "detail": "已存在" if _ddl_has(t) else "无冲突"},
        {"name": "实测有数据", "ok": rows > 0,
         "detail": "%d 行" % rows},
        {"name": "列数量 ≥ 2", "ok": len(cols) >= 2,
         "detail": "%d 列" % len(cols)},
    ]
    return {"ok": all(c["ok"] for c in checks), "target": t,
            "columns": cols, "rows": rows, "dt_min": dt_min, "dt_max": dt_max,
            "yaml": yaml_text, "ddl": ddl_text, "checks": checks,
            "note": "确认入库只登记表（可被问数召回）；命名指标与口语词条需人工补 "
                    "metrics.yaml / glossary.yaml，因为它们要进口径评测。"}


def _yaml_entry(t: str, title: str, grain: str, dims: list[str],
                has_dt: bool, rows: int, caveats: list[str]) -> str:
    esc = lambda s: str(s).replace('"', "'")  # noqa: E731
    pk = (["dt"] if has_dt else []) + dims
    lines = [f"  {t}:",
             "    layer: dev",
             f"    name_cn: {title}",
             f"    grain: {grain}",
             f"    pk: [{', '.join(pk)}]",
             "    pk_unique: true",
             "    versions: none",
             "    partition: none",
             f"    rows: {rows}",
             f"    path: /warehouse/music_dw/{t}",
             "    catalog: hive_catalog",
             "    database: music_dw",
             "    caveats:"]
    lines += [f'      - "{esc(c)}"' for c in caveats]
    return "\n".join(lines) + "\n"


def _ddl_entry(t: str, title: str, cols: list[dict]) -> str:
    body = []
    for c in cols:
        body.append("    %-20s %s" % (c["name"], hive_type(c.get("type"))))
    return ("\nCREATE EXTERNAL TABLE IF NOT EXISTS music_dw.%s\n(\n%s\n)\n"
            "COMMENT 'DEV-%s（Agent ETL 生成）'\n"
            "STORED AS TEXTFILE\n"
            "LOCATION '/warehouse/music_dw/%s';\n"
            % (t, ",\n".join(body), title.replace("'", " ")[:80], t))


def commit(script: dict, cfg: dict) -> dict:
    """确认入库：重建草案 → 复核冲突 → 四处写入 → gen_tables + sem_check。"""
    d = draft(script, cfg)
    if not d.get("ok"):
        bad = [c for c in d.get("checks", []) if not c["ok"]]
        return {"ok": False, "error": d.get("error")
                or "机检未通过：" + "；".join(c["name"] for c in bad),
                "checks": d.get("checks", [])}
    t = d["target"]

    # 1) DDL 文件追加
    today = _dt.date.today().isoformat()
    with open(DDL_FILE, "a", encoding="utf-8") as f:
        f.write("\n-- ---- %s（Agent 登记于 %s）----\n" % (t, today))
        f.write(d["ddl"])

    # 2) tables.yaml 追加
    with open(TABLES_YAML, "a", encoding="utf-8") as f:
        f.write("\n" + d["yaml"])

    # 3) rowcount.tsv（幂等：先删同名行再写）。行数写入统一走 etl_rowcount，
    #    执行路径与登记路径用同一段代码，避免两处口径不一致。
    rc = etl_rowcount.upsert({t: d["rows"]}, regen=False)

    # 4) 重新生成 tables.json + 一致性检查
    regen = _run(GEN_TABLES)
    sem = _run(SEM_CHECK) if regen["ok"] else {"ok": False, "tail": "跳过：gen_tables 失败"}

    # 5) 登记即授权：seed 是把层展开成行落库的，新表不授权等于"查不了"。
    #    放在 gen_tables 之后——_tables_with_column 读的是 tables.json。
    grant = {"granted": [], "skipped": [], "scoped": [], "error": "跳过：gen_tables 失败"}
    if regen["ok"]:
        from governance import identity
        grant = identity.grant_table(t, "dev", cfg)

    ok = bool(regen["ok"] and sem["ok"] and not grant.get("error"))
    return {"ok": ok, "target": t, "rows": d["rows"],
            "regen": regen, "semcheck": sem, "grant": grant, "rowcount": rc,
            "error": "" if ok else "登记已写入但校验/授权未过，见输出"}


def _run(script: str) -> dict:
    try:
        p = subprocess.run([sys.executable, script], capture_output=True,
                           text=True, encoding="utf-8", errors="replace",
                           timeout=180)
        out = (p.stdout or "") + (p.stderr or "")
        tail = "\n".join(ln for ln in out.splitlines() if ln.strip())[-1600:]
        return {"ok": p.returncode == 0, "tail": tail}
    except Exception as e:
        return {"ok": False, "tail": "执行 %s 失败：%s" % (os.path.basename(script),
                                                         str(e)[:160])}
