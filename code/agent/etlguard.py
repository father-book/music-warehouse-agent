"""ETL 开发脚本护栏：engineer 让 agent 生成"写入开发表"的 SQL，这里负责验它。

背景：Doris 侧问数账号 `agent_ro` 只有 Select 权限（评审结论：问数通道必须只读）。
所以 ETL 开发走"生成 + 校验 + 登记"模式——agent 产出 `CREATE TABLE music_dw.dev_* AS SELECT`，
本模块在入库登记前做四道静态检查 + 一次 EXPLAIN 实证：

  1. 语句类型：只接受 CREATE TABLE（CTAS）。INSERT / DDL 其他一律不收，
     因为执行权不在这套系统手里，收进来的东西必须是"拿到写权限就能原样跑"的。
  2. 目标表：必须落在 `music_dw.dev_` 前缀下。开发表和生产表用前缀隔离，
     比"写在注释里说明这是开发表"可靠——后者拦不住手滑。
  3. 源表：除目标表外，所有引用表必须在语义层登记过。对着语义层之外的表
     写加工逻辑，十有八九是模型编出来的表名。
  4. 危险词：DROP / DELETE / TRUNCATE / ALTER / GRANT / REVOKE 出现即拒。
  5. EXPLAIN：把 CTAS 里的 SELECT 部分抽出来真跑一次 EXPLAIN。
     静态检查全绿但列名拼错的生成物并不少见，只有 EXPLAIN 能兜住。

所有检查都通过才允许 status=verified 入库；否则以 generated 状态存档，
前端把不过的项标红——生成失败也是工程过程的一部分，不该静默丢弃。
"""
from __future__ import annotations

import importlib.util
import re
import sys

import sqlglot
from sqlglot import exp

import semantic

# 目标表：music_dw 库（可省略库前缀）+ dev_ 前缀表名
_TARGET_RE = re.compile(
    r"^(?:music_dw\.)?dev_[a-z][a-z0-9_]{2,60}$", re.IGNORECASE)
# 这些词出现在任何位置都拒——它们不该出现在"建开发表"的诉求里
_DANGER_RE = re.compile(
    r"\b(drop|delete|truncate|alter|grant|revoke|create\s+user|set\s+global)\b",
    re.IGNORECASE)
# 三段式 catalog 前缀拒收：生成物的执行通道是 161 的 Hive 引擎（hive -f），
# Hive 不认识 hive_catalog. 前缀（那是 Doris 的 catalog 语法）。2026-11-11 实测：
# Doris 侧 EXPLAIN 对三段式放行，Hive 执行时 ParseException——校验通道和执行
# 通道语法不一致，必须在静态检查里卡住。
_CATALOG_RE = re.compile(r"\bhive_catalog\s*\.", re.IGNORECASE)


# ---------------- 口径机检（与问数 R25 共用同一份实现） ----------------
# 为什么放在这里：2026-11-09 的首版收入 CTAS 五检全绿，却漏了 VIP 订阅
# （低 5.24 倍），靠人工复核才拦下。"能跑"和"算得对"是两件事，前者已经由
# 四条静态检查 + EXPLAIN 兜住，后者此前完全没人管。
#
# 规则来自 metrics.yaml 的 caliber_guard，实现来自 agent/guards/caliber.py——
# 不复制第二份代码，否则问数和 ETL 两条链路的判定迟早会漂移。
_CAL = {"module": None, "tried": False}


def _caliber(cfg):
    """返回 (check 函数, 规则列表)；环境缺文件时返回 (None, [])，不阻塞主流程。"""
    if not _CAL["tried"]:
        _CAL["tried"] = True
        try:
            path = cfg["_paths"]["guard_script"].parent / "caliber.py"
            spec = importlib.util.spec_from_file_location("_caliber_guard", str(path))
            mod = importlib.util.module_from_spec(spec)
            sys.modules["_caliber_guard"] = mod
            spec.loader.exec_module(mod)
            _CAL["module"] = mod
        except Exception:
            _CAL["module"] = None       # 部署缺文件：口径机检降级为"未启用"，见 summary
    mod = _CAL["module"]
    if mod is None:
        return None, []
    return mod.check, semantic.caliber_rules(cfg)


def _run_caliber(tree, cfg):
    """跑口径机检，返回 (ok, findings)。ok 只统计 REJECT 级（当前无），WARN 不拦人。"""
    fn, specs = _caliber(cfg)
    if fn is None or not specs:
        return True, []
    try:
        fs = fn([tree], specs)
    except Exception as e:
        return False, [{"level": "WARN", "rule": "R25-口径-检查失败",
                        "message": "口径机检执行异常：%s" % str(e)[:120],
                        "hint": "先人工复核金额与去重口径"}]
    ok = not any((f.get("level") or "WARN") == "REJECT" for f in fs)
    return ok, fs


def _extract_query(tree) -> str | None:
    q = tree.args.get("query") or tree.args.get("expression")
    try:
        return q.sql("hive") if q is not None else None
    except Exception:
        return None


def validate(sql: str, cfg: dict) -> dict:
    """返回逐项检查结果。静态项失败时跳过 EXPLAIN（没有意义）。"""
    out = {"stmt_ok": False, "target_ok": False, "target": "",
           "tables_ok": False, "bad_tables": [], "danger_ok": False,
           "danger": [], "explain_ok": False, "explain_error": "",
           "caliber_ok": True, "caliber": []}
    sql = (sql or "").strip().rstrip(";")
    if not sql:
        out["explain_error"] = "空 SQL"
        return out

    # 4) 危险词（先于解析，解析失败也能给出这条结论）
    m = _DANGER_RE.search(sql)
    out["danger_ok"] = m is None
    if m:
        out["danger"].append(m.group(0).upper())
    out["catalog_ok"] = _CATALOG_RE.search(sql) is None

    # 1) 语句类型 + 解析
    try:
        tree = sqlglot.parse_one(sql, read="hive")
    except Exception as e:
        out["explain_error"] = "无法解析：%s" % str(e)[:160]
        return out
    out["stmt_ok"] = isinstance(tree, exp.Create) and \
        str(tree.args.get("kind") or "").upper() == "TABLE"
    if not out["stmt_ok"]:
        out["explain_error"] = "只接受 CREATE TABLE ... AS SELECT 单语句"
        return out
    if not out["catalog_ok"]:
        out["explain_error"] = ("表名不允许带 hive_catalog. 前缀（执行通道是 Hive 引擎，"
                                "只认两段式 music_dw.<表名>）")
        return out

    # 2) 目标表
    target = tree.this
    tname = ""
    try:
        tname = ".".join(p.name for p in target.find_all(exp.Table)) if target else ""
        if not tname and isinstance(target, exp.Schema):
            tname = target.this.name if target.this is not None else ""
        if not tname:
            # CREATE TABLE t (...) 时 this 直接是 Table 或 Schema
            tname = target.name if isinstance(target, exp.Table) else ""
    except Exception:
        tname = ""
    if not tname:
        # 兜底：正则从原文里抓第一个 dev_ 表名
        m2 = re.search(r"CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?"
                       r"((?:music_dw\.)?dev_[a-z][a-z0-9_]*)", sql, re.IGNORECASE)
        tname = m2.group(1) if m2 else ""
    out["target"] = tname
    out["target_ok"] = bool(_TARGET_RE.match(tname or ""))

    # 3) 源表白名单
    known = set(semantic.all_tables(cfg).keys())
    bad = []
    for t in tree.find_all(exp.Table):
        name = t.name
        if not name or name.lower() == (tname.split(".")[-1].lower() if tname else ""):
            continue
        if name.lower() not in {k.lower() for k in known}:
            bad.append(name)
    out["bad_tables"] = sorted(set(bad))
    out["tables_ok"] = out["target_ok"] and not out["bad_tables"]

    # 4.5) 口径机检：只证明「能跑」不够，还要看「算得对不对」
    out["caliber_ok"], out["caliber"] = _run_caliber(tree, cfg)

    # 5) EXPLAIN 实证 SELECT 部分
    if out["stmt_ok"] and out["target_ok"] and out["danger_ok"] and out["catalog_ok"]:
        q = _extract_query(tree)
        if not q:
            out["explain_error"] = "CREATE TABLE 里没有抽出 SELECT（可能不是 CTAS 写法）"
            return out
        from db import run_sql  # 延迟导入：selftest 不一定有库
        try:
            r = run_sql("EXPLAIN " + q, cfg)
            out["explain_ok"] = not r.get("error")
            if r.get("error"):
                out["explain_error"] = str(r["error"])[:220]
        except Exception as e:  # 环境无库时不算生成物的问题
            out["explain_ok"] = False
            out["explain_error"] = "EXPLAIN 执行失败：%s" % str(e)[:160]
    return out


def _caliber_lines(checks: dict) -> list[str]:
    """口径机检的结论行。前缀刻意用「告警」——前端据此标黄，不标红：
    它不拦执行，只提醒"这条 SQL 能跑，但可能不是你要的那个数"。"""
    fs = checks.get("caliber") or []
    if not checks.get("caliber_ok", True):
        return ["不通过：口径机检执行失败，金额/去重口径未验证"]
    if not fs:
        return ["通过：口径机检未发现事件覆盖 / 退款符号 / 源表风险"]
    msgs = "；".join(str(f.get("message") or "") for f in fs[:2])
    return ["告警：口径机检 %d 项 —— %s" % (len(fs), msgs)]


def summarize(checks: dict) -> list[str]:
    """给前端/自测看的短结论列表。"""
    lines = []
    lines.append(("通过" if checks.get("stmt_ok") else "不通过")
                 + "：语句类型为 CREATE TABLE ... AS SELECT")
    t = checks.get("target") or "—"
    lines.append(("通过" if checks.get("target_ok") else "不通过")
                 + f"：目标表 {t} 落在 music_dw.dev_ 前缀下")
    if checks.get("bad_tables"):
        lines.append("不通过：引用了语义层之外的表 " + "、".join(checks["bad_tables"]))
    else:
        lines.append("通过：源表全部在语义层登记")
    if not checks.get("danger_ok"):
        lines.append("不通过：出现危险词 " + "、".join(checks.get("danger") or []))
    if not checks.get("catalog_ok", True):
        lines.append("不通过：表名带 hive_catalog. 前缀（执行通道是 Hive 引擎，只认两段式）")
    lines.extend(_caliber_lines(checks))
    if checks.get("explain_ok"):
        lines.append("通过：SELECT 部分 EXPLAIN 实证可执行")
    elif checks.get("explain_error"):
        lines.append("未通过实证：" + checks["explain_error"])
    return lines


# ---------------- 每日调度 SQL（INSERT OVERWRITE 幂等重刷） ----------------
# 调度形态的护栏与建表不同：接受 INSERT，但必须 OVERWRITE（重跑幂等）、
# 时间上界必须是 ${hivevar:bizdate} 参数（调度器按天传参）、目标仍是 dev_ 表。
_BIZDATE_RE = re.compile(r"\$\{(?:hivevar:)?bizdate\}")


def _substitute_bizdate(sql: str, sample: str = "2026-11-08") -> str:
    return _BIZDATE_RE.sub(sample, sql)


def validate_daily(sql: str, cfg: dict) -> dict:
    """每日重刷 SQL 校验。返回项与建表版对齐，外加 overwrite_ok / param_ok。"""
    out = {"stmt_ok": False, "target_ok": False, "target": "",
           "tables_ok": False, "bad_tables": [], "danger_ok": False,
           "danger": [], "overwrite_ok": False, "param_ok": False,
           "catalog_ok": True, "explain_ok": False, "explain_error": "",
           "caliber_ok": True, "caliber": []}
    sql = (sql or "").strip().rstrip(";")
    if not sql:
        out["explain_error"] = "空 SQL"
        return out

    m = _DANGER_RE.search(sql)
    out["danger_ok"] = m is None
    if m:
        out["danger"].append(m.group(0).upper())
    out["param_ok"] = bool(_BIZDATE_RE.search(sql))
    out["catalog_ok"] = _CATALOG_RE.search(sql) is None

    try:
        tree = sqlglot.parse_one(sql, read="hive")
    except Exception as e:
        out["explain_error"] = "无法解析：%s" % str(e)[:160]
        return out
    out["stmt_ok"] = isinstance(tree, exp.Insert)
    if not out["stmt_ok"]:
        out["explain_error"] = "只接受 INSERT（OVERWRITE）TABLE 单语句"
        return out
    if not out["catalog_ok"]:
        out["explain_error"] = ("表名不允许带 hive_catalog. 前缀（执行通道是 Hive 引擎，"
                                "只认两段式 music_dw.<表名>）")
        return out
    out["overwrite_ok"] = bool(tree.args.get("overwrite"))
    if not out["overwrite_ok"]:
        out["explain_error"] = "必须是 INSERT OVERWRITE（INTO 追加在重跑时会重复计数）"
        return out

    # 目标表：INSERT 的第一张表；源表白名单排除它
    tables = list(tree.find_all(exp.Table))
    tname = tables[0].name if tables else ""
    if tables and tables[0].db:
        tname = tables[0].db + "." + tname
    out["target"] = tname
    out["target_ok"] = bool(_TARGET_RE.match(tname or ""))
    known = set(semantic.all_tables(cfg).keys())
    bad = []
    for t in tables[1:]:
        if t.name and t.name.lower() not in {k.lower() for k in known} \
                and t.name.lower() != (tname.split(".")[-1].lower()):
            bad.append(t.name)
    out["bad_tables"] = sorted(set(bad))
    out["tables_ok"] = out["target_ok"] and not out["bad_tables"]

    # 每日重刷与首建表是同一段 SELECT，口径风险同样要查（改了口径的重刷更危险：
    # 表已经挂在看板上，数字悄悄变了没人会发现）
    out["caliber_ok"], out["caliber"] = _run_caliber(tree, cfg)

    if out["danger_ok"] and out["target_ok"] and out["tables_ok"] and out["catalog_ok"]:
        try:
            q = tree.expression.sql("hive") if tree.expression is not None else None
        except Exception:
            q = None
        if not q:
            out["explain_error"] = "INSERT 里没有抽出 SELECT"
            return out
        from db import run_sql
        try:
            r = run_sql("EXPLAIN " + _substitute_bizdate(q), cfg)
            out["explain_ok"] = not r.get("error")
            if r.get("error"):
                out["explain_error"] = str(r["error"])[:220]
        except Exception as e:
            out["explain_ok"] = False
            out["explain_error"] = "EXPLAIN 执行失败：%s" % str(e)[:160]
    return out


def summarize_daily(checks: dict) -> list[str]:
    lines = []
    lines.append(("通过" if checks.get("stmt_ok") else "不通过")
                 + "：语句类型为 INSERT（OVERWRITE）TABLE 单语句")
    t = checks.get("target") or "—"
    lines.append(("通过" if checks.get("target_ok") else "不通过")
                 + f"：目标表 {t} 落在 music_dw.dev_ 前缀下")
    lines.append(("通过" if checks.get("overwrite_ok") else "不通过")
                 + "：使用 OVERWRITE，同一天重跑结果幂等")
    lines.append(("通过" if checks.get("param_ok") else "不通过")
                 + "：时间上界为 ${hivevar:bizdate} 参数，调度器按天传参")
    if checks.get("bad_tables"):
        lines.append("不通过：引用了语义层之外的表 " + "、".join(checks["bad_tables"]))
    else:
        lines.append("通过：源表全部在语义层登记")
    if not checks.get("danger_ok"):
        lines.append("不通过：出现危险词 " + "、".join(checks.get("danger") or []))
    if not checks.get("catalog_ok", True):
        lines.append("不通过：表名带 hive_catalog. 前缀（执行通道是 Hive 引擎，只认两段式）")
    lines.extend(_caliber_lines(checks))
    if checks.get("explain_ok"):
        lines.append("通过：SELECT 部分 EXPLAIN 实证可执行")
    elif checks.get("explain_error"):
        lines.append("未通过实证：" + checks["explain_error"])
    return lines
