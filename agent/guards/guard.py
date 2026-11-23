#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""27 条护栏 —— SQL 静态检查器。只读，不改任何东西。

为什么用 AST 而不是正则：
  `SELECT count(*) FROM dwd_play_start WHERE dt>'2026-10-01'` 里有没有 DROP、
  有没有 AVG(complete_rate)、JOIN 带不带 ON，正则要写几十条互相干扰的模式，
  而且注释和字符串里的关键字一定会误伤。sqlglot 解析成树之后逐节点判断，
  规则之间互不干扰。

用法：
  python3 guard.py --mode dev   review/R001/02_ddl.sql      # 开发模式（允许 DDL）
  python3 guard.py --mode query "select dt, dau from ads_daily_overview limit 10"

退出码：0=全部通过；1=有 REJECT；2=只有 WARN。
"""
import argparse
import json
import os
import re
import sys

import sqlglot
from sqlglot import exp

HERE = os.path.dirname(os.path.abspath(__file__))
# tables.json 由 semantic/gen_semantic.py 生成，放在 agent/ 根目录
META = json.load(open(os.path.join(HERE, "..", "tables.json"), encoding="utf-8"))

TABLES = META["tables"]                 # {表名: {layer, partitioned_by, columns[]}}
DEV_LIBS = {"music_dev", "music_dev_offline"}
PROD_LIBS = {"music_dw", "music_offline"}
WEIGHTED = set(META.get("weighted_columns", []))   # 不可跨期求平均的比率型字段

# 2026-11-09 新增：由 semantic/gen_tables.py 从 tables.yaml / metrics.yaml 合并进来。
# guard.py 仍然只读 tables.json 一个文件——策展知识在生成阶段合并，
# 免得集群上出现"拷三个文件且版本必须配对"的漂移面。
PK_NON_UNIQUE = set(META.get("pk_non_unique") or [])     # 主键不唯一的表（拉链/流水）
VERSIONED = set(META.get("versioned_tables") or [])      # 拉链表 + 变更流水
ADDITIVITY = META.get("column_additivity") or {}         # {列: full|semi|stock|none}
REL_EDGES = META.get("relationships") or []              # 已批准的 JOIN 边
CALIBER = META.get("caliber_guard") or []                # 口径机检规则（R25）
# 2026-11-11 新增：实时链路历史快照（realtime.yaml）。R17 按这份白名单精确
# 放行 internal.music_dw.<表>，R26 保证查询落在时间窗内、且不与离线表混查。
RT_META = META.get("realtime") or {}
RT_TABLES = set(RT_META.get("tables") or {})
RT_TIME_COL = {t: (d.get("time_col") or "").lower()
               for t, d in (RT_META.get("tables") or {}).items()}
RT_ALT_COL = {t: (d.get("alt_time_col") or "").lower()
              for t, d in (RT_META.get("tables") or {}).items()}

# 收敛条件：命中任意一个，说明 JOIN 到拉链表后已把版本收敛成单行
CONVERGE_COLS = {"is_current", "start_date", "end_date"}

REJECT, WARN, INFO = "REJECT", "WARN", "INFO"
findings = []

# ---------------------------------------------------------------------------
# 阶段 1：安全护栏的上下文，由 Agent 侧通过 --context 传入（临时 JSON 文件）。
# 不带 --context 时（CI 的 test_cases.sh 直接调）CONTEXT 为空，
# R21 只做探测对象拦截，R23 只扫 SQL 文本，保证旧用例语义不变。
# ---------------------------------------------------------------------------
SECURITY = None
CONTEXT = {}


def load_security():
    global SECURITY
    if SECURITY is not None:
        return SECURITY
    try:
        with open(os.path.join(HERE, "security_patterns.json"), encoding="utf-8") as f:
            SECURITY = json.load(f)
    except Exception:
        SECURITY = {"pii_columns": [], "probe_objects": [],
                    "injection_patterns": [], "injection_words_zh": [],
                    "pii_words_cn": []}
    return SECURITY


def _tname(t):
    """sqlglot 6.3 与 30.x 都能取到裸表名。"""
    return str(getattr(t, "name", "") or "").lower()

# ---------------------------------------------------------------------------
# 同一批次 DDL 里 ALTER TABLE ... ADD COLUMNS 新增的列。
#
# 为什么需要：正常流程是「先审 02_ddl.sql 加列，再审 04_verify.sql 引用该列」。
# 而 tables.json 是从 sql/offline/*.sql 生成的**正式库**目录，
# 不含还没上线的列。若不认，R4 会把「Agent 自己刚加的列」判成幻觉，
# 于是任何"加字段"类需求都会被自己的护栏挡住——那是误报，不是安全。
#
# 用正则而不是 AST：sqlglot 6.3 对 Hive 的 ADD COLUMNS 支持不完整，
# 这里匹配的是我们自己生成的固定写法，范围可控。
# ---------------------------------------------------------------------------
EXTRA_COLS = set()
_ADD_COLS_RE = re.compile(r"(?i)add\s+columns?\s*\(([^)]*)\)")
_IDENT_RE = re.compile(r"^\s*([A-Za-z_][A-Za-z_0-9]*)")


def collect_added_columns(sql):
    """把本批次 DDL 新增的列登记进 EXTRA_COLS，供 R4 识别。"""
    for m in _ADD_COLS_RE.finditer(sql):
        for part in m.group(1).split(","):
            mm = _IDENT_RE.match(part)
            if mm:
                EXTRA_COLS.add(mm.group(1).lower())
    return EXTRA_COLS


# ---------------------------------------------------------------------------
# R11 的兜底扫描：按「语句首词」识别写操作。
# 存在的原因见 rule11 里的注释（sqlglot 6.3 把 ALTER ADD COLUMNS 解析成 Command）。
# 只匹配语句**开头**的写动作，所以 `FROM music_dw.x` 不会被误判。
# ---------------------------------------------------------------------------
RAW_SQL = ""

_WRITE_HEAD_RE = re.compile(
    r"(?i)\A\s*("
    r"alter\s+table"
    r"|insert\s+overwrite(?:\s+table)?"
    r"|insert\s+into(?:\s+table)?"
    r"|update"
    r"|delete\s+from"
    r"|drop\s+table(?:\s+if\s+exists)?"
    r"|create\s+external\s+table(?:\s+if\s+not\s+exists)?"
    r"|create\s+table(?:\s+if\s+not\s+exists)?"
    r"|truncate\s+table"
    r")\s+([a-z_0-9]+)\.([a-z_0-9]+)")


def _split_statements(sql):
    """按分号切语句，先剥掉整行注释，避免 `-- ALTER ...` 这类注释被当真。"""
    lines = [l for l in sql.splitlines() if not l.strip().startswith("--")]
    body = "\n".join(lines)
    return [s for s in body.split(";") if s.strip()]


def _node(name):
    """按名字取 sqlglot 节点类，取不到返回 None。

    集群是 Python 3.6，只能用 sqlglot 6.3（新版要求 3.7+ 的
    `from __future__ import annotations`，且需要 dataclasses）。
    6.3 与 18.x 的节点集合不完全一样（例如没有 TruncateTable），
    这里统一用 getattr 兜底，避免"换个版本就 ImportError"。
    """
    return getattr(exp, name, None)


def add(level, rule, msg, hint="", fix=None):
    """登记一条发现。

    fix 是**机器可执行**的修法，不是提示语。这是 2026-11-09 新增的契约：
      {"op": "replace" | "add_condition" | "restructure" | "annotate" | "apply_hint",
       "target": 要改的那段（SQL 片段或列名）,
       "replacement": 改成什么}

    为什么值得单独占一个字段：Agent 的循环是 draft -> check -> fix -> check。
    若只给一句人话提示，模型要自己把提示翻译成 SQL，翻译错就白跑一轮。
    sqlsure 的做法是每条违规都带 fix，官方称"照 fix 原样改"10/10 通过。

    老规则没写 fix 时自动从 hint 派生（op=apply_hint），保证 REJECT 一定有 fix；
    新规则（R17~R20）写的是结构化 fix，可直接照着替换。
    """
    if fix is None and level == REJECT:
        fix = ({"op": "apply_hint", "target": rule,
                "replacement": hint or "按提示改写"}
               if hint else {"op": "review", "target": rule,
                             "replacement": "请人工确认这条查询的语义"})
    findings.append((level, rule, msg, hint, fix))


def layer_of(tname):
    return TABLES.get(tname.lower(), {}).get("layer", "")


# R0 的修法是 deny 而不是 apply_hint（2026-11-16 补）：
# 语法崩坏不是"写法不合规"，模型拿着"再改改"的提示只会再造一条同样崩的 SQL，
# 白烧两轮调用；而 add() 对没写 fix 的 REJECT 派生的是 op=review，
# node_validate 的 _hard_block() 只认 deny，于是它还会被送进重试环。
# 判据一个字没改：解析失败仍然是 REJECT，只是把"打回重写"改成"直接拒答"。
_PARSE_HINT = ("这不是一条语法成立的 SQL。不要改写重试：同一句话再写一遍通常还是崩，"
               "直接按拒答处理，或请用户换个问法。")
_PARSE_FIX = {"op": "deny", "target": "unparsable_sql",
              "replacement": "拒答：SQL 语法不成立，无法执行"}


def parse(sql):
    # sqlglot 6.3 的 hive/spark 方言都不接受 `CREATE EXTERNAL TABLE`
    # （实测报 "Expected TABLE, VIEW, INDEX, FUNCTION, or PROCEDURE"）。
    # 去掉 EXTERNAL 再解析，AST 结构完全一致；本来就是外部表的事实不影响任何一条规则。
    norm = re.sub(r"(?i)\bcreate\s+external\s+table", "create table", sql)
    try:
        stmts = sqlglot.parse(norm, read="hive")
    except Exception as e:
        add(REJECT, "R0-parse", "SQL 解析失败：%s" % str(e)[:120],
            _PARSE_HINT, dict(_PARSE_FIX))
        return []

    # 空语句 / 纯注释会让 sqlglot 返回 None 而不是抛异常：
    #   sqlglot.parse("", read="hive")        -> [None]
    #   sqlglot.parse("SELECT 1;;", ...)      -> [Select, None]
    # 原样交给规则会在 _derived_names(None) 上炸 AttributeError，
    # guardtool 只能按 GUARD_CRASH fail-closed —— 判定没错但 trace 里是 Traceback。
    # 这里仍然判 REJECT（不放宽：以前崩了也是拒），只是换成能读懂的原因。
    if any(s is None for s in stmts):
        add(REJECT, "R0-parse",
            "SQL 解析后出现空语句（第 %d 条为空）" % (stmts.index(None) + 1),
            _PARSE_HINT, dict(_PARSE_FIX))
    return [s for s in stmts if s is not None]


# ─────────────────────────── 问数模式（1~10）───────────────────────────
READ_OK = tuple(n for n in (_node("Select"), _node("Union"), _node("With"),
                            _node("Subquery")) if n)


def rule1_select_only(stmts, mode):
    """问数模式只接受读语句。

    这里的 fix 必须是 **deny 而不是 hint**：`add()` 对没写 fix 的 REJECT 会自动
    派生 `apply_hint`，于是 DELETE 会被送进 C 阶段"换个写法再试"的闭环，模型
    把 `DELETE FROM dws_user_daily` 改成一条 SELECT 就算"修好了"——用户要删数据，
    系统回一个数字。那不是修复，是曲解意图，而且让写操作白白获得三次尝试机会。
    实测（2026-11-10）工程师账号发 DELETE 就是这样被改写后执行的。
    """
    if mode != "query":
        return
    for s in stmts:
        if not isinstance(s, READ_OK):
            add(REJECT, "R1-只读", "问数模式首关键字必须是 SELECT/WITH，实际是 %s"
                % type(s).__name__,
                "本系统只读，不执行任何写操作",
                {"op": "deny", "target": "write_statement",
                 "replacement": "改为查询：请说明要查的指标、时间与维度"})


def rule2_single_statement(stmts, mode):
    if mode == "query" and len(stmts) > 1:
        add(REJECT, "R2-单语句", "问数模式只允许一条语句，实际 %d 条" % len(stmts),
            "一次只回答一个问题",
            {"op": "deny", "target": "multi_statement",
             "replacement": "拆成单条查询，逐个提问"})


def _derived_names(stmt) -> set[str]:
    """CTE 名 + 子查询别名：它们会出现在 FROM 里，但不是实体表。

    2026-11-11 实测误伤：模型对"排名类"问题自然写出
      WITH user_prov AS (...), ranked AS (...) SELECT ... FROM ranked
    `FROM ranked` 在 AST 里是 Table 节点，R3 判成"表白名单外"、R21 判成
    "越权访问"，整条正确 SQL 被拒。CTE 定义体内的真实表仍会被遍历检查，
    跳过别名不放宽白名单。
    """
    names = set()
    for cte in stmt.find_all(exp.CTE):
        if cte.alias:
            names.add(str(cte.alias).lower())
    for sub in stmt.find_all(exp.Subquery):
        if sub.alias:
            names.add(str(sub.alias).lower())
    return names


def rule3_table_whitelist(stmts, mode):
    for s in stmts:
        derived = _derived_names(s)
        for t in s.find_all(exp.Table):
            name = t.name.lower()
            if not name or name.startswith("information_schema"):
                continue
            if name in derived:
                continue
            # music_dev / music_dev_offline 里的表是 Agent 自己新建的，
            # 天然不在正式表清单里；白名单只用来约束「读哪些正式表」。
            if _table_db(t) in DEV_LIBS:
                continue
            if name not in TABLES:
                add(REJECT, "R3-表白名单",
                    "表 `%s` 不在元数据白名单内（共 %d 张）" % (t.name, len(TABLES)),
                    "可用表见 tables.json；不要访问系统库")


def rule4_no_hallucinated_column(stmts, mode):
    """宽松版：列名必须在全部表的列并集里出现过。不做表-列精确绑定，
    因为别名/CTE 会让精确绑定误报。目标是拦住「LLM 编出来的字段」。"""
    known = set()
    for t in TABLES.values():
        known.update(c.lower() for c in t["columns"])
    # 本批次 DDL 刚加的列也算已知，否则"加字段再校验"必然自锁
    known |= EXTRA_COLS
    for s in stmts:
        for c in s.find_all(exp.Column):
            col = c.name.lower()
            if not col or col in known:
                continue
            # 别名（SELECT x AS y ... ORDER BY y）不算幻觉，跳过
            aliases = {a.alias.lower() for a in s.find_all(exp.Alias) if a.alias}
            if col in aliases:
                continue
            add(REJECT, "R4-字段存在性",
                "字段 `%s` 在 %d 张表的任何列里都不存在" % (c.name, len(TABLES)),
                "最常见的原因是模型幻觉；请先 describe 目标表")


def rule5_detail_limit(stmts, mode):
    if mode != "query":
        return
    for s in stmts:
        detail = [t.name.lower() for t in s.find_all(exp.Table)
                  if layer_of(t.name) == "dwd"]
        if not detail:
            continue
        # 聚合/GROUP BY 的结果集天然很小，不适用「明细必须 LIMIT」。
        # 之前没排除，count(distinct event_id) 会被 R5 拦下，把 R10 的问题盖住了。
        agg_nodes = tuple(n for n in (_node("Count"), _node("Sum"), _node("Avg"),
                                      _node("Min"), _node("Max")) if n)
        if s.args.get("group") or (agg_nodes and s.find(*agg_nodes)):
            continue
        lim = s.args.get("limit")
        if lim is None:
            add(REJECT, "R5-明细限流", "查询了明细表 %s 但没有 LIMIT" % ",".join(detail),
                "明细查询必须 LIMIT ≤ 1000")
        else:
            try:
                n = int(lim.expression.this)
                if n > 1000:
                    add(REJECT, "R5-明细限流", "LIMIT %d 超过 1000" % n)
            except Exception:
                pass


def rule6_partition_filter(stmts, mode):
    """强制分区裁剪。

    适用范围收窄过一次，原因写清楚：
      · 问数模式（query）：一律要求。这是给业务同学用的，必须防全表扫描。
      · 开发模式（dev）：只有语句**碰了正式库**时才要求。
        dev 表是空壳，对空表强加分区裁剪只会把 02/03/04 这类工程脚本
        全部误报成 REJECT，护栏就变成了噪音源。
        但一旦语句读了 music_dw / music_offline，仍然要求裁剪——
        那才是真的会扫 500 万行的地方。
    """
    for s in stmts:
        if not isinstance(s, READ_OK):
            continue          # DDL/DML 语句没有 WHERE，不该按分区裁剪要求
        tabs = list(s.find_all(exp.Table))
        touches_prod = any(_table_db(t) in PROD_LIBS for t in tabs)
        if mode != "query" and not touches_prod:
            continue
        partitioned = [t.name.lower() for t in tabs
                       if "dt" in TABLES.get(t.name.lower(), {}).get("partitioned_by", [])]
        if not partitioned:
            continue
        cols = {c.name.lower() for c in s.find_all(exp.Column)}
        if "dt" not in cols and "cohort_date" not in cols:
            add(REJECT, "R6-分区裁剪",
                "表 %s 按 dt 分区，但 WHERE 里没有 dt 条件" % ",".join(partitioned),
                "强制分区裁剪，否则全表扫描")


def rule7_join_on(stmts, mode):
    for s in stmts:
        for j in s.find_all(exp.Join):
            if j.args.get("on") or (j.args.get("kind") or "").upper() == "CROSS":
                continue
            if j.args.get("using"):
                continue
            add(REJECT, "R7-JOIN 条件",
                "存在没有 ON 条件的 JOIN（笛卡尔积风险）：%s" % j.sql()[:80])


def rule8_no_avg_of_rate(stmts, mode):
    for s in stmts:
        for a in s.find_all(exp.Avg):
            col = a.this.name.lower() if isinstance(a.this, exp.Column) else ""
            if col in WEIGHTED:
                add(REJECT, "R8-比率不可平均",
                    "AVG(%s) 会把日比率再平均，口径错误" % col,
                    "改用 SUM(分子)/SUM(分母)，例如 SUM(complete_cnt)/SUM(play_start_cnt)")


def rule9_no_star(stmts, mode):
    """只拦「投影里列出 *」，不拦 COUNT(*)。

    2026-11-09 修：旧版用 `s.find(exp.Star)` 全局找星号，于是 `COUNT(*)` 里的星号
    也被判成 SELECT *。而 `COUNT(*)` 是最常见的行数统计，等于让所有非 ADS 表的
    明细计数全部误报——R9 一上线就会挡住"11-08 有多少次起播"这类最基础的问题。

    正确判据：星号的父节点是 SELECT（投影），不是 COUNT / 函数参数。
    另修：旧版遇到"第一个语句全是 ADS"就 `return`，后面的语句直接不检查了。
    """
    for s in stmts:
        tables = [t.name.lower() for t in s.find_all(exp.Table)]
        if tables and all(layer_of(t) == "ads" for t in tables):
            continue        # ADS 表允许 SELECT *，但只跳过这一条语句
        for star in s.find_all(exp.Star):
            if isinstance(star.parent, exp.Select):
                add(REJECT, "R9-禁 SELECT *", "非 ADS 表禁止 SELECT *",
                    "显式列出字段，减少无效传输",
                    {"op": "replace", "target": "SELECT *",
                     "replacement": "SELECT dt, <需要的列>"})


def rule10_dedup_key(stmts, mode):
    """dwd 表上的 COUNT(DISTINCT event_id) —— 2026-11-09 起不再强制 event_ts。

    事实基础：重造后 35 天离线数据 event_id 100% 唯一（4,994,396 / 4,994,396），
    26 位含 RUN_TAG，撞号的前提（同一天多次跑、sess 从头编号）已不存在。

    为什么从 REJECT 降成 INFO：
      · 强制二元组对当前数据是冗余的 —— 加不加 event_ts 结果完全一样，
        只增加模型写错的面积，不增加正确性。
      · 它会制造噪音：任何合规的 COUNT(DISTINCT event_id) 都被拒，
        而噪音多了人就会习惯性放行，比少一条规则更危险。
      · 真正要拦的是 COUNT(*) / COUNT(event_id) 这类根本没去重的写法，
        那类写法二元组同样拦不住（压根没有 DISTINCT）。

    提示仍保留：碰到实时混存数据（20 位与 26 位并存）单键会少算，
    历史实测 11-07 少算 47.53%。提示不阻塞，只提醒。
    """
    for s in stmts:
        for c in s.find_all(exp.Count):
            # sqlglot 6.3：COUNT(DISTINCT x) 的 args 只有 this，且 this 是 Distinct 节点；
            # 18.x 改成 args['distinct']=True。两种写法都认。
            dist_cls = _node("Distinct")
            is_distinct = bool(c.args.get("distinct")) or (
                dist_cls is not None and isinstance(c.this, dist_cls))
            if not is_distinct:
                continue
            dwd = [t.name.lower() for t in s.find_all(exp.Table) if layer_of(t.name) == "dwd"]
            if not dwd:
                continue
            inner = c.this.sql().lower()
            if "event_id" in inner and "event_ts" not in inner:
                add(INFO, "R10-去重键",
                    "dwd 表上 %s 只按 event_id 去重" % c.sql()[:60],
                    "离线表 event_id 已 100% 唯一，可放心；若混算实时数据请补 event_ts")


# ─────────────────────────── 开发模式（11~15）───────────────────────────
WRITE_NODES = tuple(n for n in (_node("Insert"), _node("Update"), _node("Delete"),
                                _node("Create"), _node("Drop"), _node("Alter"),
                                _node("TruncateTable")) if n)


def _table_db(t):
    """取表所属库名。sqlglot 6.3 没有 Table.db，库名在 args['db'] 的 Identifier 里。"""
    db = t.args.get("db")
    if db is None:
        return ""
    return (getattr(db, "name", "") or str(db)).strip("`").lower()


def _target_lib(node):
    t = node.find(exp.Table)
    if t is None:
        return "", ""
    return _table_db(t), (t.name or "").lower()


def rule11_lib_whitelist(stmts, mode):
    seen = set()
    for s in stmts:
        # 注意：不要写成 `for node in s.walk()`。
        # sqlglot 的 walk() 产出的是 (node, parent, arg_key) 三元组，
        # 直接 isinstance(node, WRITE_NODES) 永远为 False —— 这条规则曾经因此完全失效，
        # 写 music_dw 的 INSERT 被判成「全部通过」。
        for node in s.find_all(*WRITE_NODES):
            if not isinstance(node, WRITE_NODES):
                continue
            lib, tname = _target_lib(node)
            if lib in PROD_LIBS:
                seen.add((type(node).__name__.lower(), lib, tname))
                add(REJECT, "R11-库名白名单",
                    "%s 写到了正式库 %s.%s" % (type(node).__name__, lib, tname),
                    "开发模式只允许写 music_dev / music_dev_offline")
    # ---- 兜底：AST 看不见的写语句 ----
    # sqlglot 6.3 把 Hive 的 `ALTER TABLE ... ADD COLUMNS` 解析成 Command 节点，
    # 不在 WRITE_NODES 里 —— 于是"给正式表加一列"这条最危险的操作会被放过。
    # 这里按「语句首词」再扫一遍，只认语句开头的写动作，不会把 FROM 误判成写。
    if mode != "dev":
        return
    for stmt in _split_statements(RAW_SQL):
        m = _WRITE_HEAD_RE.match(stmt)
        if not m:
            continue
        verb = m.group(1).strip().lower().split()[0]
        lib = m.group(2).lower()
        tname = (m.group(3) or "").lower()
        if lib in PROD_LIBS and (verb, lib, tname) not in seen:
            add(REJECT, "R11-库名白名单",
                "%s 写到了正式库 %s.%s" % (m.group(1).strip(), lib, tname),
                "开发模式只允许写 music_dev / music_dev_offline")


def rule12_no_drop_table(stmts, mode):
    for s in stmts:
        for d in s.find_all(exp.Drop):
            kind = (d.args.get("kind") or "").upper()
            if kind in ("TABLE", ""):
                add(REJECT, "R12-禁 DROP",
                    "生成 DROP TABLE：%s" % d.sql()[:90],
                    "2026-11-08 十张域表被清空就是这条；改用 CREATE OR REPLACE "
                    "或 INSERT OVERWRITE 分区")


def rule13_timezone_declared(stmts, mode):
    if mode != "dev":
        return
    for s in stmts:
        is_create = any(isinstance(n, exp.Create) for n in s.walk())
        if not is_create:
            continue
        body = s.sql().lower()
        has_ts = any(k in body for k in ("timestamp", "datetime"))
        if has_ts and "timezone" not in body and "from_utc_timestamp" not in body:
            add(WARN, "R13-时区声明",
                "新增了时间字段但没有声明时区",
                "离线 DWD 用 from_utc_timestamp(from_unixtime(ms/1000),'Asia/Shanghai')")


def rule14_song_join(stmts, mode):
    for s in stmts:
        body = s.sql().lower()
        if "dim_song" in body and "join" in body:
            if "song_id" not in body:
                add(WARN, "R14-dim_song 关联", "JOIN dim_song 但没出现 song_id")
            else:
                add(INFO, "R14-dim_song 关联",
                    "维表已统一为 S 前缀（1486 行），可直接 ON a.song_id=b.song_id")


def rule15_partition_overwrite(stmts, mode):
    if mode != "dev":
        return
    for s in stmts:
        for cr in s.find_all(exp.Create):
            body = cr.sql().upper()
            if "TABLE" in body and "PARTITION" not in body:
                add(WARN, "R15-分区落表",
                    "新建表没有 PARTITIONED BY(dt)：%s" % cr.sql()[:70],
                    "新表必须带 dt 分区，否则无法分区覆盖、无法回退")
    for s in stmts:
        for ins in s.find_all(exp.Insert):
            body = ins.sql().upper()
            if "TABLE" in body and "OVERWRITE" not in body:
                add(WARN, "R15-分区落表", "ETL 用了 INSERT INTO，不是 OVERWRITE",
                    "重跑会重复累加；分区级 INSERT OVERWRITE 才幂等")


def rule16_guest_entity(stmts, mode):
    """COUNT(DISTINCT user_id) 必须给游客留出路 —— 2026-11-09 新增。

    事实基础（实测，不是推测）：
      · 生成器 `is_login = uid % 10 < 8`，即 **20% 的用户是永久游客**；
        他们的 `user_id` 是空串 `''`，但 `device_id` 有值。
      · 裸 `COUNT(DISTINCT user_id)` 会把全体游客压成 **1 个**实体。
      · 11-08 实测：1,046 条支付里 207 条 `user_id` 为空 —— 游客是真实付费的。
      · 四张 DWS/ADS 表曾因此系统性偏低：曲风 UV 2,028→2,493、搜索 UV 1,047→1,277、
        付费用户 25,830→32,040、35 天播放 UV 16,280→20,731。

    正确写法：`COUNT(DISTINCT COALESCE(NULLIF(user_id,''), device_id))`
    登录按 user_id、游客按 device_id，命名空间 `U...` / `d_...` 不重叠，合并安全。

    不拦的情形（避免误报）：
      · SQL 里已显式限定 `user_id <> ''` / `user_id IS NOT NULL`（真的只统计登录用户）
      · DISTINCT 表达式里已经出现 device_id
      · 表本身没有 device_id 列（如 dim_* 维表、dwd_page_view 之外的少量表）
    """
    for s in stmts:
        body = s.sql().lower()
        if re.search(r"user_id\s*(<>|!=)\s*''", body) or "user_id is not null" in body:
            continue
        tables = [t.name.lower() for t in s.find_all(exp.Table)]
        if not any("device_id" in _cols(t) for t in tables):
            continue
        for c in s.find_all(exp.Count):
            dist_cls = _node("Distinct")
            is_distinct = bool(c.args.get("distinct")) or (
                dist_cls is not None and isinstance(c.this, dist_cls))
            if not is_distinct:
                continue
            inner = c.this.sql().lower()
            if "user_id" not in inner or "device_id" in inner:
                continue
            add(REJECT, "R16-游客实体",
                "COUNT(DISTINCT user_id) 会把全体游客压成 1 个：%s" % c.sql()[:70],
                "改成 COUNT(DISTINCT COALESCE(NULLIF(user_id,''), device_id))；"
                "若确实只统计登录用户，请显式加 user_id <> '' 条件",
                {"op": "replace", "target": c.sql(),
                 "replacement": "COUNT(DISTINCT COALESCE(NULLIF(user_id,''), device_id))"})


# ─────────────────── 执行面与聚合语义（17~20，2026-11-09 新增）───────────────────
#
# 为什么补这四条：前 16 条里 13 条是"通用安全与规范"（只读、白名单、限流、防幻觉），
# 一条都不管"语句合法、结果却静默算错"。而本仓库实测到的两个最贵的 bug 都属于后者：
#   · music_dw.dwd_play_start 落 internal 实时库 72,355 行，离线是 445,945 行，差 6.2 倍不报错
#   · dwd_session JOIN dim_user 不收敛，2,660 行膨胀到 3,171 行，虚高 19.2% 不报错
# 数据库不报错、linter 不报错、让 LLM 自审 SQL 也看不出——只有带元数据的静态检查能拦。

BAD_CATALOG = {"internal"}                        # 实时库：同名表装的是 Flink 结果
BAD_DB = {"music_offline", "music_dev_offline"}   # 备库/开发库：混用会破坏"一个入口"


def _ident(v):
    """把 Table.args 里的 catalog/db 统一成小写字符串。

    为什么不能直接用 t.catalog / t.db：
      集群上是 sqlglot 6.3.0，Table 只有 args（里面是 exp.Identifier），
      没有 .catalog / .db 属性；本地是 30.x，两个属性都有。
      写成 t.catalog 在本地跑得通、上集群就 AttributeError——而且是运行期才炸。
    """
    if v is None:
        return ""
    if isinstance(v, str):
        return v.lower()
    nm = getattr(v, "name", None)
    if nm:
        return str(nm).lower()
    return str(getattr(v, "this", v)).lower()


def rule17_catalog_scope(stmts, mode):
    """R17 跨 catalog 引用：切了 catalog 之后，这些写法要么报错、要么落错库。"""
    for s in stmts:
        cats = set()
        for t in s.find_all(exp.Table):
            cat = _ident(t.args.get("catalog")) or _ident(getattr(t, "catalog", None))
            db = _ident(t.args.get("db")) or _ident(getattr(t, "db", None))
            full = ".".join(x for x in (cat, db, t.name.lower()) if x)
            if cat in BAD_CATALOG:
                # internal.music_dw 里的**白名单**实时表是第二条受控数据面：
                # 它们不与离线表同名（同名明细大表都不在白名单里），R26 还会
                # 再卡时间窗与混查。除此以外的 internal 引用一律拒。
                if (cat == "internal" and db == "music_dw"
                        and t.name.lower() in RT_TABLES):
                    pass
                else:
                    add(REJECT, "R17-跨 catalog",
                        "%s 指向 internal 实时库：同名表口径不同（dwd_play_start 实时 72,355 / 离线 445,945，差 6.2 倍）" % full,
                        "去掉 catalog 前缀用裸表名，连接初始化时已 SWITCH hive_catalog + USE music_dw",
                        {"op": "replace", "target": full, "replacement": t.name})
            elif cat in BAD_DB or db in BAD_DB:
                add(REJECT, "R17-跨 catalog",
                    "%s 指向备库/开发库，与离线正库不是同一份数据" % full,
                    "问数只走 hive_catalog.music_dw；备库仅用于同步核对",
                    {"op": "replace", "target": full,
                     "replacement": "hive_catalog.music_dw.%s" % t.name})
            if cat:
                cats.add(cat)
        if len(cats) > 1:
            add(REJECT, "R17-跨 catalog",
                "同一条语句引用了多个 catalog：%s" % ", ".join(sorted(cats)),
                "统一到一个 catalog；跨库取数请先落临时表再 JOIN",
                {"op": "restructure", "target": "FROM/JOIN",
                 "replacement": "拆成两条查询，各自在自己的 catalog 内完成"})


def _has_converge(s):
    """语句里是否出现了把拉链表收敛成单版本的条件。"""
    for c in s.find_all(exp.Column):
        if c.name.lower() in CONVERGE_COLS:
            return True
    return False


def _join_table(j):
    """取 JOIN 右侧那张表，跨 sqlglot 版本。

    6.3.0 会把 `dim_user u` 解析成 Alias(TABLE)，30.x 直接给 Table 并把别名挂在
    table.alias 上。若只判 isinstance(j.this, exp.Table)，旧版本上所有带别名的
    JOIN 都会被跳过 —— R18/R19 静默失效，而且不报错。
    """
    node = j.this
    if node is None:
        return None
    if isinstance(node, exp.Table):
        return node
    try:
        return node.find(exp.Table)
    except Exception:
        return None


def _fanout_tables(s):
    """找出被 JOIN 进来、且本身是多行一实体的表（拉链/流水）。"""
    out = []
    for j in s.find_all(exp.Join):
        right = _join_table(j)
        if right is None:
            continue
        nm = right.name.lower()
        if nm in VERSIONED:
            out.append(nm)
    return out


def rule18_fanout(stmts, mode):
    """R18 JOIN 扇出：JOIN 到主键不唯一的表且未收敛，同时又做聚合。"""
    for s in stmts:
        fans = _fanout_tables(s)
        if not fans or _has_converge(s):
            continue
        if s.find(exp.Sum) is None and s.find(exp.Count) is None:
            continue          # 只取明细、不做聚合，行数膨胀还能忍
        t = fans[0]
        add(REJECT, "R18-JOIN 扇出",
            "JOIN %s 未收敛到单版本，聚合结果会按历史版本数放大（实测 11-08："
            "dwd_session JOIN dim_user 得 3,171 行，收敛后 2,660 行，虚高 19.2%%）" % t,
            "加 %s.is_current = 'Y'（或 dt BETWEEN start_date AND end_date）" % t,
            {"op": "add_condition", "target": "WHERE",
             "replacement": "%s.is_current = 'Y'" % t})


def rule19_chasm(stmts, mode):
    """R19 扇出叠加：两个以上未收敛的扇出 JOIN，误差是乘积关系而不是相加。"""
    for s in stmts:
        if _has_converge(s):
            continue
        fans = _fanout_tables(s)
        if len(fans) >= 2:
            add(REJECT, "R19-扇出叠加",
                "同一条语句有 %d 个未收敛的扇出 JOIN（%s），膨胀倍数相乘"
                % (len(fans), ", ".join(fans)),
                "先把每个扇出 JOIN 各自聚合到明细粒度，再在外层 JOIN",
                {"op": "restructure", "target": "JOIN",
                 "replacement": "拆子查询：先各自 GROUP BY 到同一粒度，再 JOIN"})


def rule20_additivity(stmts, mode):
    """R20 可加性：比率/存量/当日去重实体，被 SUM 跨日就会改变含义。

    四档（来自 metrics.yaml.additivity，经 gen_tables.py 落到 tables.json）：
      none  比率/均值/单价 —— SUM 无意义，必须 SUM(分子)/SUM(分母) 重算
      stock 存量快照（vip_user_cnt / login_user_cnt）—— 跨日相加是把"当天有多少"变成"人次"
      semi  当日去重实体（dau / *_uv / *_user_cnt）—— 跨日相加等于"人日"，合法但不是去重 UV
      full  可加，不拦

    跳过 SUM(比率 * 次数)：这是加权分子 `SUM(avg_progress * play_end_cnt)` 的合法写法，
    拦它会误伤我们自己算加权完播率的那条 SQL。
    """
    for s in stmts:
        cross_day = not _groups_by_dt(s)
        for sm in s.find_all(exp.Sum):
            arg = sm.this
            if isinstance(arg, exp.Mul):
                continue       # 加权分子，合法
            if not isinstance(arg, exp.Column):
                continue
            lv = ADDITIVITY.get(arg.name.lower())
            if lv == "none":
                add(REJECT, "R20-比率不可求和",
                    "SUM(%s) 无意义：%s 是比率/均值，跨行相加不是任何一个业务量"
                    % (arg.name, arg.name),
                    "改成 SUM(分子)/SUM(分母)；只算单日可用 AVG",
                    {"op": "replace", "target": sm.sql(),
                     "replacement": "SUM(分子) / SUM(分母)（分子分母见 metrics.yaml）"})
            elif lv == "stock" and cross_day:
                add(REJECT, "R20-存量不可加",
                    "SUM(%s) 把存量快照跨日相加，得到的是『人日』不是『用户数』"
                    % arg.name,
                    "取窗口末日的值；若确要人日，请在答案里标注『人日』",
                    {"op": "replace", "target": sm.sql(),
                     "replacement": "MAX_BY(%s, dt)  或按 dt 逐日展示" % arg.name})
            elif lv == "semi" and cross_day:
                add(WARN, "R20-人日告警",
                    "SUM(%s) 是每日去重实体的和，语义等于『人日』，不等于窗口去重 UV"
                    % arg.name,
                    "要窗口去重请用 COUNT(DISTINCT COALESCE(NULLIF(user_id,''), device_id))；"
                    "用人日作分母（如 ARPU）时必须在答案里说明",
                    {"op": "annotate", "target": sm.sql(),
                     "replacement": "结果标注『人日』，或改用 COUNT(DISTINCT 实体)"})


def _groups_by_dt(s):
    g = s.args.get("group")
    if not g:
        return False
    for e in (g.expressions or []):
        if isinstance(e, exp.Column) and e.name.lower() == "dt":
            return True
    return False


def _cols(tname):
    """返回表的小写列名集合；表不在目录里（如 dev 临时表）时返回空集。"""
    meta = TABLES.get(tname) or TABLES.get(tname.split(".")[-1]) or {}
    cols = meta.get("columns") or []
    return set(str(c).lower() for c in cols)


# ─────────────────────── 阶段 1：安全护栏（21~23）───────────────────────────
# 这三条与 1~20 有个本质区别：**不允许模型重写绕过**。
# 1~20 的 REJECT 会带 fix 进 C 阶段修复闭环；21~23 是身份/安全边界，
# 让模型"换个写法再试"等于给它三次机会越权。所以 fix 用 op=deny，
# Agent 侧的 node_validate 会把它们标成 security_block 直接走 refuse。
def rule21_permission_scope(stmts, mode):
    """R21：表级越权 + 元数据探测（information_schema / 跨库）。"""
    if mode != "query":
        return
    sec = load_security()
    probes = set(str(x).lower() for x in sec.get("probe_objects") or [])
    pol = CONTEXT.get("policy") or {}
    allow = set(str(x).lower() for x in (pol.get("allow_tables") or []))
    deny = set(str(x).lower() for x in (pol.get("deny_tables") or []))
    unrestricted = bool(pol.get("unrestricted"))
    hit = []
    for s in stmts:
        derived = _derived_names(s)
        for t in s.find_all(exp.Table):
            name = _tname(t)
            if name in derived:
                continue
            cat = str(t.args.get("catalog") or "").lower()
            db = str(t.args.get("db") or "").lower()
            if name in probes or cat in probes or db in probes:
                hit.append("%s.%s（系统元数据探测）" % (db or "?", name))
                continue
            if unrestricted:
                continue
            if name in deny or "*" in deny:
                hit.append("%s（策略显式拒绝）" % name)
            elif allow and "*" not in allow and name not in allow:
                hit.append("%s（不在角色可见范围）" % name)
    if hit:
        add(REJECT, "R21-PERM",
            "越权或越界的表访问：%s" % "、".join(sorted(set(hit))[:4]),
            "角色 %s 的可见范围由服务端策略决定，不能靠改写 SQL 绕过"
            % (CONTEXT.get("role") or "未指定"),
            {"op": "deny", "target": "table_scope",
             "replacement": "改用角色可见的表，或申请授权"})


def rule22_pii_columns(stmts, mode):
    """R22：PII / 禁用列拦截。AST 查列名，文本兜底查中文别名。"""
    if mode != "query":
        return
    sec = load_security()
    pol = CONTEXT.get("policy") or {}
    pii = set(str(x).lower() for x in (sec.get("pii_columns") or []))
    pii |= set(str(x).lower() for x in (pol.get("deny_columns") or []))
    if not pii:
        return
    cols = set()
    for s in stmts:
        for c in s.find_all(exp.Column):
            cn = str(getattr(c, "name", "") or "").lower()
            if cn in pii:
                cols.add(cn)
    words = [w for w in (sec.get("pii_words_cn") or []) if w in RAW_SQL]
    if cols or words:
        add(REJECT, "R22-PII",
            "涉及敏感/禁用列：%s" % "、".join(sorted(cols)[:5] + words[:3]),
            "PII 与角色禁用列不进问数链路；如确需，走脱敏接口并留审批记录",
            {"op": "deny", "target": "pii_column",
             "replacement": "去掉敏感列，或改查聚合口径"})


def injection_reason(text: str) -> str:
    """扫一段原文，命中返回原因，否则空串。

    抽出来是因为它有两个调用点：正常解析后的 R23，以及**解析之前**的
    前置扫描。后者不能省——`#` 这类当前方言不认的注释会让 R0 解析失败，
    若只靠解析后的 R23，注入原文就随着"解析失败"一起被跳过了。
    """
    sec = load_security()
    for pat in sec.get("injection_patterns") or []:
        try:
            m = re.search(pat, text)
        except re.error:
            continue
        if m:
            return "命中注入模式：%s" % m.group(0)[:60]
    for w in sec.get("injection_words_zh") or []:
        if w in text:
            return "命中注入关键词：%s" % w
    return ""


def _add_injection(why: str):
    add(REJECT, "R23-INJECTION", why,
        "用户指令不能覆盖系统规则；本轮请求直接拒绝，不进入重试",
        {"op": "deny", "target": "prompt_injection",
         "replacement": "用正常的问数表达重新提问"})


def rule23_prompt_injection(stmts, mode):
    """R23：提示注入。既扫用户原问，也扫 SQL 注释里藏的指令。"""
    if mode != "query":
        return
    why = injection_reason("%s\n%s" % (CONTEXT.get("question") or "", RAW_SQL))
    if why:
        _add_injection(why)


def rule24_non_pruning_dt(stmts, mode):
    """R24：有 dt，但裁不掉分区（静态可判的那几种写法）。

    为什么 R6 不够：R6 判的是"WHERE 里有没有 dt 这个列"，属于**存在性检查**。
    下面三种写法它全放行，而实际扫描量是整表：

        WHERE dt <> '1900-01-01'          -- 不等式不裁剪
        WHERE substr(dt,1,7) = '2026-11'  -- 列被函数包住，裁不了
        ... JOIN dim_date d ON a.dt = d.dt  -- 那是连接条件，不是过滤

    动态那半边在 `code/agent/costgate.py`：拿 EXPLAIN 的真计划判"到底要读多少"，
    超阈值直接拒。这里只抓**不连库就能看出来**的情况，给一条提示——
    判据以计划为准：静态规则永远猜不全优化器的行为，所以这条只 WARN 不 REJECT。

    口径收窄过一次：只在"整条语句里找不到任何一个可裁剪的 dt 条件"时才报。
    否则 `WHERE dt='2026-11-08' AND date_format(dt,'%Y%m%d')='20261108'`
    这种（多写了一个冗余条件、但主条件是对的）也会被告警，那就成了噪音。
    """
    sarg = (exp.EQ, exp.GT, exp.GTE, exp.LT, exp.LTE, exp.Between, exp.In)
    for s in stmts:
        if not isinstance(s, READ_OK):
            continue
        parts = [t.name.lower() for t in s.find_all(exp.Table)
                 if "dt" in TABLES.get(t.name.lower(), {}).get("partitioned_by", [])
                 and TABLES.get(t.name.lower(), {}).get("layer") in ("ods", "dwd")]
        if not parts:
            continue
        has_sarg = False
        non: list[str] = []
        for col in s.find_all(exp.Column):
            if (col.name or "").lower() != "dt":
                continue
            p = col.parent
            if isinstance(p, sarg) and p.find(exp.Column) is col:
                has_sarg = True
            elif isinstance(p, (exp.NEQ, exp.Func)):
                non.append((p.sql() or "")[:40])
        if has_sarg or not non:
            continue
        add(WARN, "R24-dt 不裁剪",
            "表 %s 上的 dt 条件裁不动分区：%s" % (",".join(parts), " / ".join(non[:2])),
            "改用 dt = '...' 或 dt BETWEEN ... AND ...；"
            "否则与全表扫描等价，执行前会被成本闸拦下")


def rule25_caliber(stmts, mode):
    """R25 口径机检：语法对、表在册、EXPLAIN 过，也不代表数是对的。

    2026-11-09 的真实事故：收入 CTAS 只取 event_name='order_pay'，漏掉占毛收入
    82% 的 VIP 订阅，结果 156,986 vs 真值 823,410（低 5.24 倍）——R1~R24 全绿。
    当时靠人工复核拦下；这条规则就是把那次复核变成机器判定。

    规则本身写在 metrics.yaml 的 caliber_guard 里（数据，不是代码），
    检查实现只有一份：同目录的 caliber.py。ETL 链路（etlguard.py）读同一份规则、
    同一个实现，所以「问数漏掉 VIP」和「建表漏掉 VIP」会被同样拦住。

    只报 WARN：口径偏离有时是有意的选择（只看普通支付订单也是一种问法），
    机器给黄牌 + 依据，人决定是否照做；换成 REJECT 会让正常问数被自己的护栏挡住。
    """
    if mode != "query" or not CALIBER:
        return
    try:
        from caliber import check as caliber_check
    except Exception:
        return                      # 规则文件缺失时降级为"没有这条检查"，不误伤问数
    for f in caliber_check(stmts, CALIBER):
        add(f.get("level") or WARN, f.get("rule") or "R25-口径",
            f.get("message") or "", f.get("hint") or "", f.get("fix"))


def rule26_realtime_window(stmts, mode):
    """R26 实时快照：必须落在时间窗内，且不能与离线表混在一句里。

    实时链路 2026-11-08 22:17 之后不再产出，库里是**历史快照**。两个风险：
      · 不带时间列过滤 → 把整段历史当"实时"读出来，还扫全表
      · 和离线表 JOIN/UNION → 同名口径差 6.2 倍（docs/34），数据库不报错

    白名单来自 tables.json 的 realtime 段（realtime.yaml 生成），不是硬编码。
    """
    if mode != "query" or not RT_TABLES:
        return
    for s in stmts:
        derived = _derived_names(s)
        tabs = [t for t in s.find_all(exp.Table)
                if t.name and t.name.lower() not in derived]
        rt_used = [t.name.lower() for t in tabs if t.name.lower() in RT_TABLES]
        if not rt_used:
            continue
        other = [t.name.lower() for t in tabs
                 if t.name.lower() not in RT_TABLES
                 and not t.name.lower().startswith("information_schema")]
        if other:
            add(REJECT, "R26-实时/离线混查",
                "实时快照表 %s 与离线表 %s 在同一条语句里（口径不同，不能混算）"
                % (", ".join(sorted(set(rt_used))), ", ".join(sorted(set(other)))),
                "拆成两条查询各自在自己的数据面内完成，不要 JOIN/UNION",
                {"op": "restructure", "target": "FROM/JOIN",
                 "replacement": "拆成两条独立查询"})
        # 只看 WHERE 谓词里的列。SELECT/GROUP BY 里出现时间列**不算**过滤：
        # `SELECT stat_minute ... LIMIT 10` 照样会扫整段历史快照（实测漏放过）。
        where = s.find(exp.Where)
        pred_cols = {c.name.lower() for c in (where.find_all(exp.Column) if where else [])}
        for join in s.find_all(exp.Join):
            on = join.args.get("on")
            if on is not None:
                pred_cols |= {c.name.lower() for c in on.find_all(exp.Column)}
        for name in sorted(set(rt_used)):
            allowed = {RT_TIME_COL.get(name, ""), RT_ALT_COL.get(name, "")} - {""}
            if allowed & pred_cols:
                continue
            add(REJECT, "R26-实时时间窗",
                "实时表 %s 没有时间列 %s 的过滤条件（会扫整段历史快照）"
                % (name, "/".join(sorted(allowed))),
                "在 WHERE 里加上时间窗，例如 %s >= '2026-11-08 00:00:00'"
                % sorted(allowed)[0],
                {"op": "append_where", "target": name,
                 "replacement": "%s >= '%s'" % (sorted(allowed)[0],
                                                RT_META.get("data_since") or "2026-11-02 16:32")})


def rule27_dim_join_on_pk(stmts, mode):
    """JOIN 维表必须按主键关联，不能用维度属性做连接键。

    2026-11-15 由 C18 暴露。模型写的是：

        FROM dws_genre_daily g JOIN dim_song s ON s.genre = g.genre
        WHERE g.dt = '2026-11-08' AND g.genre = '民谣' AND g.play_start_cnt > 0

    它想同时满足「当日」和「曲风」，但 dws_genre_daily 是曲风×天粒度、
    **没有 song_id**，于是被迫用 genre 关联维表——这等于 `dim_song` 全部
    240 首民谣 × 当日那 1 行，COUNT(DISTINCT s.song_id) 得 240（全曲库），
    而正确答案是 237（当日真被播放过的）。实测 5 次里错 4 次，错法完全一致。

    R7 只查「有没有 ON」，挡不住这种「ON 写了但键不对」。而维表的定义就是
    「按主键描述实体」：用属性列关联，本质是把维表当过滤器，正确写法要么换
    粒度匹配的事实表（dws_song_play_daily），要么改用 IN 子查询。

    只对 `dim_` 开头的表判定，且只在 alias 能解析到真实表名时生效——
    解析不出来的（多层子查询、别名冲突）跳过，宁可漏判也不误报。
    """
    for s in stmts:
        if not isinstance(s, READ_OK):
            continue
        alias = {}
        for t in s.find_all(exp.Table):
            name = (t.name or "").lower()
            a = (t.alias or "").lower()
            if a:
                alias[a] = name
            if name:
                alias.setdefault(name, name)
        for j in s.find_all(exp.Join):
            on = j.args.get("on")
            if on is None:
                continue
            touched: dict[str, set] = {}
            for c in on.find_all(exp.Column):
                tb = (c.table or "").lower()
                if not tb:
                    continue          # 裸列名，判定不了属于哪张表
                real = alias.get(tb)
                if not real:
                    continue
                touched.setdefault(real, set()).add((c.name or "").lower())
            for real, cols in touched.items():
                if not real.startswith("dim_"):
                    continue
                pk = TABLES.get(real, {}).get("pk")
                pks = [pk] if isinstance(pk, str) else list(pk or [])
                pks = [str(p).lower() for p in pks if p]
                if not pks or (set(pks) & cols):
                    continue
                add(REJECT, "R27-维表关联键",
                    "%s 是维表，但 ON 里没有用到它的主键 %s（实际用了 %s）——"
                    "按维度属性关联维表等于把整张维表按属性展开，聚合结果被放大"
                    % (real, "/".join(pks), "/".join(sorted(cols))),
                    "维表一律按主键关联（%s.%s）；若要按属性筛，先用子查询选出键，"
                    "或改用粒度匹配的事实表" % (real, pks[0]))


RULES = [rule1_select_only, rule2_single_statement, rule3_table_whitelist,
         rule4_no_hallucinated_column, rule5_detail_limit, rule6_partition_filter,
         rule7_join_on, rule8_no_avg_of_rate, rule9_no_star, rule10_dedup_key,
         rule11_lib_whitelist, rule12_no_drop_table, rule13_timezone_declared,
         rule14_song_join, rule15_partition_overwrite, rule16_guest_entity,
         rule17_catalog_scope, rule18_fanout, rule19_chasm, rule20_additivity,
         rule21_permission_scope, rule22_pii_columns, rule23_prompt_injection,
         rule24_non_pruning_dt, rule25_caliber, rule26_realtime_window,
         rule27_dim_join_on_pk]


def check_text(sql, mode="query", context=None):
    """跑一遍全部规则，返回 [(level, rule, message, hint, fix)]。

    抽成函数是为了 `--serve` 常驻模式：进程启动一次（sqlglot 的 import 成本
    约 0.2s），之后每条 SQL 只付解析成本。**规则本身一行没动**，
    subprocess 逐次调用仍走同一条路径。
    """
    global CONTEXT, RAW_SQL, findings
    findings = []
    CONTEXT = dict(context or {})
    EXTRA_COLS.clear()          # 每条请求独立，不能让上一条的 DDL 列漏进来
    load_security()

    RAW_SQL = sql
    collect_added_columns(sql)

    # 注入扫描必须在解析之前：解析失败（如 `#` 注释）会让后面的规则全跳过，
    # 而这恰恰是攻击者能利用的缝隙——先扫原文，命中就直接判死，不再解析。
    early = injection_reason("%s\n%s" % (CONTEXT.get("question") or "", sql))
    if early and mode == "query":
        _add_injection(early)
        return list(findings)

    stmts = parse(sql)
    if stmts:
        for r in RULES:
            r(stmts, mode)
    return list(findings)


def _serve():
    """常驻模式：stdin 一行一个 JSON 请求，stdout 一行一个 JSON 结果。

    为什么值得多一个模式：护栏是唯一规则实现，Agent 侧 subprocess 调它是
    为了"规则只有一份"。但每条 SQL 起一次进程要付 sqlglot 的 import 成本，
    并发一上来就成了瓶颈（AC-16 实测）。常驻进程把这个成本摊成一次，
    规则实现仍然是同一份文件、同一个 check_text。
    """
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        try:
            req = json.loads(line)
            fs = check_text(req.get("sql") or "", req.get("mode") or "query",
                            req.get("context") or {})
            out = {"findings": [{"level": l, "rule": r, "message": m,
                                 "hint": h, "fix": f} for l, r, m, h, f in fs]}
        except Exception as e:                      # 单条请求炸了不能带崩常驻进程
            out = {"error": "%s: %s" % (type(e).__name__, e), "findings": []}
        sys.stdout.write(json.dumps(out, ensure_ascii=False) + "\n")
        sys.stdout.flush()
    return 0


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["query", "dev"], default="query")
    ap.add_argument("--json", action="store_true",
                    help="只输出 JSON 数组，供 Agent 的 validator 节点直接解析")
    ap.add_argument("--context", default=None,
                    help="请求上下文 JSON 文件路径（R21/R22/R23 用；CI 可不带）")
    ap.add_argument("--serve", action="store_true",
                    help="常驻模式：从 stdin 逐行读 {sql, mode, context}，逐行回结果")
    ap.add_argument("sql", nargs="*", help="SQL 文件路径，或 --mode query 下直接给 SQL 串")
    a = ap.parse_args()

    if a.serve:
        return _serve()

    CONTEXT = {}
    if a.context and os.path.exists(a.context):
        try:
            with open(a.context, encoding="utf-8") as f:
                CONTEXT = json.load(f) or {}
        except Exception:
            CONTEXT = {}

    quiet = a.json          # json 模式下 stdout 只能有 json，标题/汇总一律不打印

    def say(*args):
        if not quiet:
            print(*args)

    chunks = []
    for p in a.sql:
        if os.path.exists(p):
            chunks.append(open(p, encoding="utf-8").read())
            say("== 检查文件：%s ==" % p)
        else:
            chunks.append(p)
    sql = "\n".join(chunks)

    added = collect_added_columns(sql)
    if added:
        say("== 本批次新增列（供 R4 识别）：%s ==" % ", ".join(sorted(added)))

    check_text(sql, a.mode, CONTEXT)
    stmts = _split_statements(sql)

    if a.json:
        print(json.dumps(
            [{"level": l, "rule": r, "message": m, "hint": h, "fix": f}
             for l, r, m, h, f in findings],
            ensure_ascii=False, indent=1))
        return 1 if any(f[0] == REJECT for f in findings) else (
            2 if any(f[0] == WARN for f in findings) else 0)

    say("=" * 90)
    if not findings:
        say("全部 %d 条护栏通过（mode=%s，%d 条语句）" % (len(RULES), a.mode, len(stmts)))
        return 0
    for lvl, rule, msg, hint, fix in findings:
        say("%-6s %-20s %s" % (lvl, rule, msg))
        if hint:
            say("       └─ %s" % hint)
        if fix and fix.get("op") not in ("apply_hint", "review"):
            say("       ✎  %s %s -> %s"
                % (fix.get("op"), fix.get("target"), fix.get("replacement")))
    say("=" * 90)
    n_r = sum(1 for f in findings if f[0] == REJECT)
    n_w = sum(1 for f in findings if f[0] == WARN)
    n_i = sum(1 for f in findings if f[0] == INFO)
    say("合计 %d 条：REJECT %d / WARN %d / INFO %d（%d 条规则）"
        % (len(findings), n_r, n_w, n_i, len(RULES)))
    return 1 if n_r else (2 if n_w else 0)


if __name__ == "__main__":
    sys.exit(main())
