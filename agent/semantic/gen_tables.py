#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""从离线建表 SQL 解析出表结构，生成 tables.json。

为什么不在集群上跑 DESCRIBE：
  Hive CLI 的 log4j 把 INFO 日志打到了 **stdout**（不是 stderr），
  `2>/dev/null` 拦不住，多语句 `-f` 模式的输出还会和日志交织，
  解析出来是 0 张表。而 sql/offline/*.sql 就是这些表的权威建表语句，
  本地解析既快又不会漂移。

用法：
  python3 gen_tables.py            # 输出到 agent/tables.json
"""
import io
import json
import os
import re

HERE = os.path.dirname(os.path.abspath(__file__))
SQL_DIR = os.path.join(HERE, "..", "..", "sql", "offline")
OUT = os.path.join(HERE, "..", "tables.json")
REALTIME_YAML = os.path.join(HERE, "realtime.yaml")

CREATE_RE = re.compile(
    r"CREATE\s+EXTERNAL\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?([a-zA-Z_0-9]+)\.([a-zA-Z_0-9]+)",
    re.I)
TYPE_RE = re.compile(r"^\s*([a-zA-Z_][a-zA-Z_0-9]*)\s+([A-Za-z]+(?:\s*\([^)]*\))?)")

# 00_all_ddl.sql 里保留了实时侧 v9 的两张老表定义，离线链路从未建过。
# 实测 `SHOW TABLES IN music_dw` 只有 29 张（不含这两张），
# 若把它们留在白名单里，Agent 引用它们时护栏会放行、到执行才报错。
EXCLUDE = {"dwd_collect", "dwd_share"}


def split_column_block(block):
    """把括号里的字段定义切出来。按行切，不按逗号——COMMENT 里常有中文逗号。"""
    cols, types = [], {}
    for raw in block.splitlines():
        line = raw.strip()
        if not line or line.startswith("--") or line.upper().startswith(("PRIMARY", "CONSTRAINT")):
            continue
        m = TYPE_RE.match(line)
        if not m:
            continue
        name, typ = m.group(1), m.group(2).strip()
        cols.append(name)
        types[name] = typ.upper()
    return cols, types


def layer_of(t):
    for p in ("ads_", "dws_", "dwd_", "dim_"):
        if t.startswith(p):
            return p[:-1]
    return "other"


def attach_curated(meta, tables):
    """把「手写知识」合并进 tables.json，让护栏仍然只依赖一个文件。

    为什么合并在生成阶段、而不是让 guard.py 直接读两份 YAML：
      护栏在集群上跑（/root/agent/guards/guard.py + ../tables.json）。
      若它还要读 semantic/*.yaml，部署就变成「拷三个文件且版本必须配对」，
      而这正是最容易漂移的地方。合并成一个 json，部署面就是一个文件。

    但合并会带来新风险：YAML 改了、忘了重新生成，json 就是旧的。
    所以 sem_check.py 里加了一条反向断言（第 12 项）专门盯这个。

    注入两类字段：

    A. 表级业务语义（写进每张表自己的对象里）—— Agent 单表读取即可用
      name_cn / grain / pk   这张表是什么、一行代表什么、按什么唯一
      rows                   实测行数，Agent 用它判断「这查询会不会扫爆」
      catalog / database     执行面。缺了它 Agent 不知道 SQL 该写哪个前缀
      caveats                口径陷阱，进 prompt 时最有价值的一段
      pk_unique / versions   表级一份，便于按表判断能否 JOIN 后聚合

    A 是 2026-11-09 补的。此前 tables.json 每张表只有 layer/partitioned_by/
    columns/types/source 五个键 —— 这意味着 P0-1 写进 YAML 的 catalog 在
    **护栏和 Agent 实际读的那个文件里一个字都没有**，所谓「执行面只有一个入口」
    在部署面上是空的。字段写在 YAML 里却没合并进来，等于没写。

    B. 跨表索引（顶层，护栏用）
      pk_non_unique   主键不唯一的表（dim_user 是 SCD2、ods_user_change 是变更流水）
      scd2_tables     拉链表清单，R18 需要它判断「JOIN 后是否必须收敛」
      relationships   展开成边的 JOIN 关系（R18/R19）
      column_additivity  {列名: full|semi|stock|none}，R8/R20 的唯一依据
    """
    sem_dir = os.path.join(HERE)
    try:
        import yaml
    except ImportError:
        print("!! 缺 PyYAML，跳过策展元数据注入（护栏将失去 R18/R20 判断依据）")
        return meta

    p_tbl = os.path.join(sem_dir, "tables.yaml")
    p_met = os.path.join(sem_dir, "metrics.yaml")
    for p in (p_tbl, p_met):
        if not os.path.exists(p):
            print("!! 缺 %s，跳过策展元数据注入" % p)
            return meta

    tbl_y = yaml.safe_load(io.open(p_tbl, encoding="utf-8")) or {}
    met_y = yaml.safe_load(io.open(p_met, encoding="utf-8")) or {}

    ty = tbl_y.get("tables") or {}

    # --- 0. 表级业务语义合并进每张表 ----------------------------------
    # 白名单控制：只搬「DDL 里没有、只有人能写」的字段，
    # 避免 YAML 里的拼写错误（如分区字段写成 partitions）静默带进 json。
    # partitioned_by 不在此列 —— 它以 DDL 为准，YAML 的 partition 只是冗余描述。
    TABLE_KEYS = ("name_cn", "grain", "pk", "rows", "catalog", "database",
                  "caveats", "pk_unique", "versions", "layer", "not_for")
    merged, yaml_only, missing = 0, [], []
    for t, tinfo in tables.items():
        cur = ty.get(t)
        if not cur:
            missing.append(t)
            continue
        for k in TABLE_KEYS:
            if k in cur and cur[k] is not None:
                tinfo[k] = cur[k]
        merged += 1
    yaml_only = sorted(set(ty) - set(tables))
    if yaml_only:
        print("   !! tables.yaml 有 %d 张表不在 DDL 解析结果里：%s"
              % (len(yaml_only), yaml_only))
    if missing:
        print("   !! tables.json 有 %d 张表在 YAML 里没有策展信息：%s"
              % (len(missing), missing))
    print("   表级语义已合并 %d/%d 张（name_cn/grain/pk/rows/catalog/caveats）"
          % (merged, len(tables)))

    # --- 1. 主键唯一性 ------------------------------------------------
    meta["pk_non_unique"] = sorted(t for t, v in ty.items() if v.get("pk_unique") is False)
    meta["scd2_tables"] = sorted(t for t, v in ty.items() if v.get("versions") == "scd2")
    meta["versioned_tables"] = sorted(t for t, v in ty.items()
                                      if v.get("versions") in ("scd2", "changelog"))

    # --- 2. JOIN 关系展开 ---------------------------------------------
    edges = []
    for rel in (tbl_y.get("relationships") or []):
        for f in (rel.get("from") or []):
            edges.append({
                "from": f,
                "to": rel.get("to"),
                "join_on": (rel.get("join_on") or "").replace("<from>", f),
                "cardinality": rel.get("cardinality"),
                "converge": rel.get("converge"),
                "fanout_if_missing": bool(rel.get("fanout_if_missing")),
                "name": rel.get("name"),
            })
    meta["relationships"] = edges

    # --- 3. 列级可加性 -------------------------------------------------
    # 先按命名约定给出默认判定（覆盖所有表的所有列），再用 metrics.yaml 覆写。
    # 顺序很重要：约定是兜底，指标定义是权威。
    add = {}
    for t, info in tables.items():
        for c in info["columns"]:
            lc = c.lower()
            if lc.endswith(("_rate", "_dod")) or lc.startswith("avg_") or lc in (
                    "ctr", "arpu", "ad_ctr", "avg_progress", "avg_play_per_user",
                    "click_to_play_rate", "incomplete_rate", "pay_conv_rate",
                    "retention_rate", "zero_result_rate"):
                add[c] = "none"                      # 比率/均值：不可加
            elif lc in ("vip_user_cnt", "login_user_cnt"):
                add[c] = "stock"                     # 存量快照：跨日求和无意义
            elif lc in ("dau", "user_cnt") or lc.endswith(("_uv", "_user_cnt")):
                add[c] = "semi"                      # 当日去重实体：跨日相加=人日

    # metrics.yaml 的 additivity 是权威判定，覆盖约定
    lvl_of = {"none": "none", "semi": "semi", "stock": "stock", "full": "full"}
    for code, m in (met_y.get("metrics") or {}).items():
        lv = m.get("additivity")
        if lv in lvl_of and code in add:
            add[code] = lvl_of[lv]

    meta["column_additivity"] = dict(sorted(add.items()))

    # --- 4. 口径机检规则 ------------------------------------------------
    # 2026-11-11 新增：metrics.yaml 的 caliber_guard 原样带进 json，
    # 让 guard.py（问数 R25）与 etlguard.py（ETL caliber_ok）读同一份规则。
    # 规则是数据不是代码——加一个口径坑只加一段 YAML，两条链路同时生效。
    meta["caliber_guard"] = met_y.get("caliber_guard") or []

    meta["curated_at"] = tbl_y.get("verified_at")
    return meta


def attach_realtime(meta, tables):
    """把实时链路历史快照合并进同一份 tables.json（layer=rt）。

    为什么不在 tables.yaml 里加：tables.yaml 与 sql/offline/*.sql 是双向覆盖的
    （sem_check 第 1/2 项），实时表没有离线 DDL、行数也不由 rowcount.tsv 管，
    塞进去会让"离线事实层"的断言全部失真。单独一份 realtime.yaml，
    生成阶段合入，护栏/检索/权限仍然只读 tables.json 一个文件。

    合并后 rt 表会同时进入 R3 表白名单与策略引擎的列索引；是否被放行还要
    由 R17（catalog 白名单）与 R26（时间窗必填）再判两道。
    """
    try:
        import yaml
        rt = yaml.safe_load(io.open(REALTIME_YAML, encoding="utf-8")) or {}
    except Exception as e:
        print("   !! realtime.yaml 读不到（%s），跳过实时层合并" % str(e)[:80])
        return meta
    spec = {k: rt[k] for k in ("lane", "catalog", "database", "status",
                               "data_since", "data_until", "freshness_note",
                               "default_time_col") if k in rt}
    idx = {}
    for name, d in (rt.get("tables") or {}).items():
        cols = list((d.get("columns") or {}).keys())
        types = {k: str(v).upper() for k, v in (d.get("columns") or {}).items()}
        tcol = d.get("time_col") or rt.get("default_time_col")
        tables[name] = {
            "layer": "rt", "partitioned_by": [tcol],
            "columns": cols, "types": types, "source": "realtime.yaml",
            "catalog": rt.get("catalog"), "database": rt.get("database"),
            "name_cn": d.get("name_cn"), "grain": d.get("grain"),
            "rows": d.get("rows"), "time_col": tcol,
            "alt_time_col": d.get("alt_time_col") or "",
            "caveats": list(d.get("caveats") or []), "pk_unique": False,
        }
        idx[name] = {"time_col": tcol, "alt_time_col": d.get("alt_time_col") or "",
                     "grain": d.get("grain"), "rows": d.get("rows"),
                     "name_cn": d.get("name_cn")}
        # 比率列也要进可加性索引，否则 R8/R20 对实时表形同虚设
        for c in cols:
            lc = c.lower()
            if lc.endswith(("_rate", "_dod", "_share")) or lc.startswith("avg_"):
                meta.setdefault("column_additivity", {})[c] = "none"
            elif lc.endswith("_uv") or lc.endswith("_user_cnt"):
                meta.setdefault("column_additivity", {})[c] = "semi"
    meta["column_additivity"] = dict(sorted((meta.get("column_additivity") or {}).items()))
    spec["tables"] = idx
    meta["realtime"] = spec
    meta["count"] = len(tables)
    return meta


def main():
    tables = {}
    for fn in sorted(os.listdir(SQL_DIR)):
        if not fn.endswith(".sql"):
            continue
        text = io.open(os.path.join(SQL_DIR, fn), encoding="utf-8").read()
        # 去掉整行注释，避免把注释里的示例 DDL 当真
        lines = [l for l in text.splitlines() if not l.strip().startswith("--")]
        text = "\n".join(lines)
        for m in CREATE_RE.finditer(text):
            lib, tname = m.group(1), m.group(2)
            if lib != "music_dw" or tname in EXCLUDE:
                continue
            start = text.find("(", m.end())
            if start < 0:
                continue
            depth, i = 0, start
            while i < len(text):
                if text[i] == "(":
                    depth += 1
                elif text[i] == ")":
                    depth -= 1
                    if depth == 0:
                        break
                i += 1
            block = text[start + 1:i]
            cols, types = split_column_block(block)

            tail = text[i:i + 800]
            pm = re.search(r"PARTITIONED\s+BY\s*\(([^)]*)\)", tail, re.I)
            parted = []
            if pm:
                for part in pm.group(1).split(","):
                    nm = TYPE_RE.match(part.strip())
                    if nm:
                        parted.append(nm.group(1))
                        types[nm.group(1)] = nm.group(2).strip().upper()
                        if nm.group(1) not in cols:
                            cols.append(nm.group(1))

            if not cols:
                continue
            # 同一张表在多个文件里出现时，取字段更全的那份
            if tname in tables and len(tables[tname]["columns"]) >= len(cols):
                continue
            tables[tname] = {"layer": layer_of(tname), "partitioned_by": parted,
                             "columns": cols, "types": types, "source": fn}

    weighted = sorted({c for i in tables.values() for c in i["columns"]
                       if c.endswith(("_rate", "_dod")) or c in
                       ("ctr", "arpu", "avg_progress", "avg_play_per_user", "ad_ctr")})

    meta = {"db": "music_dw", "tables": tables,
            "weighted_columns": weighted, "count": len(tables)}
    meta = attach_curated(meta, tables)
    meta = attach_realtime(meta, tables)
    io.open(OUT, "w", encoding="utf-8").write(
        json.dumps(meta, ensure_ascii=False, indent=1))

    print("== %d 张表，%d 个比率字段 -> %s ==" % (len(tables), len(weighted), OUT))
    print("   策展元数据：主键不唯一 %d 张 / 拉链 %d 张 / JOIN 边 %d 条 / 列可加性 %d 列 / 口径规则 %d 条"
          % (len(meta.get("pk_non_unique") or []), len(meta.get("scd2_tables") or []),
             len(meta.get("relationships") or []), len(meta.get("column_additivity") or {}),
             len(meta.get("caliber_guard") or [])))
    for k in ("rows", "catalog", "grain", "caveats"):
        n = sum(1 for v in tables.values() if v.get(k))
        print("   %-12s %2d/%d 张表已填" % (k, n, len(tables)))
    for lay in ("dim", "dwd", "dws", "ads"):
        sub = sorted(t for t in tables if tables[t]["layer"] == lay)
        print("   %-4s %2d 张" % (lay, len(sub)))
    miss = sorted(t for t in tables if not tables[t]["partitioned_by"])
    print("   无 dt 分区：", miss if miss else "无")


if __name__ == "__main__":
    main()
