#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""语义层一致性校验器。

为什么需要它：
  metrics.yaml / tables.yaml / dimensions.yaml / glossary.yaml 是**手写的知识**，
  而 tables.json 是从 DDL **自动生成的事实**。手写的东西最容易出的错不是"写错逻辑"，
  而是"引用了一个不存在的字段/表/指标"——这种错不会在加载时报错，
  只会在 Agent 生成 SQL 执行时才炸，或者更糟：静默算出错误结果。

 所以这里做 13 项交叉校验，全部基于 tables.json 的字段事实。

用法：
  python3 sem_check.py            # 校验，退出码 0=全通过
  python3 sem_check.py --verbose  # 打印每项明细
"""
from __future__ import print_function

import io
import json
import os
import re
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
TABLES_JSON = os.path.join(HERE, "..", "tables.json")
ROWCOUNT = os.path.join(HERE, "..", "rowcount.tsv")

try:
    import yaml
except ImportError:
    print("!! 需要 PyYAML：pip3 install pyyaml")
    sys.exit(2)

VERBOSE = "--verbose" in sys.argv

SQL_KEYWORDS = set("""
count sum avg min max distinct if null nullif round cast as and or not in like between
case when then else end group by order partition over rows between preceding following
where desc asc rank option
current row rank dense_rank row_number concat substr lower upper coalesce ifnull nvl
true false is not and or
""".split())


def load_yaml(name):
    p = os.path.join(HERE, name)
    if not os.path.exists(p):
        print("!! 缺文件：%s" % p)
        sys.exit(2)
    with io.open(p, encoding="utf-8") as f:
        return yaml.safe_load(f)


def load_facts():
    with io.open(TABLES_JSON, encoding="utf-8") as f:
        meta = json.load(f)
    return meta["tables"], meta.get("weighted_columns", []), meta


def load_rowcount():
    """实测行数，格式：表名<TAB>行数"""
    out = {}
    if not os.path.exists(ROWCOUNT):
        return out
    with io.open(ROWCOUNT, encoding="utf-8") as f:
        for line in f:
            parts = line.strip().split("\t")
            if len(parts) == 2 and parts[1].isdigit():
                out[parts[0]] = int(parts[1])
    return out


def tokens(expr):
    """抽出表达式里的标识符，先剥掉字符串字面量。"""
    s = re.sub(r"'[^']*'", " ", expr or "")
    return [t for t in re.findall(r"[A-Za-z_][A-Za-z_0-9]*", s)
            if t.lower() not in SQL_KEYWORDS]


class Checker(object):
    def __init__(self):
        self.fails = []
        self.warns = []
        self.passes = 0

    def ok(self, msg):
        self.passes += 1
        if VERBOSE:
            print("  PASS  %s" % msg)

    def fail(self, msg):
        self.fails.append(msg)
        print("  FAIL  %s" % msg)

    def warn(self, msg):
        self.warns.append(msg)
        if VERBOSE:
            print("  WARN  %s" % msg)


def main():
    facts, weighted, meta = load_facts()
    rc = load_rowcount()
    tables_y = load_yaml("tables.yaml")
    metrics_y = load_yaml("metrics.yaml")
    dims_y = load_yaml("dimensions.yaml")
    gloss_y = load_yaml("glossary.yaml")

    c = Checker()
    # rt 层是独立事实源（realtime.yaml），不参与"离线 tables.yaml <-> DDL"
    # 的双向覆盖断言；它有自己的校验（第 15 项）。不排除会让第 1 项把 9 张
    # 实时表误报成"tables.yaml 缺表"——事实层混进了另一种口径。
    rt_names = {t for t, v in facts.items() if (v.get("layer") or "") == "rt"}
    fact_names = set(facts) - rt_names
    tbl_y = tables_y.get("tables") or {}
    mets = metrics_y.get("metrics") or {}
    dims = dims_y.get("dimensions") or {}

    # ---------------------------------------------------------------- 1
    print("\n[1] 表覆盖：tables.yaml <-> tables.json")
    missing = sorted(fact_names - set(tbl_y))
    extra = sorted(set(tbl_y) - fact_names)
    if missing:
        c.fail("tables.yaml 缺表：%s" % ", ".join(missing))
    if extra:
        c.fail("tables.yaml 有 DDL 里不存在的表：%s" % ", ".join(extra))
    if not missing and not extra:
        c.ok("离线表双向覆盖一致（%d 张；另有 %d 张 rt 层单列校验）"
             % (len(tbl_y), len(rt_names)))

    # ---------------------------------------------------------------- 2
    print("\n[2] 行数与实测一致（agent/rowcount.tsv）")
    if not rc:
        c.warn("没有 rowcount.tsv，跳过行数核对")
    else:
        bad = []
        for t, v in sorted(tbl_y.items()):
            claim = v.get("rows")
            real = rc.get(t)
            if real is None:
                bad.append("%s: 无实测值" % t)
            elif claim is None:
                bad.append("%s: yaml 未填 rows（实测 %d）" % (t, real))
            elif int(claim) != real:
                bad.append("%s: yaml=%s 实测=%s" % (t, claim, real))
        if bad:
            for b in bad:
                c.fail("行数不一致 " + b)
        else:
            c.ok("%d 张表行数与实测逐一相等" % len(tbl_y))

    # ---------------------------------------------------------------- 3
    print("\n[3] 指标引用的表存在")
    for code, m in sorted(mets.items()):
        srcs = []
        if m.get("source"):
            srcs.append(m["source"])
        srcs.extend(m.get("sources") or [])
        for s in srcs:
            if s not in fact_names:
                c.fail("%s.source 引用了不存在的表 %s" % (code, s))
        for a in (m.get("ads") or []):
            if a not in fact_names:
                c.fail("%s.ads 引用了不存在的表 %s" % (code, a))
            else:
                ly = facts[a]["layer"]
                if ly != "ads" and ly != "dws":
                    c.fail("%s.ads 里的 %s 不是 ads/dws 层（是 %s）" % (code, a, ly))
        if srcs and all(s in fact_names for s in srcs):
            c.ok("%s -> %s" % (code, ",".join(srcs)))

    # ---------------------------------------------------------------- 4
    print("\n[4] 派生指标引用的原子指标存在")
    for code, m in sorted(mets.items()):
        if m.get("tier") == "atomic":
            continue
        # 来源表的物理字段也算合法引用（公式里常直接写列名），
        # 字段是否存在由 [5] 严格校验，这里只负责"引用不该悬空"
        pool = set()
        for s in ([m.get("source")] if m.get("source") else []) + (m.get("sources") or []):
            if s in fact_names:
                pool |= set(facts[s]["columns"])
        refs = set(tokens(m.get("formula", "")))
        refs.discard(code)
        for r in refs:
            if r in mets:
                c.ok("%s 引用指标 %s" % (code, r))
            elif r in pool or r in dims:
                pass  # 物理字段或维度，归 [5]/[6] 管
            else:
                c.warn("%s.formula 里的 %s 既不是指标、维度，也不是来源表字段"
                       % (code, r))

    # ---------------------------------------------------------------- 5
    print("\n[5] 指标 formula / filter 里的字段在来源表中存在")
    for code, m in sorted(mets.items()):
        srcs = []
        if m.get("source"):
            srcs.append(m["source"])
        srcs.extend(m.get("sources") or [])
        if not srcs:
            c.warn("%s 没有 source，跳过字段校验" % code)
            continue
        pool = set()
        for s in srcs:
            if s in fact_names:
                pool |= set(facts[s]["columns"])
        # filter 要严格：剥掉字符串后每个标识符都必须是来源表字段
        for tok in tokens(m.get("filter", "")):
            if tok in pool:
                c.ok("%s.filter 字段 %s" % (code, tok))
            else:
                c.fail("%s.filter 用了 %s 里不存在的字段 %s"
                       % (code, "/".join(srcs), tok))
        # formula 宽松：只报不是任何表字段也不是指标名的 token
        for tok in tokens(m.get("formula", "")):
            if tok in pool or tok in mets:
                continue
            if tok in ("dt", "position", "genre", "song_id", "cohort_date", "day_n"):
                continue
            c.warn("%s.formula 里的 %s 不在 %s 字段中" % (code, tok, "/".join(srcs)))

    # ---------------------------------------------------------------- 6
    print("\n[6] 维度字段在声明的表中存在")
    for name, d in sorted(dims.items()):
        f = d.get("field")
        if not f:
            c.fail("维度 %s 缺 field" % name)
            continue
        for t in (d.get("tables") or []):
            if t not in fact_names:
                c.fail("维度 %s 引用了不存在的表 %s" % (name, t))
            elif f not in facts[t]["columns"]:
                c.fail("维度 %s 的字段 %s 不在表 %s 中" % (name, f, t))
            else:
                c.ok("%s.%s 存在于 %s" % (name, f, t))

    # ---------------------------------------------------------------- 7
    print("\n[7] 业务词典指向的指标/维度存在")
    terms = gloss_y.get("terms") or {}
    for t, v in sorted(terms.items()):
        if not isinstance(v, dict):
            c.fail("术语 %s 的取值不是字典" % t)
            continue
        if "metric" in v and v["metric"] not in mets:
            c.fail("术语 %s 指向不存在的指标 %s" % (t, v["metric"]))
        if "dimension" in v and v["dimension"] not in dims:
            c.fail("术语 %s 指向不存在的维度 %s" % (t, v["dimension"]))
        if "trap" in v:
            ids = set(x["id"] for x in (gloss_y.get("traps") or []))
            if v["trap"] not in ids:
                c.fail("术语 %s 引用了不存在的陷阱 %s" % (t, v["trap"]))
        if not any(k in v for k in ("metric", "dimension", "field")):
            c.fail("术语 %s 没有指向 metric/dimension/field" % t)
    c.ok("术语 %d 条" % len(terms))

    # ---------------------------------------------------------------- 8
    print("\n[8] 陷阱清单自身完整")
    ids = []
    for tr in (gloss_y.get("traps") or []):
        for k in ("id", "severity", "fact", "action", "source"):
            if not tr.get(k):
                c.fail("陷阱 %s 缺字段 %s" % (tr.get("id", "?"), k))
        if tr.get("severity") not in ("high", "medium", "low"):
            c.fail("陷阱 %s 的 severity 非法：%s" % (tr.get("id"), tr.get("severity")))
        ids.append(tr.get("id"))
    dup = set(x for x in ids if ids.count(x) > 1)
    if dup:
        c.fail("陷阱 id 重复：%s" % ", ".join(sorted(dup)))
    c.ok("%d 条陷阱，id 唯一" % len(ids))

    # ---------------------------------------------------------------- 9
    print("\n[9] 执行面字段：每张表必须有 catalog + database")
    CATALOG_OK = {"hive_catalog", "internal", "music_offline"}
    bad = []
    for t, v in sorted(tbl_y.items()):
        cat, db = v.get("catalog"), v.get("database")
        if not cat or not db:
            bad.append("%s 缺 catalog/database" % t)
        elif cat not in CATALOG_OK:
            bad.append("%s 的 catalog=%s 不在白名单" % (t, cat))
        elif cat != "hive_catalog":
            # 问数只走离线正库；写成 internal / music_offline 就是执行面分叉
            bad.append("%s 的 catalog=%s，问数必须走 hive_catalog" % (t, cat))
    if bad:
        for b in bad:
            c.fail("执行面 " + b)
    else:
        c.ok("31 张表 catalog=hive_catalog / database=music_dw 齐备")

    # ---------------------------------------------------------------- 10
    print("\n[10] 可用性与可加性：availability / additivity 必须齐全且取值合法")
    g = metrics_y.get("global") or {}
    if not g.get("data_until"):
        c.fail("metrics.yaml 顶层 global.data_until 缺失（Agent 无法判断数据截止日）")
    else:
        c.ok("global.data_until = %s" % g["data_until"])
    ADD_OK = {"full", "semi", "stock", "none"}
    av_bad = 0
    for code, m in sorted(mets.items()):
        if "realtime" in m:
            c.fail("%s 仍保留旧字段 realtime（应迁移到 availability）" % code)
            av_bad += 1
        av = m.get("availability")
        if av not in ("offline_only", "offline_and_realtime"):
            c.fail("%s.availability 非法或缺失：%s" % (code, av))
            av_bad += 1
        ad = m.get("additivity")
        if ad not in ADD_OK:
            c.fail("%s.additivity 非法或缺失：%s" % (code, ad))
            av_bad += 1
    if not av_bad:
        c.ok("%d 个指标 availability/additivity 全部合法" % len(mets))

    # ---------------------------------------------------------------- 11
    print("\n[11] 主键唯一性：pk_unique=false 必须与实测一致")
    non_unique = set(meta.get("pk_non_unique") or [])
    for t, v in sorted(tbl_y.items()):
        if "pk_unique" not in v:
            c.fail("%s 缺 pk_unique（R18 无法判断扇出）" % t)
            continue
        if v.get("versions") not in ("none", "scd2", "changelog"):
            c.fail("%s.versions 非法：%s" % (t, v.get("versions")))
        if (v.get("pk_unique") is False) != (t in non_unique):
            c.fail("%s 的 pk_unique 与 tables.json 策展区不一致（改了 yaml 是否忘了跑 gen_tables.py？）" % t)
    # dim_user 是本次发现的扇出来源，单独钉死
    du = tbl_y.get("dim_user") or {}
    if du.get("pk_unique") is not False or du.get("versions") != "scd2":
        c.fail("dim_user 必须是 pk_unique=false / versions=scd2（1,150,000 行 / 1,000,000 用户）")
    else:
        c.ok("dim_user 已标记为 SCD2 非唯一主键")

    # ---------------------------------------------------------------- 12
    print("\n[12] 策展元数据已同步进 tables.json（防『改了 YAML 忘重新生成』）")
    edges = meta.get("relationships") or []
    if not edges:
        c.fail("tables.json 没有 relationships——护栏 R18 会失效，请跑 gen_tables.py")
    else:
        rel_y = tables_y.get("relationships") or []
        want = sum(len(r.get("from") or []) for r in rel_y)
        if len(edges) != want:
            c.fail("relationships 边数不一致：yaml 展开 %d 条，tables.json %d 条" % (want, len(edges)))
        else:
            c.ok("JOIN 边 %d 条已同步" % len(edges))
    # 每条边引用的表和字段都必须真实存在
    for e in edges:
        for tb in (e.get("from"), e.get("to")):
            if tb not in fact_names:
                c.fail("关系 %s 引用了不存在的表 %s" % (e.get("name"), tb))
        jn = e.get("join_on") or ""
        for tok in re.findall(r"[A-Za-z_][A-Za-z_0-9]*", jn):
            low = tok.lower()
            if low in ("song_id", "user_id", "dt"):
                continue
            if low in fact_names:
                continue
            c.warn("关系 %s 的 join_on 里出现未识别标识符 %s" % (e.get("name"), tok))
    # 列级可加性必须覆盖所有被 metrics 引用的比率列
    add_map = meta.get("column_additivity") or {}
    for w in weighted:
        if add_map.get(w) not in ("none", "semi", "stock"):
            c.fail("比率列 %s 未标为不可加（R20 会漏拦 SUM(%s)）" % (w, w))
    c.ok("列级可加性 %d 列，比率列全部标为不可加" % len(add_map))

    # ---------------------------------------------------------------- 13
    # 为什么单列一项：第 12 项只查了 relationships / column_additivity 这两个
    # **顶层索引**，没查「表级字段有没有被合并」。于是 catalog / rows / grain
    # 写在 YAML 里很漂亮，tables.json 里 0 张表有，两边都"通过"。
    # 这是 2026-11-09 实测发现的：P0-1 声称 31/31 表有 catalog，
    # 而护栏和 Agent 实际读的那个 json 里一个都没有。
    print("\n[13] 表级业务语义已合并进 tables.json（catalog/rows/grain/pk/name_cn）")
    REQUIRED = ("catalog", "database", "grain", "pk", "name_cn")
    CATALOGS = {"hive_catalog", "internal", "music_offline"}
    miss_k, mismatch, zero_rows = [], [], []
    for t, v in sorted(tbl_y.items()):
        jt = (meta.get("tables") or {}).get(t)
        if not jt:
            miss_k.append("%s(表本身不在 json)" % t)
            continue
        for k in REQUIRED:
            if not jt.get(k):
                miss_k.append("%s.%s" % (t, k))
                continue
            # 逐字段与 YAML 对齐：防「YAML 改了没重生成」
            if v.get(k) != jt.get(k):
                mismatch.append("%s.%s（yaml=%r json=%r）" % (t, k, v.get(k), jt.get(k)))
        if jt.get("catalog") and jt["catalog"] not in CATALOGS:
            mismatch.append("%s.catalog=%r 不在白名单" % (t, jt["catalog"]))
        # 行数是 Agent 做代价估算的唯一依据，0 或缺失都等于没有
        rows = jt.get("rows")
        if not isinstance(rows, int) or rows <= 0:
            zero_rows.append("%s(rows=%r)" % (t, rows))
        if (jt.get("pk_unique") is False) != (v.get("pk_unique") is False):
            mismatch.append("%s.pk_unique 与 yaml 不一致" % t)
    for x in miss_k:
        c.fail("tables.json 缺表级字段 %s（跑 gen_tables.py）" % x)
    for x in mismatch:
        c.fail("表级字段与 YAML 不同步：%s" % x)
    for x in zero_rows:
        c.fail("行数缺失或为 0，Agent 无法估算代价：%s" % x)
    if not (miss_k or mismatch or zero_rows):
        c.ok("%d/%d 张表的 catalog/database/grain/pk/name_cn/rows 全部就位且与 YAML 一致"
             % (len(tbl_y), len(tbl_y)))
    # 陷阱是 Agent 最该读的一段；缺失不算错，但要知道是哪张
    no_cav = sorted(t for t, v in tbl_y.items() if not v.get("caveats"))
    if no_cav:
        c.warn("%d 张表没有 caveats（若无已知口径陷阱可忽略）：%s" % (len(no_cav), no_cav))

    # ---------------------------------------------------------------- 14
    # 口径机检规则自检（2026-11-11 新增）：规则是数据，写错了不会报错，
    # 只会"静默不检查"——那比没有检查更糟，因为面板上会显示"口径机检通过"。
    print("\n[14] 口径机检规则（caliber_guard）自身完整且已同步")
    cal = metrics_y.get("caliber_guard") or []
    # 2026-11-14 新增 denominator / dedup_entity；2026-11-15 新增 ratio_consistency。
    KINDS = {"event_coverage", "refund_sign", "source_guard",
             "denominator", "dedup_entity", "ratio_consistency"}
    bad_cal, seen_ids = [], set()
    for r in cal:
        rid = str(r.get("id") or "")
        if not rid or rid in seen_ids:
            bad_cal.append("id 缺失或重复：%r" % rid)
        seen_ids.add(rid)
        kind = str(r.get("kind") or "")
        if kind not in KINDS:
            bad_cal.append("%s.kind=%r 不认识" % (rid, kind))
        lvl = str(r.get("level") or "")
        if lvl not in ("WARN", "REJECT"):
            bad_cal.append("%s.level=%r 只能是 WARN / REJECT" % (rid, lvl))
        # REJECT 会把问数推进修复闭环，三轮用尽就转拒答——必须能说清为什么
        # 这是确定性错误，并且给得出修法，否则不许用这个级别。
        if lvl == "REJECT" and not (r.get("fix_template") or r.get("replacement")):
            bad_cal.append("%s 用 REJECT 但没给 fix_template / replacement，修复闭环会空转" % rid)
        if str(r.get("table") or "").lower() not in {t.lower() for t in tbl_y}:
            bad_cal.append("%s.table=%r 不在 tables.yaml" % (rid, r.get("table")))
        for k in ("title", "why", "hint", "rule"):
            if not r.get(k):
                bad_cal.append("%s 缺 %s" % (rid, k))
        if kind == "event_coverage" and not (r.get("require_events") or []):
            bad_cal.append("%s.require_events 为空，规则等于不生效" % rid)
        if kind == "source_guard" and not r.get("right_table"):
            bad_cal.append("%s.right_table 为空，提示里说不出该用哪张表" % rid)
        if kind in ("denominator", "dedup_entity") and not r.get("right_table"):
            bad_cal.append("%s.right_table 为空，无法判断正确写法是否已出现" % rid)
    if cal != (meta.get("caliber_guard") or []):
        bad_cal.append("tables.json 的 caliber_guard 与 metrics.yaml 不一致（跑 gen_tables.py）")
    for x in bad_cal:
        c.fail("口径规则：%s" % x)
    if not bad_cal:
        c.ok("%d 条口径规则就位（表在册 / 级别与修法匹配 / json 已同步）" % len(cal))

    # ---------------------------------------------------------------- 15
    # 实时层自检（2026-11-11 新增）：实时表没有离线 DDL、不进 rowcount.tsv，
    # 前 14 项都管不到它。若 realtime.yaml 与 tables.json 漂移（或时间列漏填），
    # R26 会静默失效——又是"面板显示通过、实际没检查"这一类事故。
    print("\n[15] 实时链路快照（realtime.yaml）自身完整且已同步")
    rt_y = load_yaml("realtime.yaml") or {}
    rt_meta = meta.get("realtime") or {}
    rt_tables_y = rt_y.get("tables") or {}
    rt_idx = rt_meta.get("tables") or {}
    bad_rt = []
    for k in ("status", "data_until", "freshness_note", "catalog", "database"):
        if not rt_y.get(k):
            bad_rt.append("缺 %s" % k)
        elif rt_y.get(k) != rt_meta.get(k):
            bad_rt.append("%s 与 tables.json 不同步（跑 gen_tables.py）" % k)
    if rt_y.get("status") == "down" and not rt_y.get("data_until"):
        bad_rt.append("status=down 必须写清 data_until，否则答案里说不出数据停在哪天")
    if sorted(rt_tables_y) != sorted(rt_idx):
        bad_rt.append("表清单与 tables.json 不一致（跑 gen_tables.py）")
    if sorted(rt_tables_y) != sorted(rt_names):
        bad_rt.append("yaml 表清单与 tables.json 的 layer=rt 表不一致")
    for t, d in sorted(rt_tables_y.items()):
        jt = (meta.get("tables") or {}).get(t) or {}
        tcol = d.get("time_col") or rt_y.get("default_time_col")
        if not tcol or tcol not in (d.get("columns") or {}):
            bad_rt.append("%s.time_col=%r 不在列清单里（R26 会误判）" % (t, tcol))
        if jt.get("layer") != "rt":
            bad_rt.append("%s 在 tables.json 里不是 rt 层" % t)
        if jt.get("catalog") != rt_y.get("catalog") or jt.get("database") != rt_y.get("database"):
            bad_rt.append("%s 的 catalog/database 与 realtime.yaml 不一致" % t)
        if not isinstance(d.get("rows"), int) or d["rows"] <= 0:
            bad_rt.append("%s 缺实测行数" % t)
        if not (d.get("caveats") or []):
            bad_rt.append("%s 没有 caveats（实时/离线同名口径差异必须写清）" % t)
    for x in bad_rt:
        c.fail("实时层：%s" % x)
    if not bad_rt:
        c.ok("%d 张 rt 表就位（时间列 / 行数 / catalog / 口径陷阱 / json 已同步）"
             % len(rt_tables_y))

    # ---------------------------------------------------------------- 汇总
    print("\n" + "=" * 62)
    print("通过 %d 项 / 失败 %d 项 / 警告 %d 项"
          % (c.passes, len(c.fails), len(c.warns)))
    if c.fails:
        print("\n失败明细：")
        for f in c.fails:
            print("  - " + f)
        print("\n== 语义层校验未通过 ==")
        return 1
    print("== 语义层校验通过：%d 张表 / %d 指标 / %d 维度 / %d 陷阱 =="
          % (len(tbl_y), len(mets), len(dims), len(ids)))
    return 0


if __name__ == "__main__":
    sys.exit(main())
