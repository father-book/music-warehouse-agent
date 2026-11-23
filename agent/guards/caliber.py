#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""口径机检 —— 语义级护栏（规则定义在 metrics.yaml 的 caliber_guard）。

为什么需要它：
  R1~R24 检查的是「这条 SQL 会不会跑错 / 越权 / 编造表」。它们全绿并不代表
  数是对的。2026-11-09 的首版收入 CTAS：语法对、表在语义层、EXPLAIN 通过，
  但只取了 event_name='order_pay'，漏掉占毛收入 82% 的 VIP 订阅，
  算出 156,986 vs 真值 823,410（低 5.24 倍）。当时是靠人工复核拦下的——
  人工复核不能每次都指望，所以把那次判断固化成这里的规则。

设计取舍：
  · 规则是**数据**（metrics.yaml），不是代码。新增一个口径坑只加一段 YAML，
    两条链路（问数 R25 / ETL etlguard）自动同时生效。
  · 只做静态可判的部分：事件集合覆盖、退款符号、去重实体的事件流来源，
    以及比率分母 / 去重实体的量纲（2026-11-14 新增）。需要业务判断的
    （例如「这个月要不要算试用期收入」）留给人工。
  · 老三条一律 WARN（口径偏离有时是有意的选择）；C4/C5 这两条是
    **确定性错误**——分母用错事件流、把笔数当人数，不存在"有意这样算"的解释，
    所以设 REJECT 并尽量给出可机械执行的 fix，让生成节点 0 token 改对。
    判断标准：能不能说出「这样算也有业务含义」；能说出的用 WARN，说不出的才用 REJECT。
  · 兼容 sqlglot 6.3（集群 Python 3.6）与 30.x（本地）：只用两版都稳定的节点。
"""
from __future__ import annotations

import re

from sqlglot import exp

_URL = re.compile(r"count\s*\(\s*distinct\s+(?:coalesce\s*\(\s*nullif\s*\(\s*)?(user_id|device_id)",
                  re.IGNORECASE)
# 裸行数：COUNT(*) / COUNT(1)。带 DISTINCT 的不是笔数，是实体数，不算错。
_RE_COUNT_ROWS = re.compile(r"count\s*\(\s*(?:\*|1)\s*\)", re.IGNORECASE)
_RE_DT = re.compile(r"\bdt\s*=\s*'(\d{4}-\d{2}-\d{2})'", re.IGNORECASE)


# ------------------------------------------------------------------ AST 小工具

def _lit(node):
    """字符串字面量的值；不是字面量返回 None。"""
    try:
        if isinstance(node, exp.Literal) and getattr(node, "is_string", False):
            return str(node.this)
    except Exception:
        pass
    return None


def _derived_names(stmt):
    """CTE 名 + 子查询别名：它们会出现在 FROM 里，但不是实体表。"""
    names = set()
    try:
        for c in stmt.find_all(exp.CTE):
            if c.alias:
                names.add(str(c.alias).lower())
        for s in stmt.find_all(exp.Subquery):
            if s.alias:
                names.add(str(s.alias).lower())
    except Exception:
        pass
    return names


def _tables(stmt):
    names = set()
    for t in stmt.find_all(exp.Table):
        n = str(getattr(t, "name", "") or "").lower()
        if n:
            names.add(n)
    return names - _derived_names(stmt)


def _event_preds(stmt, col="event_name"):
    """收集 event_name 与字符串字面量的比较。返回 (包含的事件, 排除的事件)。

    覆盖 `= 'x'` / `<> 'x'` / `IN (...)` / `NOT IN (...)`。
    CASE WHEN 里的条件同样会被 find_all 走到，不需要单独处理。
    """
    inc, exc = set(), set()
    try:
        for node in stmt.find_all(exp.EQ, exp.NEQ):
            left, right = node.this, node.expression
            val, neg = None, isinstance(node, exp.NEQ)
            if isinstance(left, exp.Column) and left.name.lower() == col:
                val = _lit(right)
            elif isinstance(right, exp.Column) and right.name.lower() == col:
                val = _lit(left)
            if val:
                (exc if neg else inc).add(val)
    except Exception:
        pass
    try:
        for node in stmt.find_all(exp.In):
            this = node.this
            if not (isinstance(this, exp.Column) and this.name.lower() == col):
                continue
            vals = {v for v in (_lit(e) for e in (node.expressions or [])) if v}
            neg = isinstance(getattr(node, "parent", None), exp.Not)
            (exc if neg else inc).update(vals)
    except Exception:
        pass
    return inc, exc


def _sums_of(stmt, column):
    """引用了某一列的 SUM 节点。SUM(CASE WHEN ... THEN amount ...) 也算。"""
    out = []
    for sm in stmt.find_all(exp.Sum):
        for c in sm.find_all(exp.Column):
            if str(getattr(c, "name", "") or "").lower() == column:
                out.append(sm)
                break
    return out


def _is_negated(sm, column):
    """SUM 的参数里有没有对目标列取负（`-amount`、`amount * -1`）。"""
    for n in sm.find_all(exp.Neg):
        for c in n.find_all(exp.Column):
            if str(getattr(c, "name", "") or "").lower() == column:
                return True
    return False


def _groups_by(stmt, col):
    for g in stmt.find_all(exp.Group):
        for e in (g.expressions or []):
            if isinstance(e, exp.Column) and str(getattr(e, "name", "")).lower() == col:
                return True
    return False


def _sql_text(stmt):
    try:
        return stmt.sql()
    except Exception:
        return ""


def _div_denominators(stmt):
    """所有除法节点的分母表达式（sqlglot 的 Binary 用 this / expression）。"""
    out = []
    for d in stmt.find_all(exp.Div):
        den = getattr(d, "expression", None) or getattr(d, "right", None)
        if den is not None:
            out.append(den)
    return out


def _count_rows_denominator(stmt):
    """找出「分母是裸 COUNT(*) / COUNT(1)」的除法，没有就返回 None。"""
    for den in _div_denominators(stmt):
        s = den.sql()
        if _RE_COUNT_ROWS.search(s) and "DISTINCT" not in s.upper():
            return den
    return None


# ------------------------------------------------------------------ 三类检查

def _finding(spec, message, fix_target, fix_replacement, op=None):
    """op=rewrite 表示无法机械替换，交给生成节点带 hint 重写（target 留空）。"""
    return {
        "level": spec.get("level") or "WARN",
        "rule": spec.get("rule") or "R25-口径",
        "message": message,
        "hint": spec.get("hint") or "",
        "fix": {"op": op or "replace", "target": fix_target,
                "replacement": fix_replacement or spec.get("replacement") or ""},
    }


def _check_coverage(stmt, spec, tables):
    """金额求和是否覆盖了指标要求的全部事件。"""
    table = str(spec.get("table") or "").lower()
    col = str(spec.get("agg_column") or "amount").lower()
    if table not in tables:
        return []
    sums = _sums_of(stmt, col)
    if not sums:
        return []                       # 没做金额聚合，不判
    if _groups_by(stmt, "event_name"):
        return []                       # 按事件分组是显式的「分事件看金额」
    inc, exc = _event_preds(stmt)
    if not inc:
        # 一个事件条件都没有：可能是全量求和（C2 会管退款），这里不重复报
        return []
    required = set(spec.get("require_events") or [])
    if not (inc & required):
        # 连一个收入事件都没命中（例如只 SUM 退款）：这不是在算收入，
        # 归 C2 管，C1 不再叠一条"缺 VIP"，避免同一处错误报两遍。
        return []
    missing = [e for e in (spec.get("require_events") or []) if e not in inc]
    if not missing:
        return []
    msg = ("表 %s 上对 %s 求和，事件只命中 %s，缺 %s —— %s"
           % (table, col, "、".join(sorted(inc)), "、".join(missing),
              spec.get("why") or "指标口径要求覆盖这些事件"))
    return [_finding(spec, msg, sums[0].sql(), spec.get("replacement"))]


def _check_refund(stmt, spec, tables):
    """退款是否被按正数加进金额（退款事件与收入同表、amount 为正）。"""
    table = str(spec.get("table") or "").lower()
    col = str(spec.get("agg_column") or "amount").lower()
    refund = str(spec.get("refund_event") or "order_refund").lower()
    if table not in tables:
        return []
    sums = _sums_of(stmt, col)
    if not sums:
        return []
    if _groups_by(stmt, "event_name"):
        # 按事件分组的金额明细：退款自己占一行，不存在"混加"，不判
        return []
    inc, exc = _event_preds(stmt)
    has_pred = bool(inc or exc)
    refund_included = refund in inc
    refund_handled = refund in exc or (refund_included and any(
        _is_negated(sm, col) for sm in sums))

    if refund_handled:
        return []
    if refund_included and not any(_is_negated(sm, col) for sm in sums):
        msg = ("表 %s 上 %s 求和把 %s 按正数加了：退款金额本身为正，必须手工取负 —— %s"
               % (table, col, refund, spec.get("why") or ""))
    elif not has_pred:
        msg = ("表 %s 上对 %s 全量求和，没有排除 %s：退款会被当成收入加进来 —— %s"
               % (table, col, refund, spec.get("why") or ""))
    else:
        return []
    return [_finding(spec, msg, sums[0].sql(), spec.get("replacement"))]


def _check_source(stmt, spec, tables):
    """去重实体计数是否用了错误的事件流。"""
    table = str(spec.get("table") or "").lower()
    if table not in tables:
        return []
    right = str(spec.get("right_table") or "").lower()
    if right and right in tables:
        return []                       # 同时引用了正确源表，可能是对照，不判
    text = _sql_text(stmt)
    if not _URL.search(text):
        return []                       # 不是「去重实体计数」，不判
    msg = ("对 %s 做去重用户计数：这是错误的事件流，应改用 %s —— %s"
           % (table, right or "指标声明的源表", spec.get("why") or ""))
    return [_finding(spec, msg, table, spec.get("replacement") or right)]


def _check_denominator(stmt, spec, tables):
    """比率的分母必须来自声明的事实表，不能是当前表的行数。

    2026-11-14 实测 V16：完播率写成
      SUM(CASE WHEN play_progress>=0.9 THEN 1 ELSE 0 END) / COUNT(*)
      FROM dwd_play_end
    分母成了「播放结束行数」312,083，算出 0.7341；真值是
    complete_cnt / play_start_cnt = 229,038 / 445,945 = 0.5143。
    promptlib 里写了两条硬提示，模型仍有 1/5 概率踩中——所以做成 REJECT。

    只判「分母是裸 COUNT(*) 且 SQL 没引用正确分母来源表」这一种确定性错法：
    正确写法（分子分母各自标量子查询）会同时出现 dwd_play_end 和 dwd_play_start，
    一条规则都不会误伤。
    """
    table = str(spec.get("table") or "").lower()
    if table not in tables:
        return []
    right = str(spec.get("right_table") or "").lower()
    if right and right in tables:
        return []                       # 已引用声明的分母来源表，写法正确
    text = _sql_text(stmt)
    indicator = str(spec.get("indicator") or "").lower()
    if indicator and indicator not in text.lower():
        return []                       # 不是在算这个指标（例如只是行数占比）
    bad = _count_rows_denominator(stmt)
    if bad is None:
        return []
    msg = ("在 %s 上算比率，分母却是本表的 COUNT(*)（笔数/行数），"
           "而不是 %s —— %s"
           % (table, spec.get("right_denominator") or "指标声明的分母",
              spec.get("why") or ""))
    m = _RE_DT.search(text)
    tmpl = str(spec.get("fix_template") or "")
    if m and tmpl:
        # 机械替换：把分母表达式换成声明来源表的标量子查询，0 token 改对，
        # 复检时 SQL 里出现 right_table，本规则自动放行。
        return [_finding(spec, msg, bad.sql(), tmpl.format(dt=m.group(1)))]
    return [_finding(spec, msg, "", "", op="rewrite")]


def _check_dedup_entity(stmt, spec, tables):
    """「用户占比」的分母必须是去重人数，不能是支付笔数。

    2026-11-14 实测 C19：VIP 占比写成 dwd_pay 上
      SUM(is_vip='true') / COUNT(*) = 99 / 1,046 = 0.0946
    分母是支付笔数，真值是 vip_user_cnt / dau = 317 / 3,237 = 0.0979。
    语义层已补 vip_user_ratio 指标（3/3 通过），这条规则用于防回归。
    """
    table = str(spec.get("table") or "").lower()
    if table not in tables:
        return []
    right = str(spec.get("right_table") or "").lower()
    if right and right in tables:
        return []
    text = _sql_text(stmt)
    indicator = str(spec.get("indicator") or "").lower()
    if indicator and indicator not in text.lower():
        return []
    if "COUNT(DISTINCT" in text.upper().replace(" ", ""):
        return []                       # 已经按实体去重了
    bad = _count_rows_denominator(stmt)
    if bad is None:
        return []
    msg = ("在 %s 上算占比，分母是支付行数而非去重人数（%s）；"
           "应改用 %s 的用户级汇总 —— %s"
           % (table, bad.sql(), right or "指标声明的来源表",
              spec.get("why") or ""))
    # 分子也错（笔数而非人数），单点替换改不对，交给模型按 hint 整句重写。
    return [_finding(spec, msg, "", "", op="rewrite")]


def _ratio_col_text(node):
    """取表达式里第一个列引用的原始名（不做别名归一化——C15 的错法
    恰恰是别名背后换了来源层，归一化反而会把错写法放行）。"""
    try:
        col = node.find(exp.Column)
    except Exception:
        return None
    if col is None:
        return None
    return (col.name or "").lower() or None


def _check_ratio_consistency(stmt, spec, tables):
    """占比的分子分母必须同列同源。

    2026-11-15 实测 C15：省占比写成
      MAX(province_play) / NULLIF(SUM(play_start_cnt), 0)
    分子取自「按省聚合」的子查询列 province_play，分母却另起一个
    CROSS JOIN 标量子查询取 ads_province_dist 的 SUM(play_start_cnt)
    —— 两层来源对不上，算出 0.0029，真值 0.0905
    （正确写法：MAX(province_play)/SUM(province_play)，同名同源）。

    确定性判据：除法的分子列与分母列**原始列名**必须一致。
    触发条件收紧到「SQL 引用声明的省份聚合表」：ctr / ARPU 这类
    天然异列的比率不在这张表上算（回放 0 误伤后才上线）。
    注意不做别名归一化：province_play 与 play_start_cnt 物理列虽同名，
    但来自不同聚合层，正是要拦的错法。
    """
    wanted = [str(t).lower() for t in (spec.get("tables") or [spec.get("table") or ""]) if t]
    if not (set(wanted) & tables):
        return []
    for d in stmt.find_all(exp.Div):
        num = getattr(d, "this", None)
        den = getattr(d, "expression", None) or getattr(d, "right", None)
        nc, dc = _ratio_col_text(num), _ratio_col_text(den)
        if nc and dc and nc != dc:
            return [_finding(
                spec,
                "占比的分子列(%s)与分母列(%s)不同源 —— %s"
                % (nc, dc, spec.get("why") or ""),
                "", "", op="rewrite")]
    return []


_KINDS = {
    "event_coverage": _check_coverage,
    "refund_sign": _check_refund,
    "source_guard": _check_source,
    "denominator": _check_denominator,
    "dedup_entity": _check_dedup_entity,
    "ratio_consistency": _check_ratio_consistency,
}


def check(stmts, specs):
    """对一批已解析的语句跑全部口径规则，返回 findings（与 guard.py 同结构）。"""
    out = []
    for spec in (specs or []):
        fn = _KINDS.get(str(spec.get("kind") or ""))
        if not fn:
            continue
        seen = set()                    # 同一规则在一条 SQL 里只报一次
        for stmt in stmts or []:
            try:
                tables = _tables(stmt)
            except Exception:
                continue
            for f in fn(stmt, spec, tables):
                key = (f["rule"], f["message"])
                if key in seen:
                    continue
                seen.add(key)
                out.append(f)
    return out


def covered_rules(specs):
    """给前端/文档看的规则清单（id + title）。"""
    return [(s.get("id") or "?", s.get("title") or s.get("rule") or "")
            for s in (specs or [])]
