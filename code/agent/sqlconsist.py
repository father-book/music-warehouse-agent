"""基线 SQL 与当前 SQL 的「口径一致性」检查。

问题
----
对比题（"比上周多了多少"）的基线值不是模型算的：系统把当前 SQL 里的日期字面量
平移到基线窗口，**再查一遍**，差值取减法。这条路成立的前提是——两条 SQL
除了"时间窗口不同"，其它一切都相同。

这个前提并不自动成立。模型完全可能写出：

    WHERE dt = '2026-11-08' AND date_format(dt, '%Y%m%d') >= '20260101'

平移只认 `'YYYY-MM-DD'` 这一种形态，第二个字面量不动。于是基线那条 SQL
的过滤条件留在原地，两边不再是"同一口径的两个窗口"。最坏的是它**不报错**：
基线值偏小或偏大，差值看起来是个正常数字，没有任何护栏能发现。

所以这里做的是**证明**而不是**假设**：拿两条 SQL 的 AST 做字面量掩码后逐字比，
再逐一对账每个字面量——结构必须一模一样，日期必须恰好平移了 delta 天，
非日期字面量必须原样。任一条对不上就判 unsafe，宁可不给差值。

四种结论
--------
ok       结构一致 + 日期恰好平移 + 无相对时间函数 → 差值可信
warn     结构一致，但有落在窗口内部（非边界）的日期字面量。
         这种字面量可能是"业务常量"（活动日、发布日）而不是时间窗口，
         平移后语义会变，无法从 SQL 本身判断 → 给出差值但明确标注仅供参考。
unsafe   有具体原因（某条字面量没对上 / 结构不一致 / 用了 NOW()）→ 不给差值
unknown  解析失败 → 不给差值。**无法证明就是不可信**，不给数字比给个错的强。

为什么不用 `_shift_sql` 自己保证
------------------------------
`_shift_sql` 是"尽力平移"，它不知道平移的结果是否还成立；它的返回值只说
"我干没干成"，不说"干完之后是否等价"。两件事必须分开测，否则就成了自己
给自己出题、自己判卷。
"""
from __future__ import annotations

import _bootstrap  # noqa: F401

import re
from datetime import date, datetime, timedelta

import sqlglot
from sqlglot import expressions as exp

DIALECT = "hive"        # 与护栏（agent/guards/guard.py）保持同一方言

# 日期字面量的四种常见写法。**必须比 _shift_sql 认得多**：
# 它的盲区正是这里要抓的东西，认的一样就等于没检查。
_RE_ISO = re.compile(r"^(\d{4})-(\d{2})-(\d{2})$")
_RE_ISO_T = re.compile(r"^(\d{4})-(\d{2})-(\d{2})[ T](\d{2}):(\d{2})(?::(\d{2}))?")
_RE_COMPACT = re.compile(r"^(\d{4})(\d{2})(\d{2})$")
_RE_SLASH = re.compile(r"^(\d{4})/(\d{1,2})/(\d{1,2})$")
_RE_MONTH = re.compile(r"^(\d{4})-(\d{2})$")

# 相对时间函数：两条 SQL 会在**同一时刻**执行，所以这类函数取到的是同一段时间，
# 平移根本没发生。差值为 0 会被当成"没有变化"报出去——这是最像真的假答案。
_RELATIVE_NAMES = {
    "now", "curdate", "curtime", "current_date", "current_time",
    "current_timestamp", "localtime", "localtimestamp", "sysdate",
    "unix_timestamp", "utc_timestamp", "today",
}


def _parse_dateish(s: str):
    """能认成"某一天"就返回 date，否则 None。时间部分单独处理。"""
    s = (s or "").strip()
    for rx, order in ((_RE_ISO, ("y", "m", "d")), (_RE_SLASH, ("y", "m", "d"))):
        m = rx.match(s)
        if m:
            y, mo, d = (int(x) for x in m.groups())
            try:
                return date(y, mo, d)
            except ValueError:
                return None
    m = _RE_ISO_T.match(s)
    if m:
        y, mo, d = (int(x) for x in m.groups()[:3])
        try:
            return date(y, mo, d)
        except ValueError:
            return None
    m = _RE_COMPACT.match(s)
    if m:
        y, mo, d = (int(x) for x in m.groups())
        try:
            return date(y, mo, d)
        except ValueError:
            return None
    return None


def _time_part(s: str) -> str:
    """取时间部分的原文（'00:00:00'）。没有时间部分就返回空串。"""
    m = _RE_ISO_T.match((s or "").strip())
    if not m:
        return ""
    hh, mm, ss = m.group(4), m.group(5), m.group(6) or "00"
    return "%s:%s:%s" % (hh, mm, ss)


def _exprs(sql: str):
    """解析成语句列表。只接受单条语句——多条（分号串联）无法对齐平移。"""
    try:
        nodes = sqlglot.parse(sql, read=DIALECT)
    except Exception:
        return None, "SQL 解析失败"
    nodes = [n for n in nodes if n is not None]
    if len(nodes) != 1:
        return None, "不是单条语句（%d 条），无法逐语句对齐" % len(nodes)
    return nodes[0], ""


def _masked(ast) -> str:
    """把所有字面量换成同一个占位符后的 SQL 文本。

    这样得到的字符串相同 ⇔ 两条 SQL 的**结构**相同（表、列、函数、连接、
    聚合、分组、排序、行数限制都一样），差异只可能出在字面量上。
    用渲染后的文本比而不是比 AST 对象，是为了让不一致时能直接给人看。
    """
    a = ast.copy()
    for lit in a.find_all(exp.Literal):
        lit.set("this", "·")
        lit.set("is_string", True)
    try:
        return a.sql(dialect=DIALECT)
    except Exception:
        return repr(a)


def _literals(ast) -> list[dict]:
    out = []
    for lit in ast.find_all(exp.Literal):
        out.append({"v": str(lit.args.get("this")),
                    "str": bool(lit.args.get("is_string"))})
    return out


def _relative_hits(ast) -> list[str]:
    """找出相对时间函数。返回命中的名字（去重、保序）。

    名字用 SQL 里的写法而不是 AST 类名：这条消息会进答案给人看，
    `CurrentDate` / `currentdate` 都不如 `current_date` 眼熟。
    """
    named = {exp.CurrentDate: "current_date",
             exp.CurrentTimestamp: "current_timestamp",
             exp.CurrentTime: "current_time"}
    hits: list[str] = []
    for node in ast.walk():
        name = ""
        for cls, sql_name in named.items():
            if isinstance(node, cls):
                name = sql_name
                break
        if not name and isinstance(node, exp.Anonymous):
            n = str(node.args.get("this") or "").lower()
            if n in _RELATIVE_NAMES:
                name = n
        if not name and isinstance(node, exp.Func) and not isinstance(node, exp.Anonymous):
            n = node.sql_name().lower() if hasattr(node, "sql_name") else ""
            if n in _RELATIVE_NAMES:
                name = n
        if name and name not in hits:
            hits.append(name)
    return hits


def check(cur_sql: str, base_sql: str,
          cur_window: tuple[str, str], base_window: tuple[str, str]) -> dict:
    """核心入口。返回 {status, reasons, notes, detail}。

    `reasons` 是"为什么不可信"，会原样进答案的"对比"段，所以必须写得让人看懂；
    `notes` 是"可信但有保留"，进 trace 与答案的括注。
    """
    out = {"status": "unknown", "reasons": [], "notes": [], "message": "",
           "detail": {}}

    def unsafe(message: str, *reasons: str) -> dict:
        # unsafe = 查清了，就是不成立；unknown = 没查清。
        # 两者都不给差值，但前者能一句话说清给用户听（message），
        # 后者只能写"无法核对"。reasons 是排障用的细节，不直接进答案：
        # "第 2 个日期字面量 20261108 应平移到…"这种话分析师看不懂。
        out["status"] = "unsafe"
        out["message"] = message
        out["reasons"] = [r for r in reasons if r] or [message]
        return out

    try:
        bs, be = date.fromisoformat(str(base_window[0])), date.fromisoformat(str(base_window[1]))
        cs, ce = date.fromisoformat(str(cur_window[0])), date.fromisoformat(str(cur_window[1]))
    except (ValueError, TypeError):
        out["reasons"] = ["窗口日期格式不对，无法核对"]
        return out
    delta = (bs - cs).days
    out["detail"]["delta_days"] = delta

    ca, why = _exprs(cur_sql)
    ba, why2 = _exprs(base_sql)
    if ca is None or ba is None:
        out["reasons"] = [why or why2 or "SQL 解析失败"]
        return out

    # ① 结构：掩掉字面量后必须逐字相同
    mc, mb = _masked(ca), _masked(ba)
    if mc != mb:
        out["detail"].update({"cur_shape": mc[:300], "base_shape": mb[:300]})
        return unsafe("基线与当前 SQL 的结构不一致（表、列、聚合或行数限制不同），"
                      "平移只能改日期，保证不了同口径",
                      "结构差异：%s → %s" % (mc[:120], mb[:120]))

    # ② 相对时间函数：两条 SQL 同一时刻执行，取到的是同一段时间
    rel_c, rel_b = _relative_hits(ca), _relative_hits(ba)
    if rel_c or rel_b:
        names = "、".join(sorted(set(rel_c) | set(rel_b)))
        return unsafe("SQL 用了相对时间函数（%s），两条 SQL 在同一时刻执行，"
                      "查的是同一段时间，等于没做对比" % names,
                      "相对时间函数：" + names)

    # ③ 逐字面量对账
    lc, lb = _literals(ca), _literals(ba)
    out["detail"]["literals"] = len(lc)
    if len(lc) != len(lb):
        return unsafe("两边字面量个数不同（%d vs %d），结构对不上"
                      % (len(lc), len(lb)))

    shifted, interior, bad, bad_lits = 0, [], [], []
    for i, (x, y) in enumerate(zip(lc, lb)):
        dx, dy = _parse_dateish(x["v"]), _parse_dateish(y["v"])
        if dx and dy:
            want = dx + timedelta(days=delta)
            if dy != want:
                bad_lits.append(x["v"])
                bad.append("第 %d 个日期字面量 %s 应平移到 %s，实际是 %s"
                           % (i + 1, x["v"], want.isoformat(), y["v"]))
            elif _time_part(x["v"]) != _time_part(y["v"]):
                bad_lits.append(x["v"])
                bad.append("第 %d 个日期字面量的时间部分变了（%s → %s）"
                           % (i + 1, _time_part(x["v"]) or "无", _time_part(y["v"]) or "无"))
            else:
                shifted += 1
                if cs < dx < ce:        # 严格落在窗口内部：可能是业务常量而非边界
                    interior.append(x["v"])
        elif dx or dy:
            bad_lits.append(x["v"])
            bad.append("第 %d 个字面量只有一边像日期（%s vs %s）" % (i + 1, x["v"], y["v"]))
        elif x["v"] != y["v"] or x["str"] != y["str"]:
            bad_lits.append(x["v"])
            bad.append("第 %d 个字面量被改动了（%s → %s）" % (i + 1, x["v"], y["v"]))
    out["detail"]["shifted"] = shifted
    if bad:
        return unsafe("SQL 里有一处日期写法没跟着平移（%s），基线窗口没变，"
                      "两条 SQL 不是同一段时间" % ("「%s」" % bad_lits[0]), *bad[:3])
    if shifted == 0:
        return unsafe("SQL 里没有可平移的日期字面量，基线会查到同一段数据")

    # ④ 内部字面量：能平移但语义可能变（活动日 vs 时间窗口），如实标注
    out["status"] = "warn" if interior else "ok"
    if interior:
        out["notes"] = ["有 %d 个日期字面量落在窗口内部（%s），若它是业务常量而非"
                        "时间边界，平移后口径会变" % (len(interior), "、".join(interior[:3]))]
        out["detail"]["interior"] = interior
    return out


def ok(cons: dict) -> bool:
    return cons.get("status") in ("ok", "warn")
