"""确定性解读：从结果集本身算出「这句话值得说的那一句」。

为什么不用模型写解读
--------------------
让模型解读数字，等于让它编。它会说「环比增长显著」「说明用户黏性提升」——
这些句子里没有一个是**算出来的**，却和算出来的数字印在同一段文字里，
用户无法分辨哪句可复算。docs/23 §5.4 的取舍就是：不解读。

但「只说数字」也确实死板。用户看到的是一张表，还得自己做减法。
解法不是让模型去编，而是把**本来就能确定性算出来的观察**补齐：

  · 榜首是谁、占多少、和第二名差多少 —— 从行里算，可复算
  · 首日到末日的涨跌幅度 —— 从行里算，可复算
  · 占比换算成百分点 —— 乘 100，可复算

这些属于"把数字翻译成人话"，不属于"解释为什么"。边界就在这里：
**任何一个字都必须能从 result 反推出来**，否则不写。

单位换算与格式统一放在这里，narrate 只负责排版。
"""
from __future__ import annotations

import re

_RE_DATE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def _is_num(v) -> bool:
    return isinstance(v, (int, float)) and not isinstance(v, bool)


def _fmt(v) -> str:
    if isinstance(v, bool):
        return str(v).lower()
    if isinstance(v, int):
        return format(v, ",")
    if isinstance(v, float):
        s = ("%.4f" % v).rstrip("0").rstrip(".")
        return s if s else "0"
    if v is None:
        return "NULL"
    return str(v)


def _pct(part, whole) -> str:
    try:
        if not whole:
            return "—"
        return "%.1f%%" % (100.0 * float(part) / float(whole))
    except (TypeError, ValueError, ZeroDivisionError):
        return "—"


def _axes(columns: list[str], rows: list[list]) -> tuple[int | None, int | None]:
    """找出「实体列」和「数值列」。

    不能假设第 0 列是实体、第 1 列是值——模型写 SQL 时列顺序是自由的
    （`SELECT SUM(x) AS v, genre` 完全合法）。按类型猜比按位置猜稳：
    数值列 = 第一列「全是数字」的；实体列 = 第一列「含有非空字符串」的。

    实体列优先跳过 `xxx_id` 这类标识列：榜单上写 `S000014` 对读者没有意义，
    要的是 `半島鐵盒`。只有在没有可读列时才退回去用 id。
    """
    if not rows:
        return None, None
    n = min(len(columns), min(len(r) for r in rows)) if rows else 0
    if n == 0:
        return None, None

    num_cols = [i for i in range(n)
                if all(_is_num(r[i]) or r[i] is None for r in rows)]
    str_cols = [i for i in range(n) if i not in num_cols
                and any(isinstance(r[i], str) and r[i] for r in rows)]

    val = num_cols[0] if num_cols else None
    readable = [i for i in str_cols
                if "id" not in str(columns[i]).lower()]
    lab = (readable or str_cols or [None])[0]
    if lab is None and val != 0:
        lab = 0
    return lab, val


def _rank_insight(columns: list[str], rows: list[list]) -> list[str]:
    lab, val = _axes(columns, rows)
    if val is None:
        return []
    name = columns[val] if val < len(columns) else "值"
    name_lab = columns[lab] if lab is not None and lab < len(columns) else "项"
    try:
        total = sum(float(r[val]) for r in rows if _is_num(r[val]))
    except (TypeError, ValueError):
        total = 0.0

    out = []
    top = rows[0]
    if lab is not None and top[lab] is not None:
        if total > 0:
            out.append("榜首是「%s」，%s %s，占所选 %d 项合计的 %s（合计 %s）"
                       % (_fmt(top[lab]), name, _fmt(top[val]), len(rows),
                          _pct(top[val], total), _fmt(total)))
        else:
            out.append("榜首是「%s」，%s %s"
                       % (_fmt(top[lab]), name, _fmt(top[val])))

    if len(rows) >= 2 and lab is not None:
        try:
            gap = float(top[val]) - float(rows[1][val])
            if gap > 0:
                out.append("比第二名（%s，%s）多 %s，相当于榜首的 %s"
                           % (_fmt(rows[1][lab]), _fmt(rows[1][val]), _fmt(round(gap, 4)),
                              _pct(gap, top[val])))
        except (TypeError, ValueError):
            pass
    return out


def _trend_insight(columns: list[str], rows: list[list]) -> list[str]:
    if not rows:
        return []
    n = min(len(columns), min(len(r) for r in rows))

    # 日期列：优先找形如 YYYY-MM-DD 的列；找不到就按第一列当序列轴
    x_idx = None
    for i in range(n):
        if isinstance(rows[0][i], str) and _RE_DATE.match(str(rows[0][i])):
            x_idx = i
            break
    if x_idx is None:
        x_idx = 0
    y_idx = None
    for i in range(n):
        if i != x_idx and all(_is_num(r[i]) or r[i] is None for r in rows):
            y_idx = i
            break
    if y_idx is None:
        return []

    pts = [(str(r[x_idx]), float(r[y_idx])) for r in rows if _is_num(r[y_idx])]
    if len(pts) < 2:
        return []
    peak = max(pts, key=lambda p: p[1])
    low = min(pts, key=lambda p: p[1])
    first, last = pts[0], pts[-1]
    delta = last[1] - first[1]
    arrow = "上升" if delta > 0 else ("下降" if delta < 0 else "持平")
    out = ["区间内最高 %s（%s），最低 %s（%s）"
           % (peak[0], _fmt(peak[1]), low[0], _fmt(low[1]))]
    if first[1]:
        out.append("首日 %s → 末日 %s，%s %s（%s）"
                   % (first[0], last[0], arrow, _fmt(round(delta, 4)),
                      _pct(delta, first[1]) if delta else "0.0%"))
    else:
        out.append("首日 %s → 末日 %s，%s %s"
                   % (first[0], last[0], arrow, _fmt(round(delta, 4))))
    return out


def build(res: dict, contract, extra: dict | None = None) -> list[str]:
    """返回若干条可复算的观察。空列表 = 这条结果没什么可说的。

    extra 里目前只有一个键：days（窗口天数）。多日窗口的单值题补一句日均——
    「上周播放量 89,250」之后用户下一个问题一定是"平均每天多少"，
    而这是除法，能算就不该让人自己算。
    """
    columns = res.get("columns") or []
    rows = res.get("rows") or []
    if not columns or not rows:
        return []
    shape = getattr(contract, "shape", "scalar")

    if shape in ("ranking", "distribution") and len(rows) >= 1:
        return _rank_insight(columns, rows)
    if shape == "trend":
        return _trend_insight(columns, rows)
    if shape == "ratio" and len(rows) == 1 and len(rows[0]) == 1:
        v = rows[0][0]
        if _is_num(v):
            # 0~1 之间才当"比例"换算成百分点；ARPU 这类均值不做换算
            if 0 <= float(v) <= 1:
                return ["即 %.2f%%（比例值 %s）" % (float(v) * 100, _fmt(v))]
            return []
    if shape == "argmax" and len(rows) >= 1:
        lab, val = _axes(columns, rows)
        if lab is not None and val is not None and rows[0][lab] is not None:
            return ["最值项：%s = %s" % (_fmt(rows[0][lab]), _fmt(rows[0][val]))]
    if shape == "scalar" and len(rows) == 1 and len(rows[0]) >= 1:
        days = int((extra or {}).get("days") or 1)
        v = rows[0][0]
        # 多日窗口才补日均；单日窗口说"日均"是废话。比率型单值不进这里
        # （scalar 只覆盖计数/金额这类可加量）。
        if days > 1 and _is_num(v):
            return ["%d 天窗口，日均 %s（合计 ÷ %d）"
                    % (days, _fmt(round(float(v) / days, 2)), days)]
    return []
