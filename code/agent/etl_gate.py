"""金额类 ETL 的执行闸门 —— 二次确认（不是拦截，是"让人签个字"）。

为什么需要它：
  2026-11-11 的口径机检（R25 / caliber_ok）把"漏 VIP 订阅"这类错从**无人发现**
  变成**黄色告警**，但告警不阻断执行——工程师看到一个 WARN 照样能点"执行建表"，
  一秒之后 5.24 倍的错数就落进了 dev 表，下游看板直接读它。告警的价值取决于
  有没有人真的读它。

  所以对**金额类**生成物加一道门：第一次点执行不跑，先把风险摊开让人确认；
  确认令牌绑定"这条记录 + 这份 SQL"，换 SQL 就失效——防止"上次点过了"变成
  永久免检。

边界（为什么只对金额类）：
  · 金额错一次的影响是数量级的（少算 82% 收入），维度/计数类错了肉眼能看出来；
  · 每次执行都要二次确认会把确认变成肌肉记忆，反而降低警惕（告警疲劳）；
  · 判定只看 SQL 文本与语义层元数据，不联网、不跑 EXPLAIN，毫秒级。

判定不追求"精确识别所有金额"，只追求"宁可多问一句"：
  M1 金额列聚合（SUM/AVG 打在 amount/amt/price/gmv/... 上）
  M2 收入语义域的表（dwd_pay / ads_revenue / dws_revenue_daily / dev_*revenue*）
  M3 输出列名像金额（net_rev / gmv / income / 收入 / 金额 ...）
  M4 生成时的口径机检已经报过 WARN（把已知风险原样带进确认页）
"""
from __future__ import annotations

import hashlib
import hmac
import datetime as _dt
import re

import sqlglot
from sqlglot import exp

# 金额/度量列名。用分段匹配而不是裸子串：rev_ 会误伤 review，amt 会误伤 amount 以外
# 的缩写，所以统一按"下划线分段"判断。
_MONEY_SEG = {
    "amount", "amt", "price", "fee", "money", "gmv", "revenue", "rev",
    "income", "payment", "cost", "profit", "refund", "pay",
}
# 收入语义域的表（命中即提示"这张表的金额口径有历史坑"）
_MONEY_TABLE = re.compile(
    r"^(dwd_pay|ads_revenue|dws_revenue_daily|dwd_refund)$|revenue|_pay$", re.I)
_CN_MONEY = re.compile(r"收入|金额|销售额|营收|流水|退款额|客单价")


def _segments(name: str) -> list[str]:
    return [s for s in re.split(r"[^a-z0-9]+", (name or "").lower()) if s]


def is_money_name(name: str) -> bool:
    """列名是否像金额。分段匹配 + 中文关键词兜底。"""
    if not name:
        return False
    if _CN_MONEY.search(name):
        return True
    segs = _segments(name)
    if not segs:
        return False
    if segs[-1] in _MONEY_SEG or segs[0] in _MONEY_SEG:
        return True
    return any(s in ("amount", "amt", "revenue", "gmv", "income") for s in segs)


def _column_name(node) -> str:
    """从聚合参数里挖出列名（可能是 CASE WHEN ... THEN amount 的形式）。"""
    try:
        if isinstance(node, exp.Column):
            return node.name
        if isinstance(node, exp.Alias):
            return node.alias or ""
        cols = list(node.find_all(exp.Column)) if node is not None else []
        if len(cols) == 1:
            return cols[0].name
        # 多列（CASE 里的 amount 与 0）时优先取金额列
        for c in cols:
            if is_money_name(c.name):
                return c.name
    except Exception:
        pass
    return ""


def _tables(tree) -> set[str]:
    out = set()
    try:
        for t in tree.find_all(exp.Table):
            n = (t.name or "").lower()
            if n:
                out.add(n)
    except Exception:
        pass
    return out


def _target(tree) -> str:
    """CTAS 的目标表名 —— 它也叫 *_revenue_* 时不能算"源表涉及收入域"。"""
    try:
        c = tree.find(exp.Create)
        if c is not None and c.this is not None:
            return (getattr(c.this, "name", "") or "").lower()
    except Exception:
        pass
    return ""


def risks(sql: str, stored_checks: dict | None = None) -> list[dict]:
    """返回风险清单（空列表 = 不拦）。纯静态，不联网。"""
    out: list[dict] = []
    text = sql or ""
    try:
        trees = [t for t in sqlglot.parse(text, read="hive") if t is not None]
    except Exception:
        trees = []

    hit_agg, hit_alias, hit_table = [], [], []
    for tree in trees:
        try:
            for node in tree.find_all(exp.Sum, exp.Avg):
                col = _column_name(node.this)
                if col and is_money_name(col):
                    hit_agg.append(col)
        except Exception:
            pass
        try:
            for sel in tree.find_all(exp.Select):
                for e in sel.expressions:
                    alias = e.alias if isinstance(e, exp.Alias) else ""
                    if alias and is_money_name(alias):
                        hit_alias.append(alias)
        except Exception:
            pass
        hit_table += [t for t in _tables(tree) if _MONEY_TABLE.search(t)]

    tgt = {_target(t) for t in trees} - {""}
    hit_table = [t for t in hit_table if t not in tgt]

    if hit_agg:
        cols = sorted(set(hit_agg))
        out.append({"code": "M1", "title": "对金额列做聚合",
                    "detail": "聚合列：%s" % "、".join(cols),
                    "hint": "确认事件覆盖（收入需含 vip_subscribe）与退款符号（order_refund 取负）"})
    if hit_table:
        tbs = sorted(set(hit_table))
        out.append({"code": "M2", "title": "涉及收入语义域的表",
                    "detail": "源表：%s" % "、".join(tbs),
                    "hint": "这些表历史上出过口径事故（5.24 倍），执行前请对照指标定义"})
    if hit_alias:
        als = sorted(set(hit_alias))
        out.append({"code": "M3", "title": "产出列是金额指标",
                    "detail": "输出列：%s" % "、".join(als),
                    "hint": "命名指标（如 revenue_sum）有口径评测，列名对上不等于口径对上"})
    if not out and not trees:
        # 解析失败但文本里明显是金额：宁可多问一句
        if _CN_MONEY.search(text) or re.search(r"\bamount\b|\bgmv\b", text, re.I):
            out.append({"code": "M1", "title": "SQL 解析失败但含金额字样",
                        "detail": "静态解析未通过，按金额类处理",
                        "hint": "先确认 SQL 可解析（护栏会拦），再复核口径"})

    stored = (stored_checks or {}).get("caliber") or []
    if stored:
        items = ["%s：%s" % (f.get("rule") or "口径", (f.get("message") or "")[:90])
                 for f in stored[:3]]
        out.append({"code": "M4", "title": "口径机检在生成时报过告警",
                    "detail": "；".join(items),
                    "hint": "生成时的告警不会随执行消失，确认你是有意这么写的"})
    return out


def _today() -> str:
    return _dt.date.today().isoformat()


def token(script_id: int, sql: str) -> str:
    """确认令牌：绑定记录号 + SQL 指纹 + 当日日期。

    为什么要绑 SQL：库里存的 SQL 可以被重新生成覆盖，若令牌只绑记录号，
    昨天的"我确认过"就成了今天的免死金牌。绑定后换 SQL 令牌即失效。
    """
    fp = hashlib.sha1((sql or "").encode("utf-8")).hexdigest()
    raw = "%s|%s|%s" % (script_id, fp, _today())
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]


def verify(script_id: int, sql: str, given: str) -> bool:
    return bool(given) and hmac.compare_digest(token(script_id, sql), given.strip())


def gate(script_id: int, sql: str, stored_checks: dict | None = None) -> dict:
    """闸门结论：{need_confirm, risks, token}。need_confirm=False 时可直接执行。"""
    rs = risks(sql, stored_checks)
    return {"need_confirm": bool(rs), "risks": rs,
            "token": token(script_id, sql) if rs else ""}
