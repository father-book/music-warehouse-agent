"""策略引擎：把 PolicyDecision 编译进 SQL（ADR-005）。

三条硬约束：
  1. **编译期下推**：行级策略变成 WHERE 谓词，不做事后过滤。
     事后过滤会泄漏行数、能被聚合函数绕过、缓存命中时还会串号。
  2. **每一版 SQL 重新注入**：C 阶段修复会重写 SQL，策略必须跟着重算，
     不能只在首版注入一次。所以 `apply()` 是无状态纯函数。
  3. **fail closed**：解析失败 / 表越权 / 列越权一律返回 error，绝不返回原 SQL。

实现用 sqlglot AST 而不是正则：要认别名、CTE、子查询、JOIN 右表，
正则在这些场景上必然互相干扰。改完的 SQL 由 Agent 侧再过一次护栏复核。
"""
from __future__ import annotations

import json
import re
from pathlib import Path

import sqlglot
from sqlglot import exp

from .contracts import PolicyDecision
from .identity import provider

_IDENT = re.compile(r"^[A-Za-z_][A-Za-z_0-9]*$")


def _table_name(t) -> str:
    """取裸表名；兼容 sqlglot 6.3 的 .args 与 30.x 的 .name。"""
    name = t.name if hasattr(t, "name") else (t.args.get("this") or "")
    if hasattr(name, "name"):
        name = name.name
    return str(name).split(".")[-1].lower()


def _table_alias(t) -> str:
    a = t.alias if hasattr(t, "alias") else t.args.get("alias")
    if a and hasattr(a, "name"):
        a = a.name
    return str(a) if a else _table_name(t)


def _join_table(j):
    """sqlglot 6.3 会把 JOIN 右表包在 Alias 里，30.x 不一定——两种都解。"""
    right = j.this
    return right.this if isinstance(right, exp.Alias) else right


def _direct_tables(sel: exp.Select) -> list:
    """只取当前 Select 直属的 FROM / JOIN 表，不把子查询里的表误算进来。"""
    out = []
    # sqlglot 6.3 用 "from"，30.x 改成 "from_"——两个都要认，否则 FROM 主表
    # 会静默漏掉，只剩下 JOIN 表被注入（实测 30.30.0 上踩过）。
    f = sel.args.get("from_") or sel.args.get("from")
    if f is not None and f.this is not None:
        out.append(f.this.this if isinstance(f.this, exp.Alias) else f.this)
    for j in (sel.args.get("joins") or []):
        out.append(_join_table(j))
    return [t for t in out if isinstance(t, exp.Table)]


def _entity_score(cols: set[str]) -> int:
    """表的「粒度粗细」近似分：带实体 id 的比只有分组列的细。

    只用于给替代表排序，不参与任何权限判定——权限只看 allow/deny。
    """
    score = 3 if any(c.endswith("_id") for c in cols) else 0
    if cols & {"genre", "province", "city", "singer", "position",
               "network", "os", "channel", "app_version"}:
        score += 1
    return score


def _quote(v: str) -> str:
    """值只允许出现在单引号字符串里，单引号翻倍转义。"""
    return "'" + str(v).replace("\\", "\\\\").replace("'", "''") + "'"


class PolicyEngine:
    def __init__(self, cfg: dict | None = None):
        self.cfg = cfg
        self._cols: dict[str, set[str]] = {}
        p = (cfg or {}).get("_paths", {}).get("tables_json")
        if not p:
            p = Path(__file__).resolve().parents[2] / "agent" / "tables.json"
        meta = json.loads(Path(p).read_text(encoding="utf-8"))
        for name, t in meta["tables"].items():
            self._cols[name.lower()] = set(str(c).lower() for c in t.get("columns", []))

    def decide(self, ctx) -> PolicyDecision:
        return provider(self.cfg).decision(ctx)

    def column_exists(self, table: str, column: str) -> bool:
        return column in self._cols.get(table, set())

    # ---------------------------------------------------------------- 可见范围
    def visible(self, tables, decision: PolicyDecision) -> list[str]:
        """按角色可见范围裁剪候选表。

        为什么生成阶段就要裁：`report_readonly` 只能查汇总表，但语义层的
        指标 source 全是 dwd_ 明细表，模型 100% 会照 source 写 SQL，然后被
        R21 拦掉——用户的感受是"这个机器人什么都不会"。权限不该只在最后
        一道门生效，它必须**改变喂给模型的上下文**，否则受限角色等于不可用。
        """
        out = []
        for t in tables:
            n = str(t).lower()
            if n in decision.deny_tables:
                continue
            if (not decision.unrestricted and decision.allow_tables
                    and "*" not in decision.allow_tables and n not in decision.allow_tables):
                continue
            out.append(t)
        return out

    def substitute(self, metrics, decision: PolicyDecision, limit: int = 4) -> list[str]:
        """候选表全被拦掉时的替代方案：可见表里含同名列的那几张。

        原子指标的 code 就是列名（play_start_cnt / dau / revenue），
        所以"列存在"是最硬的相关性证据，比按表名猜词根可靠。

        **排序不能按字母序**。踩过的坑：产品经理问「播放量排行榜」，
        `ads_*` 字母序天然靠前，`substitute` 前三个位置被
        ads_daily_overview / ads_funnel / ads_genre_dist 占满——
        前者只有全站一行，后两者只到曲风粒度，都不足以回答歌曲榜，
        最后模型只能给一个曲风榜或干脆给全站总量。
        表的信息量取决于**粒度**：带 `xxx_id` 的最细，其次才是
        genre / province 这类分组列。按粒度排序，榜单类问题才有表可用。
        """
        if decision.unrestricted or not decision.allow_tables:
            return []
        pool = [t for t in sorted(decision.allow_tables)
                if t not in decision.deny_tables and t in self._cols]
        out: list[str] = []
        for code in metrics:
            c = str(code).lower()
            hits = [t for t in pool if c in self._cols[t]]
            # 粒度细的在前；同粒度按表名稳定排序
            hits.sort(key=lambda t: (-_entity_score(self._cols[t]), t))
            for t in hits:
                if t not in out:
                    out.append(t)
            if len(out) >= limit:
                break
        # 一个指标列都没对上（比如问的是口径里的派生量）→ 给通用总览表垫底，
        # 模型至少能在一个真实存在的表上工作，而不是对着空气编列名。
        if not out:
            for t in pool:
                if t.startswith(("ads_daily", "dws_user_daily", "ads_revenue")):
                    out.append(t)
                if len(out) >= 2:
                    break
        return out[:limit]

    def apply(self, sql: str, decision: PolicyDecision) -> dict:
        """返回 {sql, injected, tables, error, mask_columns}。

        sql 为**真正要执行的版本**。error 非空时调用方必须拒答，不能退回原 SQL。
        """
        out = {"sql": sql, "injected": [], "tables": [], "error": None,
               "mask_columns": sorted(decision.mask_columns)}
        if decision.unrestricted:
            return out
        if not sql.strip():
            out["error"] = "空 SQL，拒绝执行"
            return out

        try:
            tree = sqlglot.parse_one(sql, read="hive")
        except Exception as e:
            out["error"] = "策略引擎无法解析 SQL：%s" % str(e)[:150]
            return out
        if tree is None:
            out["error"] = "策略引擎无法解析 SQL（空 AST）"
            return out

        # ---- 表级：先判越权，fail closed ----
        # CTE 名与子查询别名会作为 Table 节点出现在 FROM 里，它们不是实体表，
        # 不能拿去做权限判定（2026-11-11 实测：正确 SQL 因 `FROM ranked` 被拒，
        # 提示"表 ranked 不在角色可见范围"）。CTE 定义体内的真实表仍会被遍历。
        derived = set()
        for cte in tree.find_all(exp.CTE):
            if cte.alias:
                derived.add(str(cte.alias).lower())
        for sub in tree.find_all(exp.Subquery):
            if sub.alias:
                derived.add(str(sub.alias).lower())
        for t in tree.find_all(exp.Table):
            name = _table_name(t)
            if name in derived:
                continue
            out["tables"].append(name)
            if name in decision.deny_tables or "*" in decision.deny_tables:
                out["error"] = "表 %s 被策略拒绝（role=%s）" % (name, decision.role)
                return out
        if decision.allow_tables and "*" not in decision.allow_tables:
            for name in out["tables"]:
                if name not in decision.allow_tables:
                    out["error"] = "表 %s 不在角色 %s 的可见范围" % (name, decision.role)
                    return out

        # ---- 列级：deny_columns 命中即拒 ----
        if decision.deny_columns:
            for c in tree.find_all(exp.Column):
                cname = str(c.name).lower()
                if cname in decision.deny_columns:
                    out["error"] = "列 %s 属于禁用列（role=%s）" % (cname, decision.role)
                    return out

        # ---- 行级：对每个 Select 的直属表注入 WHERE ----
        changed = False
        for sel in list(tree.find_all(exp.Select)):
            conds = []
            for t in _direct_tables(sel):
                name = _table_name(t)
                alias = _table_alias(t)
                for rule in decision.row_rules:
                    if rule.table != name or not rule.values:
                        continue
                    # 元数据里没有这列就不注入（避免把谓词塞给不含该列的表）
                    if not self.column_exists(name, rule.column):
                        continue
                    if not _IDENT.match(rule.column) or not _IDENT.match(alias):
                        continue
                    vals = ", ".join(_quote(v) for v in rule.values)
                    if rule.op == "eq":
                        conds.append("%s.%s = %s" % (alias, rule.column,
                                                      _quote(rule.values[0])))
                    else:
                        conds.append("%s.%s IN (%s)" % (alias, rule.column, vals))
            if not conds:
                continue
            predicate = " AND ".join(conds)
            try:
                frag = sqlglot.parse_one("SELECT 1 WHERE " + predicate, read="hive")
                extra = frag.args["where"].this
                old = sel.args.get("where")
                if old is not None:
                    sel.set("where", exp.Where(this=exp.And(this=old.this,
                                                            expression=extra)))
                else:
                    sel.set("where", exp.Where(this=extra))
            except Exception as e:
                out["error"] = "行级策略注入失败：%s" % str(e)[:150]
                return out
            out["injected"].append(predicate)
            changed = True

        if changed:
            out["sql"] = tree.sql(dialect="hive")
        return out

    def mask_result(self, result: dict, mask_columns) -> dict:
        """列脱敏（R22 已拦显式 PII，这里处理"允许查但需打码"的列）。"""
        if not mask_columns or not result or result.get("error"):
            return result
        cols = [str(c).lower() for c in (result.get("columns") or [])]
        idx = [i for i, c in enumerate(cols) if c in set(mask_columns)]
        if not idx:
            return result
        rows = []
        for r in (result.get("rows") or []):
            row = list(r)
            for i in idx:
                if i < len(row):
                    row[i] = "***"
            rows.append(row)
        return {**result, "rows": rows}


_ENGINE: PolicyEngine | None = None


def engine(cfg: dict | None = None) -> PolicyEngine:
    global _ENGINE
    if _ENGINE is None:
        _ENGINE = PolicyEngine(cfg)
    return _ENGINE
