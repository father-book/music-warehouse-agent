"""语义层读取与检索。

数据来源只有两处：agent/tables.json（事实层，DDL 自动生成 + 策展合并）
和 agent/semantic/*.yaml（知识层）。这里不实现任何"猜"，只做匹配。

A 阶段用简单的包含 + 字重合度打分。D 阶段换成 jieba 倒排索引时，
只改 find_metrics / find_tables 两个函数，调用方不动。
"""
from __future__ import annotations

import _bootstrap  # noqa: F401

import json
from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml

from conf import load_config

_CACHE: dict[Path, Any] = {}


def _load_json(p: Path) -> dict:
    if p not in _CACHE:
        _CACHE[p] = json.loads(p.read_text(encoding="utf-8"))
    return _CACHE[p]


def _load_yaml(p: Path) -> dict:
    if p not in _CACHE:
        _CACHE[p] = yaml.safe_load(p.read_text(encoding="utf-8")) or {}
    return _CACHE[p]


def clear_cache() -> None:
    _CACHE.clear()


# ---------------------------------------------------------------- 表

def all_tables(cfg: dict | None = None) -> dict[str, dict]:
    cfg = cfg or load_config()
    return _load_json(cfg["_paths"]["tables_json"])["tables"]


def get_table(name: str, cfg: dict | None = None) -> dict | None:
    return all_tables(cfg).get(name)


def table_columns(name: str, cfg: dict | None = None) -> list[str]:
    t = get_table(name, cfg)
    if not t:
        return []
    cols = t.get("columns") or []
    return list(cols) if isinstance(cols, list) else list(cols.keys())


def describe_table(name: str, cfg: dict | None = None) -> str:
    """给 LLM 看的表描述。刻意只给字段名+类型，不给全量 DDL（省 token）。"""
    t = get_table(name, cfg)
    if not t:
        return "表 %s 不在语义层里（共 %d 张可用）" % (name, len(all_tables(cfg)))

    types = t.get("types") or {}
    lines = [
        "表 %s —— %s" % (name, t.get("name_cn") or ""),
        "粒度：%s" % (t.get("grain") or "未标注"),
        "位置：%s.%s（裸表名即可，连接已切好）" % (
            t.get("catalog") or "?", t.get("database") or "?"),
        "行数：%s" % (format(t.get("rows") or 0, ",")),
        "分区：%s" % (t.get("partitioned_by") or "无"),
        "字段：",
    ]
    for c in table_columns(name, cfg):
        lines.append("  - %-24s %s" % (c, types.get(c, "")))
    if t.get("caveats"):
        lines.append("注意：")
        lines.extend("  ! " + c for c in t["caveats"])
    return "\n".join(lines)


def excluded_by_intent(tables, question: str, cfg: dict | None = None) -> list[str]:
    """按表的 not_for 边界，剔除「问题语义与排序键不符」的表。

    为什么要有这一层：`ads_song_ranking` 的 caveat 已经写明「rank_no 按完播率
    排，不是播放量」，但模型问「播放量排行榜」时照样选它——表名含 ranking、
    字段里又有 play_start_cnt，名字的吸引力压过了一行文字提醒。实测它给出的
    榜首只有 34 次起播，而真实播放量 Top1 是 264 次且不在榜内。
    能被提示词绕过的约束不算约束，所以把「哪些问法不该用我」变成元数据字段，
    在选表阶段直接剔除。
    """
    q = question or ""
    out = []
    for t in tables:
        nf = (get_table(t, cfg) or {}).get("not_for") or []
        if any(str(w) and str(w) in q for w in nf):
            continue
        out.append(t)
    return out


def all_relationships(cfg: dict | None = None) -> list[dict]:
    """tables.json 里展开好的 JOIN 边（由 gen_tables.py 从 tables.yaml 的
    relationships 规则展开，31 张表 → 23 条边）。护栏 R18/R19 用的是同一份。"""
    cfg = cfg or load_config()
    return _load_json(cfg["_paths"]["tables_json"]).get("relationships") or []


def all_dimensions(cfg: dict | None = None) -> dict[str, dict]:
    """dimensions.yaml 里已验证的维度（含 tables / notes / caution）。"""
    cfg = cfg or load_config()
    return _load_yaml(cfg["_paths"]["dimensions_yaml"]).get("dimensions") or {}


def find_tables(keyword: str, cfg: dict | None = None, top: int = 5) -> list[dict]:
    """D 阶段：委托给 jieba + BM25 倒排索引。

    backend=legacy 时回到 A 阶段的字符重合度，用于对照实验和紧急回滚。
    """
    cfg = cfg or load_config()
    if (cfg.get("retrieval") or {}).get("backend") == "legacy":
        return _legacy_find_tables(keyword, cfg, top)
    import retrieval                      # 延迟导入，避免与 retrieval 循环依赖
    return retrieval.search_tables(keyword, cfg, top=top)


def _legacy_find_tables(keyword: str, cfg: dict, top: int) -> list[dict]:
    kw = keyword.lower().strip()
    if not kw:
        return []
    scored = []
    for name, t in all_tables(cfg).items():
        score = 0.0
        if kw == name:
            score = 100.0
        elif kw in name:
            score = 60.0
        else:
            blob = "%s %s %s" % (t.get("name_cn") or "", t.get("grain") or "",
                                 " ".join(t.get("caveats") or []))
            common = len(set(kw) & set(blob))
            if common:
                score = 20.0 * common / max(len(set(kw)), 1)
        if score:
            scored.append({"table": name, "name_cn": t.get("name_cn"), "score": round(score, 1)})
    return sorted(scored, key=lambda x: -x["score"])[:top]


# ---------------------------------------------------------------- 指标

def all_metrics(cfg: dict | None = None) -> dict[str, dict]:
    cfg = cfg or load_config()
    return (_load_yaml(cfg["_paths"]["metrics_yaml"]).get("metrics") or {})


def metric_global(cfg: dict | None = None) -> dict:
    cfg = cfg or load_config()
    return _load_yaml(cfg["_paths"]["metrics_yaml"]).get("global") or {}


def get_metric(code: str, cfg: dict | None = None) -> dict | None:
    return all_metrics(cfg).get(code)


def caliber_rules(cfg: dict | None = None) -> list[dict]:
    """metrics.yaml 里的口径机检规则（ETL 护栏与问数 R25 共用同一份定义）。

    规则是数据不是代码：新增一个口径坑只加一段 YAML，两条链路同时生效。
    检查实现见 agent/guards/caliber.py（只有一份，这里不重写）。
    """
    cfg = cfg or load_config()
    return _load_yaml(cfg["_paths"]["metrics_yaml"]).get("caliber_guard") or []


def terms(cfg: dict | None = None) -> dict[str, dict]:
    cfg = cfg or load_config()
    return _load_yaml(cfg["_paths"]["glossary_yaml"]).get("terms") or {}


def find_metrics(query: str, cfg: dict | None = None, top: int = 3) -> list[dict]:
    """先查 glossary 同义词（口语 → 标准码），再按 code / 中文名匹配。

    顺序很重要：用户说「日活」，glossary 直接给 uv；
    改成先匹配指标名就会命中一堆含「活」字的无关项。
    """
    cfg = cfg or load_config()
    q = query.strip()
    ql = q.lower()
    hits: dict[str, dict] = {}

    # 1) 词典优先：同义词越长越优先，避免「用户数」被「用户」抢先
    #    大小写不敏感——DAU / dau / 日活 指的是同一个东西
    gl = terms(cfg)
    for word in sorted(gl, key=len, reverse=True):
        if word and word.lower() in ql:
            ref = gl[word] or {}
            if ref.get("metric"):
                code = ref["metric"]
                if code in all_metrics(cfg):
                    hits.setdefault(code, {
                        "code": code,
                        "name_cn": get_metric(code, cfg).get("name_cn"),
                        "score": 90.0,
                        "matched_by": "glossary:%s" % word,
                    })

    # 2) 指标本身：D 阶段换成 BM25；legacy 保留字符重合度用于对照
    if (cfg.get("retrieval") or {}).get("backend") == "legacy":
        for code, m in all_metrics(cfg).items():
            if code in hits:
                continue
            name_cn = m.get("name_cn") or ""
            score, how = 0.0, ""
            if ql == code:
                score, how = 100.0, "code"
            elif code in ql:
                score, how = 70.0, "code_substr"
            elif name_cn and name_cn in q:
                score, how = 80.0, "name_cn"
            else:
                common = len(set(q) & set(name_cn))
                if name_cn and common >= 2:
                    score, how = 20.0 * common / len(set(name_cn)), "char_overlap"
            if score >= 20:
                hits[code] = {"code": code, "name_cn": name_cn,
                              "score": round(score, 1), "matched_by": how}
    else:
        import retrieval                # 延迟导入，避免循环依赖
        bm = retrieval.search_metrics(q, cfg, top=top, exclude=set(hits))
        # BM25 会顺手召回一堆弱相关指标（问 DAU 却补出 arpu）。
        # 已有 glossary 精确命中时门槛抬高，只让强匹配进来；
        # 一条都没命中时放宽，保证至少有候选指标可给模型参考。
        rcfg = cfg.get("retrieval") or {}
        floor = float(rcfg.get("min_metric_score", 50.0)) if any(
            h.get("score", 0) >= 90 for h in hits.values()) else float(
            rcfg.get("fallback_metric_score", 35.0))
        for h in bm:
            if h["score"] >= floor:
                hits[h["code"]] = h

    return sorted(hits.values(), key=lambda x: -x["score"])[:top]


def explain_metric(code: str, cfg: dict | None = None) -> str:
    m = get_metric(code, cfg)
    if not m:
        return "指标 %s 不存在" % code
    lines = [
        "%s（%s）" % (code, m.get("name_cn") or ""),
        "  层级：%s　可加性：%s" % (m.get("tier"), m.get("additivity")),
        "  公式：%s" % (m.get("formula") or ""),
    ]
    if m.get("source"):
        lines.append("  来源表：%s" % m["source"])
    if m.get("filter"):
        lines.append("  过滤：%s" % m["filter"])
    if m.get("note"):
        lines.append("  说明：%s" % m["note"])
    if m.get("trap"):
        lines.append("  陷阱：%s" % m["trap"])
    if m.get("baseline") is not None:
        lines.append("  基线：%s" % m["baseline"])
    return "\n".join(lines)


# ---------------------------------------------------------------- 口径版本
# 「这个数是按哪版口径算的」——答案里必须能回答，否则事后追责时只能翻 git log。
# 版本号不手写：手写的版本号一定会忘记更新，而"忘记更新"比"没有版本号"更糟，
# 因为它会让人误以为两版一致。这里按**影响口径的字段**算内容指纹，
# 改错别字、调整 YAML 缩进、重排词条顺序都不会让版本变化。

_DIGEST_METRIC_KEYS = ("tier", "formula", "filter", "trap", "note", "additivity",
                       "source", "sources", "ads", "baseline")
_DIGEST_TABLE_KEYS = ("grain", "pk", "columns", "caveats", "not_for",
                      "partitioned_by", "layer")
_DIGEST_DIM_KEYS = ("name_cn", "field", "tables", "notes", "cautions")


def caliber_digest(cfg: dict | None = None, with_payload: bool = False) -> dict:
    """语义层口径指纹 → {"version": "cal-xxxxxxxx", ...}。

    为什么要分开返回 `counts`：光给一个哈希，改完口径看不出"改了什么"；
    带上指标/规则/表的数量，版本变化时人能立刻判断是不是自己期望的那类变更。
    """
    cfg = cfg or load_config()
    mets = all_metrics(cfg)
    cal = {}
    for code in sorted(mets):
        m = mets[code] or {}
        cal[code] = {k: m.get(k) for k in _DIGEST_METRIC_KEYS if m.get(k) is not None}
    rules = caliber_rules(cfg)
    tbls = {}
    for name in sorted(all_tables(cfg)):
        t = get_table(name, cfg) or {}
        tbls[name] = {k: t.get(k) for k in _DIGEST_TABLE_KEYS
                      if t.get(k) is not None}
    dims = {}
    for code in sorted(all_dimensions(cfg)):
        d = all_dimensions(cfg).get(code) or {}
        dims[code] = {k: d.get(k) for k in _DIGEST_DIM_KEYS if d.get(k) is not None}
    lex = {w: v for w, v in sorted(terms(cfg).items())}

    payload = {"metrics": cal, "caliber_guard": rules, "tables": tbls,
               "dimensions": dims, "glossary": lex}
    raw = json.dumps(payload, sort_keys=True, ensure_ascii=False,
                     separators=(",", ":"))
    import hashlib
    h = hashlib.sha1(raw.encode("utf-8")).hexdigest()
    out = {
        "version": "cal-" + h[:8],
        "hash": h,
        "counts": {"metrics": len(cal), "caliber_rules": len(rules),
                   "tables": len(tbls), "dimensions": len(dims),
                   "glossary": len(lex)},
    }
    if with_payload:
        out["payload"] = payload
    return out


def caliber_version(cfg: dict | None = None) -> str:
    """只要版本串（答案里那一行、日志里那个字段）。"""
    return caliber_digest(cfg)["version"]
