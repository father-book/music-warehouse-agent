"""Verified Query 库：人工验证过的「问题 → SQL」对，给 few-shot 用。

为什么不用「随机抽几条历史 SQL」
-------------------------------
模型看的是**形状**，不只是内容。同一批示例里如果混进一条错误口径的 SQL，
它会把错误一起学走。所以入库只收两类：

  1. `eval/baseline.json` 里 status=PASS 的题——期望值是独立重算的，不是自证；
  2. `qa_logs` 里 verdict=pass 且最终 SQL 通过全部护栏的问答——真实线上分布。

两条都要求 SQL 能解析、且不含 PII（含 phone/id_card 的整条丢弃，不做局部打码，
因为示例是"范本"，半个范本比没有更糟）。

日期一律替换成占位符
------------------
示例里如果留着 `dt = '2026-11-08'`，模型会习惯性硬编码这个日期。占位成
`<本次时间窗>` 并显式说明，才能让它学到"这里要放窗口"而不是"这里放 11-08"。
"""
from __future__ import annotations

import _bootstrap  # noqa: F401

import json
import re
from pathlib import Path

import conf
import retrieval

_CACHE: dict[str, list[dict]] = {}
_PG_DOWN = False

_RE_PII = re.compile(r"\b(phone|id_card|mobile|email|address)\b", re.I)
_RE_DATE = re.compile(r"'\d{4}-\d{2}-\d{2}'")
_RE_WS = re.compile(r"\s+")
_RE_TABLE = re.compile(r"\b(?:from|join)\s+([a-zA-Z_][\w.]*)", re.I)
_RE_TABLES_JSON = re.compile(r"tables\.json")


def _squash(sql: str) -> str:
    """压成一行：few-shot 示例占的是上下文预算，换行没有信息量。"""
    return _RE_WS.sub(" ", (sql or "").strip())


def _clean(sql: str) -> str:
    s = _squash(sql)
    s = _RE_DATE.sub("'<本次时间窗>'", s)
    if s.endswith(";"):
        s = s[:-1]
    return s


def _usable(sql: str) -> bool:
    s = (sql or "").strip()
    if len(s) < 20:
        return False
    if _RE_PII.search(s):
        return False
    low = s.lower()
    # 只收查询。DDL/DML 不是"问数答案"
    return low.startswith("select") or low.startswith("with")


def from_baseline(limit: int = 200) -> list[dict]:
    p = Path(__file__).resolve().parent.parent.parent / "eval" / "baseline.json"
    if not p.exists():
        return []
    try:
        d = json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return []
    out = []
    for c in d.get("cases") or []:
        if str(c.get("status") or "").upper() != "PASS":
            continue
        sql = c.get("sql") or ""
        if not _usable(sql):
            continue
        out.append({
            "q": str(c.get("question") or "").strip(),
            "sql": _clean(sql),
            "src": "baseline:%s" % c.get("id"),
            "domain": c.get("domain") or "",
            "shape": str(c.get("shape") or ""),
            "tables": [t.split(".")[-1].lower() for t in _RE_TABLE.findall(sql)],
        })
        if len(out) >= limit:
            break
    return out


def from_qa_logs(dsn: str = "", limit: int = 300) -> list[dict]:
    """线上真实问答。没有 PG / 没有表时返回空，绝不抛——few-shot 是增强不是依赖。"""
    global _PG_DOWN
    # 一次连不上就整个进程别再试：本地 PG 掉线时每次问数都白等 3 秒超时，
    # 比"没有示例"难受得多。重启进程或调 clear_cache() 会重置。
    if _PG_DOWN:
        return []
    try:
        import psycopg
    except Exception:
        _PG_DOWN = True
        return []
    dsn = dsn or "host=127.0.0.1 port=55432 user=postgres dbname=agent_gov"
    try:
        with psycopg.connect(dsn, connect_timeout=3) as c:
            cur = c.cursor()
            cur.execute("""
                select question, final_sql from qa_logs
                 where verdict = 'pass' and final_sql is not null
                   and length(final_sql) > 20
                   -- 只要真实用户入口的问法。eval / cli / selftest 的题是
                   -- 我们自己写的，混进示例池等于让模型学"出题人的口吻"，
                   -- 而且会自污染：2026-11-16 一次 HITL 自测写进两条
                   -- 「播放量怎么样」，`今天天气怎么样` 立刻被召回成相关示例
                   -- （BM25 命中"怎么样"，分数 4.59），把"不相关问题不给示例"
                   -- 这条护栏测挂了。示例池只该来自 web/api。
                   and source in ('web', 'api')
                 order by occurred_at desc limit %s
            """, (limit,))
            rows = cur.fetchall()
    except Exception:
        _PG_DOWN = True
        return []
    out = []
    for q, sql in rows:
        if not _usable(sql):
            continue
        out.append({
            "q": (q or "").strip(), "sql": _clean(sql), "src": "qa_logs",
            "domain": "",
            "tables": [t.split(".")[-1].lower() for t in _RE_TABLE.findall(sql)],
        })
    return out


def _key(cfg: dict) -> str:
    pc = cfg.get("prompt") or {}
    return "|".join([
        str((pc.get("fewshot") or {}).get("include_qa_logs", True)),
        str((pc.get("fewshot") or {}).get("max_pool", 200)),
    ])


def pool(cfg: dict | None = None) -> list[dict]:
    cfg = cfg or conf.load_config()
    k = _key(cfg)
    if k in _CACHE:
        return _CACHE[k]
    fs = (cfg.get("prompt") or {}).get("fewshot") or {}
    items = from_baseline(limit=int(fs.get("max_pool", 200)))
    if fs.get("include_qa_logs", True):
        items += from_qa_logs(limit=int(fs.get("qa_logs_limit", 300)))
    # 去重：同一个问题只留一条（baseline 优先，因为它有独立重算的期望值）
    seen: set[str] = set()
    uniq = []
    for it in items:
        key = _RE_WS.sub("", it["q"])
        if not key or key in seen:
            continue
        seen.add(key)
        uniq.append(it)
    _CACHE[k] = uniq
    return uniq


_IDX_CACHE: dict[str, tuple[retrieval.BM25, list[dict]]] = {}


def _index(cfg: dict) -> tuple[retrieval.BM25, list[dict]]:
    # 必须缓存：每次 retrieve 都重建 109 条文档的 BM25，分词是 jieba 逐条的，
    # 一次问数里会被调用两三次（生成 + 修复），白烧几十毫秒。
    k = _key(cfg)
    hit = _IDX_CACHE.get(k)
    if hit is not None:
        return hit
    items = pool(cfg)
    docs = {str(i): retrieval.tokenize(it["q"], cfg) for i, it in enumerate(items)}
    built = (retrieval.BM25(docs), items)
    _IDX_CACHE[k] = built
    return built


def retrieve_similar(question: str, cfg: dict, top: int = 2,
                     min_score: float = 2.0, same_shape: bool = True) -> list[dict]:
    """按问题相似度取示例，默认只取**答案形态相同**的。

    门槛 2.0 是必须的：BM25 在 100+ 条语料上，完全不相关的问题也能拿到 0.5~1.5
    的噪声分。宁可给 0 条示例，也不能给一条不相关的——后者会直接把模型带偏。

    **必须排除问题本身**：示例池里就有这道题的答案（baseline 那 115 条），
    如果用户问的正好是题库里的原句，不排除就等于把答案抄给模型——评测会虚高，
    而且会让 CoT/few-shot 的 A/B 完全失去意义。这是第一版真实踩到的坑：
    检索「2026-11-08 的 DAU 是多少」首位命中就是它自己，得分 3.7。

    same_shape 也是第一版踩出来的：光去重不够，形状不匹配的示例比没有还糟。
    """
    items = pool(cfg)
    qn = _RE_WS.sub("", question or "")
    if not items or not qn:
        return []
    want = guess_shape(question) if same_shape else ""
    idx, items = _index(cfg)
    hits = idx.search(retrieval.tokenize(question, cfg), top=top * 6)
    out = []
    skipped_shape = 0
    for i, s in hits:
        if s < min_score:
            continue
        it = items[int(i)]
        if _RE_WS.sub("", it["q"]) == qn:        # 同一道题，不能当示例
            continue
        if want and _shape_of(it) and _shape_of(it) != want:
            skipped_shape += 1
            continue
        it = dict(it)
        it["score"] = round(float(s), 2)
        out.append(it)
        if len(out) >= top:
            break
    # 一条同形态的都没有 → 返回空，让调用方不放示例块。混一条错形状的进去
    # 会把模型带向错误的输出结构，代价比"没有示例"高得多。
    return out


def render_block(examples: list[dict], max_sql: int = 340) -> str:
    """渲染成 few-shot 块。只放 2 条，且 SQL 截断——示例不是让模型抄，是让它对齐形状。"""
    if not examples:
        return ""
    lines = ["【已验证的相似问答（人工/评测确认过，口径可以直接参考）】"]
    for i, e in enumerate(examples, 1):
        sql = e["sql"]
        if len(sql) > max_sql:
            sql = sql[:max_sql].rstrip() + " …"
        lines.append("%d. 问：%s" % (i, e["q"]))
        lines.append("   答：%s" % sql)
    lines.append("以上是**形状参考**：表选择、聚合写法、时间条件的位置照它的思路，"
                 "但口径以本题给出的指标定义和时间窗为准；"
                 "示例里的 `<本次时间窗>` 必须替换成本题的时间条件。")
    return "\n".join(lines)


def clear_cache() -> None:
    global _PG_DOWN
    _CACHE.clear()
    _IDX_CACHE.clear()
    _PG_DOWN = False


# ---------------------------------------------------------------- 形态预判
# few-shot 最大的坑不是"示例不对"，是"示例形状不对"：问「DAU 是多少」，
# 检索到最相似的却是「DAU 比昨天多多少」——文本相似度 3.7 分排第一，但那个
# SQL 有对比窗口、输出两列，照着它学必然写歪。所以示例必须按**答案形态**过滤，
# 而形态要在生成之前就知道，不能等 critique 事后判。
_SHAPE_RULES: list[tuple[str, re.Pattern]] = [
    ("comparison", re.compile(r"比.{0,8}(多|少|高|低)|对比|环比|同比|相比|\bvs\b|差距")),
    ("ratio", re.compile(r"占比|比例|百分比|渗透率|率是|百分之")),
    ("ranking", re.compile(r"top\s*\d|前\s*\d+|排名|排行|最高|最低|最多|最少|第[一二三四五]")),
    ("trend", re.compile(r"趋势|走势|逐日|每天|日均|变化情况")),
    ("distribution", re.compile(r"分布|各.{0,4}(省份|城市|渠道|机型|版本|运营商)")),
    ("detail", re.compile(r"有哪些|哪些|列出|明细|分别是谁|都是谁|清单")),
]


def guess_shape(question: str) -> str:
    """问题 → 答案形态。猜不出来返回空串，调用方据此决定"不注入示例"。

    顺序有意义：comparison 优先于 ratio（「比昨天高多少占比」是对比），
    ranking 优先于 distribution（「各省份 Top3」要的是排名不是分布）。
    """
    q = (question or "").strip()
    if not q:
        return ""
    for name, pat in _SHAPE_RULES:
        if pat.search(q):
            return name
    return "scalar"


def _shape_of(item: dict) -> str:
    """示例自带 shape（baseline 有人工标注）就用它，否则现推。"""
    s = str(item.get("shape") or "")
    return s if s else guess_shape(item.get("q") or "")
