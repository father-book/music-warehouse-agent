"""语义层倒排检索（D 阶段）：jieba 分词 + BM25。

A 阶段的``find_tables`` 用「字符集合交集」打分——「各省份用户数」和
「用户维度」共有的汉字能碰出分，但「周杰伦的歌」和 ``dim_song`` 一个字都不重合。
这类召回失败是静默的：模型拿不到正确的表，就自己编一个，护栏还不一定拦得住。

这里换成真正的倒排索引：

  * **领域词典注入**：把 31 张表的中文名、56 个指标名、glossary 同义词、
    物理表名/列名用 ``jieba.add_word`` 注册，避免「日活」被切成「日 / 活」。
  * **字段加权**：结构化字段（表名、中文名、指标 code）重复 3 次进文档，
    caveats/trap 重复 2 次——BM25 天然把重复算成词频，不需要另发明打分公式。
  * **精确命中加成**：原文里出现了完整表名/中文名/code，直接加权，
    保证「dws_user_daily」这种点名查询永远排第一。
  * **glossary 仍然最高优先**：口语同义词（日活 → uv）是确定性映射，
    不参与 BM25 竞争，留在 ``semantic.find_metrics`` 里给 90 分。

jieba 放在 ``_vendor/jieba``（已精简掉 30MB 的词性/关键词模型，只留分词必需的
dict.txt + finalseg），由 ``_bootstrap`` 注入 sys.path。若环境里没有 jieba，
自动退化成「英文词 + 中文二元切分」，不会让主链路崩。
"""
from __future__ import annotations

import _bootstrap  # noqa: F401

import logging
import math
import re
from collections import Counter, defaultdict
from functools import lru_cache
from pathlib import Path

import semantic

# ---------------------------------------------------------------- 分词
try:
    import warnings
    # jieba 0.42.1 是 2020 年的包，在 Python 3.13 上会报 3 条
    # "invalid escape sequence" SyntaxWarning，与本项目无关，静音掉
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", SyntaxWarning)
        import jieba
    jieba.setLogLevel(logging.CRITICAL)      # 别在每次问答时打印词典加载日志
    _HAS_JIEBA = True
except ImportError:                          # pragma: no cover - 兜底路径
    jieba = None
    _HAS_JIEBA = False

_STOP = {
    "的", "了", "是", "在", "有", "和", "与", "及", "把", "被", "对", "从", "到",
    "各", "每", "个", "中", "上", "下", "里", "为", "以", "按", "并", "或", "啊",
    "多少", "几", "哪", "哪些", "什么", "怎么", "请问", "查", "查询", "一下", "统计",
    "看看", "给我", "帮我", "想", "要", "数据", "情况", "分别", "以及", "还有",
    "这", "那", "之", "其", "我", "你", "吗", "呢", "吧", "请", "输出", "列出",
}

_RE_EN = re.compile(r"[a-z][a-z0-9_]*")
_RE_CJK = re.compile(r"[\u4e00-\u9fff]")


_WORDS_READY = False


def _register_domain_words(cfg: dict) -> None:
    """把语义层术语注册进 jieba，防止领域词被切碎。只做一次。"""
    global _WORDS_READY
    if not _HAS_JIEBA or _WORDS_READY:
        return
    words: set[str] = set()
    for t in semantic.all_tables(cfg).values():
        for v in (t.get("name_cn"), t.get("grain")):
            if v:
                words.add(str(v))
    for name in semantic.all_tables(cfg):
        words.add(name)
    for code, m in semantic.all_metrics(cfg).items():
        words.add(code)
        if m.get("name_cn"):
            words.add(str(m["name_cn"]))
    for w in semantic.terms(cfg):
        words.add(str(w))
    for w in words:
        # 只注册 2~6 字的短术语。把整句中文名（如「会话与启动退出事件」）
        # 注册成一个词，会让文档里不再出现「启动」「会话」这些可检索的细粒度词，
        # 反而降低了召回——实测「有哪些用户启动了 App」因此召不到 dwd_session。
        if 2 <= len(w) <= 6:
            jieba.add_word(w, freq=100000)
    _WORDS_READY = True


def tokenize(text: str, cfg: dict | None = None) -> list[str]:
    """中文 jieba + 英文/下划线标识符；无 jieba 时用中文二元切分兜底。"""
    if cfg is not None:
        _register_domain_words(cfg)
    s = (text or "").lower()
    out: list[str] = []
    for en in _RE_EN.findall(s):
        if len(en) >= 2 and en not in _STOP:
            out.append(en)
    cjk = "".join(_RE_CJK.findall(s))
    if _HAS_JIEBA:
        out.extend(t for t in jieba.cut(cjk)
                   if len(t) >= 2 and t not in _STOP and not t.isdigit())
    elif cjk:
        # 二元切分：中文没有空格时，bigram 是最省事的可召回表示
        out.extend(cjk[i:i + 2] for i in range(len(cjk) - 1))
    # 单字 English 标识符（如 dt）也算，但权重低
    return out


# ---------------------------------------------------------------- BM25
class BM25:
    """标准 Okapi BM25。docs: {id: tokens}，tokens 里重复次数即字段加权。"""

    def __init__(self, docs: dict[str, list[str]], k1: float = 1.5, b: float = 0.75):
        self.k1, self.b = k1, b
        self.docs = docs
        self.N = len(docs)
        self.tf: dict[str, Counter] = {k: Counter(v) for k, v in docs.items()}
        self.dl = {k: len(v) for k, v in docs.items()}
        self.avgdl = (sum(self.dl.values()) / self.N) if self.N else 1.0
        df: dict[str, int] = defaultdict(int)
        for c in self.tf.values():
            for t in c:
                df[t] += 1
        self.idf = {t: math.log(1 + (self.N - n + 0.5) / (n + 0.5))
                    for t, n in df.items()}

    def search(self, query: list[str], top: int = 5,
               boost: dict[str, float] | None = None,
               hard_boost: dict[str, float] | None = None) -> list[tuple[str, float]]:
        """boost 只给**本来就有词面匹配**的文档加权；hard_boost 是精确点名，
        允许把一个词面没命中的表直接抬进结果。

        这个区分是必须的：给所有 dwd_* 表无条件 +30，会让一堆完全无关的
        明细表（dwd_skip / ods_user_change）凭空冒到前排，比不加权还差。
        """
        if not query or not self.N:
            return []
        qc = Counter(query)
        scored: dict[str, float] = {}
        for doc_id, tf in self.tf.items():
            dl = self.dl[doc_id] or 1
            s = 0.0
            for term, qn in qc.items():
                f = tf.get(term, 0)
                if not f:
                    continue
                idf = self.idf.get(term, 0.0)
                s += idf * (f * (self.k1 + 1)) / (
                    f + self.k1 * (1 - self.b + self.b * dl / self.avgdl)) * min(qn, 2)
            if boost and s > 0 and doc_id in boost:
                s += boost[doc_id]
            if hard_boost and doc_id in hard_boost:
                s += hard_boost[doc_id]
            if s > 0:
                scored[doc_id] = s
        return sorted(scored.items(), key=lambda x: -x[1])[:top]


# ---------------------------------------------------------------- 索引构建
_W_NAME = 3          # 物理名 / code：点名查询权重最高
_W_CN = 3            # 中文名
_W_HINT = 2          # caveats / trap / note：口径线索
_W_BODY = 1          # grain / 列名 / formula


def _repeat(tokens: list[str], n: int) -> list[str]:
    return tokens * n


@lru_cache(maxsize=4)
def _tables_index(cache_key: str, cfg_paths: tuple) -> tuple[BM25, dict]:
    """cache_key 只是为了让 lru_cache 能区分配置；真正的路径在 cfg_paths。"""
    import json
    fake = _fake_cfg(cfg_paths)
    tables = json.loads(Path(cfg_paths[0]).read_text(encoding="utf-8"))["tables"]
    docs: dict[str, list[str]] = {}
    meta: dict[str, dict] = {}
    for name, t in tables.items():
        tok: list[str] = []
        tok += _repeat(tokenize(name, fake), _W_NAME)
        tok += _repeat(tokenize(t.get("name_cn") or "", fake), _W_CN)
        tok += _repeat(tokenize(t.get("grain") or "", fake) +
                       tokenize(" ".join(t.get("columns") or []), fake), _W_BODY)
        tok += _repeat(tokenize(" ".join(t.get("caveats") or []), fake), _W_HINT)
        docs[name] = tok
        meta[name] = {"name_cn": t.get("name_cn"),
                      "catalog": t.get("catalog"), "rows": t.get("rows")}
    return BM25(docs), meta


@lru_cache(maxsize=4)
def _metrics_index(cache_key: str, cfg_paths: tuple) -> tuple[BM25, dict]:
    import yaml
    fake = _fake_cfg(cfg_paths)
    m = yaml.safe_load(Path(cfg_paths[1]).read_text(encoding="utf-8"))["metrics"]
    docs: dict[str, list[str]] = {}
    meta: dict[str, dict] = {}
    for code, d in m.items():
        tok: list[str] = []
        tok += _repeat(tokenize(code, fake), _W_NAME)
        tok += _repeat(tokenize(d.get("name_cn") or "", fake), _W_CN)
        tok += _repeat(tokenize(" ".join(str(d.get(k) or "") for k in
                                         ("formula", "source", "filter", "unit")), fake),
                       _W_BODY)
        tok += _repeat(tokenize(" ".join(str(d.get(k) or "") for k in
                                         ("note", "trap", "baseline_note")), fake),
                       _W_HINT)
        docs[code] = tok
        meta[code] = {"name_cn": d.get("name_cn"), "additivity": d.get("additivity")}
    return BM25(docs), meta


_PATH_KEYS = ("tables_json", "metrics_yaml", "glossary_yaml", "dimensions_yaml")


def _fake_cfg(paths: tuple) -> dict:
    """索引构建期只需要四个语义层文件，给 semantic 一个最小的 cfg 即可。"""
    return {"_paths": {k: Path(v) for k, v in zip(_PATH_KEYS, paths)}}


def _paths(cfg: dict) -> tuple:
    p = cfg["_paths"]
    return tuple(str(p[k]) for k in _PATH_KEYS)


def _indexes(cfg: dict) -> tuple[tuple[BM25, dict], tuple[BM25, dict]]:
    key = cfg.get("retrieval", {}).get("backend", "bm25")
    return (_tables_index(key, _paths(cfg)), _metrics_index(key, _paths(cfg)))


# ---------------------------------------------------------------- 维度识别
# 分区/同期群维度只描述"按什么切分数据"，不代表用户想按它分组 JOIN
_PARTITION_DIMS = {"dt", "day_n", "cohort_date"}
# 对 dimensions.yaml 的口语补充（正式别名仍以语义层为准，这里只收口语）
_DIM_ALIAS = {
    "province": ("省", "分省", "地区", "地域"),
    "city": ("城市", "省内城市"),
    "genre": ("风格", "音乐类型"),
    "song_id": ("曲目", "歌曲名", "单曲"),
    "singer": ("演唱者", "艺人", "歌手名"),
    "os": ("系统", "安卓", "ios", "苹果"),
    "network": ("网络制式", " wifi"),
    "channel": ("来源", "投放渠道"),
    "app_version": ("版本", "客户端"),
    "is_vip": ("会员", "vip", "付费用户"),
    "is_login": ("登录", "游客"),
    "position": ("坑位", "推荐位"),
}


_RE_DETAIL_INTENT = re.compile(
    r"哪些|哪个|列出|列一下|明细|逐条|逐日|每一条|每个用户|每首歌|"
    r"前\s*\d+|top\s*\d+|谁|具体是")


def detail_intent(query: str) -> bool:
    """列举型问题（「有哪些用户…」）要的是明细，不是 ADS 汇总表。

    本项目明确以「明细题」为主执行面，而 BM25 只看词面：
    「有哪些用户启动了 App」会把 ``dws_user_daily``（名字里有"用户"）
    排在 ``dwd_session`` 前面，可前者是全站一行、根本没有 user_id 列。
    意图识别补的是这一层：问"哪些/列出/逐条"时给 DWD/ODS 明细表提权。
    """
    return bool(_RE_DETAIL_INTENT.search(query or ""))


def detect_dimensions(query: str, cfg: dict) -> list[dict]:
    """问题里出现了哪些维度 → 这些维度关联哪些表、有什么坑。

    这是「该不该 JOIN 维表」的唯一依据：纯指标问题（如「11-08 的 DAU」）
    不命中任何分组维度，就只查指标 source，**不会**把 BM25 顺手召回的
    省份分布表塞进 prompt。
    """
    q = query or ""
    ql = q.lower()
    out: list[dict] = []
    for code, d in semantic.all_dimensions(cfg).items():
        if code in _PARTITION_DIMS:
            continue
        cn = str(d.get("name_cn") or "")
        hit = ""
        if cn and cn in q:
            hit = "name_cn:" + cn
        elif code in ql:
            hit = "code:" + code
        else:
            for a in _DIM_ALIAS.get(code, ()):
                if a in ql:
                    hit = "alias:" + a
                    break
        if hit:
            out.append({"code": code, "name_cn": cn,
                        "tables": d.get("tables") or [],
                        "caution": d.get("caution"),
                        "notes": d.get("notes") or [],
                        "matched_by": hit})
    return out


# ---------------------------------------------------------------- 对外接口
def search_tables(query: str, cfg: dict, top: int = 5) -> list[dict]:
    rc = cfg.get("retrieval") or {}
    # 后端切换：默认 hybrid（BM25 ∪ 稠密 + rerank，115 题 A/B R@4 28.7%→49.6%），
    # bm25 / legacy 保留作对照与零依赖回退；改默认值等于改基线，要先跑全量。
    # 延迟导入是必须的——vecindex 反过来要用本模块的 tokenize/BM25，
    # 模块级互相 import 会炸在 import 期。
    if str(rc.get("backend") or "bm25") == "hybrid":
        return _search_tables_hybrid(query, cfg, top)
    (idx, meta), _ = _indexes(cfg)
    ql = (query or "").lower()
    hard: dict[str, float] = {}                 # 精确点名，无条件生效
    soft: dict[str, float] = {}                 # 加权，仅对已有匹配的表生效
    for name, m in meta.items():
        if name in ql:
            hard[name] = 60.0
        elif m.get("name_cn") and str(m["name_cn"]) in (query or ""):
            hard[name] = 45.0
    # 命中维度的关联表提权：问「各省份」就该让 dim_user / dws_province_daily 靠前
    for d in detect_dimensions(query, cfg):
        for t in d["tables"]:
            soft[t] = soft.get(t, 0.0) + 25.0
    # 列举型问题提权 DWD 明细表（不给 ODS 提权，原始层不是执行面）
    if detail_intent(query):
        for name in meta:
            if name.startswith("dwd_"):
                soft[name] = soft.get(name, 0.0) + 30.0
    hits = idx.search(tokenize(query, cfg), top=top,
                      boost=soft, hard_boost=hard)
    return [{"table": name, "name_cn": meta[name]["name_cn"],
             "score": round(score, 2), "matched_by": "bm25"} for name, score in hits]


def _search_tables_hybrid(query: str, cfg: dict, top: int) -> list[dict]:
    """混合后端：召回换成 BM25 ∪ 稠密 + rerank，但**业务加权一条都不少**。

    维度关联和明细意图这两条是路由正确性的主要来源（问「各省份」必须让
    dws_province_daily 靠前），换成混合召回时如果丢掉，召回率会不升反降。
    """
    import vecindex

    (_, meta), _ = _indexes(cfg)
    ql = (query or "").lower()
    hard: set[str] = set()
    soft: dict[str, float] = {}
    for name, m in meta.items():
        if name in ql:
            hard.add(name)
        elif m.get("name_cn") and str(m["name_cn"]) in (query or ""):
            hard.add(name)
    for d in detect_dimensions(query, cfg):
        for t in d["tables"]:
            soft[t] = soft.get(t, 0.0) + 25.0
    if detail_intent(query):
        for name in meta:
            if name.startswith("dwd_"):
                soft[name] = soft.get(name, 0.0) + 30.0
    alpha = float((cfg.get("retrieval") or {}).get("dense_alpha", 0.3))
    hits = vecindex.hybrid_search(query, cfg, top=top, kinds=("table",),
                                 alpha=alpha, hard_names=hard,
                                 soft_tables=soft)
    return [{"table": c["doc"]["id"], "name_cn": c["doc"].get("cn") or meta.get(
                 c["doc"]["id"], {}).get("name_cn"),
             "score": round(float(c.get("rerank_score") or 0.0), 2),
             "matched_by": vecindex.backend_name(cfg)} for c in hits]


def search_metrics(query: str, cfg: dict, top: int = 3,
                   exclude: set[str] | None = None) -> list[dict]:
    _, (idx, meta) = _indexes(cfg)
    ql = (query or "").lower()
    boost: dict[str, float] = {}
    for code, m in meta.items():
        if code in ql:
            boost[code] = 55.0
        elif m.get("name_cn") and str(m["name_cn"]) in (query or ""):
            boost[code] = 40.0
    hits = idx.search(tokenize(query, cfg), top=top * 3, hard_boost=boost)
    out = []
    for code, score in hits:
        if exclude and code in exclude:
            continue
        # BM25 原始分是 0~20 量纲，映射到 20~75，留出 90 给 glossary 精确命中
        mapped = min(75.0, 20.0 + score * 2.2)
        out.append({"code": code, "name_cn": meta[code]["name_cn"],
                    "score": round(mapped, 1), "matched_by": "bm25"})
        if len(out) >= top:
            break
    return out


def backend_name(cfg: dict) -> str:
    return "jieba+BM25" if _HAS_JIEBA else "bigram+BM25(fallback)"
