"""稠密向量检索 + 规则重排：把「BM25 词面召不到、但语义上该来」的候选捞回来。

两种稠密后端（2026-11-15 起可切换）
----------------------------------
  lsa（默认）  TF-IDF 加权 → L2 归一化 → SVD 投影。矩阵分解，零外部依赖，
               115 题 A/B 里 R@4 24.3%（单路），与 BM25 融合后 33.9%。
  neural       BGE 中文小模型的 ONNX 神经 embedding → L2 归一化 → 余弦向量索引。
               与业界 Data Agent 的 schema/元数据向量化做法对齐；
               依赖 onnxruntime + tokenizers + 91 MB ONNX 模型（本地 CPU，无 torch）。

后端由 retrieval.dense_backend 决定，默认仍是 lsa —— 默认值是否切换由
eval/retrieval_ab.py 的 A/B 和 eval/run.py 的全量结果决定，不拍脑袋改。
两种后端共用同一套 BM25 并集融合 + rerank，所以 A/B 是干净的消融。

BM25 与稠密是**并列召回**再融合，不是互相替代：BM25 擅长专有名词精确命中
（表名、指标码），稠密擅长同义改写（「一共多少人用」→ 日活）。两者取并集后
由 rerank 统一打分，才谈得上「融合」。
"""
from __future__ import annotations

import _bootstrap  # noqa: F401

import math
import re
from collections import Counter, defaultdict
from pathlib import Path

import numpy as np

import conf
import retrieval
import semantic

# ---------------------------------------------------------------- 语料
_W_NAME = 4          # 物理名 / code：点名查询权重最高
_W_CN = 3            # 中文名
_W_HINT = 2          # caveats / not_for / note：口径线索
_W_BODY = 1          # grain / 列名 / formula / filter

_CACHE: dict[str, dict] = {}


def _documents(cfg: dict) -> list[dict]:
    """把语义层摊平成可检索文档。kind 决定 rerank 时用哪套特征。"""
    docs: list[dict] = []
    for name, t in semantic.all_tables(cfg).items():
        cols = [str(c) for c in (t.get("columns") or [])]
        hint = " ".join(str(x) for x in (t.get("caveats") or []) +
                        (t.get("not_for") or []))
        body = " ".join([str(t.get("grain") or "")] + cols)
        toks = (retrieval.tokenize(name, cfg) * _W_NAME
                + retrieval.tokenize(str(t.get("name_cn") or ""), cfg) * _W_CN
                + retrieval.tokenize(hint, cfg) * _W_HINT
                + retrieval.tokenize(body, cfg) * _W_BODY)
        docs.append({
            "id": name, "kind": "table", "layer": t.get("layer") or "",
            "cn": str(t.get("name_cn") or ""), "columns": cols,
            "not_for": [str(x) for x in (t.get("not_for") or [])],
            "text": " ".join([name, str(t.get("name_cn") or ""),
                              str(t.get("grain") or ""), hint]),
            "tokens": toks,
        })
    for code, m in semantic.all_metrics(cfg).items():
        body = " ".join([str(m.get("formula") or ""), str(m.get("filter") or ""),
                         str(m.get("unit") or "")])
        toks = (retrieval.tokenize(code, cfg) * _W_NAME
                + retrieval.tokenize(str(m.get("name_cn") or ""), cfg) * _W_CN
                + retrieval.tokenize(body, cfg) * _W_BODY)
        docs.append({
            "id": code, "kind": "metric", "layer": "",
            "cn": str(m.get("name_cn") or ""),
            "source": str(m.get("source") or ""), "columns": [],
            "not_for": [],
            "text": " ".join([code, str(m.get("name_cn") or ""), body]),
            "tokens": toks,
        })
    for word, info in semantic.terms(cfg).items():
        note = str((info or {}).get("note") or "")
        metric = str((info or {}).get("metric") or "")
        toks = (retrieval.tokenize(word, cfg) * _W_CN
                + retrieval.tokenize(metric, cfg) * _W_NAME
                + retrieval.tokenize(note, cfg) * _W_BODY)
        docs.append({
            "id": "术语:" + word, "kind": "term", "layer": "",
            "cn": word, "metric": metric, "columns": [], "not_for": [],
            "text": " ".join([word, metric, note]),
            "tokens": toks,
        })
    for r in semantic.caliber_rules(cfg):
        title = str(r.get("title") or "")
        why = str(r.get("why") or "")
        toks = (retrieval.tokenize(title, cfg) * _W_CN
                + retrieval.tokenize(why, cfg) * _W_BODY)
        docs.append({
            "id": "口径:" + str(r.get("id")), "kind": "caliber", "layer": "",
            "cn": title, "table": str(r.get("table") or ""),
            "columns": [], "not_for": [],
            "text": " ".join([title, why]),
            "tokens": toks,
        })
    return docs


# ---------------------------------------------------------------- 稠密表示
class TfidfLsa:
    """TF-IDF 加权 + SVD 投影。

    投影维度取 min(dim, N-1, V-1)：语料 177 条时 128 维是安全的，但小语料
    （自测里 3 条）必须自动收缩，否则 SVD 会返回一堆数值噪声当"语义"。
    """

    def __init__(self, docs_tokens: list[list[str]], dim: int = 128):
        self.n = len(docs_tokens)
        vocab = sorted({t for toks in docs_tokens for t in toks})
        self.vocab = {t: i for i, t in enumerate(vocab)}
        df = Counter()
        for toks in docs_tokens:
            df.update(set(toks))
        # 平滑 IDF：语料越小，IDF 越平，不能让罕见词一枝独秀
        self.idf = np.array([
            math.log((1 + self.n) / (1 + df[t])) + 1.0 for t in vocab
        ]) if vocab else np.zeros(0)
        raw = self._weigh(docs_tokens)
        self.dim = max(1, min(dim, self.n - 1, len(vocab) - 1)) if self.n > 2 else len(vocab)
        if self.n > 2 and self.dim >= 2:
            # 只做一次 SVD；177×~2k 的矩阵本地耗时在百毫秒级
            u, s, vt = np.linalg.svd(raw, full_matrices=False)
            k = self.dim
            self.basis = vt[:k].T                  # V_k: (词表, k)，查询侧要投到同一子空间
            self.matrix = self._l2(u[:, :k] * s[:k])
        else:
            self.basis = None                      # 语料太小，不投影
            self.matrix = self._l2(raw)

    def _weigh(self, docs_tokens: list[list[str]]) -> np.ndarray:
        m = np.zeros((len(docs_tokens), len(self.vocab)))
        for i, toks in enumerate(docs_tokens):
            for t, c in Counter(toks).items():
                j = self.vocab.get(t)
                if j is None:
                    continue
                # sublinear tf：出现 4 次不该是 1 次的 4 倍信息量
                m[i, j] = (1.0 + math.log(c)) * self.idf[j]
        return m

    @staticmethod
    def _l2(m: np.ndarray) -> np.ndarray:
        if m.size == 0:
            return m
        n = np.linalg.norm(m, axis=1, keepdims=True)
        n[n == 0] = 1.0
        return m / n

    def encode(self, tokens: list[str]) -> np.ndarray:
        """查询侧必须走同一套变换，否则余弦没有意义。

        有投影时 vec(V) @ basis(V,k) 落到与文档相同的 k 维子空间；没投影时
        两边都在原始词表空间。少了这一步，查询是 V 维、文档是 k 维，
        点积直接抛形状错——这正是第一版写出 [] 的原因。
        """
        vec = np.zeros(len(self.vocab))
        for t, c in Counter(tokens).items():
            j = self.vocab.get(t)
            if j is not None:
                vec[j] = (1.0 + math.log(c)) * self.idf[j]
        if self.basis is not None:
            vec = vec @ self.basis
        n = np.linalg.norm(vec)
        if n:
            vec = vec / n
        return vec

    def search(self, tokens: list[str], top: int = 10) -> list[tuple[int, float]]:
        if not tokens or self.matrix.shape[0] == 0:
            return []
        q = self.encode(tokens)
        if q.shape[0] != self.matrix.shape[1]:
            return []
        sims = self.matrix @ q
        order = np.argsort(-sims)[:top]
        return [(int(i), float(sims[i])) for i in order if sims[i] > 0]


# ---------------------------------------------------------------- 神经稠密表示
_ROOT = Path(__file__).resolve().parents[2]


class BgeOnnxEncoder:
    """神经 embedding：BGE 中文小模型的 ONNX 推理（CPU，无 torch）。

    与主流 Data Agent 的 schema/元数据向量化做法对齐：把每条结构化元数据
    （表名 + 中文名 + 粒度 + 口径线索）当作一段文本，过 transformer 取 CLS
    向量，L2 归一化后就是可余弦检索的稠密向量。

    为什么用 ONNX 而不是 sentence-transformers：后者要 torch（数百 MB + CUDA
    wheel），本机装不上也不需要；ONNX 只依赖 onnxruntime + tokenizers，91 MB
    模型从 hf-mirror 下载，CPU 上 177 条一次建索引，查询单条几毫秒。
    """

    def __init__(self, model_dir: str, max_len: int = 96):
        import onnxruntime as ort
        from tokenizers import Tokenizer

        d = Path(model_dir)
        if not d.is_absolute():
            d = _ROOT / d
        self.sess = ort.InferenceSession(
            str(d / "model.onnx"), providers=["CPUExecutionProvider"])
        self.tok = Tokenizer.from_file(str(d / "tokenizer.json"))
        self.tok.enable_truncation(max_length=max_len)
        self.tok.enable_padding(length=max_len)
        self.dim = int(self.sess.get_outputs()[0].shape[-1])

    def encode(self, texts: list[str]) -> "np.ndarray":
        enc = self.tok.encode_batch([str(t or "") for t in texts])
        ids = np.asarray([e.ids for e in enc], dtype=np.int64)
        mask = np.asarray([e.attention_mask for e in enc], dtype=np.int64)
        tids = np.zeros_like(ids)
        hidden = self.sess.run(None, {"input_ids": ids,
                                      "attention_mask": mask,
                                      "token_type_ids": tids})[0]
        vec = hidden[:, 0, :]                      # BGE 用 CLS 池化
        return (vec / np.maximum(np.linalg.norm(vec, axis=1, keepdims=True), 1e-9)
                ).astype(np.float32)


class NeuralVectorIndex:
    """归一化稠密向量矩阵 + 余弦检索，即最小可用的「向量索引」。

    177 条语料用暴力扫描（0.5 ms 级）；HNSW/IVF 是万级以上才划算，
    在小语料上是负优化，这里不假装用了近似索引。
    """

    def __init__(self, encoder: BgeOnnxEncoder, texts: list[str]):
        self.encoder = encoder
        self.matrix = encoder.encode(texts)
        self.dim = int(self.matrix.shape[1])

    def search(self, query: str, top: int = 10) -> list[tuple[int, float]]:
        q = self.encoder.encode([query])[0]
        sims = self.matrix @ q
        order = np.argsort(-sims)[:top]
        return [(int(i), float(sims[i])) for i in order if sims[i] > 0]


# ---------------------------------------------------------------- 索引缓存
def _index_key(cfg: dict) -> str:
    """缓存键不能只用 id(cfg)：cfg 被回收后地址会复用，第二次请求就拿到别人的索引。
    用「影响索引构造的配置 + 语义层文件 mtime」当键，语料一改自动失效。"""
    rc = cfg.get("retrieval") or {}
    parts = [str(rc.get("dense_dim", 128)), str(rc.get("hybrid_enabled", True)),
             str(rc.get("dense_backend") or "lsa"), str(rc.get("neural_model_dir") or "")]
    for k in ("tables_json", "metrics_yaml", "dimensions_yaml", "glossary_yaml"):
        p = (cfg.get("_paths") or {}).get(k)
        try:
            parts.append("%s=%d" % (k, p.stat().st_mtime_ns))
        except Exception:
            parts.append(k + "=?")
    return "|".join(parts)


def _index(cfg: dict) -> dict:
    key = _index_key(cfg)
    hit = _CACHE.get(key)
    if hit is not None:
        return hit
    docs = _documents(cfg)
    rc = cfg.get("retrieval") or {}
    dim = int(rc.get("dense_dim", 128))
    backend = str(rc.get("dense_backend") or "lsa").lower()
    if backend == "neural":
        enc = BgeOnnxEncoder(str(rc.get("neural_model_dir") or ""),
                             max_len=int(rc.get("neural_max_len", 96)))
        dense = NeuralVectorIndex(enc, [d["text"] for d in docs])
    else:
        backend = "lsa"
        dense = TfidfLsa([d["tokens"] for d in docs], dim=dim) if docs else None
    bm25 = retrieval.BM25({str(i): d["tokens"] for i, d in enumerate(docs)})
    idx = {"docs": docs, "dense": dense, "bm25": bm25, "dense_kind": backend}
    _CACHE[key] = idx
    return idx


def clear_cache() -> None:
    _CACHE.clear()


# ---------------------------------------------------------------- Rerank
_RE_CJK = re.compile(r"[\u4e00-\u9fff]+")
_RE_EN = re.compile(r"[a-z_][a-z0-9_]*")


def _q_tokens(q: str) -> tuple[list[str], str]:
    low = (q or "").lower()
    en = set(_RE_EN.findall(low))
    cjk = "".join(_RE_CJK.findall(q or ""))
    return list(en), cjk


def rerank(query: str, cands: list[dict], cfg: dict) -> list[dict]:
    """规则化 rerank。没有交叉编码器，就用可解释的特征加权重排。

    每个特征都对应一类真实错误：
      name_hit    用户直接点了表名/指标码，却被 BM25 排到后面（精确点名必须置顶）
      cn_cover    中文名被完整覆盖（「播放起播明细」比只命中「播放」更该进）
      col_cover   问的维度/指标在列名里有落点（「省份」→ province）
      not_for     命中负向提示，直接压到队尾（这是 caveats 存在的意义）
    """
    if not cands:
        return []
    ens, cjk = _q_tokens(query)
    out = []
    for c in cands:
        base = float(c.get("score") or 0.0)
        d = c["doc"]
        bonus = 0.0
        ident = d["id"].split(":")[-1].lower()
        # 精确点名：查询里出现完整表名/指标码（或去层前缀的形式）
        if ident and (ident in (query or "").lower()
                      or ident.split("_", 1)[-1] in (query or "").lower()):
            bonus += 6.0
        cn = d.get("cn") or ""
        if cn and len(cn) >= 2:
            if cn in (query or ""):
                bonus += 5.0
            else:
                hit = sum(1 for ch in set(cn) if ch in (query or ""))
                bonus += 1.2 * (hit / max(1, len(set(cn))))
        cols = d.get("columns") or []
        if cols and ens:
            hit = len([c2 for c2 in cols if c2.lower() in ens])
            bonus += 1.5 * min(1.0, hit / max(1, len(ens)))
        for neg in d.get("not_for") or []:
            nlow = neg.lower()
            if nlow and (nlow in (query or "") or any(
                    t in nlow for t in ens if len(t) >= 3)):
                bonus -= 4.0
                break
        out.append({**c, "rerank_score": base + bonus})
    out.sort(key=lambda x: -x["rerank_score"])
    return out


# ---------------------------------------------------------------- 对外入口
def search_dense(query: str, cfg: dict, top: int = 10,
                 kinds: tuple[str, ...] | None = None) -> list[dict]:
    idx = _index(cfg)
    if idx["dense_kind"] == "neural":
        hits = idx["dense"].search(query or "", top=top * 3) if idx["dense"] else []
    else:
        q = retrieval.tokenize(query or "", cfg)
        hits = idx["dense"].search(q, top=top * 3) if idx["dense"] else []
    out = []
    for i, s in hits:
        d = idx["docs"][i]
        if kinds and d["kind"] not in kinds:
            continue
        out.append({"doc": d, "score": s, "via": "dense"})
        if len(out) >= top:
            break
    return out


def search_bm25(query: str, cfg: dict, top: int = 10,
                kinds: tuple[str, ...] | None = None) -> list[dict]:
    idx = _index(cfg)
    q = retrieval.tokenize(query or "", cfg)
    hits = idx["bm25"].search(q, top=top * 3)
    out = []
    for i, s in hits:
        d = idx["docs"][int(i)]
        if kinds and d["kind"] not in kinds:
            continue
        out.append({"doc": d, "score": s, "via": "bm25"})
        if len(out) >= top:
            break
    return out


def hybrid_search(query: str, cfg: dict, top: int = 5,
                  kinds: tuple[str, ...] | None = None,
                  alpha: float = 0.5,
                  hard_names: set[str] | None = None,
                  soft_tables: dict[str, float] | None = None) -> list[dict]:
    """BM25 ∪ 稠密 → 归一化融合 → rerank → 业务加权。

    alpha 是稠密权重：0 等于回到纯 BM25（可用来做消融），1 等于纯稠密。
    两路分数尺度不同（BM25 无上界、余弦 0~1），必须先各自归一化再线性融合，
    否则 BM25 的长尾会吃掉整个融合分。

    hard_names / soft_tables 来自 retrieval.search_tables 的既有逻辑（表名精确
    点名 +60、维度关联 +25、明细意图 +30）。它们的量纲是 BM25 原始分，直接叠到
    余弦融合分上会把尺度搞乱，所以这里只借它们的**语义**：精确点名的置顶，
    维度关联的加固定名次奖励，在 rerank 之后再作用一次。
    """
    rc = cfg.get("retrieval") or {}
    if not rc.get("hybrid_enabled", True):
        return search_bm25(query, cfg, top=top, kinds=kinds)
    pool = search_bm25(query, cfg, top=max(top * 4, 20), kinds=kinds)
    dense = search_dense(query, cfg, top=max(top * 4, 20), kinds=kinds)
    merged: dict[str, dict] = {}
    for c in pool:
        merged[c["doc"]["id"]] = dict(c)
    for c in dense:
        k = c["doc"]["id"]
        if k in merged:
            merged[k]["dense_score"] = c["score"]
        else:
            c2 = dict(c)
            c2["score"] = 0.0
            c2["dense_score"] = c["score"]
            merged[k] = c2
    if not merged:
        return []
    xs = [v["score"] for v in merged.values()]
    ds = [v.get("dense_score", 0.0) for v in merged.values()]
    mx, md = (max(xs) or 1.0), (max(ds) or 1.0)
    for v in merged.values():
        v["score"] = (1 - alpha) * (v["score"] / mx) + alpha * (v.get("dense_score", 0.0) / md)
    ranked = rerank(query, list(merged.values()), cfg)
    if hard_names:
        # 精确点名不参与打分竞赛：用户写了表名，它就必须在最前面
        pinned = [c for c in ranked if c["doc"]["id"] in hard_names]
        rest = [c for c in ranked if c["doc"]["id"] not in hard_names]
        ranked = pinned + rest
    if soft_tables:
        ranked.sort(key=lambda c: -(c["rerank_score"] + 0.8 * soft_tables.get(c["doc"]["id"], 0.0)))
    return ranked[:top]


def backend_name(cfg: dict) -> str:
    rc = cfg.get("retrieval") or {}
    if not rc.get("hybrid_enabled", True):
        return "bm25"
    enc = "neural-bge" if str(rc.get("dense_backend") or "lsa") == "neural" else "lsa"
    return "hybrid(bm25+dense-%s)+rerank" % enc
