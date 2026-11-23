"""长期记忆：只存本人**显式确认过**的口径偏好，跨会话生效。

为什么这么窄（这是刻意的，不是没做完）
------------------------------------
选过"历史结论 + 自动归纳"那条路，风险全踩中：
  1. 历史答案里的数字会被模型抄进本轮结果——看起来像"答对了"，其实是旧的；
  2. 口径一改（收入低估 5.24 倍那次），旧结论继续影响新答案，错得更隐蔽；
  3. 自动归纳把"猜出来的偏好"当事实存下，用户根本不知道系统记住了什么。
所以最终方案：**只有显式说出口的偏好才落库，且不存任何数字**。
用户随时能看、能删；删的是 user_memory，不动不可变审计 qa_logs。

五道保险：
  · 显式写入：问题里出现"记住/以后都按…"才归纳，或走 /api/memory 手工添加；
  · 不存数字：content 里的数字串全部剥掉，只留口径/指标/窗口；
  · 口径版本：每条带 caliber_version，语义层一改就整体不注入；
  · 相关度 Top-K：只注入与本轮相关的 3 条，控 token；
  · 缓存指纹：偏好变了 key 就变，不会回放"没带记忆时"的旧答案。
"""
from __future__ import annotations

import _bootstrap  # noqa: F401

import hashlib
import re

from governance import store

# 只有这些句式才算"显式要求记住"。普通的"那昨天呢"不算。
_REMEMBER = re.compile(
    r"(记住|记一下|以后|默认|每次都|都按|统一按|下次开始)[^\n。？!]{0,60}")
_NEG = re.compile(r"(别记|不要记|忘掉|删掉.*记忆)")
# 只剥"结果数字"：完整年份/长数字/带量单位的数值。保留 11-08 这种窗口写法，
# 否则用户说"默认看 11-08"会被剥成"默认看 -"，既难读又失去口径信息。
_RE_NUM = re.compile(r"\d{4}(?:-\d{1,2}){0,2}|\d[\d,]{2,}(?:\.\d+)?|\d+(?:\.\d+)?\s*(?:万|亿|%|元|个|次)")
_MAX = 160


def _norm(s: str) -> str:
    return re.sub(r"[\s，。？！、；：\"'“”‘’（）()\[\]【】!?.,;:]+", "",
                  (s or "").lower())


def _no_numbers(s: str) -> str:
    return _RE_NUM.sub("", s or "").strip()


def memory_key(content: str) -> str:
    """同一句偏好反复说只存一条：按归一化文本取指纹。"""
    return "pref:" + hashlib.sha256(
        _norm(content).encode("utf-8")).hexdigest()[:16]


def is_remember(text: str) -> bool:
    return bool(_REMEMBER.search(text or "")) and not _NEG.search(text or "")


def is_forget(text: str) -> bool:
    return bool(_NEG.search(text or ""))


def extract_preference(question: str, state: dict | None = None) -> dict | None:
    """从显式指令里剥出口径偏好。剥不出可存内容就返回 None——不猜。"""
    m = _REMEMBER.search(question or "")
    if not m:
        return None
    raw = m.group(0)
    body = _no_numbers(raw)
    if len(body) < 4:
        return None
    st = state or {}
    metrics = [str(x) for x in (st.get("metrics") or []) if x]
    win = st.get("time_window") or {}
    bits = [body]
    if metrics:
        bits.append("指标 " + "、".join(metrics[:3]))
    if win.get("label"):
        bits.append("窗口 %s" % _no_numbers(str(win["label"])))
    content = "；".join(bits)[:_MAX]
    return {
        "kind": "preference",
        "mem_key": memory_key(content),
        "content": content,
        "payload": {"metrics": metrics[:5], "time_window": {
            "label": _no_numbers(str(win.get("label") or "")),
            "start": "", "end": ""}},
    }


def add(user_id: str, content: str, cfg: dict, *, tenant_id: str = "default",
        state: dict | None = None) -> tuple[bool, str]:
    """显式写入一条偏好。content 由用户给出或 extract_preference 剥出。"""
    if not user_id or user_id in ("internal", "system"):
        return False, "内部身份不写偏好"
    item = extract_preference(content, state) or {
        "kind": "preference", "mem_key": memory_key(content),
        "content": _no_numbers(content)[:_MAX], "payload": {},
    }
    try:
        import semantic
        version = semantic.caliber_digest(cfg)["version"]
    except Exception:
        version = ""
    return store.upsert_memory(user_id, tenant_id, item["kind"],
                               item["mem_key"], item["content"],
                               item["payload"], version, "", cfg=cfg)


def list_all(user_id: str, cfg: dict) -> list[dict]:
    return store.load_memory(user_id, cfg) if user_id else []


def remove(mem_id: int, user_id: str, cfg: dict) -> bool:
    return store.delete_memory(int(mem_id), user_id, cfg)


_ENC_KEY = ""
_ENC = None


def _encoder(cfg: dict):
    """BGE 编码器单例：91MB 模型加载一次，之后每次召回只做一次推理。

    只在 memory.semantic 打开时加载；LSA/字符方案不需要它。
    模型路径/配置变了就重建，和 vecindex 的缓存键同源。
    """
    global _ENC_KEY, _ENC
    rc = cfg.get("retrieval") or {}
    key = "%s|%s|%s" % (rc.get("dense_backend") or "",
                        rc.get("neural_model_dir") or "",
                        rc.get("neural_max_len") or 96)
    if key == _ENC_KEY and _ENC is not None:
        return _ENC
    import vecindex
    _ENC = vecindex.BgeOnnxEncoder(
        str(rc.get("neural_model_dir") or ""),
        max_len=int(rc.get("neural_max_len") or 96))
    _ENC_KEY = key
    return _ENC


def _score(item: dict, question: str, cfg: dict | None = None) -> float:
    """相关度 = 字符重叠 + 指标命中 +（可选）BGE 语义相似度。

    语义这一路解决的是"记的是播放人数、问的是去重用户数"这种字面不重叠的
    同义改写。字符那两路保留：专有名词（指标码）精确命中仍然最可靠，
    且模型加载失败时能无缝退回旧行为。
    """
    q = set(_norm(question))
    c = set(_norm(str(item.get("content") or "")))
    base = len(q & c) / max(1, len(q)) if q and c else 0.0
    text = _norm(question)
    hits = sum(1 for m in ((item.get("payload") or {}).get("metrics") or [])
               if _norm(str(m)) and _norm(str(m)) in text)
    score = base + 0.3 * hits
    # 口径限定词硬规则：BGE-small 对"人数/次数"这种一字之差反而不准
    # （实测"去重用户数"对"按人数算" 0.586 < "按次数算" 0.599），
    # 纯语义会把偏好召回反。这类口径对立必须靠规则钉死，不靠调阈值蒙。
    # 与项目一贯做法一致：prompt 是软约束，护栏是硬约束。
    def _caliber(s: str) -> int:
        """+1=人数口径，-1=次数口径，0=没说清。只判单一口径，避免重复抵消。"""
        uv = bool(re.search("人数|用户数|去重|uv", s, re.I))
        pv = bool(re.search("次数|播放次数|pv", s, re.I))
        return 1 if uv and not pv else (-1 if pv and not uv else 0)

    qc, ic = _caliber(text), _caliber(str(item.get("content") or ""))
    if qc and ic:
        score += 0.8 if qc == ic else -0.8
    m = cfg.get("memory") or {} if cfg else {}
    if m.get("semantic"):
        try:
            import numpy as np
            enc = _encoder(cfg)
            vec = enc.encode([question, str(item.get("content") or "")])
            sim = float(np.dot(vec[0], vec[1]))
            # BGE 对完全无关的两句话也会给 0.3~0.4 的基础相似度（实测
            # "今天天气怎么样" vs "以后按播放人数算" 就有分）。不设阈值
            # 会让任何问题都召回一条偏好，prompt 里全是噪声。
            # 低于阈值不加分；高于才按 0.6 权重加，且只在已有字符/口径信号
            # 时也加——纯靠语义相似不足以断定用户指的是那条偏好。
            floor = float(m.get("semantic_min") or 0.55)
            if sim >= floor and (base > 0 or hits > 0 or ic):
                score = score + 0.6 * sim
        except Exception:
            pass                      # 语义失败退回字符方案，不瘫掉问答
    return score


def recall(user_id: str, question: str, cfg: dict, *, limit: int = 3) -> list[dict]:
    """按相关度取 Top-K；口径版本不一致的一律不注入。"""
    if not user_id or not (cfg.get("memory") or {}).get("enabled", True):
        return []
    try:
        import semantic
        version = semantic.caliber_digest(cfg)["version"]
    except Exception:
        version = ""
    rows = store.load_memory(user_id, cfg,
                            limit=int((cfg.get("memory") or {}).get("pool") or 100))
    fresh = [r for r in rows if str(r.get("caliber_version") or "") == version]
    scored = [(_score(r, question, cfg), r) for r in fresh]
    scored = [(s, r) for s, r in scored if s > 0]
    scored.sort(key=lambda x: (-x[0], -int(x[1].get("mem_id") or 0)))
    top = [dict(r, relevance=round(s, 3)) for s, r in scored[:max(0, int(limit))]]
    if top:
        store.touch_memory([r.get("mem_id") for r in top], user_id, cfg)
    return top


def memory_block(state: dict, cfg: dict) -> str:
    """生成节点的注入块。空记忆返回空串，单轮 prompt 逐字不变。"""
    items = state.get("memory_turns") or []
    if not items or not (cfg.get("memory") or {}).get("enabled", True):
        return ""
    lines = ["【用户长期偏好】这是本人在过往会话里明确要求记住的口径，"
             "只用于理解本轮指的是什么。",
             "硬性要求：历史数字不得当作本轮答案；本轮数字必须来自当前查询。"]
    for i, r in enumerate(items[:3], 1):
        p = r.get("payload") or {}
        bits = []
        if p.get("metrics"):
            bits.append("指标 " + "、".join(str(m) for m in p["metrics"][:3]))
        if (p.get("time_window") or {}).get("label"):
            bits.append("窗口 %s" % p["time_window"]["label"])
        lines.append("  %d. %s%s" % (
            i, str(r.get("content") or "")[:160],
            ("（%s）" % "，".join(bits)) if bits else ""))
    return "\n".join(lines)


def fingerprint(items: list | None) -> str:
    """记忆指纹进缓存键：新增/删除/改判都让旧答案失效。"""
    rows = items or []
    if not rows:
        return ""
    raw = "|".join("%s:%s:%s" % (r.get("mem_id") or "", r.get("content") or "",
                                  r.get("hits") or 0) for r in rows)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:12]
