"""模型网关（AC-18 / AC-22）：所有模型调用的唯一出口。

四件事，缺一件「企业级网关」就只是口号：

1. **唯一出口**
   `llm.py` 降为传输层（`_transport_chat`），只有本模块能调；`egress` 在 socket
   层再兜一道。两层之后，"某个节点顺手 import httpx 直连"会当场失败，
   而不是等到月底看账单才发现。

2. **全量台账**
   成功 / 失败 / 缓存命中**各**写一条 `cost_ledger`：trace_id、谁问的、哪个岗位、
   谁调的（generate / repair / meta）、token、成本、延迟、重试几次、
   prompt 指纹与首段、模型回复首段。表上有不可变触发器，改不了删不掉。
   只存摘要不存全文：全文动辄几 KB，且语义层定义和业务问题混在一起，
   落库的收益（能对账）在指纹 + 首段这里已经拿到了，风险（数据面扩散）是真的。

3. **预算与限流**
   · `ctx.budget_tokens`：单次上限，0 = 不限；
   · `gateway.daily_cost_cny`：每用户每天成本上限；
   · `gateway.max_calls_per_min`：调用频次上限。
   被拒时**不发请求**（省的是真钱），抛 `GatewayRefused`——它是 `LLMError`
   的子类，所以现有的 `except llm.LLMError` 不用改，但上层可以靠 `.reason`
   区分"模型坏了"和"不让你调"。

4. **降级可见**
   台账写失败**不许**阻断问答，但必须挂到 `ledger_error` 上：
   "记账失败却看起来一切正常"是这套系统里最危险的假绿。
"""
from __future__ import annotations

import _bootstrap  # noqa: F401

import hashlib
import json
import threading
import time
from urllib.parse import urlparse

import egress
import llm
from governance import pii, store
from governance.contracts import cost_cny, RequestContext
from governance.ratelimit import limiter

_MAX_HEAD = 200          # prompt_head / answer_head 的截断长度
_counter = {"calls": 0, "ok": 0, "failed": 0, "cached": 0,
            "refused_budget": 0, "refused_rate": 0, "ledger_failed": 0}
_lock = threading.Lock()


class GatewayRefused(llm.LLMError):
    """网关主动拒绝（预算 / 频次）。不重试、不计成本、不发请求。"""

    def __init__(self, reason: str, detail: str, retry_after: int = 0) -> None:
        super().__init__("网关拒绝（%s）：%s" % (reason, detail))
        self.reason = reason
        self.detail = detail
        self.retry_after = retry_after


# ------------------------------------------------------------------ 小工具

def _ident(ctx) -> dict:
    """ctx 可能是 RequestContext，也可能是 state["ctx"] 里那个 dict。

    两种都支持是因为调用方一边在图里（只有 dict），一边在 webapp（有对象）。
    让网关去适配，比让每个调用点各转一遍靠谱。
    """
    if ctx is None:
        return {}
    if isinstance(ctx, dict):
        return ctx
    if isinstance(ctx, RequestContext):
        return {"trace_id": ctx.trace_id, "user_id": ctx.user_id,
                "role": ctx.role, "budget_tokens": ctx.budget_tokens}
    return {}


def _estimate_tokens(messages: list[dict]) -> int:
    """粗估（中文约 1 字 1 token，英文 4 字符 1 token）。

    刻意不复用 promptlib.estimate_tokens：那会把 semantic/shape 整条链拉进
    网关的 import，而预算判断只需要一个量级。估歪了最多是预算松一点，
    不会漏账——真正的 token 数来自响应里的 usage。
    """
    n = 0.0
    for m in messages:
        for ch in (m.get("content") or ""):
            n += 1 if ord(ch) > 127 else 0.25
    return int(n)


def _head(text: str, n: int = _MAX_HEAD) -> str:
    s = " ".join((text or "").split())
    return s[:n]


def _digest(messages: list[dict]) -> str:
    h = hashlib.sha256()
    for m in messages:
        h.update((m.get("role") or "").encode("utf-8", "replace"))
        h.update(b"\x00")
        h.update((m.get("content") or "").encode("utf-8", "replace"))
        h.update(b"\x00")
    return h.hexdigest()[:32]


def _question_of(messages: list[dict]) -> str:
    """最后一条 user 消息 ≈ 用户原问题；system 里是语义层，不是"问了什么"。"""
    for m in reversed(messages or []):
        if (m.get("role") or "") == "user":
            return m.get("content") or ""
    return (messages or [{}])[-1].get("content") or ""


def _redact_messages(messages: list[dict]) -> tuple[list[dict], int]:
    """进模型前把 PII 抹掉，返回（新的消息列表, 命中处数）。

    为什么在网关这一层做，而不是各个节点各做一遍：
    网关是**唯一出口**，只要它抹了，"某个节点漏了一处"就不可能发生；
    反过来，五个调用点各写一次，就是五种漏法。

    三条约束：
      · 不改调用方的 list / dict —— 原对象还要给审计和回放用，就地抹了就没法对账；
      · 只处理 content 是 str 的（多模态 parts 本项目不传，硬脱反而会改坏结构）；
      · 复用 `governance/pii.py`，不在这里重写正则：两份正则会漂移，
        最后一定是"入口拦了、出口漏了"。
    """
    out: list[dict] = []
    hits = 0
    for m in messages or []:
        c = (m or {}).get("content")
        if isinstance(c, str) and c:
            red, n = pii.redact(c)
            if n:
                hits += n
                m = {**m, "content": red}
        out.append(m)
    return out, hits


def _with_pii(note: str, hits: int) -> str:
    """台账备注带上命中数：脱了几处本身就是审计信息——
    "这条问题里原本带着手机号"和"没带"是两件不同的事，不该长得一样。"""
    if not hits:
        return note
    tail = "pii_hits=%d" % hits
    return (note + " ｜ " + tail) if note else tail


# ------------------------------------------------------------------ 台账

def _ledger(rec: dict, cfg: dict | None) -> tuple[bool, str]:
    ok, err = store.insert_cost(rec, cfg)
    if not ok:
        with _lock:
            _counter["ledger_failed"] += 1
    return ok, err


def chat(messages: list[dict], cfg: dict, *, caller: str = "generate",
         json_mode: bool = False, temperature: float | None = None,
         max_tokens: int | None = None, ctx=None, trace_id: str = "",
         user_id: str = "", role: str = "", note: str = "",
         tools: list[dict] | None = None,
         tool_choice: str | dict | None = None) -> dict:
    """经网关调一次模型。

    **进模型前每条 message 的 content 都会过一遍 `pii.redact`**（手机号 / 身份证 /
    邮箱 / 银行卡 / IPv4 → `[TAG]`）：发给模型的、写进 `prompt_head` 的、算进
    `prompt_hash` 的，全是脱敏后的文本。命中处数随返回值带回（`pii_hits`），
    并写进台账备注。

    返回在传输层结果之上加了：caller / cost_cny / tokens_in / tokens_out /
    ledger_ok / ledger_error / prompt_hash / pii_hits / gate。

    tools/tool_choice 是 function calling 的透传：**不允许绕开网关直调**，
    否则"模型自己决定调哪个工具"就会变成没有任何预算与台账约束的野路子，
    一次死循环能烧掉一天的额度。调用方拿到的 tool_calls 由上层（node_react）
    负责执行，网关只管记账。
    """
    ident = _ident(ctx)
    trace_id = trace_id or ident.get("trace_id") or ""
    user_id = user_id or ident.get("user_id") or "internal-baseline"
    role = role or ident.get("role") or "system"
    budget_tokens = int(ident.get("budget_tokens") or 0)
    gw = cfg.get("gateway") or {}

    # 钩子在这一层确保装上：CLI、eval、web 三条入口都会先走到这里，
    # 不必让每个入口各记一次（漏一个入口，出网就是敞开的）。
    egress.install(cfg)

    # 意图层放行：代理环境下 socket 层只看得到 127.0.0.1，只有这里知道真域名。
    url = urlparse(llm.endpoint(cfg))
    egress.assert_target(url.hostname or "", url.port or 443)

    # P0-3：进模型前脱敏。放在预算估算之前——脱敏会缩短文本，
    # 令牌估算、prompt_hash、prompt_head 都必须按"真正发出去的"算，
    # 否则台账里存的还是原文（prompt_head 就是用户问句的头 200 字）。
    messages, pii_hits = _redact_messages(messages)

    g = cfg["generator"]
    tmax = int(max_tokens or g["max_tokens"])
    tin_est = _estimate_tokens(messages)

    if budget_tokens > 0 and tin_est + tmax > budget_tokens:
        with _lock:
            _counter["refused_budget"] += 1
        raise GatewayRefused(
            "budget", "本次约 %d + %d token，超过单次上限 %d"
            % (tin_est, tmax, budget_tokens))

    daily = float(gw.get("daily_cost_cny") or 0)
    spent = 0.0
    if daily > 0:
        spent = store.cost_today(user_id, cfg)
        if spent >= daily:
            with _lock:
                _counter["refused_budget"] += 1
            raise GatewayRefused(
                "daily_budget", "今日已花 %.4f 元，达到上限 %.2f 元" % (spent, daily))

    rpm = int(gw.get("max_calls_per_min") or 0)
    allowed, left = limiter().allow("llm:" + user_id, rpm, 60.0)
    if not allowed:
        with _lock:
            _counter["refused_rate"] += 1
        wait = limiter().retry_after("llm:" + user_id, rpm, 60.0)
        raise GatewayRefused("rate_limit", "每分钟上限 %d 次，%ds 后再试" % (rpm, wait), wait)

    base = {
        "trace_id": trace_id, "user_id": user_id, "role": role,
        "model": g.get("model") or "", "caller": caller,
        "prompt_hash": _digest(messages), "prompt_chars": sum(
            len(m.get("content") or "") for m in messages),
        "prompt_head": _head(_question_of(messages)), "note": _with_pii(note, pii_hits),
    }

    t0 = time.perf_counter()
    with _lock:
        _counter["calls"] += 1
    try:
        out = llm._transport_chat(messages, cfg, json_mode=json_mode,
                                  temperature=temperature, max_tokens=max_tokens,
                                  tools=tools, tool_choice=tool_choice)
    except llm.LLMError as e:
        with _lock:
            _counter["failed"] += 1
        ok, err = _ledger({**base, "ok": False, "cached": False,
                           "latency_ms": int((time.perf_counter() - t0) * 1000),
                           "attempts": 0, "error": str(e)[:500],
                           "tokens_in": tin_est, "tokens_out": 0,
                           "cost_cny": 0.0}, cfg)
        e.ledger_error = "" if ok else err          # 挂到异常上，上层能看到
        raise

    usage = out.get("usage") or {}
    tin = int(usage.get("prompt_tokens") or tin_est or 0)
    tout = int(usage.get("completion_tokens") or 0)
    cost = cost_cny(tin, tout, cfg)

    ok, err = _ledger({**base, "ok": True, "cached": False,
                       "latency_ms": int(out.get("elapsed_ms") or 0),
                       "attempts": int(out.get("attempts") or 1),
                       "error": "", "tokens_in": tin, "tokens_out": tout,
                       "cost_cny": cost,
                       "answer_head": _head(out.get("content") or ""),
                       # 工具调用必须留痕：出了问题要能回答"这一轮模型调了哪个工具、
                       # 参数是什么"。台账只记名字与参数摘要，不记返回（返回可能含数据）
                       "note": _with_pii(note or _tools_note(out.get("tool_calls")),
                                         pii_hits)}, cfg)
    with _lock:
        _counter["ok"] += 1

    out.update({
        "caller": caller, "cost_cny": cost, "tokens_in": tin, "tokens_out": tout,
        "ledger_ok": ok, "ledger_error": err,
        "prompt_hash": base["prompt_hash"],
        # 脱敏命中数：上层（trace / 页面 / 自测）据此判断"这条问题里原本带了 PII"。
        # 0 也要有值，缺字段会被误读成"没做脱敏"。
        "pii_hits": pii_hits,
        "gate": {"budget_tokens": budget_tokens, "rate_left": left,
                 "daily_spent": spent, "daily_limit": daily},
    })
    return out


def _tools_note(tool_calls) -> str:
    """台账备注：这一轮调了哪些工具。只记名字，不记参数全文（参数里可能带业务数据）。"""
    if not tool_calls:
        return ""
    names = []
    for t in tool_calls:
        fn = (t or {}).get("function") or {}
        n = str(fn.get("name") or "?")
        names.append(n)
    return ("tools:" + ",".join(names))[:200]


def record_cache_hit(cfg: dict, *, ctx=None, trace_id: str = "", user_id: str = "",                     role: str = "", key: str = "", caller: str = "cache") -> tuple[bool, str]:
    """缓存命中也要入账：0 token / 0 成本，但"这次没花钱"本身要留证据。

    否则成本曲线会出现无法解释的缺口——账面上少了一次调用，
    却查不到是缓存省下的，还是根本没执行。
    """
    ident = _ident(ctx)
    with _lock:
        _counter["cached"] += 1
    return _ledger({
        "trace_id": trace_id or ident.get("trace_id") or "",
        "user_id": user_id or ident.get("user_id") or "internal-baseline",
        "role": role or ident.get("role") or "system",
        "model": "", "caller": caller, "tokens_in": 0, "tokens_out": 0,
        "cost_cny": 0.0, "cached": True, "ok": True, "latency_ms": 0,
        "attempts": 0, "error": "", "prompt_hash": (key or "")[:32],
        "prompt_chars": 0, "prompt_head": "", "answer_head": "",
        "note": "答案缓存命中，未调用模型",
    }, cfg)


def stats(cfg: dict | None = None, days: int = 1) -> dict:
    """给 /api/gateway 用：库里的账 + 进程内的计数 + 出网状态，三块要一起看。"""
    gw = ((cfg or {}).get("gateway") or {})
    with _lock:
        proc = dict(_counter)
    return {
        "process": proc,
        "ledger": store.cost_stats(cfg, days=days),
        "egress": egress.stats(),
        "limits": {"max_calls_per_min": int(gw.get("max_calls_per_min") or 0),
                   "daily_cost_cny": float(gw.get("daily_cost_cny") or 0)},
    }


if __name__ == "__main__":
    from conf import load_config
    cfg = load_config()
    out = chat([{"role": "user", "content": "只回复两个字：接通"}], cfg,
               caller="smoke", max_tokens=10)
    print(out["content"].strip(), "|", out["model"],
          "|", out["tokens_in"], "+", out["tokens_out"], "tok",
          "| ¥", out["cost_cny"], "| ledger_ok", out["ledger_ok"])
