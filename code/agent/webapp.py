"""问数前端（FastAPI + 原生单页）。F 阶段产出，阶段 1 加身份边界。

为什么不是 Chainlit：本机的离线 PyPI 镜像里 chainlit 2.12.0 的硬依赖
``literalai==0.1.201`` 不存在（最高 0.0.102），``syncer`` 没有任何 wheel；
而按"取最新版本"自动抓包又会抓到 pydantic 1.8.2 / PyJWT 1.6.0，
装上去会直接摧毁已跑通的 A~D 阶段。已装的 FastAPI 0.135 + Uvicorn 0.46
零依赖就能提供同样的能力，且 trace 面板可以完全按答辩需要来设计。

    python code/agent/webapp.py            # 默认 http://127.0.0.1:8000
    python code/agent/webapp.py --port 8080

阶段 1 的边界（`docs/36-ADR.md` ADR-002 / ADR-006 / ADR-008）：
  · 所有问答端点必须带会话 cookie，`ctx` 一律由登录态反查得到；
    本文件**不构造** `RequestContext.system()`，没有无身份入口。
  · 问数限流按用户、登录失败按 IP + 用户名双维度（`governance.ratelimit`）。
  · 审计只读：任何人都改不了 `qa_logs`（库里有不可变触发器），
    页面只按角色决定"看自己的"还是"看全量"。

只绑定 127.0.0.1：**这个页面能查生产数仓，不要暴露到局域网**。
"""
from __future__ import annotations

import _bootstrap  # noqa: F401

import argparse
import asyncio
import datetime as _dt
import decimal
import hashlib
import json
import os
import re
import secrets
import sys
import time
from pathlib import Path

from fastapi import Depends, FastAPI, HTTPException, Request, Response
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel
from starlette.concurrency import run_in_threadpool

import answercache
import db
import dq
import egress
import etl_exec
import etl_gate
import etl_register
import etl_rowcount
import etlguard
import gateway
import guardtool
import opsmetrics
import semantic
import vault
from conf import load_config
from governance.policy import engine as policy_engine
from governance import store
from governance.identity import provider
from governance.ratelimit import limiter
import memory as _memory
from graph import ask, hitl_enabled, resume

HERE = Path(__file__).resolve().parent
INDEX = HERE / "web" / "index.html"

COOKIE = "da_session"
# 审计可见范围来自岗位定义（identity.ROLE_SPECS 的 audit_scope），不在这里写死角色名。
# 写死过一次的代价：改角色模型时这里不会报错，只会静默变成"所有人都看不到全量审计"。
from governance.identity import AUDIT_ALL_ROLES  # noqa: E402

app = FastAPI(title="Data Agent · 问数", docs_url=None, redoc_url=None)


@app.middleware("http")
async def same_origin_guard(request: Request, call_next):
    """CSRF 第二道：写操作必须同源。

    Cookie 已经是 `SameSite=Lax`，跨站的 POST 本来就带不上 cookie；
    但 Lax 在部分老浏览器上行为不一致，而且"带不上 cookie"和"明确拒绝"
    对审计来说不是一回事。这里对带 Origin 的写请求做一次显式比对，
    不带 Origin 的（curl / 服务端调用）放行——它们本来就没有浏览器会话。
    """
    if request.method in ("POST", "PUT", "PATCH", "DELETE"):
        origin = request.headers.get("origin")
        if origin:
            from urllib.parse import urlparse
            if urlparse(origin).netloc != (request.headers.get("host") or ""):
                return JSONResponse({"detail": "跨站请求被拒绝"}, status_code=403)
    return await call_next(request)


class AskIn(BaseModel):
    question: str
    conversation_id: str | None = None


class ResumeIn(BaseModel):
    """恢复一次被挂起的问答（P1-2）。

    `thread_id` 就是 /api/ask 返回的那个：它既是 checkpointer 的钥匙，
    也是审计里"中断 → 恢复"两条事件的关联键。
    """
    thread_id: str
    answer: str = ""
    conversation_id: str | None = None


class MemoryIn(BaseModel):
    content: str


class ConvIn(BaseModel):
    title: str | None = None


class LoginIn(BaseModel):
    username: str
    password: str


class ChartIn(BaseModel):
    metric: str = ""
    dim: str | None = None
    days: int = 7
    kind: str = "trend"          # trend 时间走势 / breakdown 维度分布
    # 直接指定预聚合表时的取数模式（如品类分布走 dws_genre_daily）
    table: str | None = None
    value_col: str | None = None


class FeedbackIn(BaseModel):
    trace_id: str
    verdict: str                 # useful / wrong / unclear
    reason: str | None = None
    comment: str | None = None
    trust_level: str | None = None   # 提交时那条答案的可信度，用来回验规则准不准


class EtlIn(BaseModel):
    requirement: str


def _safe(v):
    """state 里可能混进 Decimal / date / set，统一成 JSON 能吃的类型。"""
    if v is None or isinstance(v, (bool, int, str)):
        return v
    if isinstance(v, float):
        return v
    if isinstance(v, decimal.Decimal):
        return float(v)
    if isinstance(v, (_dt.date, _dt.datetime)):
        return v.isoformat()
    if isinstance(v, dict):
        return {str(k): _safe(x) for k, x in v.items()}
    if isinstance(v, (list, tuple, set)):
        return [_safe(x) for x in v]
    return str(v)


def _token_hash(token: str) -> str:
    """库里只存哈希：审计库被读走也拿不到可用会话。"""
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


# 会话快照里保留哪些字段：前端要能把答案卡原样画出来（表格、SQL、执行过程），
# 但不需要 ctx / policy_decision 这类权限内部结构。
_SNAP_KEYS = (
    "answer", "sql", "time_start", "time_end", "time_window", "metrics", "tables",
    "dimensions", "dimension_cautions", "verdict", "attempts", "elapsed_ms",
    "freshness", "result", "findings", "history", "fix_log", "shape", "shape_issues",
    "shape_retries", "shape_unresolved", "cache_hit", "cache_key", "compare_result",
    "refused", "refuse_reason", "meta_answer", "meta_intent", "followup",
    "cost_block", "cost_gate", "exec_retries", "trust", "trace_id",
    "caliber_version",
    # P1-2：中断的那一轮也要能原样还原——刷新会话后用户得看到
    # "系统问了什么、这轮在等谁回答"，否则历史里会是一条空回答。
    "interrupted", "clarify_question", "thread_id", "hitl",
)


def _trim(v, depth: int = 0, limit: int = 400):
    """截断长字符串。trace.data 里可能塞了整段 prompt，那是审计该存的东西，
    会话快照只需要"这一步做了什么"。"""
    if depth > 6:
        return "…"
    if isinstance(v, str):
        return v if len(v) <= limit else v[:limit] + "…"
    if isinstance(v, dict):
        return {str(k): _trim(x, depth + 1, limit) for k, x in list(v.items())[:30]}
    if isinstance(v, (list, tuple, set)):
        return [_trim(x, depth + 1, limit) for x in list(v)[:30]]
    return v


def _snapshot(st: dict) -> dict:
    snap = {}
    for k in _SNAP_KEYS:
        v = st.get(k)
        if v is None or v == "" or v == [] or v == {}:
            continue
        snap[k] = v
    res = dict(snap.get("result") or {})
    rows = res.get("rows") or []
    if len(rows) > 50:                      # 会话恢复只需要看得见的那部分
        res["rows"] = rows[:50]
        res["rows_truncated"] = True
    if res:
        snap["result"] = res
    snap["trace"] = [{"node": s.get("node"),
                      "detail": (s.get("detail") or "")[:500],
                      "data": _trim(s.get("data") or {})}
                     for s in (st.get("trace") or [])[:40]]
    hist = []
    for h in (snap.get("history") or [])[:10]:
        hist.append({"attempt": h.get("attempt"), "stage": h.get("stage"),
                     "verdict": h.get("verdict"), "sql": (h.get("sql") or "")[:2000]})
    if hist:
        snap["history"] = hist
    return _safe(snap)


def _client_ip(req: Request) -> str:
    return (req.client.host if req.client else "0.0.0.0") or "0.0.0.0"


def current_user(req: Request) -> dict:
    """会话 → 用户档案。任一环节失败都 401，不做匿名降级。"""
    cfg = load_config()
    token = req.cookies.get(COOKIE)
    if not token:
        raise HTTPException(401, "未登录")
    uid = store.session_user(_token_hash(token), cfg)
    if not uid:
        raise HTTPException(401, "会话已过期，请重新登录")
    prof = provider(cfg).profile(uid)
    if not prof:
        raise HTTPException(401, "账号不可用")
    return prof


def _ctx_for(prof: dict):
    """登录态 → RequestContext。source=web，unrestricted=False。"""
    cx = provider(load_config()).context_for_user(prof["user_id"], source="web")
    if cx is None:
        raise HTTPException(401, "账号状态异常")
    return cx


# ---------------------------------------------------------------- 认证
@app.post("/api/login")
def login(body: LoginIn, req: Request, resp: Response):
    cfg = load_config()
    sec = cfg.get("security") or {}
    fail_limit = int(sec.get("login_fail_limit_per_min", 5))
    ip = _client_ip(req)
    uname = (body.username or "").strip()
    if not uname or not body.password:
        raise HTTPException(400, "用户名和密码不能为空")

    # 双维度：单账号爆破 + 单 IP 撞库都要盖住。命中任一即拒。
    for key in ("login:u:%s" % uname, "login:ip:%s" % ip):
        ok, _ = limiter().allow(key, fail_limit, 60.0)
        if not ok:
            wait = limiter().retry_after(key, fail_limit, 60.0)
            return JSONResponse(
                {"error": "登录尝试过于频繁，请 %d 秒后再试" % wait,
                 "retry_after": wait},
                status_code=429, headers={"Retry-After": str(wait)})

    cx = provider(cfg).authenticate(uname, body.password, source="web")
    if cx is None:
        # 不区分「无此账号」和「密码错」——否则就成了账号枚举接口
        raise HTTPException(401, "用户名或密码错误")

    # 成功才清失败计数（否则爆破者可以用一次正确密码重置配额）
    for key in ("login:u:%s" % uname, "login:ip:%s" % ip):
        limiter().reset(key)

    token = secrets.token_urlsafe(32)
    if not store.insert_session(_token_hash(token), cx.user_id,
                                int(sec.get("session_ttl_min", 480)), cfg):
        raise HTTPException(503, "会话创建失败（治理库不可用）")
    resp.set_cookie(COOKIE, token, httponly=True, samesite="lax",
                    max_age=int(sec.get("session_ttl_min", 480)) * 60, path="/")
    prof = provider(cfg).profile(cx.user_id) or {}
    return {"ok": True, "user": prof, "trace_id": cx.trace_id}


@app.post("/api/logout")
def logout(req: Request, resp: Response):
    token = req.cookies.get(COOKIE)
    if token:
        store.delete_session(_token_hash(token), load_config())
    resp.delete_cookie(COOKIE, path="/")
    return {"ok": True}


@app.get("/api/me")
def me(prof: dict = Depends(current_user)):
    cfg = load_config()
    cx = _ctx_for(prof)
    d = provider(cfg).decision(cx)
    sec = cfg.get("security") or {}
    qlimit = int(sec.get("rate_limit_per_min", 20))
    return {
        "user": prof,
        "policy": {
            "role": d.role, "role_name": prof.get("role_name"),
            "role_description": prof.get("role_description"),
            "scope": d.scope, "read_only": d.read_only,
            "policy_hash": d.policy_hash, "reason": d.reason,
            "allow_tables": len(d.allow_tables), "deny_tables": len(d.deny_tables),
            "row_rules": len(d.row_rules), "deny_columns": sorted(d.deny_columns),
            "mask_columns": sorted(d.mask_columns), "max_rows": d.max_rows,
        },
        "quotas": {"ask_per_min": qlimit,
                   "remaining": limiter().remaining("ask:%s" % prof["user_id"], qlimit)},
        "can_read_all_audit": prof["role"] in AUDIT_ALL_ROLES,
    }


# ---------------------------------------------------------------- 问数
@app.post("/api/ask")
async def api_ask(body: AskIn, prof: dict = Depends(current_user)):
    """问数入口（异步）。

    为什么这一条要 async，别的路由不改
    ----------------------------------
    33 个路由里只有 /api/ask 是长耗时：一次要等模型 2~10 秒。它如果占着事件
    循环，后面所有请求（包括登录、拉会话列表这种几十毫秒的）都得排队。其余
    路由本身是毫秒级，改成 async 只是把同步调用塞进线程池，多一次调度开销，
    而且在 async 函数里漏包一个阻塞调用会**直接卡死整个事件循环**——比同步 def
    危险得多。所以只在真正需要的地方异步。

    信号量是这里的实际收益：没有它，20 个并发 ask 会各自占一个线程等模型，
    anyio 默认线程池只有 40 个，几个用户连点就能把池子占满，之后连"读首页"
    都排不上。限到 8 之后，第 9 个请求在信号量上排队，而不是去抢线程。
    """
    cfg0 = load_config()
    sec = cfg0.get("security") or {}
    n = max(1, int(sec.get("max_concurrent_ask", 8)))
    tout = float(sec.get("ask_timeout_sec", 300))
    async with _ask_semaphore(n):
        try:
            return await asyncio.wait_for(
                run_in_threadpool(_ask_blocking, body, prof, cfg0), timeout=tout)
        except asyncio.TimeoutError:
            # 线程本身停不下来（Python 没有抢占式取消），但请求可以立刻返回。
            # 说清"超时"比让浏览器一直转圈好：用户能据此缩小时间范围重试。
            return JSONResponse(
                {"error": "问答超过 %.0f 秒仍未完成，已中止等待。"
                          "请缩小时间范围或稍后重试。" % tout,
                 "timeout": True}, status_code=504)


_ASK_SEM: dict = {}


def _ask_semaphore(n: int):
    """按 (事件循环, 并发上限) 缓存信号量。

    两件事都必须判：
    · 上限变了要重建——配置热改得能生效；
    · 循环变了也要重建——asyncio.Semaphore 内部会缓存创建它的 loop，
      在另一个 loop 上真正发生等待时行为不可靠（测试里每次 asyncio.run
      都是新 loop，生产里 uvicorn 只有一个，所以正常路径不会碰到）。
    """
    loop = asyncio.get_running_loop()
    key = (id(loop), n)
    cur = _ASK_SEM.get("sem")
    if cur is None or _ASK_SEM.get("key") != key:
        cur = asyncio.Semaphore(n)
        _ASK_SEM["sem"] = cur
        _ASK_SEM["key"] = key
    return cur


def _ask_blocking(body: AskIn, prof: dict, cfg0: dict):
    """同步实现的问数。整体丢线程池——不拆细粒度并发的原因见上面 docstring：
    DB 连接池只有 pool_size=3，两个查询"并发"只会互相抢连接，不如串行。"""
    q = (body.question or "").strip()
    if not q:
        raise HTTPException(400, "问题不能为空")
    cfg = load_config()
    sec = cfg.get("security") or {}
    qlimit = int(sec.get("rate_limit_per_min", 20))
    key = "ask:%s" % prof["user_id"]
    ok, left = limiter().allow(key, qlimit, 60.0)
    if not ok:
        wait = limiter().retry_after(key, qlimit, 60.0)
        return JSONResponse({"error": "配额已用尽（%d 次/分钟），请 %d 秒后再试"
                                      % (qlimit, wait), "retry_after": wait},
                            status_code=429, headers={"Retry-After": str(wait)})

    ctx = _ctx_for(prof)

    # 会话：带 id 就接着聊（先取最近几轮当上下文），不带就开一个新的。
    # 取历史必须在写这条消息之前，否则本轮问题会被当成"上一轮"。
    conv_id = (body.conversation_id or "").strip()
    history: list = []
    if conv_id:
        if not store.get_conversation(conv_id, prof["user_id"], cfg):
            # 不存在与"不是你的"返回同一个 404：不泄漏别人会话是否存在。
            raise HTTPException(404, "会话不存在或已删除")
        history = store.recent_turns(conv_id, prof["user_id"], turns=3, cfg=cfg)
    else:
        conv_id, cerr = store.create_conversation(
            prof["user_id"], ctx.tenant_id, title=q[:40], cfg=cfg)
        if not conv_id:
            return JSONResponse({"error": "无法创建会话：%s" % cerr}, status_code=500)

    # 问题先落库：模型调用崩了也不能让"用户问过什么"凭空消失。
    uok, uerr = store.append_message(conv_id, "user", q, cfg=cfg)

    # 显式记忆指令：用户说"以后都按播放人数算"才落库，且当场回报记了什么。
    # 自动归纳不做——猜出来的偏好用户不知道、也删不干净。
    if _memory.is_remember(q):
        mok, merr = _memory.add(prof["user_id"], q, cfg, tenant_id=ctx.tenant_id)
        note = ("已记住这条口径偏好，之后的同类问题会按它理解。"
                if mok else "记忆写入失败：%s" % merr[:80])
        aok, aerr = store.append_message(conv_id, "assistant", note, cfg=cfg)
        store.set_title_if_default(conv_id, q, cfg=cfg)
        out = {"answer": note, "conversation_id": conv_id, "kind": "memory",
               "memory_saved": bool(mok), "quota_left": left,
               "elapsed_ms": int((time.perf_counter() - t0) * 1000)}
        if not (uok and aok):
            out["conv_error"] = uerr or aerr
        return JSONResponse(out)
    if _memory.is_forget(q):
        note = "要删除具体某条偏好，请到「记忆」面板操作。"
        store.append_message(conv_id, "assistant", note, cfg=cfg)
        return JSONResponse({"answer": note, "conversation_id": conv_id,
                             "kind": "memory", "quota_left": left,
                             "elapsed_ms": int((time.perf_counter() - t0) * 1000)})

    # 建表意图分流：问数回答"数仓里有什么"，"给数仓加什么"必须走 ETL 开发。
    # 不分流会发生什么：用户说"帮我加一个 ads 表"，问数链路硬查一把现有表
    # 冒充建表结果（2026-11-11 实测：返回一张查询表格，表根本没建）。
    if _CREATE_INTENT_RE.search(q):
        is_eng = prof.get("role") == "data_engineer"
        note = ("这是一个建表/加工需求，已转入 ETL 开发流程，请在弹出的开发面板里确认并生成。"
                if is_eng else
                "这是一个建表/加工需求。问数只能查数仓里已有的数据，改造数仓（建表/加工）"
                "由数据开发工程师通过 ETL 开发完成——当前账号（%s）无此权限，"
                "可把需求转给工程师在「ETL」面板提交。"
                % (prof.get("role_name") or prof.get("role") or "当前角色"))
        aok, aerr = store.append_message(conv_id, "assistant", note, cfg=cfg)
        store.set_title_if_default(conv_id, q, cfg=cfg)
        return JSONResponse(_safe({
            "kind": "etl_redirect", "allowed": is_eng, "note": note,
            "requirement": q, "conversation_id": conv_id,
            "quota_left": left, "elapsed_ms": 0,
            "conv_error": None if aok else aerr}))

    t0 = time.perf_counter()
    try:
        # thread_id 只在 HITL 开着时给：带上它图才会挂 checkpointer，
        # plan 节点因此可以 interrupt() 暂停等用户澄清。
        # 长期偏好只从真实用户入口召回：eval/cli 不带 user_id，
        # 召回为空 → prompt 与缓存键与没有记忆时逐字相同。
        mems = _memory.recall(prof["user_id"], q, cfg,
                              limit=int((cfg.get("memory") or {}).get("top") or 3))
        st = ask(q, cfg, ctx=ctx,
                 history_turns=history,
                 thread_id=ctx.trace_id if hitl_enabled(cfg) else None,
                 memory_turns=mems)
    except Exception as e:                     # 单条问答崩了不能带崩服务
        return JSONResponse({"error": "%s: %s" % (type(e).__name__, e),
                             "trace_id": ctx.trace_id,
                             "conversation_id": conv_id,
                             "elapsed_ms": int((time.perf_counter() - t0) * 1000)},
                            status_code=200)
    out = _safe(dict(st))
    out["elapsed_ms"] = int((time.perf_counter() - t0) * 1000)
    out["quota_left"] = left
    out["conversation_id"] = conv_id

    # 被 HITL 挂起：这一轮没有答案，只有一句要问用户的话。
    # 仍然把它存成 assistant 消息 —— 用户刷新会话时要能看到"系统问了什么"。
    if st.get("interrupted"):
        aok, aerr = store.append_message(
            conv_id, "assistant", st.get("clarify_question") or "",
            payload=_snapshot(st), trace_id=ctx.trace_id, cfg=cfg)
        store.set_title_if_default(conv_id, q, cfg=cfg)
        out.update({"interrupted": True, "kind": "clarify",
                    "thread_id": st.get("thread_id") or ctx.trace_id,
                    "clarify_question": st.get("clarify_question") or ""})
        if not (uok and aok):
            out["conv_error"] = uerr or aerr
        return JSONResponse(out)

    aok, aerr = store.append_message(
        conv_id, "assistant", st.get("answer") or "",
        payload=_snapshot(st), trace_id=ctx.trace_id, cfg=cfg)
    store.set_title_if_default(conv_id, q, cfg=cfg)
    # 会话写失败要说出来：静默失败 = 用户以为存上了，回头翻历史发现没有。
    if not (uok and aok):
        out["conv_error"] = uerr or aerr
    return JSONResponse(out)


@app.post("/api/ask/resume")
def api_ask_resume(body: ResumeIn, prof: dict = Depends(current_user)):
    """恢复被挂起的问答（P1-2）。刻意保持同步 def —— 与其余 /api 路由一致，
    长耗时靠 FastAPI 的线程池，不在 async 里做阻塞调用。

    安全边界在这里，不在前端：
      · thread_id 不是"贴上来就能用"的——graph.resume 会核这条 checkpoint
        的 ctx.user_id，别人的 thread 直接 403；
      · 已完成的 thread 再恢复只会回放同一份结果（`duplicate_resume=true`），
        **不会重跑 SQL**——重复点击"继续"不该让数据库再被查一次。
    """
    cfg = load_config()
    sec = cfg.get("security") or {}
    qlimit = int(sec.get("rate_limit_per_min", 20))
    ok, left = limiter().allow("resume:%s" % prof["user_id"], qlimit, 60.0)
    if not ok:
        wait = limiter().retry_after("resume:%s" % prof["user_id"], qlimit, 60.0)
        return JSONResponse({"error": "配额已用尽（%d 次/分钟），请 %d 秒后再试"
                                      % (qlimit, wait), "retry_after": wait},
                            status_code=429, headers={"Retry-After": str(wait)})

    ctx = _ctx_for(prof)
    t0 = time.perf_counter()
    try:
        st = resume(body.answer, cfg, ctx=ctx, thread_id=body.thread_id)
    except PermissionError as e:
        raise HTTPException(403, str(e))
    except KeyError as e:
        raise HTTPException(404, str(e).strip("'\""))
    except (ValueError, RuntimeError) as e:
        raise HTTPException(400, str(e))
    except Exception as e:                     # 恢复失败不能带崩服务
        return JSONResponse({"error": "%s: %s" % (type(e).__name__, e),
                             "thread_id": body.thread_id,
                             "elapsed_ms": int((time.perf_counter() - t0) * 1000)},
                            status_code=200)

    out = _safe(dict(st))
    out["elapsed_ms"] = int((time.perf_counter() - t0) * 1000)
    out["quota_left"] = left
    out["thread_id"] = body.thread_id

    # 落会话历史：把"用户补的那句"和"最后的答案"都记上，否则这段对话是断的。
    conv_id = (body.conversation_id or "").strip()
    if conv_id and store.get_conversation(conv_id, prof["user_id"], cfg):
        out["conversation_id"] = conv_id
        uok, uerr = store.append_message(
            conv_id, "user", "（澄清）%s" % (body.answer or ""), cfg=cfg)
        aok, aerr = store.append_message(
            conv_id, "assistant", st.get("answer") or "",
            payload=_snapshot(st), trace_id=ctx.trace_id, cfg=cfg)
        if not (uok and aok):
            out["conv_error"] = uerr or aerr
    return JSONResponse(out)


# ---------------------------------------------------------------- 长期偏好
@app.get("/api/memory")
def api_memory_list(prof: dict = Depends(current_user)):
    """只看自己的偏好。查询条件里就带 user_id，不靠前端过滤。"""
    rows = _memory.list_all(prof["user_id"], load_config())
    return {"rows": [{"mem_id": r.get("mem_id"), "kind": r.get("kind"),
                      "content": r.get("content"),
                      "caliber_version": r.get("caliber_version"),
                      "hits": r.get("hits"), "updated_at": _safe(r.get("updated_at"))}
                     for r in rows]}


@app.post("/api/memory")
def api_memory_add(body: MemoryIn, prof: dict = Depends(current_user)):
    """手工添加一条偏好：用户说清楚记什么，系统不猜。"""
    cfg = load_config()
    text = (body.content or "").strip()
    if not text:
        raise HTTPException(400, "偏好内容不能为空")
    ok, err = _memory.add(prof["user_id"], text, cfg,
                          tenant_id=_ctx_for(prof).tenant_id)
    if not ok:
        return JSONResponse({"error": err}, status_code=400)
    return {"ok": True, "rows": _memory.list_all(prof["user_id"], cfg)}


@app.delete("/api/memory/{mem_id}")
def api_memory_delete(mem_id: int, prof: dict = Depends(current_user)):
    """按 id + user_id 软删；删别人的返回 404，不泄漏是否存在。"""
    if not _memory.remove(mem_id, prof["user_id"], load_config()):
        raise HTTPException(404, "偏好不存在或已删除")
    return {"ok": True}


# ---------------------------------------------------------------- 会话历史
@app.get("/api/conversations")
def api_conversations(limit: int = 50, prof: dict = Depends(current_user)):
    return {"rows": store.list_conversations(prof["user_id"], limit, load_config())}


@app.post("/api/conversations")
def api_conversation_create(body: ConvIn, prof: dict = Depends(current_user)):
    ctx = _ctx_for(prof)
    cid, err = store.create_conversation(
        prof["user_id"], ctx.tenant_id, (body.title or "新对话"), load_config())
    if not cid:
        raise HTTPException(500, "创建会话失败：%s" % err)
    return {"conversation_id": cid}


@app.get("/api/conversations/{conv_id}")
def api_conversation_get(conv_id: str, prof: dict = Depends(current_user)):
    """一次取全会话：会话信息 + 按轮配好的消息。

    配对放在服务端做：前端只关心"问题 + 那一轮的答案快照"，
    让它自己去猜哪条 user 对应哪条 assistant，迟早会因为半轮消息错位。
    """
    cfg = load_config()
    conv = store.get_conversation(conv_id, prof["user_id"], cfg)
    if not conv:
        raise HTTPException(404, "会话不存在或已删除")
    msgs = store.load_messages(conv_id, prof["user_id"], cfg=cfg) or []
    turns: list[dict] = []
    cur: dict | None = None
    for m in msgs:
        if m["role"] == "user":
            cur = {"question": m.get("content") or "",
                   "at": m.get("created_at"), "answer": None}
            turns.append(cur)
        elif cur is not None:
            cur["answer"] = m.get("payload")
            cur["at"] = m.get("created_at") or cur["at"]
            cur = None
    return {"conversation": conv, "turns": turns}


@app.patch("/api/conversations/{conv_id}")
def api_conversation_rename(conv_id: str, body: ConvIn,
                            prof: dict = Depends(current_user)):
    if not store.rename_conversation(conv_id, prof["user_id"],
                                     body.title or "新对话", load_config()):
        raise HTTPException(404, "会话不存在或已删除")
    return {"ok": True}


@app.delete("/api/conversations/{conv_id}")
def api_conversation_delete(conv_id: str, prof: dict = Depends(current_user)):
    """软删：从列表里消失，审计证据仍完整留在 qa_logs。"""
    if not store.delete_conversation(conv_id, prof["user_id"], load_config()):
        raise HTTPException(404, "会话不存在或已删除")
    return {"ok": True}


# ---------------------------------------------------------------- 反馈回流
@app.post("/api/feedback")
def api_feedback(body: FeedbackIn, prof: dict = Depends(current_user)):
    """对一条答案打标：有用 / 不准 / 看不懂。

    为什么要有这个
    --------------
    128 题评测是我们自己出的题，反馈来自真实提问——前者证明"我们以为的对"，
    后者证明"用户实际用得怎样"。没有反馈，badcase 只能靠人肉回忆。

    权限：只能标自己问过的那条（engineer 可代标，用于整理 badcase）。
    没有这层校验，任何人都能按 trace_id 猜着给别人的答案打标，
    统计出来的采纳率就是污染数据。
    """
    cfg = load_config()
    tid = (body.trace_id or "").strip()
    if not tid:
        raise HTTPException(400, "trace_id 不能为空")
    owner = store.qa_owner(tid, cfg)
    if not owner:
        raise HTTPException(404, "这条记录不存在（可能已过期归档）")
    if owner.get("user_id") != prof["user_id"] and prof["role"] not in AUDIT_ALL_ROLES:
        raise HTTPException(403, "只能对自己问过的答案反馈")
    ok, err = store.upsert_feedback(
        tid, prof["user_id"], (body.verdict or "").strip(),
        role=prof.get("role") or "",
        reason=(body.reason or "")[:80], comment=(body.comment or "")[:1000],
        question=str(owner.get("question") or "")[:500],
        trust_level=(body.trust_level or "")[:16], cfg=cfg)
    if not ok:
        # 失败要显式返回：前端提示"没记上"，用户还能重试——
        # 静默成功最糟，用户以为贡献了一条 badcase，实际什么都没留下。
        return JSONResponse({"error": "反馈没记上：%s" % err}, status_code=500)
    return {"ok": True, "trace_id": tid, "verdict": body.verdict}


@app.get("/api/feedback/stats")
def api_feedback_stats(days: int = 30, prof: dict = Depends(current_user)):
    """采纳率与不准原因。普通账号只看自己的，治理岗看全量。

    `by_trust` 是这套设计的自检：可信度低的答案被标不准的比例，
    必须高于可信度高的那一档，否则说明规则白定了。
    """
    cfg = load_config()
    scope_all = prof["role"] in AUDIT_ALL_ROLES
    return store.feedback_stats(cfg, days=max(1, min(int(days), 365)),
                                user_id=None if scope_all else prof["user_id"])


# ---------------------------------------------------------------- 审计
@app.get("/api/audit")
def api_audit(limit: int = 30, trace_id: str | None = None,
              prof: dict = Depends(current_user)):
    """查审计。**这次查询本身也要留痕**（AC-14）。

    记录写在返回之前、失败不阻断（审计访问记录是旁路证据，不能因为它
    失败就让"查审计"这个动作失败），但必须如实告诉调用方记没记上——
    响应里的 `access_logged` 就是这件事，前端会显示出来。
    静默失败等于"以为记了"。
    """
    cfg = load_config()
    scope_all = prof["role"] in AUDIT_ALL_ROLES
    rows = store.fetch_audit(
        cfg, trace_id=trace_id, limit=limit,
        user_id=None if scope_all else prof["user_id"])
    ok, err = store.insert_access(
        prof["user_id"], prof["role"], "read", target=trace_id or "",
        filters={"limit": limit, "scope": "all" if scope_all else "self"},
        rows=len(rows), source="web", cfg=cfg)
    return {"rows": rows, "scope": "all" if scope_all else "self",
            "access_logged": ok, "access_error": "" if ok else err[:160]}


@app.get("/api/audit/access")
def api_audit_access(limit: int = 20, prof: dict = Depends(current_user)):
    """谁看过审计。只有治理岗能看——否则"监督者"自己也没人监督。"""
    if prof["role"] not in AUDIT_ALL_ROLES:
        raise HTTPException(403, "只有治理岗可以查看审计访问记录")
    cfg = load_config()
    return {"rows": store.fetch_access(cfg, limit=limit)}


@app.get("/api/secrets")
def api_secrets(prof: dict = Depends(current_user)):
    """密钥治理视图（AC-21）。**永远不返回真值**，连掩码对有前缀的键也只给一部分。

    这里返回 `revealed_by` 是刻意的：它回答的是"哪些模块真的取过这个密钥"。
    一个键配了却从没人取（revealed_by 为空），要么是配错了名字，
    要么是功能没接上——两种都值得看一眼。
    """
    if prof["role"] not in AUDIT_ALL_ROLES:
        raise HTTPException(403, "只有治理岗可以查看密钥状态")
    cfg = load_config()
    vault.warmup(cfg)
    ok, missing = vault.validate(cfg)
    return {
        "ready": ok,
        "missing": missing,
        "items": vault.audit(),
        "weak": vault.weak(),
        "note": ("只显示来源、长度与取用模块，不显示密钥内容。"
                 "长度 < 12 位标为弱密钥；本地服务可接受，对外服务不可。"),
    }


@app.get("/api/audit/retention")
def api_audit_retention(prof: dict = Depends(current_user)):
    """保留期预览：按配置的天数算一遍"现在清理会删多少"，不真删。"""
    if prof["role"] not in AUDIT_ALL_ROLES:
        raise HTTPException(403, "只有治理岗可以查看保留期")
    from governance import retention as gov_retention
    cfg = load_config()
    return {"plan": gov_retention.plan(None, cfg),
            "trigger_enabled": gov_retention.trigger_enabled(cfg),
            "days": int((cfg.get("governance") or {}).get("retention_days") or 180)}


@app.get("/api/cache")
def api_cache(prof: dict = Depends(current_user)):
    """缓存与护栏池的运行状态。给运维看，不含任何业务数据。"""
    return {"cache": answercache.stats(),
            "guard": {"mode": (load_config().get("guard") or {}).get("mode"),
                      "serve": (load_config().get("guard") or {}).get("serve"),
                      "workers": (load_config().get("guard") or {}).get("workers")}}


@app.post("/api/cache/clear")
def api_cache_clear(prof: dict = Depends(current_user)):
    if prof["role"] not in AUDIT_ALL_ROLES:
        raise HTTPException(403, "只有治理岗可以清缓存")
    answercache.get_cache(load_config()).clear()
    return {"cleared": True}


@app.get("/api/stats")
def api_stats(days: int = 1, prof: dict = Depends(current_user)):
    if prof["role"] not in AUDIT_ALL_ROLES:
        raise HTTPException(403, "无权限查看全局统计")
    return store.stats(load_config(), days=max(1, min(int(days), 30)))


@app.get("/api/gateway")
def api_gateway(days: int = 1, prof: dict = Depends(current_user)):
    """模型网关状态（AC-18 / AC-22）：台账、预算、限流、出网。

    只有治理岗能看全量成本——成本曲线本身就是经营信息，不是人人该看的。
    其他人的视角是"我自己今天花了多少"，靠 by_role 那一段裁出来。
    """
    cfg = load_config()
    s = gateway.stats(cfg, days=max(1, min(int(days), 30)))
    if prof["role"] in AUDIT_ALL_ROLES:
        return s
    mine = [x for x in s["ledger"].get("by_role") or []
            if x.get("role") == prof["role"]]
    return {
        "process": s["process"],
        "ledger": {"ok": s["ledger"].get("ok"), "days": s["ledger"].get("days"),
                   "total": s["ledger"].get("total", {}), "by_role": mine,
                   "error": s["ledger"].get("error", "")},
        "egress": {"installed": s["egress"]["installed"],
                   "blocked": s["egress"]["blocked"],
                   "allow_hosts": s["egress"]["allow_hosts"]},
        "limits": s["limits"],
        "scope": "self",
    }


@app.get("/api/health")
def health():
    """给运维探活用：只暴露连通性，不暴露 DSN / 密码。"""
    s = store.check(load_config())
    return {"governance": s}


# ---------------------------------------------------------------- 静态
@app.get("/api/meta")
def meta():
    cfg = load_config()
    off = cfg["lane"]["offline"]
    return {
        "catalog": cfg["execution"]["catalog"],
        "database": cfg["execution"]["database"],
        "data_since": off.get("data_since"),
        "data_until": off["data_until"],
        "clock": cfg.get("clock", {}),
        "model": cfg["generator"]["model"],
        "mode": cfg["generator"]["mode"],
        "guards": 23,
    }


# ---------------------------------------------------------------- 语义层浏览（工作台：统一语义可检索）
# 对齐业界 Data Agent 的「语义层浏览器」：指标/表/维度/术语/口径规则一份出口，
# 全部从语义层现算，不落第二份副本——数字漂移的来源就是复制。
@app.get("/api/semantic")
def api_semantic(prof: dict = Depends(current_user)):
    cfg = load_config()
    metrics = []
    for code, m in (semantic.all_metrics(cfg) or {}).items():
        metrics.append({
            "code": code, "name_cn": m.get("name_cn"), "tier": m.get("tier"),
            "unit": m.get("unit"), "formula": (m.get("formula") or "").strip(),
            "source": m.get("source"), "filter": m.get("filter"),
            "additivity": m.get("additivity"),
            "availability": m.get("availability"),
            "trap": (m.get("trap") or "").strip(),
            "note": (m.get("note") or "").strip(),
        })
    tables = []
    for name, t in (semantic.all_tables(cfg) or {}).items():
        tables.append({
            "name": name, "name_cn": t.get("name_cn"), "layer": t.get("layer"),
            "grain": t.get("grain"), "rows": t.get("rows"),
            "catalog": t.get("catalog"), "database": t.get("database"),
            "pk": t.get("pk"), "columns": t.get("columns") or [],
            "caveats": t.get("caveats") or [],
            "sources": (t.get("source") or "").strip(),
        })
    dims = [{"code": k, **(v or {})}
            for k, v in (semantic.all_dimensions(cfg) or {}).items()]
    terms = [{"term": k, **(v or {})}
             for k, v in (semantic.terms(cfg) or {}).items()]
    return {
        "counts": {
            "metrics": len(metrics), "tables": len(tables),
            "dimensions": len(dims), "terms": len(terms),
            "caliber": len(semantic.caliber_rules(cfg) or []),
        },
        "metrics": metrics,
        "tables": tables,
        "dimensions": dims,
        "terms": terms,
        "caliber": semantic.caliber_rules(cfg) or [],
        "relationships": semantic.all_relationships(cfg) or [],
        "note": "语义层是唯一事实源：护栏、检索、看板与 ETL 校验共用这份数据，"
                "YAML 改动经 gen_tables.py + sem_check.py 才会到这里。",
    }


# ---------------------------------------------------------------- Trace 回放（工作台：结论的证据一键可达）
# 审计行只给摘要；这里按 trace_id 取整条运行记录（含节点事件、护栏结果、
# SQL 演化、机械修复日志），前端用它渲染 DAG 复盘。查看行为本身照 AC-14 留痕。
@app.get("/api/trace/{trace_id}")
def api_trace(trace_id: str, prof: dict = Depends(current_user)):
    cfg = load_config()
    scope_all = prof["role"] in AUDIT_ALL_ROLES
    rows = store.fetch_audit(
        cfg, trace_id=trace_id, limit=1,
        user_id=None if scope_all else prof["user_id"])
    if not rows:
        raise HTTPException(404, "没有这条 Trace，或你没有查看它的权限")
    ok, err = store.insert_access(
        prof["user_id"], prof["role"], "replay", target=trace_id,
        filters={"scope": "all" if scope_all else "self"},
        rows=1, source="web", cfg=cfg)
    return {"row": rows[0], "access_logged": ok, "access_error": "" if ok else err[:160]}


# ---------------------------------------------------------------- 看板图表
# 看板不走 LLM：指标 → SQL 是语义层里现成的机械映射（formula + source + filter），
# 让模型抄一遍只会慢和多错。真正要过的是 policy 引擎——行级 scope、列脱敏
# 必须和问数走同一套，否则看板就成了绕过权限的后门。
def _chart_sql(m: dict, kind: str, dim: str | None, days: int, cfg: dict) -> str:
    src = m.get("source") or ""
    formula = (m.get("formula") or "").strip().rstrip(";")
    cols = [c.lower() for c in semantic.table_columns(src, cfg)]
    if "dt" not in cols:
        raise HTTPException(400, "指标 %s 的源表 %s 没有时间分区列" % (m.get("code"), src))
    off = cfg["lane"]["offline"]
    end = str(off["data_until"])
    start = (_dt.date.fromisoformat(end) - _dt.timedelta(days=max(1, min(days, 35)) - 1)).isoformat()
    where = "dt BETWEEN '%s' AND '%s'" % (start, end)
    if m.get("filter"):
        where += " AND (%s)" % m["filter"]
    if kind == "trend":
        return ("SELECT dt AS label, %s AS value FROM %s WHERE %s "
                "GROUP BY dt ORDER BY dt" % (formula, src, where))
    if not dim or str(dim).lower() not in cols:
        raise HTTPException(400, "维度 %s 不在表 %s 中" % (dim, src))
    return ("SELECT %s AS label, %s AS value FROM %s WHERE %s "
            "GROUP BY %s ORDER BY value DESC LIMIT 20"
            % (dim, formula, src, where, dim))


@app.post("/api/chart")
def api_chart(body: ChartIn, prof: dict = Depends(current_user)):
    cfg = load_config()
    if body.table:
        # 预聚合表模式：数值列直接 SUM。表与列都必须在语义层登记过，
        # 否则这里就成了"任意列任意表"的裸查询口子。
        cols = [c.lower() for c in semantic.table_columns(body.table, cfg)]
        if not cols:
            raise HTTPException(400, "未知数据表：%s" % body.table)
        vc = (body.value_col or "").strip().lower()
        if not vc or vc not in cols:
            raise HTTPException(400, "数值列 %s 不在表 %s 中" % (body.value_col, body.table))
        tinfo = semantic.get_table(body.table, cfg) or {}
        m = {"code": body.table, "name_cn": tinfo.get("name_cn") or body.table,
             "source": body.table, "formula": "SUM(%s)" % vc, "filter": None, "unit": ""}
    else:
        m = semantic.get_metric(body.metric.strip(), cfg)
        if not m or not (m.get("formula") or "").strip():
            raise HTTPException(400, "未知指标：%s" % body.metric)
        m = dict(m, code=body.metric.strip())
    kind = "trend" if body.kind == "trend" else "breakdown"
    if kind == "breakdown" and not body.dim:
        raise HTTPException(400, "维度分布必须给 dim")
    try:
        sql = _chart_sql(m, kind, (body.dim or "").strip() or None, body.days, cfg)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(400, "生成图表 SQL 失败：%s" % str(e)[:160])

    ctx = _ctx_for(prof)
    dec = policy_engine(cfg).decide(ctx)
    applied = policy_engine(cfg).apply(sql, dec)
    if applied.get("error"):
        raise HTTPException(403, applied["error"])
    r = db.run_sql(applied["sql"], cfg, trace_id=ctx.trace_id)
    if r.get("error"):
        return JSONResponse({"error": str(r["error"]), "name_cn": m.get("name_cn")})
    return {
        "metric": m["code"], "name_cn": m.get("name_cn") or m["code"],
        "unit": m.get("unit") or "", "kind": kind, "dim": body.dim,
        "days": body.days, "columns": r["columns"], "rows": r["rows"],
        "row_count": r["row_count"], "elapsed_ms": r["elapsed_ms"],
        "policy_injected": applied.get("injected") or [],
    }


# ---------------------------------------------------------------- ETL 开发
# agent_ro 只读是评审定下的边界，所以 ETL 是"生成 + 校验 + 登记"：
# 生成物必须是 CREATE TABLE music_dw.dev_* AS SELECT，etlguard 逐项校验
# 并跑一次 EXPLAIN 实证，拿到写权限的账号后可以原样执行。
_ETL_SYSTEM = """你是数据开发工程师的 ETL 设计与生成助手，为 Doris（Doris 2.x，Hive 外表 catalog）设计并产出一条建表加工 SQL。

硬性规则：
1. 只输出一条 CREATE TABLE 语句，格式：CREATE TABLE music_dw.dev_<名字> AS SELECT ...（CTAS）
2. 目标表名必须以 dev_ 开头、全小写下划线，体现内容（如 dev_dau_daily）
3. 源表只能用【可用表】里列出的表；它们是只读外表，不要试图修改
4. 时间分区列 dt，务必用 dt BETWEEN '<起>' AND '<止>' 裁剪分区，离线数据截止 {data_until}
5. SQL 里不要出现 DROP/DELETE/INSERT 等其他语句（daily_sql 中的 INSERT OVERWRITE 除外）
6. FROM/JOIN 的表一律用两段式 music_dw.<表名>，禁止 hive_catalog. 三段式前缀
   （生成物在 Hive 引擎执行，不认识 Doris 的 catalog 语法；校验时也不允许出现）

口径铁律（历史上真实踩过坑，设计时必须逐条对照）：
- 收入/消费/销售额类金额 = vip_subscribe 与 order_pay 计正、order_refund 计负。
  只取 order_pay 会漏掉 VIP 订阅（约占毛收入 82%），历史上被低估 5.24 倍
- 金额列保持 DECIMAL，不要转 DOUBLE 求和
- 比率不能直接 AVG，必须 分子求和/分母求和 重算
- 若【可用表】的 caveats 里有与本题相关的陷阱，必须在口径设计步骤中显式回应

先推理、后写 SQL：steps 数组按顺序呈现设计过程，每步 detail 用中文说清"为什么这么定"：
1. 需求理解：指标、维度、粒度、过滤；Top N 说明进表方式（物化 Top N，还是存明细粒度+查询时排序）与取舍
2. 源表选择：选了哪张（些）、为什么是它、为什么不用别的表
3. 口径设计：每个金额/计数字段的确切算法，对照口径铁律逐条自检
4. 表设计：表名、粒度、主键是否唯一、预期行数量级
5. 调度设计：首建用 sql 全量落历史；之后每日重刷（说明为什么选「每日 INSERT OVERWRITE
   全量重刷」而不是增量追加——重跑幂等、dev 表量级小重刷代价可忽略；说明上游分区
   就绪依赖与建议调度时间，如每日 01:30 依赖 dwd_pay T-1 分区）

只输出 JSON：{{"title": "中文短标题", "target_table": "music_dw.dev_xxx",
 "steps": [{{"title": "需求理解", "detail": "…"}}, {{"title": "源表选择", "detail": "…"}}, {{"title": "口径设计", "detail": "…"}}, {{"title": "表设计", "detail": "…"}}, {{"title": "调度设计", "detail": "…"}}],
 "sql": "CREATE TABLE ...",
 "daily_sql": "INSERT OVERWRITE TABLE music_dw.dev_xxx SELECT ...（与 sql 的 SELECT 口径逐字一致，仅把时间上界改为 dt <= '${{hivevar:bizdate}}'，分区裁剪保留原样）"}}"""

# 建表/加工意图：命中即把请求从问数链路分流到 ETL 开发（engineer 专属）。
# 刻意保守（宁漏勿错）：漏了顶多像以前一样被问数硬答，错了会拦住正常提问。
# "做一张报表"不会命中（"做"后跟的是"报"不是"表"），"加一个 ads 表"会命中。
_CREATE_INTENT_RE = re.compile(
    r"(建表|建一张|建个|建个新|创建.{0,6}表|加一张?.{0,6}表|加个.{0,6}表|"
    r"新增.{0,6}表|新表|开发.{0,4}表|生成.{0,4}建表|create\s+table)", re.I)


def _gen_schedule_script(title: str, target: str, daily_sql: str) -> str:
    """每日调度脚本：系统确定性生成（不走 LLM），DolphinScheduler SHELL 任务可直接粘贴。
    SQL 用不带引号的 heredoc 展开，$BIZDATE 由 shell 按天替换。"""
    sql_embedded = daily_sql.strip().rstrip(";").replace("${hivevar:bizdate}", "$BIZDATE")
    sql_embedded = sql_embedded.replace('"', '\\"')
    return (
        "#!/usr/bin/env bash\n"
        f"# {title} —— 每日调度脚本（系统生成）\n"
        f"# 目标表：{target}\n"
        "# 幂等性：INSERT OVERWRITE 全量重刷，同一天重复执行结果一致，失败重试安全\n"
        "# 接入 DolphinScheduler：新建 SHELL 任务粘贴本脚本；\n"
        "#   定时：每日 01:30（Cron: 0 30 1 * * ? *）\n"
        "#   上游依赖：dwd_pay 的 T-1 分区就绪（用工作流依赖声明，不要靠时间猜）\n"
        "#   失败策略：自动重试 2 次，仍失败则告警值班\n"
        "#   行数快照：调度只重刷数据，不改仓库文件；跑一阵后在 ETL 面板点\n"
        "#     「校准行数」把 rowcount.tsv / tables.yaml 的 rows: 对齐实测\n"
        "set -euo pipefail\n"
        'BIZDATE="${1:-$(date -d \'1 day ago\' +%F)}"   # T-1，也可手动传参指定重刷哪天\n'
        f'echo "[$(date \'+%F %T\')] {target} 开始重刷 bizdate=$BIZDATE"\n'
        '/opt/module/hive/bin/hive -S -e "\n'
        f"{sql_embedded}\n"
        '"\n'
        f'echo "[$(date \'+%F %T\')] {target} 重刷完成 bizdate=$BIZDATE"\n'
    )


@app.post("/api/etl")
def api_etl_generate(body: EtlIn, prof: dict = Depends(current_user)):
    if prof.get("role") != "data_engineer":
        raise HTTPException(403, "ETL 开发仅对数据开发工程师开放")
    req = (body.requirement or "").strip()
    if len(req) < 6:
        raise HTTPException(400, "需求描述太短，说清楚要加工什么")
    if len(req) > 2000:
        raise HTTPException(400, "需求描述过长（>2000 字）")
    cfg = load_config()
    ctx = _ctx_for(prof)
    data_until = cfg["lane"]["offline"]["data_until"]

    # 只给召回到的表结构：全量十来张表的 DDL 塞进 prompt 既贵又让模型分心
    hits = semantic.find_tables(req, cfg, top=3)
    parts = []
    recalled = set()
    for h in hits:
        t = h.get("table") if isinstance(h, dict) else None
        if t:
            parts.append(semantic.describe_table(t, cfg))
            recalled.add(t)
    # 指标联动召回：金额/消费类需求字面上常不含事实表名，find_tables 会漏掉
    # dwd_pay 这类事实表（2026-11-11 实测：模型拿不到 dwd_pay，把 event_name 的
    # 取值脑补成三张表，EXPLAIN 抓出 Unknown table）。命中的指标的 source 表
    # 强制并入【可用表】，同时附上指标口径作为生成时的权威参考。
    metric_notes = []
    for m in semantic.find_metrics(req, cfg, top=2):
        code = m.get("code") if isinstance(m, dict) else None
        info = semantic.get_metric(code, cfg) if code else None
        if not info:
            continue
        src = info.get("source")
        if src and src not in recalled and semantic.get_table(src, cfg):
            parts.append(semantic.describe_table(src, cfg))
            recalled.add(src)
        if info.get("formula"):
            metric_notes.append("%s（%s）= %s，来源表 %s"
                                % (code, info.get("name_cn"),
                                   info.get("formula"), src))
    if not parts:
        parts.append("（没有召回到明确表，优先使用 ads_daily_overview / dws_user_daily）")
    schema_txt = "\n\n".join(parts)
    if metric_notes:
        schema_txt += ("\n\n【相关指标口径（权威定义，金额字段必须与此一致）】\n"
                       + "\n".join(metric_notes))

    messages = [
        {"role": "system",
         "content": _ETL_SYSTEM.format(data_until=data_until)},
        {"role": "user",
         "content": "【需求】\n%s\n\n【可用表】\n%s" % (req, schema_txt)},
    ]
    try:
        out = gateway.chat(messages, cfg, caller="etl", json_mode=True,
                           max_tokens=2400, ctx=ctx)
    except Exception as e:
        raise HTTPException(502, "模型调用失败：%s" % str(e)[:180])
    import llm as _llm
    try:
        obj = _llm.parse_json_object(out.get("content") or "")
    except Exception:
        raise HTTPException(502, "模型输出无法解析：%s"
                            % str(out.get("content"))[:180])
    sql = (obj.get("sql") or "").strip()
    if not sql:
        raise HTTPException(502, "模型没有返回 sql 字段")
    checks = etlguard.validate(sql, cfg)
    title = str(obj.get("title") or req[:40])
    target = str(obj.get("target_table") or checks.get("target") or "")

    # 分步推理：模型的设计推理 + 系统的实证检查，合成一条可读的步骤链。
    # 模型 steps 是"为什么这么设计"，系统 steps 是"验证过什么"——后者不可省，
    # 它是唯一不会说谎的部分（EXPLAIN 是真跑出来的）。
    steps = []
    for s in (obj.get("steps") or [])[:6]:
        if isinstance(s, dict) and s.get("title"):
            steps.append({"title": str(s["title"])[:60],
                          "detail": str(s.get("detail") or "")[:800]})
    if not steps:
        steps.append({"title": "SQL 生成",
                      "detail": "模型未返回分步推理，直接产出 SQL（可对照下方口径铁律复核）。"})
    if checks.get("explain_ok"):
        steps.append({"title": "静态校验 + EXPLAIN 实证（系统）",
                      "detail": "四项静态检查全过；EXPLAIN 真实执行通过，语句可跑。"})
    else:
        steps.append({"title": "静态校验 + EXPLAIN 实证（系统）",
                      "detail": "未通过：%s" % (checks.get("explain_error") or "见下方检查项")})
    # 口径机检：能跑 ≠ 算得对。这一步只标黄、不拦执行——口径偏离有时是有意的，
    # 但"有意"必须是人看过之后的有意，而不是压根没发现。
    cal = checks.get("caliber") or []
    if not checks.get("caliber_ok", True):
        steps.append({"title": "口径机检（系统）",
                      "detail": "机检本身执行失败，金额与去重口径未验证，请人工复核后再执行。"})
    elif cal:
        steps.append({"title": "口径机检（系统）",
                      "detail": "发现 %d 项口径风险：%s。机器只标黄、不拦执行——"
                                "确实要这个口径就照做，否则按下方提示改。"
                                % (len(cal), "；".join(str(f.get("message") or "")
                                                      for f in cal[:2]))})
    else:
        steps.append({"title": "口径机检（系统）",
                      "detail": "按指标定义核对了事件覆盖、退款符号与去重源表，未发现口径风险。"})
    steps.append({"title": "人工复核提醒（系统）",
                  "detail": "以上校验证明「能跑」，口径机检只覆盖已登记的坑（收入事件覆盖、"
                            "退款符号、去重源表）。口径的另一半仍要人看：这段逻辑是不是"
                            "业务真正想要的那个数。"})

    # 每日调度：模型产出 INSERT OVERWRITE 重刷 SQL，系统校验并生成调度脚本。
    # 脚本由代码确定性生成（不经过模型）——调度入口里的每一个字都必须可审计。
    daily_sql = (obj.get("daily_sql") or "").strip()
    daily_checks = daily_summary = schedule_script = None
    if daily_sql:
        daily_checks = etlguard.validate_daily(daily_sql, cfg)
        daily_summary = etlguard.summarize_daily(daily_checks)
        ok_all = all(daily_checks.get(k) for k in
                     ("stmt_ok", "target_ok", "tables_ok", "danger_ok",
                      "overwrite_ok", "param_ok", "explain_ok"))
        steps.append({"title": "每日重刷 SQL 校验（系统）",
                      "detail": "全部通过：OVERWRITE 幂等、bizdate 参数化、EXPLAIN 实证可执行。"
                                "首建表后把调度脚本挂上即可按天重刷。"
                      if ok_all else
                      "未全部通过：" + "；".join(daily_summary)})
        if ok_all:
            schedule_script = _gen_schedule_script(title, target, daily_sql)
            steps.append({"title": "调度脚本生成（系统）",
                          "detail": "已生成 T-1 每日重刷脚本（下方可复制），"
                                    "含 DolphinScheduler 接入参数：定时 01:30、"
                                    "上游依赖 dwd_pay T-1 分区、失败重试 2 次。"})

    # 登记时把调度产物一起存进 checks 载荷：历史列表里能翻出每个生成物的完整交付物
    if daily_sql:
        checks = {**checks, "daily_sql": daily_sql,
                  "daily_checks": daily_checks, "schedule_script": schedule_script}

    sid, err = store.save_etl_script(prof["user_id"], title, target,
                                     req, sql, checks)
    return {"id": sid, "title": title, "target_table": target,
            "sql": sql, "checks": checks, "steps": steps,
            "summary": etlguard.summarize(checks),
            "daily_sql": daily_sql,
            "daily_checks": daily_checks, "daily_summary": daily_summary,
            "schedule_script": schedule_script,
            "conv_error": err,
            "cost_cny": (out.get("cost_cny") or 0)}


class EtlIdIn(BaseModel):
    id: int


class EtlExecIn(BaseModel):
    id: int
    confirm_token: str = ""      # 金额类的二次确认令牌（etl_gate.token）


@app.post("/api/etl/execute")
def api_etl_execute(body: EtlExecIn, prof: dict = Depends(current_user)):
    """真正执行：SSH 到白名单机器跑 Hive CTAS，执行后刷元数据 + 回读验证。

    边界：仅 engineer；SQL 从库里取并**重新过一遍 etlguard**（不信任入库时的
    结论——旧版本护栏放行过的东西不能因为"当时验过"就一直免检）。

    金额类另加一道闸：第一次点执行不跑，先把风险摊开等人确认（etl_gate）。
    令牌绑"记录号 + SQL 指纹"，换 SQL 即失效；不确认就永远不执行——这笔账
    只能用人的判断来结，机器只负责不让他"手滑"。
    """
    if prof.get("role") != "data_engineer":
        raise HTTPException(403, "ETL 执行仅对数据开发工程师开放")
    cfg = load_config()
    rec = store.get_etl_script(body.id, prof["user_id"], cfg=cfg)
    if not rec:
        raise HTTPException(404, "找不到这条生成物（可能不属于当前账号）")
    sql = rec.get("sql_text") or ""
    stored = rec.get("checks") or {}
    if isinstance(stored, str):
        try:
            stored = json.loads(stored)
        except ValueError:
            stored = {}
    gt = etl_gate.gate(body.id, sql, stored)
    if gt["need_confirm"] and not etl_gate.verify(body.id, sql, body.confirm_token):
        return {"ok": False, "stage": "confirm_required", "need_confirm": True,
                "risks": gt["risks"], "token": gt["token"],
                "error": "这条生成物产出金额类指标，需确认口径后再执行"}
    res = etl_exec.execute_ctas(sql, cfg)
    # 行数回写：dev 表每次重建行数都会变，rowcount.tsv 不跟着动就会让 sem_check
    # 天天报"行数不一致"。执行成功后顺手刷新这一张，失败不影响执行结论。
    rc = {}
    if res.get("ok") and res.get("rows") is not None:
        try:
            rc = etl_rowcount.upsert({str(res.get("target") or "").split(".")[-1]:
                                      int(res.get("rows") or 0)})
        except Exception as e:
            rc = {"ok": False, "error": str(e)[:160]}
    info = {"at": _dt.datetime.now().isoformat(timespec="seconds"),
            "ok": bool(res.get("ok")), "target": res.get("target"),
            "rows": res.get("rows"), "seconds": res.get("seconds"),
            "refreshed": res.get("refreshed"),
            "confirmed": bool(gt["need_confirm"]),
            "rowcount_synced": bool(rc.get("ok")),
            "error": (res.get("error") or "")[:300]}
    store.record_etl_exec(body.id, info, cfg=cfg)
    res["status"] = "deployed" if res.get("ok") else "execute_failed"
    res["confirmed"] = bool(gt["need_confirm"])
    res["rowcount"] = {"synced": bool(rc.get("ok")),
                       "error": rc.get("error") or ""}
    return res


@app.post("/api/etl/rowcount/refresh")
def api_etl_rowcount_refresh(prof: dict = Depends(current_user)):
    """把全部 dev 表的实测行数写回 rowcount.tsv + tables.yaml 的 rows:。

    为什么不做成定时任务：仓库在本机、调度在集群，没有回调通道；这里给的是
    "调度跑了一阵之后，点一下对齐"的确定性动作，跑完 sem_check 会立刻告诉你
    还有没有漂移。
    """
    if prof.get("role") != "data_engineer":
        raise HTTPException(403, "行数校准仅对数据开发工程师开放")
    cfg = load_config()
    return etl_rowcount.refresh(cfg)


@app.post("/api/dq/run")
def api_dq_run(prof: dict = Depends(current_user)):
    """跑一遍统一质量体检并把结果落库（确定性检查，不调 LLM、不造数据）。"""
    if prof.get("role") != "data_engineer":
        raise HTTPException(403, "质量体检仅对数据开发工程师开放（查看报告不限）")
    return dq.run(load_config())


@app.get("/api/dq/report")
def api_dq_report(prof: dict = Depends(current_user)):
    """最新一次体检 + 最近 7 次趋势；质量状态不属于敏感数据，登录即可看。"""
    return dq.report(load_config())


@app.get("/api/ops/metrics")
def api_ops_metrics(days: int = 7, prof: dict = Depends(current_user)):
    """运营指标：从不可变审计日志只读聚合，不落第二份事实表。

    返回的是脱敏聚合值（无个人问题明细），登录即可看；逐条审计仍按
    /api/audit 的角色范围控制。
    """
    return opsmetrics.collect(load_config(), days)


@app.get("/api/etl/register/draft")
def api_etl_register_draft(id: int, prof: dict = Depends(current_user)):
    """登记草案：列结构/行数实测，冲突就地机检，工程师确认后才写入。"""
    if prof.get("role") != "data_engineer":
        raise HTTPException(403, "语义层登记仅对数据开发工程师开放")
    cfg = load_config()
    rec = store.get_etl_script(id, prof["user_id"], cfg=cfg)
    if not rec:
        raise HTTPException(404, "找不到这条生成物")
    d = etl_register.draft(rec, cfg)
    if d.get("error"):
        raise HTTPException(409, d["error"])
    return d


@app.post("/api/etl/register/confirm")
def api_etl_register_confirm(body: EtlIdIn, prof: dict = Depends(current_user)):
    """确认入库：写 DDL 文件 / tables.yaml / rowcount.tsv，再跑 gen_tables + sem_check。"""
    if prof.get("role") != "data_engineer":
        raise HTTPException(403, "语义层登记仅对数据开发工程师开放")
    cfg = load_config()
    rec = store.get_etl_script(body.id, prof["user_id"], cfg=cfg)
    if not rec:
        raise HTTPException(404, "找不到这条生成物")
    return etl_register.commit(rec, cfg)


@app.get("/api/etl")
def api_etl_list(prof: dict = Depends(current_user)):
    rows = store.list_etl_scripts(prof["user_id"]) or []
    for r in rows:
        if isinstance(r.get("checks"), str):
            try:
                r["checks"] = json.loads(r["checks"])
            except ValueError:
                r["checks"] = {}
    return {"scripts": rows}


@app.get("/")
def index():
    if not INDEX.exists():
        raise HTTPException(500, "缺少 web/index.html")
    return FileResponse(INDEX, media_type="text/html; charset=utf-8")


# ---------------------------------------------------------------- 评测历史
# 为什么要从 web 里也能看：评测报告是一次性的（report.md 每次被覆盖），
# "这一版比上一版好在哪"必须有个地方能翻。放 /eval-history 而不是再起一个服务，
# 是因为演示时多一个端口就多一个忘了启动的环节。
HISTORY_HTML = HERE.parent.parent / "eval" / "history.html"


@app.get("/eval-history")
def eval_history_page():
    sys.path.insert(0, str(HERE.parent.parent / "eval"))
    import history as H
    try:
        H.render_file(HISTORY_HTML)      # 每次访问重新渲染：读的是 jsonl 的最新状态
    except Exception as e:
        raise HTTPException(500, "生成评测看板失败：%s" % e)
    return FileResponse(str(HISTORY_HTML), media_type="text/html; charset=utf-8")


@app.get("/api/eval-history")
def eval_history_data():
    sys.path.insert(0, str(HERE.parent.parent / "eval"))
    import history as H
    recs = H.load()
    return {"total": len(recs),
            "full": sum(1 for r in recs if not r.get("partial")),
            "records": recs[-50:]}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=8000)
    a = ap.parse_args()

    # AC-21：启动前一次性校验密钥。
    # 缺 key 就现在退出，别等用户问第一句话才炸——"服务起来了但用不了"
    # 这种状态比启动失败难查得多，而且用户会以为是数据问题。
    _cfg0 = load_config()
    try:
        vault.warmup(_cfg0)              # 先登记，缺失项才会出现在下面的报错里
        vault.assert_ready(_cfg0)
    except vault.SecretMissing as e:
        print("启动中止：%s" % e)
        return 2
    print("密钥：%s" % vault.summary_line())
    for _w in vault.weak():
        print("  ⚠ %s 只有 %d 位（本地服务可接受，对外服务不可）"
              % (_w["name"], _w["length"]))

    import uvicorn
    # 预热护栏进程池：把 sqlglot 的 import 成本在启动时付掉，
    # 用户第一次问数不用等三个进程各自启动一遍。
    try:
        n = guardtool.warm(load_config())
        print("护栏常驻进程：%d 个（serve 模式）" % n)
    except Exception as e:                     # 预热失败不影响服务，会按需降级
        print("护栏预热失败（将按需启动）：%s" % e)

    # 项目自带 Redis：配置指向本机，但机器上不一定正在跑。
    # 不自动拉起的话，最坏的情况是「静默降级回进程内缓存」——
    # 功能看起来完全正常，跨实例共享其实没有生效。宁可启动时多花 0.5 秒。
    # 只对本机地址生效：远程 Redis 归运维管，本地脚本没资格替它做决定。
    try:
        _c = load_config().get("cache") or {}
        _r = _c.get("redis") or {}
        _host = str(_r.get("host") or "127.0.0.1")
        if (str(_c.get("backend") or "").lower() == "redis"
                and _r.get("autostart", True)
                and _host.split(":")[0] in ("127.0.0.1", "localhost", "::1")):
            import run_redis
            ok, msg = run_redis.ensure_running(load_config())
            print("Redis：%s%s" % ("" if ok else "⚠ ", msg))
    except Exception as e:                     # 起不来就让它降级，不阻断服务
        print("Redis 自启失败（缓存会按后端降级）：%s" % e)

    # 缓存后端在启动时就说清楚是 redis 还是降级的 local。
    # 静默降级是最坏的结果：三个实例各存一份，演示时完全看不出来，
    # 生产上就是缓存穿透。启动日志是"能被看见"的最后一道机会。
    try:
        s = answercache.get_cache(load_config()).stats()
        if s.get("degraded") or s.get("init_warning"):
            print("缓存后端：%s  ⚠ 已降级（%s）" % (
                s.get("backend"),
                (s.get("reason") or s.get("init_warning") or "")[:120]))
        else:
            print("缓存后端：%s ｜ TTL %ss" % (s.get("backend"), s.get("ttl_sec")))
    except Exception as e:
        print("缓存后端初始化失败（首次问数时重试）：%s" % e)

    # 治理库表结构：干净机器上库是空的，不建表就等于"启动成功、一查全错"。
    # schema.sql 全是 IF NOT EXISTS / ADD COLUMN IF NOT EXISTS，重复执行安全。
    try:
        ok, err = store.init_schema(load_config())
        print("治理库：%s" % ("表结构就绪" if ok else "⚠ 初始化失败：%s" % err[:120]))
    except Exception as e:
        print("治理库初始化异常（问答仍可进行，审计/台账会缺失）：%s" % e)

    # 出网闸：启动即装，别等第一次问数才装——中间那段时间是敞开的。
    try:
        st = egress.install(load_config())
        print("出网闸：%s ｜ 允许 %s%s" % (
            "已启用" if st["installed"] else "未启用（%s）" % (st["reason"] or "?"),
            ",".join(st["allow_hosts"]) or "无",
            " ｜ 经代理 %s" % ",".join(p["host"] + ":" + str(p["port"])
                                     for p in st["proxies"]) if st["proxies"] else ""))
    except Exception as e:
        print("出网闸安装失败（模型调用会直连出去）：%s" % e)

    print("Data Agent 前端：http://%s:%d" % (a.host, a.port))
    print("执行面：hive_catalog.music_dw")
    print("演示账号（密码均为 Demo@2026）：")
    print("  analyst       数据分析师 · 全国       可查明细+汇总，ODS 原始层不可见，PII 脱敏")
    print("  analyst_east  数据分析师 · 华东 7 省市 同岗位不同范围，用于验证数据范围与岗位正交")
    print("  engineer      数据开发工程师 · 全国    全层可查（含 ODS 原始事件）")
    print("  pm            数据产品经理 · 全国      仅汇总层，个体标识整列禁用")
    print("Ctrl+C 退出")
    uvicorn.run(app, host=a.host, port=a.port, log_level="warning")
    return 0


if __name__ == "__main__":
    sys.exit(main())
