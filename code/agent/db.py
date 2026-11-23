"""执行面：全项目唯一允许连 Doris 的地方。

拦得住"LLM 拿连接乱写"这类问题的方式不是 prompt，是结构——
别的模块没有 pymysql 的 import，想查数只能调这里的 run_sql()。

连接建立后立刻执行 config.execution.init_sql（SWITCH hive_catalog + USE music_dw），
之后这条会话上的所有 SQL 都用裸表名。绕过它写的 internal.xxx 由护栏 R17 拦。

阶段 1：改为进程内连接池（架构债 #3）。原实现每次问答都新建 TCP 连接、
重放 SWITCH/USE，10 路并发时连接风暴 + Doris FE 压力明显。
池按 cfg 缓存，复用前 ping 探活；`trace_id` 随调用透传到结果与异常信息，
用于把一次问答和数据库侧日志对上（不拼进 SQL，避免污染护栏解析）。
"""
from __future__ import annotations

import _bootstrap  # noqa: F401

import contextvars
import datetime as _dt
import decimal
import os
import queue
import threading
import time
from contextlib import contextmanager
from typing import Any, Iterator

import pymysql

from conf import load_config
import vault


# 只重试"连接层"的瞬时错误。SQL 写错、权限不足、表不存在都不在此列——
# 那类是确定性的，重试只会把同一个错拖长三倍。
_TRANSIENT = ("1203", "Reach limit of connections", "Lost connection",
              "Connection reset", "Broken pipe", "2013")


def _normalize(v: Any) -> Any:
    """pymysql 返回的 Decimal / date / bytes 变成 JSON 能吃的类型。"""
    if isinstance(v, decimal.Decimal):
        return int(v) if v == v.to_integral_value() else float(v)
    if isinstance(v, (_dt.date, _dt.datetime)):
        return v.isoformat()
    if isinstance(v, _dt.timedelta):
        return v.total_seconds()
    if isinstance(v, bytes):
        return v.decode("utf-8", "replace")
    if isinstance(v, (list, tuple)):
        return [_normalize(x) for x in v]
    return v


class _Pool:
    """极简 LIFO 连接池。不用第三方池：连接数少、语义明确、故障可预期。"""

    def __init__(self, cfg: dict, size: int = 6):
        self.cfg = cfg
        self.q: queue.LifoQueue = queue.LifoQueue(maxsize=size)
        self._lock = threading.Lock()
        self._made = 0

    def _new(self):
        ex = self.cfg["execution"]
        conn = pymysql.connect(
            host=ex["host"], port=int(ex["port"]), user=ex["user"],
            password=vault.require(ex["password_env"]).reveal(), charset=ex["charset"],
            connect_timeout=int(ex.get("connect_timeout", 10)),
        )
        with conn.cursor() as cur:
            for stmt in ex["init_sql"]:
                cur.execute(stmt)
            try:
                cur.execute("SET query_timeout = %d"
                            % int(self.cfg["limits"]["timeout_sec"]))
            except Exception:
                pass
        with self._lock:
            self._made += 1
        return conn

    def acquire(self):
        try:
            conn = self.q.get_nowait()
        except queue.Empty:
            return self._new()
        # 绝不用 reconnect=True：重连会丢掉 SWITCH/USE 的会话状态，
        # 同名表会静默落到 internal 实时库（差 6.2 倍且零报错）——
        # R17 存在的全部意义就是防这个。探活失败就整条重建并重放 init_sql。
        try:
            conn.ping(reconnect=False)
        except Exception:
            try:
                conn.close()
            except Exception:
                pass
            return self._new()
        return conn

    def release(self, conn):
        try:
            self.q.put_nowait(conn)
        except queue.Full:
            try:
                conn.close()
            except Exception:
                pass


_POOLS: dict[str, _Pool] = {}
_POOL_LOCK = threading.Lock()

# 2026-11-14：把「借连接花了多久」暴露到 run_sql 的返回里。
# 注意语义不是排队：_Pool.acquire 在队列空时**直接新建连接**（重放 init_sql），
# 不阻塞等待；所以 8 路并发打 3 大小的池时，这个数字量的是"超额建连"的代价，
# 而多余连接在 release 时又被丢弃。以前它藏在 P95 里，看不出是 SQL 慢还是借连接慢。
_POOL_ACQUIRE_MS = contextvars.ContextVar("pool_acquire_ms", default=0.0)


def _pool_key(cfg: dict) -> str:
    """按**执行面身份**共享池，而不是按 `id(cfg)`。

    踩过的坑：`eval/run.py` 每题 `copy.deepcopy(cfg)` 换生成模式，
    用 id 做 key 就变成每题一个新池、连接永不归还——跑到第 24 题左右
    FE 开始回 `1203 Reach limit of connections`，评测里表现为"后半段
    集体假红"。同库同账号就该共用一池，配置对象怎么深拷贝都无所谓。
    """
    ex = cfg["execution"]
    return "%s|%s|%s:%s|%s" % (
        ex.get("catalog"), ex.get("database"),
        ex["host"], ex["port"], ex["user"])


def pool(cfg: dict | None = None) -> _Pool:
    cfg = cfg or load_config()
    key = _pool_key(cfg)
    with _POOL_LOCK:
        if key not in _POOLS:
            _POOLS[key] = _Pool(cfg, size=int(cfg["limits"].get("pool_size", 3)))
        return _POOLS[key]


@contextmanager
def session(cfg: dict | None = None) -> Iterator[pymysql.connections.Connection]:
    """一个已经切好 catalog / database 的会话（从池里借，用完归还）。"""
    p = pool(cfg)
    t_acquire = time.perf_counter()
    conn = p.acquire()
    _POOL_ACQUIRE_MS.set(_POOL_ACQUIRE_MS.get()
                         + (time.perf_counter() - t_acquire) * 1000)
    try:
        yield conn
    except Exception:
        # 出过 SQL 错的连接状态可能不干净，丢掉而不是放回池里
        try:
            conn.close()
        except Exception:
            pass
        raise
    else:
        p.release(conn)


def run_sql(sql: str, cfg: dict | None = None, max_rows: int | None = None,
            trace_id: str | None = None) -> dict:
    """执行一条查询，返回 {columns, rows, row_count, elapsed_ms, truncated} 或 {error}。"""
    cfg = cfg or load_config()
    # `or` 会把 max_rows=0 当成没传——而 0 恰好是"策略已拒绝"的哨兵值，
    # 回退到全局上限等于把一次本该空跑的查询放开。用 is None 判。
    limit = int(cfg["limits"]["max_rows"] if max_rows is None else max_rows)
    t0 = time.perf_counter()
    _POOL_ACQUIRE_MS.set(0.0)       # 重试会多次借连接，这里按本次 run_sql 重新累计
    out = {"columns": [], "rows": [], "row_count": 0,
           "elapsed_ms": 0, "truncated": False, "pool_acquire_ms": 0.0}
    if trace_id:
        out["trace_id"] = trace_id

    # 连接类瞬时故障重试：并发评测（多条链路同时打 FE）会撞上
    # `1203 Reach limit of connections`，这类错误**与 SQL 对错无关**，
    # 直接判失败会把"容量抖动"记成"答错了"——评测里出现过 9 题连环假红。
    # 只对只读语句重试，且最多 2 次；语法错 / 权限错不在此列，照旧直接返回。
    attempts = 3
    for i in range(attempts):
        try:
            with session(cfg) as conn:
                with conn.cursor() as cur:
                    cur.execute(sql)
                    cols = [d[0] for d in cur.description] if cur.description else []
                    rows = list(cur.fetchmany(limit + 1)) if cols else []
                    truncated = len(rows) > limit
                    rows = rows[:limit]
                    out.update({
                        "columns": cols,
                        "rows": [_normalize(list(r)) for r in rows],
                        "row_count": len(rows),
                        "truncated": truncated,
                    })
            out.pop("error", None)
            out.pop("retries", None)
            break
        except Exception as e:  # 连接错、语法错、权限错都收进 error，不炸整个图
            # pymysql 的连接异常有时会把整段连接参数（含 password）带进消息，
            # 而 error 最终会进 trace / 审计表 / 前端页面 —— 一律先擦再存。
            msg = vault.scrub("%s: %s" % (type(e).__name__,
                                         str(e).split("\n")[0][:500]))
            transient = any(k in msg for k in _TRANSIENT)
            if transient and i < attempts - 1:
                out["retries"] = i + 1
                time.sleep(0.4 * (2 ** i))
                continue
            out["error"] = msg
            break
    out["elapsed_ms"] = int((time.perf_counter() - t0) * 1000)
    out["pool_acquire_ms"] = round(_POOL_ACQUIRE_MS.get(), 1)
    return out
