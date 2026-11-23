"""治理面持久化：PostgreSQL 唯一实现（ADR-003，不搞 SQLite 双写）。

设计取舍：
  · 每次写入新建连接。审计是低频单行 INSERT，连接池带来的收益低于
    多一层状态管理的风险；阶段 4 网关记账变高频时再引入 psycopg_pool。
  · `qa_logs` 只 INSERT，数据库层还有不可变触发器（见 schema.sql）。
  · 写失败**不静默**：返回 (False, 原因)，调用方必须把它标进 state，
    否则「审计显示成功但没落盘」比没有审计更危险。
"""
from __future__ import annotations

import json
import os
from pathlib import Path

import psycopg
from psycopg.types.json import Jsonb
from uuid import uuid4

from .contracts import AuditEvent

HERE = Path(__file__).resolve().parent
SCHEMA = HERE / "schema.sql"
DEFAULT_DSN = "postgresql://postgres@127.0.0.1:55432/agent_gov"


def dsn(cfg: dict | None = None) -> str:
    """配置优先，环境变量兜底；DSN 里不含密码（本地 trust 认证）。"""
    if cfg:
        g = cfg.get("governance") or {}
        env = g.get("dsn_env")
        if env and os.environ.get(env):
            return os.environ[env]
        if g.get("dsn"):
            return g["dsn"]
    return os.environ.get("GOV_PG_DSN", DEFAULT_DSN)


def connect(cfg: dict | None = None):
    return psycopg.connect(dsn(cfg), connect_timeout=5)


def _err(e: BaseException, limit: int = 300) -> str:
    """统一的错误文本：类型名 + 截断 + **擦掉 DSN 里的密码**。

    psycopg 的连接失败有时会把整段 conninfo 带进消息里，而这条消息最终会
    进审计表、`/api/audit` 页面、日志。治理库是独立包，看不到 agent 层的
    vault，所以这里只擦"它自己知道的那一个"：DSN 里的密码。
    """
    msg = "%s: %s" % (type(e).__name__, str(e)[:limit])
    try:
        from urllib.parse import urlparse
        pw = urlparse(dsn()).password
        if pw:
            msg = msg.replace(pw, "***")
    except Exception:
        pass
    return msg


def init_schema(cfg: dict | None = None) -> tuple[bool, str]:
    """执行 schema.sql。按分号切分即可——文件里没有函数体之外含分号的字符串。"""
    try:
        sql = SCHEMA.read_text(encoding="utf-8")
        with connect(cfg) as conn:
            with conn.cursor() as cur:
                # CREATE FUNCTION ... $$ ... $$; 里有分号，必须整段执行而不是切分
                cur.execute(sql)
        return True, ""
    except Exception as e:
        return False, _err(e, 300)


def insert_audit(ev: AuditEvent, cfg: dict | None = None) -> tuple[bool, str]:
    cols = (
        "trace_id,occurred_at,tenant_id,user_id,role,source,intent,question,"
        "time_start,time_end,sql_versions,final_sql,applied_sql,tables,row_count,"
        "elapsed_ms,tokens_in,tokens_out,cost_cny,verdict,refused,refuse_reason,"
        "answer_excerpt,pii_hits,policy_hash,trace_json,error"
    )
    sql = ("INSERT INTO qa_logs (%s) VALUES (" % cols) + ",".join(["%s"] * 27) + ")"
    d = ev.to_dict()
    vals = (
        d["trace_id"], d["occurred_at"], d["tenant_id"], d["user_id"], d["role"],
        d["source"], d["intent"], d["question"],
        d["time_start"] or None, d["time_end"] or None,
        Jsonb(d["sql_versions"]), d["final_sql"], d["applied_sql"],
        Jsonb(d["tables"]), d["row_count"], d["elapsed_ms"],
        d["tokens_in"], d["tokens_out"], d["cost_cny"], d["verdict"],
        d["refused"], d["refuse_reason"], d["answer_excerpt"],
        int(d.get("pii_hits") or 0), d["policy_hash"],
        Jsonb(d["trace_json"]), d["error"],
    )
    try:
        with connect(cfg) as conn:
            with conn.cursor() as cur:
                cur.execute(sql, vals)
        return True, ""
    except Exception as e:
        return False, _err(e, 300)


# ---------------------------------------------------------------- 审计访问（AC-14）

def insert_access(user_id: str, role: str, action: str = "read", *,
                  target: str = "", filters: dict | None = None,
                  rows: int = 0, source: str = "web", note: str = "",
                  cfg: dict | None = None) -> tuple[bool, str]:
    """谁看了审计、看了谁、拿到几行。

    写失败**不阻断**读接口：审计访问记录是"旁路证据"，
    为了记录它而让查审计这个动作失败，等于用次要目标破坏主要功能。
    但失败必须回给调用方（webapp 会在响应里带 `access_logged=false`），
    否则就成了"以为记了其实没记"——那比不记更糟。
    """
    try:
        with connect(cfg) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO audit_access "
                    "(user_id,role,action,target,filters,rows,source,note) "
                    "VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
                    (user_id or "", role or "", action or "read", target or "",
                     Jsonb(filters or {}), int(rows or 0), source or "web", note or ""))
        return True, ""
    except Exception as e:
        return False, _err(e, 200)


def fetch_access(cfg: dict | None = None, *, limit: int = 20,
                 user_id: str | None = None) -> list[dict]:
    sql = "SELECT * FROM audit_access"
    args: list = []
    if user_id:
        sql += " WHERE user_id = %s"
        args.append(user_id)
    sql += " ORDER BY occurred_at DESC, id DESC LIMIT %s"
    args.append(min(int(limit), 200))
    try:
        with connect(cfg) as conn:
            with conn.cursor() as cur:
                cur.execute(sql, args)
                names = [c.name for c in cur.description]
                out = []
                for row in cur.fetchall():
                    item = dict(zip(names, row))
                    for k, v in list(item.items()):
                        if hasattr(v, "isoformat"):
                            item[k] = v.isoformat()
                        elif isinstance(v, str) and v.startswith(("{", "[")):
                            try:
                                item[k] = json.loads(v)
                            except ValueError:
                                pass
                    out.append(item)
                return out
    except Exception:
        return []


def insert_cost(rec: dict, cfg: dict | None = None) -> tuple[bool, str]:
    """一条模型调用台账（AC-18）。

    和 `insert_audit` 一样：**失败返回 (False, 原因)，绝不吞**。网关会把原因
    挂到 `ledger_error` 上，页面和 trace 都能看见——"记账失败"必须比"没记账"
    更显眼，否则成本看板会安静地少算。
    """
    cols = (
        "trace_id,user_id,role,model,caller,tokens_in,tokens_out,cost_cny,cached,"
        "ok,latency_ms,attempts,error,prompt_hash,prompt_chars,prompt_head,"
        "answer_head,note"
    )
    names = cols.split(",")
    sql = ("INSERT INTO cost_ledger (%s) VALUES (%s)"
           % (cols, ",".join(["%s"] * len(names))))
    vals = [rec.get(k) if rec.get(k) is not None else "" for k in names]
    # 数值列不能塞空串：PG 会拒绝 '' → int。这里显式归零而不是靠 default，
    # 因为显式传 NULL 会覆盖 default，而 '' 更会直接报错。
    for k in ("tokens_in", "tokens_out", "latency_ms", "attempts", "prompt_chars"):
        vals[names.index(k)] = int(rec.get(k) or 0)
    vals[names.index("cost_cny")] = float(rec.get("cost_cny") or 0)
    vals[names.index("cached")] = bool(rec.get("cached"))
    vals[names.index("ok")] = bool(rec.get("ok", True))
    try:
        with connect(cfg) as conn:
            with conn.cursor() as cur:
                cur.execute(sql, vals)
        return True, ""
    except Exception as e:
        return False, _err(e, 300)


def cost_stats(cfg: dict | None = None, days: int = 1) -> dict:
    """网关成本看板：总量 / 按 caller / 按角色 / 按小时 / 失败与缓存。"""
    days = max(1, min(int(days or 1), 30))
    out: dict = {"ok": False, "days": days, "total": {}, "by_caller": [],
                 "by_role": [], "by_hour": [], "error": ""}
    try:
        with connect(cfg) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT count(*), count(*) FILTER (WHERE NOT ok),"
                    " count(*) FILTER (WHERE cached),"
                    " coalesce(sum(tokens_in),0), coalesce(sum(tokens_out),0),"
                    " coalesce(sum(cost_cny),0),"
                    " coalesce(avg(latency_ms) FILTER (WHERE ok),0)"
                    " FROM cost_ledger WHERE occurred_at >= now() - (%s || ' days')::interval",
                    (days,))
                r = cur.fetchone()
                out["total"] = {
                    "calls": int(r[0] or 0), "failed": int(r[1] or 0),
                    "cached": int(r[2] or 0), "tokens_in": int(r[3] or 0),
                    "tokens_out": int(r[4] or 0), "cost_cny": float(r[5] or 0),
                    "avg_latency_ms": int(r[6] or 0),
                }
                cur.execute(
                    "SELECT caller, count(*), coalesce(sum(cost_cny),0),"
                    " coalesce(avg(latency_ms),0), count(*) FILTER (WHERE NOT ok)"
                    " FROM cost_ledger WHERE occurred_at >= now() - (%s || ' days')::interval"
                    " GROUP BY caller ORDER BY 2 DESC", (days,))
                out["by_caller"] = [
                    {"caller": x[0] or "?", "calls": int(x[1]),
                     "cost_cny": float(x[2]), "avg_ms": int(x[3] or 0),
                     "failed": int(x[4] or 0)}
                    for x in cur.fetchall()]
                cur.execute(
                    "SELECT role, count(*), coalesce(sum(cost_cny),0)"
                    " FROM cost_ledger WHERE occurred_at >= now() - (%s || ' days')::interval"
                    " GROUP BY role ORDER BY 2 DESC", (days,))
                out["by_role"] = [{"role": x[0] or "?", "calls": int(x[1]),
                                   "cost_cny": float(x[2])} for x in cur.fetchall()]
                cur.execute(
                    "SELECT to_char(date_trunc('hour', occurred_at), 'MM-DD HH24:00'),"
                    " count(*), coalesce(sum(cost_cny),0)"
                    " FROM cost_ledger WHERE occurred_at >= now() - (%s || ' days')::interval"
                    " GROUP BY 1 ORDER BY 1", (days,))
                out["by_hour"] = [{"hour": x[0], "calls": int(x[1]),
                                   "cost_cny": float(x[2])} for x in cur.fetchall()]
        out["ok"] = True
    except Exception as e:
        out["error"] = _err(e, 200)
    return out


def cost_today(user_id: str, cfg: dict | None = None) -> float:
    """某用户今天已经花了多少钱。0 表示查不到（查不到时网关不放行预算判断，
    但会在 note 里留痕——不拿"查不到"当成"没花钱"。"""
    try:
        with connect(cfg) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT coalesce(sum(cost_cny),0) FROM cost_ledger"
                    " WHERE user_id = %s AND occurred_at >= date_trunc('day', now())",
                    (user_id,))
                return float(cur.fetchone()[0] or 0)
    except Exception:
        return 0.0


def fetch_audit(cfg: dict | None = None, *, trace_id: str | None = None,
               user_id: str | None = None, limit: int = 20) -> list[dict]:
    where, args = [], []
    if trace_id:
        where.append("trace_id = %s")
        args.append(trace_id)
    if user_id:
        where.append("user_id = %s")
        args.append(user_id)
    sql = "SELECT * FROM qa_logs"
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY occurred_at DESC LIMIT %s"
    args.append(min(int(limit), 200))
    try:
        with connect(cfg) as conn:
            with conn.cursor() as cur:
                cur.execute(sql, args)
                names = [c.name for c in cur.description]
                out = []
                for row in cur.fetchall():
                    item = dict(zip(names, row))
                    for k, v in list(item.items()):
                        if hasattr(v, "isoformat"):
                            item[k] = v.isoformat()
                        elif isinstance(v, str) and v.startswith(("{", "[")):
                            try:
                                item[k] = json.loads(v)
                            except ValueError:
                                pass
                    out.append(item)
                return out
    except Exception:
        return []


def stats(cfg: dict | None = None, days: int = 1,
          sources: tuple[str, ...] | None = ("web", "api")) -> dict:
    """监控看板聚合：成功率 / P95 / token / 成本 / 意图分布 / 拒答原因 Top。

    默认只统计**真实用户入口**（web / api）。审计表里还有 cli（自测、评测、
    一次性脚本）和 eval（回归），这两类每跑一轮就是几十上百条，其中大量是
    刻意构造的拒答与注入——混进业务口径后成功率会变成 54% 这种没法看的数字。
    审计本身一条不少地留着（可追溯），只是不参与业务指标。
    `sources=None` 可看全量口径（排查用）。
    """
    try:
        with connect(cfg) as conn:
            with conn.cursor() as cur:
                w = "occurred_at > now() - (%s || ' days')::interval"
                args: list = [str(days)]
                if sources:
                    w += " AND source = ANY(%s)"
                    args.append(list(sources))
                args = tuple(args)
                cur.execute(
                    "SELECT count(*), "
                    "count(*) FILTER (WHERE refused), "
                    "count(*) FILTER (WHERE error IS NOT NULL), "
                    "coalesce(avg(elapsed_ms),0)::int, "
                    "coalesce(percentile_disc(0.95) WITHIN GROUP (ORDER BY elapsed_ms),0)::int, "
                    "coalesce(sum(tokens_in),0)::int, "
                    "coalesce(sum(tokens_out),0)::int, "
                    "coalesce(sum(cost_cny),0) "
                    "FROM qa_logs WHERE " + w, args)
                r = cur.fetchone()

                cur.execute(
                    "SELECT intent, count(*) FROM qa_logs WHERE " + w +
                    " GROUP BY intent", args)
                intents = {k: v for k, v in cur.fetchall()}

                # 拒答原因只取前 40 字做聚类，否则每个问题都是一条独立原因
                cur.execute(
                    "SELECT left(coalesce(refuse_reason,''), 40) AS why, count(*) "
                    "FROM qa_logs WHERE refused AND " + w +
                    " GROUP BY why ORDER BY count(*) DESC LIMIT 5", args)
                top_refusals = [{"reason": k, "n": v} for k, v in cur.fetchall()]

        total = r[0] or 0
        return {
            "total": total, "refused": r[1], "errors": r[2],
            "success_rate": round(100.0 * (total - r[1] - r[2]) / total, 1) if total else 0.0,
            "avg_ms": r[3], "p95_ms": r[4],
            "tokens_in": r[5], "tokens_out": r[6],
            "cost_cny": round(float(r[7] or 0), 6),
            "cost_per_query": round(float(r[7] or 0) / total, 6) if total else 0.0,
            "intents": intents,
            "top_refusals": top_refusals,
            "sources": list(sources) if sources else "all",
        }
    except Exception as e:
        return {"error": _err(e, 200)}


# ---------------------------------------------------------------- 会话
def insert_session(token_hash: str, user_id: str, ttl_min: int = 480,
                   cfg: dict | None = None) -> bool:
    try:
        with connect(cfg) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO sessions(token_hash,user_id,expires_at) "
                    "VALUES (%s,%s, now() + (%s || ' minutes')::interval)",
                    (token_hash, user_id, str(int(ttl_min))))
        return True
    except Exception:
        return False


def session_user(token_hash: str, cfg: dict | None = None) -> str | None:
    """有效会话返回 user_id；过期会话顺手删掉。"""
    try:
        with connect(cfg) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT user_id, expires_at FROM sessions WHERE token_hash = %s",
                    (token_hash,))
                row = cur.fetchone()
                if not row:
                    return None
                if row[1] and row[1].timestamp() < __import__("time").time():
                    cur.execute("DELETE FROM sessions WHERE token_hash = %s",
                                (token_hash,))
                    return None
                cur.execute("UPDATE sessions SET last_seen_at = now() "
                            "WHERE token_hash = %s", (token_hash,))
                return row[0]
    except Exception:
        return None


def delete_session(token_hash: str, cfg: dict | None = None) -> None:
    try:
        with connect(cfg) as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM sessions WHERE token_hash = %s",
                            (token_hash,))
    except Exception:
        pass


# ---------------------------------------------------------------- 对话会话

def _rows(cur) -> list[dict]:
    """行转 dict：时间转 iso、jsonb 串转对象。
    与 fetch_access 的处理保持一致，前端不必区分字段来源。"""
    names = [c.name for c in cur.description]
    out = []
    for row in cur.fetchall():
        item = dict(zip(names, row))
        for k, v in list(item.items()):
            if hasattr(v, "isoformat"):
                item[k] = v.isoformat()
            elif isinstance(v, str) and v.startswith(("{", "[")):
                try:
                    item[k] = json.loads(v)
                except ValueError:
                    pass
        out.append(item)
    return out


def create_conversation(user_id: str, tenant_id: str = "default",
                        title: str = "新对话",
                        cfg: dict | None = None) -> tuple[str, str]:
    """返回 (conv_id, 错误)。空 conv_id = 失败，调用方要如实报错。"""
    cid = "c_" + uuid4().hex[:16]
    try:
        with connect(cfg) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO conversations(conv_id,user_id,tenant_id,title) "
                    "VALUES (%s,%s,%s,%s)",
                    (cid, user_id, tenant_id or "default", (title or "新对话")[:80]))
        return cid, ""
    except Exception as e:
        return "", _err(e, 200)


def list_conversations(user_id: str, limit: int = 50,
                       cfg: dict | None = None) -> list[dict]:
    """按更新时间倒序。只列自己的、未删的。"""
    try:
        with connect(cfg) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT conv_id,title,created_at,updated_at,msg_count "
                    "FROM conversations WHERE user_id = %s AND NOT deleted "
                    "AND msg_count > 0 ORDER BY updated_at DESC LIMIT %s",
                    (user_id, min(int(limit), 200)))
                return _rows(cur)
    except Exception:
        return []


def get_conversation(conv_id: str, user_id: str,
                     cfg: dict | None = None) -> dict | None:
    """带上 user_id 一起查，越权就是查不到——不用先查再比，少一次 TOCTOU。"""
    try:
        with connect(cfg) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT conv_id,title,created_at,updated_at,msg_count "
                    "FROM conversations WHERE conv_id = %s AND user_id = %s AND NOT deleted",
                    (conv_id, user_id))
                rows = _rows(cur)
                return rows[0] if rows else None
    except Exception:
        return None


def rename_conversation(conv_id: str, user_id: str, title: str,
                        cfg: dict | None = None) -> bool:
    try:
        with connect(cfg) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE conversations SET title = %s, updated_at = now() "
                    "WHERE conv_id = %s AND user_id = %s AND NOT deleted",
                    ((title or "新对话")[:80], conv_id, user_id))
                return cur.rowcount > 0
    except Exception:
        return False


def delete_conversation(conv_id: str, user_id: str,
                        cfg: dict | None = None) -> bool:
    """软删：列表里消失，消息仍在库里。
    审计证据在 qa_logs 里，不依赖这里；这里删的只是用户自己的聊天记录。"""
    try:
        with connect(cfg) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE conversations SET deleted = true, updated_at = now() "
                    "WHERE conv_id = %s AND user_id = %s AND NOT deleted",
                    (conv_id, user_id))
                return cur.rowcount > 0
    except Exception:
        return False


def append_message(conv_id: str, role: str, content: str,
                   payload: dict | None = None, trace_id: str = "",
                   cfg: dict | None = None) -> tuple[bool, str]:
    """写一条消息并推进会话的 updated_at / msg_count。

    同一事务里做两件事：消息落库 + 会话计数更新。分两次写会出现
    "消息写进去了但列表不更新"，用户以为没发出去。
    """
    try:
        with connect(cfg) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO conversation_messages(conv_id,role,content,payload,trace_id) "
                    "VALUES (%s,%s,%s,%s,%s)",
                    (conv_id, role, (content or "")[:4000],
                     Jsonb(payload) if payload is not None else None,
                     trace_id or None))
                cur.execute(
                    "UPDATE conversations SET updated_at = now(), "
                    "msg_count = msg_count + 1 WHERE conv_id = %s",
                    (conv_id,))
        return True, ""
    except Exception as e:
        return False, _err(e, 200)


def set_title_if_default(conv_id: str, title: str,
                         cfg: dict | None = None) -> bool:
    """第一轮问完用问题当标题。已经是自定义标题就不覆盖。"""
    try:
        with connect(cfg) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE conversations SET title = %s "
                    "WHERE conv_id = %s AND (title = '新对话' OR title = '')",
                    ((title or "")[:40], conv_id))
                return cur.rowcount > 0
    except Exception:
        return False


def load_messages(conv_id: str, user_id: str, limit: int = 200,
                  cfg: dict | None = None) -> list[dict] | None:
    """取会话消息。会话不属于这个人 → None（前端按 404 处理）。
    先校验归属再取消息，两者在同一个连接里，避免"查到一半被改了归属"。"""
    try:
        with connect(cfg) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT 1 FROM conversations "
                    "WHERE conv_id = %s AND user_id = %s AND NOT deleted",
                    (conv_id, user_id))
                if cur.fetchone() is None:
                    return None
                cur.execute(
                    "SELECT msg_id,role,content,payload,trace_id,created_at "
                    "FROM conversation_messages WHERE conv_id = %s "
                    "ORDER BY msg_id ASC LIMIT %s",
                    (conv_id, min(int(limit), 500)))
                return _rows(cur)
    except Exception:
        return None


def recent_turns(conv_id: str, user_id: str, turns: int = 3,
                 cfg: dict | None = None) -> list[dict]:
    """最近 N 轮问答，给生成节点当上下文用。

    只取配对成功的轮次（user 后紧跟 assistant）。半轮（刚问了还没答）
    不参与——把"只有一个问题没有答案"塞进 prompt 只会让模型自己编答案。
    """
    msgs = load_messages(conv_id, user_id, limit=max(24, turns * 4), cfg=cfg)
    if not msgs:
        return []
    pairs: list[dict] = []
    q = None
    for m in msgs:
        if m["role"] == "user":
            q = m.get("content") or ""
        elif m["role"] == "assistant" and q:
            p = m.get("payload") or {}
            if isinstance(p, str):
                try:
                    p = json.loads(p)
                except ValueError:
                    p = {}
            pairs.append({
                "question": q,
                "answer": (m.get("content") or "")[:400],
                "sql": (p.get("sql") or "")[:1200],
                "time_window": p.get("time_window") or {},
                "time_start": p.get("time_start") or "",
                "time_end": p.get("time_end") or "",
                "metrics": p.get("metrics") or [],
                "tables": p.get("tables") or [],
                "dimensions": p.get("dimensions") or [],
                "intent": p.get("intent") or "",
                "trace_id": m.get("trace_id") or "",
            })
            q = None
    return pairs[-turns:]


# ---------------------------------------------------------------- ETL 登记表
# agent_ro 只读，ETL 生成物不在这套系统里执行；这里登记"谁在什么时候
# 让 agent 生成了什么、校验过不过"，事后可追溯，也供前端历史列表回显。

def save_etl_script(user_id: str, title: str, target_table: str,
                    requirement: str, sql_text: str, checks: dict,
                    cfg: dict | None = None) -> tuple[int | None, str | None]:
    try:
        with connect(cfg) as conn, conn.cursor() as cur:
            status = "verified" if checks.get("explain_ok") and checks.get("target_ok") \
                and checks.get("stmt_ok") else "generated"
            cur.execute(
                "INSERT INTO etl_scripts (user_id, title, target_table, requirement,"
                " sql_text, checks, status) VALUES (%s,%s,%s,%s,%s,%s,%s) RETURNING id",
                (user_id, (title or "")[:200], target_table or "",
                 (requirement or "")[:4000], sql_text or "",
                 Jsonb(checks or {}), status))
            rid = cur.fetchone()[0]
        return int(rid), None
    except Exception as e:
        return None, _err(e)


def get_etl_script(script_id: int, user_id: str,
                   cfg: dict | None = None) -> dict | None:
    """按 id 取单条（执行/登记都要从库里取，不信任前端回传的 SQL）。"""
    try:
        with connect(cfg) as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT id, title, target_table, requirement, sql_text,"
                " checks, status, created_at FROM etl_scripts"
                " WHERE id = %s AND user_id = %s", (int(script_id), user_id))
            rows = _rows(cur)
            return rows[0] if rows else None
    except Exception:
        return None


def record_etl_exec(script_id: int, info: dict,
                    cfg: dict | None = None) -> bool:
    """把一次真实执行的结果写回 checks 载荷（executed_at/rows/visible），
    并升级 status：只有真跑通且问数账号可见才算 deployed。"""
    try:
        with connect(cfg) as conn, conn.cursor() as cur:
            cur.execute("SELECT checks FROM etl_scripts WHERE id = %s",
                        (int(script_id),))
            row = cur.fetchone()
            if not row:
                return False
            checks = row[0] or {}
            if isinstance(checks, str):
                import json as _json
                try:
                    checks = _json.loads(checks)
                except ValueError:
                    checks = {}
            checks["exec"] = info
            status = "deployed" if info.get("ok") else "execute_failed"
            cur.execute("UPDATE etl_scripts SET checks = %s, status = %s WHERE id = %s",
                        (Jsonb(checks), status, int(script_id)))
        return True
    except Exception:
        return False


def list_etl_scripts(user_id: str, limit: int = 20,
                     cfg: dict | None = None) -> list[dict] | None:
    try:
        with connect(cfg) as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT id, title, target_table, requirement, sql_text,"
                " checks, status, created_at"
                " FROM etl_scripts WHERE user_id = %s ORDER BY id DESC LIMIT %s",
                (user_id, min(int(limit), 100)))
            return _rows(cur)
    except Exception:
        return None


def check(cfg: dict | None = None) -> dict:
    """health check：连得上、表在、不可变触发器在。"""
    try:
        with connect(cfg) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT to_regclass('public.qa_logs')")
                has_logs = cur.fetchone()[0] is not None
                cur.execute("SELECT count(*) FROM pg_trigger "
                            "WHERE tgname = 'trg_qa_logs_immutable' AND NOT tgisinternal")
                immutable = cur.fetchone()[0] > 0
        return {"ok": True, "dsn": dsn(cfg).split("@")[-1],
                "qa_logs": has_logs, "immutable": immutable}
    except Exception as e:
        return {"ok": False, "error": _err(e, 200)}


# ---------------------------------------------------------------- 反馈回流
# 「这条答案准不准」是唯一能证明系统在真实场景里错在哪里的信号。
# 评测集的 128 题是我们自己出的，反馈来自真实提问——两者互补，不能替代。

FEEDBACK_VERDICTS = ("useful", "wrong", "unclear")


def qa_owner(trace_id: str, cfg: dict | None = None) -> dict | None:
    """取这条 trace 的归属，用于校验"能不能给它打标"。

    只返回最小字段：校验只需要 user_id/role，把整行（含 SQL、答案）交给
    调用方会让越权判断和数据处理耦合在一起。
    """
    try:
        with connect(cfg) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT user_id, role, question FROM qa_logs "
                            "WHERE trace_id = %s LIMIT 1", (trace_id,))
                rows = _rows(cur)
                return rows[0] if rows else None
    except Exception:
        return None


def upsert_feedback(trace_id: str, user_id: str, verdict: str, *,
                    role: str = "", reason: str = "", comment: str = "",
                    question: str = "", trust_level: str = "",
                    cfg: dict | None = None) -> tuple[bool, str]:
    """同一人对同一条答案只留一条：改判走 UPDATE，不追加新行。

    为什么允许改判：用户点完"不准"又发现是自己理解错了，这类反转很常见。
    追加新行会让统计口径变成"以最新一条为准"，那还不如一开始就只有一条。
    """
    if verdict not in FEEDBACK_VERDICTS:
        return False, "verdict 只能是 %s" % "/".join(FEEDBACK_VERDICTS)
    try:
        with connect(cfg) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO answer_feedback (trace_id,user_id,role,verdict,"
                    "reason,comment,question,trust_level) "
                    "VALUES (%s,%s,%s,%s,%s,%s,%s,%s) "
                    "ON CONFLICT (trace_id,user_id) DO UPDATE SET "
                    "verdict=EXCLUDED.verdict, reason=EXCLUDED.reason, "
                    "comment=EXCLUDED.comment, updated_at=now()",
                    (trace_id, user_id, role or "", verdict, reason or "",
                     comment or "", question or "", trust_level or ""))
        return True, ""
    except Exception as e:
        return False, _err(e, 200)


def feedback_stats(cfg: dict | None = None, days: int = 30,
                   user_id: str | None = None) -> dict:
    """采纳率 + 不准原因分布 + 按可信度分档的准确率感。

    最后一列是本设计的关键验证：如果"可信度低"的答案被标不准的比例
    并不高于"可信度高"的，那说明可信度规则没用——数字摆在一起才看得出来。

    user_id 非空时只统计这个人自己的反馈（普通账号看自己的，
    治理岗看全量——用户不该从聚合数字里推断别人问过什么）。
    """
    out = {"days": days, "total": 0, "useful": 0, "wrong": 0, "unclear": 0,
           "adopt_rate": None, "reasons": [], "by_trust": [], "error": "",
           "scope": "self" if user_id else "all"}
    where = "created_at >= now() - (%s || ' days')::interval"
    args: list = [int(days)]
    if user_id:
        where += " AND user_id = %s"
        args.append(user_id)
    try:
        with connect(cfg) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT verdict, count(*) FROM answer_feedback "
                    "WHERE " + where + " GROUP BY verdict", args)
                for v, n in cur.fetchall():
                    if v in out:
                        out[v] = int(n)
                    out["total"] += int(n)
                if out["total"]:
                    judged = out["useful"] + out["wrong"]
                    out["adopt_rate"] = (round(out["useful"] / judged, 4)
                                         if judged else None)
                cur.execute(
                    "SELECT coalesce(nullif(reason,''),'未填') r, count(*) "
                    "FROM answer_feedback WHERE verdict='wrong' AND " + where +
                    " GROUP BY r ORDER BY count(*) DESC LIMIT 10", args)
                out["reasons"] = [{"reason": r, "n": int(n)}
                                  for r, n in cur.fetchall()]
                cur.execute(
                    "SELECT coalesce(nullif(trust_level,''),'—') lvl, "
                    "count(*) FILTER (WHERE verdict='useful') u, "
                    "count(*) FILTER (WHERE verdict='wrong') w, count(*) n "
                    "FROM answer_feedback WHERE " + where +
                    " GROUP BY lvl ORDER BY lvl", args)
                for lvl, u, w, n in cur.fetchall():
                    judged = int(u) + int(w)
                    out["by_trust"].append({
                        "level": lvl, "useful": int(u), "wrong": int(w),
                        "n": int(n),
                        "wrong_rate": round(int(w) / judged, 4) if judged else None})
        return out
    except Exception as e:
        out["error"] = _err(e, 200)
        return out


def wrong_feedback(cfg: dict | None = None, limit: int = 200,
                   days: int = 90) -> list[dict]:
    """「不准」的原始记录，供转候选评测题。连同当时的问题、SQL、可信度级别一起带出。"""
    try:
        with connect(cfg) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT f.trace_id, f.user_id, f.verdict, f.reason, f.comment, "
                    "f.question, f.trust_level, f.created_at, "
                    "q.final_sql, q.applied_sql, q.verdict AS qa_verdict, "
                    "q.time_start, q.time_end, q.intent "
                    "FROM answer_feedback f "
                    "LEFT JOIN qa_logs q ON q.trace_id = f.trace_id "
                    "WHERE f.verdict = 'wrong' "
                    "AND f.created_at >= now() - (%s || ' days')::interval "
                    "ORDER BY f.created_at DESC LIMIT %s",
                    (int(days), min(int(limit), 500)))
                names = [c.name for c in cur.description]
                out = []
                for row in cur.fetchall():
                    item = dict(zip(names, row))
                    for k, v in list(item.items()):
                        if hasattr(v, "isoformat"):
                            item[k] = v.isoformat()
                    out.append(item)
                return out
    except Exception:
        return []


# ---------------------------------------------------------------- 长期记忆
# 与会话历史分开：会话可整段删除，记忆按条可删；都不写 qa_logs（不可变证据链）。
MEMORY_KINDS = ("preference",)
# 刻意不收 conclusion：历史结论里的数字会被模型抄进新答案，且口径一改就过期。


def upsert_memory(user_id: str, tenant_id: str, kind: str, mem_key: str,
                  content: str, payload: dict, caliber_version: str,
                  trace_id: str = "", cfg: dict | None = None) -> tuple[bool, str]:
    """同一用户同一去重键只留一条；再次命中更新内容并保留 hits。

    payload 带 answer 摘要，但 prompt 注入时强制标"历史值不得当作本轮结果"——
    数字污染是长期记忆最大的风险，不能靠调用方自觉。
    """
    if kind not in MEMORY_KINDS:
        return False, "kind 只能是 %s" % "/".join(MEMORY_KINDS)
    try:
        with connect(cfg) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO user_memory (user_id,tenant_id,kind,mem_key,content,"
                    "payload,caliber_version,trace_id) "
                    "VALUES (%s,%s,%s,%s,%s,%s,%s,%s) "
                    "ON CONFLICT (user_id,kind,mem_key) DO UPDATE SET "
                    "content=EXCLUDED.content, payload=EXCLUDED.payload, "
                    "caliber_version=EXCLUDED.caliber_version, "
                    "trace_id=EXCLUDED.trace_id, deleted=false, updated_at=now()",
                    (user_id, tenant_id or "default", kind, mem_key,
                     content[:400], Jsonb(payload), caliber_version or "",
                     trace_id or ""))
        return True, ""
    except Exception as e:
        return False, _err(e, 200)


def load_memory(user_id: str, cfg: dict | None = None, *,
                limit: int = 200, include_deleted: bool = False) -> list[dict]:
    """只取本人记忆。越权的根子在查询条件，不在调用方过滤。"""
    try:
        with connect(cfg) as conn:
            with conn.cursor() as cur:
                sql = ("SELECT mem_id,kind,mem_key,content,payload,caliber_version,"
                       "trace_id,hits,updated_at,last_used_at FROM user_memory "
                       "WHERE user_id=%s")
                if not include_deleted:
                    sql += " AND deleted=false"
                sql += " ORDER BY updated_at DESC LIMIT %s"
                cur.execute(sql, (user_id, max(1, int(limit))))
                return _rows(cur)
    except Exception:
        return []


def delete_memory(mem_id: int, user_id: str,
                  cfg: dict | None = None) -> bool:
    """按 id + user_id 软删：别人的记忆删不到，审计侧也不动。"""
    try:
        with connect(cfg) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE user_memory SET deleted=true, updated_at=now() "
                    "WHERE mem_id=%s AND user_id=%s AND deleted=false",
                    (mem_id, user_id))
                return cur.rowcount > 0
    except Exception:
        return False


def touch_memory(mem_ids: list[int], user_id: str,
                 cfg: dict | None = None) -> None:
    """命中计数：用来区分"反复要用的偏好"和"一次性的历史结论"。"""
    ids = [int(i) for i in (mem_ids or []) if str(i).isdigit()]
    if not ids:
        return
    try:
        with connect(cfg) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE user_memory SET hits=hits+1, last_used_at=now() "
                    "WHERE user_id=%s AND mem_id = ANY(%s) AND deleted=false",
                    (user_id, ids))
    except Exception:
        pass
