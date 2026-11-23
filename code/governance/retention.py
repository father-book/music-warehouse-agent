"""审计保留期：不可变表怎么做清理。

矛盾点：`qa_logs` 上有 `BEFORE UPDATE OR DELETE` 触发器，任何删除都会抛异常——
这是刻意设计的（ADR-006），目的是让"改审计"这件事在数据库层面就不可能。

但"永远不删"也不成立：审计表会一直长，合规上也有"到期应删"的要求。
所以要有一条**受控通道**，而不是把触发器删掉了事：

  1. 先把要删的行**整行**归档到 `qa_logs_archive`（payload 是 to_jsonb 整行，
     出问题时能原样还原，而不是只留几个关键字段再靠记忆）；
  2. 同一个事务里临时 `DISABLE TRIGGER`，删完立刻 `ENABLE TRIGGER`；
  3. 在 `audit_access` 里记一条 `action='purge'`：谁在什么时候删了多少行。

关键是"同一个事务"：DDL 在 PostgreSQL 里是事务性的，
**删除失败时事务回滚，触发器会自动恢复**——不会出现"触发器被关掉忘了打开"
这种最危险的状态。自测里专门验证这一点（删完触发器仍在，直接 DELETE 仍然报错）。

用法：
  python -m governance.retention                 # 只看会清多少（dry-run 默认）
  python -m governance.retention --apply         # 真清
  python -m governance.retention --days 30 --apply
"""
from __future__ import annotations

import json
import sys

from . import store

TRIGGER = "trg_qa_logs_immutable"


def plan(days: int | None = None, cfg: dict | None = None) -> dict:
    """将要清理多少行、涉及哪段时间。dry-run 与真实清理共用同一判据。"""
    d = _days(days, cfg)
    sql = ("SELECT count(*), min(occurred_at), max(occurred_at) FROM qa_logs "
           "WHERE occurred_at < now() - make_interval(days => %s)")
    try:
        with store.connect(cfg) as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (d,))
                n, lo, hi = cur.fetchone()
        return {"days": d, "rows": int(n or 0),
                "oldest": lo.isoformat() if lo else None,
                "newest": hi.isoformat() if hi else None, "error": ""}
    except Exception as e:
        return {"days": d, "rows": 0, "oldest": None, "newest": None,
                "error": "%s: %s" % (type(e).__name__, str(e)[:200])}


def run(days: int | None = None, cfg: dict | None = None, *,
        dry_run: bool = True, actor: str = "system") -> dict:
    """归档 + 删除。dry_run=True 时只报数。"""
    d = _days(days, cfg)
    if dry_run:
        p = plan(d, cfg)
        p["dry_run"] = True
        return p

    out = {"days": d, "archived": 0, "deleted": 0, "dry_run": False, "error": ""}
    try:
        with store.connect(cfg) as conn:          # 正常退出即 commit，异常即 rollback
            with conn.cursor() as cur:
                # 1) 关触发器（事务性：回滚会自动恢复）
                cur.execute("ALTER TABLE qa_logs DISABLE TRIGGER %s" % TRIGGER)
                # 2) 先归档，整行存 jsonb。ON CONFLICT：重复跑不会重复归档
                cur.execute(
                    "INSERT INTO qa_logs_archive (trace_id, payload) "
                    "SELECT trace_id, to_jsonb(q) FROM qa_logs q "
                    "WHERE occurred_at < now() - make_interval(days => %s) "
                    "ON CONFLICT (trace_id) DO NOTHING", (d,))
                out["archived"] = cur.rowcount
                # 3) 再删（删的判据与归档完全一致，不会出现"归档了但没删")
                cur.execute(
                    "DELETE FROM qa_logs "
                    "WHERE occurred_at < now() - make_interval(days => %s)", (d,))
                out["deleted"] = cur.rowcount
                # 4) 立刻恢复触发器
                cur.execute("ALTER TABLE qa_logs ENABLE TRIGGER %s" % TRIGGER)
    except Exception as e:
        out["error"] = "%s: %s" % (type(e).__name__, str(e)[:300])
        return out

    # 5) 记录这次清理本身。写在事务外：即使这一步失败，也不该回滚已完成的清理。
    ok, err = store.insert_access(
        actor, "system", "purge", target="qa_logs",
        filters={"days": d}, rows=out["deleted"],
        note="归档 %d 行并删除" % out["archived"], cfg=cfg)
    out["logged"] = ok
    out["log_error"] = err
    return out


def trigger_enabled(cfg: dict | None = None) -> bool:
    """触发器现在是不是在岗。自测与巡检用——最怕的是"被关掉忘了开"。"""
    try:
        with store.connect(cfg) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT tgenabled FROM pg_trigger WHERE tgname = %s", (TRIGGER,))
                row = cur.fetchone()
        return bool(row) and row[0] != "D"
    except Exception:
        return False


def _days(days: int | None, cfg: dict | None) -> int:
    if days:
        return int(days)
    if isinstance(cfg, dict) and cfg.get("governance"):
        return int((cfg["governance"].get("retention_days") or 180))
    from conf import load_config          # 延迟导入：governance 不依赖 agent 配置
    g = (load_config().get("governance") or {})
    return int(g.get("retention_days") or 180)


if __name__ == "__main__":
    args = sys.argv[1:]
    days = None
    if "--days" in args:
        days = int(args[args.index("--days") + 1])
    apply_ = "--apply" in args
    res = run(days, dry_run=not apply_, actor="cli:manual")
    print(json.dumps(res, ensure_ascii=False, indent=1))
    print("触发器在岗：", trigger_enabled())
