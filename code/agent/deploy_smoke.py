"""一键部署的冒烟检查（AC-20 的判据）。

    python code/agent/deploy_smoke.py               # 查全部
    python code/agent/deploy_smoke.py --skip-web    # 只查后端依赖
    python code/agent/deploy_smoke.py --port 8000

与 `smoke.py` 的分工：
  · `smoke.py`        查**业务对不对**（DAU=3237、R17/R18 拦不拦、实时问题拒答）；
  · `deploy_smoke.py` 查**依赖齐不齐**（PG/Redis/Doris/密钥/表结构/Web 活着）。

两者都会在 start.ps1 里跑。"服务起来了"和"部署对了"是两件事——
进程在、端口在听，但审计表被 drop 了、或者 Redis 其实降级回内存了，
只看进程列表全都是绿的。

退出码：0 全过；1 有失败。脚本据此判断，不靠人眼数绿点。
"""
from __future__ import annotations

import _bootstrap  # noqa: F401

import argparse
import json
import socket
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

HERE = Path(__file__).resolve().parent

PASS = FAIL = SKIP = 0
ROWS: list[tuple[str, str, str]] = []


def item(name: str, ok: bool, detail: str = "") -> None:
    global PASS, FAIL
    if ok:
        PASS += 1
        ROWS.append(("PASS", name, detail))
        print("  [PASS] %-26s %s" % (name, detail))
    else:
        FAIL += 1
        ROWS.append(("FAIL", name, detail))
        print("  [FAIL] %-26s %s" % (name, detail))


def skip(name: str, why: str = "") -> None:
    global SKIP
    SKIP += 1
    ROWS.append(("SKIP", name, why))
    print("  [SKIP] %-26s %s" % (name, why))


# ---------------------------------------------------------------- 各项检查
def check_secrets(cfg: dict) -> None:
    import vault
    vault.warmup(cfg)
    ok, missing = vault.validate(cfg)
    item("密钥齐全", ok, "" if ok else "缺失: " + ", ".join(missing))
    for w in vault.weak():
        print("         └ 提示: %s 只有 %d 位（本地可接受）" % (w["name"], w["length"]))


def _store(cfg: dict):
    """governance 在 code/ 下，agent 在 code/agent 下，得把 code 挂进路径。"""
    if str(HERE.parent) not in sys.path:
        sys.path.insert(0, str(HERE.parent))
    from governance import store
    return store


def check_pg(cfg: dict) -> None:
    store = _store(cfg)
    h = store.check(cfg)
    item("治理库 PostgreSQL", bool(h.get("ok")),
         str(h.get("error") or h.get("dsn", ""))[:70])
    if not h.get("ok"):
        return
    # 表在不在，比"能连上"更重要：连上了但没建表，审计会写失败而没人发现。
    item("qa_logs 表存在", bool(h.get("qa_logs")), "审计主表")
    item("不可变触发器在岗", bool(h.get("immutable")), "审计行不可 UPDATE/DELETE")


def check_pg_schema(cfg: dict) -> None:
    """抽查四张关键表——被 drop 过会立刻暴露。

    `answer_feedback` 也进来：反馈表缺失时前端点踩会静默失败，
    而这种"看起来记了其实没记"的错，只有部署检查能提前发现。
    """
    store = _store(cfg)
    try:
        with store.connect(cfg) as conn:
            with conn.cursor() as cur:
                cur.execute("""SELECT table_name FROM information_schema.tables
                               WHERE table_schema='public'""")
                have = {r[0] for r in cur.fetchall()}
    except Exception as e:
        item("治理表结构", False, type(e).__name__ + ": " + str(e)[:60])
        return
    want = {"qa_logs", "cost_ledger", "audit_access", "answer_feedback"}
    miss = want - have
    item("治理表结构", not miss,
         "齐备" if not miss else "缺: " + ", ".join(sorted(miss)))


def check_redis(cfg: dict) -> None:
    backend = str((cfg.get("cache") or {}).get("backend") or "").lower()
    if backend != "redis":
        skip("Redis", "缓存后端配置为 " + (backend or "local"))
        return
    try:
        import answercache
        answercache.reset_cache()          # 别捡上一轮的单例，要现连一次
        c = answercache.get_cache(cfg)
        # 判据是"真的拿到了 RedisCache"，不是"stats 里有没有 degraded"。
        # 实例的 stats() 根本没有 degraded 字段（降级标记在模块级），
        # 早先按 degraded 判会导致 Redis 挂掉时照样 PASS —— 典型假绿。
        is_redis = type(c).__name__ == "RedisCache"
        st = answercache.stats()           # 模块级，带 init_warning
        warn = st.get("init_warning") or ""
        if is_redis:
            item("Redis 连通", True, "RedisCache %s" % st.get("addr", ""))
        else:
            item("Redis 连通", False,
                 warn or ("拿到了 %s（配置要 redis）" % type(c).__name__))
    except Exception as e:
        item("Redis 连通", False, type(e).__name__ + ": " + str(e)[:60])


def check_doris(cfg: dict) -> None:
    try:
        import db
        out = db.run_sql("SELECT 1 AS ok", cfg=cfg, max_rows=1)
        err = out.get("error")
        item("Doris 数据仓", not err, "连上（远端集群，不在本机部署范围）"
             if not err else str(err)[:70])
    except Exception as e:
        item("Doris 数据仓", False, type(e).__name__ + ": " + str(e)[:60])


def check_guard(cfg: dict) -> None:
    try:
        import guardtool
        # 返回的是三元组 (verdict, findings, stderr)，不是 findings 列表
        verdict, findings, gerr = guardtool.check(
            "SELECT COUNT(*) AS c FROM dwd_play_start WHERE dt='2026-11-08'", cfg)
        rej = [f.get("rule") for f in findings if f.get("level") == "REJECT"]
        item("护栏可用", verdict != "reject" and not rej,
             ("verdict=%s" % verdict) if not rej else "被拦: " + str(rej)[:70])
        if gerr:
            print("         └ 护栏 stderr: " + str(gerr)[:100])
    except Exception as e:
        item("护栏可用", False, type(e).__name__ + ": " + str(e)[:60])


def check_port(port: int) -> None:
    """端口探测放在 Web 检查之前，好区分"进程没起"和"起了但报错"。"""
    s = socket.socket()
    s.settimeout(2)
    try:
        s.connect(("127.0.0.1", port))
        item("Web 端口 %d" % port, True, "在听")
    except OSError:
        item("Web 端口 %d" % port, False, "没人监听")
    finally:
        s.close()


def check_web(port: int) -> None:
    url = "http://127.0.0.1:%d/api/health" % port
    try:
        # 用 urllib 而不是 httpx：冒烟脚本要能在依赖没装全时也跑得起来
        with urllib.request.urlopen(url, timeout=8) as r:
            body = json.loads(r.read().decode("utf-8"))
        gov = body.get("governance") or {}
        item("Web /api/health", True, "200")
        item("Web 能看到治理库", bool(gov.get("ok")), str(gov.get("dsn", ""))[:60])
    except urllib.error.URLError as e:
        item("Web /api/health", False, "连不上 %s（%s）" % (url, e.reason))
    except Exception as e:
        item("Web /api/health", False, type(e).__name__ + ": " + str(e)[:60])


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=8000)
    ap.add_argument("--skip-web", action="store_true")
    ap.add_argument("--json", default=None, help="把结果写成 JSON")
    a = ap.parse_args()

    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

    print("=" * 62)
    print(" 部署冒烟检查 · AC-20 判据")
    print("=" * 62)

    from conf import load_config
    cfg = load_config()

    t0 = time.time()
    check_secrets(cfg)
    check_pg(cfg)
    check_pg_schema(cfg)
    check_redis(cfg)
    check_doris(cfg)
    check_guard(cfg)
    if a.skip_web:
        skip("Web 服务", "--skip-web")
    else:
        check_port(a.port)
        check_web(a.port)

    dt = time.time() - t0
    print("-" * 62)
    print(" %d 通过 / %d 失败 / %d 跳过   耗时 %.1fs" % (PASS, FAIL, SKIP, dt))
    print(" 结论：" + ("部署可用" if FAIL == 0 else "**未就绪**，先解决上面的 FAIL 项"))
    print("=" * 62)

    if a.json:
        Path(a.json).write_text(json.dumps(
            {"pass": PASS, "fail": FAIL, "skip": SKIP,
             "elapsed_sec": round(dt, 1),
             "rows": [{"status": s, "item": n, "detail": d} for s, n, d in ROWS]},
            ensure_ascii=False, indent=2), encoding="utf-8")

    return 0 if FAIL == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
