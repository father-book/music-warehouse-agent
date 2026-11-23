"""并发压测（AC-16）：并发下的 P95、成功率、缓存命中率、护栏开销。

为什么单独一个脚本
------------------
单条问答的耗时说明不了什么。真实故障长这样：一个人问 4s 没事，十个人同时问
就变成 20s、一半失败、成本翻三倍。这些只有在并发下才暴露，而且要分开看是
哪一段在排队——模型、护栏、数据库三段的瓶颈形态完全不同。

两种压测目标
------------
  --target local   进程内多线程直接调 ask()。测的是 Agent 管道本身，
                   不受 HTTP 与限流干扰，最干净。
  --target http    打真实服务，走登录 + cookie + 限流。测的是端到端。

    python eval/loadtest.py --n 40 --c 8              # 默认本地
    python eval/loadtest.py --n 40 --c 8 --target http
    python eval/loadtest.py --guard-bench             # 只测护栏开销
    python eval/loadtest.py --n 40 --c 8 --no-cache   # 关缓存看真实成本

注意：本地模式用的是 source="api"，会走缓存。要测"每次都真跑"就加 --no-cache，
否则第二遍开始全是缓存命中，P95 会被漂亮地骗人。
"""
from __future__ import annotations

import argparse
import concurrent.futures as cf
import io
import json
import os
import statistics
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "code" / "agent"))
sys.path.insert(0, str(ROOT / "code"))

# 6 条真实问句，覆盖单值/榜单/趋势/占比/对比/多表。
# 刻意不随机造问题：压测要复现，不能让"这次题目比上次难"影响结论。
QUESTIONS = [
    "11-08 的播放量",
    "2026-11-08 播放量排行榜",
    "最近7天 DAU 的走势",
    "11-08 付费用户占比是多少",
    "11-08 的播放量比 11-01 多了多少",
    "本周新增用户数是多少",
]


def pct(vals: list[float], p: float) -> float:
    if not vals:
        return 0.0
    s = sorted(vals)
    k = min(len(s) - 1, max(0, int(round((p / 100.0) * (len(s) - 1)))))
    return s[k]


def guard_bench(cfg: dict, n: int = 40) -> dict:
    """护栏开销对照：常驻进程 vs 一次性 subprocess。

    这是 AC-16 里最容易被误判的一段——"护栏是 subprocess 所以慢"这句话
    只在"每次起进程"时成立。常驻之后同样的规则、同样的文件，成本差两个数量级。
    """
    import guardtool
    sql = ("SELECT dt, dau FROM ads_daily_overview "
           "WHERE dt = '2026-11-08' LIMIT 10")

    # 常驻
    guardtool.check(sql, cfg)
    t_pool = []
    for _ in range(n):
        t0 = time.perf_counter()
        guardtool.check(sql, cfg)
        t_pool.append((time.perf_counter() - t0) * 1000)

    # 一次性
    cfg2 = dict(cfg)
    cfg2["guard"] = {**cfg["guard"], "serve": False}
    t0 = time.perf_counter()
    guardtool.check(sql, cfg2)
    cold = (time.perf_counter() - t0) * 1000
    t_one = []
    for _ in range(min(n, 10)):
        t0 = time.perf_counter()
        guardtool.check(sql, cfg2)
        t_one.append((time.perf_counter() - t0) * 1000)

    return {
        "pool_p50_ms": round(statistics.median(t_pool), 1),
        "pool_p95_ms": round(pct(t_pool, 95), 1),
        "oneshot_cold_ms": round(cold, 1),
        "oneshot_p50_ms": round(statistics.median(t_one), 1),
        "speedup": round(statistics.median(t_one) / max(0.1, statistics.median(t_pool)), 1),
    }


def run_local(args, cfg: dict) -> dict:
    from governance.identity import provider
    from graph import ask

    if args.no_cache:
        # 只改这一份副本；load_config 有 lru_cache，别污染别的调用方。
        cfg = {**cfg, "cache": {**(cfg.get("cache") or {}), "enabled": False}}
    if args.guard_oneshot:
        cfg = {**cfg, "guard": {**cfg["guard"], "serve": False}}

    idp = provider(cfg)
    ctx = idp.context_for_user(args.user, source="api")
    if ctx is None:
        raise SystemExit("取不到用户 %s 的上下文" % args.user)

    lat: list[float] = []
    errs: list[str] = []
    hits = 0
    tokens = 0

    def one(i: int) -> tuple[float, bool, int]:
        q = QUESTIONS[i % len(QUESTIONS)]
        t0 = time.perf_counter()
        try:
            st = ask(q, cfg, ctx=ctx)
        except Exception as e:                  # 单条崩了要记账，不能中断压测
            return (time.perf_counter() - t0), False, -1
        tok = sum(int((t.get("data") or {}).get("tokens_in") or 0)
                  + int((t.get("data") or {}).get("tokens_out") or 0)
                  for t in (st.get("trace") or []))
        return (time.perf_counter() - t0), bool(st.get("cache_hit")), tok

    t0 = time.perf_counter()
    with cf.ThreadPoolExecutor(max_workers=args.c) as ex:
        futs = [ex.submit(one, i) for i in range(args.n)]
        for f in cf.as_completed(futs):
            try:
                dt, hit, tok = f.result()
            except Exception as e:
                errs.append(str(e)[:100])
                continue
            lat.append(dt)
            hits += 1 if hit else 0
            if tok < 0:
                errs.append("ask 抛异常")
            else:
                tokens += tok
    wall = time.perf_counter() - t0

    return {
        "target": "local", "n": args.n, "concurrency": args.c,
        "wall_sec": round(wall, 1), "qps": round(args.n / wall, 2),
        "p50_ms": round(pct(lat, 50) * 1000), "p95_ms": round(pct(lat, 95) * 1000),
        "p99_ms": round(pct(lat, 99) * 1000),
        "max_ms": round(max(lat) * 1000) if lat else 0,
        "ok": len(lat) - len(errs), "errors": len(errs),
        "error_samples": errs[:3],
        "cache_hits": hits, "cache_hit_rate": round(100.0 * hits / max(1, len(lat)), 1),
        "tokens": tokens,
    }


def run_http(args) -> dict:
    import urllib.request
    import http.cookiejar

    base = args.base.rstrip("/")
    jar = http.cookiejar.CookieJar()
    op = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(jar))

    def post(path: str, payload: dict, timeout=180):
        req = urllib.request.Request(
            base + path, data=json.dumps(payload).encode(),
            headers={"Content-Type": "application/json"}, method="POST")
        with op.open(req, timeout=timeout) as r:
            return json.loads(r.read().decode())

    post("/api/login", {"username": args.user, "password": args.password})

    lat: list[float] = []
    errs: list[str] = []
    hits = 0

    def one(i: int):
        q = QUESTIONS[i % len(QUESTIONS)]
        t0 = time.perf_counter()
        try:
            d = post("/api/ask", {"question": q})
        except Exception as e:
            return (time.perf_counter() - t0), False, str(e)[:80]
        if d.get("error"):
            return (time.perf_counter() - t0), False, str(d["error"])[:80]
        return (time.perf_counter() - t0), bool(d.get("cache_hit")), ""

    t0 = time.perf_counter()
    with cf.ThreadPoolExecutor(max_workers=args.c) as ex:
        futs = [ex.submit(one, i) for i in range(args.n)]
        for f in cf.as_completed(futs):
            dt, hit, err = f.result()
            if err:
                errs.append(err)
                continue
            lat.append(dt)
            hits += 1 if hit else 0
    wall = time.perf_counter() - t0
    return {
        "target": "http:" + base, "n": args.n, "concurrency": args.c,
        "wall_sec": round(wall, 1), "qps": round(args.n / wall, 2),
        "p50_ms": round(pct(lat, 50) * 1000), "p95_ms": round(pct(lat, 95) * 1000),
        "p99_ms": round(pct(lat, 99) * 1000),
        "max_ms": round(max(lat) * 1000) if lat else 0,
        "ok": len(lat), "errors": len(errs), "error_samples": errs[:3],
        "cache_hits": hits, "cache_hit_rate": round(100.0 * hits / max(1, len(lat)), 1),
    }


def render(r: dict) -> str:
    L = ["并发压测 ｜ 目标 %s ｜ %d 次 / %d 并发"
         % (r["target"], r["n"], r["concurrency"]), ""]
    L.append("  总耗时 %.1fs ｜ QPS %.2f ｜ 成功 %d ｜ 失败 %d"
             % (r["wall_sec"], r["qps"], r["ok"], r["errors"]))
    L.append("  延迟 P50 %dms ｜ P95 %dms ｜ P99 %dms ｜ 最大 %dms"
             % (r["p50_ms"], r["p95_ms"], r["p99_ms"], r["max_ms"]))
    L.append("  缓存命中 %d（%.1f%%）" % (r["cache_hits"], r["cache_hit_rate"]))
    if "tokens" in r:
        L.append("  累计 token %d" % r["tokens"])
    for e in r.get("error_samples") or []:
        L.append("  !! %s" % e)
    return "\n".join(L)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--n", type=int, default=24)
    ap.add_argument("--c", type=int, default=8)
    ap.add_argument("--target", choices=["local", "http"], default="local")
    ap.add_argument("--base", default="http://127.0.0.1:8000")
    ap.add_argument("--user", default="analyst")
    ap.add_argument("--password", default="Demo@2026")
    ap.add_argument("--no-cache", action="store_true")
    ap.add_argument("--guard-oneshot", action="store_true",
                    help="护栏改回一次性 subprocess，用于对照测量")
    ap.add_argument("--guard-bench", action="store_true")
    ap.add_argument("--json", default="")
    a = ap.parse_args()

    os.chdir(ROOT)
    out: dict = {}
    if a.guard_bench:
        from conf import load_config
        g = guard_bench(load_config())
        print("护栏开销（同一条 SQL，各 40 次）")
        print("  常驻进程   P50 %.1fms ｜ P95 %.1fms" % (g["pool_p50_ms"], g["pool_p95_ms"]))
        print("  一次性进程 冷启动 %.1fms ｜ P50 %.1fms" % (g["oneshot_cold_ms"], g["oneshot_p50_ms"]))
        print("  提速 %.1fx" % g["speedup"])
        out["guard"] = g

    if a.n > 0:
        from conf import load_config
        r = run_local(a, load_config()) if a.target == "local" else run_http(a)
        print(render(r))
        out["load"] = r

    if a.json:
        io.open(a.json, "w", encoding="utf-8").write(
            json.dumps(out, ensure_ascii=False, indent=1) + "\n")
        print("\n已写入 %s" % a.json)
    return 0


if __name__ == "__main__":
    sys.exit(main())
