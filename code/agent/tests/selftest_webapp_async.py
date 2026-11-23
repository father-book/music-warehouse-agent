#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""问数路由异步化自测（离线：不起服务、不连 DB、不调模型）。

只测三件会真出事的事：
  · 路由确实是 async —— 否则"异步化"只是说法
  · 信号量真的限住了并发 —— 否则线程池还是会被长任务占满
  · 超时真的能返回 —— 线程停不下来，但请求不能挂住
"""
from __future__ import annotations

import asyncio
import inspect
import sys
import time

import _bootstrap  # noqa: F401

import webapp

PASS = FAIL = 0
FAILS: list[str] = []


def expect(cond, name, extra=""):
    global PASS, FAIL
    if cond:
        PASS += 1
        print("  ok   %s" % name)
    else:
        FAIL += 1
        FAILS.append(name)
        print("  FAIL %s %s" % (name, extra))


def test_signature():
    print("\n[1] 路由签名")
    expect(inspect.iscoroutinefunction(webapp.api_ask), "/api/ask 是 async")
    expect(not inspect.iscoroutinefunction(webapp._ask_blocking),
           "实际逻辑仍是同步函数（丢线程池）")
    paths = {r.path for r in webapp.app.routes if getattr(r, "path", "")}
    expect("/api/ask" in paths, "路由已注册")
    # 其余路由刻意保持同步，别在后续改动里被"顺手全改"
    others = [r for r in webapp.app.routes
              if getattr(r, "path", "").startswith("/api/")
              and getattr(r, "path", "") != "/api/ask"
              and hasattr(r, "endpoint")]
    expect(all(not inspect.iscoroutinefunction(r.endpoint) for r in others),
           "其余 /api 路由保持同步（避免漏包阻塞调用卡死循环）",
           "例外：%s" % [r.path for r in others
                         if inspect.iscoroutinefunction(r.endpoint)][:3])


def test_semaphore_reuse():
    print("\n[2] 信号量缓存")

    async def go():
        a = webapp._ask_semaphore(8)
        b = webapp._ask_semaphore(8)
        c = webapp._ask_semaphore(2)
        return a is b, a is c

    same, rebuilt = asyncio.run(go())
    expect(same, "同参数复用同一个信号量")
    expect(not rebuilt, "上限变化时重建")


def test_concurrency_limit():
    print("\n[3] 并发上限真的生效")
    limit = 4
    peak = {"now": 0, "max": 0}
    done = {"n": 0}

    async def worker(i):
        async with webapp._ask_semaphore(limit):
            peak["now"] += 1
            peak["max"] = max(peak["max"], peak["now"])
            await asyncio.sleep(0.02)
            peak["now"] -= 1
            done["n"] += 1

    async def go():
        await asyncio.gather(*[worker(i) for i in range(12)])

    asyncio.run(go())
    expect(done["n"] == 12, "12 个任务全部完成", str(done))
    expect(peak["max"] <= limit, "峰值并发不超过上限 %d" % limit,
           "实测峰值 %d" % peak["max"])
    expect(peak["max"] >= 2, "确实发生了并发（不是串行假象）",
           "实测峰值 %d" % peak["max"])


def test_timeout():
    print("\n[4] 超时能返回")
    # 复刻 api_ask 里的 wait_for + run_in_threadpool 组合：线程停不下来，
    # 但 await 必须立刻抛 TimeoutError，让请求能返回 504 而不是挂住。
    from starlette.concurrency import run_in_threadpool

    def slow():
        time.sleep(0.6)
        return "迟到的结果"

    async def go():
        t0 = time.perf_counter()
        try:
            await asyncio.wait_for(run_in_threadpool(slow), timeout=0.1)
            return False, 0.0
        except asyncio.TimeoutError:
            return True, time.perf_counter() - t0

    hit, cost = asyncio.run(go())
    expect(hit, "超时抛 TimeoutError")
    expect(cost < 0.4, "超时返回值远早于线程结束（%.2fs < 0.6s）" % cost,
           "实测 %.2fs" % cost)


def main():
    test_signature()
    test_semaphore_reuse()
    test_concurrency_limit()
    test_timeout()
    print("\n%d 通过 / %d 失败" % (PASS, FAIL))
    if FAIL:
        print("失败项：" + ", ".join(FAILS))
    return 0 if not FAIL else 1


if __name__ == "__main__":
    sys.exit(main())
