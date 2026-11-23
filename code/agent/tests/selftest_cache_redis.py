"""Redis 缓存后端的验收（AC-17）。

进程内缓存的自测在 selftest_cache.py 里（40 条）。这里只回答四个
"换成 Redis 才可能出现"的问题：

1. **真的跨进程共享吗**——两个独立解释器，A 写 B 读得到。
   （进程内后端跑这条会失败，正好证明测试有区分力。）
2. **跨实例单飞真的合并了吗**——两个进程同时冷启动同一个问题，
   真正"计算"的次数必须是 1。这条最难，也最能证明 `SET NX PX` 那套没白写。
3. **降级可见吗**——连不上 Redis 时回落 local，但必须报 degraded + 原因。
   静默降级是最坏的结果：演示时看不出来，生产上是缓存穿透。
4. **旧 schema 的值会被当成未命中吗**——SCHEMA_V 改了以后老值不能回放，
   否则答案会突然少一段（字段缺了）。

    python code/agent/tests/selftest_cache_redis.py
"""
from __future__ import annotations

import _bootstrap  # noqa: F401

import copy
import os
import subprocess
import sys
import tempfile
import time
from pathlib import Path

from _bootstrap import HERE  # noqa: E402
sys.path.insert(0, str(HERE))

import answercache as A  # noqa: E402
from conf import load_config  # noqa: E402

OK = FAILED = 0


def ck(name: str, cond: bool, extra: str = "") -> None:
    global OK, FAILED
    if cond:
        OK += 1
        print("  ✓ %s" % name)
    else:
        FAILED += 1
        print("  ✗ %s %s" % (name, extra))


WORKER = '''
import os, sys, time
sys.path.insert(0, r"{agent}")
import answercache as A
from conf import load_config
key, marker = sys.argv[1], sys.argv[2]
cfg = load_config()
c = A.get_cache(cfg)
with c.single_flight(key):
    # 和 graph.ask 完全一样的顺序：先看有没有，没有才算
    if c.get(key) is None:
        with open(marker, "a", encoding="utf-8") as f:
            f.write("%d\\n" % os.getpid())
        time.sleep({hold})          # 模拟"调模型 + 查库"这段耗时
        c.put(key, {{"answer": "computed", "sql": "SELECT 1"}})
'''


def _cache(cfg):
    A.reset_cache()
    c = A.get_cache(cfg)
    return c


def t_backend(cfg) -> None:
    print("\n[1] 后端选择")
    c = _cache(cfg)
    want = (cfg.get("cache") or {}).get("backend", "local")
    ck("按配置选中后端（%s）" % want, c.name == want, "实得 %s" % c.name)
    ck("PING 通", c.ping() if hasattr(c, "ping") else True)
    s = c.stats()
    ck("stats 带 backend 字段", s.get("backend") == c.name)
    ck("未降级", s.get("degraded") is False, str(s.get("reason")))


def t_roundtrip(cfg) -> None:
    print("\n[2] 读写与大值压缩")
    c = _cache(cfg)
    c.clear()
    st = {"answer": "答案" * 10, "sql": "SELECT 1",
          "compare_result": {"status": "ok", "delta": 125}}
    c.put("u:1", st)
    got = c.get("u:1") or {}
    ck("取回内容一致", got.get("answer") == st["answer"])
    ck("嵌套 dict 完整", (got.get("compare_result") or {}).get("delta") == 125)
    ck("缺失字段不会凭空出现", "ctx" not in got)

    big = {"answer": "长答案" * 5000, "sql": "SELECT 1"}   # ~60KB
    c.put("u:big", big)
    ck("大答案往返完整", (c.get("u:big") or {}).get("answer") == big["answer"])
    raw_len = len(A._dump({k: big.get(k) for k in A._KEEP if k in big}))
    ck("大答案被压缩（dump < 原文 40%%）", raw_len < len(big["answer"].encode()) * 0.4,
       "%d bytes" % raw_len)

    ck("不存在的 key 返回 None", c.get("u:nope") is None)


def t_key_layout(cfg) -> None:
    print("\n[3] key 布局")
    c = _cache(cfg)
    rc = cfg["cache"]["redis"]
    k = c._key("abc") if hasattr(c, "_key") else ""
    ck("含环境前缀", k.startswith("%s:%s:" % (rc.get("prefix"), rc.get("env"))), k)
    ck("含 schema 版本 v%d" % A.SCHEMA_V, (":v%d" % A.SCHEMA_V) in k, k)
    ck("锁 key 与数据 key 不同命名空间",
       c._lock_key("abc") != k and ":lock:" in c._lock_key("abc"))


def t_cross_process(cfg) -> None:
    print("\n[4] 跨进程共享（进程内后端在这里必然失败）")
    c = _cache(cfg)
    c.clear()
    tmp = Path(tempfile.mkdtemp(prefix="agc_"))
    worker = tmp / "w.py"
    worker.write_text(WORKER.format(agent=str(HERE), hold=0.2), encoding="utf-8")

    k1 = "x:share"
    subprocess.run([sys.executable, str(worker), k1, str(tmp / "m1.txt")],
                   capture_output=True, timeout=60)
    got = c.get(k1)
    ck("A 进程写入的值，主进程能读到", (got or {}).get("answer") == "computed")

    # B 进程：此时缓存已有，不该再计算
    subprocess.run([sys.executable, str(worker), k1, str(tmp / "m2.txt")],
                   capture_output=True, timeout=60)
    m2 = (tmp / "m2.txt")
    ck("B 进程直接命中、没有重复计算",
       (not m2.exists()) or m2.read_text(encoding="utf-8").strip() == "")


def t_single_flight(cfg) -> None:
    print("\n[5] 跨实例单飞")
    c = _cache(cfg)
    c.clear()
    tmp = Path(tempfile.mkdtemp(prefix="agc_"))
    worker = tmp / "w.py"
    worker.write_text(WORKER.format(agent=str(HERE), hold=3.0), encoding="utf-8")
    marker = tmp / "hits.txt"

    ps = [subprocess.Popen([sys.executable, str(worker), "x:sf", str(marker)],
                           stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
          for _ in range(2)]
    t0 = time.time()
    for p in ps:
        p.wait(timeout=90)
    dt = time.time() - t0

    lines = [x for x in marker.read_text(encoding="utf-8").splitlines() if x.strip()] \
        if marker.exists() else []
    ck("两个进程只计算了一次", len(lines) == 1, "实际 %d 次：%s" % (len(lines), lines))
    ck("等待方没有自己再算一遍（总耗时 ≈ 一次计算）", dt < 6.0, "%.1fs" % dt)
    ck("缓存里有结果", bool(c.get("x:sf")))


def t_stats_global(cfg) -> None:
    print("\n[7] 命中率跨实例聚合")
    c = _cache(cfg)
    c.clear()
    for k in ("hit", "miss", "put"):
        c.r.delete(c._skey(k))
    c.get("st:none")                        # 本实例 miss
    c.put("st:one", {"answer": "x"})        # 本实例 put

    # 关键：让**另一个进程**命中。如果命中率只统计本地，
    # 主进程这边会显示 0%——那正是多实例下运维看到的假象。
    code = ("import sys; sys.path.insert(0, %r)\n"
            "import answercache as A\n"
            "from conf import load_config\n"
            "c = A.get_cache(load_config())\n"
            "sys.exit(0 if c.get('st:one') else 1)\n" % str(HERE))
    r = subprocess.run([sys.executable, "-c", code], capture_output=True, timeout=60)
    ck("另一个进程确实命中了", r.returncode == 0,
       (r.stderr or b"").decode("utf-8", "replace")[-120:])

    s = c.stats()
    ck("全局 hit 计到 1（含别的进程）", s.get("hit") == 1, str(s.get("hit")))
    ck("全局 miss 计到 1", s.get("miss") == 1, str(s.get("miss")))
    ck("命中率 50%", s.get("hit_rate") == 50.0, str(s.get("hit_rate")))
    ck("本实例 hit 仍为 0 —— 证明确实不是本地计数",
       s.get("local_hit") == 0, str(s.get("local_hit")))


def t_degrade(cfg) -> None:
    print("\n[8] 降级可见（这条最容易被写成静默）")
    bad = copy.deepcopy(cfg)
    bad["cache"]["backend"] = "redis"
    bad["cache"]["redis"]["port"] = 6399          # 没人监听
    A.reset_cache()
    c = A.get_cache(bad)
    ck("连不上时回落到 local", c.name == "local", c.name)
    s = c.stats()
    warn = s.get("init_warning") or (A.stats() or {}).get("init_warning") or ""
    ck("stats 里带出了回落原因", "Redis 不可用" in warn, warn[:80])
    c.put("d:1", {"answer": "x"})
    ck("降级后仍可读写（服务不中断）", (c.get("d:1") or {}).get("answer") == "x")


def t_runtime_degrade(cfg) -> None:
    print("\n[9] 运行中 Redis 挂掉：必须降级，不能把请求打死")
    # 守的是一个真实发生过的 bug：Redis 停止后 webapp 每个请求都返回
    #   {"error":"TimeoutError: timed out"}，只用了 2 秒（正是 Redis 的超时值）。
    # 明明有进程内降级可走，用户却看到报错。根因是 socket 超时以裸 TimeoutError
    # 冒出来，而缓存层写的是 `except RedisError` —— 类型对不上就接不住。
    # 第 [8] 段只验了"启动时连不上"，验不到"跑着跑着挂掉"。
    import redisclient
    dead = {"host": "127.0.0.1", "port": 6399, "password_env": "",
            "timeout": 0.5, "prefix": "agc", "env": "test"}

    # 1) 客户端层：连接故障必须包成 RedisError
    try:
        redisclient.Redis(host="127.0.0.1", port=6399, timeout=0.5).get("x")
        ck("客户端把连接故障包成 RedisError", False, "居然没抛")
    except redisclient.RedisError:
        ck("客户端把连接故障包成 RedisError", True)
    except Exception as e:
        ck("客户端把连接故障包成 RedisError", False,
           "抛的是 %s（缓存层接不住）" % type(e).__name__)

    # 2) 缓存层：读要返回未命中，不能抛
    rc = A.RedisCache(60, 8, dead)
    ck("运行期故障时 get 返回未命中而不是抛", rc.get("k") is None, repr(rc.get("k")))

    # 3) 写要落到 fallback，服务不中断
    rc.put("k", {"answer": "fallback-ok"})
    ck("运行期故障时 put 落到 fallback",
       (rc.get("k") or {}).get("answer") == "fallback-ok")

    # 4) 故障要可观测，否则"降级了"没人知道
    ck("降级状态被记下来（可观测）", bool(rc._down), str(getattr(rc, "_down", None)))


def t_bad_value(cfg) -> None:
    print("\n[6] 坏值不粘锅")
    c = _cache(cfg)
    c.clear()
    c.r.set(c._key("bad"), b"J{not json", ex=60)
    ck("解不开的值当未命中", c.get("bad") is None)
    ck("坏值被删掉，不会每次都白读",
       c.get("bad") is None and not c.r.exists(c._key("bad")))

    c.r.set(c._key("bad2"), b"\x1fZgarbage", ex=60)
    ck("gzip 头但内容坏了也不崩", c.get("bad2") is None)


def main() -> int:
    cfg = load_config()
    if (cfg.get("cache") or {}).get("backend") != "redis":
        print("config.cache.backend 不是 redis，跳过（本自测只验 Redis 后端）")
        return 0
    t_backend(cfg)
    t_roundtrip(cfg)
    t_key_layout(cfg)
    t_cross_process(cfg)
    t_single_flight(cfg)
    t_bad_value(cfg)
    t_stats_global(cfg)
    t_degrade(cfg)
    t_runtime_degrade(cfg)
    A.reset_cache()
    _cache(cfg).clear()
    print("\n%d 通过 / %d 失败" % (OK, FAILED))
    return 0 if FAILED == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
