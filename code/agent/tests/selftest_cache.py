"""阶段 4 自测：答案缓存 + 对比基线的 SQL 平移。零 LLM、零 DB。

    python code/agent/tests/selftest_cache.py

这两块的共同点是「错了也不会报错」：
  · 缓存键漏了 policy_hash → 受限用户拿到全量结果，SQL 一条都没越权；
  · 缓存键漏了语义层指纹 → 改完口径还是旧数字，人会以为改动没生效；
  · 基线 SQL 平移错一天 → 差值算出来是另一个数，格式完全正常。
所以每条安全边界都要有断言钉住，不靠"看起来对"。
"""
from __future__ import annotations

import _bootstrap  # noqa: F401

import sys
import threading
import time

import answercache
import node_execute
import node_validate

PASS, FAIL = "PASS", "FAIL"
rows: list[tuple[str, str, str]] = []


def rec(state: str, name: str, detail: str) -> None:
    rows.append((state, name, detail))
    print("  %-4s %-34s %s" % (state, name, detail))


def _check(name: str, got, want) -> None:
    ok = got == want
    rec(PASS if ok else FAIL, name,
        "%s%s" % (got, "" if ok else "（期望 %s）" % (want,)))


class _D:
    """假的 PolicyDecision：只需要 policy_hash。"""
    def __init__(self, h: str):
        self.policy_hash = h


def _cfg() -> dict:
    return {
        "generator": {"mode": "llm", "model": "deepseek-chat", "temperature": 0},
        "guard": {"mode": "query", "strict_warn": False},
        "lane": {"offline": {"data_until": "2026-11-08"}},
        "clock": {"mode": "fixed", "fixed_today": "2026-11-09"},
        "_paths": {},
    }


# ---------------------------------------------------------------- 归一化与键
def t_norm() -> None:
    a = answercache._norm("11-08 的播放量？")
    b = answercache._norm("11-08的播放量")
    _check("空白与标点归一化", a, b)
    c = answercache._norm("11-08 的 DAU")
    _check("英文大小写归一化", c, answercache._norm("11-08 的 dau"))
    _check("不同问题归一化后仍不同",
           answercache._norm("播放量") == answercache._norm("DAU"), False)


def t_key_isolation() -> None:
    cfg = _cfg()
    k1 = answercache.make_key("11-08 的播放量", cfg, _D("hash-A"))
    k2 = answercache.make_key("11-08的播放量", cfg, _D("hash-A"))
    k3 = answercache.make_key("11-08 的播放量", cfg, _D("hash-B"))
    _check("同问同身份 → 同键", k1 == k2, True)
    _check("身份不同 → 不同键（防越权串号）", k1 == k3, False)

    # 语义层指纹变化必须让旧键失效
    old = answercache._FP
    try:
        answercache._FP = "fp-v1"
        a = answercache.make_key("11-08 的播放量", cfg, _D("hash-A"))
        answercache._FP = "fp-v2"
        b = answercache.make_key("11-08 的播放量", cfg, _D("hash-A"))
        _check("语义层指纹变化 → 键变化", a == b, False)
    finally:
        answercache._FP = old

    # 生成器配置变化也要失效（换模型/换模式）
    cfg2 = {**cfg, "generator": {**cfg["generator"], "model": "other"}}
    _check("换模型 → 键变化",
           answercache.make_key("11-08 的播放量", cfg, _D("h"))
           == answercache.make_key("11-08 的播放量", cfg2, _D("h")), False)


# ---------------------------------------------------------------- 缓存本体
def t_cache_basic() -> None:
    c = answercache.Cache(ttl_sec=60, maxsize=2)
    st = {"question": "q", "answer": "a", "sql": "SELECT 1",
          "result": {"rows": [[1]]}, "trace": [{"node": "generate"}],
          "ctx": {"secret": "should-not-be-stored"}}

    _check("未命中返回 None", c.get("k"), None)
    c.put("k", st)
    hit = c.get("k")
    _check("命中后答案一致", (hit or {}).get("answer"), "a")
    _check("敏感字段不进缓存", "ctx" in (hit or {}), False)
    _check("trace 不进缓存（每次要重新生成）", "trace" in (hit or {}), False)

    # 深拷贝：调用方改返回值不能污染缓存
    hit["answer"] = "被改了"
    _check("返回值是深拷贝", c.get("k")["answer"], "a")


def t_cache_ttl_and_lru() -> None:
    c = answercache.Cache(ttl_sec=0.05, maxsize=2)
    c.put("k", {"answer": "a"})
    time.sleep(0.08)
    _check("TTL 过期后不命中", c.get("k"), None)

    c2 = answercache.Cache(ttl_sec=60, maxsize=2)
    for k in ("a", "b", "c"):
        c2.put(k, {"answer": k})
    _check("超容量淘汰最旧", c2.get("a"), None)
    _check("新条目仍在", (c2.get("c") or {}).get("answer"), "c")
    _check("统计里有淘汰计数", c2.stats()["evict"] >= 1, True)


def t_cacheable() -> None:
    class C:
        def __init__(self, s): self.source = s

    ok = {"answer": "a", "sql": "SELECT 1", "result": {"rows": [[1]]}}
    _check("web 成功答案可缓存", answercache.cacheable(ok, C("web")), True)
    _check("api 可缓存", answercache.cacheable(ok, C("api")), True)
    _check("eval 不缓存（会污染成本统计）",
           answercache.cacheable(ok, C("eval")), False)
    _check("cli 不缓存", answercache.cacheable(ok, C("cli")), False)
    _check("安全拦截不缓存",
           answercache.cacheable({**ok, "security_block": True}, C("web")), False)
    _check("执行报错不缓存",
           answercache.cacheable({**ok, "result": {"error": "boom"}}, C("web")), False)
    _check("能力缺口不缓存",
           answercache.cacheable({**ok, "unsupported": "没有这张表"}, C("web")), False)
    _check("空答案不缓存",
           answercache.cacheable({**ok, "answer": "  "}, C("web")), False)


def t_single_flight_lock() -> None:
    c = answercache.Cache()
    l1 = c.key_lock("same")
    l2 = c.key_lock("same")
    l3 = c.key_lock("other")
    _check("同键返回同一把锁", l1 is l2, True)
    _check("不同键不同锁", l1 is l3, False)

    # 真并发下同键串行：计数必须正好等于 1
    hits = {"n": 0}
    c2 = answercache.Cache()

    def work():
        with c2.key_lock("k"):
            if c2.get("k") is None:
                time.sleep(0.02)
                hits["n"] += 1
                c2.put("k", {"answer": "a"})

    ts = [threading.Thread(target=work) for _ in range(8)]
    for t in ts:
        t.start()
    for t in ts:
        t.join()
    _check("八路并发只跑一次", hits["n"], 1)


# ---------------------------------------------------------------- 基线 SQL 平移
def t_shift_sql() -> None:
    sql = "SELECT COUNT(*) FROM dwd_play_start WHERE dt = '2026-11-08'"
    out, err = node_execute._shift_sql(sql, ("2026-11-08", "2026-11-08"),
                                       ("2026-11-01", "2026-11-01"))
    _check("单日字面量平移", ("2026-11-01" in out, err), (True, ""))

    sql = ("SELECT dt, COUNT(*) FROM dwd_play_start "
           "WHERE dt BETWEEN '2026-11-02' AND '2026-11-08' GROUP BY dt")
    out, err = node_execute._shift_sql(sql, ("2026-11-02", "2026-11-08"),
                                       ("2026-10-26", "2026-11-01"))
    _check("区间平移（两端都动）",
           ("'2026-10-26'" in out and "'2026-11-01'" in out, err), (True, ""))

    # 没有日期字面量 → 不能猜，必须明确拒绝
    out, err = node_execute._shift_sql(
        "SELECT COUNT(*) FROM dwd_play_start WHERE dt = CURRENT_DATE",
        ("2026-11-08", "2026-11-08"), ("2026-11-01", "2026-11-01"))
    _check("无需平移就拒绝", (out, bool(err)), ("", True))

    # 长度不同 → 不做
    out, err = node_execute._shift_sql(
        "SELECT COUNT(*) FROM t WHERE dt = '2026-11-08'",
        ("2026-11-08", "2026-11-08"), ("2026-10-01", "2026-10-31"))
    _check("长度不同拒绝平移", (out, bool(err)), ("", True))

    # 平移只动落在当前窗口内的日期，别的日期不许碰
    sql = ("SELECT * FROM t WHERE dt = '2026-11-08' "
           "AND created_at = '2026-10-01'")
    out, _ = node_execute._shift_sql(sql, ("2026-11-08", "2026-11-08"),
                                     ("2026-11-01", "2026-11-01"))
    _check("窗口外日期不被改写", "'2026-10-01'" in out, True)


def t_compare_violation() -> None:
    """对比题里 SQL 引用基线日期 → 必须打回重写，不能让它自己算差值。"""
    def st(sql, start="2026-11-08", base="2026-11-01", auto=True):
        return {
            "sql": sql, "time_start": start, "time_end": start,
            "time_window": {"compare": {"start": base, "end": base, "auto": auto}},
        }

    v = node_validate._compare_violation(st(
        "SELECT (SELECT COUNT(*) FROM t WHERE dt='2026-11-08') "
        "- (SELECT COUNT(*) FROM t WHERE dt='2026-11-01')"))
    _check("引用了基线日期 → 打回", (v or {}).get("rule"), "COMPARE-BASE")

    v = node_validate._compare_violation(st(
        "SELECT COUNT(*) FROM t WHERE dt='2026-11-08'"))
    _check("只查当前窗口 → 放行", v, None)

    # 基线不可自动比较时不该拦（本来就不复算）
    v = node_validate._compare_violation(st(
        "SELECT COUNT(*) FROM t WHERE dt IN ('2026-11-08','2026-11-01')", auto=False))
    _check("auto=False 不拦", v, None)


def t_scalar() -> None:
    _check("单值可取", node_execute._scalar({"rows": [[12710]]}), 12710.0)
    _check("多行不取", node_execute._scalar({"rows": [[1], [2]]}), None)
    _check("多列不取", node_execute._scalar({"rows": [[1, 2]]}), None)
    _check("带千分位字符串可取",
           node_execute._scalar({"rows": [["12,710"]]}), 12710.0)
    _check("NULL 不取", node_execute._scalar({"rows": [[None]]}), None)


def main() -> int:
    print("== 阶段 4 自测：缓存 / 对比基线 ==")
    print("-- 键与归一化 --")
    for fn in (t_norm, t_key_isolation):
        fn()
    print("-- 缓存本体 --")
    for fn in (t_cache_basic, t_cache_ttl_and_lru, t_cacheable, t_single_flight_lock):
        fn()
    print("-- 基线 SQL 平移 --")
    for fn in (t_shift_sql, t_scalar, t_compare_violation):
        fn()

    bad = [r for r in rows if r[0] == FAIL]
    print("\n%d/%d 通过" % (len(rows) - len(bad), len(rows)))
    if bad:
        print("\n失败明细：")
        for _, n, d in bad:
            print("  - %s：%s" % (n, d))
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())
