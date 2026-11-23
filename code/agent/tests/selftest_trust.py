"""可信度自测：四项结果自检 + 分级 + 反馈规则。零 LLM，DB 只用于量级复算。

    python code/agent/tests/selftest_trust.py

为什么这块最需要自测
------------------
可信度的错法是**静默的**：判成"高"但实际该低，界面照样绿色，用户照样拿去汇报。
所以每条判据都要有断言钉住，尤其这几个容易想着想着就写错的：

  · 空结果 vs 单值 0：`[[]]`、`[[0]]`、`[]` 三种形态含义完全不同；
  · 量级阈值：把指标人为放大 10 倍，因为相邻日有 ~1% 波动，比值是 9.x 倍，
    阈值定 10 就漏了（实测踩过）；
  · 比率指标：完播率 0.51 不能被"基线太小"挡在门外，否则最该报的一类永远不报；
  · 跳过 ≠ 通过：复算没做成时必须留下 note，不能混进"自检全部通过"。
"""
from __future__ import annotations

import _bootstrap  # noqa: F401

import conf
import trust

PASS, FAIL = "PASS", "FAIL"
rows: list[tuple[str, str, str]] = []


def rec(state: str, name: str, detail: str) -> None:
    rows.append((state, name, detail))
    print("  %-4s %-40s %s" % (state, name, detail))


def _check(name: str, got, want) -> None:
    ok = got == want
    rec(PASS if ok else FAIL, name,
        "%s%s" % (got, "" if ok else "（期望 %s）" % (want,)))


def _res(rows_, cols=None, truncated=False):
    """result 字典。三项自检吃的是它，不是整个 state——
    selftest 第一版就在这里传错过：传 state 进去，`res.get("rows")` 恒为 None，
    所有形态都被判成"空结果"，而断言又恰好写成期望 False，一路绿灯。"""
    return {"columns": cols or ["v"], "rows": rows_,
            "row_count": len(rows_), "truncated": truncated}


def _st(rows_, cols=None, **kw):
    st = {"result": _res(rows_, cols)}
    st.update(kw)
    return st


def _fake_sql(value, calls=None):
    """打桩的 run_sql：只回答"基线窗口那一次"，并记录被调用。"""
    def run(sql, cfg, max_rows=2, trace_id=None):
        if calls is not None:
            calls.append(sql)
        return {"rows": [[value]], "row_count": 1, "elapsed_ms": 1}
    return run


def main() -> int:
    cfg = conf.load_config()
    sql = "SELECT COUNT(*) AS v FROM dwd_play_start WHERE dt = '2026-11-08'"
    base_st = {"applied_sql": sql, "sql": sql,
               "time_start": "2026-11-08", "time_end": "2026-11-08",
               "metrics": ["play_start_cnt"],
               "metric_hits": [{"matched_by": "glossary:播放量"}]}

    print("== 1. 四项自检的形态判定 ==")
    _check("空结果 rows=[] → empty 命中", trust.check_empty(_res([]))["hit"], True)
    _check("空结果 row_count=0 → empty 命中",
           trust.check_empty({"rows": [], "row_count": 0})["hit"], True)
    _check("单值 0 不算空结果", trust.check_empty(_res([[0]]))["hit"], False)
    # [[]] 是"有一行但没列"，仍然什么都算不出来——不能当有数据
    _check("怪形态 [[ ]] 不算有数据", trust.check_empty({"rows": [[]]})["hit"], True)

    _check("截断 → truncated 命中",
           trust.check_truncated(_res([[1]], truncated=True))["hit"], True)

    dom = trust.check_dominance(
        _res([["广东", 9800], ["江苏", 60], ["浙江", 40], ["四川", 13]],
             cols=["province", "play"]), cfg)
    _check("单省占 98.9% → dominance 命中", dom["hit"], True)
    dom2 = trust.check_dominance(
        _res([["广东", 300], ["江苏", 300], ["浙江", 300], ["四川", 100]],
             cols=["province", "play"]), cfg)
    _check("四省均衡 30% → 不误报", dom2["hit"], False)
    dom3 = trust.check_dominance(
        _res([["退款", -900], ["收入", 100], ["其他", 50]], cols=["k", "v"]), cfg)
    _check("含负数（退款）→ 不做占比判定", dom3["hit"], False)
    dom4 = trust.check_dominance(_res([[1], [2]], cols=["v"]), cfg)
    _check("行数 < 3 → 不做判定且留 note", "note" in dom4, True)

    print("\n== 2. 量级自检（打桩复算）==")
    calls: list[str] = []
    t = trust.check_magnitude(dict(base_st, result={"rows": [[12710]]}, metrics=["play_start_cnt"]),
                              cfg, run_sql=_fake_sql(12815, calls))
    _check("正常 12710 vs 12815 → 不报", t["hit"], False)
    _check("复算确实跑了一次", len(calls), 1)
    _check("基线日期平移到 11-07", "2026-11-07" in calls[0], True)

    t = trust.check_magnitude(dict(base_st, result={"rows": [[127100]]}),
                              cfg, run_sql=_fake_sql(12815))
    _check("放大 10 倍（实为 9.9 倍）→ 命中", t["hit"], True)
    _check("  级别为 low", t.get("level"), "low")

    t = trust.check_magnitude(dict(base_st, result={"rows": [[0]]}),
                              cfg, run_sql=_fake_sql(12815))
    _check("跌到 0 → 命中", t["hit"], True)

    # 比率指标：0.51 不能被 min_base 挡住
    ratio_st = dict(base_st, result={"rows": [[0.05]]}, metrics=["complete_rate"])
    t = trust.check_magnitude(ratio_st, cfg, run_sql=_fake_sql(0.51))
    _check("完播率 0.05 vs 0.51 → 命中", t["hit"], True)
    t = trust.check_magnitude(dict(base_st, result={"rows": [[0.5143]]}),
                              cfg, run_sql=_fake_sql(0.5117))
    _check("完播率正常波动 0.5143 vs 0.5117 → 不报", t["hit"], False)
    # 绝对差过小即使比值大也不报：0.001 → 0.006 是 6 倍，但没有业务意义
    t = trust.check_magnitude(dict(base_st, result={"rows": [[0.006]]}),
                              cfg, run_sql=_fake_sql(0.001))
    _check("绝对差 < min_delta → 不报", t["hit"], False)

    # 跳过而不是通过：这些情况必须有 note
    for name, st, why in [
        ("结果非单值", dict(base_st, result={"rows": [[1], [2]]}), "不是单值"),
        ("没有时间窗", dict(base_st, result={"rows": [[1]]}, time_start="", time_end=""), "时间窗"),
        ("SQL 无日期字面量",
         dict(base_st, applied_sql="SELECT COUNT(*) FROM dwd_play_start",
              result={"rows": [[1]]}), "日期字面量"),
    ]:
        t = trust.check_magnitude(st, cfg, run_sql=_fake_sql(1))
        _check("%s → 跳过且留 note" % name,
               bool(t["hit"] is False and t.get("note")), True)

    # SQL 自己在算跨期：平移没意义，必须跳过
    cross = dict(base_st, result={"rows": [[1]]},
                 applied_sql="SELECT COUNT(*) FROM dwd_play_start "
                             "WHERE dt = '2026-11-08' OR dt = '2026-11-01'")
    t = trust.check_magnitude(cross, cfg, run_sql=_fake_sql(1))
    _check("SQL 引用窗口外日期 → 跳过", bool(t.get("note")), True)

    # 复算抛异常不能带崩主链
    def boom(*a, **k):
        raise RuntimeError("db down")
    t = trust.check_magnitude(dict(base_st, result={"rows": [[1]]}), cfg, run_sql=boom)
    _check("复算抛异常 → 跳过不抛", t["hit"], False)

    print("\n== 3. 链路信号 ==")
    sig = trust._signals({"findings": [{"level": "WARN", "rule": "R25", "message": "口径提示"}],
                          "metric_hits": [{"matched_by": "glossary:完播率"}]}, cfg)
    _check("WARN 一条 → 有 medium 信号", len(sig), 1)
    _check("  glossary 命中不算模糊", all(s["code"] != "fuzzy_metric" for s in sig), True)
    sig = trust._signals({"metric_hits": [{"matched_by": "hybrid(bm25+dense)"}]}, cfg)
    _check("全模糊命中 → fuzzy_metric", [s["code"] for s in sig], ["fuzzy_metric"])
    sig = trust._signals({"attempts": 2, "metric_hits": [{"matched_by": "code:x"}]}, cfg)
    _check("返工 2 轮 → many_rounds", [s["code"] for s in sig], ["many_rounds"])
    sig = trust._signals({"shape_unresolved": True,
                          "metric_hits": [{"matched_by": "code:x"}]}, cfg)
    _check("形态未解决 → low 级信号", sig[0]["level"], "low")

    print("\n== 4. 汇总分级与文案 ==")
    t = trust.evaluate(dict(base_st, result={"rows": [[12710]], "row_count": 1}),
                       cfg, run_sql=_fake_sql(12815))
    _check("正常单值 → high", t["level"], "high")
    _check("  score 满分", t["score"], 100)

    t = trust.evaluate(dict(base_st, result={"rows": [], "row_count": 0}),
                       cfg, run_sql=_fake_sql(1))
    _check("空结果 → low", t["level"], "low")

    t = trust.evaluate(_st([["广东", 9800], ["江苏", 60], ["浙江", 40]],
                           cols=["province", "play"], **base_st), cfg,
                       run_sql=_fake_sql(1))
    _check("维度失衡 → medium", t["level"], "medium")

    t = trust.evaluate(dict(base_st, result={"rows": [[12710]]},
                            findings=[{"level": "REJECT", "rule": "R25-口径-完播率分母"}],
                            attempts=2, metric_hits=[{"matched_by": "hybrid"}]),
                       cfg, run_sql=_fake_sql(12815))
    _check("修复 + 返工 + 模糊 → medium", t["level"], "medium")
    _check("  理由条数 ≥ 3", len(t["reasons"]) >= 3, True)

    line = trust.summary_line(t)
    _check("文案含级别与理由", ("可信度：中" in line) and ("返工" in line or "口径" in line), True)
    line = trust.summary_line(trust.evaluate(
        dict(base_st, result={"rows": [[12710]]}), cfg, run_sql=_fake_sql(12815)))
    _check("高位文案说明未做项", "未做" in line, True)

    # 关闭开关后不参与判定
    off = dict(cfg, trust={"enabled": False})
    t = trust.evaluate(dict(base_st, result={"rows": []}), off)
    _check("trust.enabled=false → 直接 high", t["level"], "high")

    ok = sum(1 for s, _, _ in rows if s == PASS)
    print("\n%d/%d 通过" % (ok, len(rows)))
    return 0 if ok == len(rows) else 1


if __name__ == "__main__":
    raise SystemExit(main())
