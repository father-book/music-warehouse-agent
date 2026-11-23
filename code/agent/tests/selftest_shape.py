"""阶段 3 自测：答案形态契约 + 确定性解读。

    python code/agent/tests/selftest_shape.py      # 全跑（零 LLM、零 DB，毫秒级）

为什么形态逻辑要单独测：
它是"答非所问"的唯一拦截点，而这一类错误**不会**让任何一条护栏报警。
一旦分类规则被改坏，表现是"某些问题永远不返工"或"正常问题被反复打回"——
两种都很难从评测通过率上看出来（前者不报错，后者只是慢一点）。

判据分三层：
  1. classify：问句 → 形状，逐条钉住，防止正则互相吃字（踩过：占比被当差值）
  2. check_result：形状 ↔ 结果，正例必须放过、反例必须命中
  3. insight：解读文案必须能从结果复算，且不越界（ARPU 不能换算成百分比）
"""
from __future__ import annotations

import _bootstrap  # noqa: F401

import sys

import insight
import shape

PASS, FAIL = "PASS", "FAIL"
rows: list[tuple[str, str, str]] = []


def rec(state: str, name: str, detail: str = "") -> None:
    rows.append((state, name, detail))
    print("  %-4s %-38s %s" % (state, name, detail))


def check(name: str, cond: bool, detail: str = "") -> None:
    rec(PASS if cond else FAIL, name, detail if not cond else "")


def res(cols: list[str], data: list[list]) -> dict:
    return {"columns": cols, "rows": data, "row_count": len(data)}


# ---------------------------------------------------------------- 1. 分类
CLASSIFY = [
    # (问句, 期望形状)
    ("2026-11-08 的 DAU 是多少", "scalar"),
    ("11-08 的播放量", "scalar"),
    ("昨天播放量是多少", "scalar"),
    ("最近7天的播放量", "scalar"),                     # 窗口总量 ≠ 趋势
    ("上上周的播放量", "scalar"),
    # 差值必须在「占比 / 比例」被抠掉之后判，否则"占比是多少"会被 `比.{0,16}多` 吃掉
    ("2026-11-08 的 DAU 比 11-07 多了多少", "comparison"),
    ("11-08 的播放量比 11-07 少了多少", "comparison"),
    ("11-08 付费用户占比是多少", "ratio"),
    ("11-08 和 11-07 的播放量相差多少", "comparison"),
    # 趋势：必须有"变化/走势"或"逐日/每天"，否则是窗口总量
    ("最近7天 DAU 的走势", "trend"),
    ("11-01 到 11-08 每天的收入是多少", "trend"),
    # 榜单：显式名次词最强；「前五的歌」这种省略量词也要认
    ("2026-11-08 播放量排行榜", "ranking"),
    ("播放量前五的歌", "ranking"),
    ("11-08 播放量前三名是哪几首歌", "ranking"),
    ("11-08 DAU 排名", "ranking"),
    # 最值带实体 → 仍只有 1 行，但语义是"谁"
    ("播放量最高的那首歌有多少次播放", "argmax"),
    ("昨天消费最多的用户是谁", "argmax"),
    ("完播率最高的曲风的完播率是多少", "argmax"),
    # 分组
    ("各曲风的播放量", "distribution"),
    ("各曲风的播放量占比", "distribution"),           # 分组 + 占比 → 多行，不是单值 ratio
    ("各省份的活跃用户数", "distribution"),
    # 比率/均值
    ("11-08 完播率", "ratio"),
    ("11-08 人均播放次数", "ratio"),
    ("11-08 ARPU", "ratio"),
    # 明细
    ("11-08 有哪些用户启动了 App", "detail"),
    # 兜底
    ("这周怎么样", "scalar"),
]


def t_classify() -> None:
    print("-- 形态分类 --")
    bad = 0
    for q, want in CLASSIFY:
        got = shape.classify(q).shape
        if got != want:
            bad += 1
            rec(FAIL, "分类：" + q[:22], "期望 %s，实际 %s" % (want, got))
    check("分类 %d 条全部命中" % len(CLASSIFY), bad == 0,
          "%d 条不符" % bad)


def t_contract_fields() -> None:
    print("-- 契约字段 --")
    r = shape.classify("播放量前五的歌")
    check("榜单下限随 N 抬高（前五 → ≥5 行）", r.min_rows == 5, str(r.min_rows))
    check("榜单需要实体+指标两列", r.min_cols == 2, str(r.min_cols))
    r2 = shape.classify("播放量排行榜")
    check("无 N 的榜单仍需 ≥2 行", r2.min_rows == 2, str(r2.min_rows))
    r3 = shape.classify("昨天消费最多的用户是谁")
    check("最值单选题目只允许 1 行", r3.max_rows == 1, str(r3.max_rows))
    check("scalar 契约不往 prompt 里塞骨架",
          shape.contract_block(shape.classify("11-08 的播放量")) == "")


# ---------------------------------------------------------------- 2. 结果回比
def t_check_result() -> None:
    print("-- 结果形状回比 --")
    rank = shape.classify("播放量排行榜")

    # 正例：真榜单，必须放过
    ok_rows = res(["song_name", "v"], [["A", 264], ["B", 166], ["C", 164]])
    check("榜单：3 行正常结果不报警", shape.check_result(rank, ok_rows) == [])

    # 反例：这是「播放量排行榜」那个真实 bug——SUM 出来一行
    bad_rows = res(["v"], [[12710]])
    issues = shape.check_result(rank, bad_rows)
    check("榜单：1 行合计值被判形态不符", len(issues) >= 2,
          "；".join(issues)[:70])

    trend = shape.classify("最近7天 DAU 的走势")
    check("趋势：7 个点不报警",
          shape.check_result(trend, res(["dt", "dau"],
                                        [["2026-11-02", 3235], ["2026-11-03", 3239]])) == [])
    check("趋势：只有 1 个点要返工",
          len(shape.check_result(trend, res(["dt", "dau"], [["2026-11-02", 3235]]))) == 1)

    comp = shape.classify("11-08 的 DAU 比 11-07 多了多少")
    check("对比：单值不报警", shape.check_result(comp, res(["diff"], [[2]])) == [])
    check("对比：返回两行明细要返工",
          len(shape.check_result(comp, res(["dt", "dau"],
                                           [["11-07", 3235], ["11-08", 3237]]))) == 1)

    ratio = shape.classify("付费用户占比是多少")
    check("占比：0.2799 不报警", shape.check_result(ratio, res(["r"], [[0.2799]])) == [])
    check("占比：27.99 也不报警（百分数表示合法）",
          shape.check_result(ratio, res(["r"], [[27.99]])) == [])
    check("占比：978 被判超值域",
          len(shape.check_result(ratio, res(["r"], [[978.0]]))) == 1)

    argm = shape.classify("播放量最高的那首歌有多少次播放")
    check("最值：1 行不报警", shape.check_result(argm, res(["name", "v"], [["A", 264]])) == [])
    check("最值：30 行判未收敛",
          len(shape.check_result(argm, res(["name", "v"],
                                           [["S%d" % i, 100 - i] for i in range(30)]))) == 1)

    dist = shape.classify("各曲风的播放量")
    check("分布：单组要返工",
          len(shape.check_result(dist, res(["genre", "v"], [["流行", 7372]]))) == 1)

    # 空结果 / 全 NULL 不应被当成"形状不符"去返工，那是数据问题
    check("空结果不返工", shape.check_result(rank, res(["a", "b"], [])) == [])
    null_issues = shape.check_result(rank, res(["a", "b"], [[None, None], [None, None]]))
    check("全 NULL 判为无效结果", len(null_issues) == 1, "；".join(null_issues)[:60])


# ---------------------------------------------------------------- 3. 解读
def t_insight() -> None:
    print("-- 确定性解读 --")
    rank = shape.classify("播放量排行榜")
    r = res(["song_id", "song_name", "v"],
            [["S1", "半島鐵盒", 264], ["S2", "家", 166], ["S3", "时间", 164]])
    out = insight.build(r, rank)
    joined = " ".join(out)
    check("榜单解读取可读名而非 id", "半島鐵盒" in joined, joined[:60])
    check("榜单解读给出榜首占比", "占所选 3 项合计" in joined, joined[:80])
    check("榜单解读给出与第二名的差距", "比第二名" in joined, joined[:120])

    trend = shape.classify("最近7天 DAU 的走势")
    r2 = res(["dt", "dau"], [["2026-11-02", 3200], ["2026-11-05", 3250],
                             ["2026-11-08", 3237]])
    out2 = insight.build(r2, trend)
    check("趋势解读标出峰值日", "2026-11-05" in " ".join(out2), " ".join(out2)[:80])
    check("趋势解读给出首末变化", "首日" in " ".join(out2), " ".join(out2)[:120])

    ratio = shape.classify("付费用户占比是多少")
    out3 = insight.build(res(["r"], [[0.2799]]), ratio)
    check("占比换算成百分点", "27.99%" in " ".join(out3), " ".join(out3))
    # ARPU 是元/人，不是比例；换算成百分比就是错的
    check("非比例值不换算百分比",
          insight.build(res(["r"], [[7.47]]), ratio) == [])

    check("单值无解读（不给废话）", insight.build(res(["v"], [[3237]]),
                                                  shape.classify("DAU 是多少")) == [])


def t_critique_flags() -> None:
    """critique 每一次返回都必须显式写 shape_retry_now。

    这条不是形式主义。`shape_retry_now` 曾经只在"发起返工"的分支里写 True，
    其余分支不写——而 LangGraph 的 state 是**合并**而不是覆盖，
    上一轮的 True 会留到下一轮，`after_critique` 于是永远判"回 generate"，
    图上死循环直到 RecursionError。

    这个 bug 在 122 题评测里**全绿**：因为那一轮没有一道题触发返工，
    返工分支压根没被执行过。"没测到"和"没坏"是两件事。
    """
    import node_critique

    cfg = {"generator": {"mode": "llm"},
           "limits": {"max_attempts": 3, "shape_retries": 1}}

    cases = [
        ("拒答", {"refused": True, "question": "x"}),
        ("stub 模式", {"question": "x", "result": {"rows": [[1]], "columns": ["a"]}}),
        ("调试入口", {"question": "x", "sql": "SELECT 1",
                  "result": {"rows": [[1]], "columns": ["a"]}}),
        ("形态通过", {"question": "11-08 的播放量",
                  "result": {"rows": [[12710]], "columns": ["v"], "row_count": 1}}),
        ("形态不符", {"question": "播放量排行榜",
                  "result": {"rows": [[12710]], "columns": ["v"], "row_count": 1}}),
    ]
    bad = 0
    for name, st in cases:
        use = dict(cfg)
        if name == "stub 模式":
            use = {"generator": {"mode": "stub"}, "limits": cfg["limits"]}
        out = node_critique.critique(st, use)
        if "shape_retry_now" not in out:
            bad += 1
            rec(FAIL, "critique 缺标志：" + name, "返回里没有 shape_retry_now")
        elif out["shape_retry_now"] is not False and name != "形态不符":
            bad += 1
            rec(FAIL, "critique 误置返工：" + name, str(out.get("shape_retry_now")))
    check("critique 5 个分支都显式带返工标志", bad == 0, "%d 个分支不符" % bad)

    # 能否被返工：形态不符 + 有余量 → True；上限用尽 → False
    st = {"question": "播放量排行榜", "attempts": 1,
          "result": {"rows": [[12710]], "columns": ["v"], "row_count": 1}}
    out = node_critique.critique(st, cfg)
    check("形态不符时发起返工", out.get("shape_retry_now") is True)
    st2 = dict(st, shape_retries=1)
    out2 = node_critique.critique(st2, cfg)
    check("返工上限用尽后不再返工", out2.get("shape_retry_now") is False
          and out2.get("shape_unresolved") is True)


def main() -> int:
    t_classify()
    t_contract_fields()
    t_check_result()
    t_insight()
    t_critique_flags()
    bad = [r for r in rows if r[0] == FAIL]
    print("\n%d/%d 通过" % (len(rows) - len(bad), len(rows)))
    if bad:
        print("\n失败明细：")
        for _, n, d in bad:
            print("  - %s：%s" % (n, d))
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())
