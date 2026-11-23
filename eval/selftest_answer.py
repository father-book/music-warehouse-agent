"""答案层判据的自测。

为什么判据需要自测
------------------
把判据从"SQL 返回第几列"挪到"答案正文"，等于换了一把尺子。尺子本身
没校准，全绿只是"新的假绿"。这里逐条钉死三件事：

1. **不能假过**：SQL 段里的日期/LIMIT 不能被当成答案内容。若断言 10
   而答案正文只有 `LIMIT 10`，必须判失败——这是最危险的假绿。
2. **不能假红**：`89,250` / `89250` / `+125` / `0.14%` 都该认出来。
3. **锚点有效**：断言「差值 1」时，正文里的「第 1 名」不能把它喂饱。

    python eval/selftest_answer.py
"""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "eval"))

import run as R  # noqa: E402

OK = FAILED = 0


def ck(name: str, cond: bool, extra: str = "") -> None:
    global OK, FAILED
    if cond:
        OK += 1
        print("  ✓ %s" % name)
    else:
        FAILED += 1
        print("  ✗ %s %s" % (name, extra))


ANSWER = """2026-11-02 ~ 2026-11-08 · 播放量

结论：
  · 榜首：song_001，89,250 次，占合计 12.4%
  · 对比：基线 上上周 = 89,125 ｜ 当前 = 89,250 ｜ 差值 +125（+0.14%）

  播放量             89,250
出处：hive_catalog.music_dw.dws_song_play_daily
覆盖：2026-11-02 ~ 2026-11-08（离线数据 2026-10-05 ~ 2026-11-08）
复算：
  SELECT SUM(play_cnt) FROM dws_song_play_daily
  WHERE dt BETWEEN '2026-11-02' AND '2026-11-08' LIMIT 10
"""


def t_body() -> None:
    print("\n[1] 剥掉复算段")
    body = R.answer_body({"answer": ANSWER})
    ck("正文保留结论与结果表", "89,250" in body and "结论" in body)
    ck("复算 SQL 段被剥掉", "SELECT" not in body and "LIMIT 10" not in body)
    ck("复算标记本身不残留", "复算：" not in body)
    ck("无复算段时不报错", R.answer_body({"answer": "就一句话"}) == "就一句话")


def t_tokens() -> None:
    print("\n[2] 数字识别")
    vals = [t["value"] for t in R._num_tokens("89,250 ｜ +125 ｜ -105 ｜ 0.14% ｜ 12.4%")]
    ck("千分位 → 89250", 89250.0 in vals, str(vals))
    ck("带正号 → 125", 125.0 in vals)
    ck("负数 → -105", -105.0 in vals)
    ck("百分数 → 0.14（保留原值）", 0.14 in vals)
    ck("百分号被标记", any(t["pct"] and t["value"] == 0.14
                          for t in R._num_tokens("0.14%")))


def t_assert() -> None:
    print("\n[3] 单条断言")
    body = R.answer_body({"answer": ANSWER})
    cases = [
        ("数值 89250 命中千分位", 89250, True),
        ("数值 89125 是基线也命中", 89125, True),
        ("数值 999 不命中", 999, False),
        ("带符号 +125 命中", "+125", True),
        ("片段「榜首」命中", "榜首", True),
        ("片段「环比增长」不命中", "环比增长", False),
        ("百分数 0.14 命中", 0.14, True),
        ("锚点 差值→125 命中", {"value": 125, "near": ["差值", "相差"]}, True),
        ("锚点 当前 =→89125 不命中（基线在另一侧）",
         {"value": 89_125, "near": "当前 ="}, False),
        ("容差 1 内命中", {"value": 89251, "tol": 1}, True),
        ("容差 0 差 1 不命中", {"value": 89251, "tol": 0}, False),
    ]
    for name, item, want in cases:
        hit, why = R._assert_one(item, body, 0)
        ck(name, hit == want, "→ %s" % why)


def t_no_false_green() -> None:
    print("\n[4] 假绿防护（最关键）")
    body = R.answer_body({"answer": ANSWER})
    # 10 只出现在被剥掉的 LIMIT 段里；2026 只出现在日期里但在正文也有，
    # 用 11 与 08 更干净：它们只作为日期片段大量出现。
    ck("断言 10 不因 LIMIT 10 假过", R._assert_one(10, body, 0)[0] is False)
    ck("断言 2026 即使在日期里也不该被当作业务数字……",
       # 事实上 2026 会命中（日期在正文里）。这里记录真实行为：
       # 日期数字确实存在于正文，所以「断言小整数」本身就要靠锚点约束，
       # 这就是 _derive_must 给差值加 near 的原因。
       R._assert_one({"value": 2026, "near": "差值"}, body, 0)[0] is False)

    # 裸 1 在答案里没有（只有 1 行 "song_001"? 里面有 001 -> 1.0）
    body2 = "第 1 名是 song_001，共 500 次\n差值 +1"
    ck("锚点生效：第 1 名不能喂饱「差值 1」",
       R._assert_one({"value": 1, "near": "差值"}, "第 1 名是 song_001", 0)[0]
       is False)
    ck("同一锚点在有差值时命中",
       R._assert_one({"value": 1, "near": "差值"}, body2, 0)[0] is True)


def t_derive() -> None:
    print("\n[5] 断言派生规则")
    ck("expected → 数值断言",
       R._derive_must({"expected": 3237}) == [{"value": 3237.0}])
    ck("compare_delta → 锚定断言",
       (R._derive_must({"expected": 3237, "compare_delta": 2}) or [])[1]
       == {"value": 2.0, "near": ["差值", "相差", "差距"]})
    ck("expected_min 不派生（答案里没行数）",
       R._derive_must({"expected_min": 5}) is None)
    ck("row_count 不派生", R._derive_must({"expected": 3, "extract": "row_count"}) is None)
    ck("显式 answer_must 优先",
       R._derive_must({"expected": 1, "answer_must": ["自定义"]}) is None)


def t_end_to_end() -> None:
    print("\n[6] 判定融合（模拟一个真实 state）")
    st = {"answer": ANSWER, "result": {"columns": ["v"], "rows": [[89250]],
                                       "row_count": 1, "error": None}}
    case = {"id": "T", "type": "value", "expected": 89250, "compare_delta": 125}
    status, note, d = R.judge(case, st)
    ck("答案对且差值对 → PASS", status == "PASS", note)
    ck("标为答案层判据", d.get("answer_layer") is True)

    # 答案写错数：SQL 结果对，但正文写了 8,925（少一位）
    bad = {"answer": ANSWER.replace("89,250", "8,925"),
           "result": {"columns": ["v"], "rows": [[89250]], "row_count": 1}}
    status, note, d = R.judge(case, bad)
    ck("答案数写错 → FAIL（旧判据会漏）", status == "FAIL", note)

    # 答案对，但 SQL 返回形状变了（多数了一列 song_id）→ 不该变红
    shaped = {"answer": ANSWER,
              "result": {"columns": ["song_id", "v"], "rows": [["s1", 89250]],
                         "row_count": 1}}
    c = dict(case)
    c.pop("compare_delta")
    status, note, d = R.judge(c, shaped)
    ck("结果集多一列不再假红", status == "PASS", note)

    # 对比没复算，答案写了「未自动计算」→ 必须红
    nocmp = {"answer": ANSWER.replace("差值 +125（+0.14%）", "未自动计算"),
             "result": {"columns": ["v"], "rows": [[89250]], "row_count": 1},
             "compare_result": {"status": "skipped", "note": "没日期字面量"}}
    status, note, d = R.judge(case, nocmp)
    ck("没算出差值却写别的 → FAIL", status == "FAIL", note)


def main() -> int:
    t_body()
    t_tokens()
    t_assert()
    t_no_false_green()
    t_derive()
    t_end_to_end()
    print("\n%d 通过 / %d 失败" % (OK, FAILED))
    return 0 if FAILED == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
