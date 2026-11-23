"""sqlconsist 自测：基线 SQL 与当前 SQL 的口径一致性。

为什么这段必须测
----------------
它决定"对比题给不给差值"。判太松 → 差值是错的，但看起来跟对的一样；
判太严 → 所有对比题都变成"不给差值"，功能等于废了。两头都是静默的错误，
只有测才能知道落在哪。

测法上有两条讲究：

1. **不只测 check() 本身，还要测它和 `_shift_sql` 的配合**。
   平移器生成的基线 SQL 必须能通过这套检查——否则线上一片 unsafe，
   等于把功能关掉了还看不出来。第 6 段专门做这件事。
2. **期望值是"这个 SQL 该不该信"，不是"函数返回了什么"**。
   所以用例名写成人的判断（"紧凑日期未平移 → 不可信"），而不是状态码。

用法：python code/agent/tests/selftest_consist.py
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import _bootstrap  # noqa: F401
import sqlconsist as S
from node_execute import _shift_sql, _strip_limit

PASS = FAIL = 0
FAILED: list[str] = []

D1 = ("2026-11-08", "2026-11-08")
P1 = ("2026-11-07", "2026-11-07")
WEEK = ("2026-11-02", "2026-11-08")
PWEEK = ("2026-10-26", "2026-11-01")


def ck(name: str, cond: bool, extra: str = "") -> None:
    global PASS, FAIL
    if cond:
        PASS += 1
        print("  ✓ %s" % name)
    else:
        FAIL += 1
        FAILED.append(name)
        print("  ✗ %s   %s" % (name, extra))


def st(cur: str, base: str, cw=D1, bw=P1) -> str:
    return S.check(cur, base, cw, bw)["status"]


def t_ok() -> None:
    print("\n[1] 正常平移 → 可信")
    ck("单日等值", st("SELECT COUNT(*) FROM t WHERE dt = '2026-11-08'",
                     "SELECT COUNT(*) FROM t WHERE dt = '2026-11-07'") == "ok")
    ck("区间 BETWEEN",
       st("SELECT COUNT(*) FROM t WHERE dt BETWEEN '2026-11-02' AND '2026-11-08'",
          "SELECT COUNT(*) FROM t WHERE dt BETWEEN '2026-10-26' AND '2026-11-01'",
          WEEK, PWEEK) == "ok")
    ck("含 LIMIT / ORDER BY 且未变",
       st("SELECT song_id, COUNT(*) c FROM t WHERE dt='2026-11-08' "
          "GROUP BY song_id ORDER BY c DESC LIMIT 10",
          "SELECT song_id, COUNT(*) c FROM t WHERE dt='2026-11-07' "
          "GROUP BY song_id ORDER BY c DESC LIMIT 10") == "ok")
    ck("跨年平移",
       st("SELECT COUNT(*) FROM t WHERE dt = '2027-01-01'",
          "SELECT COUNT(*) FROM t WHERE dt = '2026-12-25'",
          ("2027-01-01", "2027-01-01"), ("2026-12-25", "2026-12-25")) == "ok")
    ck("跨闰日（2028-02-29 存在）",
       st("SELECT COUNT(*) FROM t WHERE dt = '2028-03-01'",
          "SELECT COUNT(*) FROM t WHERE dt = '2028-02-28'",
          ("2028-03-01", "2028-03-01"), ("2028-02-28", "2028-02-28")) == "ok")
    ck("带时间部分且时间不变",
       st("SELECT COUNT(*) FROM t WHERE ct = '2026-11-08 10:00:00'",
          "SELECT COUNT(*) FROM t WHERE ct = '2026-11-07 10:00:00'") == "ok")


def t_unshifted() -> None:
    print("\n[2] 日期写法没跟着平移 → 不可信（这是 _shift_sql 的盲区）")
    r = S.check("SELECT COUNT(*) FROM t WHERE dt='2026-11-08' "
                "AND date_format(dt,'%Y%m%d') = '20261108'",
                "SELECT COUNT(*) FROM t WHERE dt='2026-11-07' "
                "AND date_format(dt,'%Y%m%d') = '20261108'", D1, P1)
    ck("紧凑日期 YYYYMMDD", r["status"] == "unsafe", r["status"])
    ck("  说明里点出了那个字面量", "20261108" in r["message"], r["message"])

    r = S.check("SELECT COUNT(*) FROM t WHERE dt='2026-11-08' "
                "AND ct >= '2026-11-08 00:00:00'",
                "SELECT COUNT(*) FROM t WHERE dt='2026-11-07' "
                "AND ct >= '2026-11-08 00:00:00'", D1, P1)
    ck("datetime 字面量没平移", r["status"] == "unsafe", r["status"])

    r = S.check("SELECT COUNT(*) FROM t WHERE dt='2026-11-08' AND d2 >= '2026/11/08'",
                "SELECT COUNT(*) FROM t WHERE dt='2026-11-07' AND d2 >= '2026/11/08'", D1, P1)
    ck("斜杠日期没平移", r["status"] == "unsafe", r["status"])

    r = S.check("SELECT COUNT(*) FROM t WHERE ct = '2026-11-08 10:00:00'",
                "SELECT COUNT(*) FROM t WHERE ct = '2026-11-07 00:00:00'", D1, P1)
    ck("日期平移了但时间部分被改", r["status"] == "unsafe", r["status"])


def t_structure() -> None:
    print("\n[3] 结构变了 → 不可信（平移改不了结构）")
    ck("多一列",
       st("SELECT COUNT(*) c, SUM(x) s FROM t WHERE dt='2026-11-08'",
          "SELECT COUNT(*) c FROM t WHERE dt='2026-11-07'") == "unsafe")
    ck("聚合函数换了",
       st("SELECT COUNT(*) FROM t WHERE dt='2026-11-08'",
          "SELECT COUNT(DISTINCT u) FROM t WHERE dt='2026-11-07'") == "unsafe")
    ck("表换了",
       st("SELECT COUNT(*) FROM a WHERE dt='2026-11-08'",
          "SELECT COUNT(*) FROM b WHERE dt='2026-11-07'") == "unsafe")
    ck("LIMIT 被改（非日期字面量也不许动）",
       st("SELECT a FROM t WHERE dt='2026-11-08' LIMIT 10",
          "SELECT a FROM t WHERE dt='2026-11-07' LIMIT 20") == "unsafe")
    ck("过滤列换了",
       st("SELECT COUNT(*) FROM t WHERE dt='2026-11-08' AND is_new=1",
          "SELECT COUNT(*) FROM t WHERE dt='2026-11-07' AND is_old=1") == "unsafe")


def t_relative() -> None:
    print("\n[4] 相对时间函数 → 不可信（两条 SQL 查的是同一段时间）")
    for expr, name in (("curdate()", "curdate"),
                       ("now()", "now"),
                       ("current_date", "current_date")):
        sql = "SELECT COUNT(*) FROM t WHERE dt >= date_sub(%s, interval 7 day)" % expr
        r = S.check(sql, sql, D1, P1)
        ck("%s 被识别" % name, r["status"] == "unsafe" and name in r["message"],
           r["status"] + " " + r["message"])
    ck("只用字面量的 date_sub 不算相对（可平移）",
       st("SELECT COUNT(*) FROM t WHERE dt >= date_sub('2026-11-08', interval 7 day)",
          "SELECT COUNT(*) FROM t WHERE dt >= date_sub('2026-11-07', interval 7 day)") == "ok")


def t_semantic() -> None:
    print("\n[5] 语义边界")
    r = S.check("SELECT COUNT(*) FROM t WHERE dt IN ('2026-11-02','2026-11-05','2026-11-08')",
                "SELECT COUNT(*) FROM t WHERE dt IN ('2026-10-26','2026-10-29','2026-11-01')",
                WEEK, PWEEK)
    ck("窗口内部字面量 → 可信但标注", r["status"] == "warn", r["status"])
    ck("  标注里点出了那个日期", "2026-11-05" in "".join(r["notes"]), str(r["notes"]))
    ck("只有边界字面量 → 不标注",
       st("SELECT COUNT(*) FROM t WHERE dt BETWEEN '2026-11-02' AND '2026-11-08'",
          "SELECT COUNT(*) FROM t WHERE dt BETWEEN '2026-10-26' AND '2026-11-01'",
          WEEK, PWEEK) == "ok")
    ck("没有日期字面量 → 不可信",
       st("SELECT COUNT(*) FROM t WHERE x = 1", "SELECT COUNT(*) FROM t WHERE x = 1") == "unsafe")
    ck("解析失败 → unknown（不是 unsafe：没查清）",
       st("SELECT FROM WHERE", "SELECT FROM WHERE") == "unknown")
    ck("多语句 → unknown",
       st("SELECT 1 FROM t WHERE dt='2026-11-08'; SELECT 2",
          "SELECT 1 FROM t WHERE dt='2026-11-07'") == "unknown")
    ck("ok() 认 warn", S.ok({"status": "warn"}) and not S.ok({"status": "unsafe"}))


def t_with_shifter() -> None:
    """平移器的产物必须能过检查。不过的话，功能会被自己关掉。"""
    print("\n[6] 与 _shift_sql 配合：平移出来的基线 SQL 必须通过检查")
    forms = [
        "SELECT COUNT(*) AS c FROM dwd_play_start WHERE dt = '2026-11-08'",
        "SELECT COUNT(*) AS c FROM dwd_play_start WHERE dt BETWEEN '2026-11-02' AND '2026-11-08'",
        "SELECT COUNT(*) AS c FROM dwd_play_start WHERE dt >= '2026-11-02' AND dt <= '2026-11-08'",
        "SELECT COUNT(*) AS c FROM dwd_play_start WHERE dt IN ('2026-11-02','2026-11-05','2026-11-08')",
        "SELECT song_id, COUNT(*) AS c FROM dwd_play_start WHERE dt = '2026-11-08' "
        "GROUP BY song_id ORDER BY c DESC LIMIT 10",
        "SELECT COUNT(DISTINCT user_id) AS c FROM dwd_session WHERE dt = '2026-11-08' "
        "AND is_new = 1",
    ]
    for sql in forms:
        base, why = _shift_sql(sql, WEEK, PWEEK)
        if why:
            ck("平移器能处理：%s" % sql[-46:], False, why)
            continue
        r = S.check(sql, base, WEEK, PWEEK)
        ck("平移产物通过：…%s" % sql[-46:], r["status"] in ("ok", "warn"),
           "%s %s" % (r["status"], r.get("message") or r.get("reasons")))

    # 榜单对比要先去 LIMIT 再比，否则结构必然不同——这正是当初差点写错的地方
    sql = ("SELECT song_id, COUNT(*) AS c FROM dwd_play_start WHERE dt = '2026-11-08' "
           "GROUP BY song_id ORDER BY c DESC LIMIT 10")
    base, _ = _shift_sql(sql, WEEK, PWEEK)
    ck("榜单：不去 LIMIT 会误判结构不同",
       S.check(sql, _strip_limit(base), WEEK, PWEEK)["status"] == "unsafe")
    ck("榜单：两边都去 LIMIT 才正确",
       S.check(_strip_limit(sql), _strip_limit(base), WEEK, PWEEK)["status"] == "ok")


def main() -> int:
    for fn in (t_ok, t_unshifted, t_structure, t_relative, t_semantic, t_with_shifter):
        fn()
    print("\n%d 通过 / %d 失败" % (PASS, FAIL))
    if FAILED:
        print("失败项：" + "；".join(FAILED))
    # FAIL 是失败计数：0 个失败 = 全过 = 退出 0。
    # 之前写反了（全过反而退出 1），靠人眼看输出才没炸，现在纠正。
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(main())
