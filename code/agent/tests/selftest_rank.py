"""榜单对比（TopN 名次变化）的自测。

这里不打数据库，也不打模型：把 `db.run_sql` 与 `guardtool.check` 换成
假实现，只验**对齐算法**。理由很直接——名次算错时，答案看起来完全正常：
表格照样整齐、数字照样是整数、没有任何异常。这种错只能靠构造已知数据来抓。

覆盖四种最容易错的情况：
  1. 名次分母是**基线全量**，不是基线 TopN（否则"上周第 47"永远是"未进榜"）
  2. 掉榜判断只针对基线 TopN（第 11 名本来就不在榜里，不算掉榜）
  3. 数值列不能被 ROW_NUMBER() 那列抢走（否则"播放量变化 -9"这种荒唐结论）
  4. 列顺序自由（`SELECT cnt, song_id` 也合法）

    python code/agent/tests/selftest_rank.py
"""
from __future__ import annotations

import _bootstrap  # noqa: F401

import sys
from pathlib import Path

from _bootstrap import HERE  # noqa: E402
sys.path.insert(0, str(HERE))

import node_execute as E  # noqa: E402
import node_narrate as N  # noqa: E402

OK = FAILED = 0


def ck(name: str, cond: bool, extra: str = "") -> None:
    global OK, FAILED
    if cond:
        OK += 1
        print("  ✓ %s" % name)
    else:
        FAILED += 1
        print("  ✗ %s %s" % (name, extra))


SQL = ("SELECT song_id, COUNT(*) AS c FROM dwd_play_start "
       "WHERE dt BETWEEN '2026-11-02' AND '2026-11-08' "
       "GROUP BY song_id ORDER BY c DESC LIMIT 2")

STATE = {
    "time_start": "2026-11-02", "time_end": "2026-11-08",
    "time_window": {"compare": {"start": "2026-10-26", "end": "2026-11-01",
                                "label": "上上周", "mode": "pair", "auto": True}},
    "ctx": None, "trace_id": "t", "question": "榜单变化",
}

# 当前 Top2：A(100) B(90)
RES = {"columns": ["song_id", "c"], "rows": [["A", 100], ["B", 90]],
       "row_count": 2, "error": None, "truncated": False, "elapsed_ms": 1}

# 基线**全量**：A 第 1、C 第 2、B 第 3、D 第 4
BASE = {"columns": ["song_id", "c"],
        "rows": [["A", 95], ["C", 80], ["B", 70], ["D", 60]],
        "row_count": 4, "error": None, "truncated": False, "elapsed_ms": 1}


class _Decision:
    max_rows = 5000


def _patch(base=None, seen=None):
    rows = base if base is not None else BASE

    def fake_run(sql, cfg, max_rows=0, trace_id=""):
        if seen is not None:
            seen.append(sql)
        return rows

    E.db.run_sql = fake_run
    E.guardtool.check = lambda *a, **k: ("pass", [], None)


def t_strip() -> None:
    print("\n[1] 去 LIMIT")
    ck("普通 LIMIT", E._strip_limit("SELECT 1 LIMIT 10") == "SELECT 1")
    ck("offset,n 形式", E._strip_limit("SELECT 1 LIMIT 5, 10") == "SELECT 1")
    ck("OFFSET 形式", E._strip_limit("SELECT 1 LIMIT 10 OFFSET 5") == "SELECT 1")
    ck("无 LIMIT 原样返回", E._strip_limit("SELECT 1") == "SELECT 1")
    ck("不误伤列名里的 limit_sec",
       E._strip_limit("SELECT limit_sec FROM t") == "SELECT limit_sec FROM t")


def t_layout() -> None:
    print("\n[2] 列布局识别")
    lay = E._layout(RES)
    ck("实体列 = song_id", lay and lay["key"] == 0, str(lay))
    ck("数值列 = c", lay and lay["value"] == 1)

    swapped = {"columns": ["c", "song_id"], "rows": [[100, "A"], [90, "B"]]}
    lay2 = E._layout(swapped)
    ck("列序打乱也能认", lay2 and lay2["key"] == 1 and lay2["value"] == 0, str(lay2))

    ranked = {"columns": ["rk", "song_id", "cnt"],
              "rows": [[1, "A", 100], [2, "B", 90]]}
    lay3 = E._layout(ranked)
    ck("ROW_NUMBER 列不抢指标位", lay3 and lay3["value"] == 2, str(lay3))

    ck("单行不成榜单", E._layout({"columns": ["a", "b"], "rows": [["A", 1]]}) is None)
    ck("全数字没有实体列", E._layout({"columns": ["a", "b"],
                                      "rows": [[1, 2], [3, 4]]}) is None)

    # 真机上翻过车的一种：SQL 同时 SELECT 了 id / name / singer。
    # 取"最后一个字符串列"就会显示歌手，一张表里同一个歌手出现好几行，
    # 还会出现"那英第 5 名却掉出前 10"这种自相矛盾。
    named = {"columns": ["song_id", "song_name", "singer", "cnt"],
             "rows": [["S1", "半島鐵盒", "周杰倫", 100],
                      ["S2", "家", "许巍", 90]]}
    lay4 = E._layout(named)
    ck("显示列优先 *_name，不取后面的 singer",
       lay4 and lay4["label"] == 1, str(lay4))
    ck("对齐键仍是 song_id", lay4 and lay4["key"] == 0)

    ck("key 本身可读时直接当显示列",
       (E._layout({"columns": ["province", "cnt"],
                   "rows": [["广东", 10], ["河南", 9]]}) or {}).get("label") == 0)

    # 指标列里有 NULL 时，别把整列判成字符串
    lay5 = E._layout({"columns": ["genre", "cnt"],
                      "rows": [["流行", 100], ["摇滚", None]]})
    ck("指标列含 NULL 仍认作数值列", lay5 and lay5["value"] == 1, str(lay5))


def t_align() -> None:
    print("\n[3] 对齐与名次")
    _patch()
    cr = E._compare_rank(STATE, {}, SQL, RES, _Decision())
    ck("状态 ok", cr.get("status") == "ok", str(cr.get("note")))
    rows = {r["key"]: r for r in (cr.get("rows") or [])}
    ck("A 两期都第 1", rows["A"]["rank"] == 1 and rows["A"]["base_rank"] == 1)
    ck("B 从第 3 升到第 2", rows["B"]["rank"] == 2 and rows["B"]["base_rank"] == 3)
    ck("B 的名次变化 = +1", rows["B"]["rank_delta"] == 1)
    ck("B 的值变化 = +20", rows["B"]["value_delta"] == 20)
    ck("榜首未变", cr.get("top1_changed") is False)
    ck("基线全量参与排名（分母 4）", cr.get("base_total") == 4)
    ck("基线 SQL 去掉了 LIMIT", "LIMIT" not in (cr.get("base_sql") or "").upper())
    ck("基线与当前用同一个表", "dwd_play_start" in (cr.get("base_sql") or ""))

    ex = [e["key"] for e in (cr.get("exited") or [])]
    ck("C 掉出前 2（基线第 2）", ex == ["C"], str(ex))
    ck("D 不在基线 Top2，不算掉榜", "D" not in ex)
    ck("没有新实体", not (cr.get("entered") or []))


def t_entered() -> None:
    print("\n[4] 新实体与榜首易主")
    base = {"columns": ["song_id", "c"],
            "rows": [["X", 95], ["Y", 80]]}     # 基线里没有 A/B
    _patch(base)
    cr = E._compare_rank(STATE, {}, SQL, RES, _Decision())
    ck("A 被判为新出现", [r["key"] for r in cr.get("entered") or []] == ["A", "B"],
       str([r["key"] for r in cr.get("entered") or []]))
    ck("base_rank 为空", (cr["rows"][0]).get("base_rank") is None)
    ck("榜首易主", cr.get("top1_changed") is True)
    ck("X/Y 都掉榜", [e["key"] for e in cr.get("exited") or []] == ["X", "Y"])


def t_guard() -> None:
    print("\n[5] 该拒绝的时候要拒绝")
    _patch({"columns": ["song_id", "c"], "rows": [], "error": None})
    cr = E._compare_rank(STATE, {}, SQL, RES, _Decision())
    ck("基线无数据 → skipped 且说明原因",
       cr.get("status") == "skipped" and "没有数据" in (cr.get("note") or ""),
       str(cr.get("note")))

    bad_sql = SQL.replace("2026-11-02", "2026-10-26")
    _patch()
    cr2 = E._compare_rank(STATE, {}, bad_sql, RES, _Decision())
    ck("SQL 自己引用了基线日期 → 不重复对齐",
       cr2.get("status") == "skipped" and "之外" in (cr2.get("note") or ""),
       str(cr2.get("note")))

    single = {"columns": ["song_id", "c"], "rows": [["A", 100]]}
    cr3 = E._compare_rank(STATE, {}, SQL, single, _Decision())
    ck("单行结果不做名次对比", cr3.get("status") == "skipped")


def t_text() -> None:
    print("\n[6] 结论文本")
    _patch()
    cr = E._compare_rank(STATE, {}, SQL, RES, _Decision())
    lines = N._rank_lines(cr)
    joined = "\n".join(lines)
    ck("说了升/降/持平", "升" in joined and "持平" in joined)
    ck("说了掉榜", "掉出" in joined)
    ck("上升/下降最多只写真正移动的", "上升最多" in joined and "B" in joined,
       joined)
    ck("持平的不冒充'上升最多'", "上升最多：A" not in joined)

    table = N._render_rank_table(cr)
    body = "\n".join(table)
    ck("表格含表头", "名次变化" in body and "本期" in body)
    ck("表格含掉榜说明", "掉出前 2" in body)
    ck("表格按显示宽度对齐（表头与数据列数一致）",
       len(table[1].split()) >= 7 and len(table[3].split()) >= 7,
       "%r" % table[3][:40])


def t_w(U=None) -> None:
    print("\n[7] 中英混排宽度")
    ck("中文占两格", N._w("本期") == 4)
    ck("英文占一格", N._w("abc") == 3)
    ck("补到同一显示宽度", N._w(N._pad("本期", 8)) == 8 == N._w(N._pad("abcd", 8)))


def main() -> int:
    t_strip()
    t_layout()
    t_align()
    t_entered()
    t_guard()
    t_text()
    t_w()
    print("\n%d 通过 / %d 失败" % (OK, FAILED))
    return 0 if FAILED == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
