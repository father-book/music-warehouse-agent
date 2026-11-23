"""A 阶段冒烟测试：证明骨架、护栏、执行面三件事真的通。

    python code/agent/smoke.py

六条断言，不依赖 LLM：
  1. 执行面自检——裸表名必须解析到离线正库（445,945 行），不是 internal（72,355）
  2. 正例——DAU 必须等于 docs/32 的基线 3,237
  3. 跨 catalog 必须被 R17 拦
  4. SCD2 扇出必须被 R18 拦
  5. 实时问题必须拒答（不说"今天"）
  6. 超范围日期必须拒答
"""
from __future__ import annotations

import _bootstrap  # noqa: F401

import sys

from conf import load_config
from graph import ask

PASS, FAIL = "PASS", "FAIL"
results: list[tuple[str, str, str]] = []


def _record(name: str, ok: bool, detail: str) -> None:
    results.append((PASS if ok else FAIL, name, detail))
    print("  %-4s %-28s %s" % (PASS if ok else FAIL, name, detail))


def _rules(state: dict) -> str:
    return ",".join(sorted({f.get("rule", "") for f in (state.get("findings") or [])
                            if f.get("level") == "REJECT"}))


def main() -> int:
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    cfg = load_config()
    # A 阶段冒烟刻意锁死 stub：这里验的是"骨架/护栏/执行面"通不通，
    # 一旦掺进模型，CI 就会因为模型抖动而红，测不出真问题。
    # B/C 的验收在 selftest_bc.py。
    import copy
    cfg = copy.deepcopy(cfg)
    cfg["generator"]["mode"] = "stub"
    print("== A 阶段冒烟 ==")

    # 1. 执行面：裸表名落在哪个库
    st = ask("x", cfg, sql="SELECT COUNT(*) AS n FROM dwd_play_start "
                           "WHERE dt BETWEEN '2026-10-05' AND '2026-11-08'")
    res = st.get("result") or {}
    n = (res.get("rows") or [[None]])[0][0]
    _record("执行面=hive_catalog", n == 445945,
            "35 天 dwd_play_start = %s（离线应为 445,945；若为 72,355 说明落到了 internal）" % n)

    # 2. 正例
    st = ask("2026-11-08 的 DAU 是多少", cfg)
    res = st.get("result") or {}
    cols = res.get("columns") or []
    dau = (res.get("rows") or [[None]])[0][cols.index("dau")] if "dau" in cols else None
    _record("正例：DAU=3,237", dau == 3237,
            "dau=%s，判定=%s，护栏=%s" % (dau, st.get("verdict"), _rules(st) or "全过"))

    # 3. 跨 catalog
    st = ask("x", cfg, sql="SELECT COUNT(*) FROM internal.music_dw.dwd_play_start "
                           "WHERE dt = '2026-11-08'")
    _record("负例：跨 catalog", st.get("refused") and "R17" in _rules(st),
            "拒答=%s，命中=%s" % (st.get("refused"), _rules(st) or "无"))

    # 4. 扇出
    st = ask("x", cfg, sql="SELECT u.province, COUNT(*) FROM dwd_session s "
                           "JOIN dim_user u ON s.user_id = u.user_id "
                           "WHERE s.dt = '2026-11-08' GROUP BY u.province")
    _record("负例：SCD2 扇出", st.get("refused") and "R18" in _rules(st),
            "拒答=%s，命中=%s" % (st.get("refused"), _rules(st) or "无"))

    # 5. 实时
    st = ask("今天 DAU 多少", cfg)
    _record("负例：实时问题", bool(st.get("refused")),
            (st.get("refuse_reason") or "")[:40])

    # 6. 超范围
    st = ask("2099-01-01 的播放量", cfg)
    _record("负例：超范围日期", bool(st.get("refused")),
            (st.get("refuse_reason") or "")[:40])

    bad = [r for r in results if r[0] == FAIL]
    print("\n%d/%d 通过" % (len(results) - len(bad), len(results)))
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())
