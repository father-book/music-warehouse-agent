"""护栏 CTE/子查询别名回归测试。

2026-11-11 真实事故：模型对"排名类"问题自然写出
    WITH user_prov AS (...), ranked AS (...) SELECT ... FROM ranked
`FROM ranked` 在 AST 里是 Table 节点，R3（表白名单）与 R21（权限范围）
把它当成实体表，正确 SQL 被拒。修复后这里固化三条断言：

  · 带 CTE 的合法 SQL 通过护栏（别名不算表）
  · CTE 定义体里的真实表仍受白名单约束（跳过别名没放宽边界）
  · 真越权表仍然被拒（修复不能把权限检查放空）
"""
from __future__ import annotations

import _bootstrap  # noqa: F401
from conf import load_config
import guardtool

PASS = FAIL = 0


def expect(cond, msg):
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"  [ok] {msg}")
    else:
        FAIL += 1
        print(f"  [FAIL] {msg}")


CTE_SQL = """WITH pay AS (
  SELECT user_id, province,
         SUM(CASE WHEN event_name IN ('vip_subscribe','order_pay') THEN amount ELSE 0 END) AS amt
  FROM dwd_pay WHERE dt = '2026-11-08' GROUP BY user_id, province
), ranked AS (
  SELECT user_id, province, amt, ROW_NUMBER() OVER (PARTITION BY province ORDER BY amt DESC) AS rn
  FROM pay
)
SELECT user_id, province, amt FROM ranked WHERE province = '广东省' AND rn <= 20 ORDER BY amt DESC"""


def main() -> int:
    cfg = load_config()

    print("[1] CTE 合法 SQL 不被别名误伤")
    verdict, findings, err = guardtool.check(CTE_SQL, cfg)
    rules = [f.get("rule") for f in findings if f.get("level") == "REJECT"]
    expect(verdict != "reject", f"verdict={verdict}")
    expect(not any("R3" in str(r) for r in rules), "R3 未命中（pay/ranked 是 CTE 不是表）")
    expect(not any("R21" in str(r) for r in rules), "R21 未命中")

    print("[2] CTE 定义体内的真实表仍受白名单约束")
    bad = CTE_SQL.replace("FROM dwd_pay", "FROM dwd_pay_not_exist")
    verdict, findings, _ = guardtool.check(bad, cfg)
    rules = [f.get("rule") for f in findings if f.get("level") == "REJECT"]
    expect(verdict == "reject", f"verdict={verdict}")
    expect(any("R3" in str(r) for r in rules), "R3 仍然拦下语义层外的表")

    print("[3] 真越权（pm 读 dwd 明细）仍被拒")
    verdict, findings, _ = guardtool.check(
        "SELECT COUNT(*) FROM dwd_pay WHERE dt = '2026-11-08'", cfg,
        context={"role": "data_pm", "question": "支付明细"},
        decision={"allow_tables": ["ads_daily_overview"], "deny_tables": [],
                  "deny_columns": [], "mask_columns": [], "row_rules": [],
                  "unrestricted": False})
    rules = [f.get("rule") for f in findings if f.get("level") == "REJECT"]
    expect(verdict == "reject" and any("R21" in str(r) for r in rules),
           "越权表仍拒")

    print(f"\n通过 {PASS} / 失败 {FAIL}")
    return 1 if FAIL else 0


if __name__ == "__main__":
    raise SystemExit(main())
