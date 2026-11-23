"""口径机检自测：规则定义（metrics.yaml）→ 检查实现（caliber.py）→ 两条链路。

覆盖：
  · C1 收入事件覆盖：只取 order_pay 必须报；补齐 vip_subscribe 必须不报
  · C2 退款符号：全量 SUM(amount) 报；-amount 取负不报；按事件分组不报
  · C3 UV 源表：dwd_play_start 去重用户报；dwd_session 不报；COUNT(DISTINCT session_id) 不报
  · 不误伤：订单数 COUNT(*) 不报；CTE 别名不当成实体表
  · 问数链路：guard.py R25 输出 WARN（退出码 2），正确 SQL 无 finding
  · ETL 链路：etlguard.validate 带出 caliber 字段、summarize 以「告警」开头、不拦执行
  · 规则来源：metrics.yaml 有 3 条，且 tables.json 已同步（防"改了 YAML 忘记重新生成"）
"""
from __future__ import annotations

from _bootstrap import ROOT as _ROOT  # noqa: F401,E402

import importlib.util
import json
import subprocess
import sys
from pathlib import Path

import sqlglot

import etlguard
import semantic
from conf import load_config

ROOT = _ROOT   # tests/_bootstrap 提供，等价于原 code/agent 的 parents[2]
PASS = FAIL = 0


def expect(cond, msg):
    global PASS, FAIL
    if cond:
        PASS += 1
        print("  [ok] %s" % msg)
    else:
        FAIL += 1
        print("  [FAIL] %s" % msg)


def load_caliber(cfg):
    path = cfg["_paths"]["guard_script"].parent / "caliber.py"
    spec = importlib.util.spec_from_file_location("_caliber_test", str(path))
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


BUGGY_REV = """SELECT dt,
  SUM(CASE WHEN event_name = 'order_pay' THEN amount ELSE 0 END) AS rev
FROM dwd_pay WHERE dt BETWEEN '2026-10-05' AND '2026-11-08' GROUP BY dt"""

FIXED_REV = """SELECT dt,
  SUM(CASE WHEN event_name IN ('vip_subscribe','order_pay') THEN amount
           WHEN event_name = 'order_refund' THEN -amount ELSE 0 END) AS rev
FROM dwd_pay WHERE dt BETWEEN '2026-10-05' AND '2026-11-08' GROUP BY dt"""


def run_caliber(mod, specs, sql):
    tree = sqlglot.parse_one(sql, read="hive")
    return mod.check([tree], specs)


def main() -> int:
    cfg = load_config()
    mod = load_caliber(cfg)
    specs = semantic.caliber_rules(cfg)

    print("[0] 规则来源")
    # 2026-11-15：这两个断言原来写死 3 条（C1~C3），C4/C5/C6 上线后一直没改，
    # 于是每次跑都是 20 通过 / 2 失败。**长期红灯比没有红灯更糟**——
    # 它会训练人"这条失败可以不管"，等真的坏了一条也看不出来。
    expect(len(specs) == 6, "metrics.yaml 有 %d 条口径规则（期望 6）" % len(specs))
    meta = json.load(open(cfg["_paths"]["tables_json"], encoding="utf-8"))
    expect([c.get("id") for c in (meta.get("caliber_guard") or [])]
           == ["C1", "C2", "C3", "C4", "C5", "C6"],
           "tables.json 已同步 caliber_guard（gen_tables 跑过）")

    print("[1] C1 收入事件覆盖")
    fs = run_caliber(mod, specs, BUGGY_REV)
    expect(len(fs) == 1 and "vip_subscribe" in fs[0]["message"],
           "只取 order_pay 被拦下，并点名缺 vip_subscribe")
    expect(fs and fs[0]["level"] == "WARN", "口径问题只标黄（WARN），不拦死")
    expect(fs and fs[0]["fix"]["op"] == "replace"
           and "vip_subscribe" in fs[0]["fix"]["replacement"],
           "fix 是可直接替换的 SQL 片段")
    expect(not run_caliber(mod, specs, FIXED_REV), "补齐 VIP + 退款取负后无告警")

    print("[2] C2 退款符号")
    fs = run_caliber(mod, specs, "SELECT SUM(amount) FROM dwd_pay WHERE dt = '2026-11-08'")
    expect(len(fs) == 1 and "order_refund" in fs[0]["message"],
           "全量 SUM(amount) 报「退款按正数加了」")
    fs = run_caliber(mod, specs,
                     "SELECT SUM(CASE WHEN event_name = 'order_refund' THEN amount END) "
                     "FROM dwd_pay WHERE dt = '2026-11-08'")
    expect(len(fs) == 1, "显式把退款按正数求和 → 报")
    expect(not run_caliber(mod, specs,
                           "SELECT event_name, SUM(amount) FROM dwd_pay "
                           "WHERE dt = '2026-11-08' GROUP BY event_name"),
           "按事件分组的金额明细不报（退款自占一行，不存在混加）")

    print("[3] C3 UV 源表")
    UV = ("SELECT dt, COUNT(DISTINCT COALESCE(NULLIF(user_id,''), device_id)) AS uv "
          "FROM %s WHERE dt = '2026-11-08' GROUP BY dt")
    fs = run_caliber(mod, specs, UV % "dwd_play_start")
    expect(len(fs) == 1 and "dwd_session" in fs[0]["message"],
           "播放口径冒充活跃用户被拦下")
    expect(not run_caliber(mod, specs, UV % "dwd_session"), "启动口径不报")
    expect(not run_caliber(mod, specs,
                           "SELECT COUNT(DISTINCT session_id) FROM dwd_play_start "
                           "WHERE dt = '2026-11-08'"),
           "COUNT(DISTINCT session_id) 是会话数，不报")

    print("[4] 不误伤")
    expect(not run_caliber(mod, specs,
                           "SELECT COUNT(*) FROM dwd_pay WHERE dt = '2026-11-08' "
                           "AND event_name = 'order_pay'"),
           "订单数 COUNT(*) 不报（没做金额聚合）")
    fs = run_caliber(mod, specs,
                     "WITH t AS (SELECT user_id FROM dwd_play_start WHERE dt = '2026-11-08') "
                     "SELECT COUNT(DISTINCT user_id) FROM t")
    expect(len([f for f in fs if f["rule"].endswith("UV 源表")]) == 1,
           "CTE 里包着的错误源表仍然被识别（FROM t 不产生第二条误报）")

    print("[5] 问数链路（guard.py R25）")
    script = str(cfg["_paths"]["guard_script"])
    p = subprocess.run([sys.executable, script, "--mode", "query", "--json", BUGGY_REV],
                       capture_output=True, text=True, encoding="utf-8")
    out = p.stdout.strip()
    try:
        fs = json.loads(out) if out.startswith("[") else []
    except Exception:
        fs = []
    expect(any(f.get("rule", "").startswith("R25") for f in fs),
           "guard.py --json 输出 R25 finding（实际 %d 条）" % len(fs))
    expect(p.returncode == 2, "只有 WARN 时退出码 2（不阻断）实际 %d" % p.returncode)
    p2 = subprocess.run([sys.executable, script, "--mode", "query", "--json", FIXED_REV],
                        capture_output=True, text=True, encoding="utf-8")
    expect(p2.returncode == 0 and p2.stdout.strip() in ("[]", ""),
           "正确 SQL 在问数护栏下全绿（退出码 %d）" % p2.returncode)

    print("[6] ETL 链路（etlguard）")
    c = etlguard.validate(
        "CREATE TABLE music_dw.dev_rev_daily AS " + BUGGY_REV, cfg)
    expect(c.get("caliber_ok") is True, "caliber_ok 不因 WARN 变 False（不拦执行）")
    expect(len(c.get("caliber") or []) == 1, "ETL 生成物带出 1 条口径告警")
    expect(any(x.startswith("告警") for x in etlguard.summarize(c)),
           "summary 用「告警」开头（前端据此标黄）")
    c2 = etlguard.validate(
        "CREATE TABLE music_dw.dev_rev_daily AS " + FIXED_REV, cfg)
    expect(not (c2.get("caliber") or []), "口径正确的 CTAS 无告警")
    expect(any("口径机检未发现" in x for x in etlguard.summarize(c2)),
           "summary 明确写出「口径机检未发现风险」")

    print("[7] 口径版本与变更影响面")
    d1 = semantic.caliber_digest(cfg)
    d2 = semantic.caliber_digest(cfg)
    expect(d1["version"] == d2["version"],
           "版本可重复（同内容两次算出 %s）" % d1["version"])
    expect(len(d1["version"]) == 12 and d1["version"].startswith("cal-"),
           "版本格式 cal-xxxxxxxx（实得 %r）" % d1["version"])
    # 数量必须与语义层实际一致：版本号对不上内容就没有意义
    expect(d1["counts"]["metrics"] == len(semantic.all_metrics(cfg)),
           "版本里的指标数与语义层一致（%d）" % d1["counts"]["metrics"])
    expect(d1["counts"]["caliber_rules"] == len(semantic.caliber_rules(cfg)),
           "版本里的口径规则数与语义层一致（%d）" % d1["counts"]["caliber_rules"])

    # 影响面脚本的核心是 diff：用构造数据测，不碰真实 YAML（碰了就不是自测而是改数据）
    sys.path.insert(0, str(ROOT / "eval"))
    import caliber_impact as ci
    old_p = {"metrics": {"a": {"formula": "COUNT(*)"}},
             "tables": {"t1": {"grain": "行"}}, "caliber_guard": [],
             "dimensions": {}, "glossary": {"词": {"metric": "a"}}}
    new_p = {"metrics": {"a": {"formula": "COUNT(1)"}, "b": {"formula": "SUM(x)"}},
             "tables": {"t1": {"grain": "行/天"}, "t2": {}}, "caliber_guard": [],
             "dimensions": {}, "glossary": {}}
    dd = ci.diff_payload(old_p, new_p)
    expect(dd["metrics"]["add"] == ["b"], "diff 认出新增指标 b")
    expect(dd["metrics"]["modify"]["a"]["formula"] == ["COUNT(*)", "COUNT(1)"],
           "diff 认出 a 的 formula 变化（字段级）")
    expect(dd["tables"]["modify"]["t1"]["grain"] == ["行", "行/天"], "diff 认出表粒度变化")
    expect(ci._is_empty(dd["dimensions"]), "无变化的段判为空")
    # 词条变更必须折算成指标，否则改词条会得出「0 题受影响」的假结论
    expect(ci._terms_metrics(old_p, {"词"}) == {"a"}, "词条变更折算到指标")

    print("\n== 口径机检自测：%d 通过 / %d 失败 ==" % (PASS, FAIL))
    return 0 if FAIL == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
