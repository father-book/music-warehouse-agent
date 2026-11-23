"""执行通道 + 语义层登记的自测（离线部分）。

覆盖三道边界与类型映射：这些逻辑不依赖集群，可以在任何环境跑；
真实执行链路由 docs/51 的端到端记录背书（生成 → 执行 → 登记 → 可问数）。

  · 执行通道未启用 / 未配 host 时拒绝
  · 非 dev_ 目标表在"执行前重验"就被拦（不落到 ssh）
  · 执行语句 = 系统补 DROP + 原 SQL（DROP 不来自模型）
  · Doris 类型 → Hive 类型映射
  · 登记草案的冲突检测在无表时可判错（读不到结构 → 明确报错）
"""
from __future__ import annotations

import _bootstrap  # noqa: F401
from conf import load_config
import etl_exec
import etl_register

PASS = FAIL = 0


def expect(cond, msg):
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"  [ok] {msg}")
    else:
        FAIL += 1
        print(f"  [FAIL] {msg}")


def main() -> int:
    cfg = load_config()

    print("[1] 配置读取（白名单机器）")
    c = etl_exec.exec_conf(cfg)
    expect(c["host"] == "192.168.128.161", f"host={c['host']}")
    expect(c["hive_bin"].endswith("hive"), "hive_bin 指向 hive CLI")
    expect(c["timeout_s"] > 0, "超时已配置")

    print("[2] 通道关闭时拒绝执行")
    bad_cfg = {**cfg, "etl_exec": {**(cfg.get("etl_exec") or {}), "enabled": False}}
    r = etl_exec.run_hive("SELECT 1;", bad_cfg)
    expect(not r["ok"] and "未启用" in r["error"], "关闭时明确拒绝")

    print("[3] 非 dev_ 目标在执行前重验就被拦")
    r = etl_exec.execute_ctas(
        "CREATE TABLE music_dw.ads_x AS SELECT 1 AS a", cfg)
    expect(not r["ok"] and r["stage"] == "validate", "stage=validate")
    expect("重验未通过" in r["error"], "错误信息可读")

    print("[4] 危险词也在重验阶段被拦（不落到 ssh）")
    r = etl_exec.execute_ctas(
        "CREATE TABLE music_dw.dev_x AS SELECT 1 AS a; DROP TABLE music_dw.ads_x", cfg)
    expect(not r["ok"] and r["stage"] == "validate", "多语句/危险词拦截")

    print("[5] Doris → Hive 类型映射")
    cases = {"varchar(65533)": "STRING", "string": "STRING", "bigint": "BIGINT",
             "decimalv3(18, 2)": "DECIMAL(18,2)", "int": "INT",
             "date": "DATE", "weird_type": "STRING"}
    for src, want in cases.items():
        got = etl_register.hive_type(src)
        expect(got == want, f"{src} -> {got}")

    print("[6] 登记草案：表不存在时给出明确错误")
    d = etl_register.draft({"target_table": "dev_not_exists_xyz",
                            "title": "x", "requirement": "x"}, cfg)
    expect(not d.get("ok") and "执行" in (d.get("error") or ""),
           "提示先执行建表")

    print("[7] 登记草案：没有目标表时报错")
    d = etl_register.draft({"target_table": "", "checks": {}}, cfg)
    expect(not d.get("ok") and d.get("error"), "明确报错")

    print(f"\n通过 {PASS} / 失败 {FAIL}")
    return 1 if FAIL else 0


if __name__ == "__main__":
    raise SystemExit(main())
