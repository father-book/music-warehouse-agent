"""ETL 护栏自测：etlguard.validate 的四道静态检查 + EXPLAIN 实证。

覆盖：
  · 正常 CTAS：四项全过 + EXPLAIN 通过
  · 目标表不带 dev_ 前缀：target_ok / tables_ok 拒
  · 引用语义层之外的表：tables_ok 拒，且 EXPLAIN 给出真实错误
  · 混入 DROP：danger 拒（在解析之前就拦）
  · INSERT 语句：stmt_ok 拒
  · 多语句：拒
  · summarize 的文案与 checks 一致
"""
from __future__ import annotations

import _bootstrap  # noqa: F401
from conf import load_config
import etlguard

PASS = FAIL = 0


def expect(cond, msg):
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"  [ok] {msg}")
    else:
        FAIL += 1
        print(f"  [FAIL] {msg}")


GOOD = """CREATE TABLE music_dw.dev_dau_daily AS
SELECT dt, COUNT(DISTINCT COALESCE(NULLIF(user_id,''), device_id)) AS dau
FROM dwd_session WHERE dt BETWEEN '2026-10-10' AND '2026-11-08'
AND event_name = 'app_launch' GROUP BY dt ORDER BY dt"""


def main() -> int:
    cfg = load_config()

    print("[1] 正常 CTAS")
    c = etlguard.validate(GOOD, cfg)
    expect(c["stmt_ok"] and c["target_ok"] and c["tables_ok"]
           and c["danger_ok"], "四项静态检查全过")
    expect(c["target"] == "dev_dau_daily", f"目标表识别 {c['target']}")
    expect(c["explain_ok"], "EXPLAIN 实证通过")

    print("[2] 目标表不带 dev_ 前缀")
    c = etlguard.validate(GOOD.replace("dev_dau_daily", "ads_dau_daily"), cfg)
    expect(not c["target_ok"] and c["target"] == "ads_dau_daily", "target_ok 拒")
    expect(not c["explain_ok"], "不跑 EXPLAIN（静态已挂）")

    print("[3] 语义层之外的源表")
    c = etlguard.validate(GOOD.replace("dwd_session", "dwd_session_old"), cfg)
    expect(c["bad_tables"] == ["dwd_session_old"], "bad_tables 命中")
    expect(not c["tables_ok"], "tables_ok 拒")

    print("[4] 混入 DROP")
    c = etlguard.validate(GOOD + "; DROP TABLE music_dw.dev_dau_daily", cfg)
    expect(not c["danger_ok"] and "DROP" in (c["danger"] or []), "危险词拦下")
    expect(not c["stmt_ok"], "多语句整体拒绝")

    print("[5] INSERT 不收")
    c = etlguard.validate(
        "INSERT INTO music_dw.dev_x SELECT 1", cfg)
    expect(not c["stmt_ok"], "只接受 CREATE TABLE")

    print("[6] 大小写与库前缀的合法变体")
    c = etlguard.validate(GOOD.replace("music_dw.dev_dau_daily", "DEV_DAU_2"), cfg)
    expect(c["target_ok"] and c["target"] == "DEV_DAU_2", "无库前缀 + 大写也识别")

    print("[7] summarize 与 checks 一致")
    c = etlguard.validate(GOOD, cfg)
    lines = etlguard.summarize(c)
    # danger_ok / catalog_ok 时对应行不输出，全过 = 5 行（第 5 行是口径机检）
    expect(len(lines) == 5 and all(x.startswith("通过") for x in lines),
           "全过时 5 行结论（含口径机检）")
    c2 = etlguard.validate(GOOD.replace("dev_dau_daily", "ads_x"), cfg)
    expect(any(x.startswith("不通过") for x in etlguard.summarize(c2)),
           "有挂时出现不通过行")

    print("[7b] hive_catalog 三段式前缀拒收")
    c = etlguard.validate(GOOD.replace("FROM dwd_session", "FROM hive_catalog.music_dw.dwd_session"), cfg)
    expect(not c["catalog_ok"], "catalog_ok 拒")
    expect(not c["explain_ok"], "不跑 EXPLAIN（执行通道跑不了三段式）")

    # ---------------- 每日调度 SQL（validate_daily） ----------------
    DAILY_OK = """INSERT OVERWRITE TABLE music_dw.dev_dau_daily
SELECT dt, COUNT(DISTINCT COALESCE(NULLIF(user_id,''), device_id)) AS dau
FROM dwd_session WHERE dt BETWEEN '2026-10-10' AND '${hivevar:bizdate}'
AND event_name = 'app_launch' GROUP BY dt ORDER BY dt"""

    print("[8] 每日重刷 SQL 正例")
    c = etlguard.validate_daily(DAILY_OK, cfg)
    expect(c["stmt_ok"] and c["target_ok"] and c["tables_ok"]
           and c["danger_ok"] and c["overwrite_ok"] and c["param_ok"],
           "六项检查全过")
    expect(c["target"].endswith("dev_dau_daily"), f"目标表识别 {c['target']}")
    expect(c["explain_ok"], "EXPLAIN（bizdate 已替换为样本日期）通过")
    lines = etlguard.summarize_daily(c)
    expect(len(lines) == 7 and all(x.startswith("通过") for x in lines),
           "全过时 7 行结论（含口径机检）")

    print("[8b] 每日 SQL 三段式前缀拒收")
    c = etlguard.validate_daily(DAILY_OK.replace("FROM dwd_session",
                                                 "FROM hive_catalog.music_dw.dwd_session"), cfg)
    expect(not c["catalog_ok"] and not c["explain_ok"], "catalog_ok 拒")

    print("[9] INTO 追加拒收（重跑会重复计数）")
    c = etlguard.validate_daily(DAILY_OK.replace("OVERWRITE TABLE", "INTO TABLE")
                                .replace("INSERT INTO", "INSERT INTO"), cfg)
    expect(c["stmt_ok"] and not c["overwrite_ok"], "overwrite_ok 拒")
    expect(not c["explain_ok"], "不跑 EXPLAIN")

    print("[10] 缺 bizdate 参数拒收（调度器没法按天传参）")
    c = etlguard.validate_daily(
        DAILY_OK.replace("'${hivevar:bizdate}'", "'2026-11-08'"), cfg)
    expect(not c["param_ok"], "param_ok 拒")

    print("[11] 每日目标不带 dev_ 前缀 / 外部源表")
    c = etlguard.validate_daily(DAILY_OK.replace("dev_dau_daily", "ads_dau_daily"), cfg)
    expect(not c["target_ok"], "target_ok 拒")
    c = etlguard.validate_daily(DAILY_OK.replace("dwd_session", "dwd_session_old"), cfg)
    expect(c["bad_tables"] == ["dwd_session_old"] and not c["tables_ok"], "tables_ok 拒")

    print("[12] 建表校验仍拒 INSERT（两条通道互不越界）")
    c = etlguard.validate(DAILY_OK, cfg)
    expect(not c["stmt_ok"], "validate 只收 CREATE TABLE")

    print(f"\n通过 {PASS} / 失败 {FAIL}")
    return 1 if FAIL else 0


if __name__ == "__main__":
    raise SystemExit(main())
