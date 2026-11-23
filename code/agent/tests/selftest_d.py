"""D 阶段自测：时间解析 / 倒排检索 / 新鲜度 / 主链路集成。

    python code/agent/tests/selftest_d.py           # 全跑（额外调 2 次模型，约 15 秒）
    python code/agent/tests/selftest_d.py --quick   # 只跑确定性用例，零 LLM、零 DB

为什么和 smoke.py / selftest_bc.py 分三个文件：
三者的验收对象不同（骨架管道 / 模型生成与修复 / 语义检索与时间），
混在一起时任何一环抖动都会让整片变红，反而看不出是哪坏了。

全部时间断言都基于 ``clock.mode=fixed`` + ``fixed_today=2026-11-09``，
否则「昨天」会随着真实日期漂移，今天过、明天挂。
"""
from __future__ import annotations

import _bootstrap  # noqa: F401

import copy
import datetime as dt
import sys

import node_route
import promptlib
import retrieval
import semantic
import timeparse
from conf import load_config

PASS, FAIL, SKIP = "PASS", "FAIL", "SKIP"
rows: list[tuple[str, str, str]] = []


def rec(state: str, name: str, detail: str) -> None:
    rows.append((state, name, detail))
    print("  %-4s %-32s %s" % (state, name, detail))


def _cfg() -> dict:
    cfg = load_config()
    cfg["clock"] = {"mode": "fixed", "fixed_today": "2026-11-09"}
    return cfg


def _d(s: str) -> dt.date:
    return dt.date.fromisoformat(s)


def _check(name: str, got, want, extra: str = "") -> None:
    ok = got == want
    rec(PASS if ok else FAIL, name,
        "%s = %s%s" % (extra or "结果", got, "" if ok else "（期望 %s）" % want))


# ---------------------------------------------------------------- 时间解析
def t_relative_day(cfg: dict) -> None:
    """A 阶段的原 bug：把「昨天」相对 data_until 算成 11-07。"""
    w = timeparse.parse("昨天播放量", cfg)
    _check("昨天相对今天而非截止日", w.start.isoformat(), "2026-11-08")
    _check("昨天是单日窗口", w.is_range, False)


def t_realtime(cfg: dict) -> None:
    w = timeparse.parse("今天 DAU 多少", cfg)
    _check("实时词判不可答", w.status, "realtime")
    rec(PASS if "实时链路" in w.note else FAIL, "实时拒答理由完整", w.note[:56])


def t_recent_days(cfg: dict) -> None:
    for q in ("最近7天的播放量", "最近七天 收入", "过去 7 天 DAU"):
        w = timeparse.parse(q, cfg)
        ok = (w.start == _d("2026-11-03") and w.end == _d("2026-11-08")
              and w.status == "truncated")
        rec(PASS if ok else FAIL, "最近 N 天 · " + q[:8],
            "%s ~ %s / %s" % (w.start, w.end, w.status))


def t_month_clip(cfg: dict) -> None:
    w = timeparse.parse("上个月各省份活跃用户数", cfg)
    ok = (w.start == _d("2026-10-05") and w.end == _d("2026-10-31")
          and w.status == "truncated" and "少了 4 天" in w.note)
    rec(PASS if ok else FAIL, "上个月裁剪到数据起点",
        "%s ~ %s / %s" % (w.start, w.end, w.note[:46]))

    w2 = timeparse.parse("11月 的 UV", cfg)
    ok2 = (w2.start == _d("2026-11-01") and w2.end == _d("2026-11-08")
           and "少了 22 天" in w2.note)
    rec(PASS if ok2 else FAIL, "当月裁剪到最新分区",
        "%s ~ %s / %s" % (w2.start, w2.end, w2.note[:46]))


def t_week_and_range(cfg: dict) -> None:
    w = timeparse.parse("上周 播放量", cfg)
    _check("上周是 ISO 周一~周日", (w.start.isoformat(), w.end.isoformat()),
           ("2026-11-02", "2026-11-08"))
    # 回归：2026-11-10 发现「上上周」被「上周」的正则先吃掉，返回了两周前
    # 本该属于上周的区间；数值看起来完全合理，是典型的静默错。
    w3 = timeparse.parse("上上周的播放量", cfg)
    _check("上上周要多退一周", (w3.start.isoformat(), w3.end.isoformat()),
           ("2026-10-26", "2026-11-01"))
    w4 = timeparse.parse("上上个月 的收入", cfg)
    _check("上上个月要多退一个月", (w4.start.isoformat(), w4.end.isoformat()),
           ("2026-09-01", "2026-09-30"))
    w2 = timeparse.parse("10-05 到 10-12 的播放量", cfg)
    _check("显式区间", (w2.start.isoformat(), w2.end.isoformat()),
           ("2026-10-05", "2026-10-12"))
    _check("区间生成 BETWEEN", w2.sql_hint(),
           "dt BETWEEN '2026-10-05' AND '2026-10-12'")


def t_cn_date(cfg: dict) -> None:
    """「11月8日」不能被整月正则吞掉。"""
    for q in ("11月8日 的 DAU", "11/08 播放量", "2026年11月8日 DAU"):
        w = timeparse.parse(q, cfg)
        ok = w.start == w.end == _d("2026-11-08") and w.kind == "single"
        rec(PASS if ok else FAIL, "中文/简写日期 · " + q[:10],
            "%s ~ %s / %s" % (w.start, w.end, w.kind))


def t_empty(cfg: dict) -> None:
    w = timeparse.parse("2026-12-01 的播放量", cfg)
    _check("完全无数据判 empty", w.status, "empty")
    rec(PASS if "完全没有数据" in w.note else FAIL, "empty 说明数据边界", w.note[:60])


def t_freshness(cfg: dict) -> None:
    """P1-10：now() - data_until > 24h 必须显式写出滞后。"""
    stale = copy.deepcopy(cfg)
    stale["clock"]["fixed_today"] = "2026-11-13"
    w = timeparse.parse("2026-11-08 的 DAU", stale)
    ok = w.lag_days == 5 and "滞后 5 天" in w.note
    rec(PASS if ok else FAIL, "滞后 5 天显式标注",
        "lag=%d ｜ %s" % (w.lag_days, w.note[:60]))

    c = timeparse.clocks(cfg)
    rec(PASS if c["lag_days"] == 1 and c["data_since"] == _d("2026-10-05")
        else FAIL, "双基准读数",
        "today=%s since=%s until=%s lag=%d" % (
            c["today"], c["data_since"], c["data_until"], c["lag_days"]))


def t_fixed_clock(cfg: dict) -> None:
    a = timeparse.parse("昨天播放量", cfg)
    b = timeparse.parse("昨天播放量", cfg)
    rec(PASS if a == b else FAIL, "固定时钟可复现",
        "两次解析结果一致=%s" % (a == b))


# ---------------------------------------------------------------- 检索
def t_dimension_detect(cfg: dict) -> None:
    cases = [
        ("各省份的活跃用户数", "province"),
        ("按曲风统计播放量", "genre"),
        ("VIP 用户的播放量", "is_vip"),
    ]
    for q, want in cases:
        codes = [d["code"] for d in retrieval.detect_dimensions(q, cfg)]
        rec(PASS if want in codes else FAIL, "维度命中 · " + q[:10],
            "%s → %s" % (want, codes))
    pure = retrieval.detect_dimensions("11-08 的 DAU 是多少", cfg)
    rec(PASS if not pure else FAIL, "纯指标问题不命中分组维度", "命中 %d 个" % len(pure))


def t_bm25_recall(cfg: dict) -> None:
    hits = semantic.find_tables("各省份的活跃用户数", cfg, top=4)
    names = [h["table"] for h in hits]
    # 不写死 dim_user：dws_province_daily 自带 province+dau，不用 JOIN
    # SCD2 维表，对分省问题反而更合适。断言只要求「省份相关表」居首。
    ok = names and names[0] in ("dim_user", "dws_province_daily", "ads_province_dist")
    rec(PASS if ok else FAIL, "省份问题召回省份相关表居首",
        str([(h["table"], h["score"]) for h in hits[:3]]))

    song = [h["table"] for h in semantic.find_tables("周杰伦的歌曲完播率", cfg, top=5)]
    ok = "dim_song" in song or "ads_song_ranking" in song
    rec(PASS if ok else FAIL, "歌曲问题召回歌曲相关表", str(song))

    # 与 A 阶段 legacy 对照：字符重合度召不出「周杰伦」
    legacy = copy.deepcopy(cfg)
    legacy["retrieval"]["backend"] = "legacy"
    old = [h["table"] for h in semantic.find_tables("周杰伦的歌曲完播率", legacy, top=5)]
    rec(PASS if "dim_song" not in old else SKIP, "legacy 召不到 dim_song 维表",
        "legacy=%s ｜ bm25=%s ｜ backend=%s" % (
            old[:3], song[:3], retrieval.backend_name(cfg)))


def t_glossary_priority(cfg: dict) -> None:
    hits = semantic.find_metrics("11-08 的 DAU 是多少", cfg, top=3)
    ok = hits and hits[0]["code"] == "uv" and hits[0]["score"] >= 90
    rec(PASS if ok else FAIL, "glossary 精确同义词优先",
        str([(h["code"], h["score"], h["matched_by"]) for h in hits]))
    weak = [h for h in hits[1:] if h["score"] < 50]
    rec(PASS if not weak else FAIL, "弱 BM25 补充被阈值挡掉",
        "剩余 %s" % [(h["code"], h["score"]) for h in weak])


def t_no_metric_recall(cfg: dict) -> None:
    """一条指标、一个维度都没命中时，也必须有表可给模型看。

    实测事故：这类问题 route 返回空表清单，模型修 R9 时只能凭空编出
    `dws_user_daily.user_id` / `is_active`，被 R4 连拦两轮后耗尽重试。
    另外「启动了」必须能召回 dwd_session——把整句中文名注册成 jieba 用户词
    会让「启动」这个细粒度词消失，这里也是回归保护。
    """
    q = "11-08 有哪些用户启动了 App"
    st = node_route.route({"question": q}, cfg)
    ok = st["tables"] and st["tables"][0] == "dwd_session"
    rec(PASS if ok else FAIL, "无指标时召回明细表且居首",
        "tables=%s ｜ metrics=%s" % (st["tables"], st["metrics"]))
    rec(PASS if retrieval.detail_intent(q) else FAIL, "列举型问题识别为明细意图",
        "detail_intent=%s" % retrieval.detail_intent(q))


def t_repair_boundary(cfg: dict) -> None:
    """C 阶段修复请求必须带上「只能用哪些表/列」的边界。"""
    q = "11-08 有哪些用户启动了 App"
    st = node_route.route({"question": q}, cfg)
    st["question"] = q
    msgs = promptlib.build_generate_messages(
        st, cfg,
        findings=[{"level": "REJECT", "rule": "R9-禁 SELECT *", "message": "禁止 SELECT *"}],
        prev_sql="SELECT * FROM dwd_session WHERE dt='2026-11-08' LIMIT 10")
    u = msgs[1]["content"]
    ok = "【重写边界" in u and "user_id" in u and "【可用表】" in u
    rec(PASS if ok else FAIL, "修复 prompt 含表/列边界",
        "边界=%s 可用表=%s user_id=%s" % (
            "【重写边界" in u, "【可用表】" in u, "user_id" in u))


# ---------------------------------------------------------------- 集成
def t_route_tables(cfg: dict) -> None:
    pure = node_route.route({"question": "11-08 的 DAU 是多少"}, cfg)
    dims = [t for t in pure["tables"] if t.startswith("dim_")]
    rec(PASS if not dims else FAIL, "纯指标不塞维表", "tables=%s" % pure["tables"])

    grouped = node_route.route({"question": "各省份的活跃用户数"}, cfg)
    ok = "dim_user" in grouped["tables"] and "province" in grouped["dimensions"]
    rec(PASS if ok else FAIL, "分组问题带维表与 caution",
        "tables=%s cautions=%d" % (grouped["tables"], len(grouped["dimension_cautions"])))

    refused = node_route.route({"question": "今天 DAU 多少"}, cfg)
    rec(PASS if refused["unsupported"] else FAIL, "route 判实时拒答",
        refused["unsupported"][:52])


def t_prompt_window(cfg: dict) -> None:
    st = node_route.route({"question": "最近7天的播放量"}, cfg)
    st["question"] = "最近7天的播放量"
    ctx = promptlib.context_block(st, cfg)
    need = ("dt BETWEEN '2026-11-03' AND '2026-11-08'", "人日", "少了 1 天")
    miss = [x for x in need if x not in ctx]
    rec(PASS if not miss else FAIL, "prompt 含区间/裁剪/人日警告",
        "缺 %s ｜ 长度 %d 字" % (miss or "无", len(ctx)))


# ---------------------------------------------------------------- 依赖模型
def t_e2e_dau(cfg: dict) -> None:
    from graph import ask
    st = ask("2026-11-08 的 DAU 是多少", cfg)
    res = st.get("result") or {}
    val = (res.get("rows") or [[]])[0]
    ok = val and int(val[0]) == 3237 and not st.get("refused")
    rec(PASS if ok else FAIL, "端到端 DAU 回归",
        "值=%s 判定=%s" % (val, st.get("verdict")))


def t_e2e_range(cfg: dict) -> None:
    from graph import ask
    st = ask("最近7天的播放量", cfg)
    sql = (st.get("sql") or "").upper()
    ok = ("BETWEEN" in sql and "2026-11-03" in sql and "2026-11-08" in sql
          and not st.get("refused"))
    rec(PASS if ok else FAIL, "端到端多日窗口 SQL",
        "%s ｜ 判定=%s" % ((st.get("sql") or "")[:96], st.get("verdict")))


# ---------------------------------------------------------------- 入口

def t_compare_window(cfg: dict) -> None:
    """阶段 4：对比基线必须由 timeparse 解析，不能留给模型抄日期。"""
    w = timeparse.parse("11-08 的播放量比 11-01 多了多少", cfg)
    c = w.compare or {}
    _check("对比·当前窗口", w.start.isoformat(), "2026-11-08")
    _check("对比·基线日期", c.get("start"), "2026-11-01")
    _check("对比·可自动比较", bool(c.get("auto")), True)

    # 基线时间词不能被主窗口抢走：「比上上周」里的上上周是基线。
    w = timeparse.parse("上周的播放量相比上上周变化了多少", cfg)
    c = w.compare or {}
    _check("对比·主窗口仍是上周", [w.start.isoformat(), w.end.isoformat()],
           ["2026-11-02", "2026-11-08"])
    _check("对比·基线是上上周", [c.get("start"), c.get("end")],
           ["2026-10-26", "2026-11-01"])

    # 长度不同的两个窗口做差没有业务含义，必须拒绝自动比较而不是硬算。
    w = timeparse.parse("占比比上个月高多少", cfg)
    _check("对比·长度不同不硬算", (w.compare or {}).get("status"), "mismatch")

    # 数据只有 35 天，同比必然落在数据之外 → 应该如实说无数据。
    w = timeparse.parse("上个月的收入同比", cfg)
    _check("对比·同比无数据", (w.compare or {}).get("status"), "empty")

    w = timeparse.parse("11-08 的播放量", cfg)
    _check("对比·普通问题不误判", w.compare, None)

def main() -> int:
    quick = "--quick" in sys.argv
    cfg = _cfg()
    print("== D 阶段自测（%s）==" % ("仅确定性" if quick else "全量"))
    print("-- 时间解析 / 新鲜度 --")
    for fn in (t_relative_day, t_realtime, t_recent_days, t_month_clip,
               t_week_and_range, t_cn_date, t_empty, t_freshness, t_fixed_clock,
               t_compare_window):
        fn(cfg)
    print("-- 倒排检索 --")
    for fn in (t_dimension_detect, t_bm25_recall, t_glossary_priority,
               t_no_metric_recall, t_repair_boundary):
        fn(cfg)
    print("-- 主链路集成 --")
    for fn in (t_route_tables, t_prompt_window):
        fn(cfg)
    if not quick:
        print("-- 依赖模型的用例 --")
        for fn in (t_e2e_dau, t_e2e_range):
            fn(cfg)
    else:
        rec(SKIP, "端到端 2 条", "--quick 跳过")

    bad = [r for r in rows if r[0] == FAIL]
    skip = sum(1 for r in rows if r[0] == SKIP)
    print("\n%d/%d 通过，%d 跳过" % (
        sum(1 for r in rows if r[0] == PASS), len(rows) - skip, skip))
    if bad:
        print("\n失败明细：")
        for _, n, d in bad:
            print("  - %s：%s" % (n, d))
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())
