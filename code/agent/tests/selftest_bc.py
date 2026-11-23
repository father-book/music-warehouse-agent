"""B/C 阶段自测。

    python code/agent/tests/selftest_bc.py           # 全跑（会调 3~4 次模型，约 30 秒）
    python code/agent/tests/selftest_bc.py --quick   # 只跑不依赖模型的用例

分两类断言：
  · 确定性的（机械修复、别名转换、重试上限）—— 任何环境都必须过
  · 依赖模型的（真生成、真修复）—— 模型抽风时允许失败，但要能看出是哪条

为什么单独一个文件而不是塞进 smoke.py：
  smoke.py 是"A 阶段骨架通不通"，必须零 LLM 依赖，否则模型一抖动 CI 就红；
  这里测的是"模型接进来之后对不对"，天然依赖外部服务，两者不能混。
"""
from __future__ import annotations

import _bootstrap  # noqa: F401

import copy
import sys

import fixer
import guardtool
from conf import load_config
from graph import ask

PASS, FAIL, SKIP = "PASS", "FAIL", "SKIP"
rows: list[tuple[str, str, str]] = []


def rec(state: str, name: str, detail: str) -> None:
    rows.append((state, name, detail))
    print("  %-4s %-30s %s" % (state, name, detail))


def _rejects(st: dict) -> list[str]:
    return [f.get("rule", "") for f in (st.get("findings") or [])
            if f.get("level") == "REJECT"]


# ---------------------------------------------------------------- 确定性用例

def t_mechanical_fanout(cfg: dict) -> None:
    """R18 的 fix 必须被机械应用，且表名要换成别名。

    这一条是真 bug 的保护：护栏给的 fix 是 `dim_user.is_current = 'Y'`，
    而 SQL 里写的是 `JOIN dim_user u`。直接拼进去，Doris 报
    Unknown column 'is_current' in 'dim_user'——护栏放行了，执行才炸。
    """
    sql = ("SELECT u.province, COUNT(*) AS c FROM dwd_session s "
           "JOIN dim_user u ON s.user_id = u.user_id "
           "WHERE s.dt = '2026-11-08' GROUP BY u.province")
    verdict, findings, _ = guardtool.check(sql, cfg)
    if verdict != "reject":
        rec(FAIL, "机械修复 R18", "前置条件不成立：这条 SQL 没被拦（%s）" % verdict)
        return

    new_sql, applied = fixer.mechanical_fix(sql, findings)
    ok = "u.is_current" in new_sql and "dim_user.is_current" not in new_sql
    rec(PASS if ok else FAIL, "机械修复 R18 + 别名",
        "改后含 u.is_current=%s，遗留 dim_user.is_current=%s ｜ %s"
        % ("u.is_current" in new_sql, "dim_user.is_current" in new_sql, applied))

    if new_sql != sql:
        v2, _, _ = guardtool.check(new_sql, cfg)
        rec(PASS if v2 != "reject" else FAIL, "机械修复后复检通过", "verdict=%s" % v2)


def t_mechanical_skip_cjk(cfg: dict) -> None:
    """replacement 是中文说明（R20 那种）时，绝不能往 SQL 里塞。

    用 SUM(vip_user_cnt)：它是 stock（存量快照），跨日求和会被 R20 判 REJECT，
    而护栏给的 replacement 是「MAX_BY(...) 或按 dt 逐日展示」——一句人话。
    机械套进 SQL 会直接语法错，所以 fixer 必须跳过它交给模型。
    """
    sql = ("SELECT SUM(vip_user_cnt) AS s FROM dws_user_daily "
           "WHERE dt BETWEEN '2026-11-01' AND '2026-11-08'")
    verdict, findings, _ = guardtool.check(sql, cfg)
    if verdict != "reject":
        rec(SKIP, "中文 fix 不机械套用", "这条没被拦（%s），跳过" % verdict)
        return

    cjk = [f for f in findings if fixer._RE_CJK.search((f.get("fix") or {}).get("replacement") or "")]
    if not cjk:
        rec(SKIP, "中文 fix 不机械套用", "护栏没给中文 replacement，用例前提不成立")
        return

    new_sql, applied = fixer.mechanical_fix(sql, findings)
    ok = new_sql == sql
    rec(PASS if ok else FAIL, "中文 fix 不机械套用",
        "SQL 未改动=%s ｜ 应用了 %s" % (ok, applied or "无"))


def t_alias_map(cfg: dict) -> None:
    """别名映射本身要正确：有别名用别名，没别名用表名。"""
    cases = [
        ("SELECT 1 FROM dim_user u WHERE u.x=1", "dim_user.is_current='Y'", "u.is_current"),
        ("SELECT 1 FROM dim_user WHERE x=1", "dim_user.is_current='Y'", "dim_user.is_current"),
        ("SELECT 1 FROM t AS x WHERE 1=1", "t.a=1", "x.a"),
    ]
    bad = []
    for sql, cond, want in cases:
        got = fixer.rewrite_alias(cond, sql)
        if not got.startswith(want):
            bad.append("%s → %s（期望 %s）" % (cond, got, want))
    rec(PASS if not bad else FAIL, "别名映射", "3 例 ｜ %s" % ("全对" if not bad else "; ".join(bad)))


def t_no_where_insert(cfg: dict) -> None:
    """没有 WHERE 时，条件要插在 GROUP BY 之前，不能拼到语句末尾。"""
    sql = "SELECT province, COUNT(*) FROM dim_user GROUP BY province"
    got = fixer._add_condition(sql, "is_current = 'Y'")
    ok = got.index("WHERE") < got.index("GROUP BY")
    rec(PASS if ok else FAIL, "无 WHERE 时插入位置", got[:90])


def t_max_attempts(cfg: dict) -> None:
    """重试上限必须生效：max_attempts=1 时，一个坏 SQL 只能生成一版。"""
    c = copy.deepcopy(cfg)
    c["limits"]["max_attempts"] = 1
    st = ask("x", c, sql="SELECT * FROM dwd_session WHERE dt = '2026-11-08'")
    ok = st.get("refused") and (st.get("attempts") or 0) <= 1
    rec(PASS if ok else FAIL, "重试上限生效",
        "attempts=%s refused=%s" % (st.get("attempts"), st.get("refused")))


# ---------------------------------------------------------------- 确定性：执行失败自愈
def t_exec_error_feedback(cfg: dict) -> None:
    """执行失败必须留下证据 + 递增自愈计数，不能只把原始异常显示出去。

    这条 SQL 护栏会放行（user_id 在别的表里存在、有 dt、有 LIMIT），
    但 dws_user_daily 全站一行、根本没有 user_id，Doris 一定报 Unknown column。
    """
    import node_execute
    st = {"sql": "SELECT user_id FROM dws_user_daily WHERE dt = '2026-11-08' LIMIT 5",
          "verdict": "pass", "attempts": 1, "exec_retries": 0}
    out = node_execute.execute(st, cfg)
    h = (out.get("history") or [{}])[0]
    err = (out.get("result") or {}).get("error") or ""
    ok = (bool(err) and out.get("exec_retries") == 1
          and h.get("stage") == "exec_error"
          and "EXEC-ERROR" in (h.get("rejects") or []))
    rec(PASS if ok else FAIL, "执行错误进入修复历史",
        "error=%s ｜ retries=%s ｜ stage=%s" % (
            err[:52], out.get("exec_retries"), h.get("stage")))


# ---------------------------------------------------------------- 依赖模型

def t_generate_dau(cfg: dict) -> None:
    """B 阶段：模型自己写 SQL，答案必须等于 docs/32 的基线 3,237。"""
    st = ask("2026-11-08 的 DAU 是多少", cfg)
    res = st.get("result") or {}
    cols = res.get("columns") or []
    val = (res.get("rows") or [[None]])[0][0] if res.get("rows") else None
    # 列可能是 dau / uv / cnt，取第一个数值列
    got = None
    if res.get("rows"):
        for i, c in enumerate(cols):
            if c.lower() in ("dau", "uv", "cnt", "user_cnt", "active_user_cnt"):
                got = res["rows"][0][i]
                break
        if got is None:
            got = val
    ok = got == 3237 and not st.get("refused")
    rec(PASS if ok else FAIL, "B 生成 DAU=3,237",
        "值=%s 列=%s 判定=%s 轮数=%s" % (got, cols, st.get("verdict"), st.get("attempts")))
    if not ok:
        print("      SQL: %s" % (st.get("sql") or "")[:200])


def t_repair_select_star(cfg: dict) -> None:
    """C 阶段：R9（SELECT *）机械修不了，必须由模型改写并在 3 轮内通过。"""
    st = ask("11-08 有哪些用户启动了 App", cfg,
             sql="SELECT * FROM dwd_session WHERE dt = '2026-11-08' LIMIT 10")
    versions = len([h for h in (st.get("history") or []) if h.get("sql")])
    ok = not st.get("refused") and not (st.get("result") or {}).get("error")
    rec(PASS if ok else FAIL, "C 模型修复 R9",
        "轮数=%s 版本=%s refused=%s" % (st.get("attempts"), versions, st.get("refused")))
    trail = [h.get("stage") for h in (st.get("history") or []) if h.get("sql")]
    print("      SQL 演化: %s" % " → ".join(trail))


def t_repair_history(cfg: dict) -> None:
    """多轮重试必须带上前几轮「试过什么、被哪条拦」。

    用户前端实测截图里连续 3 轮都是 REJECT 2 条最后拒答：
    每轮只看得见上一版 SQL 和本轮 findings，于是换个姿势又撞同一条规则。
    """
    import promptlib
    st = {
        "question": "昨天一共有多少人退款了",
        "metrics": [], "tables": ["dwd_pay"],
        "time_start": "2026-11-08", "time_end": "2026-11-08",
        "history": [
            {"attempt": 1, "stage": "generate",
             "sql": "SELECT COUNT(DISTINCT user_id) FROM dwd_pay WHERE dt='2026-11-08'"},
            {"attempt": 1, "stage": "check", "verdict": "reject",
             "rejects": ["R16-游客实体"]},
            {"attempt": 2, "stage": "llm_repair",
             "sql": "SELECT COUNT(DISTINCT user_id) AS c FROM dwd_pay d WHERE d.dt='2026-11-08'"},
            {"attempt": 2, "stage": "check", "verdict": "reject",
             "rejects": ["R16-游客实体", "R4-字段存在性"]},
        ],
    }
    u = promptlib.build_generate_messages(
        st, cfg,
        findings=[{"level": "REJECT", "rule": "R16-游客实体", "message": "游客未计入"}],
        prev_sql="SELECT COUNT(DISTINCT user_id) FROM dwd_pay d WHERE d.dt='2026-11-08'"
    )[1]["content"]
    ok = ("之前各轮试过的写法" in u and "第 1 轮被拦" in u
          and "第 2 轮被拦" in u and "不要再做同义替换" in u)
    rec(PASS if ok else FAIL, "重试 prompt 带失败历史",
        "两轮历史均可见=%s ｜ R16 重复出现=%s" % (ok, u.count("R16-游客实体")))


def t_dim_join_pk(cfg: dict) -> None:
    """R27：JOIN 维表必须按主键，不能用维度属性当连接键。

    C18 实测形态（2026-11-15）：模型写
        FROM dws_genre_daily g JOIN dim_song s ON s.genre = g.genre
    它想同时满足「当日」和「曲风」，但 dws_genre_daily 是曲风×天粒度、
    没有 song_id，只能拿 genre 去关联维表——等于把全部 240 首民谣拉进来，
    得 240（全曲库）而不是 237（当日真被播放过的）。5 次里错 4 次，
    错法完全一致，说明这不是随机波动。
    R7 只查「有没有 ON 条件」，挡不住「ON 写了但键不对」，所以补这条。
    """
    import guardtool
    bad = ("SELECT COUNT(DISTINCT s.song_id) FROM dws_genre_daily g "
           "JOIN dim_song s ON s.genre = g.genre WHERE g.dt = '2026-11-08' "
           "AND g.genre = '民谣' AND g.play_start_cnt > 0")
    good = ("SELECT COUNT(DISTINCT p.song_id) FROM dws_song_play_daily p "
            "JOIN dim_song s ON s.song_id = p.song_id WHERE p.dt = '2026-11-08' "
            "AND s.genre = '民谣'")
    sub = ("SELECT COUNT(DISTINCT song_id) FROM dws_song_play_daily "
           "WHERE dt = '2026-11-08' AND song_id IN "
           "(SELECT song_id FROM dim_song WHERE genre = '民谣')")

    v, fs, _ = guardtool.check(bad, cfg, question="11-08 民谣曲风有多少首歌被播放过")
    hit = [f for f in fs if str(f.get("rule", "")).startswith("R27")]
    rec(PASS if (v == "reject" and hit) else FAIL, "R27 拦 genre 关联维表",
        "verdict=%s 命中=%d" % (v, len(hit)))

    # 必须同时证明「不误伤」：主键 JOIN 和 IN 子查询都是合法写法。
    # 只测「能拦住」的规则很容易写成宁杀错不放过的噪音源。
    for _name, _sql in (("主键 JOIN", good), ("IN 子查询", sub)):
        _v, _fs, _ = guardtool.check(_sql, cfg, question="11-08 民谣曲风有多少首歌被播放过")
        _r27 = [f for f in _fs if str(f.get("rule", "")).startswith("R27")]
        rec(PASS if not _r27 else FAIL, "R27 不误伤：%s" % _name,
            "verdict=%s R27=%d" % (_v, len(_r27)))


def t_end_to_end_join(cfg: dict) -> None:
    """端到端：一个必须 JOIN 且必须收敛的问题，看模型会不会自己踩 R18。"""
    st = ask("2026-11-08 各省份的活跃用户数", cfg)
    res = st.get("result") or {}
    ok = not st.get("refused") and bool(res.get("rows"))
    hits = _rejects(st)
    rec(PASS if ok else FAIL, "端到端 省份维度",
        "行数=%s refused=%s 最终判定=%s" % (res.get("row_count"), st.get("refused"),
                                          st.get("verdict")))
    for h in (st.get("history") or []):
        if h.get("stage") == "check" and h.get("rejects"):
            print("      第 %s 轮被拦：%s" % (h.get("attempt"), ",".join(h["rejects"])))


def main() -> int:
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    quick = "--quick" in sys.argv
    cfg = load_config()

    print("== B/C 阶段自测（%s）==" % ("quick，跳过模型用例" if quick else "全量"))
    print("\n-- 确定性用例 --")
    t_alias_map(cfg)
    t_no_where_insert(cfg)
    t_mechanical_fanout(cfg)
    t_mechanical_skip_cjk(cfg)
    t_max_attempts(cfg)
    t_repair_history(cfg)
    t_dim_join_pk(cfg)
    t_exec_error_feedback(cfg)

    if not quick:
        print("\n-- 依赖模型的用例 --")
        t_generate_dau(cfg)
        t_repair_select_star(cfg)
        t_end_to_end_join(cfg)
    else:
        rec(SKIP, "B 生成 DAU", "--quick 跳过")
        rec(SKIP, "C 模型修复 R9", "--quick 跳过")
        rec(SKIP, "端到端 省份维度", "--quick 跳过")

    bad = [r for r in rows if r[0] == FAIL]
    print("\n%d/%d 通过，%d 跳过" % (len(rows) - len(bad), len(rows),
                                     sum(1 for r in rows if r[0] == SKIP)))
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())
