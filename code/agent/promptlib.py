"""Prompt 构建。

设计原则（每一条都对应一次踩坑）：

1. **上下文只给"能用得上的"**。31 张表全塞进去约 1.2 万 token，模型反而
   抓不住重点，而且贵。这里只给 route 命中的候选表 + 它们的 JOIN 目标表。
2. **指标定义连同 trap 一起给**。语义层的 trap 字段是给人看的"别踩这个坑"，
   对模型同样有效——arpu 那条 trap 里写清了"分母是人日不是去重 UV"。
3. **铁律要短**。20 条护栏全写进 prompt 等于没写，模型会全部忽略。
   这里只放最常触发的 9 条，其余靠护栏事后拦 + fix 回填。
4. **要求 JSON 输出**。SQL 里可能有换行、引号、中文别名，
   纯文本输出再加分隔符解析必然出错。

B（首次生成）和 C（失败重写）共用同一份上下文，只是 C 多带一段
"上一版错在哪"的证据。这一点很重要：**不给模型看它自己写的那版，
它会换个姿势再错一次**。
"""
from __future__ import annotations

import _bootstrap  # noqa: F401

import re

import semantic
import shape
from governance.contracts import as_decision

_RE_REF = re.compile(r"[a-z][a-z0-9_]{2,}")
_SQL_WORDS = {"sum", "count", "avg", "min", "max", "distinct", "case", "when",
              "then", "else", "end", "nullif", "coalesce", "cast", "round",
              "and", "not", "for", "the", "over", "from", "where"}


def _formula_refs(m: dict, cfg: dict) -> list[tuple[str, dict]]:
    """把 derived 指标 formula 里引用的其他指标展开出来。

    实测 arpu 的 trap 明明写了「分母是 Σ 日 UV，别用 dws_revenue_daily.arpu」，
    模型还是用 pay_user_cnt 当分母算出 ARPPU。原因是 trap 是叙述性的，
    而公式里的 ``uv`` 到底怎么算，模型得自己去别处找。这里把引用指标的定义
    **就地展开**——分子分母各自的公式、来源表、过滤条件一次给全。
    """
    out = []
    for tok in _RE_REF.findall(m.get("formula") or ""):
        if tok in _SQL_WORDS:
            continue
        ref = semantic.get_metric(tok, cfg)
        if ref:
            out.append((tok, ref))
    return out

# ---------------------------------------------------------------- 铁律
# 每条都来自 agent/guards/guard.py 里真实存在的规则，不是凭空写的口号。
# 规则编号写在后面，方便排查"模型为什么这么写"。
RULES = """\
【必须遵守的铁律】
1. 只输出单条 SELECT 或 WITH 查询，不要分号，不要多语句，不要注释。         (R1/R2)
2. 表名**裸写**，不加 hive_catalog. / music_dw. / internal. 前缀——          (R17)
   连接初始化时已经 SWITCH hive_catalog + USE music_dw，加前缀反而会落错库。
3. 凡是按 dt 分区的表，WHERE 里必须有 dt 条件，禁止全表扫描。               (R6)
4. 查 dwd 明细表且没有聚合、没有 GROUP BY 时，必须带 LIMIT ≤ 1000。         (R5)
5. 禁止 SELECT *（ADS 宽表除外），只取需要的列。                            (R9)
6. JOIN dim_user 必须收敛到单版本：加 dim_user.is_current = 'Y'，           (R18)
   因为它是 SCD2 拉链表（115 万行 / 100 万用户），不收敛会让聚合结果虚高。
7. 比率类指标不能 SUM：要 SUM(分子)/SUM(分母) 重算。                        (R20)
8. 当日去重实体（dau / *_uv / *_user_cnt）跨日相加等于"人日"，
   不等于窗口去重 UV。两者差 4.87 倍，别混用。                              (R20)
9. 只 JOIN 下面【关系】里登记过的表，并且按给定的 join_on 写 JOIN 条件。     (R7)
10. 收入类：dwd_pay 上算金额必须覆盖 vip_subscribe + order_pay 两个事件，   (R25)
    退款（order_refund）amount 是正数，要手工取负；只取 order_pay 会低 5.24 倍。
"""

OUTPUT_SPEC = """\
【输出格式】
严格输出一个 JSON 对象，不要任何额外文字：
{
  "sql": "完整可执行的 SQL，单条语句",
  "used_tables": ["实际用到的表名"],
  "reason": "一句话说明口径选择（例如：用 dws_user_daily.dau 而非 dwd_play_start 去重）"
}
"""

# CoT 版：把思考拆成结构化字段，且**放在 sql 之前**。
# 为什么不用"请一步一步思考"这种自由文本：模型会写出一大段散文，还得再解析一遍，
# 而且这段话不会进入任何下游校验。结构化 plan 的每个字段都能被 validate 事后核对
# （比如 plan.source_table 必须出现在 SQL 里），思维链才不只是"看起来更努力"。
OUTPUT_SPEC_COT = """\
【输出格式】
严格输出一个 JSON 对象，不要任何额外文字。**plan 必须写在 sql 前面**，
先把口径想清楚，再落笔写 SQL：
{
  "plan": {
    "metric": "用哪个指标口径（指标码或聚合表达式）",
    "source_table": "主表（必须在上面给出的候选表里）",
    "joins": ["需要 JOIN 的表和关联键，没有就空数组"],
    "filters": ["除时间窗以外的过滤条件，没有就空数组"],
    "time_condition": "时间过滤的写法",
    "shape": "结果形态：scalar | detail | ranking | comparison | ratio | trend | distribution"
  },
  "sql": "完整可执行的 SQL，单条语句",
  "used_tables": ["实际用到的表名，必须与 plan 一致"],
  "reason": "一句话说明口径选择"
}
"""

# 实时链路历史快照：数据面是 internal.music_dw，与默认的 hive_catalog.music_dw
# 不是同一份数据（同名表差 6.2 倍）。表名写法、时间列、混查禁令全部反过来，
# 所以不能复用离线铁律——照抄第 2 条会写出裸表名，直接落到离线库。
RT_RULES = """\
【必须遵守的铁律（实时链路历史快照）】
1. 只输出单条 SELECT 或 WITH 查询，不要分号，不要注释。                    (R1/R2)
2. 实时表**必须写全三段式** internal.music_dw.<表名>，禁止裸写表名——       (R17)
   连接默认停在 hive_catalog.music_dw，裸写会落到同名的离线表上。
3. WHERE 必须带该表的时间列：ops_* 与 ads_*_1min 用 stat_minute，          (R26)
   ads_*_10min 用 window_start，ads_realtime_overview 用 stat_date，
   dwd_quality_error 用 dt 或 event_ts。时间窗见下方，禁止不带过滤扫全表。
4. 不得与离线表 JOIN / UNION，也不要写 `dt = '...'` 这类离线分区条件。      (R26)
5. 比率列（error_rate / bounce_rate / skip_rate / play_share）不能 SUM/AVG，
   要 SUM(分子)/SUM(分母) 重算。                                          (R8/R20)
6. 禁止 SELECT *；明细表无聚合时带 LIMIT ≤ 1000。                         (R9/R5)
7. 这是 2026-11-08 之前的历史快照，链路已停止产出：reason 里不要出现
   "当前/现在/实时结果"，只描述查的是哪个历史窗口。
"""

SYSTEM = """\
你是音乐 App 数仓的问数助手。你的唯一任务是把业务问题翻译成一条能在
Doris 上直接执行的 SQL。

{rules}
"""


# ---------------------------------------------------------------- 上下文块

def _visible(decision, table: str) -> bool:
    if decision is None or decision.unrestricted:
        return True
    if not decision.allow_tables or "*" in decision.allow_tables:
        return str(table).lower() not in decision.deny_tables
    return str(table).lower() in decision.allow_tables


def _alt_tables(code: str, cfg: dict, decision, question: str = "",
                limit: int = 3) -> list[str]:
    """来源表不可见时，找可见表里含同名列的几张来替代。

    原子指标的 code 通常就是列名（play_start_cnt / dau / revenue），所以
    "列存在"是最硬的相关性证据。只给一句"这张表你看不到"没用——模型必须
    知道**改看哪张**，否则它只会换个写法再撞一次 R21。

    这里必须再过一次 not_for：ads_song_ranking 有 play_start_cnt 列，若不过滤
    就会在"播放量"类问题里被当成替代表推荐出去——而它按完播率排序，等于
    把正确答案换成错误答案。
    """
    if decision is None or decision.unrestricted:
        return []
    col = str(code).lower()
    out = []
    for t in sorted(decision.allow_tables):
        if not _visible(decision, t):
            continue
        tb = semantic.get_table(t, cfg) or {}
        cols = [str(c).lower() for c in (tb.get("columns") or [])]
        if col in cols:
            out.append(t)
        if len(out) >= limit:
            break
    return semantic.excluded_by_intent(out, question, cfg)


def _metric_block(code: str, cfg: dict, decision=None, question: str = "") -> str:
    m = semantic.get_metric(code, cfg) or {}
    lines = ["### 指标 %s —— %s" % (code, m.get("name_cn") or "")]
    if m.get("formula"):
        lines.append("  公式：%s" % m["formula"])
    if m.get("filter"):
        lines.append("  过滤：%s" % m["filter"])
    src = m.get("source") or ""
    if src:
        lines.append("  来源表：%s%s" % (src, "" if _visible(decision, src)
                                        else "（当前岗位不可见，禁止使用）"))
    if m.get("sources"):
        vis = [s for s in m["sources"] if _visible(decision, s)]
        hid = [s for s in m["sources"] if not _visible(decision, s)]
        if vis:
            lines.append("  来源表：%s" % ", ".join(vis))
        if hid:
            lines.append("  来源表（当前岗位不可见，禁止使用）：%s" % ", ".join(hid))
    # 来源被挡时必须给出可执行的替代，否则模型只能瞎猜或反复撞墙
    if src and not _visible(decision, src):
        alt = _alt_tables(code, cfg, decision, question)
        if alt:
            lines.append("  ★ 改用这些可见表（含同名列 %s）：%s" % (code, ", ".join(alt)))
    if m.get("ads"):
        vis_ads = [a for a in m["ads"] if _visible(decision, str(a).split(".")[0])]
        if vis_ads:
            lines.append("  ★ 现成 ADS 列（**最优先**：直接 SELECT 该列，别自己重算）：%s"
                         % ", ".join(vis_ads))
    lines.append("  可加性：%s（full=可跨日相加 / semi=人日 / stock=存量 / none=比率）"
                 % (m.get("additivity") or "未标注"))
    if m.get("additivity") == "none":
        # 2026-11-09 E 阶段评测 V18：arpu 的 trap 明明写了
        # 「dws_revenue_daily.arpu 实为 ARPPU」，模型还是用 pay_user_cnt 当分母。
        # trap 是叙述性的，容易被当成背景略过；这里补一条**可执行的硬约束**。
        lines.append("  !! 【口径硬约束】上面的 formula 分子分母必须逐字照用，"
                     "不得换成「看起来相关」的其他去重口径，"
                     "也不得直接取库里的同名列（列名相同 ≠ 口径相同）。")
        lines.append("  !! 【比率的写法】分子、分母各自在**自己的来源表**上独立聚合"
                     "（标量子查询或 CTE），最后相除；"
                     "禁止把两张明细表 JOIN 后一起 COUNT——"
                     "dwd_play_start 与 dwd_play_end 的 event_id 不同，JOIN 会全落空，"
                     "CROSS JOIN 又会把分母放大上千倍。")
        refs = _formula_refs(m, cfg)
        if refs:
            lines.append("  formula 引用到的指标定义（就地展开，按各自定义实现）：")
            for tok, r in refs:
                lines.append("    - %s = %s ｜ 表 %s%s" % (
                    tok, r.get("formula") or "—",
                    r.get("source") or ", ".join(r.get("sources") or []) or "—",
                    " ｜ 过滤 " + r["filter"] if r.get("filter") else ""))
    if m.get("trap"):
        lines.append("  !! 陷阱：%s" % m["trap"])
    if m.get("note"):
        lines.append("  口径说明：%s" % m["note"])
    if m.get("baseline") is not None:
        lines.append("  已知基线：%s　（**仅供参考，不是你这次要算的值**）%s"
                     % (m["baseline"], m.get("baseline_note") or ""))
    return "\n".join(lines)


def _table_block(name: str, cfg: dict) -> str:
    return semantic.describe_table(name, cfg)


def _relation_block(tables: set[str], cfg: dict) -> str:
    """只列**两端都在候选表里**的关系边。

    这里踩过一个隐蔽的坑：`relationships.from` 是**列表**（一条边从多张表指向
    同一张维表），原来写的是 `r.get("from") in tables`——list 永远不等于某个
    表名，于是匹配完全靠 `to`。后果是只要 dim_song 在候选里，所有指向它的边
    都会被列出来，包括那些**已被权限或语义边界剔除**的表：
    产品经理问「播放量排行榜」时，prompt 里凭空出现
    `ads_song_ranking → dim_song`，模型照着写了这条表，然后被 R21 拦掉。
    约束在选表阶段生效、却在 prompt 组装阶段被关系块泄漏回去，等于没做。
    """
    rels = semantic.all_relationships(cfg) if hasattr(semantic, "all_relationships") else []
    lines = []
    for r in rels:
        fr = r.get("from")
        froms = [x for x in (fr if isinstance(fr, list) else [fr]) if x]
        to = r.get("to")
        # 两端都必须可用，否则这条边在本次查询里根本用不上
        usable = [x for x in froms if x in tables]
        if not usable or not to or to not in tables:
            continue
        line = "  %s → %s : %s" % ("/".join(usable), to, r.get("join_on"))
        if r.get("converge"):
            line += "　必须加 %s" % r["converge"]
        lines.append(line)
    if not lines:
        return ""
    return "【关系】（JOIN 时按这里的条件写，不要自己猜）\n" + "\n".join(lines)


def _time_block(state: dict, cfg: dict) -> str:
    """D 阶段：时间从「单日」升级成「区间 + 裁剪说明 + 人日警告」。"""
    w = state.get("time_window") or {}
    off = cfg["lane"]["offline"]
    day = state.get("time_end") or off["data_until"]
    if state.get("lane") == "realtime":
        import realtimelane
        s = realtimelane.spec(cfg)
        return "\n".join([
            "【本次查询的数据面】实时链路历史快照（internal.music_dw）",
            "  可查窗口：%s ~ %s" % (s.get("data_since"), s.get("data_until")),
            "  时间列：按表选 stat_minute / window_start / stat_date / dt（R26 强制过滤）",
            "  !! 链路已停止产出，不代表当前时刻；答案里必须带新鲜度声明",
            "  !! 不得与离线表混查，表名必须写 internal.music_dw.<表名>",
        ])
    hint = w.get("sql_hint") or "dt = '%s'" % day
    lines = [
        "【本次查询的时间窗口】",
        "  %s" % hint,
        "  解析：%s" % (w.get("label") or "单日 %s" % day),
        "  数据可用范围：%s ~ %s（离线 T+1）"
        % (off.get("data_since") or "2026-10-05", off["data_until"]),
    ]
    if w.get("requested"):
        lines.append("  用户原本想要：%s ~ %s" % tuple(w["requested"]))

    # 阶段 4：对比问题。基线日期由 timeparse 解析、由 execute 复算，
    # 模型只负责查「当前窗口」这一个数。以前这里没有这段，模型只能自己
    # 把基线日期抄进 SQL——抄错一个数字，答案看起来完全正常。
    cmp = w.get("compare") or {}
    if cmp:
        lines.append("  对比基线：%s ｜ %s" % (cmp.get("label"), cmp.get("sql_hint")))
        if cmp.get("auto"):
            lines.append(
                "  !! 这是对比问题，但**只需要查当前窗口**（%s）。基线与差值由系统另算，"
                "SQL 里不要出现基线日期，也不要在答案里自己算差值。"
                % (w.get("sql_hint") or ""))
        else:
            lines.append("  !! 基线无法自动对比：%s。SQL 仍只查当前窗口。"
                         % (cmp.get("note") or "基线窗口不可用"))

    if w.get("note"):
        lines.append("  !! %s" % w["note"])
    if w.get("is_range"):
        lines.append(
            "  !! 多日窗口：dau / *_uv / *_user_cnt 这类当日去重实体跨日相加是「人日」，"
            "不是窗口去重人数；比率类必须 SUM(分子)/SUM(分母)，不能直接 SUM(比率)。")
    return "\n".join(lines)


def _glossary_hits_block(state: dict, cfg: dict) -> str:
    """把命中的业务词典同义词及其 note 摆进 prompt。

    踩过的坑：glossary 里写了「消费」的词条和口径说明，但 ``find_metrics``
    只返回 ``matched_by`` 字符串，``_metric_block`` 输出的又是**指标**的 note，
    词典自己的 note 从来没进过 prompt。结果模型照样把「消费最多」理解成
    「播放次数最多」。同义词的歧义说明必须跟着命中一起送到模型眼前。
    """
    hits = [h for h in (state.get("metric_hits") or [])
            if str(h.get("matched_by") or "").startswith("glossary:")]
    if not hits:
        return ""
    terms = semantic.terms(cfg)
    lines = ["【业务词典命中（同义词口径以这里为准，优先于字面理解）】"]
    for h in hits:
        word = h["matched_by"].split(":", 1)[1]
        ref = terms.get(word) or {}
        line = "  「%s」→ 指标 %s" % (word, h.get("code"))
        if ref.get("note"):
            line += "；口径：%s" % ref["note"]
        lines.append(line)
    return "\n".join(lines)


def _dimension_block(state: dict, cfg: dict) -> str:
    """维度层里的 caution（如「分省 DAU 求和 > 全站 DAU」）必须进 prompt。"""
    cs = state.get("dimension_cautions") or []
    if not cs:
        return ""
    return "【这次分组的已知陷阱】\n" + "\n".join("  !! " + c for c in cs)


def _pick_tables(state: dict, cfg: dict) -> list[str]:
    """候选表 + 命中维度的维表 + 关系边目标表，最多 6 张。

    A 阶段这里手写了一串「用户/省份/歌」关键词判断；D 阶段换成
    dimensions.yaml 的维度命中结果——维度清单是实测过的，关键词是拍脑袋的。
    """
    want: list[str] = []
    for t in (state.get("tables") or []):
        if t and t not in want:
            want.append(t)
    for code in (state.get("metrics") or []):
        m = semantic.get_metric(code, cfg) or {}
        cand = ([m.get("source")] if m.get("source") else [])
        cand += list(m.get("sources") or []) + list(m.get("ads") or [])
        for t in cand:
            if t and t not in want:
                want.append(t)

    # 只有真命中维度，才把维表带进来；纯指标问题不白占 token
    dims = semantic.all_dimensions(cfg)
    for code in (state.get("dimensions") or []):
        for t in (dims.get(code) or {}).get("tables") or []:
            if t.startswith("dim_") and t not in want:
                want.append(t)

    # 关系边的目标表也要给字段，否则模型只能猜 JOIN 列（R7 会拦）
    for r in semantic.all_relationships(cfg):
        to = r.get("to")
        if to and to.startswith("dim_") and (r.get("from") in want) and to not in want:
            want.append(to)

    # 最后一道：裁到角色可见范围。前面几段会按指标 source 把 dwd_ 明细表
    # 加回来——对 data_pm 这类只看汇总的岗位，加了等于让模型写
    # 一条必被 R21 拦掉的 SQL。
    d = as_decision(state.get("policy_decision"))
    if not d.unrestricted and d.allow_tables and "*" not in d.allow_tables:
        want = [t for t in want if t.lower() in d.allow_tables]

    # 语义边界：与"谁能看"无关，是"这表能不能回答这个问题"。
    # 指标 source 会把表加回来，所以这里必须再过滤一次，否则 route 剔完又被加回。
    out = [t for t in want if semantic.get_table(t, cfg)][:6]
    return semantic.excluded_by_intent(out, state.get("question") or "", cfg)


def context_block(state: dict, cfg: dict) -> str:
    parts = []

    parts = [_time_block(state, cfg)]

    # 阶段 3：答案形态放在时间之后、指标之前。
    # 模型读上下文是"先定框架再填内容"，形态就是框架；
    # 放在指标定义后面容易被一堆口径细节淹没。
    contract = shape.classify(state.get("question") or "")
    cmp = (state.get("time_window") or {}).get("compare") or {}
    if cmp:
        contract = shape.compare_contract(contract, bool(cmp.get("auto")))
    cb = shape.contract_block(contract)
    if cb:
        parts.append(cb)

    gloss = _glossary_hits_block(state, cfg)
    if gloss:
        parts.append(gloss)

    dim = _dimension_block(state, cfg)
    if dim:
        parts.append(dim)

    codes = state.get("metrics") or []
    d = as_decision(state.get("policy_decision"))
    q = state.get("question") or ""
    if codes:
        parts.append("【命中的指标定义】\n"
                     + "\n\n".join(_metric_block(c, cfg, d, q) for c in codes))
    else:
        parts.append("【命中的指标定义】\n  （语义层没有精确命中，请按问题字面含义自己判断口径，"
                     "并在 reason 里说明你的口径选择）")

    tables = _pick_tables(state, cfg)
    if tables:
        parts.append("【可用表】\n\n" + "\n\n".join(_table_block(t, cfg) for t in tables))
        rel = _relation_block(set(tables), cfg)
        if rel:
            parts.append(rel)

    parts.append(
        "【执行环境】\n"
        "  Doris，连接建立时已执行：SWITCH hive_catalog; USE music_dw;\n"
        "  所以 SQL 里的表名直接写 dws_user_daily 这种，不要带库前缀。"
    )
    contrast = _contrast_block(state.get("question") or "")
    if contrast:
        parts.append(contrast)
    return "\n\n".join(parts)


_RE_DIFF = re.compile(
    r"比.{0,14}(多|少|高|低|涨|增加|减少|增长|下降|差)|"
    r"多多少|少多少|差多少|相差|差值|差距|环比|同比|变化了多少")


def _contrast_block(q: str) -> str:
    """「A 比 B 多多少」要的是一个**数**，不是两行明细。

    实测：这类问题模型固定输出 `GROUP BY dt` 的两行，让用户自己去减。
    结果数值上没错，但答案不可用——评测里表现为「取不到数值」。
    这里把"单标量差值"的要求写死在上下文里，只在命中对比句式时出现，
    避免给普通单日问题增加无关约束。
    """
    if not _RE_DIFF.search(q or ""):
        return ""
    return (
        "【这是一个「对比/差值」问题——必须返回单个数字】\n"
        "  不要 GROUP BY dt 返回两行让用户自己减。要直接算出差值，例如：\n"
        "    SELECT (SELECT COUNT(*) FROM t WHERE dt='2026-11-08')\n"
        "         - (SELECT COUNT(*) FROM t WHERE dt='2026-11-07') AS diff\n"
        "  符号约定（按中文语义，不要一律 A-B）：\n"
        "    · 「A 比 B 多/高/增长多少」→ A - B\n"
        "    · 「A 比 B 少/低/下降多少」→ B - A（返回**正数幅度**，因为问的是「少了多少」）\n"
        "    · 「相差多少」→ 取绝对值\n"
        "  只返回一列一行。"
    )


# ---------------------------------------------------------------- 两个入口

def _attempt_history_block(state: dict) -> str:
    """把前几轮「试过什么、被哪条规则拦下」摆给模型看。

    用户实测截图里出现过连续 3 轮都是 REJECT 2 条、最后耗尽重试拒答：
    每轮 generate 只看得见**上一版** SQL 和**本轮** findings，
    于是换个姿势又撞同一条规则。修复闭环缺的不是再试一次，是记忆。
    """
    hist = state.get("history") or []
    by_round: dict[int, dict] = {}
    for h in hist:
        n = h.get("attempt")
        if n is None:
            continue
        rec = by_round.setdefault(n, {"sql": "", "rejects": []})
        if h.get("sql") and not rec["sql"]:
            rec["sql"] = h["sql"]
        if h.get("rejects"):
            rec["rejects"] = h["rejects"]
    done = [(n, r) for n, r in sorted(by_round.items()) if r["rejects"]]
    if not done:
        return ""
    lines = ["【之前各轮试过的写法——全部被拦，不要重复】"]
    for n, r in done[-2:]:
        lines.append("  第 %s 轮被拦：%s" % (n, "、".join(r["rejects"]) or "—"))
        if r["sql"]:
            lines.append("    " + r["sql"].strip().replace("\n", " ")[:260])
    lines.append("  若同一条规则在多轮重复出现，说明只是换了写法却没改掉根因；"
                 "请直接按该规则的 fix 改，不要再做同义替换。")
    return "\n".join(lines)


def _boundary_block(state: dict, cfg: dict, findings: list[dict]) -> str:
    """C 阶段修复专用：把「能用哪些表、哪些列」再贴一遍，紧贴输出要求。

    实测踩过的坑：模型修 R9（SELECT *）时，因为上下文里表结构离得远，
    自己编出 `dws_user_daily.user_id` / `is_active` / `app_launch_cnt`，
    被 R4 连续拦两轮后耗尽重试。修复请求必须把边界放在模型最后看到的位置。
    """
    tables = _pick_tables(state, cfg)
    if not tables:
        return ""
    lines = ["【重写边界：只能用下列表，以及表里真实存在的列】"]
    for t in tables:
        cols = semantic.table_columns(t, cfg)
        shown = ", ".join(cols[:24])
        if len(cols) > 24:
            shown += " …（共 %d 列）" % len(cols)
        lines.append("  %s: %s" % (t, shown))
    if any(str(f.get("rule") or "").startswith("R4") for f in findings or []):
        lines.append("  !! 上一版用了语义层里不存在的列，已被 R4 拦下；"
                     "不要自己造列名，只从上面清单里挑。")
    return "\n".join(lines)


def _result_echo_block(state: dict) -> str:
    """把上一版**实际返回的结果**摆给模型看。

    形态返工与护栏返工的区别在这里：护栏错的时候模型只需要看 SQL，
    形态错的时候它必须看到"自己给出的东西长什么样"——否则它不知道
    用户为什么会觉得答错了，只会把同一句 SQL 换个写法再交一次。
    """
    res = state.get("result") or {}
    if not res or res.get("error"):
        return ""
    cols = res.get("columns") or []
    rows = res.get("rows") or []
    if not cols:
        return ""
    lines = ["【上一版实际返回的结果】",
             "  %d 行 × %d 列：%s" % (len(rows), len(cols), ", ".join(map(str, cols)))]
    for r in rows[:3]:
        lines.append("    " + " | ".join(str(v) for v in r))
    if len(rows) > 3:
        lines.append("    …（共 %d 行）" % len(rows))
    return "\n".join(lines)


def dialogue_block(state: dict, limit: int = 3) -> str:
    """前几轮问答。空历史返回空串——单轮请求的 prompt 因此逐字不变。

    只给"够理解指代"的信息：问句、答案摘要、窗口、指标。
    不放 SQL 全文和 trace：那些对生成当前 SQL 没有帮助，只烧 token。
    """
    turns = (state.get("history_turns") or [])[-limit:]
    if not turns:
        return ""
    lines = ["【前几轮对话】仅用于理解本轮指的是什么，不要复用其中的数字"]
    for i, t in enumerate(turns, 1):
        q = (t.get("question") or "").strip().replace("\n", " ")[:120]
        a = (t.get("answer") or "").strip().replace("\n", " ")[:160]
        bits = []
        tw = (t.get("time_window") or {}).get("label") if isinstance(t.get("time_window"), dict) else ""
        if tw:
            bits.append("窗口 %s" % tw)
        ms = "、".join(t.get("metrics") or [])
        if ms:
            bits.append("指标 %s" % ms)
        lines.append("  %d. 问：%s" % (i, q))
        lines.append("     答：%s%s" % (a, ("（%s）" % "，".join(bits)) if bits else ""))

    fu = state.get("followup") or {}
    parts = []
    if fu.get("metrics"):
        parts.append("指标 %s" % "、".join(fu["metrics"]))
    if fu.get("time"):
        parts.append("时间窗口 %s ~ %s" % (fu["time"].get("start"), fu["time"].get("end")))
    if parts:
        lines.append("")
        lines.append("【本轮是追问】问句里没写明的 %s 已按上一轮补上，不要再反问用户；"
                     "但本轮一旦明确换了对象（别的指标 / 地区 / 时间），以本轮为准。"
                     % "、".join(parts))
    return "\n".join(lines)


def _failcase_block(state: dict, cfg: dict, findings: list[dict]) -> str:
    """注入同类失败案例。

    只在**修复路径**注入。首次生成不注入，理由有两条：
    ① 那等于提前告诉模型"你会在哪儿错"，128 题基线就不是干净的了；
    ② 案例是"别人在别的问题上踩的坑"，直接塞进首次生成会让它照着例子抄，
       而不是去读语义层。
    """
    fc = (cfg or {}).get("failcase") or {}
    if not fc.get("enabled", True):
        return ""
    try:
        import failcase
        cases = failcase.retrieve(
            state.get("question") or "", findings,
            top=int(fc.get("top_k") or 2), cfg=cfg)
        return failcase.render_block(cases)
    except Exception:
        # 知识库坏了不能把修复链路一起带崩：它只是"多知道一点"，不是必需环节。
        return ""


def _plan_block(state: dict, cfg: dict) -> str:
    """把 Planning 阶段的产物注入生成提示词。

    plan 无效（被规则 REJECT）时不注入：一份自己都过不了校验的计划，
    塞给模型只会把 SQL 一起带歪。
    """
    if not ((cfg.get("prompt") or {}).get("plan") or {}).get("enabled"):
        return ""
    p = state.get("plan") or {}
    if not p or state.get("plan_valid") is False:
        return ""
    lines = ["【查询计划（已通过规则校验，必须严格遵守）】"]
    if p.get("intent"):
        lines.append("  意图：%s" % str(p["intent"])[:120])
    if p.get("metrics"):
        lines.append("  指标：%s" % "、".join(str(x) for x in p["metrics"][:6]))
    if p.get("dimensions"):
        lines.append("  维度：%s" % "、".join(str(x) for x in p["dimensions"][:6]))
    if p.get("tables"):
        lines.append("  用表：%s" % "、".join(
            str(x).split(".")[-1] for x in p["tables"][:6]))
    if p.get("filters"):
        lines.append("  过滤：%s" % "；".join(str(x) for x in p["filters"][:6]))
    if p.get("time_condition"):
        lines.append("  时间：%s" % str(p["time_condition"])[:120])
    if p.get("shape"):
        lines.append("  形态：%s" % p["shape"])
    lines.append("按上面的计划写 SQL；计划里定下的口径不要再改。")
    return "\n".join(lines)


def _fewshot_block(state: dict, cfg: dict) -> str:
    """Verified Query few-shot。只在首次生成时用，修复路径不注入。

    修复路径已经有明确的 findings 和上一版 SQL，那是最具体的上下文；再塞两条
    "别人怎么写的"只会稀释它。这个判断来自失败案例库那次 A/B 的教训：
    护栏 fix 已经说清改法时，再补案例是纯重复信息（6/6 对 6/6，token +6.8%）。
    """
    pc = (cfg.get("prompt") or {}).get("fewshot") or {}
    if not pc.get("enabled", False):
        return ""
    try:
        import verified
        ex = verified.retrieve_similar(
            state.get("question") or "", cfg,
            top=int(pc.get("top", 2)),
            min_score=float(pc.get("min_score", 2.0)))
        return verified.render_block(ex)
    except Exception:
        # few-shot 是增强不是依赖：库坏了就当作没有，不能让问数挂掉
        return ""


def build_generate_messages(state: dict, cfg: dict,
                            findings: list[dict] | None = None,
                            prev_sql: str = "") -> list[dict]:
    """B 阶段：首次生成。findings 非空时自动升级为 C 阶段的修复请求。"""
    messages = [{"role": "system", "content": SYSTEM.format(
        rules=RT_RULES if state.get("lane") == "realtime" else RULES)}]

    user = [
        "【业务问题】",
        state.get("question") or "",
        "",
    ]
    # 多轮上下文放在本轮的上下文之前：先看聊过什么，再看这次要什么。
    # 单轮时这里什么都不加，拼接结果与没有记忆时完全一致。
    dlg = dialogue_block(state)
    if dlg:
        user.append(dlg)
        user.append("")
    # 长期偏好：本人显式要求记住的口径。只用于理解意图，不含任何历史数字。
    # 单轮时为空，prompt 逐字不变。
    try:
        import memory as _memory
        mem = _memory.memory_block(state, cfg)
    except Exception:
        mem = ""
    if mem:
        user.append(mem)
        user.append("")
    user.append(context_block(state, cfg))

    plan_blk = _plan_block(state, cfg)
    if plan_blk:
        user.append("")
        user.append(plan_blk)

    if not (findings and prev_sql):
        shots = _fewshot_block(state, cfg)
        if shots:
            user.append("")
            user.append(shots)

    if findings and prev_sql:
        user.append("")
        user.append("【上一版 SQL 未通过校验】")
        user.append(prev_sql.strip())
        user.append("")
        user.append("【护栏给出的具体问题】")
        for f in findings:
            if f.get("level") != "REJECT":
                continue
            user.append("  - [%s] %s" % (f.get("rule"), f.get("message")))
            fx = f.get("fix") or {}
            if fx.get("replacement"):
                user.append("    建议改法：%s → %s" % (fx.get("op"), fx.get("replacement")))

        if any(f.get("rule") == "SHAPE" for f in findings):
            echo = _result_echo_block(state)
            if echo:
                user.append("")
                user.append(echo)
            user.append("")
            user.append("上一版 SQL 本身没被护栏拦下，错在**答案形状**："
                        "SQL 能跑、数字也有，但它回答的不是用户问的那个形状。"
                        "请按【答案形态】一节重写，重点改 GROUP BY / ORDER BY / LIMIT 这几处，"
                        "不要动已经算对的口径。")

        history = _attempt_history_block(state)
        if history:
            user.append("")
            user.append(history)

        fails = _failcase_block(state, cfg, findings)
        if fails:
            user.append("")
            user.append(fails)

        boundary = _boundary_block(state, cfg, findings)
        if boundary:
            user.append("")
            user.append(boundary)

        user.append("")
        user.append("请针对上面每一条问题逐一修正，重新输出完整 SQL。"
                    "不要只输出改动片段，也不要在没改动的地方换写法。")

    user.append("")
    # 已经注入了独立计划时不再要 CoT 的 plan 字段：两份计划并存，模型会二选一，
    # 而且多半选后者（更靠后）——那等于白做了规划阶段。
    cot = bool((cfg.get("prompt") or {}).get("cot", False)) and not plan_blk
    user.append(OUTPUT_SPEC_COT if cot else OUTPUT_SPEC)
    messages.append({"role": "user", "content": "\n".join(user)})
    return messages


def build_repair_messages(state: dict, cfg: dict,
                          findings: list[dict], prev_sql: str) -> list[dict]:
    """C 阶段：机械修复失败后，带着证据让模型重写。"""
    return build_generate_messages(state, cfg, findings=findings, prev_sql=prev_sql)


def estimate_tokens(messages: list[dict]) -> int:
    """粗估（中文约 1 字 1 token，英文 4 字符 1 token）。只用于日志。"""
    n = 0
    for m in messages:
        s = m.get("content") or ""
        n += sum(1 if ord(c) > 127 else 0.25 for c in s)
    return int(n)
