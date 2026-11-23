"""答案形态契约（Answer Contract）—— 问句要的是「什么形状」的答案。

为什么需要这一层
----------------
护栏只管 SQL 合不合法、口径对不对，管不了「答非所问」。真实反例：

    产品经理问「2026-11-08 播放量排行榜」
    → 模型给出 `SELECT SUM(play_start_cnt) FROM ... WHERE dt='2026-11-08'`
    → 护栏全过（单表、有分区、无 SELECT *）
    → 执行成功，返回 1 行 1 列 12710
    → 答案写成「2026-11-08 播放量：12,710」

数字对、语法对、口径对，但用户要的是榜，给的是一行汇总。
这类错误**没有任何一条现有护栏能拦**——它不在 SQL 层，在「问题形态 ↔ 结果形状」
的比对上。本模块就是把这件事显式化：

  1. 生成前：把形态契约与 SQL 骨架写进 prompt，减少模型自由发挥跑偏；
  2. 生成后：拿结果形状回比契约，明显不符就返工一次。

分类是确定性的（关键词 + 优先级），不花 token、可单测、可解释。
刻意不使用模型判断——判断标准必须能被复现，否则同一个问题会时对时错。

保守原则
--------
宁可漏判，不可误判。误判的代价是把一条本来正确的 SQL 打回重写，
既花钱又可能越改越差；漏判只是维持现状。所以每条 result 检查都只在
「形态明显崩塌」时触发（见 `check_result`），拿不准的一律放过。
"""
from __future__ import annotations

import re
from dataclasses import dataclass, replace


@dataclass(frozen=True)
class Contract:
    """一类问题的答案形状。字段全部可判定，没有"看情况"。"""
    shape: str
    label: str                       # 中文名，进 trace 和答案
    min_rows: int = 1                # 结果至少几行
    max_rows: int | None = None      # 至多几行（None = 不限）
    min_cols: int = 1                # 至少几列
    need_order: bool = False         # SQL 是否应当有 ORDER BY
    unit: str = ""                   # 值的单位说明（count / ratio / cny / point）
    hint: str = ""                   # 进 prompt 的一句话约束
    skeleton: str = ""               # 进 prompt 的 SQL 骨架

    def as_dict(self) -> dict:
        return {
            "shape": self.shape, "label": self.label,
            "min_rows": self.min_rows, "max_rows": self.max_rows,
            "min_cols": self.min_cols, "need_order": self.need_order,
            "unit": self.unit,
        }


# ---------------------------------------------------------------- 形状字典
# skeleton 里的 <...> 是占位符，不是真 SQL，模型看得懂「保留结构、替换内容」。
CONTRACTS: dict[str, Contract] = {
    "scalar": Contract(
        shape="scalar", label="单值",
        min_rows=1, max_rows=1, min_cols=1, unit="count",
        hint="问的是一个数，结果必须只有 1 行；不要 GROUP BY 出一个列表让用户自己找。",
        skeleton="SELECT <聚合表达式> AS <别名>\nFROM <表>\nWHERE dt = '<日期>'",
    ),
    "argmax": Contract(
        shape="argmax", label="单项最值（带实体）",
        min_rows=1, max_rows=1, min_cols=2, unit="count",
        hint="问的是「哪一个最高/最少」，必须同时给出实体与数值，且只有 1 行："
             "先 GROUP BY 实体，再 ORDER BY 排序键 DESC/ASC，最后 LIMIT 1。"
             "不要只返回那个数值——用户要知道是谁。",
        skeleton=("SELECT <实体列>, <指标> AS v\nFROM <表>\nWHERE dt = '<日期>'\n"
                  "GROUP BY <实体列>\nORDER BY v DESC\nLIMIT 1"),
    ),
    "ranking": Contract(
        shape="ranking", label="榜单 / TopN",
        min_rows=2, max_rows=None, min_cols=2, need_order=True, unit="count",
        hint="问的是排行榜，必须返回**多行明细**（实体 + 指标），按指标降序，"
             "并 LIMIT 到用户要的名次数。绝不能返回一个合计值代替榜单。",
        skeleton=("SELECT <实体列>, <指标> AS v\nFROM <表>\nWHERE dt = '<日期>'\n"
                  "GROUP BY <实体列>\nORDER BY v DESC\nLIMIT <N>"),
    ),
    "comparison": Contract(
        shape="comparison", label="对比 / 差值",
        min_rows=1, max_rows=1, min_cols=1, unit="count",
        hint="问的是两个值之间的差，必须直接算出单个数字，"
             "不要 GROUP BY dt 返回两行让用户自己减。",
        skeleton=("SELECT (SELECT <聚合> FROM <表> WHERE dt = '<A>')\n"
                  "     - (SELECT <聚合> FROM <表> WHERE dt = '<B>') AS diff"),
    ),
    "trend": Contract(
        shape="trend", label="趋势 / 逐日序列",
        min_rows=2, max_rows=None, min_cols=2, need_order=True, unit="count",
        hint="问的是随时间的变化，必须按天分组返回序列（日期列 + 指标列），"
             "并按日期升序。只返回一个合计值等于没答趋势。",
        skeleton=("SELECT dt, <指标> AS v\nFROM <表>\n"
                  "WHERE dt BETWEEN '<开始>' AND '<结束>'\nGROUP BY dt\nORDER BY dt"),
    ),
    "ratio": Contract(
        shape="ratio", label="占比 / 比率 / 均值",
        min_rows=1, max_rows=1, min_cols=1, unit="ratio",
        hint="问的是比率或均值，必须 SUM(分子)/SUM(分母) 重算，禁止 AVG(比率列)、"
             "也禁止把分子分母各自的行数直接相加。占比类结果应在 0~1（或 0~100 百分数）。",
        skeleton=("SELECT SUM(<分子>) / NULLIF(SUM(<分母>), 0) AS ratio\n"
                  "FROM <表>\nWHERE dt = '<日期>'"),
    ),
    "distribution": Contract(
        shape="distribution", label="分组分布",
        min_rows=2, max_rows=None, min_cols=2, need_order=True, unit="count",
        hint="问的是「各 X 分别多少」，必须 GROUP BY 该维度返回多组，"
             "不要把各组合并成一个总数。",
        skeleton=("SELECT <维度列>, <指标> AS v\nFROM <表>\nWHERE dt = '<日期>'\n"
                  "GROUP BY <维度列>\nORDER BY v DESC"),
    ),
    "detail": Contract(
        shape="detail", label="明细列举",
        min_rows=1, max_rows=None, min_cols=1, unit="row",
        hint="问的是「有哪些」，返回明细行即可，不要额外套一层聚合把行数压成 1。",
        skeleton="SELECT <需要的列>\nFROM <表>\nWHERE dt = '<日期>'\nLIMIT 100",
    ),
}


# ---------------------------------------------------------------- 分类正则
# 顺序即优先级：先判「差值」，再判「趋势」，最后才落到标量。
# 每条都对着真实问句调过，注释里写了为什么这么切。
_RE_DIFF = re.compile(
    r"比.{0,16}(多|少|高|低|涨|增加|减少|增长|下降|差)"
    r"|多多少|少多少|差多少|相差|差值|差距|净增|净减"
    r"|环比|同比|变化了多少|增加了多少|减少了多少|提升了多少|下降了多少")

_RE_TREND = re.compile(
    r"趋势|走势|逐日|每天|按天|日趋势|变化情况|波动情况"
    r"|最近\s*[0-9一二三四五六七八九十]+\s*天.{0,8}(变化|走势|趋势)")

# 「前3」「前三名」「Top 10」「排行榜」「第 1 名」——名次是显式的形状信号
# 「前五的歌」这种省略量词的写法也要认，但必须排除「前5天」这类时间表述，
# 否则「前 5 天的播放量」会被当成榜单，反过来要 5 行明细。
_RE_TOPN = re.compile(
    r"前\s*[0-9一二三四五六七八九十]+\s*(名|个|首|位|条|大|款)"
    r"|前\s*[0-9一二三四五六七八九十]+\s*的(?![天周月年日])"
    r"|top\s*[0-9]+|排行榜|排行|榜单|排名|第\s*[0-9一二三四五六七八九十]+\s*名", re.I)

_RE_ARGMAX = re.compile(r"最高|最低|最多|最少|最大|最小|最好|最差|最强|最弱")

_RE_ASK_ENTITY = re.compile(r"哪些|哪几|哪首|哪一首|哪位|哪一个|是谁|叫什么|什么歌|什么曲风|列出|列举|分别是")
_RE_ASK_VALUE = re.compile(r"多少|几次|是多少|有多大|有多高|贡献了|占多少")

# 比率类分强弱两档，因为「率」既可能是指标名（完播率），也可能是答题要求（占全站多少）。
#   强：答案**必须**是一个比例 → 在 argmax 之前判，否则「最高省份占全站多少」
#       会被 argmax 抢走，模型只给 1150 而不做除法（C15 实测踩到）。
#   弱：只是指标名里带「率/人均/ARPU」→ 放到 argmax 之后判，
#       否则「完播率最高的曲风」会被判成 ratio，丢掉"是哪一类"的诉求。
_RE_RATIO_STRONG = re.compile(r"占比|比例|百分比|占总体|占全站|占多少|渗透率|转化率")
_RE_RATIO_WEAK = re.compile(r"[一-龥]率|人均|平均|arpu|arppu|单价|客单", re.I)
_RE_RATIO = re.compile(_RE_RATIO_STRONG.pattern + "|" + _RE_RATIO_WEAK.pattern, re.I)

# 「占比」两个字里的「比」会被差值正则的 `比.{0,16}多` 误吃
# （「付费用户占比是多少」→ 匹配到「比是…多」），所以差值判定前先把它抠掉。
_RE_RATIO_WORD = re.compile(r"占比|百分比|比例")
_RE_DIST = re.compile(r"分布|构成|各.{0,10}的|按.{0,8}(分组|拆分|维度)|集中度")

_RE_CN = {"一": 1, "二": 2, "两": 2, "三": 3, "四": 4, "五": 5,
          "六": 6, "七": 7, "八": 8, "九": 9, "十": 10}


def _topn_count(q: str) -> int | None:
    """从问句里抽 N。「前三名」→3，「Top10」→10，「前五的歌」→5。抽不到返回 None。"""
    m = re.search(r"前\s*([0-9]+|[一二三四五六七八九十]+)\s*(名|个|首|位|条|大|款|的)", q)
    if m:
        s = m.group(1)
        if s.isdigit():
            return int(s)
        if s in _RE_CN:
            return _RE_CN[s]
    m = re.search(r"top\s*([0-9]+)", q, re.I)
    if m:
        return int(m.group(1))
    return None


def classify(question: str, metrics: list[str] | None = None) -> Contract:
    """问句 → 契约。纯规则，无随机性。

    metrics 目前不参与判定，留作后续「比率型指标强制 ratio」的钩子；
    这里显式接住但不使用，是为了让调用方不必等到真有规则时再改签名。
    """
    q = (question or "").strip()
    if not q:
        return CONTRACTS["scalar"]

    # 1) 差值优先。「A 比 B 多多少」里既有对比也有数值询问，
    #    但答案形状只有一个——单标量差值。放在最前面，避免被后面的规则抢走。
    #    先把「占比/比例/百分比」抠掉，否则「占比是多少」会被 `比.{0,16}多` 吃掉。
    if _RE_DIFF.search(_RE_RATIO_WORD.sub("", q)):
        return CONTRACTS["comparison"]

    # 2) 趋势。注意别把「最近 7 天的播放量」判成趋势——
    #    那问的是窗口总量，不是逐日序列；所以要求问句里还要有"变化/走势"类词，
    #    单纯「逐日/每天」也算显式要求序列。
    if _RE_TREND.search(q) or re.search(r"逐日|每天|按天", q):
        return CONTRACTS["trend"]

    # 3) 分组 + 占比。「各曲风的播放量占比」要的是多行，不是单值。
    #    这一条必须排在 ratio 之前，否则会被压成一行。
    if _RE_DIST.search(q) and _RE_RATIO.search(q):
        return CONTRACTS["distribution"]

    # 4) 强比率信号：答案必须是一个比例/商，优先于榜单与最值。
    #    「播放量最高的省份占全站多少」问的是除法结果，不是那个省份的量。
    if _RE_RATIO_STRONG.search(q):
        return CONTRACTS["ratio"]

    # 5) 榜单。显式名次词是最强信号，其次才是「最高 + 问实体」。
    if _RE_TOPN.search(q):
        n = _topn_count(q)
        base = CONTRACTS["ranking"]
        # 名次数 ≤1 时退化成单行；否则按 N 抬高下限，让"前三名只回 1 行"能被抓到。
        if n is not None and n <= 1:
            return replace(base, min_rows=1, max_rows=1)
        if n:
            return replace(base, min_rows=min(n, 5))
        return base

    # 6) 最值。分流点在于用户要的是「谁」还是「多少」：
    #    「播放量最高的那首歌有多少次播放」→ 一个是非要说清名字，但仍只有 1 行。
    if _RE_ARGMAX.search(q):
        if _RE_ASK_ENTITY.search(q):
            return replace(CONTRACTS["argmax"], label="最值实体")
        return CONTRACTS["argmax"]

    # 7) 弱比率（指标名里带「率/人均」）
    if _RE_RATIO_WEAK.search(q):
        return CONTRACTS["ratio"]

    # 8) 分组分布
    if _RE_DIST.search(q):
        return CONTRACTS["distribution"]

    # 9) 明细列举
    if _RE_ASK_ENTITY.search(q) and not _RE_ASK_VALUE.search(q):
        return CONTRACTS["detail"]

    return CONTRACTS["scalar"]


# ---------------------------------------------------------------- 结果回比
def _cells(res: dict) -> list:
    out = []
    for r in (res.get("rows") or []):
        out.extend(r or [])
    return out


def check_result(contract: Contract, res: dict, sql: str = "") -> list[str]:
    """结果形状 vs 契约。返回不符项（空 = 通过）。

    只报「形态崩塌」，不报「可能不太对」。判断依据全部是结果集本身
    与 SQL 的粗粒度特征，不做数值合理性推断（那属于口径问题，护栏管）。
    """
    if not res or res.get("error"):
        return []

    rows = res.get("rows") or []
    cols = res.get("columns") or []
    n_row, n_col = len(rows), len(cols)
    issues: list[str] = []

    # 空结果不是"形状不符"，是"没数据"——留给 narrate 说明，不返工。
    if n_row == 0:
        return []

    # 全 NULL：SQL 跑通了但一个值都没算出来，形态再对也没用。
    vals = _cells(res)
    if vals and all(v is None for v in vals):
        issues.append("结果全为 NULL：SQL 执行成功但没有任何有效值，"
                      "通常是分区取错或过滤条件把数据全排除了。")
        return issues

    if contract.shape == "comparison" and n_row > 1:
        issues.append("这是「对比/差值」问题，应只返回 1 行差值，实际返回 %d 行；"
                      "用户要的是减法结果，不是两张明细让他自己减。" % n_row)

    elif contract.shape == "trend" and n_row < 2:
        issues.append("这是「趋势」问题，至少要 2 个时间点，实际只返回 %d 行；"
                      "返回一个合计值等于没答趋势，请按 dt 分组输出序列。" % n_row)

    elif contract.shape == "ranking" and n_row < contract.min_rows:
        issues.append("这是榜单问题（至少 %d 条），实际只返回 %d 行；"
                      "请按实体分组、按指标排序并给出完整名次，不要用合计值代替榜单。"
                      % (contract.min_rows, n_row))
        if n_col < 2:
            issues.append("榜单每行至少要「实体 + 指标」两列，实际只有 %d 列。" % n_col)

    elif contract.shape == "argmax" and n_row > 20:
        issues.append("问的是单个最值（哪一个最高），实际返回 %d 行；"
                      "请 ORDER BY 排序后 LIMIT 1。" % n_row)

    elif contract.shape == "distribution" and n_row < 2:
        issues.append("这是分组分布问题（各 X 分别多少），实际只返回 1 组；"
                      "请按维度 GROUP BY 输出各组。" )

    elif contract.shape == "ratio" and n_row == 1 and n_col == 1:
        v = rows[0][0] if rows[0] else None
        if isinstance(v, (int, float)) and not isinstance(v, bool) and abs(v) > 100:
            issues.append("这是占比问题，返回值 %s 超出合理值域（应为 0~1 的比例，"
                          "或 0~100 的百分数）；请检查分子分母是否取反或漏了除法。" % v)

    return issues


def contract_block(contract: Contract) -> str:
    """进 prompt 的形态约束块。只在形态非 scalar 时输出——标量题不需要额外噪音。"""
    if contract.shape == "scalar" or not contract.hint:
        return ""
    return "【答案形态】%s\n  %s\n  参考骨架（保留结构，替换内容）：\n%s" % (
        contract.label, contract.hint,
        "\n".join("    " + ln for ln in contract.skeleton.splitlines()))


def compare_contract(base: Contract, auto: bool = True) -> Contract:
    """对比问题、且基线由系统复算时的契约。

    comparison 的默认骨架是「两条子查询相减」。一旦基线日期由系统给出，
    这个骨架就成了引诱：模型会开始自己拼 `<A>` `<B>`，也就开始自己抄日期。
    所以这里把契约换成「只查当前窗口一个数」，差值交给 execute 算。
    """
    if auto:
        hint = ("问的是与基线窗口的差，但**差值由系统用同一张表再查一遍基线后算出**。"
                "你只负责查当前窗口的一个数：1 行 1 列，不要写基线日期，"
                "不要写两条子查询相减。")
    else:
        hint = ("基线窗口不可用（长度不同或无数据），系统无法算差值。"
                "只查当前窗口的一个数：1 行 1 列，并在答案里说明没有做对比。")
    return replace(
        base, shape="comparison", label="当前窗口单值（差值系统另算）",
        min_rows=1, max_rows=1, min_cols=1,
        hint=hint,
        skeleton="SELECT <聚合表达式> AS <别名>\nFROM <表>\nWHERE dt = '<当前窗口>'")
