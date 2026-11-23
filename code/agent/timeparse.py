"""时间解析 + 数据新鲜度判定（D 阶段）。

A 阶段的 ``_pick_day`` 有两个静默错误，这里一并修掉：

1. **相对词基准错了**。它把「昨天」相对 ``data_until``(11-08) 算成 11-07，
   但真实今天是 11-09，昨天应该是 11-08（恰好有数）。相对词必须相对 *现在*。
2. **只支持单日**。问「上个月」「最近 7 天」会悄悄落到 11-08 一天，
   给出一个看起来合理、其实完全答非所问的数。

因此这里引入**双基准**：

    today       真实"现在"（可用 clock.fixed_today 固定，让答辩/评测可复现）
    data_until  离线数据最新分区
    data_since  离线数据最早分区

所有请求窗口都先按 today 解析出「用户想要的区间」，再与 [data_since, data_until]
求交：超出部分裁掉并明确写出裁了多少，**绝不静默缩短**。区间完全落在数据之外
就判 empty（拒答），而不是硬算一个 0 出来。

这个模块不碰数据库、不碰 LLM，纯函数，方便单测。
"""
from __future__ import annotations

import _bootstrap  # noqa: F401

import calendar
import datetime as _dt
import re
from dataclasses import dataclass, replace

# ---------------------------------------------------------------- 正则
_RE_FULL = re.compile(r"(\d{4})[-/年](\d{1,2})[-/月](\d{1,2})\s*日?")
_RE_MD = re.compile(r"(?<!\d)(\d{1,2})[-/月](\d{1,2})\s*日?(?!\d)")
_RE_YM = re.compile(r"(?:(\d{4})\s*年\s*)?(\d{1,2})\s*月(?:份)?(?!\s*\d)")
_RE_RECENT = re.compile(r"(?:最近|近|过去|过去\s*的)\s*([0-9一二三四五六七八九十两]+)\s*天")
_RE_RANGE = re.compile(
    r"(\d{4}[-/]\d{1,2}[-/]\d{1,2}|\d{1,2}[-/]\d{1,2})\s*"
    r"(?:到|至|~|—|\-)\s*"
    r"(\d{4}[-/]\d{1,2}[-/]\d{1,2}|\d{1,2}[-/]\d{1,2})")

_RE_REALTIME = re.compile(
    r"今天|今日|此刻|现在|当前|刚才|实时|最近一小时|这几小时|近一小时|"
    r"最近\s*[0-9一二三四五六七八九十两]+\s*小时|分钟级")

_CN_NUM = {"一": 1, "二": 2, "两": 2, "三": 3, "四": 4, "五": 5,
           "六": 6, "七": 7, "八": 8, "九": 9, "十": 10}


def _cn2int(s: str) -> int | None:
    if s.isdigit():
        return int(s)
    if s in _CN_NUM:
        return _CN_NUM[s]
    if s.startswith("十"):            # 十一 / 十二
        return 10 + _CN_NUM.get(s[1:], 0)
    if "十" in s:                     # 二十 / 二十五
        a, _, b = s.partition("十")
        return _CN_NUM.get(a, 0) * 10 + _CN_NUM.get(b, 0)
    return None


@dataclass(frozen=True)
class TimeWindow:
    """一次查询的时间窗口。start/end 是**裁剪后**实际可查的区间。"""
    start: _dt.date
    end: _dt.date
    label: str                  # 给用户看的解析说明
    kind: str                   # single|range|month|week|recent|default
    status: str                 # ok|truncated|empty|realtime
    note: str = ""              # 新鲜度 / 裁剪提示，narrate 会带上
    requested: tuple[_dt.date, _dt.date] | None = None   # 裁剪前的原始请求
    lag_days: int = 0           # today - data_until
    compare: dict | None = None # 对比基线窗口（见 _baseline）。None = 不是对比问题

    @property
    def is_range(self) -> bool:
        return self.start != self.end

    def sql_hint(self) -> str:
        if not self.is_range:
            return "dt = '%s'" % self.start.isoformat()
        return "dt BETWEEN '%s' AND '%s'" % (self.start.isoformat(), self.end.isoformat())


def clocks(cfg: dict) -> dict:
    """读出三个基准日。fixed_today 让评测集不受"今天几号"影响。"""
    off = cfg["lane"]["offline"]
    clock = cfg.get("clock") or {}
    if clock.get("mode") == "fixed" and clock.get("fixed_today"):
        today = _dt.date.fromisoformat(str(clock["fixed_today"]))
    else:
        today = _dt.date.today()
    data_until = _dt.date.fromisoformat(str(off["data_until"]))
    data_since = _dt.date.fromisoformat(str(off.get("data_since") or "2026-10-05"))
    return {"today": today, "data_until": data_until, "data_since": data_since,
            "lag_days": (today - data_until).days}


def _mk_date(y: int, m: int, d: int) -> _dt.date | None:
    try:
        return _dt.date(y, m, d)
    except ValueError:
        return None


def _parse_one(expr: str, today: _dt.date) -> _dt.date | None:
    m = _RE_FULL.search(expr)
    if m:
        return _mk_date(*map(int, m.groups()))
    m = _RE_MD.search(expr)
    if m:
        return _mk_date(today.year, *map(int, m.groups()))
    return None


def _raw_window(q: str, c: dict) -> tuple[_dt.date, _dt.date, str, str] | None:
    """解析出用户**想要**的区间，不做任何裁剪。返回 (start, end, label, kind)。"""
    today = c["today"]

    # 1) 显式区间：10-05 到 10-12 / 2026-10-01~2026-10-31
    m = _RE_RANGE.search(q)
    if m:
        a, b = _parse_one(m.group(1), today), _parse_one(m.group(2), today)
        if a and b:
            return min(a, b), max(a, b), "显式区间 %s ~ %s" % (a, b), "range"

    # 2) 最近 N 天（含今天，N=7 → 今天往前 6 天）
    m = _RE_RECENT.search(q)
    if m:
        n = _cn2int(m.group(1))
        if n and 1 <= n <= 365:
            return (today - _dt.timedelta(days=n - 1), today,
                    "最近 %d 天（含今天）" % n, "recent")

    # 3) 周：上周按 ISO 周（周一~周日），本周到今天为止
    # 注意顺序："上上周" 必须先判，否则会被 "上周" 的正则先吃掉，
    # 结果把两周前的数据当成上周返回——数值看起来完全合理，最难发现。
    if re.search(r"上上周|上上星期|上上一周|大上周", q):
        mon = today - _dt.timedelta(days=today.weekday() + 14)
        return mon, mon + _dt.timedelta(days=6), "上上周（周一~周日）", "week"
    if re.search(r"上周|上星期|上一周", q):
        mon = today - _dt.timedelta(days=today.weekday() + 7)
        return mon, mon + _dt.timedelta(days=6), "上周（周一~周日）", "week"
    if re.search(r"本周|这周|这一周|本星期", q):
        mon = today - _dt.timedelta(days=today.weekday())
        return mon, today, "本周（周一~今天）", "week"

    # 4) 月：上个月 / 本月 / N月
    if re.search(r"上上个月|上上月|上上一月", q):
        first = today.replace(day=1)
        prev_last = first - _dt.timedelta(days=1)          # 上月最后一天
        prev2_last = prev_last.replace(day=1) - _dt.timedelta(days=1)
        return prev2_last.replace(day=1), prev2_last, "上上个月（整月）", "month"
    if re.search(r"上个月|上月|上一月", q):
        first = today.replace(day=1)
        last = first - _dt.timedelta(days=1)
        return last.replace(day=1), last, "上个月（整月）", "month"
    if re.search(r"本月|这个月|这一月|当月", q):
        first = today.replace(day=1)
        return first, today, "本月（1 号~今天）", "month"
    m = _RE_YM.search(q)
    if m:
        y = int(m.group(1)) if m.group(1) else today.year
        mo = int(m.group(2))
        if 1 <= mo <= 12:
            last = calendar.monthrange(y, mo)[1]
            a, b = _mk_date(y, mo, 1), _mk_date(y, mo, last)
            if a and b:
                return a, b, "%d 年 %d 月（整月）" % (y, mo), "month"

    # 5) 相对日
    if "前天" in q:
        d = today - _dt.timedelta(days=2)
        return d, d, "前天 %s" % d, "single"
    if "昨天" in q or "昨日" in q:
        d = today - _dt.timedelta(days=1)
        return d, d, "昨天 %s" % d, "single"

    # 6) 单个显式日期
    m = _RE_FULL.search(q)
    if m:
        d = _mk_date(*map(int, m.groups()))
        if d:
            return d, d, "显式日期 %s" % d, "single"
    m = _RE_MD.search(q)
    if m:
        d = _mk_date(today.year, *map(int, m.groups()))
        if d:
            return d, d, "简写 %s（补全年份）" % d, "single"

    # 7) 未指定：默认取最新分区，但必须显式说明，不能假装用户问了"所有时间"
    u = c["data_until"]
    return u, u, "未指定时间，取最新分区 %s" % u, "default"


def freshness_note(c: dict) -> str:
    """P1-10：把 now() 与 data_until 的差讲清楚，而不是含糊说"数据可能不全"。"""
    lag = c["lag_days"]
    if lag <= 0:
        return "数据已更新至今天（%s）" % c["data_until"]
    if lag == 1:
        return "T+1 离线数据，最新分区 %s（昨天）" % c["data_until"]
    return ("⚠ 数据滞后 %d 天：今天是 %s，离线最新分区仍停在 %s。"
            % (lag, c["today"], c["data_until"]))


def parse(q: str, cfg: dict) -> TimeWindow:
    c = clocks(cfg)

    # 实时词优先级最高：它问的根本不是离线窗口，直接判不可答
    rt = _RE_REALTIME.search(q or "")
    if rt:
        hi = c["data_until"]
        return TimeWindow(
            start=hi, end=hi, label="实时词「%s」" % rt.group(0), kind="default",
            status="realtime",
            note=("这个问题问的是实时数据（%s），但实时链路自 %s 之后未再产出；"
                  "可用的是 T+1 离线数据，最新到 %s。"
                  % (rt.group(0), hi, hi)),
            lag_days=c["lag_days"])

    spec = _find_compare(q or "", c["today"])
    body = q or ""
    if spec and spec.get("mask"):
        # 把基线时间从问句里抠掉再解析主窗口。
        # 不抠掉的话「上周比上上周」会先匹配到上上周，主窗口整个错位——
        # 而错的数字看起来和对的完全一样。
        body = body.replace(spec["mask"], "　")

    if spec and spec.get("main_raw"):
        a, b = spec["main_raw"]
        kind = "single" if a == b else "range"
        label = "显式对比 %s ~ %s" % (a, b) if a != b else "显式对比 %s" % a
        w = _finalize(a, b, label, kind, c)
    else:
        a, b, label, kind = _raw_window(body, c)
        w = _finalize(a, b, label, kind, c)

    if not spec:
        return w

    if kind == "default":
        w = replace(w, label="未指定时间，取最新分区 %s（对比基线 %s）"
                              % (w.start, spec.get("expr") or spec["mode"]))

    base = _baseline(spec, w, c)
    if spec.get("abs"):
        base["abs"] = True
    return replace(w, compare=base)


def _finalize(a: _dt.date, b: _dt.date, label: str, kind: str,
              c: dict, fresh: bool = True) -> TimeWindow:
    """把「想要的区间」裁到 [data_since, data_until]，并把裁剪量写进 note。

    绝不静默缩短：少算几天必须说出来，否则用户拿到的是"看起来完整"的错误答案。
    """
    lo, hi = c["data_since"], c["data_until"]
    if a > b:
        a, b = b, a

    notes: list[str] = []
    sa, sb = max(a, lo), min(b, hi)
    if sa > sb:
        return TimeWindow(
            start=a, end=b, label=label, kind=kind, status="empty",
            note=("请求的 %s ~ %s 完全没有数据：离线数据只有 %s ~ %s。"
                  % (a, b, lo, hi)),
            requested=(a, b), lag_days=c["lag_days"])

    if a < lo:
        notes.append("请求起点 %s 早于数据起点，实际只从 %s 开始（少了 %d 天）"
                     % (a, lo, (lo - a).days))
    if b > hi:
        notes.append("请求终点 %s 晚于最新分区，实际只到 %s（少了 %d 天）"
                     % (b, hi, (b - hi).days))

    status = "truncated" if notes else "ok"

    if fresh:
        # P1-10：只要问句里出现相对时间词或数据滞后，就把截止日写进答案
        if c["lag_days"] > 1:
            notes.append(freshness_note(c))
        elif kind != "default":
            notes.append("数据截止 %s（离线 T+1）" % hi)

    return TimeWindow(start=sa, end=sb, label=label, kind=kind, status=status,
                      note="；".join(notes), requested=(a, b),
                      lag_days=c["lag_days"])


# ---------------------------------------------------------------- 对比基线
# 「比 11-01 多了多少」这类问题，基线日期一直靠模型自己抄进 SQL。
# 模型对日期没有感知，只会复述它看到的字符串；一旦把 11-01 抄成 11-02，
# 数字看起来完全合理，没人发现。所以基线必须在这里解析出来，
# 再由 execute 用同一张表复算——**差值不由模型提供**。
_TIMEPAT = (
    r"(?:\d{4}[-/年]\d{1,2}[-/月]\d{1,2}\s*日?"
    r"|(?<!\d)\d{1,2}[-/月]\d{1,2}\s*日?"
    r"|上上周|上上星期|上上一周|上上周"
    r"|上周|上星期|上一周|本周|这周|这一周"
    r"|上上个月|上上月|上个月|上月|本月|这个月"
    r"|前天|昨天|昨日"
    r"|最近\s*[0-9一二三四五六七八九十两]+\s*天"
    r"|(?<!\d)\d{1,2}\s*月(?:份)?(?!\s*\d))"
)

# 「和 X 相比」必须整体匹配，否则单个「和」会误吃正常句子
_RE_CMP_SURROUND = re.compile(r"(?:和|跟|与)\s*(%s)\s*(?:相比|对比|做比较)" % _TIMEPAT)
_RE_CMP_AFTER = re.compile(r"(?:比|对比|相比|较)\s*(%s)" % _TIMEPAT)
_RE_CMP_PAIR = re.compile(r"(%s)\s*(?:和|与|跟|vs\.?|VS)\s*(%s)" % (_TIMEPAT, _TIMEPAT))
_RE_CMP_CHAIN = re.compile(r"环比|同比")
# 「相差 / 差距 / 差多少」问的是绝对值大小，不问方向。
# 「少了多少」问的是方向 + 幅度，符号要保留。这两类不能用同一个数回答。
_RE_CMP_ABS = re.compile(r"相差|差多少|差距|差异|绝对值")
_RE_CMP_PAIR_HINT = re.compile(r"对比|比较|相差|差异|多多少|少多少|差多少|增长|下降")


def _find_compare(q: str, today: _dt.date) -> dict | None:
    """识别对比意图，返回 {mode, expr, marker, mask, main_raw}。

    mode:
      explicit  用户点名了基线时间（比 11-01 / 和上周相比）
      pair      两个时间并列（11-01 和 11-08 对比），晚的当当前、早的当基线
      chain     环比 / 同比，基线由当前窗口推算
    """
    if not q:
        return None

    is_abs = bool(_RE_CMP_ABS.search(q))

    m = _RE_CMP_SURROUND.search(q)
    if m:
        return {"mode": "explicit", "expr": m.group(1), "marker": "相比",
                "mask": m.group(0), "abs": is_abs}

    m = _RE_CMP_AFTER.search(q)
    if m:
        return {"mode": "explicit", "expr": m.group(1), "marker": "比",
                "mask": m.group(0), "abs": is_abs}

    if _RE_CMP_PAIR_HINT.search(q):
        m = _RE_CMP_PAIR.search(q)
        if m:
            a, b = _parse_one(m.group(1), today), _parse_one(m.group(2), today)
            if a and b:
                # 两个日期并列时，晚的那个是"现在"，早的那个是基线。
                cur = max(a, b)
                return {"mode": "pair", "expr": m.group(0), "marker": "并列",
                        "mask": None, "main_raw": (cur, cur), "abs": is_abs}

    m = _RE_CMP_CHAIN.search(q)
    if m:
        return {"mode": "chain", "expr": m.group(0), "marker": m.group(0),
                "mask": None, "abs": is_abs}
    return None


def _shift_month(d: _dt.date, months: int) -> _dt.date:
    """整月平移，日号超过目标月天数时取月末（1-31 上个月 → 12-31/2-28）。"""
    y, m = d.year, d.month + months
    while m <= 0:
        m += 12
        y -= 1
    while m > 12:
        m -= 12
        y += 1
    last = calendar.monthrange(y, m)[1]
    return _dt.date(y, m, min(d.day, last))


def _baseline(spec: dict, w: TimeWindow, c: dict) -> dict:
    """由对比意图推出基线窗口，返回可直接 JSON 化的 dict。"""
    mode = spec["mode"]

    if mode == "pair":
        m = _RE_CMP_PAIR.search(spec["expr"] or "")
        a = _parse_one(m.group(1), c["today"]) if m else None
        b = _parse_one(m.group(2), c["today"]) if m else None
        if not (a and b):
            return {"status": "unparsed", "label": spec["expr"], "mode": mode}
        base = min(a, b)
        bw = _finalize(base, base, "基线 %s" % base, "single", c, fresh=False)
        return _cmp_dict(bw, mode, spec["marker"])

    main_days = (w.end - w.start).days + 1
    if mode == "explicit":
        raw = _raw_window(spec["expr"] or "", c)
        bw = _finalize(raw[0], raw[1], raw[2], raw[3], c, fresh=False)
        return _cmp_dict(bw, mode, spec["marker"], main_days)

    # chain：环比 / 同比
    req = w.requested or (w.start, w.end)
    r_start, r_end = req
    days = max(1, (r_end - r_start).days + 1)
    if spec["marker"] == "同比":
        bs, be = _shift_month(r_start, -12), _shift_month(r_end, -12)
        label = "去年同期 %s ~ %s" % (bs, be)
    elif w.kind == "week":
        bs, be = r_start - _dt.timedelta(days=7), r_end - _dt.timedelta(days=7)
        label = "上一周 %s ~ %s" % (bs, be)
    elif w.kind == "month":
        bs, be = _shift_month(r_start, -1), _shift_month(r_end, -1)
        label = "上个月同期 %s ~ %s" % (bs, be)
    else:
        be = r_start - _dt.timedelta(days=1)
        bs = be - _dt.timedelta(days=days - 1)
        label = "上一周期 %s ~ %s" % (bs, be)

    bw = _finalize(bs, be, label, "range" if bs != be else "single",
                   c, fresh=False)
    return _cmp_dict(bw, mode, spec["marker"], main_days)


def _cmp_dict(bw: TimeWindow, mode: str, marker: str,
              main_days: int = 1) -> dict:
    days = (bw.end - bw.start).days + 1
    note = bw.note
    status = bw.status
    auto = True

    if status == "empty":
        note = ("基线窗口 %s ~ %s 没有数据，无法对比（离线数据最新只到 %s）。"
                % (bw.start, bw.end, bw.end))
        auto = False
    elif days != main_days:
        # 长度不同的两个窗口做差没有业务含义（1 天 vs 27 天）。
        # 宁可说"没法直接比"，也不给一个看起来像增长率的假数字。
        status = "mismatch"
        auto = False
        note = ("基线窗口是 %d 天（%s ~ %s），当前窗口是 %d 天，"
                "长度不同，不做自动差值。"
                % (days, bw.start, bw.end, main_days))
    rng = (bw.start.isoformat() if bw.start == bw.end
           else "%s ~ %s" % (bw.start.isoformat(), bw.end.isoformat()))
    return {
        "mode": mode, "marker": marker, "label": rng, "label_raw": bw.label,
        "start": bw.start.isoformat(), "end": bw.end.isoformat(),
        "sql_hint": bw.sql_hint(), "status": status,
        "days": days, "note": note, "auto": auto,
        "same_length": days == main_days,
    }


def explain(w: TimeWindow) -> str:
    """给 trace 用的一行摘要。"""
    rng = w.start.isoformat() if not w.is_range else "%s ~ %s" % (
        w.start.isoformat(), w.end.isoformat())
    out = "%s｜%s｜%s" % (rng, w.label, w.status)
    if w.compare:
        out += "｜对比基线 %s" % w.compare.get("label")
    return out
