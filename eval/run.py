"""评测运行器（阶段 2 升级版）。

    python eval/run.py                       # 全量（cases.yaml + adversarial.yaml）
    python eval/run.py --quick               # 只跑零 LLM 的题
    python eval/run.py --tag 商业化           # 按 tag 过滤
    python eval/run.py --domain 商业化        # 按域过滤
    python eval/run.py --id V21              # 单题复现
    python eval/run.py --json eval/last.json # 机读产物，供 gate.py 比对
    python eval/run.py --md eval/report.md

三条判据（企业级验收要求分层，不能只报一个通过率）：

  L1 可执行  SQL 能跑通 / 该拒的拒对了 / 元问题答出来了。衡量"链路没崩"。
  L2 正确    数值命中期望、拒答理由命中、元答案含关键词。衡量"算得对"。
  L3 可采纳  L2 之上还要求：答案有正文、不截断、value 题带 SQL。衡量"能直接交付"。

加权分 `0.2·L1 + 0.5·L2 + 0.3·L3`。三者分开看的意义：
L1 高 L2 低 = 模型在写 SQL 但写错；L2 高 L3 低 = 数对但答案交付不了。

期望值来源见 cases.yaml 头部，**不来自 Agent 自己的输出**。
"""
from __future__ import annotations

import argparse
import copy
import io
import json
import re
import sys
import time
from collections import Counter, OrderedDict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "code" / "agent"))
sys.path.insert(0, str(ROOT / "code"))

import yaml  # noqa: E402
from conf import load_config  # noqa: E402
from governance.contracts import RequestContext  # noqa: E402
from graph import ask  # noqa: E402

PASS, FAIL = "PASS", "FAIL"
W_L1, W_L2, W_L3 = 0.2, 0.5, 0.3


# ---------------------------------------------------------------- 用例装载
def _read(path: Path) -> dict:
    return yaml.safe_load(io.open(path, encoding="utf-8").read())


def load_cases(with_adversarial: bool = True) -> dict:
    data = _read(ROOT / "eval" / "cases.yaml")
    if with_adversarial:
        adv_path = ROOT / "eval" / "adversarial.yaml"
        if adv_path.exists():
            adv = _read(adv_path)
            for c in adv["cases"]:
                c.setdefault("set", "adversarial")
                c.setdefault("source", "adversarial.yaml")
            data["cases"] = list(data["cases"]) + list(adv["cases"])
    return data


def _classify(case: dict, taxonomy: dict, kind: str) -> str:
    """先认显式字段，再按 tag 顺序匹配。命中不了就落"未分类"——
    未分类本身是信号：说明覆盖矩阵有空格，不该悄悄归到别的域里。"""
    explicit = case.get(kind)
    if explicit:
        return str(explicit)
    tags = set(case.get("tags") or [])
    for rule in (taxonomy or {}).get(kind) or []:
        if tags & set(rule.get("match") or []):
            return str(rule.get("value"))
    return "未分类"


# ---------------------------------------------------------------- 单题判定
def _number(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _extract(state: dict, case: dict):
    res = state.get("result") or {}
    if case.get("extract", "first") == "row_count":
        return res.get("row_count"), "row_count"
    rows = res.get("rows") or []
    if not rows or not rows[0]:
        return None, "first"
    # 排名类问题首列是实体名（song_id / genre），数值在第二列；
    # 只取 rows[0][0] 会把「答对了」判成「取不到数值」。
    idx = int(case.get("col", 0))
    if idx < len(rows[0]):
        v = _number(rows[0][idx])
        if v is not None:
            return v, "col%d" % idx
    # 显式列号取不到数值时回退扫描：模型把 COUNT(*) 放第几列是自由的
    # （`SELECT id, name, cnt` 与 `SELECT cnt, name` 都合法），
    # 用列号锁死会让"答对了"变成"取不到数值"——N03 就这么假红过一次。
    for i, cell in enumerate(rows[0]):
        v = _number(cell)
        if v is not None:
            return v, "col%d(回退)" % i
    return None, "col%d" % idx


# ---------------------------------------------------------------- 答案层断言
# 阶段 5 的核心改动：判据从「SQL 返回了什么」抬到「用户看到了什么」。
#
# 为什么必须改：`expected` 只能按列号去 rows 里捞一个数，于是判过与否取决于
# 模型把 COUNT(*) 放在第几列、多没多带一个 song_id。结果是**答案完全正确却判红**
# （N03 假红过一次），而真正的坏情况——SQL 算对了但答案里没写出来、写错了数——
# 反而抓不到，因为判据根本不在答案上。
#
# 现在的分工：
#   硬判据  answer_must / answer_any / answer_must_not —— 只看答案正文
#   软校验  expected / compare_delta —— 答案过了但期望不符只记录（detail.soft_mismatch）
# 软校验保留的意义：答案对而期望不符，说明是期望值过期或口径漂移，这类信息
# 不该让用例变红（那是判据的问题），但也不该消失（那是数据的问题）。

_TOKEN_RE = re.compile(r"[-+]?\d[\d,]*(?:\.\d+)?%?")
_NUMSTR_RE = re.compile(r"[-+]?\d[\d,]*(?:\.\d+)?%?$")
_WS = re.compile(r"[\s，。？！、；：\"'“”‘’（）()\[\]【】!?,;:]+")
_RE_FIX = re.compile(r"\n复算[（:：]")


def answer_body(state: dict) -> str:
    """答案里"人能读"的那部分：剥掉复算 SQL 段。

    这一步不是洁癖。SQL 段里有 `'2026-11-08'`（会被切成 2026 / 11 / 08）、
    有 `LIMIT 10`（10）、有表名里的 `dws_song_play_daily`。拿整篇答案扫数字，
    断言 10 或 2026 会**因为 LIMIT 和日期而假过**——假过比假红危险得多，
    它会让一个没写出答案的系统拿到满分。
    """
    ans = state.get("answer") or ""
    m = _RE_FIX.search(ans)
    return ans[:m.start()] if m else ans


def _flat(s: str) -> str:
    return _WS.sub("", s or "").lower()


def _num_tokens(text: str) -> list[dict]:
    """答案里的数字 token。`89,250` / `+125` / `0.14%` 都要认。"""
    out = []
    for m in _TOKEN_RE.finditer(text or ""):
        raw = m.group(0)
        pct = raw.endswith("%")
        try:
            v = float(raw.rstrip("%").replace(",", ""))
        except ValueError:
            continue
        out.append({"raw": raw, "value": v, "pct": pct})
    return out


def _near(text: str, anchors) -> str:
    """锚点窗口：只在这个词附近找数字。

    用来治「差值 +1」这类断言。裸数字 1 在答案里到处都有（第 1 名、1 行），
    单独断言等于没断言；锚定到「差值」后面 30 个字符内，才是"系统真的说出了
    这个差值"。
    """
    if isinstance(anchors, str):
        anchors = [anchors]
    buf = []
    for a in anchors:
        start = 0
        while True:
            i = text.find(a, start)
            if i < 0:
                break
            buf.append(text[max(0, i - 8):i + len(a) + 32])
            start = i + len(a)
    return "\n".join(buf)


def _hit_number(toks: list[dict], want: float, tol: float,
                allow_abs: bool = False) -> dict | None:
    for t in toks:
        v = abs(t["value"]) if allow_abs else t["value"]
        if abs(v - want) <= tol:
            return t
    return None


def _assert_one(item, body: str, tol: float) -> tuple[bool, str]:
    """单条答案断言。支持三种写法：

        "复算"                       → 片段：答案正文含这三个字
        89250                        → 数值：正文任一处出现 89,250 即过
        {value: 125, tol: 1, near: 差值} → 锚定数值
        {value: 105, abs: true}      → 允许以绝对值形式出现（"少了 105"）
    """
    if isinstance(item, bool):
        return False, "断言写法不合法：%r" % (item,)
    if isinstance(item, (int, float)):
        item = {"value": float(item)}
    if isinstance(item, str):
        s = item.strip()
        if _NUMSTR_RE.match(s):
            item = {"value": float(s.rstrip("%").replace(",", ""))}
        else:
            hit = _flat(s) in _flat(body)
            return hit, "答案含「%s」=%s" % (s, "✓" if hit else "✗")
    if not isinstance(item, dict) or "value" not in item:
        return False, "断言写法不合法：%r" % (item,)

    want = float(item["value"])
    t = float(item.get("tol", tol))
    scope = body if not item.get("near") else _near(body, item["near"])
    hit = _hit_number(_num_tokens(scope), want, t, bool(item.get("abs")))
    tail = "（锚点 %s）" % item["near"] if item.get("near") else ""
    return (bool(hit),
            "答案含%g%s=%s%s" % (want, tail, "✓" if hit else "✗",
                               "（实得 %s）" % hit["raw"] if hit else ""))


def _derive_must(case: dict):
    """没有显式 answer_must 时，把 expected / compare_delta 推导成答案断言。

    推导只覆盖"答案里一定会出现的数"：
      · `expected_min` 是行数门槛，答案正文里不一定写行数 → 不推导
      · `extract: row_count` 同理 → 不推导
    这样存量 124 条用例不用改写就能切到答案层，而显式 answer_must 仍然优先。
    """
    if case.get("answer_must") is not None:
        return None
    if case.get("expected_min") is not None or case.get("extract") == "row_count":
        return None
    out = []
    want = _number(case.get("expected"))
    if want is not None:
        out.append({"value": want})
    d = _number(case.get("compare_delta"))
    if d is not None:
        # 差值锚定在「差值 / 相差」后面：裸 1 会被"第 1 名"喂饱，
        # 那等于断言失效却看起来全绿。
        out.append({"value": d, "near": ["差值", "相差", "差距"]})
    return out


def _l1(case: dict, state: dict) -> bool:
    """可执行：链路走到终点了没有。"""
    if case["type"] == "refuse":
        return bool(state.get("refused"))
    if case["type"] == "meta":
        return bool(state.get("meta_answer")) and bool((state.get("answer") or "").strip())
    res = state.get("result") or {}
    return (not state.get("refused")) and not res.get("error") \
        and int(res.get("row_count") or 0) > 0


def _l3(case: dict, state: dict, l2_ok: bool) -> bool:
    """可采纳：数对了还不够，答案得能直接给人看。"""
    if not l2_ok:
        return False
    ans = (state.get("answer") or "").strip()
    if len(ans) < 20:
        return False
    res = state.get("result") or {}
    if res.get("truncated"):
        return False
    if case["type"] == "value" and not (state.get("sql") or "").strip():
        return False
    return True


def judge(case: dict, state: dict) -> tuple[str, str, dict]:
    """返回 (L2 判定, 说明, 详情)。L1/L3 在 main 里补，避免这里重复取数。"""
    detail = {
        "sql": (state.get("sql") or "").strip(),
        "applied_sql": (state.get("applied_sql") or "").strip(),
        "verdict": state.get("verdict"),
        "attempts": state.get("attempts"),
        "refused": bool(state.get("refused")),
        "findings": [f.get("rule") for f in (state.get("findings") or [])
                     if f.get("level") == "REJECT"],
        "shape": state.get("shape"),
        "shape_retries": state.get("shape_retries") or 0,
        "shape_unresolved": bool(state.get("shape_unresolved")),
        "tokens_in": sum(int((t.get("data") or {}).get("tokens_in") or 0)
                         for t in (state.get("trace") or [])),
        "tokens_out": sum(int((t.get("data") or {}).get("tokens_out") or 0)
                          for t in (state.get("trace") or [])),
    }
    res = state.get("result") or {}

    if case["type"] == "meta":
        ans = state.get("answer") or ""
        want = case.get("expect_text") or ""
        kind = (state.get("meta_intent") or {}).get("kind") or "—"
        ok = bool(state.get("meta_answer")) and (not want or want in ans)
        detail["meta_kind"] = kind
        detail["answer_head"] = ans.split("\n")[0][:120]
        return (PASS if ok else FAIL,
                "元答案=%s 类型=%s 含「%s」=%s" % (
                    bool(state.get("meta_answer")), kind, want, want in ans),
                detail)

    if case["type"] == "refuse":
        reason = state.get("refuse_reason") or ""
        want = case.get("expect_reason") or ""
        ok = bool(state.get("refused")) and (not want or want in reason)
        detail["refuse_reason"] = reason.split("\n")[0][:160]
        bits = ["拒答=%s" % bool(state.get("refused")),
                "理由含「%s」=%s" % (want, want in reason)]
        # 拒答也要说得对：只允许"拒"还不够，得说清**为什么拒、拒掉的是什么**。
        # 所以 refuse 类型也可以带答案层断言（answer_must / answer_must_not），
        # 断言对象是用户看到的整段正文——包括那张扫描量表格。
        must = case.get("answer_must") or []
        if must or case.get("answer_must_not"):
            body = answer_body(state)
            fails: list[str] = []
            for it in must:
                hit, why = _assert_one(it, body, float(case.get("tolerance", 0)))
                bits.append(why)
                if not hit:
                    fails.append(why)
            for frag in case.get("answer_must_not") or []:
                hitb = _flat(frag) in _flat(body)
                bits.append("答案不含「%s」=%s" % (frag, "✓" if not hitb else "✗"))
                if hitb:
                    fails.append("答案出现了不该出现的「%s」" % frag)
            detail["answer_layer"] = True
            detail["answer_must_failed"] = fails
            ok = ok and not fails
        return (PASS if ok else FAIL, "；".join(bits), detail)

    # ---- value ----
    if state.get("refused"):
        return FAIL, "被拒答了：%s" % (state.get("refuse_reason") or "")[:100], detail
    if res.get("error"):
        return FAIL, "执行报错：%s" % str(res["error"])[:120], detail

    tol = float(case.get("tolerance", 0))
    actual, how = _extract(state, case)
    detail["actual"] = actual
    detail["rows"] = res.get("row_count")

    bits: list[str] = []
    hard_ok = True
    soft: list[dict] = []

    # 1) 答案层（硬）：只看用户看到的正文
    must = case.get("answer_must")
    if must is None:
        must = _derive_must(case) or []
    detail["answer_layer"] = bool(must)
    if must:
        body = answer_body(state)
        detail["answer_head"] = body.strip().split("\n")[0][:120]
        fails = []
        for it in must:
            hit, why = _assert_one(it, body, tol)
            bits.append(why)
            if not hit:
                fails.append(why)
        for frag in case.get("answer_must_not") or []:
            hitb = _flat(frag) in _flat(body)
            bits.append("答案不含「%s」=%s" % (frag, "✓" if not hitb else "✗"))
            if hitb:
                fails.append("答案出现了不该出现的「%s」" % frag)
        hard_ok = not fails
        detail["answer_must"] = [str(x) for x in must]
        detail["answer_must_failed"] = fails

    # 2) 行数门槛：本身就是"结果集规模"的判据，答案里没有对应数字，保持硬判
    if case.get("expected_min") is not None:
        ok_row = actual is not None and actual >= case["expected_min"]
        bits.append("行数 %s ≥ %s" % (actual, case["expected_min"]))
        hard_ok = hard_ok and ok_row
        detail["expected_min"] = case["expected_min"]

    # 3) 结果集数值：有答案层断言时降级为软校验
    want = _number(case.get("expected"))
    if want is not None and case.get("extract") != "row_count":
        agree = actual is not None and abs(actual - want) <= tol
        detail["expected"], detail["tolerance"] = want, tol
        soft.append({"field": "expected", "actual": actual, "want": want,
                     "agree": agree})
        if not must:
            bits.append("%s=%s 期望=%s 容差=%s" % (how, actual, want, tol))
            hard_ok = hard_ok and agree

    if case.get("compare_delta") is not None:
        cr = state.get("compare_result") or {}
        d = cr.get("delta")
        want_d = _number(case["compare_delta"])
        c_ok = (cr.get("status") == "ok" and d is not None
                and want_d is not None and abs(d - want_d) <= tol)
        detail["compare_delta"] = d
        detail["compare_base"] = cr.get("base_value")
        soft.append({"field": "compare_delta", "actual": d, "want": want_d,
                     "agree": c_ok})
        if not must:
            bits.append("差值=%s 期望=%s" % (d, want_d))
            hard_ok = hard_ok and c_ok

    mismatch = [s for s in soft if not s["agree"]]
    if mismatch and must:
        # 答案对了但期望值不符：不当红，但要留下痕迹。
        # 它意味着"期望值过期"或"答案与 SQL 结果不是同一件事"，两者都该被看见。
        detail["soft_mismatch"] = mismatch
        bits.append("软校验不符（不计入判定）：" + "、".join(
            "%s 实得 %s 期望 %s" % (s["field"], s["actual"], s["want"])
            for s in mismatch))

    sql_up = (state.get("sql") or "").upper()
    for frag in case.get("sql_contains") or []:
        hit = frag.upper() in sql_up
        hard_ok = hard_ok and hit
        bits.append("SQL含%s=%s" % (frag, hit))
    for frag in case.get("sql_not_contains") or []:
        hit = frag.upper() not in sql_up
        hard_ok = hard_ok and hit
        bits.append("SQL不含%s=%s" % (frag, hit))
    return (PASS if hard_ok else FAIL, " ｜ ".join(bits), detail)


# ---------------------------------------------------------------- 聚合
def _rate(part: int, total: int) -> float:
    return round(100.0 * part / total, 1) if total else 0.0


def summarize(rows: list[tuple]) -> dict:
    """整体 + 分域的 L1/L2/L3 与加权分。"""
    def block(items: list[tuple]) -> dict:
        n = len(items)
        l1 = sum(1 for r in items if r[5]["l1"])
        l2 = sum(1 for r in items if r[1] == PASS)
        l3 = sum(1 for r in items if r[5]["l3"])
        return {
            "n": n,
            "l1": _rate(l1, n), "l2": _rate(l2, n), "l3": _rate(l3, n),
            "score": round(W_L1 * _rate(l1, n) + W_L2 * _rate(l2, n)
                           + W_L3 * _rate(l3, n), 1),
            "passed": l2, "failed": n - l2,
            "tokens_in": sum(r[5].get("tokens_in") or 0 for r in items),
            "tokens_out": sum(r[5].get("tokens_out") or 0 for r in items),
            # 答案层覆盖：多少题是"看答案判的"。这个数字低，说明通过率
            # 仍然依赖 SQL 返回形状，换个列序就可能假红/假绿。
            "answer_layer": sum(1 for r in items if r[5].get("answer_layer")),
            "soft_mismatch": sum(1 for r in items if r[5].get("soft_mismatch")),
        }

    by_domain: "OrderedDict[str, list]" = OrderedDict()
    for r in rows:
        by_domain.setdefault(r[3], []).append(r)
    # 稳定性分布（--repeat 时有值）：{"3/3": 74, "2/3": 3, ...}
    votes = Counter(r[5].get("votes") or "" for r in rows if r[5].get("votes"))
    return {
        "overall": block(rows),
        "domains": {k: block(v) for k, v in sorted(by_domain.items())},
        "stability": dict(votes),
    }


def default_md_path(partial: bool) -> str:
    """默认报告路径：全量写 report.md，部分跑写 report_partial.md（不覆盖全量）。"""
    return str(ROOT / "eval" / ("report_partial.md" if partial else "report.md"))


# ---------------------------------------------------------------- 主流程
def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--quick", action="store_true", help="只跑零 LLM 的用例")
    ap.add_argument("--tag", action="append", default=[], help="按 tag 过滤，可重复")
    ap.add_argument("--domain", action="append", default=[], help="按域过滤，可重复")
    ap.add_argument("--id", action="append", default=[], help="只跑指定 id，可重复")
    ap.add_argument("--prefix", default="", help="只跑 id 以此开头的用例（如 C 只跑补题）")
    ap.add_argument("--no-adversarial", action="store_true", help="不加载对抗集")
    ap.add_argument("--md", default="",
                    help="Markdown 报告路径；默认全量写 eval/report.md，部分跑写 eval/report_partial.md")
    ap.add_argument("--json", default="", help="机读结果路径，供 gate.py 比对")
    ap.add_argument("--no-history", action="store_true",
                    help="不写 eval/history.jsonl（临时试验时用）")
    ap.add_argument("--repeat", type=int, default=1,
                    help="走 LLM 的题重复跑 N 次取多数（门禁稳定性）；"
                         "零 LLM 题结果确定，不重复浪费次数")
    a = ap.parse_args()

    # 部分跑（--id/--tag/--quick 等）绝不能覆盖全量报告：
    # 之前 --md 恒默认 eval/report.md，一次 quick 就把 126 题的基线报告
    # 刷成 46 题，gate.py 随后拿它当"最近一次全量"做分域对比，报假警。
    # 显式传 --md 时尊重调用方，未传时按是否 partial 分流。
    partial = bool(a.id or a.prefix or a.tag or a.domain or a.quick)
    md_path = a.md or default_md_path(partial)

    data = load_cases(with_adversarial=not a.no_adversarial)
    taxonomy = data.get("taxonomy") or {}
    cases = data["cases"]

    # 先分类再过滤：--domain 依赖分类结果
    for c in cases:
        c["_domain"] = _classify(c, taxonomy, "domain")
        c["_intent"] = _classify(c, taxonomy, "intent")

    if a.id:
        cases = [c for c in cases if c["id"] in a.id]
    if a.prefix:
        cases = [c for c in cases if str(c.get("id", "")).startswith(a.prefix)]
    if a.tag:
        cases = [c for c in cases if set(a.tag) & set(c.get("tags") or [])]
    if a.domain:
        cases = [c for c in cases if c["_domain"] in a.domain]
    if a.quick:
        cases = [c for c in cases if "零LLM" in (c.get("tags") or [])]

    cfg = load_config()
    print("== 评测（%d 题）==" % len(cases))
    print("数据窗口 %s ｜ 时钟 %s ｜ 生成器 %s\n"
          % ("~".join(data["data_window"]), data["clock"], cfg["generator"]["mode"]))

    from governance.identity import provider
    idp = provider(cfg)

    rows: list[tuple] = []
    t0 = time.perf_counter()
    for i, c in enumerate(cases, 1):
        use_cfg = copy.deepcopy(cfg)
        if c.get("mode") == "stub":
            use_cfg["generator"]["mode"] = "stub"

        # 身份：声明了 user 的题按该账号的策略跑，用来验权限矩阵（AC-06）。
        # 没有 user 的题走内部基线通道，但 source 一律标 eval——
        # 否则评测流量会和真实使用混在同一张审计表里，把成功率看板污染成
        # "54%"这种假数字（实测 452 次拒答全是评测产生的）。
        ctx = None
        if c.get("user"):
            ctx = idp.context_for_user(c["user"], source="eval")
            if ctx is None:
                rows.append((c, FAIL, "账号不存在：%s" % c["user"], c["_domain"],
                             c["_intent"], {"l1": False, "l3": False}))
                print("  %-4s %-4s %-9s  账号不存在：%s"
                      % (FAIL, c["id"], c.get("type"), c["user"]))
                continue
        else:
            ctx = RequestContext.system(source="eval")

        ts = time.perf_counter()
        # --repeat N：走 LLM 的题跑 N 次取多数。单轮判定分不清"本来错"
        # 和"偶尔错"（V16 曾 4 过 1 败），多数投票把稳定性变成显式判据：
        # votes 记 3/3、2/3，门禁和报表都看得到。
        reps = a.repeat if (a.repeat > 1 and "零LLM" not in (c.get("tags") or [])
                            and c.get("mode") != "stub") else 1
        statuses, notes, details, sts_all = [], [], [], []
        for _rep in range(reps):
            try:
                st_r = ask(c["question"], use_cfg, c.get("sql"), ctx=ctx)
                status_r, note_r, detail_r = judge(c, st_r)
            except Exception as e:                   # 单题崩了不能中断整轮
                status_r, note_r = FAIL, "异常：%s: %s" % (type(e).__name__, str(e)[:120])
                detail_r = {"sql": "", "findings": []}
                st_r = {}
            statuses.append(status_r)
            notes.append(note_r)
            details.append(detail_r)
            sts_all.append(st_r)
        dt = time.perf_counter() - ts

        if reps > 1:
            n_pass = statuses.count(PASS)
            status = PASS if n_pass * 2 > reps else FAIL
            # 失败时取一次失败样本做 note / detail（便于排查），全过取第一次
            k = statuses.index(FAIL) if status == FAIL else 0
            note = "%d/%d 稳定" % (n_pass, reps) + \
                ("" if status == PASS else " ｜ " + notes[k])
            detail = details[k]
            detail["votes"] = "%d/%d" % (n_pass, reps)
            detail["repeat_statuses"] = list(statuses)
            st = sts_all[k]
        else:
            status, note, detail, st = statuses[0], notes[0], details[0], sts_all[0]

        detail["l1"] = _l1(c, st)
        detail["l3"] = _l3(c, st, status == PASS)
        rows.append((c, status, note, c["_domain"], c["_intent"], detail, dt))
        print("  %-4s %-4s %-9s %5.1fs  %s %s" % (
            status, c["id"], c.get("type"), dt,
            "L1✓" if detail["l1"] else "L1✗",
            note[:88]))

    total = time.perf_counter() - t0
    stats = summarize(rows)
    over = stats["overall"]

    print("\n%d/%d 通过 ｜ L1 %.1f%% L2 %.1f%% L3 %.1f%% ｜ 加权 %.1f ｜ 耗时 %.1fs ｜ %d+%d tok"
          % (over["passed"], over["n"], over["l1"], over["l2"], over["l3"],
             over["score"], total, over["tokens_in"], over["tokens_out"]))
    print("答案层判据 %d/%d 题 ｜ 软校验不符 %d 题（不计入判定）"
          % (over["answer_layer"], over["n"], over["soft_mismatch"]))

    if stats["domains"]:
        print("\n分域：")
        print("  %-12s %4s %7s %7s %7s %7s" % ("域", "题数", "L1", "L2", "L3", "加权"))
        for name, b in stats["domains"].items():
            print("  %-12s %4d %6.1f%% %6.1f%% %6.1f%% %7.1f"
                  % (name, b["n"], b["l1"], b["l2"], b["l3"], b["score"]))

    if stats.get("stability"):
        print("\n稳定性（--repeat 多数投票）："
              + " ｜ ".join("%s：%d 题" % (k, v)
                             for k, v in sorted(stats["stability"].items())))

    bad = [r for r in rows if r[1] == FAIL]
    if bad:
        print("\n失败明细：")
        for c, _, note, dom, it, d, _ in bad:
            print("  - %s [%s/%s] %s：%s" % (c["id"], dom, it, c["question"], note))
            if d.get("sql"):
                print("    SQL: %s" % d["sql"][:160])

    write_md(md_path, data, rows, stats, total)
    print("\n报告已写入 %s%s" % (md_path, "（部分跑，未覆盖全量报告）" if partial else ""))

    # 历史记录：一次评测只说明"现在好不好"，回答不了"是不是在变好"。
    # 只追加不改写——同一天重跑十次就是十行，因为"改完立刻重跑"本身是信息。
    # 部分跑（--id/--tag/--quick）标 partial，不进趋势线：
    # 拿 3 道题的成绩去接 127 道题的曲线，只会把趋势画成锯齿。
    if not a.no_history:
        try:
            import history
            history.append(history.from_run(
                stats,
                {"data_window": "~".join(data["data_window"]),
                 "clock": data["clock"],
                 "generator": cfg["generator"]["mode"],
                 "model": cfg["generator"].get("model"),
                 "cases": len(cases)},
                partial=partial, elapsed=total))
            print("评测历史：已记录%s" % ("（部分跑，不进趋势线）" if partial else ""))
        except Exception as e:            # 记录失败不该让整轮评测判失败
            print("评测历史记录失败（不影响判定）：%s" % e)

    if a.json:
        write_json(a.json, data, rows, stats, total, cfg)
        print("机读结果已写入 %s" % a.json)
    return 1 if bad else 0


def write_json(path: str, data: dict, rows: list, stats: dict,
               elapsed: float, cfg: dict) -> None:
    out = {
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "meta": {
            "data_window": data["data_window"], "clock": data["clock"],
            "generator": cfg["generator"]["mode"],
            "model": cfg["generator"]["model"],
            "cost_price": cfg.get("cost") or {},
        },
        "stats": stats,
        "elapsed_sec": round(elapsed, 1),
        "cases": [
            {
                "id": c["id"], "type": c.get("type"), "domain": dom, "intent": it,
                "question": c.get("question"), "user": c.get("user") or "internal",
                "status": st, "note": note,
                "votes": d.get("votes"),
                "l1": d.get("l1"), "l3": d.get("l3"),
                # 阶段 3：形态返工次数要进机读产物，否则"返工了多少题"
                # 只能靠翻日志数——这个数字本身就是推理质量的指标。
                "shape": (d.get("shape") or {}).get("shape"),
                "shape_retries": d.get("shape_retries") or 0,
                "shape_unresolved": bool(d.get("shape_unresolved")),
                "sql": d.get("sql"), "actual": d.get("actual"),
                "expected": c.get("expected", c.get("expected_min")),
                # 阶段 5：判据在答案层还是结果集层，要能从机读产物里区分出来。
                # 否则"全绿"分不清是"答案写对了"还是"结果集凑巧对上了"。
                "answer_layer": bool(d.get("answer_layer")),
                "answer_failed": d.get("answer_must_failed") or [],
                "soft_mismatch": [
                    {"field": s["field"], "actual": s["actual"], "want": s["want"]}
                    for s in (d.get("soft_mismatch") or [])],
                "compare_delta": d.get("compare_delta"),
                "compare_base": d.get("compare_base"),
                "tokens_in": d.get("tokens_in"), "tokens_out": d.get("tokens_out"),
            }
            for c, st, note, dom, it, d, _ in rows
        ],
    }
    io.open(path, "w", encoding="utf-8").write(
        json.dumps(out, ensure_ascii=False, indent=1) + "\n")


def write_md(path: str, data: dict, rows: list, stats: dict, elapsed: float) -> None:
    over = stats["overall"]
    L = ["# 评测报告（阶段 2）\n",
         "窗口：%s ｜ 时钟：%s ｜ 用例：%d 条\n"
         % ("~".join(data["data_window"]), data["clock"], len(rows)),
         "> 期望值来自独立重算与 SQL 实测，**不是** Agent 自己的输出。",
         "> L1=可执行，L2=正确，L3=可采纳；加权 `0.2·L1 + 0.5·L2 + 0.3·L3`。\n",
         "**总体：%d/%d 通过 ｜ L1 %.1f%% · L2 %.1f%% · L3 %.1f%% ｜ 加权 %.1f ｜ %.1fs**\n"
         % (over["passed"], over["n"], over["l1"], over["l2"], over["l3"],
            over["score"], elapsed),
         "## 分域\n",
         "| 域 | 题数 | L1 | L2 | L3 | 加权 | 失败 |",
         "|---|---|---|---|---|---|---|"]
    for name, b in stats["domains"].items():
        L.append("| %s | %d | %.1f%% | %.1f%% | %.1f%% | %.1f | %d |"
                 % (name, b["n"], b["l1"], b["l2"], b["l3"], b["score"], b["failed"]))

    L += ["\n## 用例明细\n",
          "| 用例 | 域 | 意图 | 类型 | 问题 | 期望 | 实测 | L1 | L2 | L3 | 耗时 |",
          "|---|---|---|---|---|---|---|---|---|---|---|"]
    for c, status, note, dom, it, d, dt in rows:
        want = c.get("expected_min") if c.get("expected_min") is not None else c.get("expected", "—")
        L.append("| `%s` | %s | %s | %s | %s | %s | %s | %s | %s | %s | %.1fs |" % (
            c["id"], dom, it, c.get("type"), (c.get("question") or "").replace("|", "\\|"),
            want, d.get("actual", d.get("rows", "—")),
            "✓" if d.get("l1") else "✗", "✓" if status == PASS else "✗",
            "✓" if d.get("l3") else "✗", dt))

    n_layer = sum(1 for r in rows if r[5].get("answer_layer"))
    L.append("\n## 答案层判据\n")
    L.append("以答案正文为准判定的题：**%d / %d**。其余用例仍按结果集/行数判定"
             "（行数门槛类问题在答案里没有对应数字，无法抬到答案层）。\n"
             % (n_layer, len(rows)))
    soft = [r for r in rows if r[5].get("soft_mismatch")]
    if soft:
        L.append("答案判过、但结果集与期望不符——多半是期望值过期或口径漂移，"
                 "按设计**不计入判定**，但需要人看一眼：\n")
        L.append("| 用例 | 字段 | 结果集实得 | 期望 |")
        L.append("|---|---|---|---|")
        for c, status, note, dom, it, d, _ in soft:
            for s in d["soft_mismatch"]:
                L.append("| `%s` | %s | %s | %s |"
                         % (c["id"], s["field"], s["actual"], s["want"]))
        L.append("")

    L.append("\n## 失败明细\n")
    bad = [r for r in rows if r[1] == FAIL]
    if not bad:
        L.append("无。\n")
    for c, status, note, dom, it, d, _ in bad:
        L.append("### `%s` %s\n" % (c["id"], c.get("question")))
        L.append("- 域/意图：%s / %s ｜ 身份：%s" % (dom, it, c.get("user") or "internal"))
        L.append("- 判定：%s" % note)
        L.append("- SQL：`%s`" % (d.get("sql") or "").replace("`", "'"))
        if d.get("refuse_reason"):
            L.append("- 拒答理由：%s" % d["refuse_reason"])
        if d.get("findings"):
            L.append("- 护栏拦截：%s" % ", ".join(d["findings"]))
        L.append("")

    io.open(path, "w", encoding="utf-8").write("\n".join(L) + "\n")


if __name__ == "__main__":
    sys.exit(main())
