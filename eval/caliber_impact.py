#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""口径变更影响面：改口径前后各跑一次，回答三个问题。

    python eval/caliber_impact.py --save     # 改口径之前：把当前语义层存成快照
    python eval/caliber_impact.py            # 改完之后：对比，列出变更 + 受影响的评测题
    python eval/caliber_impact.py --gate     # 顺带跑门禁，一条命令给出结论
    python eval/caliber_impact.py --json eval/caliber_impact.json

为什么需要它：
  语义层是「改一处、全链路生效」的。改一条指标的 formula，影响的不只是直接
  问这个指标的题——凡是被 route 召回该指标或该表的题都可能变。手工判断"改了
  这个会打坏哪几道"从来没有可靠过，而评测 128 题跑一轮要 5 分钟：不知道
  该看哪几题时，人只会看总分，总分没掉就当没事——那正是口径悄悄漂移的温床。

判据说明（为什么受影响题用 route 而不是关键词匹配）：
  关键词匹配只能看到"问题文本里有没有提到这个指标"。而 C18 那种情况恰恰是
  问题里没有指标名、由 route 的语义召回补出来的。所以默认跑一遍 route，
  拿它真实的候选指标与候选表去和变更集求交——**按系统实际会用的东西算影响面，
  而不是按人以为会用的东西算**。--fast 可退回关键词匹配（约快 60 倍，会漏）。
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "code" / "agent"))

import _bootstrap  # noqa: E402,F401

import semantic  # noqa: E402
from conf import load_config  # noqa: E402

SNAP_PATH = ROOT / "eval" / "semantic_snapshot.json"
CASES_PATH = ROOT / "eval" / "cases.yaml"
ADV_PATH = ROOT / "eval" / "adversarial.yaml"

SECTIONS = (
    ("metrics", "指标"),
    ("caliber_guard", "口径规则"),
    ("tables", "表边界"),
    ("dimensions", "维度"),
    ("glossary", "词典"),
)


def _read_yaml(p: Path) -> dict:
    import io
    import yaml
    return yaml.safe_load(io.open(p, encoding="utf-8").read()) or {}


def load_cases() -> list[dict]:
    """与 eval/run.py 一致：cases.yaml + adversarial.yaml。"""
    data = _read_yaml(CASES_PATH)
    cases = list(data.get("cases") or [])
    if ADV_PATH.exists():
        for c in (_read_yaml(ADV_PATH).get("cases") or []):
            c.setdefault("set", "adversarial")
            cases.append(c)
    return cases


def snapshot(cfg: dict) -> dict:
    """快照 = 影响口径的字段 + 版本号。

    直接复用 caliber_digest(with_payload=True) 的 payload：版本号和差异分析
    必须基于**同一份**数据，否则会出现"版本变了但 diff 说没变"这种自相矛盾。
    """
    d = semantic.caliber_digest(cfg, with_payload=True)
    return {
        "version": d["version"],
        "hash": d["hash"],
        "counts": d["counts"],
        "saved_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        "payload": d["payload"],
    }


def _as_map(section: str, value) -> dict:
    """caliber_guard 是 list（按 id 索引），其余是 dict。统一成 dict 才能 diff。"""
    if isinstance(value, list):
        return {(str(x.get("id")) if isinstance(x, dict) else str(x)): x
                for x in value}
    return dict(value or {})


def _diff_map(old: dict, new: dict, limit_fields: int = 6) -> dict:
    out = {"add": [], "remove": [], "modify": {}}
    o = _as_map("", old)
    n = _as_map("", new)
    out["add"] = sorted(set(n) - set(o))
    out["remove"] = sorted(set(o) - set(n))
    for k in sorted(set(o) & set(n)):
        if o[k] == n[k]:
            continue
        fo = o[k] if isinstance(o[k], dict) else {"value": o[k]}
        fn = n[k] if isinstance(n[k], dict) else {"value": n[k]}
        fields = {}
        for f in sorted(set(fo) | set(fn)):
            if fo.get(f) != fn.get(f):
                fields[f] = [fo.get(f), fn.get(f)]
        out["modify"][k] = fields
    return out


def diff_payload(old: dict, new: dict) -> dict:
    return {name: _diff_map(old.get(name), new.get(name)) for name, _ in SECTIONS}


def _is_empty(sec: dict) -> bool:
    return not (sec.get("add") or sec.get("remove") or sec.get("modify"))


def changed_keys(d: dict) -> tuple[set, set, bool]:
    """返回 (变更指标集, 变更表集, 口径规则是否变)。

    词条变更也要折算成指标：改一条 glossary 词条的本质影响是"这个问题以后
    会命中另一个指标"，所以它指向的 metric 必须进变更集，否则影响面会漏题。
    """
    mets = set(d["metrics"]["add"]) | set(d["metrics"]["remove"]) \
        | set(d["metrics"]["modify"])
    tabs = set(d["tables"]["add"]) | set(d["tables"]["remove"]) \
        | set(d["tables"]["modify"])
    for word in list(d["glossary"]["add"]) + list(d["glossary"]["remove"]) \
            + list(d["glossary"]["modify"]):
        pass  # 词条到指标的映射需要看内容，交给下面按 payload 补
    rules_changed = not _is_empty(d["caliber_guard"])
    return mets, tabs, rules_changed


def _terms_metrics(payload: dict, words: set) -> set:
    gl = payload.get("glossary") or {}
    out = set()
    for w in words:
        ref = gl.get(w) or {}
        if isinstance(ref, dict) and ref.get("metric"):
            out.add(ref["metric"])
    return out


def _rule_tables(payload: dict, ids: set) -> set:
    """口径规则作用在哪张表上——规则里写的是 `table` / `tables`。

    这一步不能省：改一条口径规则（比如 C6 占比分子分母同源，只挂在
    ads_province_dist 上）不会改任何指标的 formula，若只按指标算影响面，
    会得出"0 条受影响"的错误结论，然后门禁一跑却发现 C15 变了。
    """
    guards = _as_map("caliber_guard", (payload or {}).get("caliber_guard"))
    out = set()
    for i in ids:
        g = guards.get(i) or {}
        if g.get("table"):
            out.add(str(g["table"]))
        for t in (g.get("tables") or []):
            out.add(str(t))
    return out


def affected_cases(cases: list[dict], mets: set, tabs: set, cfg: dict,
                   fast: bool = False) -> list[dict]:
    """按 route 实际召回求交。route 抛异常的题目按"未判定"列出来，不静默跳过。"""
    out = []
    for c in cases:
        q = c.get("question") or ""
        if not q:
            continue
        got_m, got_t, how = set(), set(), "route"
        try:
            if fast:
                got_m = {h["code"] for h in semantic.find_metrics(q, cfg)}
                got_t = {t.get("table") for t in semantic.find_tables(q, cfg, top=8)}
                how = "关键词"
            else:
                import node_route
                st = node_route.route({"question": q, "lane": "offline"}, cfg)
                got_m = set(st.get("metrics") or [])
                got_t = set(st.get("tables") or [])
        except Exception as e:
            out.append({"id": c.get("id"), "question": q, "error": str(e)[:120]})
            continue
        hit_m, hit_t = got_m & mets, got_t & tabs
        if hit_m or hit_t:
            out.append({
                "id": c.get("id"), "question": q,
                "intent": c.get("intent"), "domain": c.get("domain"),
                "metrics": sorted(hit_m), "tables": sorted(hit_t), "by": how,
            })
    return out


def _fmt(v, width: int = 90) -> str:
    s = str(v)
    return s if len(s) <= width else s[:width] + "…"


def _delta_text(a, b, width: int = 46) -> tuple[str, str]:
    """截到**第一个不同处**附近。

    直接截前 60 字在长 trap 上完全没用：两条 text 的前缀一模一样，
    输出成 "…同样的一长串… → …同样的一长串…"，看起来像没改。
    看 diff 的人要的正是"哪一句话变了"。
    """
    sa, sb = str(a or ""), str(b or "")
    if sa == sb:
        return _fmt(sa, width), _fmt(sb, width)
    i = 0
    while i < min(len(sa), len(sb)) and sa[i] == sb[i]:
        i += 1
    lo = max(0, i - 16)
    pre = "…" if lo > 0 else ""
    return (pre + sa[lo:lo + width] + "…", pre + sb[lo:lo + width] + "…")


def print_report(old: dict, new: dict, d: dict, hits: list[dict],
                 gate_result: dict | None = None,
                 rules_changed: bool = False, rule_tables: set | None = None) -> None:
    print("== 口径变更影响面 ==")
    print("版本：%s → %s%s" % (
        old.get("version"), new.get("version"),
        "（未变）" if old.get("version") == new.get("version") else ""))
    print("规模：%s → %s" % (old.get("counts"), new.get("counts")))
    print()

    any_change = False
    for name, cn in SECTIONS:
        sec = d[name]
        if _is_empty(sec):
            continue
        any_change = True
        bits = []
        if sec["add"]:
            bits.append("新增 %d" % len(sec["add"]))
        if sec["remove"]:
            bits.append("删除 %d" % len(sec["remove"]))
        if sec["modify"]:
            bits.append("修改 %d" % len(sec["modify"]))
        print("【%s】%s" % (cn, " / ".join(bits)))
        for k in sec["add"]:
            print("  + %s" % k)
        for k in sec["remove"]:
            print("  - %s" % k)
        for k, fields in sec["modify"].items():
            print("  ~ %s" % k)
            for f, (a, b) in list(fields.items())[:6]:
                da, db = _delta_text(a, b)
                print("      %s: %s → %s" % (f, da, db))
            if len(fields) > 6:
                print("      （另有 %d 个字段变化）" % (len(fields) - 6))
        print()

    if not any_change:
        print("语义层内容无变化（版本一致时这是正常的：只改了注释/缩进不算口径变更）。")
        print()
        return

    if rules_changed:
        if rule_tables:
            print("口径规则变更，作用表：%s（只影响用到这些表的题）"
                  % "、".join(sorted(rule_tables)))
        else:
            print("⚠ 口径规则变更且规则未声明作用表 → 影响面等同全量，"
                  "必须跑全量评测 + 门禁。")
        print()

    if hits:
        err = [h for h in hits if h.get("error")]
        real = [h for h in hits if not h.get("error")]
        print("受影响评测题：%d 条%s" % (
            len(real), "（另有 %d 条判定失败，见下）" % len(err) if err else ""))
        for h in real:
            bits = []
            if h.get("metrics"):
                bits.append("指标 " + "、".join(h["metrics"]))
            if h.get("tables"):
                bits.append("表 " + "、".join(h["tables"]))
            print("  · %-4s [%s/%s] %s" % (
                h.get("id"), h.get("domain") or "-", h.get("intent") or "-",
                _fmt(h.get("question"), 60)))
            print("        命中 %s" % "；".join(bits))
        for h in err:
            print("  ? %-4s 召回失败：%s" % (h.get("id"), h.get("error")))
        print()
        print("建议：先只跑这几题确认方向，再跑全量。")
        ids = " ".join("--id %s" % h["id"] for h in real[:8])
        if ids:
            print("  python eval/run.py %s --repeat 3" % ids)
        print("  python eval/run.py --json eval/last.json && python eval/gate.py")
    else:
        print("受影响评测题：0 条")
        print("说明：变更的指标/表没有被任何用例的 route 召回。这通常意味着")
        print("      ① 改的是边角指标，或 ② 改动没真正生效（检查是否忘了")
        print("      gen_tables.py 重生成 tables.json）。")
    print()

    if gate_result is not None:
        print("== 门禁 ==")
        print(gate_result.get("raw") or "(无输出)")
        print("结论：%s" % ("通过" if gate_result.get("ok") else "未通过"))


def main() -> int:
    ap = argparse.ArgumentParser(description="语义层口径变更影响面")
    ap.add_argument("--save", action="store_true", help="保存当前语义层快照")
    ap.add_argument("--gate", action="store_true", help="对比后自动跑 eval/gate.py")
    ap.add_argument("--fast", action="store_true",
                    help="用关键词匹配代替 route（快 60 倍，会漏召回题）")
    ap.add_argument("--json", default="", help="机读结果输出路径")
    ap.add_argument("--limit", type=int, default=40, help="最多列几条受影响题")
    a = ap.parse_args()

    cfg = load_config()
    cur = snapshot(cfg)

    if a.save:
        SNAP_PATH.write_text(json.dumps(cur, ensure_ascii=False, indent=1),
                             encoding="utf-8")
        print("已保存语义层快照：%s" % SNAP_PATH)
        print("  版本 %s ｜ %s" % (cur["version"], cur["counts"]))
        print("  改完口径后直接跑 python eval/caliber_impact.py 看影响面。")
        return 0

    if not SNAP_PATH.exists():
        print("没有快照可比：%s" % SNAP_PATH)
        print("先跑 python eval/caliber_impact.py --save（建议在改口径之前）。")
        return 2

    old = json.loads(SNAP_PATH.read_text(encoding="utf-8"))
    d = diff_payload(old.get("payload") or {}, cur.get("payload") or {})
    mets, tabs, rules_changed = changed_keys(d)

    # 词条变更折算成指标：词条改了，本质是"这个问题以后命中另一个指标"
    gl_changed = set(d["glossary"]["add"]) | set(d["glossary"]["remove"]) \
        | set(d["glossary"]["modify"])
    if gl_changed:
        for side in (old.get("payload") or {}, cur.get("payload") or {}):
            mets |= _terms_metrics(side, gl_changed)

    # 口径规则变更折算成表：规则挂在表上，改规则不改指标公式
    rule_ids = set(d["caliber_guard"]["add"]) | set(d["caliber_guard"]["remove"]) \
        | set(d["caliber_guard"]["modify"])
    rule_tabs: set = set()
    if rule_ids:
        for side in (old.get("payload") or {}, cur.get("payload") or {}):
            rule_tabs |= _rule_tables(side, rule_ids)
        tabs |= rule_tabs

    hits = affected_cases(load_cases(), mets, tabs, cfg, fast=a.fast)
    hits = hits[:max(1, a.limit)] if hits else hits

    gate_result = None
    if a.gate:
        p = subprocess.run(
            [sys.executable, str(ROOT / "eval" / "gate.py")],
            capture_output=True, text=True, encoding="utf-8", errors="replace",
            cwd=str(ROOT))
        raw = (p.stdout or "") + (p.stderr or "")
        gate_result = {"raw": raw.strip()[-1200:], "ok": p.returncode == 0}

    if a.fast:
        # 实测差异：改 C6（只挂 ads_province_dist）时，fast 报 38 条、route 报 11 条。
        # fast 走的是纯检索 top-8，不经过粒度/权限/语义边界过滤，会把
        # "检索上相关但 route 根本不会选"的表也算进来。
        print("⚠ --fast 用关键词粗筛，会**高估**影响面：它不过粒度/权限/语义边界，")
        print("  会把 route 根本不会选中的表也算进受影响因素。结论以不带 --fast 为准。")
        print()

    print_report(old, cur, d, hits, gate_result,
                 rules_changed=rules_changed, rule_tables=rule_tabs)

    if a.json:
        Path(a.json).write_text(json.dumps({
            "old_version": old.get("version"), "new_version": cur.get("version"),
            "diff": d, "changed_metrics": sorted(mets),
            "changed_tables": sorted(tabs), "caliber_rules_changed": rules_changed,
            "affected": hits, "gate_ok": (gate_result or {}).get("ok"),
        }, ensure_ascii=False, indent=1), encoding="utf-8")
        print("机读结果已写入 %s" % a.json)

    # 门禁没过必须让 CI 拿到非 0，否则这个脚本又变成"看一眼就算过"的装饰
    if gate_result is not None and not gate_result.get("ok"):
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
