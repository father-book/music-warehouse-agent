"""把「不准」反馈转成候选评测题草稿。

为什么只出候选、不直接进基线
--------------------------
一条反馈能告诉我们"这条答错了"，但**不能告诉我们正确答案是多少**。
如果拿系统当时那条 SQL 的结果当 expected，就是用 Agent 自己的输出当标准答案——
那评测就变成了自证，和 docs/57 §4.10 强调的「期望值必须独立重算」直接冲突。

所以这个脚本的产出是**带上下文的草稿**：问题、当时的 SQL、用户说的错在哪、
当时的可信度级别；`expected` 留空，等人独立重算后填。填完再合进 `cases.yaml`
并跑一遍全量——**没填 expected 的条目不会被评测运行器读取**。

用法
----
    python eval/feedback_to_cases.py                 # 生成草稿到 eval/candidates_YYYYMMDD.yaml
    python eval/feedback_to_cases.py --dry-run       # 只打印统计，不写文件
    python eval/feedback_to_cases.py --days 180      # 放宽时间范围

去重键是「归一化问题 + 当时的 SQL」：同一个人反复点不准、或两个人问同一句话，
都只出一条——否则候选池会被同一个 badcase 灌满。
"""
from __future__ import annotations

import argparse
import datetime as _dt
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "code" / "agent"))

import _bootstrap  # noqa: E402,F401

import yaml  # noqa: E402
from conf import load_config  # noqa: E402
from governance import store  # noqa: E402

_RE_SPACE = re.compile(r"\s+")
_RE_DATE = re.compile(r"\d{4}-\d{2}-\d{2}|\d{1,2}-\d{1,2}")

REASON_CN = {"caliber": "口径不对", "number": "数字不对",
             "offtopic": "答非所问", "other": "其他"}


def _norm_q(q: str) -> str:
    """归一化问题：去空白、把具体日期替换成占位符。

    为什么要把日期抹掉：用户问「11-07 的完播率」标了不准，和「11-08 的完播率」
    是同一条口径问题；不抹日期，每天都来一条，候选池就没法看了。
    """
    s = _RE_SPACE.sub("", (q or "").strip())
    return _RE_DATE.sub("<日期>", s)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=90, help="看最近多少天的反馈")
    ap.add_argument("--limit", type=int, default=200)
    ap.add_argument("--out", default="", help="输出路径，默认 eval/candidates_YYYYMMDD.yaml")
    ap.add_argument("--dry-run", action="store_true", help="只统计，不写文件")
    a = ap.parse_args()

    cfg = load_config()
    rows = store.wrong_feedback(cfg, limit=a.limit, days=a.days)
    if not rows:
        print("最近 %d 天没有「不准」反馈，不用生成。" % a.days)
        return 0

    seen: set[tuple[str, str]] = set()
    picked: list[dict] = []
    for r in rows:
        key = (_norm_q(r.get("question") or ""),
               (r.get("final_sql") or r.get("applied_sql") or "").strip())
        if key in seen:
            continue
        seen.add(key)
        picked.append(r)

    today = _dt.date.today().isoformat()
    cases = []
    for i, r in enumerate(picked, 1):
        cases.append({
            "id": "FB%02d" % i,
            "type": "value",
            "domain": "待定",
            "intent": "待定",
            "question": r.get("question") or "",
            # 空 expected 是有意的：填之前必须独立重算，不能抄下面这条 SQL
            "expected": None,
            "tags": ["来自反馈", "待重算"],
            "feedback": {
                "trace_id": r.get("trace_id"),
                "reason": r.get("reason") or "",
                "reason_cn": REASON_CN.get(r.get("reason") or "", "未填"),
                "comment": r.get("comment") or "",
                "trust_level": r.get("trust_level") or "",
                "at": r.get("created_at"),
                "window": [r.get("time_start"), r.get("time_end")],
            },
            "sql_seen": (r.get("final_sql") or r.get("applied_sql") or "").strip(),
            "note": "用户在「%s」上标了『%s』。填 expected 前先独立重算；"
                    "sql_seen 只是当时系统给的写法，**不是标准答案**。"
                    % ((r.get("created_at") or "")[:10],
                       REASON_CN.get(r.get("reason") or "", "未填")),
        })

    by_reason: dict[str, int] = {}
    by_trust: dict[str, int] = {}
    for r in picked:
        by_reason[REASON_CN.get(r.get("reason") or "", "未填")] = \
            by_reason.get(REASON_CN.get(r.get("reason") or "", "未填"), 0) + 1
        by_trust[r.get("trust_level") or "—"] = by_trust.get(r.get("trust_level") or "—", 0) + 1

    print("== 反馈转候选评测题 ==")
    print("窗口：最近 %d 天 ｜ 原始「不准」%d 条 ｜ 去重后 %d 条"
          % (a.days, len(rows), len(cases)))
    print("原因分布：%s" % "、".join("%s %d" % (k, v) for k, v in
                                  sorted(by_reason.items(), key=lambda x: -x[1])))
    print("可信度分布：%s" % "、".join("%s %d" % (k, v) for k, v in
                                   sorted(by_trust.items(), key=lambda x: -x[1])))
    print("末条：%s ｜ %s"
          % ((cases[0]["question"] or "")[:40],
             (cases[0]["feedback"]["comment"] or "（无备注）")[:40]))

    if a.dry_run:
        print("\n--dry-run：未写文件。")
        return 0

    out = Path(a.out) if a.out else (ROOT / "eval" / ("candidates_%s.yaml"
                                                      % today.replace("-", "")))
    doc = {
        "version": 1,
        "generated_at": today,
        "source": "answer_feedback (verdict=wrong)",
        "warning": "期望值必须独立重算后填写；expected 为空的条目不会被评测运行器读取。",
        "cases": cases,
    }
    out.write_text(yaml.safe_dump(doc, allow_unicode=True, sort_keys=False),
                   encoding="utf-8")
    print("\n已写入 %s（%d 条，expected 全部留空待重算）" % (out.relative_to(ROOT), len(cases)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
