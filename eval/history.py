"""评测历史：把每次评测的分数记下来，画成趋势。

为什么值得单独做一件事
----------------------
单次评测只回答"现在好不好"，回答不了"是不是在变好"。而没有趋势线时，
一个隐蔽的退化会被当成噪声：某次改动让 L3 从 100 掉到 96，单看报告
还是"96 分，不错"；连着五轮都 96 就会以为是天花板，其实是从 100 掉下来的。

这套 Agent 最容易发生的退化恰好不是崩溃，而是**判据变松**：
答案层覆盖率悄悄从 51% 掉到 40%，通过率还是 100%——因为大家又回去看
SQL 返回的列了。分数不会报警，覆盖率会。

所以这里记两件事：分数（L1/L2/L3/加权）和**判据的硬度**（答案层覆盖数、
软校验不符数）。后者才是这套系统有没有在"真判"的证据。

设计上的三个选择
----------------
· **只追加，不改写**：`history.jsonl` 一行一次，任何一次跑都留痕。
  允许重复（同一天跑十次就是十行），因为"改完立刻重跑"本身就是信息。
· **部分跑单独标记**：`--id` / `--tag` / `--quick` 的结果不进趋势线。
  用 3 道题的成绩去接 127 道题的曲线，会把趋势画成锯齿。
· **不引图表库**：内联 SVG + 原生 CSS，单文件、离线可看、能直接提交进仓库。
  引 ECharts 意味着看板依赖 CDN，答辩现场断网就白屏。
"""
from __future__ import annotations

import argparse
import io
import json
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
PATH = ROOT / "eval" / "history.jsonl"
HTML_PATH = ROOT / "eval" / "history.html"


# ---------------------------------------------------------------- 读写
def append(rec: dict) -> dict:
    rec = dict(rec)
    rec.setdefault("ts", time.strftime("%Y-%m-%dT%H:%M:%S"))
    PATH.parent.mkdir(parents=True, exist_ok=True)
    with io.open(PATH, "a", encoding="utf-8") as f:
        f.write(json.dumps(rec, ensure_ascii=False) + "\n")
    return rec


def load() -> list[dict]:
    if not PATH.exists():
        return []
    out = []
    for line in PATH.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            out.append(json.loads(line))
        except ValueError:
            continue          # 坏行不阻断看板：宁可少一条，也不要白屏
    return out


def from_run(stats: dict, meta: dict, *, partial: bool = False,
             label: str = "", elapsed: float = 0.0) -> dict:
    """把 run.py 的 summarize() 结果压成一条历史记录。"""
    o = stats.get("overall") or {}
    n = int(o.get("n") or 0)
    return {
        "n": n, "passed": int(o.get("passed") or 0),
        "l1": o.get("l1"), "l2": o.get("l2"), "l3": o.get("l3"),
        "score": o.get("score"),
        # 判据硬度：这两项一起看才知道"100%"是真判出来的还是凑出来的
        "answer_layer": int(o.get("answer_layer") or 0),
        "answer_cover": round(100.0 * (o.get("answer_layer") or 0) / n, 1) if n else 0.0,
        "soft_mismatch": int(o.get("soft_mismatch") or 0),
        "tokens_in": int(o.get("tokens_in") or 0),
        "tokens_out": int(o.get("tokens_out") or 0),
        "elapsed_sec": round(elapsed, 1),
        "domains": {k: {"n": v.get("n"), "score": v.get("score"),
                        "l2": v.get("l2"), "l3": v.get("l3")}
                    for k, v in (stats.get("domains") or {}).items()},
        "meta": meta, "partial": bool(partial), "label": label,
    }


# ---------------------------------------------------------------- 绘图
def _series(recs: list[dict], key: str) -> list[float]:
    out = []
    for r in recs:
        v = r.get(key)
        out.append(float(v) if isinstance(v, (int, float)) else 0.0)
    return out


def _poly(vals: list[float], w: int, h: int, pad: int,
          lo: float = 0.0, hi: float = 100.0) -> str:
    if not vals:
        return ""
    if len(vals) == 1:                       # 一个点画不出线，给个点
        x, y = w / 2.0, h - pad
        return "%0.1f,%0.1f" % (x, y)
    span = max(1e-9, hi - lo)
    step = (w - 2 * pad) / (len(vals) - 1)
    pts = []
    for i, v in enumerate(vals):
        x = pad + i * step
        y = h - pad - (max(lo, min(hi, v)) - lo) / span * (h - 2 * pad)
        pts.append("%0.1f,%0.1f" % (x, y))
    return " ".join(pts)


def _chart(recs: list[dict], series: list[tuple[str, str]], title: str,
           lo: float = 0.0, hi: float = 100.0, fmt: str = "%.0f") -> str:
    """series = [(字段, 图例名)]。单条线时填充面积，多条线只画折线。"""
    w, h, pad = 660, 170, 34
    if not recs:
        return ('<div class="card"><h3>%s</h3>'
                '<p class="empty">还没有记录。跑一次 <code>eval/run.py</code> 就会出现。</p></div>'
                % title)
    parts = ['<div class="card"><h3>%s</h3>' % title,
             '<svg viewBox="0 0 %d %d" class="chart" role="img">' % (w, h)]
    # 网格：4 条水平线
    for i in range(5):
        v = lo + (hi - lo) * i / 4.0
        y = h - pad - (h - 2 * pad) * i / 4.0
        parts.append('<line x1="%d" y1="%0.1f" x2="%d" y2="%0.1f" class="grid"/>'
                     % (pad - 6, y, w - pad + 6, y))
        parts.append('<text x="%d" y="%0.1f" class="axis" text-anchor="end">%s</text>'
                     % (pad - 10, y + 4, fmt % v))
    colors = ["var(--c1)", "var(--c2)", "var(--c3)"]
    for si, (key, name) in enumerate(series):
        vals = _series(recs, key)
        pts = _poly(vals, w, h, pad, lo, hi)
        if len(series) == 1 and pts:
            parts.append('<polygon points="%s %d,%d %d,%d" class="area" '
                         'style="fill:%s"/>' % (pts, w - pad, h - pad, pad, h - pad,
                                                colors[si]))
        parts.append('<polyline points="%s" class="line" style="stroke:%s"/>'
                     % (pts, colors[si]))
        step = (w - 2 * pad) / max(1, len(vals) - 1)
        for i, v in enumerate(vals):
            x = pad + i * step if len(vals) > 1 else w / 2.0
            y = h - pad - (max(lo, min(hi, v)) - lo) / max(1e-9, hi - lo) * (h - 2 * pad)
            parts.append('<circle cx="%0.1f" cy="%0.1f" r="2.6" style="fill:%s">'
                         '<title>%s · %s = %s</title></circle>'
                         % (x, y, colors[si], recs[i].get("ts", "")[:16], name,
                            fmt % v))
    parts.append("</svg>")
    parts.append('<div class="legend">%s</div>' % "".join(
        '<span><i style="background:%s"></i>%s</span>' % (colors[i], n)
        for i, (_, n) in enumerate(series)))
    parts.append("</div>")
    return "".join(parts)


def _domain_table(rec: dict | None) -> str:
    if not rec:
        return ""
    doms = rec.get("domains") or {}
    if not doms:
        return ""
    rows = []
    for name, d in sorted(doms.items(), key=lambda kv: -(kv[1].get("score") or 0)):
        s = d.get("score")
        s = 0 if s is None else float(s)
        # 分数越低背景越偏红：一眼看出哪个域在拖后腿
        tint = "rgba(210,60,60,%.2f)" % min(0.42, max(0.0, (95 - s) / 100.0))
        if s >= 99.5:
            tint = "rgba(40,150,90,0.16)"
        rows.append('<tr><td>%s</td><td class="num">%s</td>'
                    '<td class="num" style="background:%s">%.1f</td>'
                    '<td class="num">%s</td><td class="num">%s</td></tr>'
                    % (name, d.get("n") or 0, tint, s,
                       d.get("l2"), d.get("l3")))
    return ('<div class="card"><h3>分域（最近一次）</h3><table><thead><tr>'
            '<th>业务域</th><th class="num">题数</th><th class="num">加权</th>'
            '<th class="num">L2 正确</th><th class="num">L3 可采纳</th></tr></thead>'
            '<tbody>%s</tbody></table></div>' % "".join(rows))


def _run_table(recs: list[dict], limit: int = 12) -> str:
    if not recs:
        return ""
    rows = []
    for r in reversed(recs[-limit:]):
        tag = "部分" if r.get("partial") else "全量"
        rows.append(
            '<tr><td>%s</td><td>%s</td><td class="num">%s/%s</td>'
            '<td class="num">%s</td><td class="num">%s</td>'
            '<td class="num">%s%%</td><td class="num">%s</td><td class="num">%ss</td></tr>'
            % (r.get("ts", "")[:16].replace("T", " "), tag,
               r.get("passed"), r.get("n"), r.get("score") or 0,
               r.get("l3") or 0, r.get("answer_cover") or 0,
               r.get("soft_mismatch") or 0, r.get("elapsed_sec") or 0))
    return ('<div class="card"><h3>最近 %d 次</h3><table><thead><tr>'
            '<th>时间</th><th>范围</th><th class="num">通过</th><th class="num">加权</th>'
            '<th class="num">L3</th><th class="num">答案层覆盖</th>'
            '<th class="num">软校验不符</th><th class="num">耗时</th></tr></thead>'
            '<tbody>%s</tbody></table></div>' % (len(rows), "".join(rows)))


CSS = """
:root{--bg:#f6f7f9;--fg:#1c2028;--muted:#6b7280;--card:#fff;--bd:#e3e6ec;
--c1:#2f6df6;--c2:#2aa06a;--c3:#d4741f;--grid:#e7eaf0}
@media (prefers-color-scheme:dark){:root{--bg:#14171c;--fg:#e8ebf1;
--muted:#98a1b0;--card:#1b1f26;--bd:#2a303a;--c1:#6ea0ff;--c2:#4cd19a;
--c3:#f0a55a;--grid:#252b34}}
*{box-sizing:border-box}
body{margin:0;background:var(--bg);color:var(--fg);
font:14px/1.65 -apple-system,"Segoe UI","PingFang SC","Microsoft YaHei",sans-serif}
.wrap{max-width:760px;margin:0 auto;padding:28px 20px 60px}
h1{font-size:20px;margin:0 0 4px}
.sub{color:var(--muted);font-size:13px;margin-bottom:22px}
.card{background:var(--card);border:1px solid var(--bd);border-radius:10px;
padding:16px 18px;margin-bottom:16px}
.card h3{margin:0 0 10px;font-size:14px;font-weight:600}
.chart{width:100%;height:auto;display:block}
.grid{stroke:var(--grid);stroke-width:1}
.axis{fill:var(--muted);font-size:10px}
.line{fill:none;stroke-width:1.8;stroke-linejoin:round}
.area{opacity:.10}
.legend{display:flex;gap:14px;margin-top:6px;color:var(--muted);font-size:12px}
.legend i{display:inline-block;width:9px;height:9px;border-radius:2px;margin-right:5px}
table{width:100%;border-collapse:collapse;font-size:12.5px}
th,td{padding:6px 8px;border-bottom:1px solid var(--bd);text-align:left}
th{color:var(--muted);font-weight:500;font-size:12px}
.num{text-align:right;font-variant-numeric:tabular-nums}
tr:last-child td{border-bottom:none}
.empty{color:var(--muted);margin:6px 0 0}
code{background:var(--grid);padding:1px 5px;border-radius:4px;font-size:12px}
.note{color:var(--muted);font-size:12px;margin-top:18px}
"""


def render_html(recs: list[dict] | None = None) -> str:
    recs = load() if recs is None else recs
    full = [r for r in recs if not r.get("partial")]
    last = full[-1] if full else (recs[-1] if recs else None)
    head = ('<h1>评测历史</h1><div class="sub">%d 次记录（其中 %d 次全量）'
            ' ｜ 数据窗口 %s ｜ 生成于 %s</div>'
            % (len(recs), len(full),
               ((last or {}).get("meta") or {}).get("data_window", "—"),
               time.strftime("%Y-%m-%d %H:%M")))
    body = [
        _chart(full, [("score", "加权分")], "加权分（0.2·L1 + 0.5·L2 + 0.3·L3）"),
        _chart(full, [("l1", "L1 可执行"), ("l2", "L2 正确"), ("l3", "L3 可采纳")],
               "三层通过率"),
        # 这条线是本次新增的重点：分数不动而覆盖率下滑，是最危险的退化
        _chart(full, [("answer_cover", "答案层覆盖")],
               "答案层覆盖率 —— 判据有多硬（越高越不依赖结果集凑巧）",
               fmt="%.0f"),
        _domain_table(last),
        _run_table(recs),
        '<p class="note">只追加不改写：同一轮改动重跑几次就有几行。'
        '用 <code>--id/--tag/--quick</code> 跑的记为「部分」，不画进趋势线——'
        '拿 3 道题的成绩接 127 道题的曲线，只会画出锯齿。</p>',
    ]
    return ('<!DOCTYPE html><html lang="zh-CN"><head><meta charset="utf-8">'
            '<meta name="viewport" content="width=device-width,initial-scale=1">'
            '<title>评测历史 · Data Agent</title><style>%s</style></head>'
            '<body><div class="wrap">%s%s</div></body></html>' % (CSS, head, "".join(body)))


def by_day(recs: list[dict]) -> list[dict]:
    """按天汇总：同一天跑多次取**最后一次全量**。

    验收里"分域准确率按天可查"要的就是这个口径——一天之内反复重跑是过程，
    当天最终状态才是结论。取平均会把"早上跑坏的两次"摊到结果里，
    看板上就永远看不到真实的修复效果。
    """
    days: dict[str, dict] = {}
    for r in recs:
        if r.get("partial"):
            continue
        d = str(r.get("ts") or "")[:10]
        if d:
            days[d] = r              # 顺序即时间顺序，后面的覆盖前面的
    return [dict(v, day=k) for k, v in sorted(days.items())]


def render_file(path: Path | None = None) -> Path:
    path = path or HTML_PATH
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(render_html(), encoding="utf-8")
    return path


def main() -> int:
    ap = argparse.ArgumentParser(description="评测历史与看板")
    ap.add_argument("--html", action="store_true", help="生成 history.html")
    ap.add_argument("--tail", type=int, default=0, help="打印最近 N 条")
    ap.add_argument("--days", action="store_true", help="按天汇总（取当天最后一次全量）")
    ap.add_argument("--json", action="store_true", help="打印全部记录（机读）")
    a = ap.parse_args()
    recs = load()
    if a.json:
        print(json.dumps(recs, ensure_ascii=False, indent=1))
        return 0
    if a.days:
        rows = by_day(recs)
        if not rows:
            print("还没有全量评测记录")
            return 0
        print("%-12s %8s %7s %7s %8s  %s" % ("日期", "通过", "加权", "L3", "答案层", "最弱域"))
        for r in rows:
            doms = r.get("domains") or {}
            weak = min(doms.items(), key=lambda kv: kv[1].get("score") or 0) if doms else None
            print("%-12s %7s/%-3s %7s %7s %7s%%  %s"
                  % (r.get("day"), r.get("passed"), r.get("n"), r.get("score"),
                     r.get("l3"), r.get("answer_cover"),
                     ("%s %.1f" % (weak[0], weak[1].get("score") or 0)) if weak else "—"))
        return 0
    if a.tail:
        for r in recs[-a.tail:]:
            print("%s  %s  %s/%s  加权 %s  L3 %s%%  答案层 %s%%  %.1fs"
                  % (r.get("ts", "")[:16], "部分" if r.get("partial") else "全量",
                     r.get("passed"), r.get("n"), r.get("score"), r.get("l3"),
                     r.get("answer_cover"), r.get("elapsed_sec") or 0))
        return 0
    if a.html:
        p = render_file()
        print("看板已写入 %s（%d 条记录）" % (p, len(recs)))
        return 0
    ap.print_help()
    return 1


if __name__ == "__main__":
    sys.exit(main())
