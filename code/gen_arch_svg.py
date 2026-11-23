# -*- coding: utf-8 -*-
"""生成双链路数仓架构图 SVG（答辩用，1680x1020，可直接插入 PPT / WPS）。

用法：python code/gen_arch_svg.py
输出：docs/figures/architecture-v10.svg
转 PNG：chrome --headless --window-size=1680,1020 --screenshot=... file:///...svg

改版记录：
  v1 首版：标题与副标题基线间距小于字号 → 文字重叠；中栏箭头斜穿左栏卡片。
  v2 修正 box() 垂直排版（副标题基线 = 标题基线 + 字号 + 4）；
     中栏卡片改为与对应行水平对齐，箭头只走 40px 横段，不再斜穿。
"""

W, H = 1680, 1020
P = []


def esc(s):
    return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


FONT = "Microsoft YaHei, PingFang SC, sans-serif"


def box(x, y, w, h, title, subs, fill, stroke, tsize=15.5, ssize=12.2, rx=9, dash=None, lw=1.6):
    d = ' stroke-dasharray="6 4"' if dash else ''
    P.append(f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="{rx}" ry="{rx}" '
             f'fill="{fill}" stroke="{stroke}" stroke-width="{lw}"{d}/>')
    n = len(subs)
    # 整块（标题 + n 行副标题）在框内垂直居中
    block = tsize + (4 + n * (ssize + 3) if n else 0)
    title_base = y + (h - block) / 2 + tsize * 0.78
    P.append(f'<text x="{x + w/2}" y="{title_base:.1f}" text-anchor="middle" font-family="{FONT}" '
             f'font-size="{tsize}" font-weight="bold" fill="#0f172a">{esc(title)}</text>')
    cy = title_base + ssize + 4
    for s in subs:
        P.append(f'<text x="{x + w/2}" y="{cy:.1f}" text-anchor="middle" font-family="{FONT}" '
                 f'font-size="{ssize}" fill="#475569">{esc(s)}</text>')
        cy += ssize + 3


def vdown(x, y1, y2, color="#94a3b8", lw=1.8):
    P.append(f'<line x1="{x}" y1="{y1}" x2="{x}" y2="{y2}" stroke="{color}" stroke-width="{lw}" '
             f'stroke-linecap="round" marker-end="url(#ah)"/>')


def hright(x1, x2, y, color="#94a3b8", lw=1.7, dash=True):
    d = ' stroke-dasharray="5 4"' if dash else ''
    P.append(f'<line x1="{x1}" y1="{y}" x2="{x2}" y2="{y}" stroke="{color}" stroke-width="{lw}" '
             f'stroke-linecap="round"{d} marker-end="url(#ah)"/>')


def hleft(x1, x2, y, color="#94a3b8", lw=1.7, dash=True):
    d = ' stroke-dasharray="5 4"' if dash else ''
    P.append(f'<line x1="{x1}" y1="{y}" x2="{x2}" y2="{y}" stroke="{color}" stroke-width="{lw}" '
             f'stroke-linecap="round"{d} marker-end="url(#ah)"/>')


def text(x, y, s, color="#475569", size=11, anchor="middle", weight="normal"):
    P.append(f'<text x="{x}" y="{y}" text-anchor="{anchor}" font-family="{FONT}" '
             f'font-size="{size}" font-weight="{weight}" fill="{color}">{esc(s)}</text>')


C = {
    "gen":   ("#f1f5f9", "#94a3b8"),
    "kafka": ("#fff7ed", "#f97316"),
    "flink": ("#eff6ff", "#3b82f6"),
    "doris": ("#faf5ff", "#a855f7"),
    "hive":  ("#f0fdf4", "#16a34a"),
    "hdfs":  ("#ecfeff", "#0891b2"),
    "bi":    ("#fdf2f8", "#db2777"),
    "key":   ("#fefce8", "#ca8a04"),
    "dim":   ("#f8fafc", "#94a3b8"),
}

P.append(f'<svg xmlns="http://www.w3.org/2000/svg" width="{W}" height="{H}" viewBox="0 0 {W} {H}">')
P.append('<defs><marker id="ah" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="6.5" markerHeight="6.5" '
         'orient="auto"><path d="M0,0 L10,5 L0,10 z" fill="#94a3b8"/></marker></defs>')
P.append(f'<rect width="{W}" height="{H}" fill="#ffffff"/>')

# ── 标题 ──────────────────────────────────────────────
text(50, 50, "音乐流媒体数仓 · 实时 + 离线双链路架构", "#0f172a", 26, "start", "bold")
text(50, 78, "v10.1  |  Flink 4 作业 · Doris 31 表 · Hive 31 表 × 31 天 · 批流同源同表名", "#64748b", 13, "start")

# ── 列标题 ────────────────────────────────────────────
for x, w, fill, col, label in [
    (50, 560, "#eff6ff", "#1d4ed8", "实时链路 · 秒级（Flink + Doris）"),
    (650, 380, "#f8fafc", "#334155", "共享层"),
    (1070, 560, "#f0fdf4", "#15803d", "离线链路 · T+1（Hive + Spark）"),
]:
    P.append(f'<rect x="{x}" y="100" width="{w}" height="36" rx="8" fill="{fill}"/>')
    P.append(f'<text x="{x + w/2}" y="124" text-anchor="middle" font-family="{FONT}" font-size="15" '
             f'font-weight="bold" fill="{col}">{label}</text>')

# ── 行坐标 ────────────────────────────────────────────
LX, LW, BH = 50, 560, 62
RX, RW = 1070, 560
MX, MW = 650, 380
rows = [160 + i * 80 for i in range(9)]      # 0..8

# 0 造数  1 Kafka  2 作业A  3 作业B  4 v_play_term
# 5 作业C  6 作业D  7 Doris  8 大屏

# ── 左栏：实时 ────────────────────────────────────────
box(LX, rows[0], LW, BH, "造数器  gen_events.py",
    ["26 类事件 · 跳出率 22% · 发送时时间戳右移"], *C["gen"])
box(LX, rows[1], LW, BH, "Kafka  music_events", ["原始埋点 topic · 12 分区"], *C["kafka"])
box(LX, rows[2], LW, BH, "作业A  rt_job_a_ods  →  Kafka ods_music_event",
    ["格式校验 · 脏数据侧输出到 dwd_quality_error"], *C["flink"])
box(LX, rows[3], LW, BH, "作业B  rt_job_b_dwd",
    ["清洗 · HBase 维表打宽 · 分流 11 张明细表", "出两个 topic：dwd_play_end / dwd_skip"], *C["flink"])
box(LX, rows[4], LW, BH, "v_play_term  播放终止口径视图",
    ["= dwd_play_end ∪ dwd_skip  （UNION ALL，不是相加）"], *C["key"], lw=2.4)
box(LX, rows[5], LW, BH, "作业C  rt_job_c_dws",
    ["1min / 5min 窗口 · 读 v_play_term 算完播率与切歌率"], *C["flink"])
box(LX, rows[6], LW, BH, "作业D  rt_job_d_ads",
    ["10min 热榜 / 省份占比 / 会话跳出 / 商业化日报"], *C["flink"])
box(LX, rows[7], LW, BH, "Doris  music_dw",
    ["DWD 11 表 · DWS 3 表 · ADS 7 表（日分区 + 动态 TTL）"], *C["doris"])
box(LX, rows[8], LW, BH, "实时大屏  Flask :5000",
    ["ADS 超 12 分钟未更新 → 自动回退明细现算"], *C["bi"])

for i in range(8):
    vdown(LX + LW / 2, rows[i] + BH + 1, rows[i + 1] - 3)

# ── 右栏：离线 ────────────────────────────────────────
box(RX, rows[0], RW, BH, "造数器  gen_offline_ods",
    ["T+1 每日 100 万行 · 出生-死亡生命周期模型"], *C["gen"])
box(RX, rows[1], RW, BH, "Kafka  music_events", ["与实时同源，同一个 topic"], *C["kafka"])
box(RX, rows[2], RW, BH, "Flume  →  landing",
    ["HDFS /landing/music_event/dt=…/  原样 text.gz"], *C["hdfs"])
box(RX, rows[3], RW, BH, "Spark  spark_landing_to_ods",
    ["按 event_id 去重 · 转 ORC · 写 ODS 分区"], *C["hdfs"])
box(RX, rows[4], RW, BH, "Hive  ODS  ods_music_event",
    ["31 分区（10-05 ~ 11-04）· 单日 99.99 万行"], *C["hive"])
box(RX, rows[5], RW, BH, "Hive  DWD  12 表",
    ["03 / 04 / 11 号脚本 · 含 dwd_play_start、dwd_play_end、dwd_skip"], *C["hive"])
box(RX, rows[6], RW, BH, "Hive  DWS  7 表",
    ["05 / 08 / 12 号脚本 · 歌曲·用户·曲风·省份·漏斗·搜索·收入"], *C["hive"])
box(RX, rows[7], RW, BH, "Hive  ADS  7 表",
    ["09 / 12 号脚本 · 日报·榜单·留存·漏斗·收入·曲风·省份"], *C["hive"])
box(RX, rows[8], RW, BH, "Doris  music_offline  →  Datart :8080",
    ["sync_doris.py INSERT OVERWRITE · meta_metric 55 条"], *C["bi"])

for i in range(8):
    vdown(RX + RW / 2, rows[i] + BH + 1, rows[i + 1] - 3)

# ── 中栏：共享层（与对应行水平对齐，箭头只走横段）────────
box(MX, rows[3], MW, BH, "HBase  维表",
    ["dim_user / dim_song / dim_artist / dim_album / dim_playlist"], *C["dim"], tsize=14.5, ssize=11.4)
hleft(MX, LX + LW + 4, rows[3] + BH / 2)

box(MX, rows[4], MW, BH, "meta_metric  指标字典",
    ["55 条口径定义 · 实时与离线共用同一份"], *C["key"], tsize=14.5, ssize=11.4, lw=2.2)

box(MX, rows[5], MW, BH, "Hive  dim_*  3 表",
    ["dim_date / dim_song / dim_user（离线维度快照）"], *C["dim"], tsize=14.5, ssize=11.4)
hright(MX + MW, RX - 4, rows[5] + BH / 2)

box(MX, rows[7], MW, BH, "Doris  dim_*  6 表",
    ["song 1486 · user 105,000 · playlist_song 16,440"], *C["dim"], tsize=14.5, ssize=11.4)
hleft(MX, LX + LW + 4, rows[7] + BH / 2)

# ── 底部：治理条 ──────────────────────────────────────
GY = 878
P.append(f'<rect x="50" y="{GY}" width="1580" height="74" rx="10" fill="#f8fafc" stroke="#cbd5e1" stroke-width="1.4"/>')
text(78, GY + 31, "数据治理", "#334155", 14.5, "start", "bold")
text(78, GY + 53, "异常不进主链路，单独沉淀", "#64748b", 11.5, "start")

gx = 300
for name, desc, col in [
    ("dwd_quality_error", "规则命中异常 · 实时 1.14%", "#ef4444"),
    ("dwd_unrouted", "未建明细的 15 类事件", "#f59e0b"),
    ("ops_job_health", "作业状态 · Kafka 积压", "#3b82f6"),
    ("ops_data_quality", "每分钟异常率 Top 规则", "#8b5cf6"),
    ("ops_pipeline_latency", "四层水位延迟", "#0891b2"),
]:
    P.append(f'<rect x="{gx}" y="{GY + 17}" width="5" height="40" rx="2.5" fill="{col}"/>')
    text(gx + 14, GY + 34, name, "#0f172a", 12.5, "start", "bold")
    text(gx + 14, GY + 52, desc, "#64748b", 10.8, "start")
    gx += 258

# ── 图例 ──────────────────────────────────────────────
LY = 972
text(50, LY + 13, "图例", "#64748b", 11.5, "start")
lx = 96
for name, key in [("造数 / 源", "gen"), ("消息队列", "kafka"), ("Flink 算子", "flink"),
                  ("Doris", "doris"), ("Hive", "hive"), ("Spark / HDFS", "hdfs"),
                  ("BI / 大屏", "bi"), ("关键口径", "key"), ("维度", "dim")]:
    f, s = C[key]
    P.append(f'<rect x="{lx}" y="{LY + 1}" width="15" height="15" rx="4" fill="{f}" stroke="{s}" stroke-width="1.6"/>')
    text(lx + 21, LY + 13, name, "#475569", 11.5, "start")
    lx += 21 + len(name) * 12 + 24

P.append('</svg>')

out = "docs/figures/architecture-v10.svg"
with open(out, "w", encoding="utf-8") as f:
    f.write("\n".join(P))
print("written:", out)
