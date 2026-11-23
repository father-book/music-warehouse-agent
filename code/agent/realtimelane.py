"""实时链路历史快照 —— 路由判定与元数据读取（只读，不连库）。

为什么要单独一层：
  `internal.music_dw` 里存着 Flink 链路 2026-11-08 之前产出的分钟级/10 分钟级
  结果与质检明细，但此前 Agent 碰到"实时"两个字就一律拒答
  （timeparse.status=realtime → unsupported → refuse），这批数据等于白存。

  接进来最容易犯的错，是把 3 天前的快照说成"现在"。所以分流必须是**双时态**：

    纯当前时刻（现在/此刻/今天/最近一小时）      → 仍拒答，但告诉用户能查什么
    带历史锚点或回溯词（11-08/当时/峰值/快照）  → 放行，答案强制带新鲜度声明
    只提实时域但不带当前词（实时链路质量如何）  → 按历史快照回答

  数据面隔离由护栏兜底：R17 只放行 realtime.yaml 白名单的 internal 表，
  R26 强制时间窗过滤且禁止与离线表混查。这里只决定"走哪条面"。
"""
from __future__ import annotations

import datetime as _dt
import json
import os
import re

# 纯当前时刻：链路已停，答了就是把历史数据冒充现在
_NOW_RE = re.compile(
    r"今天|今日|此刻|现在|当前|刚才|最近一小时|这几小时|近一小时|"
    r"最近\s*[0-9一二三四五六七八九十两]+\s*小时|目前")
# 历史锚点：出现这些词，说明用户问的是过去某段窗口，不是"此刻"
_ANCHOR_RE = re.compile(
    r"\d{4}-\d{2}-\d{2}|\d{1,2}\s*月\s*\d{1,2}\s*日|\b\d{1,2}-\d{1,2}\b|"
    r"当时|那天|历史|快照|峰值|最高|最大|最后一次|上次|停之前|截止")
# 实时域专属词：timeparse 没判 realtime，但问题明显属于实时链路时也能切过去
_RT_HINT_RE = re.compile(
    r"实时|Flink|flink|分钟级|10\s*分钟|checkpoint|Checkpoint|积压|"
    r"kafka_lag|链路延迟|质检异常|作业健康")

_FALLBACK = {"data_since": "2026-11-02 16:32", "data_until": "2026-11-08 22:17",
             "status": "down", "freshness_note": "实时链路历史快照（链路已停止产出）"}


def spec(cfg: dict) -> dict:
    """从 tables.json 的 realtime 段读元数据（gen_tables.py 合并 realtime.yaml）。"""
    p = ((cfg or {}).get("_paths") or {}).get("tables_json")
    if not p:
        p = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                         "..", "..", "agent", "tables.json")
    try:
        with open(str(p), encoding="utf-8") as f:
            return json.load(f).get("realtime") or {}
    except Exception:
        return {}


def table_names(cfg: dict) -> set[str]:
    return set((spec(cfg).get("tables") or {}).keys())


def freshness(cfg: dict) -> str:
    s = spec(cfg) or _FALLBACK
    if s.get("freshness_note"):
        return str(s["freshness_note"])
    return ("实时链路历史快照，窗口 %s ~ %s；链路已停止产出，不代表当前时刻"
            % (s.get("data_since"), s.get("data_until")))


def window(cfg: dict) -> tuple[str, str]:
    """快照窗口的日期边界（供 timewindow / sql_hint 用）。"""
    s = spec(cfg) or _FALLBACK
    try:
        a = _dt.date.fromisoformat(str(s.get("data_since"))[:10])
        b = _dt.date.fromisoformat(str(s.get("data_until"))[:10])
        return a.isoformat(), b.isoformat()
    except Exception:
        return "2026-11-02", "2026-11-08"


def suggest(question: str, cfg: dict, top: int = 3) -> list[str]:
    """检索没召回 rt 表时的兜底：按中文名/时间粒度关键词挑候选，进入 prompt。"""
    q = question or ""
    idx = spec(cfg).get("tables") or {}
    if not idx:
        return []
    hints = [
        (("错误率", "错误", "异常率", "埋点质量", "质检"), "ops_data_quality"),
        (("延迟", "lag", "积压", "管道", "pipeline"), "ops_pipeline_latency"),
        (("作业", "job", "checkpoint", "健康", "状态"), "ops_job_health"),
        (("客户端", "播放错误", "error_code"), "ads_client_quality_1min"),
        (("会话", "跳出", "session", "bounce"), "ads_session_1min"),
        (("歌曲榜", "10分钟榜", "rank"), "ads_song_rank_10min"),
        (("省份", "地域", "province"), "ads_province_10min"),
        (("总览", "大盘", "概览", "每日"), "ads_realtime_overview"),
        (("异常明细", "规则", "rule", "明细"), "dwd_quality_error"),
    ]
    out = []
    for words, table in hints:
        if table in idx and any(w in q for w in words):
            out.append(table)
    if not out:
        out = ["ops_data_quality", "ops_pipeline_latency"]
    return [t for t in out if t in idx][:top]


def classify(question: str, cfg: dict, time_status: str = "ok") -> dict:
    """返回 {lane, unsupported, note, window}。node_route 唯一调用点。

    time_status 是 timeparse.parse 的结果：realtime 表示问句里出现了实时词。
    """
    q = question or ""
    if not spec(cfg):
        return {"lane": "offline", "unsupported": "", "note": "", "window": None}
    now = bool(_NOW_RE.search(q))
    anchor = bool(_ANCHOR_RE.search(q))
    hint = bool(_RT_HINT_RE.search(q))

    if now and not anchor:
        note = ("这个问题问的是当前时刻，但实时链路自 %s 之后未再产出，不能拿历史快照当实时结果。"
                "可改问历史窗口内的问题，例如「2026-11-08 分钟级错误率峰值是多少」"
                "（可查窗口 %s ~ %s）。"
                % ((spec(cfg).get("data_until") or _FALLBACK["data_until"]),
                   *window(cfg)))
        return {"lane": "offline", "unsupported": note, "note": "", "window": None}

    if time_status == "realtime" or (hint and not now):
        return {"lane": "realtime", "unsupported": "", "note": freshness(cfg),
                "window": window(cfg)}
    return {"lane": "offline", "unsupported": "", "note": "", "window": None}
