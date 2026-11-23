"""实时大屏后端：Flask 只提供查询接口。

数据全部由 Flink 的四个作业实时写入 Doris（作业 A/B/C/D 分别负责 ODS/DWD/DWS/ADS），
本服务只读不写，不承担任何计算、聚合或调度职责——没有后台线程，没有定时任务。

启动：
    python dashboard/app.py                  # 默认 5000 端口
    python dashboard/app.py --port 8080

访问：http://127.0.0.1:5000
"""
import argparse
import os
import sys
import time
from datetime import datetime

import pymysql
from flask import Flask, jsonify, render_template

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(BASE_DIR, "code"))

DB = dict(host="192.168.128.163", port=9030, user="root", password="",
          database="music_dw", charset="utf8mb4")

app = Flask(__name__, template_folder="templates", static_folder="static")
app.json.ensure_ascii = False
# 模板改动即时生效：默认只有 debug 模式才 auto_reload，改 index.html 必须重启服务才看得到，
# 调布局时特别费劲（改一版重启一次）。这一行只影响开发期，生产多一次 stat 调用可忽略。
app.config["TEMPLATES_AUTO_RELOAD"] = True

# 作业 B 的分流出口清单 —— 「今日处理量」按这些表求和，见 _health_payload() 里的说明。
# 加表/改表时这里必须同步，否则顶栏的「今日处理」会偏小。
DWD_ENTRY_TABLES = (
    "dwd_play_start", "dwd_play_end", "dwd_play_error", "dwd_skip",
    "dwd_collect", "dwd_search", "dwd_share", "dwd_order", "dwd_page_view",
    "dwd_quality_error", "dwd_unrouted",
)

# 「今日处理量」的缓存：COUNT(*) 在 Doris 上是列存扫描，百万级也很快，
# 但大屏每 5 秒刷一次，没必要重复扫同一张日增长的表。60 秒缓存够用。
_VOL_CACHE = {"ts": 0.0, "n": None}
# 「新老用户」要跟昨天比，得扫昨日分区的设备集合，比较重；结果 300 秒内有效
#（从 120 秒拉长 —— dim_user 涨到 10 万后这条 join 的成本涨了 30 倍，
#  新老用户又是跨天概念，几分钟不更新没有信息损失）。
_USER_CACHE = {"ts": 0.0, "dev": None, "new": None, "old": None, "guest": None}
# 内容与互动四联（推荐 / 互动 / 搜索 / 交易）：四张明细各扫一次，
# 其中 dwd_unrouted 今天 14 万行还要按 event_name 过滤，10 秒轮询没必要每轮都扫。
_CONTENT_CACHE = {"ts": 0.0, "v": None}


def query(sql, args=None):
    conn = pymysql.connect(**DB)
    try:
        cur = conn.cursor(pymysql.cursors.DictCursor)
        cur.execute(sql, args)
        return cur.fetchall()
    finally:
        conn.close()


# ── ADS 窗口新鲜度 ──────────────────────────────────────────────────────────
# 作业 D 用的是 10 分钟 HOP 窗口，窗口 [w, w+10min) 要等 watermark >= w+10min
# 才会 emit —— 也就是说「灌完数想看到热榜/地图」得等 10~12 分钟。
# 这跟「数据一产生就要能看见」直接冲突，而作业 D 的窗口又不能为了这个调小
# （10 分钟是热门榜的业务口径）。折中：ADS 窗口超过 STALE_MIN 没更新就回退
# 到明细现算 —— 明细是逐条映射（job_b 不开窗），秒级可查，兜底代价可忽略。
ADS_STALE_MIN = 12


def _fresh_enough(w, minutes=ADS_STALE_MIN):
    """ADS 最新窗口是否还在 minutes 分钟内。

    w 来自 UNIX_TIMESTAMP()，Doris 返回 DECIMAL（pymysql 给 Decimal），
    也可能因为版本差异给字符串 / datetime，这里都兜住。
    """
    if not w:
        return False
    try:
        ts = float(w)                       # Decimal / int / 数字字符串
    except (TypeError, ValueError):
        try:
            ts = w.timestamp()              # datetime
        except AttributeError:
            try:
                ts = datetime.strptime(w.replace("T", " ")[:19],
                                       "%Y-%m-%d %H:%M:%S").timestamp()
            except ValueError:
                return False
    return time.time() - ts <= minutes * 60


@app.route("/")
def index():
    return render_template("index.html")


def _new_old_users():
    """新用户 / 老用户 / 游客：以**维表** dim_user.register_time 为准，且必须按**设备**计数。

    为什么必须按设备（这条踩过坑）：
      大屏这个面板的分母是「近 10 分钟 X 台设备」。如果新老按 user_id 去重算，
      得到的是「登录用户」的切分，加起来只有 1138，与 1342 台设备差 204，
      读者一算就不平 —— 而且 new + old 恰好等于面板上的「登录用户」，等于同一份信息显示两遍。
      改成按设备切分后：新 + 老 + 游客 = 总设备数，恒等式成立，指标才自洽。

    判定规则：
      · 老设备 = 设备对应的 user_id 在维表里且 register_time 早于今天
      · 新设备 = 注册时间在今天（今日新增注册）
      · 游客   = 该设备窗口内压根没有 user_id（未登录试听，占比不低，必须单列而不是塞进新用户）

    一个设备可能对应多个 user_id（换账号登录），用 MAX 取其中一个即可 ——
    新老判定只关心「是不是今天注册的」，取哪个不影响结论。
    结果缓存 300 秒：这条要 join 维表，10 万行维表 + 10 秒轮询，5 分钟刷新一次足够
    （新老用户是「跨天」概念，几分钟内几乎不变，缓存拉长没有信息损失）。
    """
    global _USER_CACHE
    # 60 秒而不是 300 秒：这个面板的分母（近 10 分钟设备数）一直在滑，
    # 缓存太久会出现「设备数还是 5 分钟前的、拆分是刚刚的」的错位。
    if time.time() - _USER_CACHE["ts"] > 60:
        rows = query("""
            WITH win AS (
                SELECT device_id, MAX(user_id) AS uid
                FROM dwd_play_end
                WHERE dt = CURDATE() AND event_ts >= NOW() - INTERVAL 10 MINUTE
                GROUP BY device_id
            )
            SELECT COUNT(*)                                                          AS dev_total,
                   SUM(CASE WHEN COALESCE(w.uid,'') = '' THEN 1 ELSE 0 END)          AS guest_dev,
                   SUM(CASE WHEN u.register_time >= CURDATE() THEN 1 ELSE 0 END)     AS new_dev,
                   SUM(CASE WHEN u.register_time <  CURDATE() THEN 1 ELSE 0 END)     AS old_dev
            FROM win w
            LEFT JOIN dim_user u ON w.uid = u.user_id
        """)
        r = rows[0] if rows else {}
        # dev 一起返回：面板上「新+老+游客」必须恒等于分母。
        # 之前分母是另一次查询（dev_cnt），与这里窗口边界不同 ——
        # 实测出现过 143 台设备却只拆出 44 台（7+0+37），
        # 差在两次查询跑在窗口滑动的两侧。让拆分自带分母就不会对不上。
        _USER_CACHE = {"ts": time.time(),
                       "dev": r.get("dev_total"),
                       "new": r.get("new_dev"),
                       "old": r.get("old_dev"),
                       "guest": r.get("guest_dev")}
    return _USER_CACHE


def _content_payload():
    """内容与互动四联：推荐 CTR / 互动率 / 搜索转化 / 今日成单。

    为什么这里敢直接查明细、不像播放侧那样读 DWS/ADS：
      这四类事件在作业 B 里各只落一张**明细**表（collect / share / search / order），
      没有任何窗口表覆盖它们 —— 实时层至今没为它们建 DWS。
      口径又简单到「数行数 + 两个数相除」，现算比再多开一条 Flink SQL 作业便宜得多。
      真要做趋势（按分钟），才值得补一张 DWS。

    四个口径里有两个坑，写在这里免得下次又踩：

    1) **搜索点击率不能用 dwd_search.clicked_song_id 算**。
       造数器从来没填过这个字段，今天 1,807 条搜索里 0 条有点击 —— 用它算恒等于 0%。
       真实点击在 `dwd_unrouted` 里，event_name = 'search_click'。
       所以这里的分母是 dwd_search 的搜索次数，分子是 unrouted 里的 search_click 条数。
       两套事件天生不同源，只能当**宏观转化率**看，不能当漏斗做归因。

    2) **点赞率的分母是「点击」不是「起播」**。
       点赞发生在推荐流的卡片上（impression → click → like），
       用起播做分母会算出 24% 这种没意义的数；用点击做分母是 23.9%，
       才是「点进来的人有多少会点赞」。

    曝光/点击/点赞来自 dwd_unrouted（26 类埋点里这 3 类没建明细表，见 docs/21 §10），
    这也是为什么这个函数要 60 秒缓存 —— unrouted 是当日最大的表。
    """
    global _CONTENT_CACHE
    if time.time() - _CONTENT_CACHE["ts"] <= 60 and _CONTENT_CACHE["v"] is not None:
        return _CONTENT_CACHE["v"]

    def one(sql):
        rows = query(sql)
        return rows[0] if rows else {}

    def f(v, nd=2):
        return None if v is None else round(float(v), nd)

    def rate(a, b, nd=2):
        # 分母为 0 时返回 None（前端显示 —），绝不能返回 0 冒充「转化率为零」
        return None if not b else round(float(a) * 100.0 / float(b), nd)

    # 推荐流：曝光 / 点击 / 点赞 / 搜索点击（都只在 unrouted 里）
    rec = one("""
        SELECT SUM(CASE WHEN event_name = 'impression'   THEN 1 ELSE 0 END) AS imp,
               SUM(CASE WHEN event_name = 'click'        THEN 1 ELSE 0 END) AS clk,
               SUM(CASE WHEN event_name = 'like'         THEN 1 ELSE 0 END) AS lk,
               SUM(CASE WHEN event_name = 'search_click' THEN 1 ELSE 0 END) AS sclk
        FROM dwd_unrouted
        WHERE dt = CURDATE()
          AND event_name IN ('impression', 'click', 'like', 'search_click')
    """)

    collect = one("SELECT COUNT(*) AS n FROM dwd_collect WHERE dt = CURDATE() AND collect_type = 'add'")
    cancel = one("SELECT COUNT(*) AS n FROM dwd_collect WHERE dt = CURDATE() AND collect_type = 'cancel'")
    share = one("SELECT COUNT(*) AS n FROM dwd_share WHERE dt = CURDATE()")
    start = one("SELECT COUNT(*) AS n FROM dwd_play_start WHERE dt = CURDATE()")
    srch = one("""
        SELECT COUNT(*) AS cnt, AVG(result_count) AS avg_results
        FROM dwd_search WHERE dt = CURDATE()
    """)
    od = one("""
        SELECT SUM(CASE WHEN event_name IN ('order_pay','vip_subscribe') THEN 1 ELSE 0 END)        AS pay_cnt,
               SUM(CASE WHEN event_name = 'vip_subscribe' THEN 1 ELSE 0 END)                       AS sub_cnt,
               SUM(CASE WHEN event_name = 'order_refund'  THEN 1 ELSE 0 END)                       AS refund_cnt,
               SUM(CASE WHEN event_name IN ('order_pay','vip_subscribe') THEN amount ELSE 0 END)    AS gmv,
               SUM(CASE WHEN event_name = 'order_refund'  THEN amount ELSE 0 END)                   AS refund_amt
        FROM dwd_order WHERE dt = CURDATE()
    """)
    # 实时成单：最近 10 分钟。停灌时会归零，前端要按「无近期成交」显示，不能显示成 0 元收入
    rt = one("""
        SELECT SUM(CASE WHEN event_name IN ('order_pay','vip_subscribe') THEN 1 ELSE 0 END)     AS cnt,
               SUM(CASE WHEN event_name IN ('order_pay','vip_subscribe') THEN amount ELSE 0 END) AS amt
        FROM dwd_order
        WHERE dt = CURDATE() AND event_ts >= NOW() - INTERVAL 10 MINUTE
    """)

    start_n = start.get("n") or 0
    imp, clk, lk = rec.get("imp") or 0, rec.get("clk") or 0, rec.get("lk") or 0

    payload = {
        "recommend": {
            "imp": imp, "clk": clk, "like": lk,
            "ctr": rate(clk, imp),                # 曝光 → 点击
        },
        "social": {
            "start": start_n,
            "collect": collect.get("n") or 0, "cancel": cancel.get("n") or 0,
            "share": share.get("n") or 0, "like": lk,
            "collect_rate": rate(collect.get("n") or 0, start_n, 1),
            "share_rate": rate(share.get("n") or 0, start_n, 1),
            "like_rate": rate(lk, clk, 1),
        },
        "search": {
            "cnt": srch.get("cnt") or 0,
            "click_cnt": rec.get("sclk") or 0,
            "click_rate": rate(rec.get("sclk") or 0, srch.get("cnt") or 0, 1),
            "avg_results": f(srch.get("avg_results"), 1),
        },
        "order": {
            "pay_cnt": od.get("pay_cnt") or 0,
            "sub_cnt": od.get("sub_cnt") or 0,
            "refund_cnt": od.get("refund_cnt") or 0,
            "gmv": f(od.get("gmv")),
            "refund_amt": f(od.get("refund_amt")),
            "net": round(float(od.get("gmv") or 0) - float(od.get("refund_amt") or 0), 2),
            "rt_cnt": rt.get("cnt") or 0,
            "rt_gmv": f(rt.get("amt")),
        },
    }
    _CONTENT_CACHE = {"ts": time.time(), "v": payload}
    return payload


@app.route("/api/all")
def api_all():
    """一次返回大屏所需的全部数据，减少请求数。"""
    overview = query("""
        SELECT DATE_FORMAT(stat_date, '%Y-%m-%d') AS stat_date,
               total_play, total_uv, login_uv,
               finish_rate, skip_rate, avg_play_duration,
               active_minutes,
               DATE_FORMAT(last_event_ts, '%H:%i:%s') AS last_event,
               DATE_FORMAT(updated_at, '%H:%i:%s')    AS data_time
        FROM ads_realtime_overview
        ORDER BY stat_date DESC LIMIT 1
    """)

    # 实时在线：最近 5 分钟有播放行为的去重设备数（回明细算，不能用 DWS 相加）
    active = query("""
        SELECT COUNT(DISTINCT device_id) AS online_uv,
               COUNT(DISTINCT CASE WHEN user_id <> '' THEN user_id END) AS online_login
        FROM dwd_play_end
        WHERE event_ts >= NOW() - INTERVAL 5 MINUTE
    """)

    # 趋势：按「最近 30 分钟」过滤，前端用时间轴按真实间隔绘制
    trend = query("""
        SELECT UNIX_TIMESTAMP(window_start) * 1000 AS ts,
               DATE_FORMAT(window_start, '%H:%i') AS t,
               play_cnt, uv, finish_rate, skip_rate
        FROM dws_play_overview_1min
        WHERE window_start >= NOW() - INTERVAL 30 MINUTE
        ORDER BY window_start
    """)
    # Doris 的 UNIX_TIMESTAMP 返回 DECIMAL，jsonify 会序列化成字符串，
    # 时间轴要数字，这里强制转 int
    for r in trend:
        r["ts"] = int(r["ts"])

    # 近 1 小时流量分布：把 1 分钟窗口聚合到 5 分钟一档（最多 12 根柱子）。
    # 用「近 60 分钟」滑动窗口而非「今日」，避免上午的旧数据把横轴拉散。
    hourly = query("""
        SELECT DATE_FORMAT(t.bucket, '%H:%i') AS h,
               t.bucket AS ts,
               SUM(t.play_cnt) AS play_cnt
        FROM (
            SELECT FROM_UNIXTIME(FLOOR(UNIX_TIMESTAMP(window_start) / 300) * 300) AS bucket,
                   play_cnt
            FROM dws_play_overview_1min
            WHERE window_start >= NOW() - INTERVAL 60 MINUTE
        ) t
        GROUP BY t.bucket
        ORDER BY t.bucket
    """)
    for r in hourly:
        r["ts"] = int(r["ts"].timestamp() * 1000) if hasattr(r["ts"], "timestamp") else r["ts"]

    # 热榜：正常路径直接读 ADS 成品牌表（作业 D 的 10 分钟滚动窗口已算好 Top20），
    # 大屏不查明细、不做窗口聚合 —— 这正是 ADS 层存在的意义。
    # 只有 ADS 还没有任何窗口时（作业 D 刚启动、首个窗口未闭合）才回退到明细现算，
    # 保证任何时刻榜单都不空。
    rank = query("""
        SELECT r.rank_no,
               r.song_id,
               COALESCE(s.song_name, r.song_id) AS song_name,
               COALESCE(a.artist_name, '')      AS artist_name,
               r.play_cnt,
               r.uv,
               r.skip_rate,
               UNIX_TIMESTAMP(r.window_start) AS win_ts,
               DATE_FORMAT(r.window_start, '%H:%i') AS win_start,
               DATE_FORMAT(DATE_ADD(r.window_start, INTERVAL 10 MINUTE), '%H:%i') AS win_end
        FROM ads_song_rank_10min r
        LEFT JOIN dim_song   s ON r.song_id   = s.song_id
        LEFT JOIN dim_artist a ON s.artist_id = a.artist_id
        WHERE r.window_start = (SELECT MAX(window_start) FROM ads_song_rank_10min)
        ORDER BY r.rank_no
    """)
    rank_window = None
    if rank and rank[0].get("win_start"):
        rank_window = "%s–%s" % (rank[0]["win_start"], rank[0]["win_end"])

    _rank_fresh = bool(rank) and _fresh_enough(rank[0].get("win_ts"))
    for _r in rank:
        _r.pop("win_ts", None)
    if not _rank_fresh:
        # 两种情况都回明细现算：① ADS 还没有任何窗口（作业 D 刚启动）；
        # ② 有窗口但已经超过 12 分钟没更新（HOP 还没闭合新一轮）—— 后者才是常态：
        #    作业 D 的 10 分钟窗口天然滞后 10~12 分钟，灌完数不等它就是空的。
        # 回退时把窗口文案改成「近 10 分钟（实时）」，避免和大屏时间轴对不上。
        rank = query("""
            SELECT d.song_id,
                   COALESCE(MAX(NULLIF(d.song_name, '')), MAX(s.song_name), d.song_id) AS song_name,
                   COALESCE(MAX(NULLIF(d.artist_name, '')), MAX(a.artist_name), '')    AS artist_name,
                   COUNT(DISTINCT d.event_id)  AS play_cnt,
                   COUNT(DISTINCT d.device_id) AS uv,
                   ROUND(COUNT(DISTINCT CASE WHEN d.end_reason = 'skip' THEN d.event_id END)
                         * 100.0 / NULLIF(COUNT(DISTINCT d.event_id), 0), 2) AS skip_rate
            FROM dwd_play_end d
            LEFT JOIN dim_song   s ON d.song_id = s.song_id
            LEFT JOIN dim_artist a ON s.artist_id = a.artist_id
            WHERE d.event_ts >= NOW() - INTERVAL 10 MINUTE
            GROUP BY d.song_id
            ORDER BY play_cnt DESC
            LIMIT 20
        """)
        # 明细现算没有 rank_no（ADS 才存名次），补上让前端按同一套渲染
        for _i, _r in enumerate(rank, 1):
            _r["rank_no"] = _i
        if rank:
            rank_window = "近 10 分钟（实时）"

    province = query("""
        SELECT province, play_cnt, uv, play_share,
               UNIX_TIMESTAMP(window_start) AS win_ts,
               DATE_FORMAT(window_start, '%H:%i') AS win_start,
               DATE_FORMAT(DATE_ADD(window_start, INTERVAL 10 MINUTE), '%H:%i') AS win_end
        FROM ads_province_10min
        WHERE window_start = (SELECT MAX(window_start) FROM ads_province_10min)
        ORDER BY play_cnt DESC
    """)
    _prov_fresh = bool(province) and _fresh_enough(province[0].get("win_ts"))
    for _r in province:
        _r.pop("win_ts", None)
    if not _prov_fresh:
        # 同热榜：作业 D 的 10 分钟窗口没闭合就回明细现算，地图不空着等
        province = query("""
            SELECT COALESCE(NULLIF(d.province, ''), '未知') AS province,
                   COUNT(DISTINCT d.event_id)  AS play_cnt,
                   COUNT(DISTINCT d.device_id) AS uv
            FROM dwd_play_end d
            WHERE d.event_ts >= NOW() - INTERVAL 10 MINUTE
            GROUP BY COALESCE(NULLIF(d.province, ''), '未知')
            ORDER BY play_cnt DESC
        """)
        _tot = sum(r["play_cnt"] or 0 for r in province) or 1
        for _r in province:
            _r["play_share"] = round((_r["play_cnt"] or 0) * 100.0 / _tot, 2)
            _r["win_start"] = "近10分钟"
            _r["win_end"] = "实时"

    province_window = None
    if province and province[0].get("win_start"):
        province_window = "%s–%s" % (province[0]["win_start"], province[0]["win_end"])

    def _genre_mix(minutes):
        """近 N 分钟的曲风播放分布。

        为什么用 dws_song_play_5min 而不是 ads_song_rank_10min：
          后者只有 Top20，曲风分布会被头部几首歌完全带偏（60 首 R&B 一度算出 13% 播放）。
          要回答「全站此刻在放什么类型的歌」，必须用**全量歌曲**的窗口汇总。

        为什么要在 Doris 侧 join dim_song 而不是查维表：
          genre 只有 4 类，聚合后返回 4 行，join 成本可以忽略；
          放前端 join 反而要传上千条歌曲明细。
        """
        return query(f"""
            SELECT COALESCE(NULLIF(s.genre, ''), '未知') AS name,
                   SUM(w.play_cnt)                         AS cnt
            FROM dws_song_play_5min w
            LEFT JOIN dim_song s ON w.song_id = s.song_id
            WHERE w.window_start >= NOW() - INTERVAL {int(minutes)} MINUTE
            GROUP BY 1
            ORDER BY cnt DESC
        """)

    # 曲风用近 10 分钟（两个 5 分钟窗口），单个窗口样本太小、比例会跳。
    # 停灌时近 10 分钟为空 → 回退到「最近一个完整窗口」并如实标注，不让面板空着。
    genres_span = 10
    genres = _genre_mix(10)
    if not genres:
        genres_span = 5
        genres = query("""
            SELECT COALESCE(NULLIF(s.genre, ''), '未知') AS name,
                   SUM(w.play_cnt)                         AS cnt
            FROM dws_song_play_5min w
            LEFT JOIN dim_song s ON w.song_id = s.song_id
            WHERE w.window_start = (SELECT MAX(window_start) FROM dws_song_play_5min)
            GROUP BY 1
            ORDER BY cnt DESC
        """)

    # 热门歌手 Top10：把最新 10 分钟窗口的歌曲榜单按歌手合并。
    # 单曲播放量直接相加即可（同一歌手多首歌同时进 Top20 时自然合并）；
    # song_cnt 用 COUNT(DISTINCT song_id) 给出「上榜歌曲数」，比只显示播放量更有信息量。
    artists = query("""
        SELECT COALESCE(a.artist_name, '未知歌手') AS artist_name,
               SUM(r.play_cnt)                   AS play_cnt,
               COUNT(DISTINCT r.song_id)         AS song_cnt
        FROM ads_song_rank_10min r
        LEFT JOIN dim_song   s ON r.song_id   = s.song_id
        LEFT JOIN dim_artist a ON s.artist_id = a.artist_id
        WHERE r.window_start = (SELECT MAX(window_start) FROM ads_song_rank_10min)
        GROUP BY COALESCE(a.artist_name, '未知歌手')
        ORDER BY play_cnt DESC
        LIMIT 10
    """)

    # ---- 用户结构 ----
    # 窗口用「近 10 分钟」而不是「今日」：
    #   1) 大屏是实时视图，今日累计已经有 KPI 卡在展示；
    #   2) 用今日算「新设备」会得出 100% 的假象 —— 今天产生的数据里，
    #      所有设备今天都是第一次出现，这个指标在当日窗口下没有区分度。
    # 登录率/VIP 率按**设备**算而不是用户：未登录用户没有 user_id，
    # 用用户做分母会把未登录流量整个丢掉，登录率被高估。
    users = query("""
        SELECT COUNT(DISTINCT device_id)                                            AS dev_cnt,
               COUNT(DISTINCT CASE WHEN is_login = 'true' THEN device_id END)       AS login_dev,
               COUNT(DISTINCT CASE WHEN is_vip  = 'true' THEN device_id END)        AS vip_dev,
               COUNT(DISTINCT CASE WHEN COALESCE(user_id,'') <> '' THEN user_id END) AS user_cnt
        FROM dwd_play_end
        WHERE dt = CURDATE() AND event_ts >= NOW() - INTERVAL 10 MINUTE
    """)
    u0 = users[0] if users else {}
    dev = u0.get("dev_cnt") or 0

    os_dist = query("""
        SELECT COALESCE(NULLIF(os, ''), '未知') AS name, COUNT(DISTINCT device_id) AS cnt
        FROM dwd_play_end
        WHERE dt = CURDATE() AND event_ts >= NOW() - INTERVAL 10 MINUTE
        GROUP BY 1 ORDER BY cnt DESC LIMIT 4
    """)
    # 新老用户按**设备**切分，与上面的分母（X 台设备）同源。
    # 三者相加恒等于 dev_cnt —— 这是面板能被一眼算平的前提。
    split = _new_old_users() or {}
    users_struct = {
        # 分母优先用拆分自带的 dev_total，保证「新+老+游客 == dev_cnt」；
        # 拿不到时才退回单独查的 dev（会有一点点窗口错位，但不会长期错）
        "dev_cnt": split.get("dev") or dev,
        "user_cnt": u0.get("user_cnt") or 0,
        "login_rate": round((u0.get("login_dev") or 0) * 100.0 / dev, 2) if dev else None,
        "vip_rate": round((u0.get("vip_dev") or 0) * 100.0 / dev, 2) if dev else None,
        "os": os_dist,
        "new_user": split.get("new"),
        "old_user": split.get("old"),
        "guest_user": split.get("guest"),
    }

    # ---- 商业化 ----
    # 只读 ADS，不在这里现算：收入类指标必须与订单明细同一套口径，
    # 大屏里现算迟早会与 ads_commercial_daily 出现「两个版本」。
    biz_rows = query("""
        SELECT stat_date, pay_order_cnt, pay_user_cnt, active_user_cnt, pay_rate,
               revenue, refund_amt, net_revenue, refund_cnt,
               subscribe_cnt, single_cnt, arppu,
               DATE_FORMAT(updated_at, '%H:%i:%s') AS updated_at
        FROM ads_commercial_daily ORDER BY stat_date DESC LIMIT 1
    """)
    sku = query("""
        SELECT sku_id, order_cnt, revenue FROM ads_sku_daily
        WHERE stat_date = (SELECT MAX(stat_date) FROM ads_sku_daily)
        ORDER BY revenue DESC
    """)
    biz_payload = dict(biz_rows[0]) if biz_rows else {}
    if biz_payload.get("stat_date"):
        biz_payload["stat_date"] = str(biz_payload["stat_date"])
    biz_payload["skus"] = sku

    # ---- 客户端质量（分钟级）----
    cq = query("""
        SELECT play_cnt, error_cnt, error_rate, top_error_code, top_error_cnt,
               DATE_FORMAT(stat_minute, '%H:%i') AS stat_hm
        FROM ads_client_quality_1min ORDER BY stat_minute DESC LIMIT 1
    """)

    # 会话指标：近 10 分钟滚动（由采集器微批刷新）
    sess = query("""
        SELECT session_cnt, avg_evts_per_sess, bounce_sess_cnt, bounce_rate,
               DATE_FORMAT(stat_minute, '%H:%i') AS stat_hm
        FROM ads_session_1min ORDER BY stat_minute DESC LIMIT 1
    """)

    return jsonify({
        "ts": datetime.now().strftime("%H:%M:%S"),
        "overview": overview[0] if overview else None,
        "active": active[0] if active else {"online_uv": 0, "online_login": 0},
        "trend": trend,
        "hourly": hourly,
        "genres": genres,
        "genres_span": genres_span,
        "artists": artists,
        "users": users_struct,
        "biz": biz_payload,
        "client_quality": cq[0] if cq else {},
        "session": sess[0] if sess else {},
        "rank": rank,
        "rank_window": rank_window,
        "province": province,
        "province_window": province_window,
        # 内容与互动四联：推荐 CTR / 互动率 / 搜索转化 / 今日成单
        "content": _content_payload(),
        # 数据新鲜度由 ADS 表的 updated_at 给出（Flink 写这条记录的时刻），
        # 而不是"本服务上次重算的时间"——现在没有任何重算了。
        "last_refresh": (overview[0].get("data_time") if overview else None),
        # 链路健康跟着一起返回：大屏 5 秒刷一次，没必要为几个指标多发一个请求
        "health": _health_payload(),
    })


def _health_payload():
    """链路健康的计算逻辑。api_all 内嵌一份，/api/health 单独暴露一份。

    分两层：
      1. 存活：Doris 连得上吗
      2. 业务健康：四个作业是否 RUNNING、各层延迟多少、异常率多少
    第 2 层的判据来自 ops_ 三张表，由 code/collect_ops_metrics.py 每分钟写入。
    如果采集器没在跑，age_s 会变大，据此把「健康」降级为「未知」——
    不能让「没有数据」被误读成「一切正常」。
    """
    # 注意这里的别名：绝不能写成 `AS stat_minute`。
    # SQL 里一旦把输出列别名成与源列同名的 stat_minute，`ORDER BY stat_minute`
    # 会解析到「字符串别名」而不是 DATETIME 源列，于是按字典序排序 —— '20:34' > '12:29'，
    # 永远取到昨天那一行。这正是「采集器未运行」误报的根因（age_s 算成 5 万多秒）。
    lag = query("""
        SELECT DATE_FORMAT(stat_minute, '%H:%i') AS stat_hm,
               ods_lag_s, dwd_lag_s, dws_lag_s, ads_lag_s,
               input_rate, dwd_rows_1m, kafka_lag_rows,
               TIMESTAMPDIFF(SECOND, stat_minute, NOW()) AS age_s
        FROM ops_pipeline_latency
        ORDER BY stat_minute DESC LIMIT 1
    """)
    jobs = query("""
        SELECT job_name, state, run_seconds, ckpt_size_mb, ckpt_age_s, ckpt_failed
        FROM ops_job_health
        WHERE stat_minute = (SELECT MAX(stat_minute) FROM ops_job_health)
        ORDER BY job_name
    """)
    qual = query("""
        SELECT events_1m, errors_1m, error_rate, top_rule, top_rule_cnt
        FROM ops_data_quality
        ORDER BY stat_minute DESC LIMIT 1
    """)
    # 上游重报率：埋点客户端重试会产生 event_id 相同的重复上报（生成器按 0.6% 注入）。
    # 这是业务事实，不是链路故障；聚合层用 COUNT(DISTINCT event_id) 消除，
    # 所以把它当成一个可观测指标展示，而不是当成错误藏起来。
    dup = query("""
        SELECT COUNT(*) AS rows_5m, COUNT(DISTINCT event_id) AS uniq_5m
        FROM dwd_play_end WHERE etl_time >= NOW() - INTERVAL 5 MINUTE
    """)
    dup_row = dup[0] if dup else {}
    dup_rate = None
    if dup_row.get("rows_5m"):
        dup_rate = round((dup_row["rows_5m"] - dup_row["uniq_5m"]) * 100.0
                         / dup_row["rows_5m"], 2)

    # 今日累计处理量：入口事件总数，给大屏一个「今天处理了多少」的体量感。
    #
    # v10 之后 Doris 里已经没有 ODS 表（实时 ODS 就是 Kafka topic，见 docs/21），
    # 而 Kafka 位点只能读「全量累计」，切不出「今日」。所以口径换成 DWD 入口恒等式：
    #     入口 = 10 张明细表 + dwd_quality_error + dwd_unrouted
    # 作业 B 对每条事件只做一次判定、且必定落入其中之一（要么明细、要么异常、要么兜底），
    # 所以这个和 == 今日进入链路的事件数，与 v9 数 ODS 行数是同一个数。
    # 注意：不能只加「有数的那几张」—— 少加一张就漏一类事件，恒等式才是口径。
    # 缓存 300 秒：11 个 COUNT(*) 各自扫一次当日分区，10 秒轮询没必要重复扫。
    global _VOL_CACHE
    if time.time() - _VOL_CACHE["ts"] > 300:
        total = 0
        ok = True
        for t in DWD_ENTRY_TABLES:
            try:
                r = query(f"SELECT COUNT(*) AS n FROM {t} WHERE dt = CURDATE()")
                total += (r[0]["n"] if r else 0)
            except Exception:
                # 某张表还没建（比如刚扩表还没提交作业）不该让整块顶栏崩掉，
                # 但也不能静默算出偏小的数——置 None，前端显示 —。
                ok = False
                break
        _VOL_CACHE = {"ts": time.time(), "n": total if ok else None}
    processed_today = _VOL_CACHE["n"]

    row = lag[0] if lag else {}
    age = row.get("age_s")
    # 采集器 3 分钟没写过数据，说明它自己停了，此时所有指标都不能信
    stale = age is None or age > 180
    states = {j["job_name"]: j["state"] for j in jobs}
    all_running = bool(states) and all(s == "RUNNING" for s in states.values())

    # 空闲判定：最近一分钟没有任何事件进仓。
    # 这时 lag = now - max(etl_time) 会随时间单调增长，但它衡量的是「多久没新数据」，
    # 不是「处理得慢」。没有输入就没有延迟可言，必须单独识别，否则每次停灌都会误报「延迟偏高」。
    q0 = qual[0] if qual else {}
    idle = (q0.get("events_1m") == 0) and not (row.get("input_rate") or 0)

    if stale:
        state, level = "采集器未运行", "unknown"
    elif not all_running:
        state, level = "作业异常", "bad"
    elif idle:
        state, level = "链路就绪（空闲）", "ok"
    elif (row.get("kafka_lag_rows") or 0) > 50000:
        # 真正的积压信号：dwd_lag 只能说明「最新一行多旧」，B 一边写旧数据一边刷新它，
        # 积压 6 万行时 dwd_lag 仍显示 11~15s。consumer lag 才是待消费行数。
        state, level = "处理积压", "warn"
    elif (row.get("dwd_lag_s") or 0) > 60:
        state, level = "数据延迟偏高", "warn"
    else:
        state, level = "链路运行中", "ok"

    return {
        "ok": level == "ok",
        "level": level,
        "state": state,
        "idle": idle,
        "collector_age_s": age,
        "stat_minute": row.get("stat_hm"),
        "processed_today": processed_today,
        "lag": {"ods": row.get("ods_lag_s"), "dwd": row.get("dwd_lag_s"),
                "dws": row.get("dws_lag_s"), "ads": row.get("ads_lag_s")},
        "input_rate": row.get("input_rate"),
        "kafka_lag": row.get("kafka_lag_rows"),
        "jobs": states,
        "job_detail": jobs,
        "quality": qual[0] if qual else {},
        "dup_rate": dup_rate,
    }


@app.route("/api/health")
def api_health():
    try:
        query("SELECT 1")
    except Exception as e:
        return jsonify({"ok": False, "doris": "down", "error": str(e)}), 503
    return jsonify(_health_payload())


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=5000)
    ap.add_argument("--host", default="127.0.0.1",
                    help="监听地址。默认只绑本机；要局域网访问用 0.0.0.0")
    args = ap.parse_args()

    print(f"大屏地址：http://127.0.0.1:{args.port}")
    print("数据由 Flink 作业 A/B/C/D 实时写入，本服务只读")
    app.run(host=args.host, port=args.port, debug=False, threaded=True)


if __name__ == "__main__":
    main()
