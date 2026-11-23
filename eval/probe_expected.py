"""覆盖矩阵补题的期望值探针（一次性，但保留以便随时重核）。

原则：**期望值只能来自独立 SQL 实测**，不能来自 Agent 自己跑出来的结果。
每条 SQL 都按 metrics.yaml 的口径手写，跟 Agent 用什么表、怎么写无关。
"""
import sys

sys.path.insert(0, ".")
sys.path.insert(0, "..")

import _bootstrap  # noqa: F401
import db  # noqa: E402
from conf import load_config  # noqa: E402

cfg = load_config()

Q = {
    # ---------------- 对比 ----------------
    "C1 互动差值 11-08 vs 11-07": """
        SELECT (SELECT COUNT(*) FROM dwd_interaction WHERE dt='2026-11-08'
                  AND event_name IN ('collect','like','share','comment','download','follow_artist','playlist_add'))
             - (SELECT COUNT(*) FROM dwd_interaction WHERE dt='2026-11-07'
                  AND event_name IN ('collect','like','share','comment','download','follow_artist','playlist_add')) AS diff""",
    "C2 收入增长 11-08 vs 11-07": """
        SELECT (SELECT SUM(CASE WHEN event_name IN ('vip_subscribe','order_pay') THEN amount
                                WHEN event_name='order_refund' THEN -amount ELSE 0 END)
                  FROM dwd_pay WHERE dt='2026-11-08')
             - (SELECT SUM(CASE WHEN event_name IN ('vip_subscribe','order_pay') THEN amount
                                WHEN event_name='order_refund' THEN -amount ELSE 0 END)
                  FROM dwd_pay WHERE dt='2026-11-07') AS diff""",
    "C3 完播 vs 播放结束差": """
        SELECT (SELECT COUNT(*) FROM dwd_play_end WHERE dt='2026-11-08' AND event_name='play_complete')
             - (SELECT COUNT(*) FROM dwd_play_end WHERE dt='2026-11-08' AND event_name='play_end') AS diff""",
    # ---------------- 趋势 ----------------
    # 「最近 7 天」= 含今天(11-09) 往前 6 天 = 11-03~11-09；裁剪到数据末尾后是
    # 11-03~11-08 共 6 个分区。第一版按 11-02~11-08 数成 7，是探针口径错了。
    "T1 最近7天DAU 行数": """
        SELECT COUNT(*) FROM (SELECT dt FROM dws_user_daily
          WHERE dt BETWEEN '2026-11-03' AND '2026-11-08' GROUP BY dt) t""",
    "T1b 最近7天DAU 明细": """
        SELECT dt, dau FROM dws_user_daily WHERE dt BETWEEN '2026-11-03' AND '2026-11-08' ORDER BY dt""",
    "T2 11-01~11-08 收入行数": """
        SELECT COUNT(*) FROM (SELECT dt FROM dwd_pay WHERE dt BETWEEN '2026-11-01' AND '2026-11-08' GROUP BY dt) t""",
    # ---------------- 排名 ----------------
    "R1 前五歌曲行数": """
        SELECT COUNT(*) FROM (SELECT song_id FROM dws_song_play_daily WHERE dt='2026-11-08'
          GROUP BY song_id ORDER BY SUM(play_start_cnt) DESC LIMIT 5) t""",
    # 注意：dws_province_daily 是「省 × 市」粒度，直接 GROUP BY province 会出多行。
    # 必须 SUM(dau) 再排序，否则会得到同一个省的三行（广东 102/97/90）。
    "R2 DAU 前三省份": """
        SELECT province, SUM(dau) v FROM dws_province_daily WHERE dt='2026-11-08'
        GROUP BY province ORDER BY v DESC LIMIT 3""",
    # ---------------- 分布 ----------------
    "D1 各曲风播放量": """
        SELECT genre, SUM(play_start_cnt) v FROM dws_genre_daily WHERE dt='2026-11-08' GROUP BY genre ORDER BY v DESC""",
    "D2 各省份播放量行数": """
        SELECT COUNT(*) FROM (SELECT province FROM dws_province_daily WHERE dt='2026-11-08' GROUP BY province) t""",
    "D3 各 OS 用户数": """
        SELECT os, COUNT(DISTINCT COALESCE(NULLIF(user_id,''), device_id)) uv
        FROM dwd_session WHERE dt='2026-11-08' AND event_name='app_launch' GROUP BY os ORDER BY uv DESC""",
    "D4 各渠道启动次数": """
        SELECT channel, COUNT(*) c FROM dwd_session WHERE dt='2026-11-08' AND event_name='app_launch'
        GROUP BY channel ORDER BY c DESC""",
    "D5 各网络播放量": """
        SELECT network, COUNT(*) c FROM dwd_play_start WHERE dt='2026-11-08' GROUP BY network ORDER BY c DESC""",
    # ---------------- 归因 ----------------
    "A1 播放量最高省份": """
        SELECT province, SUM(play_start_cnt) v FROM dws_province_daily WHERE dt='2026-11-08'
        GROUP BY province ORDER BY v DESC LIMIT 1""",
    "A2 完播率最高曲风": """
        SELECT genre, SUM(complete_cnt)/NULLIF(SUM(play_start_cnt),0) r FROM dws_genre_daily
        WHERE dt='2026-11-08' GROUP BY genre ORDER BY r DESC LIMIT 1""",
    "A3 启动最多渠道": """
        SELECT channel, COUNT(*) c FROM dwd_session WHERE dt='2026-11-08' AND event_name='app_launch'
        GROUP BY channel ORDER BY c DESC LIMIT 1""",
    # ---------------- 多表 JOIN ----------------
    "J1 Top1 歌曲的歌手": """
        SELECT s.song_name, s.singer, SUM(d.play_start_cnt) v
        FROM dws_song_play_daily d JOIN dim_song s ON d.song_id=s.song_id
        WHERE d.dt='2026-11-08' GROUP BY s.song_name, s.singer ORDER BY v DESC LIMIT 1""",
    "J2 各歌手播放量行数": """
        SELECT COUNT(*) FROM (SELECT s.singer FROM dws_song_play_daily d JOIN dim_song s
          ON d.song_id=s.song_id WHERE d.dt='2026-11-08' GROUP BY s.singer) t""",
    "J3 各曲风平均进度": """
        SELECT genre, ROUND(SUM(avg_progress*play_start_cnt)/SUM(play_start_cnt),4) p
        FROM dws_genre_daily WHERE dt='2026-11-08' GROUP BY genre ORDER BY p DESC""",
    # ---------------- 口径歧义 ----------------
    "K1 11-08 播放用户数": """
        SELECT COUNT(DISTINCT COALESCE(NULLIF(user_id,''), device_id)) FROM dwd_play_start WHERE dt='2026-11-08'""",
    "K2 35天窗口去重UV": """
        SELECT COUNT(DISTINCT COALESCE(NULLIF(user_id,''), device_id)) FROM dwd_session
        WHERE dt BETWEEN '2026-10-05' AND '2026-11-08' AND event_name='app_launch'""",
    "K3 DAU-播放用户": """
        SELECT (SELECT dau FROM dws_user_daily WHERE dt='2026-11-08')
             - (SELECT COUNT(DISTINCT COALESCE(NULLIF(user_id,''), device_id)) FROM dwd_play_start WHERE dt='2026-11-08')""",
    # ---------------- 明细 ----------------
    "E1 播放量>200 的歌曲数": """
        SELECT COUNT(*) FROM (SELECT song_id FROM dws_song_play_daily WHERE dt='2026-11-08'
          GROUP BY song_id HAVING SUM(play_start_cnt) > 200) t""",
    "E2 有播放的曲风数": """
        SELECT COUNT(DISTINCT genre) FROM dws_genre_daily WHERE dt='2026-11-08' AND play_start_cnt>0""",
    # ---------------- 时间比较 ----------------
    "P1 11-08 比 11-01 播放量": """
        SELECT (SELECT SUM(play_start_cnt) FROM dws_user_daily WHERE dt='2026-11-08')
             - (SELECT SUM(play_start_cnt) FROM dws_user_daily WHERE dt='2026-11-01')""",
    "P2 11-08 比上周同期(11-01) DAU": """
        SELECT (SELECT dau FROM dws_user_daily WHERE dt='2026-11-08')
             - (SELECT dau FROM dws_user_daily WHERE dt='2026-11-01')""",
}

for name, sql in Q.items():
    r = db.run_sql(sql, cfg, max_rows=50)
    if r.get("error"):
        print("%-32s ERROR %s" % (name, str(r["error"])[:100]))
    else:
        print("%-32s %s" % (name, r["rows"]))
