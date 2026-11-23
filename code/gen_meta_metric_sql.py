"""按 docs/20-指标字典定义.md 生成 music_offline.meta_metric 的建表 + 灌数 SQL。

口径全部来自 docs/20 §三（28 原子）/ §四（20 派生）/ §五（7 衍生）= 55 条，
本文件只是把定稿的表格翻译成 Doris SQL，不新增、不改写任何口径。
改口径请改 docs/20，再重跑本脚本（version 字段用于留痕）。

用法：
    python code/gen_meta_metric_sql.py          # 生成 sql/meta_metric.sql
    python3 /root/offline/run_doris_sql.py /root/offline/meta_metric.sql
"""
import os

OUT = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                   "sql", "meta_metric.sql")

OWNER = "数仓组"
VER = 1

# (code, 中文名, 公式, 来源表, 过滤条件, 实时可算性, 叙事)
ATOMIC = [
    ("launch_cnt", "启动次数", "COUNT(*)", "dwd_session", "event_name='app_launch'", "可用", "通用"),
    ("uv", "活跃用户数", "COUNT(DISTINCT user_id)", "dwd_session", "event_name='app_launch'", "近似", "通用"),
    ("session_cnt", "会话数", "COUNT(DISTINCT session_id)", "dwd_session", "", "可用", "数开"),
    ("session_duration_sum", "会话总时长", "SUM(stay_duration)", "dwd_session", "event_name='app_exit'", "仅离线", "数开"),
    ("impression_cnt", "曝光次数", "COUNT(*)", "dwd_impression", "", "可用", "AI/算法"),
    ("click_cnt", "点击次数", "COUNT(*)", "dwd_click", "", "可用", "AI/算法"),
    ("play_start_cnt", "起播次数", "COUNT(*)", "dwd_play_start", "", "可用", "通用"),
    ("play_uv", "播放用户数", "COUNT(DISTINCT user_id)", "dwd_play_start", "", "近似", "通用"),
    ("play_end_cnt", "播放结束次数", "COUNT(*)", "dwd_play_end", "", "可用", "通用"),
    ("complete_cnt", "完播次数", "COUNT(*)", "dwd_play_end", "play_progress>=0.9", "可用", "通用"),
    ("skip_cnt", "跳过次数", "COUNT(*)", "dwd_skip", "", "可用", "通用"),
    ("play_duration_sum", "播放时长总和", "SUM(play_duration)", "dwd_play_end", "", "可用", "算法"),
    ("seek_cnt", "拖动次数", "COUNT(*)", "dwd_play_control", "event_name='seek'", "仅离线", "算法"),
    ("play_error_cnt", "播放失败次数", "COUNT(*)", "dwd_play_control", "event_name='play_error'", "可用", "数开"),
    ("collect_cnt", "收藏次数", "COUNT(*)", "dwd_interaction", "event_name='collect' AND action='add'", "可用", "算法"),
    ("like_cnt", "点赞次数", "COUNT(*)", "dwd_interaction", "event_name='like' AND action='add'", "可用", "算法"),
    ("share_cnt", "分享次数", "COUNT(*)", "dwd_interaction", "event_name='share'", "可用", "通用"),
    ("interact_cnt", "互动总次数", "COUNT(*)", "dwd_interaction",
     "event_name IN ('collect','like','share','comment','download','follow_artist','playlist_add') AND (action IS NULL OR action != 'remove')",
     "可用", "通用"),
    ("search_cnt", "搜索次数", "COUNT(*)", "dwd_search", "event_name='search'", "可用", "数开"),
    ("search_click_cnt", "搜索点击次数", "COUNT(*)", "dwd_search", "event_name='search_click'", "仅离线", "算法"),
    ("search_zero_cnt", "零结果搜索数", "COUNT(*)", "dwd_search", "event_name='search' AND result_cnt=0", "仅离线", "数开"),
    ("order_cnt", "支付订单数", "COUNT(*)", "dwd_pay", "event_name='order_pay'", "可用", "通用"),
    ("revenue_sum", "收入金额", "SUM(amount)", "dwd_pay", "event_name='order_pay'", "可用", "通用"),
    ("pay_uv", "付费用户数", "COUNT(DISTINCT user_id)", "dwd_pay", "event_name='order_pay'", "近似", "通用"),
    ("refund_cnt", "退款订单数", "COUNT(*)", "dwd_pay", "event_name='order_refund'", "可用", "数开"),
    ("ad_impression_cnt", "广告曝光次数", "COUNT(*)", "dwd_ad", "event_name='ad_impression'", "可用", "通用"),
    ("ad_click_cnt", "广告点击次数", "COUNT(*)", "dwd_ad", "event_name='ad_click'", "可用", "通用"),
    ("pv", "页面浏览量", "COUNT(*)", "dwd_page_view", "", "可用", "数开"),
]

# (code, 中文名, 公式, 实时可算性, 叙事) —— 派生指标不绑定单张来源表
DERIVED = [
    ("ctr", "点击率", "click_cnt / impression_cnt", "可用", "AI/算法"),
    ("complete_rate", "完播率", "complete_cnt / play_start_cnt", "可用", "通用"),
    ("skip_rate", "跳过率", "skip_cnt / play_start_cnt", "可用", "通用"),
    ("seek_rate", "拖动率", "seek_cnt / play_start_cnt", "仅离线", "算法"),
    ("play_error_rate", "播放失败率", "play_error_cnt / play_start_cnt", "可用", "数开"),
    ("interact_rate", "互动率", "interact_cnt / play_start_cnt", "可用", "通用"),
    ("collect_rate", "收藏率", "collect_cnt / play_start_cnt", "可用", "算法"),
    ("like_rate", "点赞率", "like_cnt / play_start_cnt", "可用", "算法"),
    ("share_rate", "分享率", "share_cnt / play_start_cnt", "可用", "通用"),
    ("search_click_rate", "搜索点击率", "search_click_cnt / search_cnt", "仅离线", "算法"),
    ("search_zero_rate", "搜索零结果率", "search_zero_cnt / search_cnt", "仅离线", "数开"),
    ("ad_ctr", "广告点击率", "ad_click_cnt / ad_impression_cnt", "可用", "通用"),
    ("pay_rate", "付费率", "pay_uv / uv", "近似", "通用"),
    ("refund_rate", "退款率", "refund_cnt / order_cnt", "可用", "数开"),
    ("arpu", "人均收入", "revenue_sum / uv", "近似", "通用"),
    ("arppu", "付费人均", "revenue_sum / pay_uv", "近似", "通用"),
    ("avg_session_duration", "人均会话时长", "session_duration_sum / session_cnt", "仅离线", "数开"),
    ("avg_play_per_user", "人均起播", "play_start_cnt / play_uv", "可用", "通用"),
    ("avg_progress", "平均播放进度", "AVG(play_progress)", "可用", "通用"),
    ("retention_d1", "次日留存", "次日活跃∩当日新增 / 当日新增", "仅离线", "通用"),
]

# (code, 中文名, 公式, 维度, 叙事) —— 衍生全部仅离线、需要全量或跨天状态
COMPOUND = [
    ("ctr_by_position", "分坑位点击率", "ctr 按 position 分组", "坑位 0~24", "算法"),
    ("complete_rate_by_genre", "分曲风完播率", "complete_rate 按 genre", "曲风", "通用"),
    ("song_rank_daily", "歌曲日榜", "play_start_cnt 按 song_id 排名", "歌曲", "通用"),
    ("revenue_7d", "近 7 日收入", "revenue_sum 滚动 7 天", "时间", "通用"),
    ("retention_d1_by_cohort", "分 cohort 次日留存", "retention_d1 按 cohort_date", "同期群", "通用"),
    ("ad_ecpm", "广告千次曝光收入", "ad_ctr × 单价 × 1000", "广告位", "通用"),
    ("funnel_rate", "漏斗各层转化率", "曝光→点击→起播→完播→付费", "环节", "通用"),
]

# 口径注释：docs/20 §4.1 的三个易错点 + 造数局限，写进 description 防止误读
NOTES = {
    "complete_rate": "分母必须是 play_start_cnt；用 play_end_cnt 会剔除跳过的人，完播率虚高（实测 73% vs 51.36%）。",
    "pay_rate": "分子用 pay_uv 而非 order_cnt：一个用户可能买多次，用订单数会让付费率超过 100%。",
    "avg_progress": "只统计 dwd_play_end，skip 没有进度记录；含义是「播完那批人的平均进度」，不是全体。",
    "interact_cnt": "过滤条件见 docs/20 §3.1：排除 action='remove'，否则取消收藏/点赞会让互动率虚高。",
    "funnel_rate": "每一层的分母是上一层的分子，这是漏斗的定义，不能换分母。",
    "uv": "实时侧为 HLL/分桶近似，报表须带 _approx 后缀或脚注，不能与精确值同列环比。",
    "play_uv": "实时侧近似去重，见 docs/20 §七。",
    "pay_uv": "实时侧近似去重，见 docs/20 §七。",
    "search_zero_cnt": "v9 造数起 result_cnt 按 8% 零结果 / 30% 1~15 / 42% 16~80 / 20% 81~300 分档，该指标已有真实取值；口径为 dwd_search 中 result_cnt=0 的 search 事件数。",
    "search_zero_rate": "零结果搜索数 / 搜索数；v9 起数值非零，可按关键词下钻（keyword 有 10 个取值）。",
    "revenue_sum": "已按 docs/20 §1.3 落地：dwd_pay.amount → dws/ads_revenue → Doris 全链路 DECIMAL(18,2)；比率列仍用 DOUBLE。",
    "arpu": "12 号脚本 arpu 的分母是付费用户数（ARPPU 口径），与本字典「revenue_sum/uv」不同，答辩需说明。",
}

DDL = """-- 由 code/gen_meta_metric_sql.py 生成，口径来源 docs/20-指标字典定义.md
-- 28 原子 + 20 派生 + 7 衍生 = 55 条。不要手改本文件，改 docs/20 后重跑生成器。
CREATE TABLE IF NOT EXISTS music_offline.meta_metric (
  metric_code      VARCHAR(64)  NOT NULL COMMENT '唯一编码',
  metric_name      VARCHAR(64)  NOT NULL COMMENT '中文名',
  metric_level     VARCHAR(16)  NOT NULL COMMENT '原子/派生/衍生',
  formula          VARCHAR(512) NOT NULL COMMENT '公式；派生指标引用原子 code',
  filter_condition VARCHAR(512)          COMMENT '口径过滤条件',
  source_table     VARCHAR(128)          COMMENT '主表（派生指标可空）',
  time_grain       VARCHAR(16)  NOT NULL COMMENT '日/小时/10分钟/batch_only',
  -- Doris 的 VARCHAR(n) 按字节计：中文 3 字节/字。「仅离线」= 9 字节，
  -- 设计稿初版的 VARCHAR(8) 会被 strict mode 拦下，这里统一放宽。
  realtime_able    VARCHAR(16)  NOT NULL COMMENT '可用/仅离线/近似',
  narrative        VARCHAR(32)           COMMENT '主叙事：数开/AI数工/算法/通用',
  owner            VARCHAR(32),
  version          INT          NOT NULL DEFAULT 1 COMMENT '口径变更升版本',
  description      VARCHAR(512)
) ENGINE=OLAP UNIQUE KEY(metric_code)
DISTRIBUTED BY HASH(metric_code) BUCKETS 1
PROPERTIES ("replication_num" = "1");
"""


def esc(s):
    """Doris SQL 字符串字面量：单引号转义。"""
    return (s or "").replace("'", "''")


def rows():
    out = []
    for code, name, formula, src, cond, rt, nar in ATOMIC:
        out.append((code, name, "原子", formula, cond, src, "日", rt, nar,
                    NOTES.get(code, "")))
    for code, name, formula, rt, nar in DERIVED:
        grain = "batch_only" if code == "retention_d1" else "日"
        out.append((code, name, "派生", formula, "", "", grain, rt, nar,
                    NOTES.get(code, "")))
    for code, name, formula, dim, nar in COMPOUND:
        out.append((code, name, "衍生", formula, "维度=" + dim, "",
                    "batch_only", "仅离线", nar,
                    "需全量历史或跨天状态，流侧不做（docs/20 §七）。"
                    + NOTES.get(code, "")))
    return out


def main():
    data = rows()
    codes = [r[0] for r in data]
    assert len(codes) == 55, "指标数不是 55：%d" % len(codes)
    assert len(set(codes)) == 55, "存在重复 metric_code"

    lines = [DDL]
    # 每 15 条一批 INSERT，避免单条 SQL 过长；UNIQUE KEY 下重复执行是覆盖，幂等。
    for i in range(0, len(data), 15):
        chunk = data[i:i + 15]
        vals = []
        for r in chunk:
            lit = lambda x: "'%s'" % esc(str(x))
            # version 是 INT，不加引号；其余字段都是字符串
            vals.append("(%s)" % ", ".join(
                [lit(x) for x in (r[0], r[1], r[2], r[3], r[4], r[5],
                                  r[6], r[7], r[8], OWNER)] +
                [str(VER), lit(r[9])]))
        lines.append(
            "INSERT INTO music_offline.meta_metric "
            "(metric_code, metric_name, metric_level, formula, filter_condition, "
            "source_table, time_grain, realtime_able, narrative, owner, version, description) "
            "VALUES\n" + ",\n".join(vals) + ";\n")
    lines.append("-- 校验：应为 55 / 28 / 20 / 7\n"
                 "SELECT metric_level, COUNT(*) FROM music_offline.meta_metric "
                 "GROUP BY metric_level;\n")
    with open(OUT, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    print("生成 %s：%d 条指标，%d 条 INSERT" %
          (OUT, len(data), (len(data) + 14) // 15))
    from collections import Counter
    print("分层:", dict(Counter(r[2] for r in data)))
    print("实时可算性:", dict(Counter(r[7] for r in data)))


if __name__ == "__main__":
    main()
