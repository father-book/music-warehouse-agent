-- 由 code/gen_meta_metric_sql.py 生成，口径来源 docs/20-指标字典定义.md
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

INSERT INTO music_offline.meta_metric (metric_code, metric_name, metric_level, formula, filter_condition, source_table, time_grain, realtime_able, narrative, owner, version, description) VALUES
('launch_cnt', '启动次数', '原子', 'COUNT(*)', 'event_name=''app_launch''', 'dwd_session', '日', '可用', '通用', '数仓组', 1, ''),
('uv', '活跃用户数', '原子', 'COUNT(DISTINCT user_id)', 'event_name=''app_launch''', 'dwd_session', '日', '近似', '通用', '数仓组', 1, '实时侧为 HLL/分桶近似，报表须带 _approx 后缀或脚注，不能与精确值同列环比。'),
('session_cnt', '会话数', '原子', 'COUNT(DISTINCT session_id)', '', 'dwd_session', '日', '可用', '数开', '数仓组', 1, ''),
('session_duration_sum', '会话总时长', '原子', 'SUM(stay_duration)', 'event_name=''app_exit''', 'dwd_session', '日', '仅离线', '数开', '数仓组', 1, ''),
('impression_cnt', '曝光次数', '原子', 'COUNT(*)', '', 'dwd_impression', '日', '可用', 'AI/算法', '数仓组', 1, ''),
('click_cnt', '点击次数', '原子', 'COUNT(*)', '', 'dwd_click', '日', '可用', 'AI/算法', '数仓组', 1, ''),
('play_start_cnt', '起播次数', '原子', 'COUNT(*)', '', 'dwd_play_start', '日', '可用', '通用', '数仓组', 1, ''),
('play_uv', '播放用户数', '原子', 'COUNT(DISTINCT user_id)', '', 'dwd_play_start', '日', '近似', '通用', '数仓组', 1, '实时侧近似去重，见 docs/20 §七。'),
('play_end_cnt', '播放结束次数', '原子', 'COUNT(*)', '', 'dwd_play_end', '日', '可用', '通用', '数仓组', 1, ''),
('complete_cnt', '完播次数', '原子', 'COUNT(*)', 'play_progress>=0.9', 'dwd_play_end', '日', '可用', '通用', '数仓组', 1, ''),
('skip_cnt', '跳过次数', '原子', 'COUNT(*)', '', 'dwd_skip', '日', '可用', '通用', '数仓组', 1, ''),
('play_duration_sum', '播放时长总和', '原子', 'SUM(play_duration)', '', 'dwd_play_end', '日', '可用', '算法', '数仓组', 1, ''),
('seek_cnt', '拖动次数', '原子', 'COUNT(*)', 'event_name=''seek''', 'dwd_play_control', '日', '仅离线', '算法', '数仓组', 1, ''),
('play_error_cnt', '播放失败次数', '原子', 'COUNT(*)', 'event_name=''play_error''', 'dwd_play_control', '日', '可用', '数开', '数仓组', 1, ''),
('collect_cnt', '收藏次数', '原子', 'COUNT(*)', 'event_name=''collect'' AND action=''add''', 'dwd_interaction', '日', '可用', '算法', '数仓组', 1, '');

INSERT INTO music_offline.meta_metric (metric_code, metric_name, metric_level, formula, filter_condition, source_table, time_grain, realtime_able, narrative, owner, version, description) VALUES
('like_cnt', '点赞次数', '原子', 'COUNT(*)', 'event_name=''like'' AND action=''add''', 'dwd_interaction', '日', '可用', '算法', '数仓组', 1, ''),
('share_cnt', '分享次数', '原子', 'COUNT(*)', 'event_name=''share''', 'dwd_interaction', '日', '可用', '通用', '数仓组', 1, ''),
('interact_cnt', '互动总次数', '原子', 'COUNT(*)', 'event_name IN (''collect'',''like'',''share'',''comment'',''download'',''follow_artist'',''playlist_add'') AND (action IS NULL OR action != ''remove'')', 'dwd_interaction', '日', '可用', '通用', '数仓组', 1, '过滤条件见 docs/20 §3.1：排除 action=''remove''，否则取消收藏/点赞会让互动率虚高。'),
('search_cnt', '搜索次数', '原子', 'COUNT(*)', 'event_name=''search''', 'dwd_search', '日', '可用', '数开', '数仓组', 1, ''),
('search_click_cnt', '搜索点击次数', '原子', 'COUNT(*)', 'event_name=''search_click''', 'dwd_search', '日', '仅离线', '算法', '数仓组', 1, ''),
('search_zero_cnt', '零结果搜索数', '原子', 'COUNT(*)', 'event_name=''search'' AND result_cnt=0', 'dwd_search', '日', '仅离线', '数开', '数仓组', 1, 'v9 造数起 result_cnt 按 8% 零结果 / 30% 1~15 / 42% 16~80 / 20% 81~300 分档，该指标已有真实取值；口径为 dwd_search 中 result_cnt=0 的 search 事件数。'),
('order_cnt', '支付订单数', '原子', 'COUNT(*)', 'event_name=''order_pay''', 'dwd_pay', '日', '可用', '通用', '数仓组', 1, ''),
('revenue_sum', '收入金额', '原子', 'SUM(amount)', 'event_name=''order_pay''', 'dwd_pay', '日', '可用', '通用', '数仓组', 1, '已按 docs/20 §1.3 落地：dwd_pay.amount → dws/ads_revenue → Doris 全链路 DECIMAL(18,2)；比率列仍用 DOUBLE。'),
('pay_uv', '付费用户数', '原子', 'COUNT(DISTINCT user_id)', 'event_name=''order_pay''', 'dwd_pay', '日', '近似', '通用', '数仓组', 1, '实时侧近似去重，见 docs/20 §七。'),
('refund_cnt', '退款订单数', '原子', 'COUNT(*)', 'event_name=''order_refund''', 'dwd_pay', '日', '可用', '数开', '数仓组', 1, ''),
('ad_impression_cnt', '广告曝光次数', '原子', 'COUNT(*)', 'event_name=''ad_impression''', 'dwd_ad', '日', '可用', '通用', '数仓组', 1, ''),
('ad_click_cnt', '广告点击次数', '原子', 'COUNT(*)', 'event_name=''ad_click''', 'dwd_ad', '日', '可用', '通用', '数仓组', 1, ''),
('pv', '页面浏览量', '原子', 'COUNT(*)', '', 'dwd_page_view', '日', '可用', '数开', '数仓组', 1, ''),
('ctr', '点击率', '派生', 'click_cnt / impression_cnt', '', '', '日', '可用', 'AI/算法', '数仓组', 1, ''),
('complete_rate', '完播率', '派生', 'complete_cnt / play_start_cnt', '', '', '日', '可用', '通用', '数仓组', 1, '分母必须是 play_start_cnt；用 play_end_cnt 会剔除跳过的人，完播率虚高（实测 73% vs 51.36%）。');

INSERT INTO music_offline.meta_metric (metric_code, metric_name, metric_level, formula, filter_condition, source_table, time_grain, realtime_able, narrative, owner, version, description) VALUES
('skip_rate', '跳过率', '派生', 'skip_cnt / play_start_cnt', '', '', '日', '可用', '通用', '数仓组', 1, ''),
('seek_rate', '拖动率', '派生', 'seek_cnt / play_start_cnt', '', '', '日', '仅离线', '算法', '数仓组', 1, ''),
('play_error_rate', '播放失败率', '派生', 'play_error_cnt / play_start_cnt', '', '', '日', '可用', '数开', '数仓组', 1, ''),
('interact_rate', '互动率', '派生', 'interact_cnt / play_start_cnt', '', '', '日', '可用', '通用', '数仓组', 1, ''),
('collect_rate', '收藏率', '派生', 'collect_cnt / play_start_cnt', '', '', '日', '可用', '算法', '数仓组', 1, ''),
('like_rate', '点赞率', '派生', 'like_cnt / play_start_cnt', '', '', '日', '可用', '算法', '数仓组', 1, ''),
('share_rate', '分享率', '派生', 'share_cnt / play_start_cnt', '', '', '日', '可用', '通用', '数仓组', 1, ''),
('search_click_rate', '搜索点击率', '派生', 'search_click_cnt / search_cnt', '', '', '日', '仅离线', '算法', '数仓组', 1, ''),
('search_zero_rate', '搜索零结果率', '派生', 'search_zero_cnt / search_cnt', '', '', '日', '仅离线', '数开', '数仓组', 1, '零结果搜索数 / 搜索数；v9 起数值非零，可按关键词下钻（keyword 有 10 个取值）。'),
('ad_ctr', '广告点击率', '派生', 'ad_click_cnt / ad_impression_cnt', '', '', '日', '可用', '通用', '数仓组', 1, ''),
('pay_rate', '付费率', '派生', 'pay_uv / uv', '', '', '日', '近似', '通用', '数仓组', 1, '分子用 pay_uv 而非 order_cnt：一个用户可能买多次，用订单数会让付费率超过 100%。'),
('refund_rate', '退款率', '派生', 'refund_cnt / order_cnt', '', '', '日', '可用', '数开', '数仓组', 1, ''),
('arpu', '人均收入', '派生', 'revenue_sum / uv', '', '', '日', '近似', '通用', '数仓组', 1, '12 号脚本 arpu 的分母是付费用户数（ARPPU 口径），与本字典「revenue_sum/uv」不同，答辩需说明。'),
('arppu', '付费人均', '派生', 'revenue_sum / pay_uv', '', '', '日', '近似', '通用', '数仓组', 1, ''),
('avg_session_duration', '人均会话时长', '派生', 'session_duration_sum / session_cnt', '', '', '日', '仅离线', '数开', '数仓组', 1, '');

INSERT INTO music_offline.meta_metric (metric_code, metric_name, metric_level, formula, filter_condition, source_table, time_grain, realtime_able, narrative, owner, version, description) VALUES
('avg_play_per_user', '人均起播', '派生', 'play_start_cnt / play_uv', '', '', '日', '可用', '通用', '数仓组', 1, ''),
('avg_progress', '平均播放进度', '派生', 'AVG(play_progress)', '', '', '日', '可用', '通用', '数仓组', 1, '只统计 dwd_play_end，skip 没有进度记录；含义是「播完那批人的平均进度」，不是全体。'),
('retention_d1', '次日留存', '派生', '次日活跃∩当日新增 / 当日新增', '', '', 'batch_only', '仅离线', '通用', '数仓组', 1, ''),
('ctr_by_position', '分坑位点击率', '衍生', 'ctr 按 position 分组', '维度=坑位 0~24', '', 'batch_only', '仅离线', '算法', '数仓组', 1, '需全量历史或跨天状态，流侧不做（docs/20 §七）。'),
('complete_rate_by_genre', '分曲风完播率', '衍生', 'complete_rate 按 genre', '维度=曲风', '', 'batch_only', '仅离线', '通用', '数仓组', 1, '需全量历史或跨天状态，流侧不做（docs/20 §七）。'),
('song_rank_daily', '歌曲日榜', '衍生', 'play_start_cnt 按 song_id 排名', '维度=歌曲', '', 'batch_only', '仅离线', '通用', '数仓组', 1, '需全量历史或跨天状态，流侧不做（docs/20 §七）。'),
('revenue_7d', '近 7 日收入', '衍生', 'revenue_sum 滚动 7 天', '维度=时间', '', 'batch_only', '仅离线', '通用', '数仓组', 1, '需全量历史或跨天状态，流侧不做（docs/20 §七）。'),
('retention_d1_by_cohort', '分 cohort 次日留存', '衍生', 'retention_d1 按 cohort_date', '维度=同期群', '', 'batch_only', '仅离线', '通用', '数仓组', 1, '需全量历史或跨天状态，流侧不做（docs/20 §七）。'),
('ad_ecpm', '广告千次曝光收入', '衍生', 'ad_ctr × 单价 × 1000', '维度=广告位', '', 'batch_only', '仅离线', '通用', '数仓组', 1, '需全量历史或跨天状态，流侧不做（docs/20 §七）。'),
('funnel_rate', '漏斗各层转化率', '衍生', '曝光→点击→起播→完播→付费', '维度=环节', '', 'batch_only', '仅离线', '通用', '数仓组', 1, '需全量历史或跨天状态，流侧不做（docs/20 §七）。每一层的分母是上一层的分子，这是漏斗的定义，不能换分母。');

-- 校验：应为 55 / 28 / 20 / 7
SELECT metric_level, COUNT(*) FROM music_offline.meta_metric GROUP BY metric_level;
