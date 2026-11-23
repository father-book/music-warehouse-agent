-- 实时链路历史快照只读授权（Doris 2.1）
-- 执行：mysql -h192.168.128.163 -P9030 -uroot < sql/agent/grant_rt_ro.sql
--
-- 背景：agent_ro 原本只有 hive_catalog.music_dw 与 internal.music_offline 的
--      SELECT 权。实时快照在 internal.music_dw，三段式查询会报
--      "SELECT command denied ... for table 'music_dw.ops_data_quality'"。
-- 原则：**逐表授权，不给整库**。internal.music_dw 里有 32 张表，其中
--      dwd_play_start 等同名明细表与离线口径差 6.2 倍（docs/34），
--      整库授权会把护栏从"唯一入口"降级成"唯一提醒"。
--      白名单以 agent/semantic/realtime.yaml 为准，改那份要同步这里。
--
-- 2026-11-11 实测：9 条 GRANT 全部生效；白名单外的 dwd_play_start 仍由
--      护栏 R17 拦（数据库层因历史库级授权可能仍可读，护栏才是硬边界）。

GRANT SELECT_PRIV ON internal.music_dw.ops_data_quality        TO 'agent_ro'@'%';
GRANT SELECT_PRIV ON internal.music_dw.ops_pipeline_latency    TO 'agent_ro'@'%';
GRANT SELECT_PRIV ON internal.music_dw.ops_job_health          TO 'agent_ro'@'%';
GRANT SELECT_PRIV ON internal.music_dw.ads_client_quality_1min TO 'agent_ro'@'%';
GRANT SELECT_PRIV ON internal.music_dw.ads_session_1min        TO 'agent_ro'@'%';
GRANT SELECT_PRIV ON internal.music_dw.ads_song_rank_10min     TO 'agent_ro'@'%';
GRANT SELECT_PRIV ON internal.music_dw.ads_province_10min      TO 'agent_ro'@'%';
GRANT SELECT_PRIV ON internal.music_dw.ads_realtime_overview   TO 'agent_ro'@'%';
GRANT SELECT_PRIV ON internal.music_dw.dwd_quality_error       TO 'agent_ro'@'%';

-- 角色层（governance/roles.layers）也要带 rt，否则策略引擎会判"不在可见范围"。
-- 增量更新，不用 force seed（那会清 sessions 把在线用户踢下线）：
--   UPDATE roles SET layers = layers || ',rt'
--    WHERE role <> 'data_engineer' AND layers NOT LIKE '%rt%';
--   python -c "import sys;sys.path.insert(0,'code/agent');import _bootstrap;
--     from conf import load_config;from governance import identity;import realtimelane;
--     cfg=load_config();[identity.grant_table(t,'rt',cfg) for t in realtimelane.table_names(cfg)]"
--
-- 验证：
--   SELECT COUNT(*) FROM internal.music_dw.ops_data_quality;   -- 1043
--   SELECT COUNT(*) FROM internal.music_dw.dwd_play_start;     -- 护栏 R17 应拒
