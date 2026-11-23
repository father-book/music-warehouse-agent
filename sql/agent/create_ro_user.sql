-- Data Agent 只读账号（Doris 2.1）
-- 执行位置：能连 Doris FE 9030 的机器
-- 执行方式：mysql -h192.168.128.163 -P9030 -uroot < sql/agent/create_ro_user.sql
--
-- 背景：当前集群只有 root / admin 两个管理员账号，且 root 为空密码、持有 Node_priv Admin_priv。
--      Agent 一旦被诱导生成 DELETE / DROP，没有任何一道数据库层护栏。
-- 原则：Agent 只需要 SELECT，多一分都不要给。
--
-- 注意：本文件含明文密码，仅适用于本地实验集群；生产环境应由密钥管理下发。

CREATE USER IF NOT EXISTS 'agent_ro'@'%' IDENTIFIED BY 'AgentRo_2026#dw';

-- 只给两张库的查询权：离线 ADS（P0 唯一数据源）+ 实时库（P1 用）
GRANT SELECT_PRIV ON music_offline.* TO 'agent_ro'@'%';
GRANT SELECT_PRIV ON music_dw.*      TO 'agent_ro'@'%';

-- 元数据要给，否则 Agent 无法读 information_schema 自省表结构
GRANT SELECT_PRIV ON information_schema.* TO 'agent_ro'@'%';

-- 连接数限制：防止一次批量评测把连接池打满，影响正在跑的 Flink 写入
SET PROPERTY FOR 'agent_ro' 'max_user_connections' = '20';

-- 验证方法（用 agent_ro 登录后逐条执行）：
--   SELECT current_user();                                       -- 应为 agent_ro
--   SELECT COUNT(*) FROM music_offline.ads_daily_overview;       -- 应成功，31
--   SELECT COUNT(*) FROM music_dw.dwd_play_start;                -- 应成功
--   SHOW TABLES FROM music_offline;                              -- 应成功
--   DROP TABLE music_offline.ads_funnel;                         -- 应报 DROP privilege denied
--   INSERT INTO music_offline.ads_funnel VALUES (1);             -- 应报 LOAD command denied
--   CREATE TABLE music_offline.zz(a int);                        -- 应报 CREATE privilege denied
--   ALTER TABLE music_offline.ads_funnel ADD COLUMN x INT;       -- 应报 ALTER denied
--
-- 【坑】不要用 `DELETE FROM t WHERE 1=0` 验证写权限。
--   Doris 对恒假条件做了短路优化：既不执行删除、也不校验 LOAD 权限，语句会"成功返回"，
--   让人误判只读账号能删数据。2026-11-08 实测就踩了这个假阴性。
--   正确做法：用 root 建一张临时表并插入数据，用 agent_ro 执行带真实条件的 DELETE，
--   然后回查行数有没有变。实测结果：拒绝（LOAD privilege denied），行数不变。
--
-- 实测结论（2026-11-08 19:00）：
--   SELECT / SHOW  ✅ 通过
--   DELETE（真实条件） / TRUNCATE / UPDATE / INSERT / CREATE / ALTER / DROP TABLE / DROP DATABASE
--   ❌ 全部被拒绝，无需额外 REVOKE。

