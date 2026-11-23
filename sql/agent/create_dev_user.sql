-- =============================================================================
-- 数仓开发 Agent 的 dev 账号（2026-11-09 执行）
--
-- 设计：问数模式用 agent_ro（只读），开发模式用 agent_dev（只写 dev 库）。
--       两个账号分开，避免"Agent 为了干活而获得正式库写权限"。
--
-- 执行前确认：
--   music_dev 库已建好（Hive + Doris 各一个，见 docs/26）
--
-- ⚠️ 权限验证必须按 create_ro_user.sql 里的教训做：
--    Doris 对 `DELETE ... WHERE 1=0` 有恒假短路，会"成功返回"但不校验权限，
--    用它验证会误判成"能删数据"。正确做法：建临时表 + 插几行 + 真实条件 DELETE
--    + 回查行数不变。本文件末尾附完整验证清单。
-- =============================================================================

-- ---------- 1. 账号 ----------
CREATE USER IF NOT EXISTS 'agent_dev'@'%' IDENTIFIED BY 'AgentDev_2026#dw';

-- ---------- 2. 只读正式库（问数仍需要读正式表做对照）----------
GRANT SELECT_PRIV ON music_dw.*      TO 'agent_dev'@'%';
GRANT SELECT_PRIV ON music_offline.* TO 'agent_dev'@'%';
GRANT SELECT_PRIV ON information_schema.* TO 'agent_dev'@'%';

-- ---------- 3. 只在 dev 库有写权限 ----------
-- Doris 权限是库级的，所以"能写 music_dev"与"不能写 music_dw"由数据库强制保证，
-- 这是硬隔离。Hive 侧没有账号体系，见 docs/26 第三节，必须靠应用层补。
GRANT CREATE_PRIV, DROP_PRIV, ALTER_PRIV, LOAD_PRIV ON music_dev.* TO 'agent_dev'@'%';
GRANT SELECT_PRIV ON music_dev.* TO 'agent_dev'@'%';
-- music_offline 的 dev  counterpart 单独一个库，避免两张同名表撞车（见 gen_dev_ddl.py）
GRANT CREATE_PRIV, DROP_PRIV, ALTER_PRIV, LOAD_PRIV ON music_dev_offline.* TO 'agent_dev'@'%';
GRANT SELECT_PRIV ON music_dev_offline.* TO 'agent_dev'@'%';

SET PROPERTY FOR 'agent_dev' 'max_user_connections' = '10';


-- =============================================================================
-- 验证清单（逐条执行，结果写进 docs/26 的验收表）
-- =============================================================================
-- 用 agent_dev 登录：
--   SELECT current_user();                                    -- agent_dev@%
--   SELECT COUNT(*) FROM music_dw.dwd_play_start;             -- 应成功（只读）
--   CREATE TABLE music_dev.zz_probe (id INT);                 -- 应成功
--   INSERT INTO music_dev.zz_probe VALUES (1);                -- 应成功
--   DROP TABLE music_dev.zz_probe;                            -- 应成功
--
-- 用 root 先建一张正式库测试表：
--   CREATE TABLE music_dw.zz_perm_test (id INT, v VARCHAR(10))
--     UNIQUE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
--     PROPERTIES("replication_num"="1");
--   INSERT INTO music_dw.zz_perm_test VALUES (1,'a'),(2,'b');
--
-- 再用 agent_dev 执行，全部应被拒绝，且回查行数必须仍为 2：
--   INSERT INTO music_dw.zz_perm_test VALUES (3,'c');         -- 拒绝
--   DELETE FROM music_dw.zz_perm_test WHERE id = 1;           -- 拒绝（真实条件！）
--   UPDATE music_dw.zz_perm_test SET v='z' WHERE id = 2;      -- 拒绝
--   ALTER TABLE music_dw.zz_perm_test ADD COLUMN x INT;       -- 拒绝
--   DROP TABLE music_dw.zz_perm_test;                         -- 拒绝
--   CREATE TABLE music_dw.zz_probe2 (id INT);                 -- 拒绝
--   DROP DATABASE music_dw;                                   -- 拒绝
--
-- 清理：root DROP TABLE music_dw.zz_perm_test;
-- =============================================================================
