-- =============================================================================
-- 01_ddl_database.sql —— 离线数仓库（Hive 侧，单库 + 表名前缀分层）
-- 执行：hive -f sql/offline/01_ddl_database.sql
--
-- 为什么是「一个库 + 前缀」而不是 ods/dwd/dws/ads 四个库：
--
--   1) 与实时链路完全对称。实时那条链路的四层都在 Doris 的 music_dw 里，
--      表名就是 ods_/dwd_/dws_/ads_ 前缀。离线这边用同样的库名、同样的表名，
--      于是批流对账时可以写两句几乎相同的 SQL：
--          SELECT ... FROM music_dw.dwd_play_end   -- Doris（实时）
--          SELECT ... FROM music_dw.dwd_play_end   -- Hive（离线）
--      只有引擎不同，SQL 一字不改。这比「库名不同、表名也不同」更能证明
--      「批流同源」——同名同构，数字必须一致。
--
--   2) 表少的时候一层一库是过度设计。本项目全量约 20 张表，
--      Hive 的 `SHOW TABLES` 一眼能扫完，前缀已经足够分层。
--
--   3) 一层一库真正的价值在「按库授权」：分析师只给 ads 库的读权限、
--      DWD 库只给 ETL 账号写。这是数据安全需求，不是建模需求。
--      本项目单用户（root）开发，没有这个场景。将来真要做权限隔离，
--      再拆库也不影响表名（`ALTER TABLE ... RENAME` 迁移路径很短）。
--
-- 前缀约定（与 Doris 侧一致）：
--   ods_  原始层：按 dt 分区落 HDFS，不清洗、不解析 properties
--   dwd_  明细层：清洗、类型转换、维度退化、拉链表
--   dws_  汇总层：按主题轻度聚合（1d / 7d 等时间粒度）
--   ads_  应用层：直接对接报表与批流对账
-- =============================================================================

CREATE DATABASE IF NOT EXISTS music_dw
COMMENT '音乐APP数仓（离线/Hive侧，四层用表名前缀区分）'
LOCATION 'hdfs://WWC160:9820/warehouse/music_dw';

USE music_dw;
SHOW DATABASES;
