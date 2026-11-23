-- =============================================================================
-- 02_ods_music_event.sql —— 离线 ODS：音乐 APP 埋点原始明细（外部表 + dt 分区）
-- 执行：hive -f sql/offline/02_ods_music_event.sql
--
-- 与实时链路的对照（答辩可直接讲）：
--
--   | 维度       | 实时（Doris）              | 离线（Hive）              |
--   |------------|---------------------------|--------------------------|
--   | 库表名     | music_dw.ods_music_event   | music_dw.ods_music_event  |  ← 完全一致
--   | 存储模型   | DUPLICATE KEY              | EXTERNAL + ORC            |
--   | 分区字段   | etl_date（入仓日期）        | dt（事件日期）             |
--   | 写入口     | Kafka → Flink → Stream Load| Spark INSERT OVERWRITE    |
--
--   分区语义的差异是刻意的，不是疏漏：
--     实时侧用「入仓日期」，因为事件时间可能是脏的（未来时间/乱码），
--     用事件时间分区会让整批 Stream Load 因「找不到分区」而回滚（10-31 实际踩过）。
--     离线侧用「事件日期」，因为数据是 T+1 批处理，已经过实时链路的质检，
--     事件时间可信；而且离线查询天然按事件日期过滤（"看 11 月 4 日的数据"）。
--
-- 为什么用 EXTERNAL：
--   外部表的元数据与数据解耦，DROP TABLE 不会删 HDFS 文件。
--   离线 ODS 是原始归档，任何误操作都不该让它消失。
--
-- 24 列字段与 Doris 版逐字一致 —— 这是批流同源的前提。
-- =============================================================================

CREATE EXTERNAL TABLE IF NOT EXISTS music_dw.ods_music_event
(
    event_id      STRING COMMENT '事件唯一ID',
    event_name    STRING COMMENT 'play_start/play_end/skip/collect/search/share/page_view',
    event_time    STRING COMMENT '客户端事件时间（毫秒时间戳）',
    server_time   STRING COMMENT '服务端接收时间（毫秒时间戳）',
    user_id       STRING COMMENT '用户ID，未登录为空',
    device_id     STRING COMMENT '设备ID',
    is_login      STRING COMMENT '是否登录',
    is_vip        STRING COMMENT '是否会员',
    app_version   STRING COMMENT 'APP版本',
    os            STRING COMMENT 'android/ios/web/car',
    os_version    STRING COMMENT '系统版本',
    device_model  STRING COMMENT '设备型号',
    network       STRING COMMENT 'wifi/4g/5g/3g',
    carrier       STRING COMMENT '运营商',
    ip            STRING COMMENT '客户端IP',
    province      STRING COMMENT '省份',
    city          STRING COMMENT '城市',
    session_id    STRING COMMENT '会话ID',
    session_seq   STRING COMMENT '会话内事件序号',
    is_foreground STRING COMMENT '前后台标记',
    channel       STRING COMMENT '曝光来源渠道',
    page_id       STRING COMMENT '事件发生页面',
    properties    STRING COMMENT '事件私有属性 JSON 原文，不解析',
    etl_time      STRING COMMENT '入仓时间（yyyy-MM-dd HH:mm:ss）'
)
COMMENT '离线ODS-音乐APP埋点原始明细（24列，与 Doris 版同构）'
PARTITIONED BY (dt STRING COMMENT '事件日期 yyyy-MM-dd')
STORED AS ORC
LOCATION 'hdfs://WWC160:9820/warehouse/music_dw/ods_music_event'
TBLPROPERTIES ('orc.compress' = 'SNAPPY');

-- 分区登记：数据由 Spark 直接写 dt=YYYY-MM-DD 目录，用 MSCK 自动修复最省事
-- （幂等，重复执行无副作用；新增日期后重跑这一句即可）
MSCK REPAIR TABLE music_dw.ods_music_event;

SHOW PARTITIONS music_dw.ods_music_event;
