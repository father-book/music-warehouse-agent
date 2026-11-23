-- =============================================================================
-- ODS 层建表：音乐 APP 埋点原始明细（公共属性列 + properties JSON 原文）
-- 引擎：Apache Doris 2.x
-- 执行：mysql -hWWC163 -P9030 -uroot < doris_ods_music_event.sql
-- 变更（2026-10-31，对应 docs/04）：
--   1. 字段改为「公共属性 22 列 + properties 字符串」
--   2. 分区字段由 dt（事件时间）改为 etl_date（入仓日期）
-- =============================================================================
-- 设计说明（论文 3.2 节 / 答辩可直接讲）
--
-- 1) 为什么 properties 用 STRING 而不是 JSON 类型
--    用 JSON 类型时，非法 JSON 会在写入阶段被拒绝，违背「ODS 必须收得下任何数据」
--    的原则。存字符串则不解析、不校验，解析推迟到 DWD。
--    另一个好处：不同事件类型的私有属性结构不同，用 STRING 无需为每种类型定义
--    schema，将来新增事件类型不用改表结构。
--
-- 2) 为什么用 DUPLICATE 模型而不是 UNIQUE
--    ODS 是原始层，唯一职责是原样归档。DUPLICATE 追加写入、不做主键去重，才能保留
--    埋点中真实存在的重复上报供质检检出。若用 UNIQUE，重复数据在落库时被合并，
--    「主键重复校验」这条规则将永远检不出异常——质检失去观测对象。
--
-- 3) 为什么分区用 etl_date（入仓日期）而不是事件时间
--    事件时间可能是脏的（未来时间、乱码）。用事件时间分区时，这类脏数据会因
--    「找不到分区」导致整批 Stream Load 回滚——2026-10-31 已实际踩过：首次落库
--    只成功 34910 条，恰好丢掉一个 subtask 的全部数据。
--    入仓日期永远是「现在」，脏数据不会导致写入失败，质检才有东西可查。
--
-- 4) 为什么除 etl_date 外全部 STRING
--    同上，避免类型转换失败在写入阶段发生。转型放到 DWD 做，转型失败本身可被统计。
--
-- 5) etl_time 的作用
--    记录落库时刻，用于计算端到端延迟 = etl_time - server_time，
--    这是「实时」二字唯一的量化证据。
-- =============================================================================

CREATE DATABASE IF NOT EXISTS music_dw;

DROP TABLE IF EXISTS music_dw.ods_music_event;

CREATE TABLE music_dw.ods_music_event
(
    `event_id`      VARCHAR(64)  NULL COMMENT '事件唯一ID，客户端生成',
    `event_name`    VARCHAR(32)  NULL COMMENT '事件名：play_start/play_end/skip/collect/search/share/page_view',
    `event_time`    VARCHAR(20)  NULL COMMENT '客户端事件时间（毫秒时间戳）',
    `server_time`   VARCHAR(20)  NULL COMMENT '服务端接收时间（毫秒时间戳）',
    `user_id`       VARCHAR(32)  NULL COMMENT '用户ID，未登录为空',
    `device_id`     VARCHAR(32)  NULL COMMENT '设备ID',
    `is_login`      VARCHAR(8)   NULL COMMENT '是否登录 true/false',
    `is_vip`        VARCHAR(8)   NULL COMMENT '是否会员',
    `app_version`   VARCHAR(16)  NULL COMMENT 'APP版本，质检关键维度',
    `os`            VARCHAR(16)  NULL COMMENT '操作系统 android/ios/web/car',
    `os_version`    VARCHAR(16)  NULL COMMENT '系统版本',
    `device_model`  VARCHAR(64)  NULL COMMENT '设备型号',
    `network`       VARCHAR(16)  NULL COMMENT '网络类型 wifi/4g/5g/3g',
    `carrier`       VARCHAR(32)  NULL COMMENT '运营商',
    `ip`            VARCHAR(64)  NULL COMMENT '客户端IP',
    `province`      VARCHAR(32)  NULL COMMENT '省份',
    `city`          VARCHAR(32)  NULL COMMENT '城市',
    `session_id`    VARCHAR(64)  NULL COMMENT '会话ID',
    `session_seq`   VARCHAR(16)  NULL COMMENT '会话内事件序号',
    `is_foreground` VARCHAR(8)   NULL COMMENT '前后台标记',
    `channel`       VARCHAR(32)  NULL COMMENT '曝光来源渠道',
    `page_id`       VARCHAR(32)  NULL COMMENT '事件发生页面',
    `properties`    STRING       NULL COMMENT '事件私有属性 JSON 原文，不解析',
    `etl_time`      DATETIME     NULL COMMENT '入仓时间，用于计算端到端延迟',
    `etl_date`      DATE         NOT NULL COMMENT '分区字段：入仓日期'
)
ENGINE = OLAP
DUPLICATE KEY(`event_id`)
COMMENT 'ODS-音乐APP埋点原始明细（公共属性列 + properties JSON）'
PARTITION BY RANGE(`etl_date`) ()
DISTRIBUTED BY HASH(`event_id`) BUCKETS 8
PROPERTIES (
    "replication_num"          = "1",
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "DAY",
    "dynamic_partition.start"  = "-30",
    "dynamic_partition.end"    = "3",
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.buckets" = "8",
    "dynamic_partition.create_history_partition" = "true"
);

-- -----------------------------------------------------------------------------
-- 建完自检
-- -----------------------------------------------------------------------------
-- SHOW PARTITIONS FROM music_dw.ods_music_event;
-- DESC music_dw.ods_music_event;
-- SELECT COUNT(*) FROM music_dw.ods_music_event;
