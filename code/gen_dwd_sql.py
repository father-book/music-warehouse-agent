"""生成 DWD 层 7 张明细表的建表与清洗 SQL。

用模板避免手写重复出错，也方便后续调整规则后一键重生成。
输出：
    sql/dwd/generated_tables.sql   建表（6 张新表，play_end 已有）
    sql/dwd/generated_clean.sql    统一清洗（公共规则 + 各事件规则 + 7 张明细）

用法：python code/gen_dwd_sql.py
"""
import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUT_DIR = os.path.join(BASE_DIR, "sql", "dwd")

# 公共属性列：DWD 各表共用，不含 event_name（表已按类型拆分）
COMMON_COLS = [
    ("event_id", "VARCHAR(64)"), ("event_ts", "DATETIME"), ("server_ts", "DATETIME"),
    ("user_id", "VARCHAR(32)"), ("device_id", "VARCHAR(32)"),
    ("is_login", "VARCHAR(8)"), ("is_vip", "VARCHAR(8)"),
    ("app_version", "VARCHAR(16)"), ("os", "VARCHAR(16)"), ("os_version", "VARCHAR(16)"),
    ("device_model", "VARCHAR(64)"), ("network", "VARCHAR(16)"), ("carrier", "VARCHAR(32)"),
    ("ip", "VARCHAR(64)"), ("province", "VARCHAR(32)"), ("city", "VARCHAR(32)"),
    ("session_id", "VARCHAR(64)"), ("session_seq", "VARCHAR(16)"),
    ("is_foreground", "VARCHAR(8)"), ("channel", "VARCHAR(32)"), ("page_id", "VARCHAR(32)"),
]

# 各表配置：私有字段 (列名, 类型, JSON路径, 转换方式)、事件级必填规则
TABLES = [
    {
        "table": "dwd_play_start", "event": "play_start", "comment": "播放开始明细",
        "props": [("song_id", "VARCHAR(32)", "$.song_id", "str"),
                  ("playlist_id", "VARCHAR(32)", "$.playlist_id", "str"),
                  ("quality", "VARCHAR(16)", "$.quality", "str"),
                  ("play_mode", "VARCHAR(16)", "$.play_mode", "str"),
                  ("song_index", "INT", "$.song_index", "int")],
        "required": ["song_id"],
        "rules": [],
    },
    {
        "table": "dwd_skip", "event": "skip", "comment": "切歌明细",
        "props": [("song_id", "VARCHAR(32)", "$.song_id", "str"),
                  ("played_duration", "INT", "$.played_duration", "int"),
                  ("skip_type", "VARCHAR(16)", "$.skip_type", "str")],
        "required": ["song_id", "played_duration"],
        "rules": [("played_duration_negative", "p_played_duration < 0",
                   "played_duration<0")],
    },
    {
        "table": "dwd_search", "event": "search", "comment": "搜索明细",
        "props": [("keyword", "VARCHAR(128)", "$.keyword", "str"),
                  ("search_type", "VARCHAR(32)", "$.search_type", "str"),
                  ("result_count", "INT", "$.result_count", "int"),
                  ("clicked_song_id", "VARCHAR(32)", "$.clicked_song_id", "str")],
        "required": ["keyword"],
        "rules": [("result_count_negative", "p_result_count < 0", "result_count<0")],
    },
    {
        "table": "dwd_collect", "event": "collect", "comment": "收藏明细",
        "props": [("song_id", "VARCHAR(32)", "$.song_id", "str"),
                  ("collect_type", "VARCHAR(16)", "$.collect_type", "str")],
        "required": ["song_id", "collect_type"],
        "rules": [("collect_type_invalid",
                   "p_collect_type NOT IN ('add','cancel')", "collect_type 非 add/cancel")],
    },
    {
        "table": "dwd_share", "event": "share", "comment": "分享明细",
        "props": [("song_id", "VARCHAR(32)", "$.song_id", "str"),
                  ("share_channel", "VARCHAR(32)", "$.share_channel", "str")],
        "required": ["song_id", "share_channel"],
        "rules": [],
    },
    {
        "table": "dwd_page_view", "event": "page_view", "comment": "页面浏览明细",
        # properties.page_id 与公共属性 page_id 同名，改名为 view_page_id 避免冲突
        "props": [("view_page_id", "VARCHAR(32)", "$.page_id", "str"),
                  ("referrer_page", "VARCHAR(32)", "$.referrer_page", "str"),
                  ("stay_duration", "INT", "$.stay_duration", "int")],
        "required": ["view_page_id"],
        "rules": [("stay_duration_negative", "p_stay_duration < 0", "stay_duration<0")],
    },
]

# 公共规则通过条件（7 类事件通用）
COMMON_OK = """  AND (t.event_id IS NOT NULL AND t.event_id <> '')
  AND (COALESCE(t.device_id, '') <> '')
  AND NOT (t.is_login = 'true' AND COALESCE(t.user_id, '') = '')
  AND t.event_time REGEXP '^[0-9]{13}$'
  AND CAST(t.event_time AS BIGINT) <= UNIX_TIMESTAMP(NOW()) * 1000 + 300000
  AND CAST(t.event_time AS BIGINT) >= 1700000000000"""

CAST = {"str": "get_json_string(properties, '{}')",
        "int": "CAST(get_json_string(properties, '{}') AS INT)",
        "double": "CAST(get_json_string(properties, '{}') AS DOUBLE)"}


def json_expr(path, kind="str"):
    return CAST[kind].format(path)


def gen_ddl():
    out = ["-- 自动生成：DWD 明细表建表语句（6 张，dwd_play_end 见 doris_dwd_tables.sql）",
           "-- 生成器：code/gen_dwd_sql.py", "USE music_dw;", ""]
    for cfg in TABLES:
        cols = ["    event_id        VARCHAR(64)  NOT NULL COMMENT '事件ID'",
                "    event_ts        DATETIME     NOT NULL COMMENT '事件时间'",
                "    server_ts       DATETIME     NULL     COMMENT '服务端接收时间'"]
        for name, typ in COMMON_COLS[3:]:
            cols.append(f"    {name:<15} {typ:<12} NULL")
        for pname, ptyp, ppath, pkind in cfg["props"]:
            cols.append(f"    {pname:<15} {ptyp:<12} NULL     COMMENT '{ppath}'")
        cols.append("    etl_time        DATETIME     NULL     COMMENT '入仓时间'")
        cols.append("    dt              DATE         NOT NULL COMMENT '事件日期，分区键'")
        out.append(f"""
-- {cfg['comment']}
CREATE TABLE IF NOT EXISTS {cfg['table']} (
{",".join(chr(10) + c for c in cols)[1:]}
) ENGINE=OLAP
DUPLICATE KEY(event_id, event_ts)
COMMENT 'DWD-{cfg['comment']}'
PARTITION BY RANGE(dt) ()
DISTRIBUTED BY HASH(event_id) BUCKETS 8
PROPERTIES (
    "replication_allocation" = "tag.location.default: 1",
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "DAY",
    "dynamic_partition.start" = "-365",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.buckets" = "8"
);
""")
    return "\n".join(out)


def gen_event_name_error():
    """事件名非法：不在 7 类白名单内，无法归属任何明细表，单独汇总到异常表。"""
    return """
-- =============================================================================
-- Part 0  事件名非法（不在白名单）
-- =============================================================================
-- 这类数据无法归属任何 DWD 明细表，但必须被记录——
-- 否则 ODS 总行数会比「DWD 合计 + 异常合计」多出一截，对账时无法解释。
INSERT INTO dwd_quality_error
SELECT event_id,
       'event_name_invalid'                       AS rule_name,
       event_name,
       'block'                                    AS rule_level,
       CONCAT('event_name=', COALESCE(event_name, 'NULL')) AS detail,
       get_json_string(properties, '$.song_id'),
       user_id, device_id,
       CASE WHEN event_time REGEXP '^[0-9]{13}$'
            THEN FROM_UNIXTIME(CAST(event_time AS BIGINT)/1000) ELSE NULL END,
       properties, etl_time, CURDATE()
FROM ods_music_event
WHERE event_name IS NULL
   OR event_name NOT IN ('play_start','play_end','skip','search','collect','share','page_view');
"""


def gen_common_error():
    """公共规则异常：覆盖 7 类事件，一条数据只记一行。"""
    return """
-- =============================================================================
-- Part 1  公共规则异常（7 类事件通用）
-- =============================================================================
INSERT INTO dwd_quality_error
SELECT t.event_id, t.rule_name, t.event_name, 'block', t.detail,
       get_json_string(t.properties, '$.song_id'), t.user_id, t.device_id,
       t.event_ts, t.properties, t.etl_time, CURDATE()
FROM (
    SELECT event_id, event_name, user_id, device_id, is_login, event_time,
           properties, etl_time,
           CASE WHEN event_time REGEXP '^[0-9]{13}$'
                THEN FROM_UNIXTIME(CAST(event_time AS BIGINT)/1000) ELSE NULL END AS event_ts,
           CASE
               WHEN event_id IS NULL OR event_id = '' THEN 'event_id_missing'
               WHEN COALESCE(device_id,'') = ''      THEN 'device_id_missing'
               WHEN is_login = 'true' AND COALESCE(user_id,'') = ''
                                                     THEN 'user_id_missing_when_login'
               WHEN event_time IS NULL OR event_time NOT REGEXP '^[0-9]{13}$'
                                                     THEN 'event_time_missing'
               WHEN CAST(event_time AS BIGINT) > UNIX_TIMESTAMP(NOW())*1000 + 300000
                                                     THEN 'event_time_future'
               WHEN CAST(event_time AS BIGINT) < 1700000000000
                                                     THEN 'event_time_too_old'
               ELSE NULL END AS rule_name,
           CASE
               WHEN event_id IS NULL OR event_id = '' THEN 'event_id 为空'
               WHEN COALESCE(device_id,'') = ''      THEN 'device_id 为空（设备不可识别）'
               WHEN is_login = 'true' AND COALESCE(user_id,'') = ''
                                                     THEN 'is_login=true 但 user_id 为空'
               WHEN event_time IS NULL OR event_time NOT REGEXP '^[0-9]{13}$'
                                                     THEN CONCAT('event_time=', COALESCE(event_time,'NULL'))
               WHEN CAST(event_time AS BIGINT) > UNIX_TIMESTAMP(NOW())*1000 + 300000
                                                     THEN CONCAT('event_time 超前: ', event_time)
               WHEN CAST(event_time AS BIGINT) < 1700000000000
                                                     THEN CONCAT('event_time 过早: ', event_time)
               ELSE '' END AS detail
    FROM ods_music_event
    WHERE event_name IN ('play_start','skip','search','collect','share','page_view')
) t
WHERE t.rule_name IS NOT NULL;
"""


def gen_event_error(cfg):
    """事件属性异常：只处理公共规则已通过的数据，保证一条数据只命中一类。"""
    if not cfg["required"] and not cfg["rules"]:
        return ""
    exprs = ",\n".join(
        f"        {json_expr(p, k)} AS p_{n}" for n, _, p, k in cfg["props"])
    req_cond = " OR ".join(
        f"t.p_{r} IS NULL" + (f" OR t.p_{r} = ''" if _is_str(cfg, r) else "")
        for r in cfg["required"])
    req_rule = "".join(
        f"\n               WHEN t.p_{r} IS NULL" + (f" OR t.p_{r} = ''" if _is_str(cfg, r) else "") +
        f" THEN '{r}_missing'" for r in cfg["required"])
    req_detail = "".join(
        f"\n               WHEN t.p_{r} IS NULL" + (f" OR t.p_{r} = ''" if _is_str(cfg, r) else "") +
        f" THEN '{r} 缺失'" for r in cfg["required"])
    extra_rule = "".join(f"\n               WHEN {c} THEN '{n}'" for n, c, _ in cfg["rules"])
    extra_detail = "".join(f"\n               WHEN {c} THEN '{d}'" for _, c, d in cfg["rules"])
    extra_cond = (" OR " + " OR ".join(f"({c})" for _, c, _ in cfg["rules"])) if cfg["rules"] else ""

    return f"""
-- -----------------------------------------------------------------------------
-- {cfg['event']} 事件属性异常
-- -----------------------------------------------------------------------------
INSERT INTO dwd_quality_error
SELECT t.event_id,
       CASE{req_rule}{extra_rule}
           ELSE NULL END AS rule_name,
       t.event_name, 'block',
       CASE{req_detail}{extra_detail}
           ELSE '' END   AS detail,
       get_json_string(t.properties, '$.song_id'), t.user_id, t.device_id,
       t.event_ts, t.properties, t.etl_time, CURDATE()
FROM (
    SELECT u.*,
{exprs}
    FROM (
        SELECT event_id, event_name, user_id, device_id, is_login, event_time,
               properties, etl_time,
               CASE WHEN event_time REGEXP '^[0-9]{{13}}$'
                    THEN FROM_UNIXTIME(CAST(event_time AS BIGINT)/1000) ELSE NULL END AS event_ts
        FROM ods_music_event
        WHERE event_name = '{cfg['event']}'
    ) u
) t
WHERE (t.event_id IS NOT NULL AND t.event_id <> '')
  AND (COALESCE(t.device_id,'') <> '')
  AND NOT (t.is_login = 'true' AND COALESCE(t.user_id,'') = '')
  AND t.event_time REGEXP '^[0-9]{{13}}$'
  AND CAST(t.event_time AS BIGINT) <= UNIX_TIMESTAMP(NOW())*1000 + 300000
  AND CAST(t.event_time AS BIGINT) >= 1700000000000
  AND ({req_cond}{extra_cond});
"""


def _is_str(cfg, name):
    return any(n == name and k == "str" for n, _, _, k in cfg["props"])


def gen_dwd_insert(cfg):
    """明细写入：公共规则 + 事件规则全部通过。"""
    common = ",\n".join(f"    t.{c}" for c, _ in COMMON_COLS[:2])
    rest = ",\n".join(f"    t.{c}" for c, _ in COMMON_COLS[3:])
    props = ",\n".join(f"    t.p_{n}" for n, _, _, _ in cfg["props"])
    exprs = ",\n".join(f"        {json_expr(p, k)} AS p_{n}" for n, _, p, k in cfg["props"])
    req_ok = "\n  AND ".join(
        f"t.p_{r} IS NOT NULL" + (f" AND t.p_{r} <> ''" if _is_str(cfg, r) else "")
        for r in cfg["required"])
    rules_ok = "".join(f"\n  AND NOT ({c})" for _, c, _ in cfg["rules"])

    return f"""
-- -----------------------------------------------------------------------------
-- {cfg['table']} 明细写入
-- -----------------------------------------------------------------------------
INSERT INTO {cfg['table']}
SELECT
{common},
    COALESCE(t.server_ts, t.event_ts),
{rest},
{props},
    t.etl_time,
    DATE(t.event_ts)
FROM (
    SELECT u.*,
{exprs}
    FROM (
        SELECT event_id, event_name, user_id, device_id,
               is_login, is_vip, app_version, os, os_version, device_model,
               network, carrier, ip, province, city, session_id, session_seq,
               is_foreground, channel, page_id, properties, etl_time, event_time,
               CASE WHEN event_time REGEXP '^[0-9]{{13}}$'
                    THEN FROM_UNIXTIME(CAST(event_time AS BIGINT)/1000) ELSE NULL END AS event_ts,
               CASE WHEN server_time REGEXP '^[0-9]{{13}}$'
                    THEN FROM_UNIXTIME(CAST(server_time AS BIGINT)/1000) ELSE NULL END AS server_ts
        FROM ods_music_event
        WHERE event_name = '{cfg['event']}'
    ) u
) t
WHERE (t.event_id IS NOT NULL AND t.event_id <> '')
  AND (COALESCE(t.device_id,'') <> '')
  AND NOT (t.is_login = 'true' AND COALESCE(t.user_id,'') = '')
  AND t.event_time REGEXP '^[0-9]{{13}}$'
  AND CAST(t.event_time AS BIGINT) <= UNIX_TIMESTAMP(NOW())*1000 + 300000
  AND CAST(t.event_time AS BIGINT) >= 1700000000000
  AND {req_ok}{rules_ok};
"""


def main():
    os.makedirs(OUT_DIR, exist_ok=True)
    ddl = gen_ddl()
    open(os.path.join(OUT_DIR, "generated_tables.sql"), "w", encoding="utf-8").write(ddl)

    parts = ["-- 自动生成：DWD 清洗（公共规则 + 6 类事件 + 6 张明细）",
             "-- 注意：dwd_play_end 的清洗见 doris_dwd_play_end_clean.sql",
             "USE music_dw;"]
    parts.append(gen_event_name_error())
    parts.append(gen_common_error())
    for cfg in TABLES:
        parts.append(gen_event_error(cfg))
    for cfg in TABLES:
        parts.append(gen_dwd_insert(cfg))
    clean = "\n".join(parts)
    open(os.path.join(OUT_DIR, "generated_clean.sql"), "w", encoding="utf-8").write(clean)

    print(f"已生成：{len(TABLES)} 张表 DDL，{len(TABLES)} 段事件清洗")
    print(f"  {OUT_DIR}/generated_tables.sql  {len(ddl)} 字符")
    print(f"  {OUT_DIR}/generated_clean.sql   {len(clean)} 字符")


if __name__ == "__main__":
    main()
