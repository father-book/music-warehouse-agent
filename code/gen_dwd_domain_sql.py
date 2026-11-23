"""生成离线 DWD 域表建表 + 写入 SQL（docs/19 §四 的 12 张目标态）。

背景：
  现有 DWD 只有 5 张（play_start / play_end / skip / collect / share），
  26 类事件里只有 5 类落到了 DWD，其余 21 类留在 ODS。
  本脚本生成 11_dwd_domain_tables.sql：
    · 新建 9 张域表（session / impression / click / play_control /
      interaction / search / ad / pay / page_view）
    · 接管 dwd_skip（替代 07 号脚本，07 不再进 DAG）
    · DROP 合并掉的 dwd_collect / dwd_share
  dwd_play_start / dwd_play_end 仍由 04 / 03 负责（有派生列，下游指标依赖，不动）。
  合计 12 张，26 类事件全覆盖。

设计取舍（重要）：
  v9 生成器已按 docs/19 事件矩阵补齐 26 类事件的 properties，下面这些私有列
  现在都有真实取值（此前多为 NULL）。每张表仍同时保留两类列：
    1) 按设计口径用 get_json_object 提取的私有列；
    2) properties_raw —— JSON 原文，保证字段扩展时信息不丢。
  表结构从立项起就按目标态设计，补齐属性时无需改表 —— 这正是分层的价值。

金额口径：
  dwd_pay.amount / dwd_ad.bid_price 用 DECIMAL(18,2)，不用 DOUBLE ——
  docs/20 §1.3 的硬要求；下游 12 号脚本的 DWS/ADS 金额列沿用同一类型，
  避免「明细层 DOUBLE 累加、应用层再转 DECIMAL」的假精度。

用法：
  python code/gen_dwd_domain_sql.py
  hive --hivevar start_dt=2026-10-05 --hivevar end_dt=2026-11-04 \
       -f sql/offline/11_dwd_domain_tables.sql
"""
import os

OUT = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                   "sql", "offline", "11_dwd_domain_tables.sql")

# 公共 21 列，与 03/04/07 逐字段一致（批流同源）
COMMON_DDL = [
    ("event_id", "STRING", "事件ID"),
    ("event_ts", "TIMESTAMP", "事件时间"),
    ("server_ts", "TIMESTAMP", "服务端接收时间"),
    ("user_id", "STRING", "用户ID"),
    ("device_id", "STRING", "设备ID"),
    ("is_login", "STRING", "是否登录"),
    ("is_vip", "STRING", "是否会员"),
    ("app_version", "STRING", "APP版本"),
    ("os", "STRING", "操作系统"),
    ("os_version", "STRING", "系统版本"),
    ("device_model", "STRING", "设备型号"),
    ("network", "STRING", "网络类型"),
    ("carrier", "STRING", "运营商"),
    ("ip", "STRING", "客户端IP"),
    ("province", "STRING", "省份"),
    ("city", "STRING", "城市"),
    ("session_id", "STRING", "会话ID"),
    ("session_seq", "STRING", "会话内序号"),
    ("is_foreground", "STRING", "前后台标记"),
    ("channel", "STRING", "渠道"),
    ("page_id", "STRING", "页面ID"),
]

COMMON_SELECT = """    event_id,
    CAST(from_unixtime(CAST(event_time AS BIGINT) DIV 1000) AS TIMESTAMP)  AS event_ts,
    CAST(from_unixtime(CAST(server_time AS BIGINT) DIV 1000) AS TIMESTAMP) AS server_ts,
    user_id, device_id, is_login, is_vip, app_version, os, os_version,
    device_model, network, carrier, ip, province, city,
    session_id, session_seq, is_foreground, channel, page_id"""


def j(key, typ="STRING"):
    """从 properties 取字段；数值类型做 CAST。"""
    expr = "get_json_object(properties, '$.%s')" % key
    if typ != "STRING":
        expr = "CAST(%s AS %s)" % (expr, typ)
    return expr


# (表名, 中文名, 事件列表, 私有列[(列名, 类型, SQL表达式, 注释)])
SPECS = [
    ("dwd_skip", "切歌明细", ["skip"], [
        ("song_id", "STRING", j("song_id"), "跳过歌曲"),
        ("played_duration", "INT", j("position", "INT"), "跳过前已播放秒数"),
        ("skip_type", "STRING", j("skip_type"), "跳过类型 manual/auto（v9 已生成）"),
    ]),
    ("dwd_session", "会话明细", ["app_launch", "app_exit"], [
        ("launch_type", "STRING", j("launch_type"), "冷/热启动"),
        ("source", "STRING", j("source"), "启动来源"),
        ("stay_duration", "BIGINT", j("stay_duration", "BIGINT"), "会话停留秒数"),
        ("exit_type", "STRING", j("exit_type"), "退出方式"),
    ]),
    ("dwd_impression", "曝光明细", ["impression"], [
        ("song_id", "STRING", j("song_id"), "曝光歌曲"),
        ("position", "INT", j("position", "INT"), "坑位 0~24"),
        ("scene", "STRING", j("scene"), "曝光场景"),
        ("is_click", "INT", j("is_click", "INT"), "是否点击（CTR 分子标记）"),
    ]),
    ("dwd_click", "点击明细", ["click"], [
        ("song_id", "STRING", j("song_id"), "点击歌曲"),
        ("target_type", "STRING", j("target_type"), "目标类型"),
        ("target_id", "STRING", j("target_id"), "目标ID"),
        ("position", "INT", j("position", "INT"), "点击坑位"),
        ("page", "STRING", j("page"), "所在页面"),
        ("is_click", "INT", j("is_click", "INT"), "点击标记"),
    ]),
    ("dwd_play_control", "播放控制明细",
     ["pause", "resume", "seek", "play_error"], [
        ("song_id", "STRING", j("song_id"), "歌曲"),
        ("position", "INT", j("position", "INT"), "操作位置（秒）"),
        ("from_pos", "INT", j("from", "INT"), "拖动起点（seek）"),
        ("to_pos", "INT", j("to", "INT"), "拖动终点（seek）"),
        ("direction", "STRING", j("direction"), "拖动方向"),
        ("pause_duration", "INT", j("pause_duration", "INT"), "暂停时长"),
        ("error_type", "STRING", j("error_type"), "播放错误类型"),
    ]),
    ("dwd_interaction", "互动域统一明细",
     ["collect", "like", "share", "comment", "download",
      "follow_artist", "playlist_add"], [
        ("song_id", "STRING", j("song_id"), "歌曲"),
        ("action", "STRING", j("action"), "add/remove 等动作"),
        ("share_channel", "STRING", j("channel"), "分享渠道"),
        ("comment_len", "INT", j("comment_len", "INT"), "评论长度"),
        ("quality", "STRING", j("quality"), "下载音质"),
        ("artist_id", "STRING", j("artist_id"), "歌手ID"),
        ("playlist_id", "STRING", j("playlist_id"), "歌单ID"),
    ]),
    ("dwd_search", "搜索域明细", ["search", "search_click"], [
        ("keyword", "STRING", j("keyword"), "搜索词"),
        ("search_type", "STRING", j("search_type"), "hot/suggest/input"),
        ("result_cnt", "INT", j("result_cnt", "INT"), "结果数（零结果率）"),
        ("result_type", "STRING", j("result_type"), "结果类型"),
        ("result_id", "STRING", j("result_id"), "结果ID"),
        ("position", "INT", j("position", "INT"), "结果位置"),
    ]),
    ("dwd_ad", "广告域明细", ["ad_impression", "ad_click"], [
        ("ad_id", "STRING", j("ad_id"), "广告ID"),
        ("ad_type", "STRING", j("ad_type"), "广告形式"),
        ("position", "INT", j("position", "INT"), "广告位"),
        ("bid_price", "DECIMAL(18,2)", j("bid_price", "DECIMAL(18,2)"), "出价（eCPM 估算，金额口径 DECIMAL）"),
    ]),
    ("dwd_pay", "商业化域明细",
     ["vip_subscribe", "order_pay", "order_refund"], [
        ("sku_id", "STRING", j("sku_id"), "商品SKU"),
        ("amount", "DECIMAL(18,2)", j("amount", "DECIMAL(18,2)"), "金额（DECIMAL，docs/20 §1.3）"),
        ("pay_channel", "STRING", j("pay_channel"), "支付渠道"),
        ("order_id", "STRING", j("order_id"), "订单号"),
        ("reason", "STRING", j("reason"), "退款原因"),
    ]),
    ("dwd_page_view", "页面浏览明细", ["page_view"], [
        ("page", "STRING", j("page"), "页面"),
        ("from_page", "STRING", j("from_page"), "来源页面"),
        ("stay_duration", "INT", j("stay_duration", "INT"), "停留秒数"),
    ]),
]

HEADER = """-- =============================================================================
-- 11_dwd_domain_tables.sql —— 离线 DWD 域表（docs/19 §四 的 12 张目标态）
-- 由 code/gen_dwd_domain_sql.py 生成，不要手改本文件，改生成器后重跑。
--
-- 执行：
--   hive --hivevar start_dt=2026-10-05 --hivevar end_dt=2026-11-04 \\
--        -f 11_dwd_domain_tables.sql
--
-- 本文件负责 10 张表：9 张新域表 + 接管 dwd_skip（替代 07 号脚本）。
-- dwd_play_start / dwd_play_end 仍由 04 / 03 负责（含派生列，下游指标依赖）。
-- 12 张表覆盖 26 类事件，无遗漏、无重叠。
--
-- 事件名以 config/event_weights.json 为准：docs/19 §四 把 dwd_pay 写成
-- vip_expose/vip_click/pay_success，与事件矩阵及生成器不一致，此处按实际
-- 的 vip_subscribe/order_pay/order_refund 落地（文档已加勘误）。
--
-- 每张表都保留 properties_raw（JSON 原文）：表结构按 docs/19 目标态设计，
-- v9 生成器已补齐 26 类事件的私有属性，此前多为 NULL 的私有列现在均有取值。
-- =============================================================================

SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;
SET hive.exec.max.dynamic.partitions = 200;
SET hive.exec.max.dynamic.partitions.pernode = 100;
SET hive.stats.autogather = false;

"""


def ddl(table, cn, cols):
    # 先 DROP 再建：CREATE IF NOT EXISTS 对已存在的表不会改结构，
    # 接管 dwd_skip 时旧表 26 列 vs 新查询 28 列会直接 SemanticException。
    # 外部表 DROP 只删元数据；随后 INSERT OVERWRITE 会重写全部 31 个分区，
    # 因此整份脚本幂等可重放。
    lines = ["DROP TABLE IF EXISTS music_dw.%s;" % table,
             "CREATE EXTERNAL TABLE IF NOT EXISTS music_dw.%s (" % table]
    body = ["    %-18s %-10s COMMENT '%s'" % (n, t, c) for n, t, c in COMMON_DDL]
    body.append("    %-18s %-10s COMMENT '%s'" % ("event_name", "STRING", "域内事件名"))
    body += ["    %-18s %-10s COMMENT '%s'" % (n, t, c) for n, t, _, c in cols]
    body.append("    %-18s %-10s COMMENT '%s'" %
                ("properties_raw", "STRING", "事件私有属性 JSON 原文（保真）"))
    body.append("    %-18s %-10s COMMENT '%s'" % ("etl_time", "TIMESTAMP", "入仓时间"))
    lines.append(",\n".join(body))
    lines.append(")")
    lines.append("COMMENT 'DWD-%s（离线）'" % cn)
    lines.append("PARTITIONED BY (dt STRING)")
    lines.append("STORED AS ORC")
    lines.append("LOCATION 'hdfs://WWC160:9820/warehouse/music_dw/%s'" % table)
    lines.append("TBLPROPERTIES ('orc.compress' = 'SNAPPY');")
    return "\n".join(lines)


def insert(table, events, cols):
    ev = ", ".join("'%s'" % e for e in events)
    exprs = ",\n".join("    %s AS %s" % (e, n) for n, _, e, _ in cols)
    return """INSERT OVERWRITE TABLE music_dw.{t} PARTITION (dt)
SELECT
{common},
    event_name,
{exprs},
    properties                              AS properties_raw,
    CAST(etl_time AS TIMESTAMP)             AS etl_time,
    dt
FROM music_dw.ods_music_event
WHERE dt BETWEEN '${{start_dt}}' AND '${{end_dt}}'
  AND event_name IN ({ev})
DISTRIBUTE BY dt;""".format(t=table, common=COMMON_SELECT, exprs=exprs, ev=ev)


def main():
    parts = [HEADER]
    for table, cn, events, cols in SPECS:
        parts.append("-- %s: %s\n-- 事件: %s" % (table, cn, ", ".join(events)))
        parts.append(ddl(table, cn, cols))
        parts.append("")
        parts.append(insert(table, events, cols))
        parts.append("\n")

    parts.append("""-- ===========================================================================
-- 合并收口：collect / share 已并入 dwd_interaction，删掉两张旧表
-- 下游 05/08/09 未引用它们（已 grep 确认），外部表 DROP 只删元数据不删 HDFS。
-- 07 号脚本自此不再进 DAG（它是历史脚本，保留但不执行）。
-- ===========================================================================
DROP TABLE IF EXISTS music_dw.dwd_collect;
DROP TABLE IF EXISTS music_dw.dwd_share;

-- 验收：10 张新表逐表行数
""")
    for table, _, _, _ in SPECS:
        parts.append("SELECT '%s' AS tbl, count(*) AS cnt FROM music_dw.%s;" % (table, table))
    parts.append("")
    parts.append("SHOW TABLES IN music_dw LIKE 'dwd_*';\n")

    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    with open(OUT, "w", encoding="utf-8", newline="\n") as f:
        f.write("\n".join(parts))
    print("生成:", OUT)
    print("表数:", len(SPECS))
    print("事件覆盖:", sum(len(s[2]) for s in SPECS), "类（另有 play_start/play_end 由 03/04 覆盖）")


if __name__ == "__main__":
    main()
