#!/usr/bin/env bash
# =============================================================================
# gen_offline_ods.sh —— 在 161 上批量生成离线 ODS 数据（写入 HDFS，ORC 分区目录）
#
# 为什么用 Spark 而不是 Hive：
#   生成 3000 万行只需要 `range()` + 表达式投影，Spark 3.3.1 几分钟就能写完；
#   而 Hive on Spark 要先建中间表再 INSERT，绕过 DDL 反而更慢。
#   这里刻意用 `INSERT OVERWRITE DIRECTORY`（不落 Hive 表），
#   这样 Spark 可以完全绕开 Metastore —— 不依赖 Hive 表结构，也不受
#   Spark 3.3.1 与 Hive 3.1.3 的 metastore 客户端版本兼容性影响。
#   目录建好后由 Hive 侧的外部表（sql/offline/02_ods_music_event.sql）挂上去。
#
# 数据形态刻意贴近真实埋点（答辩可讲）：
#   · 事件分布倾斜：play_start 30% / play_end 30% / skip 12% / 其余长尾
#   · 会话粘性：连续 20 条属同一 session_id，session_seq 0~19
#   · 维度可枚举：10 万用户 / 12 万设备 / 5 城市 / 4 个 APP 版本
#   · 全部用 id 取模生成，同一 id 每次结果一致 —— 可复现，便于对账与回归
#
# 用法：
#   bash code/gen_offline_ods.sh 2026-11-04 30000000
#   缺省：dt=昨天、行数=1000 万
# =============================================================================
set -u

DT=${1:-$(date -d "yesterday" +%F)}
ROWS=${2:-10000000}
SPARK_HOME_33=/opt/module/spark
# 本脚本用 3.3.1（SPARK_HOME 已在 11-03 被注释掉，这里显式指定，避免歧义）
HDFS_DIR="hdfs://WWC160:9820/warehouse/music_dw/ods_music_event/dt=$DT"
BASE_MS=1793678400000   # 2026-11-03 12:00:00 作为事件时间的基准

echo "== 生成离线 ODS：dt=$DT  行数=$ROWS =="
echo "   目标目录：$HDFS_DIR"

cat > /tmp/gen_ods_${DT}.sql <<SQL
INSERT OVERWRITE DIRECTORY '${HDFS_DIR}'
USING ORC
SELECT
  concat('e_', lpad(cast(id as string), 12, '0'))                                   AS event_id,
  CASE WHEN id % 100 < 30 THEN 'play_start'
       WHEN id % 100 < 60 THEN 'play_end'
       WHEN id % 100 < 72 THEN 'skip'
       WHEN id % 100 < 80 THEN 'search'
       WHEN id % 100 < 88 THEN 'collect'
       WHEN id % 100 < 94 THEN 'share'
       ELSE 'page_view' END                                                          AS event_name,
  cast(${BASE_MS} + (id % 86400) * 1000 as string)                                   AS event_time,
  cast(${BASE_MS} + (id % 86400) * 1000 + 1500 as string)                            AS server_time,
  concat('u_', lpad(cast((id % 120000) % 100000 as string), 6, '0'))                 AS user_id,
  concat('d_', lpad(cast(id % 120000 as string), 6, '0'))                            AS device_id,
  CASE WHEN id % 10 < 8 THEN 'true' ELSE 'false' END                                 AS is_login,
  CASE WHEN id % 10 = 0 THEN 'true' ELSE 'false' END                                 AS is_vip,
  element_at(array('8.1.0','8.2.0','8.3.1','9.0.0'), cast(id % 4 as int) + 1)        AS app_version,
  element_at(array('android','android','android','ios','ios','web','car'),
             cast(id % 7 as int) + 1)                                                AS os,
  element_at(array('13','14','12','16'), cast(id % 4 as int) + 1)                    AS os_version,
  element_at(array('MI 13','iPhone 14','Redmi Note 12','Pixel 7','vivo X90','HUAWEI P60'),
             cast(id % 6 as int) + 1)                                                AS device_model,
  element_at(array('wifi','wifi','4g','4g','5g','3g'), cast(id % 6 as int) + 1)      AS network,
  element_at(array('中国移动','中国联通','中国电信'), cast(id % 3 as int) + 1)          AS carrier,
  concat('10.', cast(id % 255 as string), '.', cast((id / 255) % 255 as string),
         '.', cast(id % 200 as string))                                              AS ip,
  element_at(array('江西省','广东省','浙江省','江苏省','四川省'),
             cast(id % 5 as int) + 1)                                                AS province,
  element_at(array('南昌市','深圳市','杭州市','南京市','成都市'),
             cast(id % 5 as int) + 1)                                                AS city,
  concat('s_', lpad(cast(floor(id / 20) as string), 8, '0'))                          AS session_id,
  cast(id % 20 as string)                                                            AS session_seq,
  CASE WHEN id % 100 < 92 THEN 'true' ELSE 'false' END                                AS is_foreground,
  element_at(array('推荐','搜索','歌单','电台','排行榜'), cast(id % 5 as int) + 1)     AS channel,
  element_at(array('home','player','search','playlist','rank'),
             cast(id % 5 as int) + 1)                                                AS page_id,
  concat('{"song_id":"sg_', lpad(cast(id % 5000 as string), 5, '0'),
         '","duration":', cast(id % 300 as string),
         ',"position":', cast(id % 240 as string), '}')                              AS properties,
  concat('${DT}', ' 03:00:00')                                                       AS etl_time
FROM range(0, ${ROWS})
SQL

echo "   生成 SQL：/tmp/gen_ods_${DT}.sql"
"$SPARK_HOME_33/bin/spark-sql" \
  --master yarn --deploy-mode client \
  --conf spark.sql.catalogImplementation=in_memory \
  --conf spark.sql.shuffle.partitions=16 \
  --conf spark.driver.memory=1g \
  --conf spark.executor.memory=1g \
  --conf spark.executor.cores=1 \
  --conf spark.executor.instances=3 \
  -f /tmp/gen_ods_${DT}.sql 2>&1 | grep -vE "^(WARN|SLF4J|log4j)" | tail -12

echo
echo "== 落盘结果 =="
hdfs dfs -du -h "$HDFS_DIR" 2>/dev/null || echo "   目录不存在，生成失败"
hdfs dfs -ls "$HDFS_DIR" 2>/dev/null | head -5
echo
echo "下一步：hive -f sql/offline/02_ods_music_event.sql   （MSCK 登记分区）"
