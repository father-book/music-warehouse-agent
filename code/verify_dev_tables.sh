#!/bin/bash
# 验证 music_dev 空壳表：数量、LOCATION 归属、行数全 0
# 用法：bash verify_dev_tables.sh
set -u
HIVE=hive
FILTER='^[0-9]{4}-|INFO|WARN|tab_name|^$|Logging|SLF4J|log4j'

echo "===== 1. 表清单 ====="
$HIVE -e 'SHOW TABLES IN music_dev;' 2>/dev/null | grep -vE "$FILTER" | grep -v '^dev_' > /tmp/dev_tbls.txt
# 上一层过滤可能误伤，重新取一次（表名以 dim_/dwd_/dws_/ads_ 开头）
$HIVE -e 'SHOW TABLES IN music_dev;' 2>/dev/null | grep -vE "$FILTER" | grep -E '^(ods|dim|dwd|dws|ads)_' > /tmp/dev_tbls.txt
N=$(wc -l < /tmp/dev_tbls.txt)
echo "dev 表数 = $N"

echo "===== 2. LOCATION 归属 ====="
> /tmp/desc_all.sql
while read -r t; do
  echo "DESCRIBE FORMATTED music_dev.$t;" >> /tmp/desc_all.sql
done < /tmp/dev_tbls.txt
$HIVE --hiveconf hive.cli.errors.ignore=true -f /tmp/desc_all.sql 2>/dev/null \
  | grep -E '^Location:' | awk '{print $2}' > /tmp/dev_locs.txt
echo "取到 LOCATION 条数 = $(wc -l < /tmp/dev_locs.txt)"
GOOD=$(grep -c '/warehouse/music_dev/' /tmp/dev_locs.txt || true)
BAD=$(grep -vc '/warehouse/music_dev/' /tmp/dev_locs.txt || true)
echo "指向 music_dev = $GOOD"
echo "指向他处     = $BAD"
if [ "$BAD" -gt 0 ]; then
  echo "!! 异常路径如下："
  grep -v '/warehouse/music_dev/' /tmp/dev_locs.txt
fi

echo "===== 3. 行数（应全为 0）====="
Q=""
while read -r t; do
  if [ -z "$Q" ]; then
    Q="SELECT '$t' AS tbl, COUNT(*) AS cnt FROM music_dev.$t"
  else
    Q="$Q UNION ALL SELECT '$t', COUNT(*) FROM music_dev.$t"
  fi
done < /tmp/dev_tbls.txt
echo "$Q;" > /tmp/dev_cnt.sql
$HIVE -f /tmp/dev_cnt.sql 2>/dev/null | grep -E "^[a-z_0-9]+[[:space:]]+[0-9]+$" > /tmp/dev_cnt.txt
echo "返回行数统计的表 = $(wc -l < /tmp/dev_cnt.txt)"
echo "非零行数的表："
awk '$2 != 0 {print "  !! " $0}' /tmp/dev_cnt.txt
if [ -z "$(awk '$2 != 0' /tmp/dev_cnt.txt)" ]; then
  echo "  （无，全部为 0）"
fi
echo
echo "总行数 = $(awk '{s+=$2} END {print s+0}' /tmp/dev_cnt.txt)"
