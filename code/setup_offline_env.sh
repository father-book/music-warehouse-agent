#!/usr/bin/env bash
# =============================================================================
# setup_offline_env.sh —— 二期离线数仓的引擎环境准备（在 WWC161 上执行）
#
# 背景：集群是 Spark 3.3.1 + Hive 3.1.3 + JDK8。
#       Hive 3.1 只支持到 Spark 2.4，所以 Hive on Spark 不可用（ADR-024），
#       改走「Spark SQL 直读 Hive Metastore」。
#
# 本脚本做三件事（幂等，可重复执行）：
#   1. 把 hive.execution.engine 从 spark 改回 mr —— 当前是 spark，
#      与 Spark 3.3.1 不匹配，任何 Hive CLI 的 INSERT/SELECT 都会失败
#   2. 启动 Hive Metastore（9083），Spark SQL 靠它读 Hive 表元数据
#   3. 写 spark-defaults.conf，钉住 catalog 与 Metastore 版本
#
# 用法：bash setup_offline_env.sh
# =============================================================================
set -u

HIVE_HOME=/opt/module/hive
SPARK_HOME=/opt/module/spark
HIVE_CONF=$HIVE_HOME/conf/hive-site.xml
SPARK_CONF=$SPARK_HOME/conf/spark-defaults.conf
TS=$(date +%m%d%H%M)

ok()  { echo -e "  \033[32m✓\033[0m $*"; }
ng()  { echo -e "  \033[31m✗\033[0m $*"; }
log() { echo -e "\n[$TS] $*"; }

# ---------------------------------------------------------------- 0. 探测目录
log "== 0/3 探测安装目录 =="
[ -d "$SPARK_HOME" ] || SPARK_HOME=$(ls -d /opt/module/spark* 2>/dev/null | head -1)
[ -d "$HIVE_HOME" ]  || HIVE_HOME=$(ls -d /opt/module/hive* 2>/dev/null | head -1)
echo "     HIVE_HOME=$HIVE_HOME"
echo "     SPARK_HOME=$SPARK_HOME"
[ -f "$HIVE_CONF" ] || { ng "找不到 $HIVE_CONF，退出"; exit 1; }

# ------------------------------------------------- 1. execution.engine -> mr
log "== 1/3 修正 hive.execution.engine（spark → mr）=="
CUR=$(grep -A1 "<name>hive.execution.engine</name>" "$HIVE_CONF" \
      | grep -o '<value>[^<]*</value>' | head -1 | sed 's/<[^>]*>//g')
echo "     当前值：${CUR:-未配置}"

if [ "$CUR" = "mr" ]; then
    ok "已经是 mr，跳过"
elif [ -z "$CUR" ]; then
    # 没配置就用默认值 mr，不用改
    ok "未显式配置（Hive 默认 mr），无需修改"
else
    cp "$HIVE_CONF" "$HIVE_CONF.bak.$TS"
    # 只在 <name>hive.execution.engine</name> 到下一个 <value> 之间替换，
    # 避免误伤其他 property 的 <value>spark</value>
    sed -i "/<name>hive.execution.engine<\/name>/,/<value>/ s|<value>spark</value>|<value>mr</value>|" "$HIVE_CONF"
    NEW=$(grep -A1 "<name>hive.execution.engine</name>" "$HIVE_CONF" \
          | grep -o '<value>[^<]*</value>' | head -1 | sed 's/<[^>]*>//g')
    if [ "$NEW" = "mr" ]; then
        ok "已改为 mr（备份：$HIVE_CONF.bak.$TS）"
    else
        ng "改失败，仍为 $NEW。手工改：$HIVE_CONF"
        echo "     找 <name>hive.execution.engine</name> 下一行的 <value>，改成 mr"
    fi
fi

# ------------------------------------------------------- 2. 启动 Metastore
log "== 2/3 启动 Hive Metastore（9083）=="
if ss -tnlp 2>/dev/null | grep -q ":9083"; then
    ok "已经在运行"
else
    nohup "$HIVE_HOME/bin/hive" --service metastore > /tmp/hive-metastore.log 2>&1 &
    echo "     等待启动（最多 120 秒）..."
    for i in $(seq 1 120); do
        ss -tnlp 2>/dev/null | grep -q ":9083" && break
        sleep 1
    done
    if ss -tnlp 2>/dev/null | grep -q ":9083"; then
        ok "Metastore 已监听 9083"
    else
        ng "Metastore 没起来，看日志：tail -40 /tmp/hive-metastore.log"
        echo "     常见原因：MySQL 元数据库没起、或 hive-site.xml 里连接串不对"
    fi
fi

# --------------------------------------------------- 3. spark-defaults.conf
log "== 3/3 写 Spark 配置（钉住 catalog 与 Metastore 版本）=="
if [ ! -f "$SPARK_CONF" ]; then
    touch "$SPARK_CONF"
else
    cp "$SPARK_CONF" "$SPARK_CONF.bak.$TS"
fi

# 先删掉可能存在的同名旧键，再追加，保证幂等
for k in spark.sql.catalogImplementation spark.hadoop.hive.metastore.uris \
         spark.sql.hive.metastore.version spark.sql.hive.metastore.jars; do
    sed -i "/^${k}[[:space:]]/d" "$SPARK_CONF"
done

cat >> "$SPARK_CONF" <<EOF

# ---- 二期离线数仓（$(date +%F) 由 setup_offline_env.sh 写入）----
# Spark 3.3.1 自带 Hive 2.3.9 客户端，直连 Hive 3.1.3 的 Metastore 会因
# schema 版本不匹配报错，所以显式指定用 Hive 自己的 jar（隔离类加载）
spark.sql.catalogImplementation   hive
spark.hadoop.hive.metastore.uris  thrift://WWC161:9083
spark.sql.hive.metastore.version  3.1.3
spark.sql.hive.metastore.jars     /opt/module/hive/lib/*
EOF
ok "已写入 $SPARK_CONF（备份：$SPARK_CONF.bak.$TS）"

# ---------------------------------------------------------------- 验证
log "== 验证 =="
echo "     —— 当前配置 ——"
grep -v '^#' "$SPARK_CONF" | grep -v '^$' | sed 's/^/     /'
echo
echo "     —— Spark 能否看到 Hive 库表 ——"
"$SPARK_HOME/bin/spark-sql" --master local[1] -e "show databases;" 2>&1 \
    | grep -vE "^(WARN|INFO|Using Spark|SLF4J|log4j)" | head -12 | sed 's/^/     /'

echo
echo "  如果上面能列出 default 等库，说明 Spark ↔ Metastore 通了。"
echo "  若报 schema version 不匹配：把 spark.sql.hive.metastore.* 两行注释掉再试"
echo "  （退回内置 2.3.9 客户端，通常也能连 3.1.3 的 Metastore）。"
