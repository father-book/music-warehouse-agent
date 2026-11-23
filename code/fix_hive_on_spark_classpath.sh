#!/usr/bin/env bash
# =============================================================================
# fix_hive_on_spark_classpath.sh —— 固化 11-03 调通 Hive on Spark 时踩到的
# 全部 classpath / 配置修正（在 WWC161 执行，幂等可重复跑）
#
# 背景：Hive 3.1.3 + Spark 2.4.8 不是官方验证过的组合，需要人工清理三类冲突：
#   1. Spark 自带的 Hive 1.2.1 遮挡 Hive 3.1.3  → NoSuchFieldError: SPARK_RPC_SERVER_ADDRESS
#   2. Spark 自带的 ORC 是 -nohive 裁剪版      → NoSuchMethodError: createRowBatch
#   3. executor 缺 Hive 3.1.3 的类            → NoClassDefFoundError: ColumnVector
#   4. hdfs-default 里 dfs.client.datanode-restart.timeout 默认 "30s"，getLong 不认
#   5. hive-druid-handler fat jar 内嵌 lz4-java 1.3 → NoSuchMethodError:
#      LZ4BlockInputStream.<init>(InputStream, boolean)（11-07 v9 重造后暴露）
#
# 详细复盘见 docs/15-Hive-on-Spark落地记录.md
#
# 用法：bash fix_hive_on_spark_classpath.sh
# =============================================================================
set -u

SPARK248=/opt/module/spark-2.4.8
HIVE_HOME=/opt/module/hive
AUXLIB=$HIVE_HOME/auxlib
HIVENV=$HIVE_HOME/conf/hive-env.sh
DISABLED=$SPARK248/jars-disabled
HDFS_JARS=/spark-jars
TS=$(date +%m%d%H%M)

ok()  { echo -e "  \033[32m✓\033[0m $*"; }
ng()  { echo -e "  \033[31m✗\033[0m $*"; }
log() { echo -e "\n[$TS] $*"; }

# ---------------------------------------------------------------- 1. hive-env
log "== 1/5 hive-env.sh 固定 SPARK_HOME = 2.4.8 =="
# 为什么要写死在 hive-env.sh：环境变量是继承的，用户 shell 里若有 SPARK_HOME=spark(3.3.1)
# 会被 Hive 拉起的 spark-submit 继承，变成"client 3.3.1 / AM 2.4.8"，报 --dist-cache-conf 不支持。
[ -f "$HIVENV" ] || touch "$HIVENV"
if grep -q "^export SPARK_HOME=$SPARK248$" "$HIVENV" 2>/dev/null; then
    ok "已固定，跳过"
else
    cp "$HIVENV" "$HIVENV.bak.$TS" 2>/dev/null
    # 先删掉旧的 unset / 别的 export，避免两行打架
    sed -i '/^unset SPARK_HOME$/d; /^export SPARK_HOME=\/opt\/module\/spark$/d' "$HIVENV"
    # 【关键】先补换行再追加。文件末尾没有换行时 `>>` 会把最后一行粘坏
    # （踩过：export HADOOP_HEAPSIZE=2048 被粘成 2048unset → Invalid maximum heap size）
    [ -n "$(tail -c1 "$HIVENV")" ] && echo >> "$HIVENV"
    {
        echo "# ——— fix_hive_on_spark_classpath.sh 于 $TS 写入 ———"
        echo "export SPARK_HOME=$SPARK248"
    } >> "$HIVENV"
    ok "已写入（备份：$HIVENV.bak.$TS）"
fi

# ------------------------------------- 2. 清掉 Spark 自带的 Hive 1.2.1 与裁剪版 ORC
log "== 2/5 清理 $SPARK248/jars 里的冲突包 =="
mkdir -p "$DISABLED"
MOVED=0
for pat in 'hive-*1.2.1*' 'orc-*nohive*' 'orc-shims-1.5.5.jar'; do
    for f in $SPARK248/jars/$pat; do
        [ -e "$f" ] || continue
        mv "$f" "$DISABLED"/ && MOVED=$((MOVED+1))
    done
done
ok "本次挪走 $MOVED 个（累计 disabled：$(ls "$DISABLED" 2>/dev/null | wc -l) 个）"

# --------------------------------------------- 3. auxlib 换成 Hive 完整版 ORC
log "== 3/5 auxlib：删裁剪版 ORC 软链，换 Hive 完整版 =="
[ -d "$AUXLIB" ] || mkdir -p "$AUXLIB"
rm -f "$AUXLIB"/orc-*nohive*.jar "$AUXLIB"/orc-shims-1.5.5.jar
N=0
for j in "$HIVE_HOME"/lib/orc-*.jar; do
    [ -e "$j" ] || continue
    ln -sf "$j" "$AUXLIB/$(basename "$j")" && N=$((N+1))
done
ok "orc 软链已指向 Hive 版本（$N 个）"

# --------------------------------------- 4. HDFS /spark-jars 同步（executor 用）
log "== 4/5 同步 HDFS $HDFS_JARS =="
hdfs dfs -rm -f "$HDFS_JARS"'/hive-*1.2.1*' >/dev/null 2>&1
hdfs dfs -rm -f "$HDFS_JARS"'/orc-*nohive*' >/dev/null 2>&1
hdfs dfs -rm -f "$HDFS_JARS"'/orc-shims-1.5.5.jar' >/dev/null 2>&1
ok "已删除冲突包"

# 4b. hive-druid-handler-3.1.3.jar 是 52MB 的 fat jar，里面打包了 lz4-java 1.3，
#     其 LZ4BlockInputStream 只有单参构造器；Spark 2.4.8 的 LZ4CompressionCodec
#     解 TorrentBroadcast 时会调两参构造器。/spark-jars 按字典序加载，
#     hive-druid... 排在 lz4-java-1.4.0.jar 前面，executor 就吃到旧类：
#       java.lang.NoSuchMethodError: net.jpountz.lz4.LZ4BlockInputStream.<init>(Ljava/io/InputStream;Z)V
#     Druid 存储句柄本项目完全不用，移出 spark-jars 即可（不删除，留备份可回滚）。
hdfs dfs -mkdir -p /spark-jars-disabled >/dev/null 2>&1
for j in $(hdfs dfs -ls "$HDFS_JARS" 2>/dev/null | awk '{print $8}' | grep 'hive-druid-handler-.*\.jar$'); do
    hdfs dfs -mv "$j" /spark-jars-disabled/ >/dev/null 2>&1
done
ok "已把 hive-druid-handler fat jar 移出 $HDFS_JARS（备份在 /spark-jars-disabled）"

# executor 跑 Hive 算子需要 hive-exec 等类，必须随 spark.yarn.jars 下发
HN=$(hdfs dfs -ls "$HDFS_JARS" 2>/dev/null | grep -c "/hive-exec-3")
if [ "$HN" -gt 0 ]; then
    ok "hive-* jar 已在 HDFS，跳过上传"
else
    echo "     上传 $HIVE_HOME/lib/hive-*.jar（约 40 个，首次较慢）..."
    for j in "$HIVE_HOME"/lib/hive-*.jar; do
        [ -e "$j" ] || continue
        case "$j" in *hive-druid-handler-*) continue ;; esac   # 见 4b：内嵌旧 lz4
        hdfs dfs -put -f "$j" "$HDFS_JARS/" >/dev/null 2>&1
    done
    ok "已上传 $(hdfs dfs -ls "$HDFS_JARS" 2>/dev/null | grep -c '/hive-') 个 hive-* jar"
fi
for j in "$HIVE_HOME"/lib/orc-*.jar; do
    [ -e "$j" ] || continue
    hdfs dfs -put -f "$j" "$HDFS_JARS/" >/dev/null 2>&1
done
ok "已上传 Hive 版 orc"

# ------------------------------- 5. hdfs-site：datanode-restart.timeout 改数字
log "== 5/5 hdfs-site.xml：dfs.client.datanode-restart.timeout = 30 =="
HDFS_SITE=/opt/module/hadoop-3.1.4/etc/hadoop/hdfs-site.xml
python - "$HDFS_SITE" <<'PY'
import io, sys
p = sys.argv[1]
s = io.open(p, encoding='utf-8').read()
key = 'dfs.client.datanode-restart.timeout'
if key in s:
    # 已存在：把值统一成 30（秒）。原值 30000 会被按秒解释成 8.3 小时
    import re
    s2 = re.sub(r'(<name>' + key + r'</name>\s*<value>)[^<]*(</value>)', r'\g<1>30\g<2>', s)
    if s2 != s:
        io.open(p, 'w', encoding='utf-8').write(s2)
        print('  updated -> 30')
    else:
        print('  exists, value unchanged')
else:
    add = ('  <property>\n'
           '    <name>dfs.client.datanode-restart.timeout</name>\n'
           '    <value>30</value>\n'
           '  </property>\n')
    io.open(p, 'w', encoding='utf-8').write(s.replace('</configuration>', add + '</configuration>'))
    print('  written')
PY
ok "本机（161）已处理；其他三台需各自执行同一段（见下）"

cat <<'TIP'

  ── 其他三台（160/162/163）也要改，在 160 上执行这一段 ──
  for h in WWC160 WWC162 WWC163; do
    ssh $h "python -c \"
import io
p='/opt/module/hadoop-3.1.4/etc/hadoop/hdfs-site.xml'
s=io.open(p,encoding='utf-8').read()
k='dfs.client.datanode-restart.timeout'
if k in s:
    print('$h exists')
else:
    io.open(p,'w',encoding='utf-8').write(s.replace('</configuration>','  <property>\n    <name>'+k+'</name>\n    <value>30</value>\n  </property>\n</configuration>'))
    print('$h written')
\""
  done

  ── 验证（三步，缺一不可）──
  hive -e "select 1;"                                  # 只证明 JVM 能起
  hive --hiveconf hive.root.logger=ERROR,console -e \
    "insert into tmp_smoke.t1 values (1),(2),(3);"      # 证明 Spark 作业能跑
  hdfs dfs -ls /user/hive/warehouse/tmp_smoke.db/t1    # 证明真落盘（唯一无法作弊的证据）

TIP
