#!/usr/bin/env bash
# =============================================================================
# setup_hive_on_spark.sh —— 把 Hive 3.1.3 接到 Spark 2.4.8 上（在 WWC161 执行）
#
# 为什么必须另装 Spark 2.4.8：
#   Hive 3.1.3 的 Spark 执行器用的是旧接口（SparkContext + JavaSparkContext），
#   Spark 3.x 已改为 SparkSession，Hive 要 4.0 才适配。所以「Hive on Spark」
#   在 Hive 3.1.3 上唯一可用的 Spark 版本是 2.4.x。
#   本机现有 Spark 3.3.1 保留不动，另外装 2.4.8 给 Hive 专用，互不干扰。
#
# 本脚本做七件事：
#   0. 校验 Spark 2.4.8（含 SPARK_HOME 劫持检测）
#   1. 修掉全局 SPARK_HOME（这是"装 2.4.8 却报 3.3.1"的真正原因）
#   2. 确保 hive.metastore.uris 指向本机 thrift（否则 CLI 与 metastore 用两套库）
#   3. 启动 Hive Metastore（9083）
#   4. 写 hive-site.xml 的 Hive on Spark 配置块
#   5. 把 Spark jars 上 HDFS（/spark-jars）+ 建 /spark-events
#   6. 把 Spark jars 软链进 $HIVE_HOME/auxlib（不污染 lib，易回滚）
#   7. 验证：hive -e "select 1"
#
# 用法：
#   bash setup_hive_on_spark.sh                      # 默认 /opt/module/spark-2.4.8
#   bash setup_hive_on_spark.sh /opt/module/spark-2.4.8
#   bash setup_hive_on_spark.sh --rollback           # 恢复 profile + hive-site + auxlib
# =============================================================================
set -u

HIVE_HOME=/opt/module/hive
SPARK248=${1:-/opt/module/spark-2.4.8}
HIVE_CONF=$HIVE_HOME/conf/hive-site.xml
AUXLIB=$HIVE_HOME/auxlib
ENVFILE=/etc/profile.d/my_env.sh
NN=hdfs://WWC160:9820          # HDFS RPC 端口是 9820（不是 8020）
# Metastore 的宿主：必须是 WWC160 —— 元数据库（MySQL 5.7，库名 hive）就在 160 上。
# 161 只当客户端，绝不在这里起 metastore（起过，报 Unknown database 'metastore'）。
MS_HOST=WWC160
TS=$(date +%m%d%H%M)
MARK="hive-on-spark"

ok()  { echo -e "  \033[32m✓\033[0m $*"; }
ng()  { echo -e "  \033[31m✗\033[0m $*"; }
info(){ echo "     $*"; }
log() { echo -e "\n[$TS] $*"; }

# ------------------------------------------------------------ rollback
if [ "${1:-}" = "--rollback" ]; then
    log "== 回滚 =="
    LATEST=$(ls -t "$HIVE_CONF".bak.* 2>/dev/null | head -1)
    if [ -n "$LATEST" ]; then cp "$LATEST" "$HIVE_CONF"; ok "已恢复 $LATEST"; fi
    ENVB=$(ls -t "$ENVFILE".bak.* 2>/dev/null | head -1)
    if [ -n "$ENVB" ]; then cp "$ENVB" "$ENVFILE"; ok "已恢复 $ENVB（SPARK_HOME 回到 3.3.1）"; fi
    HIVENVB=$(ls -t "$HIVE_HOME"/conf/hive-env.sh.bak.* 2>/dev/null | head -1)
    if [ -n "$HIVENVB" ]; then
        cp "$HIVENVB" "$HIVE_HOME/conf/hive-env.sh"; ok "已恢复 hive-env.sh"
    else
        sed -i '/unset SPARK_HOME/d' "$HIVE_HOME/conf/hive-env.sh" 2>/dev/null && ok "已删掉 hive-env.sh 里的 unset SPARK_HOME"
    fi
    if [ -d "$AUXLIB" ]; then
        find "$AUXLIB" -type l -name "*.jar" -delete 2>/dev/null
        ok "已删除 auxlib 下的软链"
    fi
    info "提示：Hive Metastore 未停止，如需停：pkill -f 'hive.*metastore'"
    info "提示：如需彻底卸载 Spark 2.4.8：rm -rf $SPARK248"
    exit 0
fi

# ------------------------------------------------------- 0. 校验 Spark
log "== 0/7 校验 Spark 2.4.x =="
if [ ! -x "$SPARK248/bin/spark-submit" ]; then
    ng "找不到 $SPARK248/bin/spark-submit"
    info "先解压安装包：tar -zxf spark-2.4.8-bin-hadoop2.7.tgz -C /opt/module/"
    info "             mv /opt/module/spark-2.4.8-bin-hadoop2.7 $SPARK248"
    exit 1
fi

# 【坑】spark-submit 脚本开头是：
#   if [ -z "${SPARK_HOME}" ]; then source "$(dirname "$0")"/find-spark-home; fi
# 也就是说：**环境变量 SPARK_HOME 一旦有值，脚本就不再按自身路径推导**。
# 本机 /etc/profile.d/my_env.sh 里 SPARK_HOME=/opt/module/spark（3.3.1），
# 于是直接执行 /opt/module/spark-2.4.8/bin/spark-submit，加载的仍是 3.3.1 的 jars，
# 版本号报 3.3.1 —— 看起来像"装错了包"，其实是环境变量劫持。
VER=$(SPARK_HOME="$SPARK248" "$SPARK248/bin/spark-submit" --version 2>&1 \
      | grep -o "version [0-9.]*" | head -1 | awk '{print $2}')
info "Spark 版本：$VER（$SPARK248）"
case "$VER" in
    2.4.*) ok "版本符合 Hive 3.1.3 的要求" ;;
    *)
        ng "版本 ${VER:-未知} 不是 2.4.x，Hive on Spark 会失败，退出"
        info "检查：cat $SPARK248/RELEASE"
        exit 1
        ;;
esac

# ------------------------------------------ 1. 修掉全局 SPARK_HOME 劫持
log "== 1/7 修掉全局 SPARK_HOME（根因）=="
CUR_SH=$(bash -lc 'echo ${SPARK_HOME:-}' 2>/dev/null)
info "当前登录 shell 里的 SPARK_HOME=${CUR_SH:-（空）}"
if [ "$CUR_SH" = "$SPARK248" ]; then
    ok "已指向 2.4.8，无需修改"
elif [ -z "$CUR_SH" ]; then
    ok "为空（spark-submit 会按自身路径推导，正是我们要的），无需修改"
else
    if [ -f "$ENVFILE" ]; then
        cp "$ENVFILE" "$ENVFILE.bak.$TS"
        # 先把 PATH 里的 $SPARK_HOME/bin 变成绝对路径，再把 export SPARK_HOME 注释掉。
        # 顺序不能反：先注释掉，$SPARK_HOME 就成了空串，PATH 会变成 /bin。
        sed -i 's|[$]SPARK_HOME/bin|/opt/module/spark/bin|g' "$ENVFILE"
        sed -i 's|^export SPARK_HOME=.*|# &   # 已注释：全局 SPARK_HOME 会让 spark-2.4.8/bin/spark-submit 被劫持成 3.3.1|' "$ENVFILE"
        ok "已注释 $ENVFILE 里的 SPARK_HOME，PATH 改为绝对路径 /opt/module/spark/bin"
        info "备份：$ENVFILE.bak.$TS"
        info "效果：spark-submit 按自身路径推导 —— 敲 spark-submit 是 3.3.1，"
        info "      敲 $SPARK248/bin/spark-submit 是 2.4.8，两者互不干扰"
    else
        ng "$ENVFILE 不存在，请手工把 SPARK_HOME 注释掉"
    fi
fi
# 本脚本后续所有动作都显式用 2.4.8，不依赖登录 shell 的环境
export SPARK_HOME="$SPARK248"

# 第二层保险：往 hive-env.sh 里写 unset SPARK_HOME。
# 为什么光改 profile 不够 —— 环境变量是**继承**的：已经开着的终端、
# 甚至 `bash -l` 新开的登录 shell，都会从父进程继承旧的 SPARK_HOME，
# profile 里不写它并不等于它不存在。只有真正新开 SSH 连接才干净。
# 而 HIVE 启动脚本会 source hive-env.sh，在这里 unset 就与用户 shell 彻底解耦：
# Hive 只认 hive-site.xml 里的 spark.home（=2.4.8）。
HIVENV=$HIVE_HOME/conf/hive-env.sh
[ -f "$HIVENV" ] || touch "$HIVENV"
if grep -q "unset SPARK_HOME" "$HIVENV" 2>/dev/null; then
    ok "hive-env.sh 已有 unset SPARK_HOME"
else
    cp "$HIVENV" "$HIVENV.bak.$TS" 2>/dev/null
    {
        echo ""
        echo "# ——— 由 setup_hive_on_spark.sh 于 $TS 写入 ———"
        echo "# 防止调用者 shell 里的 SPARK_HOME（可能指向 3.3.1）劫持 Hive 拉起的 spark-submit，"
        echo "# 导致 client=3.3.1 / AM=2.4.8，报 Unknown/unsupported param --dist-cache-conf。"
        echo "unset SPARK_HOME"
    } >> "$HIVENV"
    ok "已在 hive-env.sh 写入 unset SPARK_HOME（Hive 只认 spark.home）"
fi

# ------------------------------- 2. 确保 hive.metastore.uris（CLI 必须走 thrift）
log "== 2/7 确保 hive.metastore.uris = thrift://$MS_HOST:9083 =="
# 为什么必须配：不配的话 Hive CLI 会自己开一个内嵌 Derby 直连元数据库，
# 而 Metastore 服务也在用同一份元数据 —— 两边各写各的，看到的表还不一样。
# 为什么指向 160：元数据库（MySQL 的 hive 库）在 160，Metastore 服务也在 160。
WANT_URI="thrift://$MS_HOST:9083"
CUR_URI=$(grep -A1 "<name>hive.metastore.uris</name>" "$HIVE_CONF" 2>/dev/null \
          | grep -o '<value>[^<]*</value>' | head -1 | sed 's/<[^>]*>//g')
if [ "$CUR_URI" = "$WANT_URI" ]; then
    ok "已正确指向：$CUR_URI"
elif [ -n "$CUR_URI" ]; then
    # 指向了别处（典型：指向 161 本机，但本机没有 metastore 库）
    cp "$HIVE_CONF" "$HIVE_CONF.uri.bak.$TS"
    sed -i "/<name>hive\.metastore\.uris<\/name>/,/<value>/ s|<value>[^<]*</value>|<value>$WANT_URI</value>|" "$HIVE_CONF"
    NEW_URI=$(grep -A1 "<name>hive.metastore.uris</name>" "$HIVE_CONF" \
              | grep -o '<value>[^<]*</value>' | head -1 | sed 's/<[^>]*>//g')
    if [ "$NEW_URI" = "$WANT_URI" ]; then
        ok "已修正：$CUR_URI → $NEW_URI（备份：$HIVE_CONF.uri.bak.$TS）"
    else
        ng "修正失败（仍为 $NEW_URI），请手工改 $HIVE_CONF"
        exit 1
    fi
else
    cp "$HIVE_CONF" "$HIVE_CONF.uri.bak.$TS"
    PROPS=$(mktemp)
    printf '    <property><name>hive.metastore.uris</name><value>%s</value></property>\n' "$WANT_URI" > "$PROPS"
    awk -v propsf="$PROPS" '
      { lines[NR] = $0 }
      END {
        idx = 0
        for (i = NR; i >= 1; i--) if (lines[i] ~ /<\/configuration>/) { idx = i; break }
        if (idx == 0) { for (i = 1; i <= NR; i++) print lines[i]; exit }
        for (i = 1; i < idx; i++) print lines[i]
        while ((getline l < propsf) > 0) print l
        print "  </configuration>"
        for (i = idx + 1; i <= NR; i++) print lines[i]
      }' "$HIVE_CONF" > "$HIVE_CONF.new" && mv "$HIVE_CONF.new" "$HIVE_CONF"
    rm -f "$PROPS"
    ok "已写入 $WANT_URI（备份：$HIVE_CONF.uri.bak.$TS）"
fi

# ------------------------------------------------------- 3. 启动 Metastore
log "== 3/7 检查 $MS_HOST 上的 Hive Metastore（9083）=="
probe() { timeout 3 bash -c "cat < /dev/null > /dev/tcp/$1/$2" 2>/dev/null; }
if probe "$MS_HOST" 9083; then
    ok "Metastore 可达（$MS_HOST:9083）"
else
    ng "$MS_HOST:9083 不通。Metastore 跑在 160 上（元数据库 MySQL 同机），请在 160 上执行："
    info "systemctl start mysqld"
    info "systemctl start hive-metastore   # 若无此单元：nohup \$HIVE_HOME/bin/hive --service metastore > /tmp/hive-metastore.log 2>&1 &"
    info ""
    info "【别在 161 上起 metastore】161 的 MySQL 里没有 metastore 库，"
    info "  在 161 起会报 Unknown database 'metastore'，而真正的服务一直在 160 上。"
    exit 1
fi

# ------------------------------------------- 4. 写 hive-site.xml 配置块
log "== 4/7 写入 hive-site.xml（Hive on Spark 配置）=="
if grep -q "$MARK" "$HIVE_CONF" 2>/dev/null; then
    ok "已存在配置块，跳过（要重写先跑 --rollback）"
else
    cp "$HIVE_CONF" "$HIVE_CONF.bak.$TS"
    PROPS=$(mktemp)
    cat > "$PROPS" <<EOF
    <!-- $MARK:begin (由 setup_hive_on_spark.sh 于 $TS 写入) -->
    <property><name>hive.execution.engine</name><value>spark</value></property>
    <property><name>spark.home</name><value>$SPARK248</value></property>
    <!-- Hive on Spark 只支持 client 模式，不支持 cluster -->
    <property><name>spark.master</name><value>yarn</value></property>
    <property><name>spark.submit.deployMode</name><value>client</value></property>
    <!-- 预先把 spark jars 放 HDFS，否则每次提交都要上传几百 MB -->
    <property><name>spark.yarn.jars</name><value>$NN/spark-jars/*.jar</value></property>
    <property><name>spark.eventLog.enabled</name><value>true</value></property>
    <property><name>spark.eventLog.dir</name><value>$NN/spark-events</value></property>
    <!-- 161 只有 6G 且 Flink 已占 5G，driver 压到 512m 才塞得下 -->
    <property><name>spark.driver.memory</name><value>512m</value></property>
    <property><name>spark.executor.memory</name><value>1g</value></property>
    <property><name>spark.executor.cores</name><value>1</value></property>
    <property><name>spark.dynamicAllocation.enabled</name><value>false</value></property>
    <property><name>spark.sql.shuffle.partitions</name><value>8</value></property>
    <!-- $MARK:end -->
EOF
    awk -v propsf="$PROPS" '
      { lines[NR] = $0 }
      END {
        idx = 0
        for (i = NR; i >= 1; i--) if (lines[i] ~ /<\/configuration>/) { idx = i; break }
        if (idx == 0) { for (i = 1; i <= NR; i++) print lines[i]; exit }
        for (i = 1; i < idx; i++) print lines[i]
        while ((getline l < propsf) > 0) print l
        print "  </configuration>"
        for (i = idx + 1; i <= NR; i++) print lines[i]
      }' "$HIVE_CONF" > "$HIVE_CONF.new" && mv "$HIVE_CONF.new" "$HIVE_CONF"
    rm -f "$PROPS"
    ok "已写入（备份：$HIVE_CONF.bak.$TS）"
fi

# --------------------------------------------- 5. HDFS 目录 + 上传 jars
log "== 5/7 准备 HDFS 上的 spark-jars / spark-events =="
hdfs dfs -mkdir -p /spark-jars /spark-events 2>/dev/null && ok "目录就绪" || ng "建目录失败（HDFS 起了吗）"
hdfs dfs -chmod -R 755 /spark-jars /spark-events 2>/dev/null
EXIST=$(hdfs dfs -ls /spark-jars 2>/dev/null | grep -c "\.jar")
if [ "$EXIST" -gt 100 ]; then
    ok "/spark-jars 已有 $EXIST 个 jar，跳过上传"
else
    info "上传 $SPARK248/jars（226 个，首次较慢）..."
    hdfs dfs -put -f "$SPARK248"/jars/*.jar /spark-jars/ 2>/dev/null
    N=$(hdfs dfs -ls /spark-jars 2>/dev/null | grep -c "\.jar")
    [ "$N" -gt 100 ] && ok "已上传 $N 个 jar" || ng "上传异常，只有 $N 个"
fi

# ------------------------------------------- 6. 软链 spark jars 到 auxlib
log "== 6/7 把 spark jars 链进 \$HIVE_HOME/auxlib =="
mkdir -p "$AUXLIB"
LINKED=0
for j in "$SPARK248"/jars/*.jar; do
    b=$(basename "$j")
    # 跳过 Hive 自带且版本不同会冲突的核心包（guava/log4j/slf4j/hadoop/hive 由 Hive 提供）
    case "$b" in
        guava-*|log4j-*|slf4j-*|hadoop-*|hive-*) continue ;;
    esac
    [ -e "$AUXLIB/$b" ] || { ln -s "$j" "$AUXLIB/$b" 2>/dev/null && LINKED=$((LINKED+1)); }
done
ok "新建软链 $LINKED 个（已存在的跳过）"
info "auxlib 现有 jar：$(ls "$AUXLIB" 2>/dev/null | wc -l) 个"

# ---------------------------------------------------------------- 7. 验证
log "== 7/7 验证：用 Hive 跑一条 SQL，看是否走 Spark =="
info "（首次启动 Spark 上下文较慢，等 1~3 分钟；driver 起来前会先卡在 yarn 分配）"
OUT=$(timeout 420 "$HIVE_HOME/bin/hive" -e "select 1 as ok;" 2>&1)
echo "$OUT" | tail -20 | sed 's/^/     /'
echo
if echo "$OUT" | grep -qE "^1[[:space:]]*$|ok[[:space:]]*=[[:space:]]*1|_c0|Time taken"; then
    ok "Hive on Spark 跑通了"
    info "确认引擎：hive -e 'set hive.execution.engine;'  应输出 spark"
    info "新开终端后这个结论依然成立（SPARK_HOME 已修）"
else
    ng "没有成功。常见原因见下："
    info "· NoClassDefFoundError SparkContext  → auxlib 软链没生效，看 ls \$HIVE_HOME/auxlib | head"
    info "· NoSuchMethodError guava             → 删掉 \$HIVE_HOME/auxlib/guava-*（用 Hive 自带的）"
    info "· YARN 资源不足 / container 被杀       → 停实时作业释放 161 内存，或调小 executor"
    info "· hdfs:///spark-jars 读不到            → hdfs dfs -ls /spark-jars | head"
    info "· 报 3.3.1 的类找不到                  → 仍然有别的 SPARK_HOME 在生效，看 env | grep SPARK"
    info "回滚：bash $0 --rollback"
fi
