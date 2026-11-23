#!/bin/bash
# =============================================================================
# 集群一键启停脚本 —— 在 WWC160 上执行
#
#   ./cluster_ctl.sh start     按依赖顺序启动全部服务
#   ./cluster_ctl.sh stop      逆序停止全部服务
#   ./cluster_ctl.sh restart   先停再起
#   ./cluster_ctl.sh status    查看全部关键端口
#
# 依赖：160 到 161/162/163 的 root 免密 SSH（Hadoop 集群本就要求）
#
# 设计要点：
#   1) 顺序由依赖决定，不是拍脑袋：
#      ZK → HDFS/YARN → Kafka → Flume → Flink → HBase → Doris → MySQL/Metastore → DataArt → DS
#   2) 每一步都用「端口真的监听」判断成功，不 sleep 猜时间（-daemon 静默失败是踩过的坑）
#      Flume 是唯一没有监听端口的组件，退一步用主类名匹配进程（见 flume_up）
#   3) 幂等：已经起来的服务跳过，重复执行不会起第二个实例
#   4) 失败只告警不中断：某个组件挂了，其余继续起，最后统一汇总，便于一眼看出谁没起来
#   5) 可选服务用环境变量跳过：SKIP_DATART=1 ./cluster_ctl.sh start
#
# 内存提醒：160 上 DataArt 约 1G、DolphinScheduler 3.4.2 standalone 约 1G。
# 两者与 NameNode 共存时需要 ≥8G 物理内存。脚本启动前会做可用内存检查，不够只告警不阻止。
# 3.4.2 用单 JVM + JDBC 注册表，比 3.1.9 四进程伪集群省 3G，且不需要 ZK。
# =============================================================================
set -u

ZK_HOME=/opt/module/zookeeper
KAFKA_HOME=/opt/module/kafka
FLINK_HOME=/opt/module/flink
DORIS_HOME=/opt/module/doris
HIVE_HOME=/opt/module/hive
DATART_HOME=/opt/module/datart
DS_HOME=/opt/module/apache-dolphinscheduler-3.4.2-bin
NODES="WWC161 WWC162 WWC163"

# Kafka 只跑 161 一台（docs/17 §3.6.1）。
# 原来 161/162/163 各起一个 broker（id 0/1/2），topic 默认 RF=2：
# 3 分区 × 2 副本 = 6 份数据，而 Flink 只连 WWC161:9092，另两台纯占内存和磁盘。
# 162 总共才 3G（HBase 占 1.78G），163 跑着 Doris BE，都不该再背一个 Kafka 堆。
KAFKA_NODES="WWC161"

# Flume 只在 161（Kafka 同机，省一次跨机拉取），负责 music_events → HDFS landing，
# 是离线链路唯一的"到达缓冲"。以前它没进这个脚本，导致一键起完集群后
# 实时在跑、landing 一个文件都不落，`flume-ods` 消费组积压 64 万也没人发现（docs/21 §9.6）。
# 它没有监听端口，也不像 -daemon 那样有静默失败，只能靠进程判断。
FLUME_HOME=/opt/module/flume
FLUME_CONF=conf/flume-music-hdfs.conf
FLUME_AGENT=a1
FLUME_LOGF=$FLUME_HOME/logs/flume-ods.log
# 用 [.] 包住点号：pkill/pgrep -f 会把"执行这条命令的 shell 自己"也算进去，
# 加了中括号后自己的 cmdline 里是字面量 [.]，匹配不上，不会自杀。
FLUME_PAT='org[.]apache[.]flume[.]node[.]Application'

log() { echo -e "[$(date +%H:%M:%S)] $*"; }

ssh_do() { local h=$1; shift; ssh -o BatchMode=yes -o ConnectTimeout=5 -o StrictHostKeyChecking=no root@"$h" "$@" 2>/dev/null; }

port_up() { (echo > /dev/tcp/"$1"/"$2") 2>/dev/null; }

wait_port() {  # host port timeout_s
    local i=0
    while [ $i -lt "${3:-60}" ]; do
        port_up "$1" "$2" && return 0
        sleep 1; i=$((i + 1))
    done
    return 1
}

ok() { log "  \033[32m✓\033[0m $*"; }
ng() { log "  \033[31m✗\033[0m $*"; }
skip() { log "  \033[33m-\033[0m $*"; }

# Flume 不看端口看主类：jps 里显示为 Application（不可靠，容易和其他 Java 混），
# 用 pgrep -f 匹配完整主类名最稳。
flume_pid() { ssh_do WWC161 "pgrep -f '$FLUME_PAT' | head -1"; }
flume_up() { [ -n "$(flume_pid)" ]; }
# 注意：Flume 不在 160 上，[-x 路径] 这种本地判断一定会得到"未安装"，
# 必须 ssh 到 161 去测（第一次写就栽在这，脚本静默 skip 了 Flume）。
flume_installed() { [ "$(ssh_do WWC161 "test -x $FLUME_HOME/bin/flume-ng && echo yes")" = "yes" ]; }

# 可用内存（MB），取 available 而不是 free —— buff/cache 会被回收，不算真占用
mem_avail_mb() { free -m | awk '/^Mem:/{print $7}'; }

mem_guard() {  # need_mb 说明
    local a; a=$(mem_avail_mb)
    if [ "${a:-0}" -lt "$1" ]; then
        log "  \033[33m! 可用内存 ${a}MB < 建议 ${1}MB（$2），继续启动但可能 OOM\033[0m"
    fi
}

# ---------------------------------------------------------------- start
start_all() {
    mem_guard 4096 "起 DataArt/DS 前建议"

    log "== 1/10 ZooKeeper（三台，必须全部起来否则 Kafka 起不来）=="
    for n in $NODES; do
        port_up "$n" 2181 || ssh_do "$n" "$ZK_HOME/bin/zkServer.sh start" >/dev/null
    done
    for n in $NODES; do wait_port "$n" 2181 40 && ok "ZK $n" || ng "ZK $n"; done

    log "== 2/10 HDFS + YARN（本机 160）=="
    port_up WWC160 9870 || { start-dfs.sh >/dev/null 2>&1; start-yarn.sh >/dev/null 2>&1; }
    wait_port WWC160 9870 90 && ok "HDFS NameNode" || ng "HDFS NameNode"
    wait_port WWC160 8088 60 && ok "YARN RM" || ng "YARN ResourceManager"

    log "== 3/10 Kafka（仅 161，单副本 topic）=="
    for n in $KAFKA_NODES; do
        port_up "$n" 9092 || ssh_do "$n" "$KAFKA_HOME/bin/kafka-server-start.sh -daemon $KAFKA_HOME/config/server.properties" >/dev/null
    done
    for n in $KAFKA_NODES; do wait_port "$n" 9092 90 && ok "Kafka $n" || ng "Kafka $n"; done

    # Flume 必须排在 Kafka 之后：KafkaSource 起来就要连 9092，
    # 连不上会一直刷 WARN 并退避重试（不会崩，但会白等，看起来像"启动了却没数据"）。
    log "== 4/10 Flume（161，music_events → HDFS landing）=="
    if ! flume_installed; then
        skip "Flume（161 上未安装 $FLUME_HOME）"
    elif flume_up; then
        ok "Flume agent $FLUME_AGENT（已在运行，pid $(flume_pid)）"
    else
        # logs 目录不预建的话，shell 重定向在读 conf 之前就失败，进程静默起不来（踩过）
        #
        # 这条 ssh 曾把整个 start 卡死 20+ 分钟（docs/ 冷启动复盘）：远端后台进程只要
        # 还持有 ssh 会话的 stdin/stdout/stderr，ssh 就一直不返回，后面的组件全不启动。
        # 三层保险：
        #   ① 本地 `ssh -n` + </dev/null 断开 stdin；
        #   ② 远端 `nohup setsid` + 三个 fd 全部重定向，末尾 `echo` 让远端 shell 立即退出；
        #   ③ `timeout 15` 兜底，即使 ssh 仍不返回也只记一条告警，不阻断后续组件。
        # 启动成功与否由下面 30 秒的 flume_up 轮询判定，不依赖 ssh 的退出码。
        timeout 15 ssh -n -o BatchMode=yes -o ConnectTimeout=5 -o StrictHostKeyChecking=no \
            root@WWC161 "mkdir -p $FLUME_HOME/logs && cd $FLUME_HOME && \
nohup setsid bin/flume-ng agent --conf conf --conf-file $FLUME_CONF --name $FLUME_AGENT \
-Dflume.root.logger=INFO,console </dev/null >logs/flume-ods.log 2>&1 & echo launched" \
            </dev/null >/dev/null 2>&1 \
            || ng "Flume 启动 ssh 超时/失败（继续用进程轮询判定，不中断后续组件）"
        # 没有端口可探：给 JVM 最多 30 秒把主类拉起来（实测 3~5 秒）
        local i=0
        while [ $i -lt 30 ] && ! flume_up; do sleep 1; i=$((i + 1)); done
        if flume_up; then
            ok "Flume agent $FLUME_AGENT（pid $(flume_pid)，日志 161:$FLUME_LOGF）"
        else
            ng "Flume agent $FLUME_AGENT（看 161:$FLUME_LOGF，常见原因：logs 目录不存在 / Java 堆太小）"
        fi
    fi

    log "== 5/10 Flink + SQL Gateway（161）=="
    port_up WWC161 8081 || ssh_do WWC161 "source /etc/profile.d/flink-hadoop.sh 2>/dev/null; $FLINK_HOME/bin/start-cluster.sh" >/dev/null
    wait_port WWC161 8081 90 && ok "Flink JobManager" || ng "Flink JobManager"
    port_up WWC161 8083 || ssh_do WWC161 "$FLINK_HOME/bin/sql-gateway.sh start" >/dev/null
    wait_port WWC161 8083 60 && ok "SQL Gateway" || ng "SQL Gateway"

    log "== 6/10 HBase（162）=="
    if ! port_up WWC162 16010; then
        HBASE_HOME=$(ssh_do WWC162 "ls -d /opt/module/hbase* 2>/dev/null | head -1")
        [ -n "$HBASE_HOME" ] && ssh_do WWC162 "source /etc/profile 2>/dev/null; $HBASE_HOME/bin/start-hbase.sh" >/dev/null
    fi
    wait_port WWC162 16010 120 && ok "HBase Master" || ng "HBase Master"

    log "== 7/10 Doris（163，FE 先于 BE）=="
    port_up WWC163 9030 || ssh_do WWC163 "sh $DORIS_HOME/fe/bin/start_fe.sh --daemon" >/dev/null
    wait_port WWC163 9030 120 && ok "Doris FE" || ng "Doris FE"
    port_up WWC163 9060 || ssh_do WWC163 "sh $DORIS_HOME/be/bin/start_be.sh --daemon" >/dev/null
    wait_port WWC163 9060 90 && ok "Doris BE" || ng "Doris BE"

    # MySQL + Hive Metastore 必须常驻：二期离线数仓（Hive on Spark）要读写 Hive 表，
    # 元数据全部存在 160 的 MySQL 里，thrift 9083 是唯一入口。
    #
    # 【易踩坑】Metastore 必须跑在 160（元数据库在同机），161 只当客户端，
    # 靠 hive.metastore.uris 指过来。曾把 metastore 起在 161 上，结果 161 的
    # hive-site.xml 指向本机 MySQL 的 metastore 库（不存在）→ 报
    # Unknown database 'metastore'，而真正的服务其实一直在 160 上跑着。
    log "== 8/10 MySQL + Hive Metastore（160，离线数仓的前提）=="
    systemctl is-active mysqld >/dev/null 2>&1 || systemctl start mysqld >/dev/null 2>&1
    wait_port WWC160 3306 60 && ok "MySQL 5.7" || ng "MySQL 5.7（systemctl status mysqld）"
    if ! port_up WWC160 9083; then
        if systemctl list-unit-files 2>/dev/null | grep -q "^hive-metastore"; then
            systemctl start hive-metastore >/dev/null 2>&1
        else
            # nohup + & 让 metastore 脱离当前会话；日志落 /tmp 便于排查
            nohup $HIVE_HOME/bin/hive --service metastore > /tmp/hive-metastore.log 2>&1 &
        fi
    fi
    wait_port WWC160 9083 120 && ok "Hive Metastore" || ng "Hive Metastore（看 160:/tmp/hive-metastore.log）"

    # DataArt：BI 展示层，连 Doris 163:9030 的 music_offline 库。
    # 不在 Hadoop 体系内，独立进程，内存约 1G（启动脚本里 -Xmx1G）。
    log "== 9/10 DataArt（160，BI 报表）=="
    if [ "${SKIP_DATART:-0}" = "1" ]; then
        skip "DataArt（SKIP_DATART=1 跳过）"
    elif [ ! -x "$DATART_HOME/bin/datart-server.sh" ]; then
        skip "DataArt（未安装 $DATART_HOME）"
    else
        mem_guard 1536 "DataArt 需要"
        port_up WWC160 8080 || $DATART_HOME/bin/datart-server.sh start >/dev/null 2>&1
        # 首次启动要跑 migration 建表，30 秒不够，给 120 秒
        wait_port WWC160 8080 120 && ok "DataArt" || ng "DataArt（看 $DATART_HOME/nohup.out）"
    fi

    # DolphinScheduler 3.4.2 standalone：单 JVM（master/worker/api/alert 合一），
    # JDBC 注册表不需要 ZK；端口 12345，元数据在 160 的 MySQL dolphinscheduler 库。
    # 3.1.9 伪集群时代的 dolphinscheduler-daemon.sh 四进程写法已不适用。
    log "== 10/10 DolphinScheduler（160，调度）=="
    if [ "${SKIP_DS:-0}" = "1" ]; then
        skip "DolphinScheduler（SKIP_DS=1 跳过）"
    elif [ ! -f "$DS_HOME/standalone-server/bin/start.sh" ]; then
        skip "DolphinScheduler（未安装 $DS_HOME）"
    else
        mem_guard 1536 "DolphinScheduler 需要"
        if ! port_up WWC160 12345; then
            (cd "$DS_HOME" && nohup bash standalone-server/bin/start.sh \
                > /tmp/dolphinscheduler-standalone.log 2>&1 &)
        fi
        wait_port WWC160 12345 180 && ok "DS API" || ng "DS API（看 $DS_HOME/logs）"
    fi

    echo
    status_all
    log "启动完成。Flink 作业在大屏侧用 redeploy_realtime.py 提交。"
}

# ---------------------------------------------------------------- stop
stop_all() {
    log "提示：先取消 Flink 作业（Windows 上 python code/flink_admin.py cancel ...），再执行 stop"

    log "== 1/10 DolphinScheduler（160，先停调度，避免停库时还在跑任务）=="
    if [ "${SKIP_DS:-0}" != "1" ] && pgrep -f "[S]tandaloneServer" >/dev/null; then
        # 3.4.2 standalone 只有一个进程，按主类名精确匹配
        pkill -f "[S]tandaloneServer" 2>/dev/null || true
        ok "DolphinScheduler（standalone 已停）"
    else
        skip "DolphinScheduler"
    fi

    log "== 2/10 DataArt（160）=="
    if [ -x "$DATART_HOME/bin/datart-server.sh" ] && [ "${SKIP_DATART:-0}" != "1" ]; then
        $DATART_HOME/bin/datart-server.sh stop >/dev/null 2>&1
        ok "DataArt"
    else
        skip "DataArt"
    fi

    log "== 3/10 Flink（161）=="
    ssh_do WWC161 "$FLINK_HOME/bin/sql-gateway.sh stop" >/dev/null
    ssh_do WWC161 "$FLINK_HOME/bin/stop-cluster.sh" >/dev/null
    # Flume 要在 Kafka 之前停：它是 Kafka 的消费者，反过来停会先丢 broker 再停消费者，
    # offset 提交不上（memory channel 里的数据也就跟着没了）。当前积压为 0，但仍按依赖顺序来。
    log "== 4/10 Flume（161）=="
    if flume_up; then
        ssh_do WWC161 "pkill -f '$FLUME_PAT'" >/dev/null
        local i=0
        while [ $i -lt 20 ] && flume_up; do sleep 1; i=$((i + 1)); done
        flume_up && ng "Flume（pkill 后仍在运行，pid $(flume_pid)）" || ok "Flume agent $FLUME_AGENT"
    else
        skip "Flume"
    fi
    log "== 5/10 HBase（162）=="
    HBASE_HOME=$(ssh_do WWC162 "ls -d /opt/module/hbase* 2>/dev/null | head -1")
    [ -n "$HBASE_HOME" ] && ssh_do WWC162 "$HBASE_HOME/bin/stop-hbase.sh" >/dev/null
    log "== 6/10 Doris（163，先 BE 后 FE）=="
    ssh_do WWC163 "sh $DORIS_HOME/be/bin/stop_be.sh" >/dev/null
    ssh_do WWC163 "sh $DORIS_HOME/fe/bin/stop_fe.sh" >/dev/null
    log "== 7/10 Kafka（仅 161）=="
    for n in $KAFKA_NODES; do ssh_do "$n" "$KAFKA_HOME/bin/kafka-server-stop.sh" >/dev/null; done
    sleep 5
    log "== 8/10 ZooKeeper（三台）=="
    for n in $NODES; do ssh_do "$n" "$ZK_HOME/bin/zkServer.sh stop" >/dev/null; done
    log "== 9/10 Hive Metastore（160）=="
    if systemctl list-unit-files 2>/dev/null | grep -q "^hive-metastore"; then
        systemctl stop hive-metastore >/dev/null 2>&1
    else
        pkill -f 'hive.*metastore' >/dev/null 2>&1
    fi
    log "== 10/10 HDFS + YARN（160，最后停）=="
    stop-yarn.sh >/dev/null 2>&1; stop-dfs.sh >/dev/null 2>&1
    log "停止完成。"
}

# ---------------------------------------------------------------- status
status_all() {
    log "== 端口状态 =="
    chk() { port_up "$2" "$3" && printf "  %-24s \033[32mOPEN\033[0m   %s\n" "$1" "$3" || printf "  %-24s \033[31mCLOSED\033[0m %s\n" "$1" "$3"; }
    chk "ZK 161"                WWC161 2181
    chk "ZK 162"                WWC162 2181
    chk "ZK 163"                WWC163 2181
    chk "HDFS NameNode (160)"   WWC160 9870
    chk "YARN RM (160)"         WWC160 8088
    chk "Kafka 161"             WWC161 9092
    # Flume 没有监听端口，单独按进程判断（其余组件都看端口）
    if flume_up; then
        printf "  %-24s \033[32mUP\033[0m     pid %s\n" "Flume agent (161)" "$(flume_pid)"
    else
        printf "  %-24s \033[31mDOWN\033[0m   (无监听端口)\n" "Flume agent (161)"
    fi
    chk "Flink JM (161)"        WWC161 8081
    chk "SQL Gateway (161)"     WWC161 8083
    chk "HBase Master (162)"    WWC162 16010
    chk "Doris FE (163)"        WWC163 9030
    chk "Doris BE (163)"        WWC163 9060
    chk "MySQL (160)"           WWC160 3306
    chk "Hive Metastore (160)"  WWC160 9083
    chk "DataArt (160)"         WWC160 8080
    chk "DolphinScheduler (160)" WWC160 12345
    echo
    printf "  %-24s %s MB\n" "可用内存 (160)" "$(mem_avail_mb)"
}

case "${1:-}" in
    start)   start_all ;;
    stop)    stop_all ;;
    restart) stop_all; echo; sleep 3; start_all ;;
    status)  status_all ;;
    *)       echo "用法: $0 {start|stop|restart|status}"
             echo "      可选环境变量: SKIP_DATART=1 / SKIP_DS=1 跳过对应服务"
             exit 1 ;;esac
