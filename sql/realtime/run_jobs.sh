#!/bin/bash
# =============================================================================
# 按顺序提交三个实时作业（A → B → C）
#
# 前置条件：
#   1. Hadoop / Kafka / Flink session cluster 已启动
#      jps 在 WWC161 上应能看到 StandaloneSessionClusterEntrypoint + TaskManagerRunner
#   2. Doris FE(WWC163:8030) 可访问，15 张表已建好
#   3. 中间 topic 已创建（用 Windows 上的 code/init_topics.py，或本目录 00_init_topics.sh）
#   4. Doris 与 Kafka 的 connector jar 已在 $FLINK_HOME/lib 下
#
# 用法：
#   bash run_jobs.sh           # 提交全部
#   bash run_jobs.sh job_b     # 只提交某一个
# =============================================================================
set -e

FLINK_HOME=${FLINK_HOME:-/opt/module/flink}
DIR=$(cd "$(dirname "$0")" && pwd)
CONF="$DIR/session-remote.yaml"
JM_REST=${JM_REST:-http://WWC161:8081}

# v10：job_b 实际部署的是 job_b_dwd_dim.sql（含 HBase 维度点查 + 26 类白名单 +
# dwd_unrouted 兜底），与 code/redeploy_realtime.py 保持一致。job_b_dwd.sql 是历史版本。
ALL_JOBS=("job_a_ods" "job_b_dwd_dim" "job_c_dws" "job_d_ads")

# 每个作业的并行度，与 SQL 文件里的 parallelism.default 对应
# 合计 5 个 slot（1+2+1+1）：本机 TM 只有 1G 堆，slot 开太多每个分到的内存太少
job_slots() {
    case "$1" in
        job_a_ods) echo 1 ;;
        job_b_dwd|job_b_dwd_dim) echo 2 ;;
        job_c_dws) echo 1 ;;
        job_d_ads) echo 1 ;;
        *)         echo 1 ;;
    esac
}

if [ -n "$1" ]; then
    JOBS=()
    for j in "${ALL_JOBS[@]}"; do
        if [[ "$j" == *"$1"* ]]; then
            JOBS=("$j")
            break
        fi
    done
    if [ ${#JOBS[@]} -eq 0 ]; then
        echo "未知作业: $1（可选 job_a / job_b / job_c）" >&2
        exit 1
    fi
else
    JOBS=("${ALL_JOBS[@]}")
fi

# -----------------------------------------------------------------------------
# 检查 connector jar。缺 jar 时提交会报 "Could not find any factory for
# identifier 'doris'" 之类，错误信息指向性差，不如提前拦住。
# -----------------------------------------------------------------------------
MISSING=""
ls "$FLINK_HOME/lib" 2>/dev/null | grep -q "flink-sql-connector-kafka" || MISSING="$MISSING kafka"
ls "$FLINK_HOME/lib" 2>/dev/null | grep -qi "flink-doris-connector"  || MISSING="$MISSING doris"
if [ -n "$MISSING" ]; then
    echo "缺少 connector jar:$MISSING" >&2
    echo "当前 \$FLINK_HOME/lib 下的 connector:" >&2
    ls "$FLINK_HOME/lib" | grep -iE "connector|kafka|doris" >&2 || echo "  （一个都没有）" >&2
    exit 1
fi

# -----------------------------------------------------------------------------
# 提交前检查 slot。slot 不够时 Flink 不会报错，作业会一直停在 CREATED 状态等资源，
# 比直接失败更难排查，所以在这里提前拦住。
# -----------------------------------------------------------------------------
NEED=0
for j in "${JOBS[@]}"; do
    NEED=$((NEED + $(job_slots "$j")))
done

FREE=$(curl -s -m 5 "$JM_REST/overview" \
       | grep -o '"slots-available":[0-9]*' | cut -d: -f2)
if [ -z "$FREE" ]; then
    echo "连不上 Flink REST（$JM_REST），先确认 161 上 start-cluster.sh 已执行" >&2
    exit 1
fi
echo "本次需 $NEED 个 slot，当前空闲 $FREE 个"
if [ "$FREE" -lt "$NEED" ]; then
    echo "" >&2
    echo "slot 不够。两种办法：" >&2
    echo "  ① 扩容（推荐）：改 \$FLINK_HOME/conf/flink-conf.yaml" >&2
    echo "       taskmanager.numberOfTaskSlots: 6" >&2
    echo "     然后 bin/stop-cluster.sh && bin/start-cluster.sh（会杀掉现有作业）" >&2
    echo "  ② 先验证功能：把 job_b_dwd.sql / job_c_dws.sql 的 parallelism.default 改成 1" >&2
    exit 1
fi

for j in "${JOBS[@]}"; do
    echo "=========================================="
    echo ">>> 提交 $j"
    echo "=========================================="
    $FLINK_HOME/bin/sql-client.sh -s "$CONF" -f "$DIR/$j.sql"
    sleep 3
done

echo
echo ">>> 当前运行中的作业"
$FLINK_HOME/bin/flink list -r
echo
echo ">>> 如果列表为空，说明作业提交后立刻退出了"
echo "    用 bin/sql-client.sh -s $CONF 进交互模式，手工粘贴 SQL 看报错"
