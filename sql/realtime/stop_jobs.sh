#!/bin/bash
# =============================================================================
# 停止三个实时作业
# 只取消作业名匹配 rt_job_* 的作业，不动集群上其他作业。
# =============================================================================
FLINK_HOME=${FLINK_HOME:-/opt/module/flink}

echo ">>> 当前作业"
$FLINK_HOME/bin/flink list -r 2>/dev/null || true

IDS=$($FLINK_HOME/bin/flink list -r 2>/dev/null \
      | grep -E "rt_job_a_ods|rt_job_b_dwd|rt_job_c_dws|rt_job_d_ads" \
      | awk -F' : ' '{print $2}' \
      | tr -d ' ')

if [ -z "$IDS" ]; then
    echo "没有匹配的作业在运行"
    exit 0
fi

for id in $IDS; do
    echo ">>> cancel $id"
    $FLINK_HOME/bin/flink cancel "$id" || true
done

echo
$FLINK_HOME/bin/flink list -r
