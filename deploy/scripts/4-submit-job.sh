#!/usr/bin/env bash
# ============================================================================
# 4-submit-job.sh
# 提交 Flink jobs 到集群 (共 2 个 main 类):
#   1. CoordinatorJob   (parallelism=1, 在 master 中央协调 iTree 聚合 + 漂移投票)
#   2. LocalProcessor   (parallelism=8, 在 worker 上做 scoring + per-feature 检测)
#
# 用法 / Usage:
#   bash 4-submit-job.sh                # 提交两个 (默认, 推荐)
#   bash 4-submit-job.sh --only coord   # 只提交 CoordinatorJob (调试用)
#   bash 4-submit-job.sh --only local   # 只提交 LocalProcessor
#   bash 4-submit-job.sh --extra '--hasHeader false'  # 给 LocalProcessor 加额外参数
#
# 顺序 / Order:
#   先 CoordinatorJob 后 LocalProcessor —— 否则 LocalProcessor 一开始发的 iTree
#   没人消费, 头几秒日志会有 "no decision" / "active VOTING never resolved" 噪声.
# ============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEPLOY_DIR="$(dirname "$SCRIPT_DIR")"
set -a; source "$DEPLOY_DIR/.env"; set +a

# 解析参数 / Parse args
ONLY=""
EXTRA_LOCAL_ARGS=""
while [[ $# -gt 0 ]]; do
    case "$1" in
        --only)  ONLY="$2"; shift 2 ;;
        --extra) EXTRA_LOCAL_ARGS="$2"; shift 2 ;;
        *)       echo "Unknown arg: $1"; exit 1 ;;
    esac
done

SSH_OPTS="-i $SSH_KEY -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null"
MASTER_SSH="${NODE_MASTER_PUBLIC_IP:-$NODE_MASTER_IP}"
BROKERS="$NODE_MASTER_IP:9092,$NODE_WORKER1_IP:9092,$NODE_WORKER2_IP:9092"

# 公共 Kafka topic 参数 (两个 job 都用) / Shared topic args
TOPIC_ARGS=(
    --treeTopic "$TOPIC_TREE"
    --modelTopic "$TOPIC_MODEL"
    --featureDriftTopic "$TOPIC_FEATURE_DRIFT"
    --driftRoundTopic "$TOPIC_DRIFT_ROUND"
)
# 字符串化 (传给 ssh)
TOPIC_ARGS_STR="${TOPIC_ARGS[*]}"

# ============================================================================
# 1. CoordinatorJob (parallelism=1, 中央协调)
# ============================================================================
submit_coordinator() {
    echo ""
    echo "===================================="
    echo "[submit] CoordinatorJob (p=$JOB_COORDINATOR_PARALLELISM)"
    echo "===================================="
    # 提交并抓 JobID (从 "Job has been submitted with JobID xxxxx" 提取)
    submit_output=$(ssh $SSH_OPTS "$SSH_USER@$MASTER_SSH" "
        docker exec jobmanager flink run -d \
            -c $JOB_COORDINATOR_MAIN \
            -p $JOB_COORDINATOR_PARALLELISM \
            /opt/flink/usrlib/$JOB_JAR_NAME \
            --broker $BROKERS \
            $TOPIC_ARGS_STR \
            --parallelism $JOB_LOCAL_PARALLELISM \
            --totalTrees $JOB_TOTAL_TREES \
            --votingTimeoutMs $JOB_VOTING_TIMEOUT_MS
    " 2>&1)
    echo "$submit_output"
    coord_jobid=$(echo "$submit_output" | grep -oE 'JobID [a-f0-9]{32}' | awk '{print $2}' | head -1)
    if [[ -z "$coord_jobid" ]]; then
        echo "ERROR: failed to extract JobID from submit output"
        return 1
    fi

    # 用 JobID 轮询状态 (JobID 是唯一标识, 不依赖 job 显示名)
    echo "[wait] CoordinatorJob ($coord_jobid) to be RUNNING ..."
    for _ in $(seq 1 12); do
        status=$(ssh $SSH_OPTS "$SSH_USER@$MASTER_SSH" \
            "docker exec jobmanager flink list 2>&1" \
            | grep -F "$coord_jobid" | grep -oE '\(RUNNING\)|\(FAILED\)|\(FINISHED\)|\(SCHEDULED\)' | head -1 || true)
        if [[ "$status" == "(RUNNING)" ]]; then
            echo "  CoordinatorJob is RUNNING"
            return 0
        fi
        sleep 5
    done
    echo "ERROR: CoordinatorJob not RUNNING after 60s. Check 'flink list' on master."
    exit 1
}

# ============================================================================
# 2. LocalProcessor (parallelism=8, 在 worker TM 上分布执行)
# ============================================================================
submit_local() {
    echo ""
    echo "===================================="
    echo "[submit] LocalProcessor (p=$JOB_LOCAL_PARALLELISM)"
    echo "===================================="
    ssh $SSH_OPTS "$SSH_USER@$MASTER_SSH" "
        docker exec jobmanager flink run -d \
            -c $JOB_LOCAL_MAIN \
            -p $JOB_LOCAL_PARALLELISM \
            /opt/flink/usrlib/$JOB_JAR_NAME \
            --broker $BROKERS \
            --topic $TOPIC_SOURCE \
            $TOPIC_ARGS_STR \
            --scoreTopic $TOPIC_SCORE \
            --parallelism $JOB_LOCAL_PARALLELISM \
            --totalTrees $JOB_TOTAL_TREES \
            --subsampleSize $JOB_SUBSAMPLE_SIZE \
            --ringBufferSize $JOB_RING_BUFFER_SIZE \
            $EXTRA_LOCAL_ARGS
    "
}

# ============================================================================
# 主流程 / Main
# ============================================================================
case "$ONLY" in
    coord)  submit_coordinator ;;
    local)  submit_local ;;
    "")     submit_coordinator; submit_local ;;
    *)      echo "Unknown --only value: $ONLY (use coord|local)"; exit 1 ;;
esac

echo ""
echo "===================================="
echo "Submitted. Verify:"
echo "  Flink UI:  http://$MASTER_SSH:8081"
echo "  CLI:       ssh $SSH_USER@$MASTER_SSH docker exec jobmanager flink list"
echo "===================================="
