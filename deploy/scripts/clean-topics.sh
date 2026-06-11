#!/usr/bin/env bash
# ============================================================================
# clean-topics.sh
# 删除并重建全部 6 个 topic, 用于实验间严格隔离 (每次实验从零开始)
#
# 与 3-create-topics.sh 的区别:
#   - 无交互确认 (供 run-experiment.sh 自动调用)
#   - 总是 delete → create (不是 if-not-exists)
#   - output-scores partition 可参数化 (实验 3 随 parallelism 变)
#
# 为什么 delete→create 而不是 alter:
#   Kafka 只允许 partition 增加不允许减少. 实验 3 跑完 p=6 再跑 p=2 时,
#   alter 会失败. delete→create 则任意 partition 数都行.
#
# 用法 / Usage:
#   bash clean-topics.sh                       # output-scores 用 .env 默认 (4)
#   bash clean-topics.sh --score-partitions 2  # 实验 3: output-scores 2 partition
# ============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEPLOY_DIR="$(dirname "$SCRIPT_DIR")"
set -a; source "$DEPLOY_DIR/.env"; set +a

# 解析参数: --score-partitions N
SCORE_PARTITIONS="${TOPIC_SCORE_PARTITIONS:-4}"
while [[ $# -gt 0 ]]; do
    case "$1" in
        --score-partitions) SCORE_PARTITIONS="$2"; shift 2 ;;
        *) echo "Unknown arg: $1"; exit 1 ;;
    esac
done

RUN_MODE="${RUN_MODE:-remote}"
SSH_OPTS="-i ${SSH_KEY:-} -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR"
MASTER_SSH="${NODE_MASTER_PUBLIC_IP:-$NODE_MASTER_IP}"
BROKERS="$NODE_MASTER_IP:9092,$NODE_WORKER1_IP:9092,$NODE_WORKER2_IP:9092"

if [[ "$RUN_MODE" == "local" ]]; then
    kcmd() { docker exec kafka-1 "$@"; }
else
    kcmd() { ssh $SSH_OPTS "$SSH_USER@$MASTER_SSH" "docker exec kafka-1 $*"; }
fi

# 6 个 topic
TOPICS=(
    "$TOPIC_SOURCE"
    "$TOPIC_TREE"
    "$TOPIC_MODEL"
    "$TOPIC_SCORE"
    "$TOPIC_FEATURE_DRIFT"
    "$TOPIC_DRIFT_ROUND"
)

# 单 topic 的 partition 数:
#   - output-scores 用 --score-partitions 参数值 (随 LocalProcessor parallelism 变);
#   - source-topic  用 SOURCE_PARTITIONS (Fork 1 v1=1;Fork 2/EXP3=P_d);
#   - 其余协议性 topic 用 TOPIC_PARTITIONS (=1)。
partitions_for() {
    local t="$1"
    if [[ "$t" == "$TOPIC_SCORE" ]]; then
        echo "$SCORE_PARTITIONS"
    elif [[ "$t" == "$TOPIC_SOURCE" ]]; then
        echo "${SOURCE_PARTITIONS:-1}"
    else
        echo "$TOPIC_PARTITIONS"
    fi
}

echo "=== clean-topics: delete all 6 topics ==="
for t in "${TOPICS[@]}"; do
    echo "[delete] $t"
    kcmd kafka-topics.sh --bootstrap-server "$BROKERS" --delete --topic "$t" --if-exists || true
done

# 等删除传播 (Kafka 删 topic 是异步的, 立刻重建会撞 "topic marked for deletion")
echo "Waiting 10s for delete to propagate..."
sleep 10

echo ""
echo "=== clean-topics: recreate all 6 topics ==="
for t in "${TOPICS[@]}"; do
    p=$(partitions_for "$t")
    echo "[create] $t (partitions=$p, replication=$TOPIC_REPLICATION)"
    # 重试机制: 删除可能还没完全传播, 失败重试 3 次
    for attempt in 1 2 3; do
        if kcmd kafka-topics.sh --bootstrap-server "$BROKERS" \
            --create --topic "$t" \
            --partitions "$p" \
            --replication-factor "$TOPIC_REPLICATION" 2>/dev/null; then
            break
        fi
        if [[ "$attempt" -lt 3 ]]; then
            echo "  create failed (topic maybe still deleting), retry $attempt/3 after 5s..."
            sleep 5
        else
            echo "  ERROR: failed to create $t after 3 attempts"
            exit 1
        fi
    done
done

echo ""
echo "=== verify ==="
kcmd kafka-topics.sh --bootstrap-server "$BROKERS" --list | grep -vE '^__' || true
echo "DONE. output-scores partitions = $SCORE_PARTITIONS"
