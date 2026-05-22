#!/usr/bin/env bash
# ============================================================================
# 3-create-topics.sh
# 创建 6 个 Kafka topics, 名字与代码默认对齐:
#   source-topic        partition=1   FileToKafkaProducer -> LocalProcessor
#   tree-topic          partition=1   LocalProcessor.sideOutput -> CoordinatorJob
#   model-topic         partition=1   CoordinatorJob -> LocalProcessor.broadcast
#   output-scores       partition=1   LocalProcessor.sink (异常分数落盘)
#   drift-topic         partition=1   LocalProcessor.sideOutput -> Coordinator
#   drift-round-topic   partition=1   Coordinator -> LocalProcessor.broadcast (投票决议)
#
# 用法 / Usage:
#   bash 3-create-topics.sh            # 创建(已存在则跳过)
#   bash 3-create-topics.sh --recreate # 先删后建(慎用, 数据全丢)
# ============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEPLOY_DIR="$(dirname "$SCRIPT_DIR")"
set -a; source "$DEPLOY_DIR/.env"; set +a

RECREATE=false
[[ "${1:-}" == "--recreate" ]] && RECREATE=true

SSH_OPTS="-i $SSH_KEY -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null"
MASTER_SSH="${NODE_MASTER_PUBLIC_IP:-$NODE_MASTER_IP}"
BROKERS="$NODE_MASTER_IP:9092,$NODE_WORKER1_IP:9092,$NODE_WORKER2_IP:9092"

# 在 master 的 kafka-1 容器内调用 CLI
kcmd() {
    ssh $SSH_OPTS "$SSH_USER@$MASTER_SSH" "docker exec kafka-1 $*"
}

# 6 个 topic (代码 default)
TOPICS=(
    "$TOPIC_SOURCE"
    "$TOPIC_TREE"
    "$TOPIC_MODEL"
    "$TOPIC_SCORE"
    "$TOPIC_DRIFT"
    "$TOPIC_DRIFT_ROUND"
)

if $RECREATE; then
    echo "WARN: --recreate will DELETE all data in topics: ${TOPICS[*]}"
    read -p "Continue? [y/N] " confirm
    [[ "$confirm" =~ ^[Yy]$ ]] || { echo "aborted"; exit 0; }
    for t in "${TOPICS[@]}"; do
        echo "[delete] $t"
        kcmd kafka-topics.sh --bootstrap-server "$BROKERS" --delete --topic "$t" --if-exists || true
    done
    echo "Waiting 10s for delete to propagate..."
    sleep 10
fi

for t in "${TOPICS[@]}"; do
    echo "[create] $t (partitions=$TOPIC_PARTITIONS, replication=$TOPIC_REPLICATION)"
    kcmd kafka-topics.sh --bootstrap-server "$BROKERS" \
        --create --if-not-exists \
        --topic "$t" \
        --partitions "$TOPIC_PARTITIONS" \
        --replication-factor "$TOPIC_REPLICATION"
done

echo ""
echo "[verify] topic list:"
kcmd kafka-topics.sh --bootstrap-server "$BROKERS" --list

echo ""
echo "[verify] topic details (partition & leader distribution):"
kcmd kafka-topics.sh --bootstrap-server "$BROKERS" --describe \
    | grep -E "(Topic:|Leader:)" | head -20

echo ""
echo "===================================="
echo "DONE. Next step:"
echo "  bash $SCRIPT_DIR/4-submit-job.sh"
echo "===================================="
