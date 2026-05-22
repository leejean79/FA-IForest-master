#!/usr/bin/env bash
# ============================================================================
# 2-up-all.sh
# 远程启动各节点的 docker compose
# 顺序: master(ZK 先起) → worker-1 → worker-2
# ============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEPLOY_DIR="$(dirname "$SCRIPT_DIR")"

set -a; source "$DEPLOY_DIR/.env"; set +a

SSH_OPTS="-i $SSH_KEY -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null"
MASTER_SSH="${NODE_MASTER_PUBLIC_IP:-$NODE_MASTER_IP}"
WORKER1_SSH="${NODE_WORKER1_PUBLIC_IP:-$NODE_WORKER1_IP}"
WORKER2_SSH="${NODE_WORKER2_PUBLIC_IP:-$NODE_WORKER2_IP}"

# 是否启用监控 profile
COMPOSE_PROFILES=""
if [[ "${ENABLE_MONITORING:-false}" == "true" ]]; then
    COMPOSE_PROFILES="--profile monitoring"
fi

# ---------- master 起 ZK + Kafka-1 + Flink JM + 监控 ----------
echo "[master] starting ZK + kafka-1 + jobmanager ..."
ssh $SSH_OPTS "$SSH_USER@$MASTER_SSH" "
    cd $REMOTE_HOME/compose && \
    docker compose -f docker-compose.master.yml --env-file ../.env $COMPOSE_PROFILES up -d
"

# 等 ZK + Kafka-1 真正起来再起 worker 上的 broker, 否则 broker 注册会失败
echo "[wait] zookeeper + kafka-1 ready ..."
for i in $(seq 1 30); do
    if ssh $SSH_OPTS "$SSH_USER@$MASTER_SSH" \
        "docker exec kafka-1 kafka-broker-api-versions.sh --bootstrap-server $NODE_MASTER_IP:9092" \
        > /dev/null 2>&1; then
        echo "  ready after ${i}0s"
        break
    fi
    sleep 10
    if [[ $i -eq 30 ]]; then
        echo "ERROR: kafka-1 not ready in 300s, abort"
        exit 1
    fi
done

# ---------- worker-1: kafka-2 + TM ----------
echo "[worker-1] starting kafka-2 + taskmanager ..."
ssh $SSH_OPTS "$SSH_USER@$WORKER1_SSH" "
    cd $REMOTE_HOME/compose && \
    BROKER_ID=2 NODE_SELF_IP=$NODE_WORKER1_IP \
        docker compose -f docker-compose.worker.yml --env-file ../.env up -d
"

# ---------- worker-2: kafka-3 + TM ----------
echo "[worker-2] starting kafka-3 + taskmanager ..."
ssh $SSH_OPTS "$SSH_USER@$WORKER2_SSH" "
    cd $REMOTE_HOME/compose && \
    BROKER_ID=3 NODE_SELF_IP=$NODE_WORKER2_IP \
        docker compose -f docker-compose.worker.yml --env-file ../.env up -d
"

echo ""
echo "===================================="
echo "Cluster up. Verify:"
echo "  Flink UI:   http://$MASTER_SSH:8081"
if [[ "${ENABLE_MONITORING:-false}" == "true" ]]; then
echo "  Prometheus: http://$MASTER_SSH:$PROMETHEUS_PORT"
echo "  Grafana:    http://$MASTER_SSH:$GRAFANA_PORT  (admin / $GRAFANA_ADMIN_PASSWORD)"
fi
echo ""
echo "Next step:"
echo "  bash $SCRIPT_DIR/3-create-topics.sh"
echo "===================================="
