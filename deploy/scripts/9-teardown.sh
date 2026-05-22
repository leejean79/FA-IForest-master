#!/usr/bin/env bash
# ============================================================================
# 9-teardown.sh
# 停集群; 加 --purge 会同时删除 volumes(ZK/Kafka 数据)和远端目录
# ============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEPLOY_DIR="$(dirname "$SCRIPT_DIR")"
set -a; source "$DEPLOY_DIR/.env"; set +a

PURGE=false
if [[ "${1:-}" == "--purge" ]]; then
    PURGE=true
    echo "WARNING: --purge will delete all Kafka/ZK data and remote files."
    read -p "Continue? [y/N] " confirm
    [[ "$confirm" =~ ^[Yy]$ ]] || { echo "aborted"; exit 0; }
fi

SSH_OPTS="-i $SSH_KEY -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null"
MASTER_SSH="${NODE_MASTER_PUBLIC_IP:-$NODE_MASTER_IP}"
WORKER1_SSH="${NODE_WORKER1_PUBLIC_IP:-$NODE_WORKER1_IP}"
WORKER2_SSH="${NODE_WORKER2_PUBLIC_IP:-$NODE_WORKER2_IP}"

down_node() {
    local host=$1 yml=$2 label=$3 extra_env=$4
    echo "[$label] stopping ..."
    DOWN_ARGS=""
    $PURGE && DOWN_ARGS="-v"
    ssh $SSH_OPTS "$SSH_USER@$host" "
        cd $REMOTE_HOME/compose && \
        $extra_env docker compose -f $yml --env-file ../.env down $DOWN_ARGS || true
    "
    if $PURGE; then
        ssh $SSH_OPTS "$SSH_USER@$host" "rm -rf $REMOTE_HOME"
    fi
}

down_node "$WORKER2_SSH" "docker-compose.worker.yml" "worker-2" "BROKER_ID=3 NODE_SELF_IP=$NODE_WORKER2_IP"
down_node "$WORKER1_SSH" "docker-compose.worker.yml" "worker-1" "BROKER_ID=2 NODE_SELF_IP=$NODE_WORKER1_IP"
down_node "$MASTER_SSH"  "docker-compose.master.yml" "master"   ""

echo ""
echo "Cluster down."
$PURGE && echo "Remote dirs purged."
