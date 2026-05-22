#!/usr/bin/env bash
# ============================================================================
# 1-sync-to-nodes.sh
# 将本地构建产物 rsync 到 3 个节点
#   - master: compose 全套 + jar + prometheus + flink 镜像
#   - workers: compose 子集 + flink 镜像
# ============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEPLOY_DIR="$(dirname "$SCRIPT_DIR")"

set -a; source "$DEPLOY_DIR/.env"; set +a

BUILD_DIR="$DEPLOY_DIR/.build"
SSH_OPTS="-i $SSH_KEY -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null"

# ssh 跳板地址(优先用公网, 没填就用内网, 那种情况一般是从同 VPC 跳板机上跑)
resolve_ssh_host() {
    local internal=$1 public=$2
    echo "${public:-$internal}"
}

MASTER_SSH=$(resolve_ssh_host "$NODE_MASTER_IP" "${NODE_MASTER_PUBLIC_IP:-}")
WORKER1_SSH=$(resolve_ssh_host "$NODE_WORKER1_IP" "${NODE_WORKER1_PUBLIC_IP:-}")
WORKER2_SSH=$(resolve_ssh_host "$NODE_WORKER2_IP" "${NODE_WORKER2_PUBLIC_IP:-}")

# ---------- 各节点都需要的 ----------
sync_common() {
    local host=$1
    echo "  [common -> $host]"
    ssh $SSH_OPTS "$SSH_USER@$host" "mkdir -p $REMOTE_HOME/{compose,jars,datasets,monitoring,logs}"
    rsync -az -e "ssh $SSH_OPTS" \
        "$DEPLOY_DIR/.env" \
        "$SSH_USER@$host:$REMOTE_HOME/.env"
    rsync -az -e "ssh $SSH_OPTS" \
        "$BUILD_DIR/fa-iforest-flink.tar" \
        "$SSH_USER@$host:$REMOTE_HOME/fa-iforest-flink.tar"
}

# ---------- master 专属 ----------
sync_master() {
    echo "[master @ $MASTER_SSH]"
    sync_common "$MASTER_SSH"
    rsync -az -e "ssh $SSH_OPTS" \
        "$DEPLOY_DIR/compose/docker-compose.master.yml" \
        "$SSH_USER@$MASTER_SSH:$REMOTE_HOME/compose/"
    rsync -az -e "ssh $SSH_OPTS" \
        "$BUILD_DIR/$JOB_JAR_NAME" \
        "$SSH_USER@$MASTER_SSH:$REMOTE_HOME/jars/"
    rsync -az -e "ssh $SSH_OPTS" \
        "$BUILD_DIR/prometheus.yml" \
        "$SSH_USER@$MASTER_SSH:$REMOTE_HOME/monitoring/prometheus.yml"
}

# ---------- worker 专属 ----------
sync_worker() {
    local host=$1 label=$2
    echo "[$label @ $host]"
    sync_common "$host"
    rsync -az -e "ssh $SSH_OPTS" \
        "$DEPLOY_DIR/compose/docker-compose.worker.yml" \
        "$SSH_USER@$host:$REMOTE_HOME/compose/"
}

sync_master
sync_worker "$WORKER1_SSH" "worker-1"
sync_worker "$WORKER2_SSH" "worker-2"

# 在各节点加载 docker 镜像(rsync 已传过去了)
load_image() {
    local host=$1
    echo "[load image @ $host]"
    ssh $SSH_OPTS "$SSH_USER@$host" \
        "docker load -i $REMOTE_HOME/fa-iforest-flink.tar"
}

load_image "$MASTER_SSH"
load_image "$WORKER1_SSH"
load_image "$WORKER2_SSH"

echo ""
echo "===================================="
echo "DONE. Next step:"
echo "  bash $SCRIPT_DIR/2-up-all.sh"
echo "===================================="
