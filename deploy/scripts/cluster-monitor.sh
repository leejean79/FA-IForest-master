#!/usr/bin/env bash
# ============================================================================
# cluster-monitor.sh - 实时集群监控 (watch 风格, Ctrl+C 退出)
#
# 显示: 容器状态 / Flink jobs / topic offset / 节点 CPU内存 / 当前实验进度
# 刷新: 默认 3 秒. ssh ControlMaster 复用连接, 避免每次重连慢+刷屏.
#
# 用法 / Usage:
#   bash cluster-monitor.sh           # 3 秒刷新
#   bash cluster-monitor.sh -i 5      # 5 秒刷新
# ============================================================================
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEPLOY_DIR="$(dirname "$SCRIPT_DIR")"
set -a; source "$DEPLOY_DIR/.env"; set +a

INTERVAL=3
[[ "${1:-}" == "-i" ]] && INTERVAL="${2:-3}"

MASTER="${NODE_MASTER_PUBLIC_IP:-$NODE_MASTER_IP}"
W1="${NODE_WORKER1_PUBLIC_IP:-$NODE_WORKER1_IP}"
W2="${NODE_WORKER2_PUBLIC_IP:-$NODE_WORKER2_IP}"

# ssh ControlMaster: 复用连接, 第一次连后续秒回
CTRL="/tmp/fa-mon-%h"
SSH_OPTS="-i $SSH_KEY -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null \
    -o ControlMaster=auto -o ControlPath=$CTRL -o ControlPersist=60"

s_master() { ssh $SSH_OPTS "$SSH_USER@$MASTER" "$@" 2>/dev/null; }
s_w1()     { ssh $SSH_OPTS "$SSH_USER@$W1" "$@" 2>/dev/null; }
s_w2()     { ssh $SSH_OPTS "$SSH_USER@$W2" "$@" 2>/dev/null; }

# 退出时清理 ssh 复用连接
cleanup() {
    ssh $SSH_OPTS -O exit "$SSH_USER@$MASTER" 2>/dev/null || true
    ssh $SSH_OPTS -O exit "$SSH_USER@$W1" 2>/dev/null || true
    ssh $SSH_OPTS -O exit "$SSH_USER@$W2" 2>/dev/null || true
    echo ""
    echo "monitor stopped."
    exit 0
}
trap cleanup INT TERM

# 节点一行资源摘要 (CPU% / Mem used/total)
node_summary() {
    local sfn="$1"
    # top 取 CPU idle, free 取内存; 一次 ssh 取回
    $sfn "
        cpu=\$(top -bn1 | grep -i '%Cpu' | head -1 | sed 's/.*ni, *//;s/ *id.*//')
        mem=\$(free -m | awk '/^Mem:/{printf \"%d/%dMB\", \$3, \$2}')
        echo \"idle=\${cpu} mem=\${mem}\"
    "
}

draw() {
    clear
    echo "═══════════════════════ FA-iForest Cluster Monitor ═══════════════════════"
    echo " $(date '+%Y-%m-%d %H:%M:%S')   refresh=${INTERVAL}s   (Ctrl+C to exit)"
    echo ""

    # ---- 节点资源 ----
    echo "[节点 / Nodes]"
    local m w1 w2
    m=$(node_summary s_master)
    w1=$(node_summary s_w1)
    w2=$(node_summary s_w2)
    echo "  master   $m"
    echo "  worker1  $w1"
    echo "  worker2  $w2"
    echo ""

    # ---- 容器状态 ----
    echo "[容器 / Containers]"
    {
        s_master "docker ps --format '{{.Names}}|{{.Status}}'" | sed 's/^/  M /'
        s_w1 "docker ps --format '{{.Names}}|{{.Status}}'" | sed 's/^/  1 /'
        s_w2 "docker ps --format '{{.Names}}|{{.Status}}'" | sed 's/^/  2 /'
    } | awk -F'|' '{printf "  %-22s %s\n", $1, $2}'
    echo ""

    # ---- Flink jobs ----
    echo "[Flink Jobs]"
    local jobs
    jobs=$(s_master "docker exec jobmanager flink list 2>/dev/null" \
        | grep -E 'RUNNING|RESTARTING|CREATED' | sed 's/^/  /')
    if [[ -z "$jobs" ]]; then
        echo "  (no running jobs)"
    else
        echo "$jobs" | sed -E 's/ : [a-f0-9]{32} : / : /'   # 隐去长 JobID
    fi
    echo ""

    # ---- Topic offsets ----
    echo "[Kafka Topic Offsets]"
    for t in "$TOPIC_SOURCE" "$TOPIC_TREE" "$TOPIC_MODEL" "$TOPIC_SCORE" "$TOPIC_DRIFT" "$TOPIC_DRIFT_ROUND"; do
        off=$(s_master "docker exec kafka-1 kafka-run-class.sh kafka.tools.GetOffsetShell \
            --broker-list $NODE_MASTER_IP:9092 --topic $t 2>/dev/null \
            | awk -F: '{sum+=\$3} END{print sum+0}'")
        printf "  %-20s %s\n" "$t" "${off:-?}"
    done
    echo ""

    # ---- 当前实验进度 (从最新 result 目录推断) ----
    echo "[当前实验 / Current Experiment]"
    local latest
    latest=$(s_master "ls -t $REMOTE_HOME/results/ 2>/dev/null | head -1")
    if [[ -n "$latest" ]]; then
        echo "  latest result dir: $latest"
        # 看有没有正在跑的 dumper
        local dumper
        dumper=$(s_master "docker ps --format '{{.Names}}' | grep '^dumper-' | head -1")
        [[ -n "$dumper" ]] && echo "  active dumper: $dumper (实验进行中)"
    else
        echo "  (no experiments yet)"
    fi
    echo "═══════════════════════════════════════════════════════════════════════════"
}

echo "Connecting to cluster (first connect may take a few seconds)..."
while true; do
    draw
    sleep "$INTERVAL"
done
