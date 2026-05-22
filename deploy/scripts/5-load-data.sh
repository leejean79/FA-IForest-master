#!/usr/bin/env bash
# ============================================================================
# 5-load-data.sh
# 把本地数据集 CSV 上传到 master 节点, 然后在 master 跑 FileToKafkaProducer
# 灌入 source_topic
#
# 用法 / Usage:
#   bash 5-load-data.sh <local_csv_path> [rate]
#   bash 5-load-data.sh ~/data/http.csv 500
# ============================================================================
set -euo pipefail

if [[ $# -lt 1 ]]; then
    echo "Usage: $0 <local_csv_path> [rate=200]"
    exit 1
fi

LOCAL_CSV="$1"
RATE="${2:-200}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEPLOY_DIR="$(dirname "$SCRIPT_DIR")"
set -a; source "$DEPLOY_DIR/.env"; set +a

if [[ ! -f "$LOCAL_CSV" ]]; then
    echo "ERROR: file not found: $LOCAL_CSV"
    exit 1
fi

SSH_OPTS="-i $SSH_KEY -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null"
MASTER_SSH="${NODE_MASTER_PUBLIC_IP:-$NODE_MASTER_IP}"
BROKERS="$NODE_MASTER_IP:9092,$NODE_WORKER1_IP:9092,$NODE_WORKER2_IP:9092"

CSV_NAME="$(basename "$LOCAL_CSV")"
REMOTE_CSV="$REMOTE_HOME/datasets/$CSV_NAME"

# 1. rsync 数据集到 master
echo "[1/2] rsync $LOCAL_CSV -> $MASTER_SSH:$REMOTE_CSV"
rsync -az --progress -e "ssh $SSH_OPTS" \
    "$LOCAL_CSV" "$SSH_USER@$MASTER_SSH:$REMOTE_CSV"

# 2. 在 master 上用 java -cp jar 直接调用 FileToKafkaProducer
#    JM 容器里挂了 $REMOTE_HOME/jars 和 $REMOTE_HOME/datasets, 在容器内运行
echo "[2/2] running FileToKafkaProducer @ master (rate=$RATE/sec)"
ssh $SSH_OPTS "$SSH_USER@$MASTER_SSH" "
    docker run --rm \
        --network host \
        -v $REMOTE_HOME/jars:/jars:ro \
        -v $REMOTE_HOME/datasets:/data \
        fa-iforest/flink:$FLINK_VERSION \
        java -cp /jars/$JOB_JAR_NAME com.leejean.source.FileToKafkaProducer \
            --broker $BROKERS \
            --topic $TOPIC_SOURCE \
            --file /data/$CSV_NAME \
            --rate $RATE \
            --fromBeginning true
"

echo ""
echo "===================================="
echo "Data load complete."
echo "Tail Flink TM logs to see DataPoint output:"
echo "  ssh $SSH_USER@$NODE_WORKER1_IP docker logs -f taskmanager-2"
echo "===================================="
