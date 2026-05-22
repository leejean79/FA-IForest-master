#!/usr/bin/env bash
# ============================================================================
# upload-datasets.sh
# 一次性把本地 data/ 下所有数据集上传到 master 节点.
# 之后所有实验直接用 master 上的数据, 不再每次上传.
#
# rsync -az 增量传输: 已传过且没变的文件自动跳过, 重复跑很快.
#
# 用法 / Usage:
#   bash upload-datasets.sh              # 传 data/ 全部
#   bash upload-datasets.sh --dry-run    # 只看要传什么, 不实际传
# ============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEPLOY_DIR="$(dirname "$SCRIPT_DIR")"
PROJECT_ROOT="$(dirname "$DEPLOY_DIR")"
set -a; source "$DEPLOY_DIR/.env"; set +a

DRY_RUN=""
[[ "${1:-}" == "--dry-run" ]] && DRY_RUN="--dry-run"

LOCAL_DATA="$PROJECT_ROOT/data"
REMOTE_DATA="$REMOTE_HOME/datasets"

if [[ ! -d "$LOCAL_DATA" ]]; then
    echo "ERROR: local data dir not found: $LOCAL_DATA"
    echo "请先把数据集整理到 $LOCAL_DATA/{synth,stationary,insects}/"
    exit 1
fi

SSH_OPTS="-i $SSH_KEY -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null"
MASTER_SSH="${NODE_MASTER_PUBLIC_IP:-$NODE_MASTER_IP}"

echo "=== upload-datasets ==="
echo "  local:  $LOCAL_DATA"
echo "  remote: $MASTER_SSH:$REMOTE_DATA"
echo ""

# 本地数据总览
echo "本地数据集 / Local datasets:"
find "$LOCAL_DATA" -type f \( -name "*.csv" -o -name "*.txt" -o -name "*.json" \) \
    | sort | sed "s|$LOCAL_DATA/|  |"
local_size=$(du -sh "$LOCAL_DATA" | cut -f1)
echo "  总大小 / total: $local_size"
echo ""

# 确保远端目录存在
ssh $SSH_OPTS "$SSH_USER@$MASTER_SSH" "mkdir -p $REMOTE_DATA"

# rsync 上传 (增量, 保留目录结构)
echo "Uploading (rsync -az)..."
rsync -az --progress $DRY_RUN \
    -e "ssh $SSH_OPTS" \
    "$LOCAL_DATA/" \
    "$SSH_USER@$MASTER_SSH:$REMOTE_DATA/"

if [[ -n "$DRY_RUN" ]]; then
    echo ""
    echo "(--dry-run: 以上是将要传输的文件, 未实际传输)"
    exit 0
fi

# 验证远端
echo ""
echo "=== verify on master ==="
ssh $SSH_OPTS "$SSH_USER@$MASTER_SSH" "
    cd $REMOTE_DATA
    echo '远端文件 / Remote files:'
    find . -type f \( -name '*.csv' -o -name '*.txt' -o -name '*.json' \) | sort | sed 's|^|  |'
    echo \"远端总大小 / remote total: \$(du -sh . | cut -f1)\"
"

echo ""
echo "DONE. 数据已上传到 master:$REMOTE_DATA"
echo "之后实验直接用, 不需再传 (除非数据集有更新)."
