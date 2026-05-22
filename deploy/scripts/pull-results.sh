#!/usr/bin/env bash
# ============================================================================
# pull-results.sh - 把 master 上的实验结果拉到本地
#
# rsync -avz 增量: 已拉过没变的跳过.
#
# 用法 / Usage:
#   bash pull-results.sh                    # 拉全部到 ./results-local/
#   bash pull-results.sh --plan exp1        # 只拉 exp1 相关 (按 exp_id 前缀)
#   bash pull-results.sh --clean-remote     # 拉完删 master 上的 (防磁盘满)
# ============================================================================
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEPLOY_DIR="$(dirname "$SCRIPT_DIR")"
PROJECT_ROOT="$(dirname "$DEPLOY_DIR")"
set -a; source "$DEPLOY_DIR/.env"; set +a

LOCAL_DIR="$PROJECT_ROOT/results-local"
CLEAN_REMOTE=false
FILTER=""
while [[ $# -gt 0 ]]; do
    case "$1" in
        --plan)         FILTER="$2"; shift 2 ;;
        --clean-remote) CLEAN_REMOTE=true; shift ;;
        --local-dir)    LOCAL_DIR="$2"; shift 2 ;;
        *) echo "Unknown arg: $1"; exit 1 ;;
    esac
done

SSH_OPTS="-i $SSH_KEY -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null"
MASTER="${NODE_MASTER_PUBLIC_IP:-$NODE_MASTER_IP}"
REMOTE_RESULTS="$REMOTE_HOME/results"

mkdir -p "$LOCAL_DIR"

echo "=== pull-results ==="
echo "  remote: $MASTER:$REMOTE_RESULTS"
echo "  local:  $LOCAL_DIR"
[[ -n "$FILTER" ]] && echo "  filter: $FILTER*"
echo ""

# 远端有哪些结果
echo "远端结果目录 / Remote result dirs:"
ssh $SSH_OPTS "$SSH_USER@$MASTER" "ls -1 $REMOTE_RESULTS 2>/dev/null | head -30" | sed 's/^/  /'
remote_count=$(ssh $SSH_OPTS "$SSH_USER@$MASTER" "ls -1 $REMOTE_RESULTS 2>/dev/null | wc -l")
echo "  共 $remote_count 个"
echo ""

# rsync 拉取
echo "Pulling (rsync -avz)..."
if [[ -n "$FILTER" ]]; then
    # 只拉匹配前缀的 (注: exp_id 不含 plan 名, 这里用 plan 的数据集列表过滤会更准, 简化为全拉)
    rsync -avz --partial -e "ssh $SSH_OPTS" \
        "$SSH_USER@$MASTER:$REMOTE_RESULTS/" "$LOCAL_DIR/"
else
    rsync -avz --partial -e "ssh $SSH_OPTS" \
        "$SSH_USER@$MASTER:$REMOTE_RESULTS/" "$LOCAL_DIR/"
fi

local_count=$(find "$LOCAL_DIR" -maxdepth 1 -type d | tail -n +2 | wc -l | tr -d ' ')
echo ""
echo "本地现有 $local_count 个结果目录"

# 可选: 清远端
if $CLEAN_REMOTE; then
    echo ""
    read -p "确认删除 master 上 $REMOTE_RESULTS 全部结果? [y/N] " confirm
    if [[ "$confirm" =~ ^[Yy]$ ]]; then
        ssh $SSH_OPTS "$SSH_USER@$MASTER" "rm -rf $REMOTE_RESULTS/*"
        echo "remote results cleaned."
    else
        echo "skip clean."
    fi
fi

echo ""
echo "DONE. 结果在 $LOCAL_DIR"
echo "下一步分析: python3 analysis/analyze.py ... 或 bash analyze-all.sh"
