#!/usr/bin/env bash
# ============================================================================
# refresh-ips.sh
# 每次实例重启后跑这个, 自动同步 3 台实例的公网 IP 到 .env 和 ~/.ssh/config
#
# 前置 / Prerequisites:
#   1. 已安装阿里云 CLI (aliyun)
#   2. ~/.fa-iforest-aliyun.conf 配好 AccessKey 和 3 个 instance ID
#
# 用法 / Usage:
#   bash refresh-ips.sh           # 拉新 IP + 改 .env + 改 ssh config
#   bash refresh-ips.sh --dry-run # 只打印, 不改任何文件
# ============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEPLOY_DIR="$(dirname "$SCRIPT_DIR")"
ENV_FILE="$DEPLOY_DIR/.env"
SSH_CONFIG="${HOME}/.ssh/config"
CONF_FILE="${HOME}/.fa-iforest-aliyun.conf"

DRY_RUN=false
[[ "${1:-}" == "--dry-run" ]] && DRY_RUN=true

# ---------- 1. 检查前置 / Check prerequisites ----------
if ! command -v aliyun >/dev/null 2>&1; then
    echo "ERROR: aliyun CLI not installed."
    echo "Install on Mac:  brew install aliyun-cli"
    echo "Or download:     https://help.aliyun.com/zh/cli/install-cli"
    exit 1
fi

if [[ ! -f "$CONF_FILE" ]]; then
    cat <<EOF
ERROR: $CONF_FILE not found.

Create it with content like:
-----------------------------------------------
# 阿里云 AccessKey (RAM 子账号, 只读 ECS 权限)
ALIYUN_ACCESS_KEY_ID=LTAI5txxxxxxxxxxxxxxxxxx
ALIYUN_ACCESS_KEY_SECRET=xxxxxxxxxxxxxxxxxxxxxxxxxxxx
ALIYUN_REGION=cn-heyuan

# 3 台实例 ID
MASTER_INSTANCE_ID=i-xxxxxxxxxxxxxxxxxxxx
WORKER1_INSTANCE_ID=i-xxxxxxxxxxxxxxxxxxxx
WORKER2_INSTANCE_ID=i-xxxxxxxxxxxxxxxxxxxx
-----------------------------------------------

Then:  chmod 600 $CONF_FILE
EOF
    exit 1
fi

# 权限检查 / Permission check
if [[ "$(stat -f '%A' "$CONF_FILE" 2>/dev/null || stat -c '%a' "$CONF_FILE")" != "600" ]]; then
    echo "WARN: $CONF_FILE permission not 600. Fixing..."
    chmod 600 "$CONF_FILE"
fi

# shellcheck disable=SC1090
set -a; source "$CONF_FILE"; set +a

# ---------- 2. 配置 aliyun CLI (用环境变量, 不写入持久 profile) ----------
# 阿里云 CLI 通过 ALIBABA_CLOUD_* 环境变量识别认证, 优先级低于 --profile 但高于无配置时报错
export ALIBABA_CLOUD_ACCESS_KEY_ID="$ALIYUN_ACCESS_KEY_ID"
export ALIBABA_CLOUD_ACCESS_KEY_SECRET="$ALIYUN_ACCESS_KEY_SECRET"
export ALIBABA_CLOUD_REGION_ID="$ALIYUN_REGION"

ALIYUN_CMD="aliyun ecs DescribeInstances"

# ---------- 3. 查询每台实例当前 IP ----------
fetch_ip() {
    local instance_id=$1 label=$2
    # 用 --InstanceIds 批量查会有 JSON 嵌套, 单台查更稳定
    local json
    json=$($ALIYUN_CMD --RegionId "$ALIYUN_REGION" --InstanceIds "[\"$instance_id\"]" 2>&1) || {
        echo "ERROR fetching $label ($instance_id): $json" >&2
        return 1
    }

    # 提取公网 IP 和私网 IP (用 python 解析, 避免依赖 jq)
    python3 - <<PYEOF
import json, sys
data = json.loads('''$json''')
inst = data["Instances"]["Instance"][0]
public = inst.get("PublicIpAddress", {}).get("IpAddress", [""])
public = public[0] if public else ""
private = inst.get("VpcAttributes", {}).get("PrivateIpAddress", {}).get("IpAddress", [""])
private = private[0] if private else ""
status = inst.get("Status", "Unknown")
print(f"$label|{public}|{private}|{status}")
PYEOF
}

echo "Fetching current IPs from Aliyun OpenAPI..."
MASTER_INFO=$(fetch_ip "$MASTER_INSTANCE_ID" "master")
WORKER1_INFO=$(fetch_ip "$WORKER1_INSTANCE_ID" "worker1")
WORKER2_INFO=$(fetch_ip "$WORKER2_INSTANCE_ID" "worker2")

parse() { echo "$1" | cut -d'|' -f"$2"; }

MASTER_PUB=$(parse "$MASTER_INFO" 2)
MASTER_PRI=$(parse "$MASTER_INFO" 3)
MASTER_STATUS=$(parse "$MASTER_INFO" 4)
WORKER1_PUB=$(parse "$WORKER1_INFO" 2)
WORKER1_PRI=$(parse "$WORKER1_INFO" 3)
WORKER1_STATUS=$(parse "$WORKER1_INFO" 4)
WORKER2_PUB=$(parse "$WORKER2_INFO" 2)
WORKER2_PRI=$(parse "$WORKER2_INFO" 3)
WORKER2_STATUS=$(parse "$WORKER2_INFO" 4)

echo ""
echo "===================================="
printf "%-10s %-8s %-16s %-16s\n" "Node" "Status" "Public IP" "Private IP"
printf "%-10s %-8s %-16s %-16s\n" "master"  "$MASTER_STATUS"  "$MASTER_PUB"  "$MASTER_PRI"
printf "%-10s %-8s %-16s %-16s\n" "worker1" "$WORKER1_STATUS" "$WORKER1_PUB" "$WORKER1_PRI"
printf "%-10s %-8s %-16s %-16s\n" "worker2" "$WORKER2_STATUS" "$WORKER2_PUB" "$WORKER2_PRI"
echo "===================================="

# 状态检查 / Status check
for s in "$MASTER_STATUS" "$WORKER1_STATUS" "$WORKER2_STATUS"; do
    if [[ "$s" != "Running" ]]; then
        echo "WARN: at least one instance is not Running. Public IP may be empty."
    fi
done

# 空 IP 检查 (节省停机时可能 IP 是空) / Empty IP check
for ip in "$MASTER_PUB" "$WORKER1_PUB" "$WORKER2_PUB"; do
    if [[ -z "$ip" ]]; then
        echo "ERROR: at least one instance has no public IP. Start it first."
        exit 1
    fi
done

# ---------- 4. dry-run 提前退出 ----------
if $DRY_RUN; then
    echo ""
    echo "[DRY RUN] No files modified."
    exit 0
fi

# ---------- 5. 更新 .env ----------
echo ""
echo "Updating $ENV_FILE ..."
sed -i.bak \
    -e "s|^NODE_MASTER_IP=.*|NODE_MASTER_IP=$MASTER_PRI|" \
    -e "s|^NODE_WORKER1_IP=.*|NODE_WORKER1_IP=$WORKER1_PRI|" \
    -e "s|^NODE_WORKER2_IP=.*|NODE_WORKER2_IP=$WORKER2_PRI|" \
    -e "s|^NODE_MASTER_PUBLIC_IP=.*|NODE_MASTER_PUBLIC_IP=$MASTER_PUB|" \
    -e "s|^NODE_WORKER1_PUBLIC_IP=.*|NODE_WORKER1_PUBLIC_IP=$WORKER1_PUB|" \
    -e "s|^NODE_WORKER2_PUBLIC_IP=.*|NODE_WORKER2_PUBLIC_IP=$WORKER2_PUB|" \
    "$ENV_FILE"
rm -f "${ENV_FILE}.bak"
echo "  done."

# ---------- 6. 更新 ~/.ssh/config 中的 fa-master / fa-worker1 / fa-worker2 ----------
update_ssh_block() {
    local alias=$1 newip=$2
    # 找到 "Host $alias" 段下面的 HostName 行, 替换
    # 用 awk: 进入 alias 段 → 直到下一个 Host 块为止, 替换 HostName
    python3 - "$SSH_CONFIG" "$alias" "$newip" <<'PYEOF'
import sys, re
path, alias, newip = sys.argv[1], sys.argv[2], sys.argv[3]
with open(path) as f:
    content = f.read()

# 匹配 "Host fa-master\n ... HostName x.x.x.x" 这种 block 内的 HostName 行
pattern = re.compile(
    r'(Host\s+' + re.escape(alias) + r'\b[^\n]*\n(?:(?!^Host\s).+\n)*?\s*HostName\s+)([^\s]+)',
    re.MULTILINE
)
new_content, n = pattern.subn(r'\g<1>' + newip, content)
if n == 0:
    print(f"  WARN: no '{alias}' block found in {path}, skip")
else:
    with open(path, 'w') as f:
        f.write(new_content)
    print(f"  {alias} -> {newip}")
PYEOF
}

echo "Updating $SSH_CONFIG ..."
update_ssh_block "fa-master"  "$MASTER_PUB"
update_ssh_block "fa-worker1" "$WORKER1_PUB"
update_ssh_block "fa-worker2" "$WORKER2_PUB"

# ---------- 7. 验证连通 / Verify ----------
echo ""
echo "Verifying SSH connectivity..."
for alias in fa-master fa-worker1 fa-worker2; do
    if ssh -o ConnectTimeout=10 -o StrictHostKeyChecking=accept-new "$alias" "hostname" 2>/dev/null; then
        echo "  $alias OK"
    else
        echo "  $alias FAILED (instance may still be booting, retry in 30s)"
    fi
done

echo ""
echo "DONE. Run sync if you want to redeploy:"
echo "  bash $SCRIPT_DIR/1-sync-to-nodes.sh"
