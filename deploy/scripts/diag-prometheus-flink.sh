#!/usr/bin/env bash
# ============================================================================
# diag-prometheus-flink.sh — 诊断 Prometheus 抓取 Flink 9249 metrics 的连通性
#
# 在 master 节点直接跑(它会自动 ssh 到 worker 查):
#   bash diag-prometheus-flink.sh
#
# 目的:定位 Prometheus(master, compose_fa-net bridge)抓不到 Flink 9249 的原因,
#       输出"情形 1/2/3/4"判断 + 对应修复方向。不修改任何东西,只读取诊断。
#
# 前置(强烈建议):先开一个 EXP3 run 让 job RUNNING,再跑本脚本。
#   因为 Flink 9249 只在 job 运行时有 metrics;job 停了所有 9249 都连不上,会误判。
#   开 run:  bash deploy/scripts/run-exp3-single.sh donors 2 1   (另一终端)
#   待看到 job RUNNING 后,本终端跑本脚本。
# ============================================================================
set -uo pipefail

NODES=(162 163 164)          # master / worker1 / worker2 末段
MASTER_IP_PREFIX="172.16.0"
PORT=9249

# SSH 到 worker 的方式:默认用 root@IP。若你的集群用别的用户/密钥,改这里。
ssh_node() {
    local ip="$1"; shift
    if [[ "$ip" == "${MASTER_IP_PREFIX}.162" ]]; then
        bash -c "$*"                          # master 本地执行
    else
        ssh -o StrictHostKeyChecking=no -o ConnectTimeout=5 "root@${ip}" "$*" 2>&1
    fi
}

line() { printf '%.0s=' {1..70}; echo; }

echo; line; echo "Prometheus ↔ Flink 9249 连通性诊断"; line

# ---------------------------------------------------------------------------
echo; echo "### 0. job 是否在运行(9249 仅运行时有数据)"
JOB_RUNNING=$(docker ps --format '{{.Names}}' | grep -iE 'jobmanager|taskmanager' | head -1)
if [[ -n "$JOB_RUNNING" ]]; then
    echo "  Flink 容器在运行: $(docker ps --format '{{.Names}}' | grep -iE 'jobmanager|taskmanager' | tr '\n' ' ')"
    echo "  注意:容器在 ≠ job 在跑。请确认确实有 EXP3 job RUNNING,否则 9249 无数据。"
else
    echo "  [WARN] master 上没看到 Flink 容器。若 TM 全在 worker,属正常。"
fi

# ---------------------------------------------------------------------------
echo; line; echo "### A. 各节点 Flink 容器 9249 端口映射(到宿主机?)"; line
PORT_MAPPED="unknown"
for n in "${NODES[@]}"; do
    ip="${MASTER_IP_PREFIX}.${n}"
    echo "--- 节点 ${ip} ---"
    cname=$(ssh_node "$ip" "docker ps --format '{{.Names}}' | grep -iE 'jobmanager|taskmanager' | head -1")
    if [[ -z "$cname" ]]; then echo "  无 Flink 容器"; continue; fi
    echo "  容器: $cname"
    binds=$(ssh_node "$ip" "docker inspect $cname --format '{{json .HostConfig.PortBindings}}'")
    netmode=$(ssh_node "$ip" "docker inspect $cname --format '{{.HostConfig.NetworkMode}}'")
    echo "  网络模式: $netmode"
    echo "  端口绑定: $binds"
    if echo "$binds" | grep -q "9249"; then
        echo "  → 9249 已映射到宿主机 ✓"; PORT_MAPPED="yes"
    elif [[ "$netmode" == "host" ]]; then
        echo "  → host 网络模式,9249 直接在宿主机 ✓"; PORT_MAPPED="host"
    else
        echo "  → 9249 未映射到宿主机 ✗(bridge 内部可见,宿主机/Prometheus 够不到)"
        [[ "$PORT_MAPPED" == "unknown" ]] && PORT_MAPPED="no"
    fi
done

# ---------------------------------------------------------------------------
echo; line; echo "### C. 从 master 宿主机直连各节点 9249(job 须在跑)"; line
HOST_REACH="unknown"
for n in "${NODES[@]}"; do
    ip="${MASTER_IP_PREFIX}.${n}"
    printf "  %s:%s ... " "$ip" "$PORT"
    if curl -s --max-time 4 "http://${ip}:${PORT}/metrics" | head -1 | grep -q .; then
        echo "通(有 metrics)✓"; HOST_REACH="yes"
    else
        echo "连不上/空 ✗"
        [[ "$HOST_REACH" == "unknown" ]] && HOST_REACH="no"
    fi
done
echo "  (若全空:确认 job 正在 RUNNING;否则可能是端口未映射或安全组未放行)"

# ---------------------------------------------------------------------------
echo; line; echo "### D. Prometheus 容器内能否到达各节点 9249(最关键)"; line
PROM=$(docker ps --format '{{.Names}}' | grep -i prometheus | head -1)
PROM_REACH="unknown"
if [[ -z "$PROM" ]]; then
    echo "  [WARN] 未找到 prometheus 容器"
else
    echo "  prometheus 容器: $PROM"
    promnet=$(docker inspect "$PROM" --format '{{.HostConfig.NetworkMode}}')
    echo "  prometheus 网络模式: $promnet"
    # 容器内可能没 curl,优先 wget,再退 curl;都没有则用 docker run 临时探测同网络
    for n in "${NODES[@]}"; do
        ip="${MASTER_IP_PREFIX}.${n}"
        printf "  prom→%s:%s ... " "$ip" "$PORT"
        out=$(docker exec "$PROM" sh -c "wget -qO- --timeout=4 http://${ip}:${PORT}/metrics 2>/dev/null | head -1 || curl -s --max-time 4 http://${ip}:${PORT}/metrics 2>/dev/null | head -1")
        if echo "$out" | grep -q .; then
            echo "通 ✓"; PROM_REACH="yes"
        else
            echo "不通/空 ✗"
            [[ "$PROM_REACH" == "unknown" ]] && PROM_REACH="no"
        fi
    done
fi

# ---------------------------------------------------------------------------
echo; line; echo "### Prometheus targets 当前状态"; line
curl -s "http://localhost:9090/api/v1/targets" 2>/dev/null \
    | python3 -c "import sys,json
try:
    d=json.load(sys.stdin)
    for t in d['data']['activeTargets']:
        if '9249' in t['scrapeUrl'] or 'flink' in t.get('labels',{}).get('job',''):
            print(f\"  {t['scrapeUrl']}  health={t['health']}  {t.get('lastError','')[:60]}\")
except Exception as e:
    print('  (无法解析 targets:', e, ')')" 2>/dev/null || echo "  (Prometheus API 不可达)"

# ---------------------------------------------------------------------------
echo; line; echo "### 诊断结论"; line
echo "  端口映射(A): $PORT_MAPPED   宿主机直连(C): $HOST_REACH   Prom容器可达(D): $PROM_REACH"
echo
if [[ "$PROM_REACH" == "yes" ]]; then
    echo "  → 情形 1:网络已通,只是 Prometheus 配置 IP 错(.11/.12/.13)。"
    echo "    修:prometheus.yml 的 target IP 改成 .162/.163/.164 → reload。"
elif [[ "$PORT_MAPPED" == "yes" && "$HOST_REACH" == "yes" && "$PROM_REACH" == "no" ]]; then
    echo "  → 情形 2:端口映射OK、宿主机能连,但 Prometheus 容器(bridge)到不了宿主机IP。"
    echo "    修:把 prometheus 容器改 network_mode: host;或用 host.docker.internal/网关IP。"
elif [[ "$PORT_MAPPED" == "no" ]]; then
    echo "  → 情形 3(最常见):Flink 9249 未映射到宿主机,仅容器网络内可见。"
    echo "    修:docker-compose 给 JM/各TM 加 ports: [\"9249:9249\"] → 重建容器。"
    echo "       再配合 prometheus.yml IP 改 .162/.163/.164。"
elif [[ "$HOST_REACH" == "no" && "$PORT_MAPPED" != "no" ]]; then
    echo "  → 情形 4:宿主机间都连不上 9249,疑似 Alibaba Cloud 安全组未放行节点间 9249。"
    echo "    修:安全组放行 VPC 内 9249;再回到情形 1/2/3。"
else
    echo "  → 结果不明确(可能 job 未运行导致 9249 全空)。请确认 EXP3 job RUNNING 后重跑本脚本。"
fi
echo
echo "  把以上完整输出回传给 planning,据此出精确修复 handover。"
line
