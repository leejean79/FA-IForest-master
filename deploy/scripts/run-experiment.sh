#!/usr/bin/env bash
# ============================================================================
# run-experiment.sh - 单次实验完整流程
#
# 9 步: 解析 → 清topic → (shuffle) → 起Dumper → 提交job → 灌数据 → 等完成
#        → 收集结果 → 清理
#
# 用法 / Usage:
#   bash run-experiment.sh --dataset http --config-id C1 --run-id 1
#   bash run-experiment.sh --dataset donors --config-id C4 --run-id 1 --parallelism 2
#   bash run-experiment.sh --dataset http --config-id C1 --run-id 5 --shuffle  # 实验2
#
# 退出码 / Exit codes:
#   0 成功 / 1 超时 / 2 job失败 / 3 参数或数据错
# ============================================================================
set -uo pipefail   # 不用 -e: 要自己处理各步失败做清理

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEPLOY_DIR="$(dirname "$SCRIPT_DIR")"
set -a; source "$DEPLOY_DIR/.env"; set +a

CFG="python3 $SCRIPT_DIR/cfg_query.py"

# ---------- 解析参数 ----------
DATASET=""; CONFIG_ID=""; RUN_ID=""; PARALLELISM=""; ALGORITHM=""; SHUFFLE=false
EXTRA_PARAM=""    # sensitivity 用, 形如 ringBufferSize=512
while [[ $# -gt 0 ]]; do
    case "$1" in
        --dataset)     DATASET="$2"; shift 2 ;;
        --config-id)   CONFIG_ID="$2"; shift 2 ;;
        --run-id)      RUN_ID="$2"; shift 2 ;;
        --parallelism) PARALLELISM="$2"; shift 2 ;;
        --algorithm)   ALGORITHM="$2"; shift 2 ;;
        --shuffle)     SHUFFLE=true; shift ;;
        --extra-param) EXTRA_PARAM="$2"; shift 2 ;;
        *) echo "Unknown arg: $1"; exit 3 ;;
    esac
done

[[ -z "$DATASET" || -z "$CONFIG_ID" || -z "$RUN_ID" ]] && {
    echo "ERROR: --dataset --config-id --run-id 必填"; exit 3; }

# parallelism: 命令行 > .env 默认
[[ -z "$PARALLELISM" ]] && PARALLELISM="$JOB_LOCAL_PARALLELISM"

# ---------- 查数据集元数据 ----------
DS_PATH=$($CFG dataset "$DATASET" path) || exit 3
HAS_HEADER=$($CFG dataset "$DATASET" hasHeader)
HAS_ID=$($CFG dataset "$DATASET" hasId)
HAS_LABEL=$($CFG dataset "$DATASET" hasLabel)
N_SAMPLES=$($CFG dataset "$DATASET" n_samples)
ANOMALY_LABEL=$($CFG dataset "$DATASET" anomalyLabel)

# ---------- 查配置 ----------
PAUSE_MODE=$($CFG config "$CONFIG_ID" pauseMode)
WARN_BEHAVIOR=$($CFG config "$CONFIG_ID" warnTimeoutBehavior)

# ---------- 算法 (实验4, 可选) ----------
DETECTOR=""
if [[ -n "$ALGORITHM" && "$ALGORITHM" != "_" ]]; then
    DETECTOR=$($CFG algorithm "$ALGORITHM" detector) || exit 3
fi

# ---------- 构造 exp_id ----------
algo_tag="${ALGORITHM:-default}"; [[ "$algo_tag" == "_" ]] && algo_tag="default"
EXP_ID="${DATASET}_${CONFIG_ID}_${algo_tag}_p${PARALLELISM}_r${RUN_ID}"
[[ -n "$EXTRA_PARAM" ]] && EXP_ID="${EXP_ID}_${EXTRA_PARAM//=/-}"

# ---------- 连接配置 ----------
# RUN_MODE: remote (mac 上, ssh 进集群, 默认) | local (master 上, 本地执行)
RUN_MODE="${RUN_MODE:-remote}"
export RUN_MODE   # 传给子进程 clean-topics.sh
SSH_OPTS="-i ${SSH_KEY:-} -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR"
MASTER="${NODE_MASTER_PUBLIC_IP:-$NODE_MASTER_IP}"
BROKERS="$NODE_MASTER_IP:9092,$NODE_WORKER1_IP:9092,$NODE_WORKER2_IP:9092"
REMOTE_DATA="$REMOTE_HOME/datasets"
RESULT_DIR="$REMOTE_HOME/results/$EXP_ID"
JAR="/opt/flink/usrlib/$JOB_JAR_NAME"

if [[ "$RUN_MODE" == "local" ]]; then
    # master 上本地执行 (无 ssh)
    ssh_master() { bash -c "$*"; }
    kcmd() { docker exec kafka-1 "$@"; }
else
    # mac 上 ssh 进 master
    ssh_master() { ssh $SSH_OPTS "$SSH_USER@$MASTER" "$@"; }
    kcmd() { ssh_master "docker exec kafka-1 $*"; }
fi

echo "════════════════════════════════════════════════════"
echo "EXPERIMENT: $EXP_ID"
echo "  dataset=$DATASET (n=$N_SAMPLES, header=$HAS_HEADER, id=$HAS_ID)"
echo "  config=$CONFIG_ID (pause=$PAUSE_MODE, warn=$WARN_BEHAVIOR)"
echo "  parallelism=$PARALLELISM  detector=${DETECTOR:-default}"
echo "  result → master:$RESULT_DIR"
echo "════════════════════════════════════════════════════"

# 已存在则跳过 (断点续跑)
if ssh_master "test -f $RESULT_DIR/scores.jsonl"; then
    echo "[skip] result already exists: $RESULT_DIR"
    exit 0
fi

# ============================================================================
# Step 1: 清 topic (实验3 时 output-scores partition = parallelism)
# ============================================================================
echo ""
echo "[1/9] clean topics ..."
SCORE_P="$PARALLELISM"   # output-scores partition 同步 parallelism (实验3)
bash "$SCRIPT_DIR/clean-topics.sh" --score-partitions "$SCORE_P" || { echo "clean failed"; exit 2; }

# ============================================================================
# Step 2: 准备数据 (可能 shuffle), 确认 master 上存在
# ============================================================================
echo ""
echo "[2/9] prepare data ..."
REMOTE_CSV="$REMOTE_DATA/${DS_PATH#data/}"   # data/synth/x.csv → datasets/synth/x.csv
if ! ssh_master "test -f $REMOTE_CSV"; then
    echo "ERROR: 数据不在 master: $REMOTE_CSV"
    echo "请先跑: bash deploy/scripts/upload-datasets.sh"
    exit 3
fi

ACTIVE_CSV="$REMOTE_CSV"
if $SHUFFLE; then
    # 实验2: 每个 run 用 run-id 作 seed shuffle (在 master 上做)
    SHUFFLED="$REMOTE_DATA/_shuffled_${EXP_ID}.csv"
    echo "  shuffle (seed=$RUN_ID) → $SHUFFLED"
    if [[ "$HAS_HEADER" == "true" ]]; then
        ssh_master "cd $REMOTE_HOME && python3 - <<'PY'
import csv, random
rows=open('$REMOTE_CSV').read().splitlines()
header, data = rows[0], rows[1:]
random.Random($RUN_ID).shuffle(data)
open('$SHUFFLED','w').write(header+'\n'+'\n'.join(data)+'\n')
PY"
    else
        ssh_master "cd $REMOTE_HOME && python3 - <<'PY'
import random
data=open('$REMOTE_CSV').read().splitlines()
random.Random($RUN_ID).shuffle(data)
open('$SHUFFLED','w').write('\n'.join(data)+'\n')
PY"
    fi
    ACTIVE_CSV="$SHUFFLED"
fi

# ============================================================================
# Step 3: 启动 ScoreResultDumper (临时容器, idle 退出)
# ============================================================================
echo ""
echo "[3/9] start dumper ..."
ssh_master "mkdir -p $RESULT_DIR && chmod 777 $RESULT_DIR"
DUMPER_NAME="dumper-$EXP_ID"
# 30s idle timeout: 给 Phase B 训树留时间
ssh_master "docker run -d --name $DUMPER_NAME --network host --user root \
    -v $RESULT_DIR:/out \
    -v $REMOTE_HOME/jars:/jars \
    fa-iforest/flink:$FLINK_VERSION \
    java -cp /jars/$JOB_JAR_NAME com.leejean.tools.ScoreResultDumper \
    $NODE_MASTER_IP:9092 $TOPIC_SCORE /out/scores.jsonl 30000" >/dev/null 2>&1 \
    || { echo "dumper start failed"; exit 2; }

# 健康检查: 启动 3 秒后确认 Dumper 还活着 (不是启动即挂)
sleep 3
dumper_state=$(ssh_master "docker inspect -f '{{.State.Status}}' $DUMPER_NAME 2>/dev/null")
if [[ "$dumper_state" != "running" ]]; then
    echo "ERROR: dumper 启动即退出 (state=$dumper_state)"
    echo "--- dumper 日志 ---"
    ssh_master "docker logs $DUMPER_NAME 2>&1 | tail -20"
    ssh_master "docker rm $DUMPER_NAME >/dev/null 2>&1" || true
    exit 2
fi
echo "  dumper running ok"

# ============================================================================
# Step 4: 提交两个 Flink job (后台)
# ============================================================================
echo ""
echo "[4/9] submit jobs ..."
# Coordinator
COORD_OUT=$(ssh_master "docker exec jobmanager flink run -d \
    -c $JOB_COORDINATOR_MAIN -p $JOB_COORDINATOR_PARALLELISM $JAR \
    --broker $BROKERS \
    --treeTopic $TOPIC_TREE --modelTopic $TOPIC_MODEL \
    --driftTopic $TOPIC_DRIFT --driftRoundTopic $TOPIC_DRIFT_ROUND \
    --parallelism $PARALLELISM --totalTrees $JOB_TOTAL_TREES \
    --votingTimeoutMs $JOB_VOTING_TIMEOUT_MS" 2>&1)
COORD_JID=$(echo "$COORD_OUT" | grep -oE 'JobID [a-f0-9]{32}' | awk '{print $2}' | head -1)
[[ -z "$COORD_JID" ]] && {
    echo "coordinator submit failed: $COORD_OUT"
    ssh_master "docker stop $DUMPER_NAME >/dev/null 2>&1; docker rm $DUMPER_NAME >/dev/null 2>&1" || true
    exit 2
}

# 等 Coordinator RUNNING
for _ in $(seq 1 12); do
    st=$(ssh_master "docker exec jobmanager flink list 2>&1" | grep -F "$COORD_JID" | grep -oE '\(RUNNING\)' | head -1)
    [[ "$st" == "(RUNNING)" ]] && break
    sleep 5
done

# LocalProcessor (HDDM 参数: pauseMode + warnTimeoutBehavior + 可选 detector + extra)
LOCAL_ARGS="--broker $BROKERS --topic $TOPIC_SOURCE \
    --treeTopic $TOPIC_TREE --modelTopic $TOPIC_MODEL \
    --driftTopic $TOPIC_DRIFT --driftRoundTopic $TOPIC_DRIFT_ROUND \
    --scoreTopic $TOPIC_SCORE \
    --hasHeader $HAS_HEADER --hasId $HAS_ID --hasLabel $HAS_LABEL \
    --parallelism $PARALLELISM --totalTrees $JOB_TOTAL_TREES \
    --subsampleSize $JOB_SUBSAMPLE_SIZE --ringBufferSize $JOB_RING_BUFFER_SIZE \
    --pauseMode $PAUSE_MODE --warnTimeoutBehavior $WARN_BEHAVIOR"
[[ -n "$DETECTOR" ]] && LOCAL_ARGS="$LOCAL_ARGS --detector $DETECTOR"
# sensitivity: extra-param 形如 ringBufferSize=512 → --ringBufferSize 512
if [[ -n "$EXTRA_PARAM" ]]; then
    k="${EXTRA_PARAM%%=*}"; v="${EXTRA_PARAM##*=}"
    LOCAL_ARGS="$LOCAL_ARGS --$k $v"
fi

LOCAL_OUT=$(ssh_master "docker exec jobmanager flink run -d \
    -c $JOB_LOCAL_MAIN -p $PARALLELISM $JAR $LOCAL_ARGS" 2>&1)
LOCAL_JID=$(echo "$LOCAL_OUT" | grep -oE 'JobID [a-f0-9]{32}' | awk '{print $2}' | head -1)
[[ -z "$LOCAL_JID" ]] && {
    echo "local submit failed: $LOCAL_OUT"
    ssh_master "docker stop $DUMPER_NAME >/dev/null 2>&1; docker rm $DUMPER_NAME >/dev/null 2>&1" || true
    ssh_master "docker exec jobmanager flink cancel $COORD_JID" >/dev/null 2>&1 || true
    exit 2
}

echo "  coordinator=$COORD_JID  local=$LOCAL_JID"

# ============================================================================
# Step 5: 灌数据 (前台阻塞跑完, 不用后台 & — 避免 ssh 返回后 SIGHUP 杀掉 producer)
# producer 跑完 = 数据全进 topic; Flink 同时在消费, Step 6 再等它处理完剩余
# ============================================================================
echo ""
echo "[5/9] load data (rate=$JOB_LOAD_RATE) ..."
echo "  producer 前台运行中 (约 $((N_SAMPLES / JOB_LOAD_RATE))s)..."
LOAD_START=$(date +%s)
ssh_master "docker run --rm --network host --user root \
    -v $REMOTE_DATA:/data -v $REMOTE_HOME/jars:/jars \
    fa-iforest/flink:$FLINK_VERSION \
    java -cp /jars/$JOB_JAR_NAME com.leejean.source.FileToKafkaProducer \
    --broker $BROKERS --topic $TOPIC_SOURCE \
    --file /data/${ACTIVE_CSV#$REMOTE_DATA/} --rate $JOB_LOAD_RATE" 2>&1 \
    | grep -E 'Finished|Total|Lines sent' || true
LOAD_END=$(date +%s)
echo "  数据灌入完成 (耗时 $((LOAD_END - LOAD_START))s), source-topic 已就绪"

# ============================================================================
# Step 6: 等完成 (两段超时: LOAD + PROCESS)
# ============================================================================
echo ""
echo "[6/9] wait for completion (target offset=$N_SAMPLES) ..."
LOAD_TIMEOUT=$(( N_SAMPLES / JOB_LOAD_RATE * 3 / 2 + 60 ))   # 1.5x 灌入时间 + 60s
PROCESS_TIMEOUT=600                                          # 灌完后 10 分钟
DEADLINE=$(( $(date +%s) + LOAD_TIMEOUT + PROCESS_TIMEOUT ))

last_offset=0
STATUS=1   # 默认失败, 循环里成功才置 0
while true; do
    now=$(date +%s)
    if [[ $now -gt $DEADLINE ]]; then
        echo "  TIMEOUT after $((LOAD_TIMEOUT + PROCESS_TIMEOUT))s (offset=$last_offset/$N_SAMPLES)"
        STATUS=1; break
    fi
    # 查 output-scores 总 offset (4 partition 求和)
    offset=$(kcmd kafka-run-class.sh kafka.tools.GetOffsetShell \
        --broker-list "$NODE_MASTER_IP:9092" --topic "$TOPIC_SCORE" 2>/dev/null \
        | awk -F: '{sum+=$3} END{print sum+0}')
    last_offset="$offset"
    if [[ "$offset" -ge "$N_SAMPLES" ]]; then
        echo "  DONE (offset=$offset >= $N_SAMPLES)"
        STATUS=0; break
    fi
    pct=$(( offset * 100 / N_SAMPLES ))
    echo "  progress: $offset / $N_SAMPLES ($pct%)"
    sleep 10
done

# ============================================================================
# Step 7: 收集结果
# ============================================================================
echo ""
echo "[7/9] collect results ..."
# cancel jobs (停止产生新 score)
ssh_master "docker exec jobmanager flink cancel $LOCAL_JID" >/dev/null 2>&1 || true
ssh_master "docker exec jobmanager flink cancel $COORD_JID" >/dev/null 2>&1 || true

# 等 Dumper 消费完剩余消息后自然退出 (它 30s idle 自动退; 这里最多等 90s)
echo "  waiting for dumper to finish writing scores.jsonl ..."
for _ in $(seq 1 18); do
    st=$(ssh_master "docker inspect -f '{{.State.Status}}' $DUMPER_NAME 2>/dev/null")
    [[ "$st" != "running" ]] && break
    sleep 5
done

# 校验 scores.jsonl 真的写出来了 (避免 Dumper 假成功)
score_lines=$(ssh_master "wc -l < $RESULT_DIR/scores.jsonl 2>/dev/null" || echo 0)
score_lines=$(echo "$score_lines" | tr -d ' ')
if [[ "${score_lines:-0}" -lt 1 ]]; then
    echo "  ERROR: scores.jsonl 为空或不存在 (Dumper 可能挂了)"
    echo "  --- dumper 日志 ---"
    ssh_master "docker logs $DUMPER_NAME 2>&1 | tail -20" || true
    STATUS=2
else
    echo "  scores.jsonl: $score_lines 行"
    # 字段映射: ScoreResult 序列化用 originalSequence/dataPointId,
    # 但 analyze_old.py 要 seq/id. 逐行 rename, 不改 Java 也不改 analyze_old.py.
    echo "  mapping fields (originalSequence→seq, dataPointId→id) ..."
    ssh_master "python3 - <<'PY'
import json
src='$RESULT_DIR/scores.jsonl'
tmp=src+'.tmp'
with open(src) as fi, open(tmp,'w') as fo:
    for line in fi:
        line=line.strip()
        if not line: continue
        r=json.loads(line)
        if 'originalSequence' in r: r['seq']=r.pop('originalSequence')
        if 'dataPointId' in r:      r['id']=r.pop('dataPointId')
        fo.write(json.dumps(r)+'\n')
import os; os.replace(tmp, src)
PY"
fi

# dump 其他 topic
for t in "$TOPIC_DRIFT" "$TOPIC_DRIFT_ROUND" "$TOPIC_MODEL"; do
    kcmd kafka-console-consumer.sh --bootstrap-server "$NODE_MASTER_IP:9092" \
        --topic "$t" --from-beginning --timeout-ms 5000 2>/dev/null \
        | ssh_master "cat > $RESULT_DIR/$t.jsonl" || true
done

# 复制 driftspec (如果数据集有)
DRIFT_SPEC=$($CFG dataset "$DATASET" drift_spec)
if [[ -n "$DRIFT_SPEC" ]]; then
    REMOTE_SPEC="$REMOTE_DATA/${DRIFT_SPEC#data/}"
    ssh_master "cp $REMOTE_SPEC $RESULT_DIR/driftspec.json 2>/dev/null" || true
fi

# job-config.json
GIT_HASH=$(ssh_master "cd $REMOTE_HOME && git rev-parse --short HEAD 2>/dev/null" || echo "unknown")
ssh_master "cat > $RESULT_DIR/job-config.json <<EOF
{
  \"exp_id\": \"$EXP_ID\",
  \"dataset\": \"$DATASET\",
  \"config_id\": \"$CONFIG_ID\",
  \"pauseMode\": \"$PAUSE_MODE\",
  \"warnTimeoutBehavior\": \"$WARN_BEHAVIOR\",
  \"parallelism\": $PARALLELISM,
  \"detector\": \"${DETECTOR:-default}\",
  \"n_samples\": $N_SAMPLES,
  \"final_offset\": $last_offset,
  \"status\": $STATUS,
  \"timestamp\": \"$(date -u +%Y-%m-%dT%H:%M:%SZ)\"
}
EOF"

# JM 日志末尾
ssh_master "docker logs jobmanager 2>&1 | tail -100 > $RESULT_DIR/runtime.log" || true

# ============================================================================
# Step 8: 清理
# ============================================================================
echo ""
echo "[8/9] cleanup ..."
ssh_master "docker stop $DUMPER_NAME >/dev/null 2>&1; docker rm $DUMPER_NAME >/dev/null 2>&1" || true
$SHUFFLE && ssh_master "rm -f $ACTIVE_CSV" || true

# ============================================================================
# Step 9: 报告
# ============================================================================
echo ""
echo "[9/9] done. status=$STATUS"
echo "  result: master:$RESULT_DIR"
exit $STATUS
