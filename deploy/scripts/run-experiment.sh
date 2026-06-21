#!/usr/bin/env bash
# ============================================================================
# run-experiment.sh - 单次实验完整流程
#
# 9 步: 解析 → 清topic → 备结果目录 → 提交job → 灌数据 → 等完成(stable-K)
#        → 一次性 dump (--max-messages EXPECTED) → 清理
#
# 用法 / Usage:
#   bash run-experiment.sh --dataset http --config-id USE_OLD_FOREST --run-id 1
#   bash run-experiment.sh --dataset donors --config-id BACKLOG_THEN_NEW_FOREST --run-id 1 --parallelism 2
#   bash run-experiment.sh --dataset http --config-id USE_OLD_FOREST --run-id 5 --shuffle  # EXP2
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
DETECTION_FOLLOW_PARALLELISM=false   # EXP3:令检测面并行度 = 全局 parallelism
while [[ $# -gt 0 ]]; do
    case "$1" in
        --dataset)     DATASET="$2"; shift 2 ;;
        --config-id)   CONFIG_ID="$2"; shift 2 ;;
        --run-id)      RUN_ID="$2"; shift 2 ;;
        --parallelism) PARALLELISM="$2"; shift 2 ;;
        --algorithm)   ALGORITHM="$2"; shift 2 ;;
        --shuffle)     SHUFFLE=true; shift ;;
        --extra-param) EXTRA_PARAM="$2"; shift 2 ;;
        --detection-follow-parallelism) DETECTION_FOLLOW_PARALLELISM=true; shift ;;
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

# ---------- 算法 (实验4, 可选) ----------
DETECTOR=""
if [[ -n "$ALGORITHM" && "$ALGORITHM" != "_" ]]; then
    DETECTOR=$($CFG algorithm "$ALGORITHM" detector) || exit 3
fi

# ---------- 构造 exp_id ----------
algo_tag="${ALGORITHM:-default}"; [[ "$algo_tag" == "_" ]] && algo_tag="default"
EXP_ID="${DATASET}_${CONFIG_ID}_${algo_tag}_p${PARALLELISM}_r${RUN_ID}"
[[ -n "$EXTRA_PARAM" ]] && EXP_ID="${EXP_ID}_${EXTRA_PARAM//=/-}"
[[ -n "$EXTRA_PARAM" ]] && EXP_ID="${EXP_ID//;/_}"   # 新增:多参数配对的分号换下划线

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
echo "  config=$CONFIG_ID (pause=$PAUSE_MODE)"
echo "  parallelism=$PARALLELISM  detector=${DETECTOR:-default(per-feature IKS)}"
echo "  extra=${EXTRA_PARAM:-(none)}"
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
# Step 3: 准备结果目录(Fix B:不再启用 ScoreResultDumper 容器,改 Step 7 一次性
#         按 latest-offset 总数 console-consumer --max-messages EXPECTED 收尾,
#         避免空闲超时导致末段卡死时的确定性截断)
# ============================================================================
echo ""
echo "[3/9] prepare result dir ..."
ssh_master "mkdir -p $RESULT_DIR && chmod 777 $RESULT_DIR"
echo "  $RESULT_DIR ready"

# ============================================================================
# Step 4: 提交两个 Flink job (后台)
# extra-param 同时透传给 Coordinator + LocalProcessor:聚合器的 aggK / aggWin /
# refractory 走 CoordinatorJob;iksWindowSize / iksPValue / confirmWin / ksConfirm
# 走 LocalProcessor。Flink ParameterTool 对未知 key 静默忽略,整串直接传两侧即可。
# ============================================================================
echo ""
echo "[4/9] submit jobs ..."

# extra-param 通用翻译:k=v[;k2=v2] → --k v --k2 v2
EXTRA_ARGS=""
if [[ -n "$EXTRA_PARAM" ]]; then
    IFS=';' read -ra PAIRS <<< "$EXTRA_PARAM"
    for pair in "${PAIRS[@]}"; do
        k="${pair%%=*}"; v="${pair##*=}"
        EXTRA_ARGS="$EXTRA_ARGS --$k $v"
    done
fi

# Coordinator (聚合器参数从 EXTRA_ARGS 拿;Flink 忽略未知 key)
COORD_OUT=$(ssh_master "docker exec jobmanager flink run -d \
    -c $JOB_COORDINATOR_MAIN -p $JOB_COORDINATOR_PARALLELISM $JAR \
    --broker $BROKERS \
    --treeTopic $TOPIC_TREE --modelTopic $TOPIC_MODEL \
    --featureDriftTopic $TOPIC_FEATURE_DRIFT --driftRoundTopic $TOPIC_DRIFT_ROUND \
    --parallelism $PARALLELISM --totalTrees $JOB_TOTAL_TREES \
    $EXTRA_ARGS" 2>&1)
COORD_JID=$(echo "$COORD_OUT" | grep -oE 'JobID [a-f0-9]{32}' | awk '{print $2}' | head -1)
[[ -z "$COORD_JID" ]] && {
    echo "coordinator submit failed: $COORD_OUT"
    : # Fix B: dumper container 已废,无清理
    exit 2
}

# 等 Coordinator RUNNING
for _ in $(seq 1 12); do
    st=$(ssh_master "docker exec jobmanager flink list 2>&1" | grep -F "$COORD_JID" | grep -oE '\(RUNNING\)' | head -1)
    [[ "$st" == "(RUNNING)" ]] && break
    sleep 5
done

# LocalProcessor (检测面 + 打分面;无 WARN/投票/HDDM 置信度)
LOCAL_ARGS="--broker $BROKERS --topic $TOPIC_SOURCE \
    --treeTopic $TOPIC_TREE --modelTopic $TOPIC_MODEL \
    --featureDriftTopic $TOPIC_FEATURE_DRIFT --driftRoundTopic $TOPIC_DRIFT_ROUND \
    --scoreTopic $TOPIC_SCORE \
    --hasHeader $HAS_HEADER --hasId $HAS_ID --hasLabel $HAS_LABEL \
    --parallelism $PARALLELISM --totalTrees $JOB_TOTAL_TREES \
    --subsampleSize $JOB_SUBSAMPLE_SIZE --ringBufferSize $JOB_RING_BUFFER_SIZE \
    --pauseMode $PAUSE_MODE"
# detector 参数 (EXP4 才用) 仍允许通过 --algorithm 走 algorithms 段
[[ -n "$DETECTOR" ]] && LOCAL_ARGS="$LOCAL_ARGS --detector $DETECTOR"
# EXP3:检测面并行度跟随全局 parallelism(source 仍受 1 分区限制单线;见 handover §0.2)
[[ "$DETECTION_FOLLOW_PARALLELISM" == "true" ]] && LOCAL_ARGS="$LOCAL_ARGS --detectionParallelism $PARALLELISM"
LOCAL_ARGS="$LOCAL_ARGS $EXTRA_ARGS"

LOCAL_OUT=$(ssh_master "docker exec jobmanager flink run -d \
    -c $JOB_LOCAL_MAIN -p $PARALLELISM $JAR $LOCAL_ARGS" 2>&1)
LOCAL_JID=$(echo "$LOCAL_OUT" | grep -oE 'JobID [a-f0-9]{32}' | awk '{print $2}' | head -1)
[[ -z "$LOCAL_JID" ]] && {
    echo "local submit failed: $LOCAL_OUT"
    : # Fix B: dumper container 已废,无清理
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
# Step 6: 等完成 (Fix B 容差版:offset==N_SAMPLES OR 连续 N 次轮询不动)
#
# 旧版死等 offset==N_SAMPLES → 末段一次伪 COMMITTED → 全 subtask COOLDOWN 卡死
# 且数据不足以让 COOLDOWN 自然完成 → 整步永远超时,scores.jsonl 还被 idle 截断。
# 新版:offset 连续 STABLE_POLLS 次不动 → 视为「管线已尽力」→ 退到 Step 7 一次性
# 收尾;Java 端 Fix A 的 ABORT timer 是部署级兜底(无界流),实验靠这里。
# ============================================================================
echo ""
echo "[6/9] wait for completion (target offset=$N_SAMPLES, stable-K tolerance) ..."
LOAD_TIMEOUT=$(( N_SAMPLES / JOB_LOAD_RATE * 3 / 2 + 60 ))   # 1.5x 灌入时间 + 60s
PROCESS_TIMEOUT=600                                          # 灌完后 10 分钟
DEADLINE=$(( $(date +%s) + LOAD_TIMEOUT + PROCESS_TIMEOUT ))

STABLE_POLLS=5         # 连续 N 次轮询 offset 不动 = 视为完成
#STABLE_POLLS=${STABLE_POLLS:-12}
POLL_INTERVAL=10       # 每 10 秒一轮 → 50s no-progress 即收尾
#POLL_INTERVAL=${POLL_INTERVAL:-10}

last_offset=0
stable_count=0
STATUS=1   # 默认失败, 循环里成功才置 0
while true; do
    now=$(date +%s)
    if [[ $now -gt $DEADLINE ]]; then
        echo "  TIMEOUT after $((LOAD_TIMEOUT + PROCESS_TIMEOUT))s (offset=$last_offset/$N_SAMPLES)"
        STATUS=1; break
    fi
    # 查 output-scores 总 offset (所有 partition 求和)
    offset=$(kcmd kafka-run-class.sh kafka.tools.GetOffsetShell \
        --broker-list "$NODE_MASTER_IP:9092" --topic "$TOPIC_SCORE" 2>/dev/null \
        | awk -F: '{sum+=$3} END{print sum+0}')
    if [[ "$offset" -ge "$N_SAMPLES" ]]; then
        echo "  DONE (offset=$offset >= $N_SAMPLES)"
        last_offset="$offset"
        STATUS=0; break
    fi
    # stable 检测:offset 和上一次相同 → 计数 +1;不同 → 重置
    if [[ "$offset" -eq "$last_offset" ]]; then
        stable_count=$((stable_count + 1))
        if [[ "$stable_count" -ge "$STABLE_POLLS" ]]; then
            echo "  STABLE-EARLY-EXIT (offset=$offset 连续 ${STABLE_POLLS} 次不动, " \
                 "未达目标 $N_SAMPLES;管线可能末段卡死 → 进 dump 收尾)"
            STATUS=0; break
        fi
        pct=$(( offset * 100 / N_SAMPLES ))
        echo "  progress: $offset / $N_SAMPLES ($pct%) [stable ${stable_count}/${STABLE_POLLS}]"
    else
        stable_count=0
        last_offset="$offset"
        pct=$(( offset * 100 / N_SAMPLES ))
        echo "  progress: $offset / $N_SAMPLES ($pct%)"
    fi
    sleep "$POLL_INTERVAL"
done

# 处理期结束时刻(Step 6 等待循环退出);[LOAD_END, PROCESS_END] = 纯处理期,
# 供 analyze.py --mode throughput 圈定 Prometheus 时间窗(排除灌数据期)。
PROCESS_END=$(date +%s)

# ============================================================================
# Step 7: 收集结果 (Fix B:job 停后,按 latest-offset 总数一次性 dump scores)
#
# 旧版用 ScoreResultDumper 容器流式写盘 + 30s idle timeout 退出 → 末段卡死时
# 出现「无新消息 ≥ 30s」会被 idle 超时早退,确定性截断到 ~124k(实测案例)。
# 新版:job 停 → 求 EXPECTED = sum(partition latest offsets) → console-consumer
# --max-messages EXPECTED 读满即停,不靠空闲超时。
# ============================================================================
echo ""
echo "[7/9] collect results ..."
# cancel jobs (停止产生新 score),并等其退出,防 dump 时 producer 仍在追加
ssh_master "docker exec jobmanager flink cancel $LOCAL_JID" >/dev/null 2>&1 || true
ssh_master "docker exec jobmanager flink cancel $COORD_JID" >/dev/null 2>&1 || true
# 等 cancel 落地;5s 通常足够 source/sink 清理
sleep 5

# 求 EXPECTED:各 partition 的 latest offset 之和
EXPECTED=$(kcmd kafka-run-class.sh kafka.tools.GetOffsetShell \
    --broker-list "$NODE_MASTER_IP:9092" --topic "$TOPIC_SCORE" --time -1 2>/dev/null \
    | awk -F: '{s+=$NF} END{print s+0}')
echo "  EXPECTED scores = $EXPECTED (latest offsets sum)"

if [[ "${EXPECTED:-0}" -lt 1 ]]; then
    echo "  ERROR: $TOPIC_SCORE 为空(EXPECTED=0),job 可能从未产分数"
    STATUS=2
else
    # console-consumer --max-messages：读满即停,不靠空闲超时
    # 走 master 容器写出 → 落 master 盘
    echo "  dump $TOPIC_SCORE → scores.jsonl (--max-messages $EXPECTED) ..."
    ssh_master "docker exec kafka-1 kafka-console-consumer.sh \
        --bootstrap-server $NODE_MASTER_IP:9092 \
        --topic $TOPIC_SCORE --from-beginning --max-messages $EXPECTED \
        > $RESULT_DIR/scores.jsonl 2>/dev/null" || true

    score_lines=$(ssh_master "wc -l < $RESULT_DIR/scores.jsonl 2>/dev/null" || echo 0)
    score_lines=$(echo "$score_lines" | tr -d ' ')
    if [[ "${score_lines:-0}" -lt 1 ]]; then
        echo "  ERROR: scores.jsonl 为空(console-consumer 可能没消费到)"
        STATUS=2
    else
        if [[ "$score_lines" -lt "$EXPECTED" ]]; then
            echo "  WARN: dump 不足 ($score_lines / $EXPECTED 行)"
        else
            echo "  scores.jsonl: $score_lines 行 (与 EXPECTED 一致)"
        fi
        # 字段映射: ScoreResult 序列化用 originalSequence/dataPointId,
        # 但 analyze.py 要 seq/id. 逐行 rename。
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
fi

# dump 其他 topic (EXP1 三观测项依赖:scores + model-topic + drift-round-topic + feature-drift-topic)
for t in "$TOPIC_FEATURE_DRIFT" "$TOPIC_DRIFT_ROUND" "$TOPIC_MODEL"; do
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
  \"parallelism\": $PARALLELISM,
  \"detector\": \"${DETECTOR:-default(per-feature IKS)}\",
  \"extra_param\": \"${EXTRA_PARAM}\",
  \"n_samples\": $N_SAMPLES,
  \"final_offset\": $last_offset,
  \"status\": $STATUS,
  \"timestamp\": \"$(date -u +%Y-%m-%dT%H:%M:%SZ)\",
  \"flink_local_job_id\": \"$LOCAL_JID\",
  \"flink_coord_job_id\": \"$COORD_JID\",
  \"load_start_ts\": ${LOAD_START:-0},
  \"load_end_ts\": ${LOAD_END:-0},
  \"process_end_ts\": ${PROCESS_END:-0}
}
EOF"

# JM 日志末尾
ssh_master "docker logs jobmanager 2>&1 | tail -100 > $RESULT_DIR/runtime.log" || true

# ============================================================================
# Step 8: 清理
# ============================================================================
echo ""
echo "[8/9] cleanup ..."
# Fix B: dumper container 已废,无清理
$SHUFFLE && ssh_master "rm -f $ACTIVE_CSV" || true

# ============================================================================
# Step 9: 报告
# ============================================================================
echo ""
echo "[9/9] done. status=$STATUS"
echo "  result: master:$RESULT_DIR"
exit $STATUS
