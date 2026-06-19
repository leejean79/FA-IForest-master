#!/usr/bin/env bash
# ============================================================================
# run-exp3-single.sh — 单跑一个 EXP3 扩展性实验
#
# EXP3 配置(见 HANDOVER_v1.0-exp3.md):
#   - 全局 parallelism = P(打分面 + 其他算子并行;source 因 1 分区单线)
#   - detectionParallelism = P(检测面并行)→ 经 --extra-param 透传
#   - source-topic 保持 1 分区(保数据时序;不改 SOURCE_PARTITIONS)
#   - 主检测器 IKS(默认,不传 --algorithm)
#   - pauseMode BACKLOG_THEN_NEW_FOREST
#
# 用法:
#   bash run-exp3-single.sh <dataset> <parallelism> <run_id>
#   例:bash run-exp3-single.sh donors 2 1
#       bash run-exp3-single.sh http 6 1
#
# 前置:
#   - 已 clean-topics(source 1 分区,output-scores 分区 ≥ P)
#   - P=6 时确认 output-scores 分区 ≥6:bash clean-topics.sh --score-partitions 6
#   - SOURCE_PARTITIONS 保持 1(不设或设 1)
# ============================================================================
set -euo pipefail

DATASET="${1:?用法: run-exp3-single.sh <dataset> <parallelism> <run_id>}"
P="${2:?缺 parallelism}"
RUN_ID="${3:?缺 run_id}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# EXP3 固定:BACKLOG + IKS 参数 + detectionParallelism=P(与全局 P 同步)
CONFIG_ID="BACKLOG_THEN_NEW_FOREST"
EXTRA="detectionParallelism=${P};iksWindowSize=2000;iksPValue=0.001;aggK=2"

echo "============================================================"
echo "EXP3 single run"
echo "  dataset=${DATASET}  parallelism=${P}  detectionParallelism=${P}  run=${RUN_ID}"
echo "  config=${CONFIG_ID}  detector=IKS(default)"
echo "  source: 单分区单线(保时序);打分面+检测面 并行度=${P}"
echo "  extra=${EXTRA}"
echo "============================================================"

# 提醒:output-scores 分区需 ≥ P(否则打分输出分区不足)
echo "[提醒] 确认 output-scores 分区 ≥ ${P}(P=6 需 clean-topics --score-partitions 6)"
echo "[提醒] 确认 SOURCE_PARTITIONS=1(source 保持单分区单线)"
echo ""

RUN_MODE="${RUN_MODE:-local}" bash "${SCRIPT_DIR}/run-experiment.sh" \
    --dataset "${DATASET}" \
    --config-id "${CONFIG_ID}" \
    --run-id "${RUN_ID}" \
    --parallelism "${P}" \
    --extra-param "${EXTRA}"

echo ""
echo "============================================================"
echo "[完成] run 产物在 results 目录,exp_id 含 _default_p${P}_r${RUN_ID}_"
echo ""
echo "[下一步 — 第 1 个 run 必做的校准]"
echo "1. 核对处理时间窗已写入:"
echo "     cat <results>/<exp_id>/job-config.json   # 应含 load_end_ts / process_end_ts"
echo "2. 核对实际并行度(taskmanager 日志或 Flink UI):"
echo "     检测面/打分面 subtask 数应 = ${P};source 实际工作 subtask = 1"
echo "     ssh fa-master \"docker logs taskmanager 2>&1 | grep -E 'Detection parallelism|Per-Feature|Local Processor'\""
echo "3. Prometheus 校准三算子 operator_name(写 PromQL 过滤前必做):"
echo "     在 Prometheus UI(master:9090)查:"
echo "       flink_taskmanager_job_task_operator_numRecordsIn"
echo "     看 operator_name label 实际取值,确认能匹配:"
echo "       source 端到端: =~\".*Kafka Source.*\""
echo "       检测面:        =~\".*Per-Feature.*\""
echo "       打分面:        =~\".*Local Processor.*\""
echo "     (label 值可能转义/截断,以实际为准校准过滤式)"
echo "============================================================"
