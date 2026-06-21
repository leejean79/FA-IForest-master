#!/usr/bin/env bash
# ============================================================================
# run-exp3-batch.sh — 批量跑 EXP3(吞吐口径),每个 run 正确同步 detectionParallelism
#
# 为什么不用 run-batch.sh --plan exp3:
#   run-batch 从 plan 读固定的 extra(aggK=2),无法让 detectionParallelism 随并行度变,
#   导致检测面恒为单并行度、检测面吞吐口径测不出扩展。本脚本循环调 run-exp3-single.sh
#   (已正确设 detectionParallelism=P),逐 run 同步。
#
# 用法(在 master,RUN_MODE=local):
#   bash run-exp3-batch.sh
#   bash run-exp3-batch.sh --datasets "donors http" --parallelisms "1 2 4 6" --repeats 3
#
# 前置:
#   - .env JOB_LOAD_RATE 已设为吞吐档(如 50000,解 producer 限速)。本脚本不改 .env。
#   - source 保持 1 分区(SOURCE_PARTITIONS=1,保时序)。
#   - P=6 需 output-scores 分区 ≥6:跑前 bash clean-topics.sh --score-partitions 6(或脚本自动,见下)。
#   - run-exp3-single.sh 与本脚本在同目录(deploy/scripts/)。
#
# 产物:每个 run 一个结果目录(含 scores.jsonl + job-config.json),
#       并把每个 run 的 job_id 追加进 exp3-jobids.tsv(供后续 Prometheus 分析对应)。
# ============================================================================
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DATASETS="donors http"
PARALLELISMS="1 2 4 6"
REPEATS=3

while [[ $# -gt 0 ]]; do
    case "$1" in
        --datasets)     DATASETS="$2"; shift 2 ;;
        --parallelisms) PARALLELISMS="$2"; shift 2 ;;
        --repeats)      REPEATS="$2"; shift 2 ;;
        *) echo "未知参数: $1"; exit 1 ;;
    esac
done

JOBIDS_FILE="${SCRIPT_DIR}/exp3-jobids.tsv"
echo -e "exp_id\tdataset\tparallelism\trun\tjob_id" > "$JOBIDS_FILE"

TOTAL=0
for ds in $DATASETS; do for p in $PARALLELISMS; do for r in $(seq 1 "$REPEATS"); do
    TOTAL=$((TOTAL+1))
done; done; done

echo "============================================================"
echo "EXP3 批量(吞吐):datasets=[$DATASETS] P=[$PARALLELISMS] repeats=$REPEATS"
echo "总计 $TOTAL run。每 run detectionParallelism=P,source 单线。"
echo "JOB_LOAD_RATE 取 .env 当前值(应为吞吐档 50000)。"
echo "============================================================"

i=0
for ds in $DATASETS; do
  for p in $PARALLELISMS; do
    # P=6(或 >score 分区)时确保 output-scores 分区足够
    if [[ "$p" -ge 6 ]]; then
        echo "[preflight] P=$p:确保 output-scores 分区 ≥ $p"
        bash "${SCRIPT_DIR}/clean-topics.sh" --score-partitions "$p" 2>/dev/null || \
            echo "  [warn] clean-topics 失败,确认手动已建足够分区"
    fi
    for r in $(seq 1 "$REPEATS"); do
        i=$((i+1))
        echo ""
        echo "########## [$i/$TOTAL] $ds P=$p run=$r ##########"
        RUN_MODE="${RUN_MODE:-local}" bash "${SCRIPT_DIR}/run-exp3-single.sh" "$ds" "$p" "$r"
        rc=$?
        if [[ $rc -ne 0 ]]; then
            echo "[ERROR] $ds P=$p r=$r 失败(rc=$rc),继续下一个"
            continue
        fi
        # 抓本 run 的 job_id(从 jobmanager 日志最近一次提交)
        exp_id="${ds}_BACKLOG_THEN_NEW_FOREST_default_p${p}_r${r}_detectionParallelism-${p}_iksWindowSize-2000_iksPValue-0.001_aggK-2"
        jid=$(docker logs jobmanager 2>&1 | grep -oE "Job [0-9a-f]{32}" | tail -1 | awk '{print $2}')
        echo -e "${exp_id}\t${ds}\t${p}\t${r}\t${jid:-UNKNOWN}" >> "$JOBIDS_FILE"
        echo "  job_id=${jid:-UNKNOWN}  (已记入 $JOBIDS_FILE)"
    done
  done
done

echo ""
echo "============================================================"
echo "[完成] $TOTAL run。job_id 映射表:$JOBIDS_FILE"
echo ""
echo "[下一步 — 吞吐分析(在 master,job 跑完后尽快,趁 Prometheus 数据未过期)]"
echo "逐 run 用 exp3_throughput_prom.py + 上表的 job_id。批量分析示例:"
echo ""
echo "  while IFS=\$'\\t' read -r exp_id ds p run jid; do"
echo "    [[ \"\$exp_id\" == exp_id ]] && continue   # 跳表头"
echo "    [[ \"\$jid\" == UNKNOWN ]] && continue"
echo "    echo \"=== \$ds P=\$p r=\$run ===\""
echo "    python3 ${SCRIPT_DIR}/exp3_throughput_prom.py \\"
echo "        --prometheus http://localhost:9090 \\"
echo "        --job-id \"\$jid\" --auto-window --parallelism \"\$p\" \\"
echo "        --out results/\${exp_id}/throughput.json"
echo "  done < $JOBIDS_FILE"
echo ""
echo "  汇总三口径 vs 并行度后,即得吞吐扩展性(端到端受单分区限、检测面随 P 变化)。"
echo "============================================================"
