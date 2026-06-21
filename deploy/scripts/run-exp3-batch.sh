#!/usr/bin/env bash
# ============================================================================
# run-exp3-batch.sh — 批量跑 EXP3(吞吐口径),每个 run 同步 detectionParallelism
#
# v2 修订(修复卡顿):
#   - 去掉循环内的 clean-topics preflight。原因:
#     (1) clean-topics.sh 默认 RUN_MODE=remote,本脚本未传 RUN_MODE=local → 它走 SSH
#         (mac 密钥路径),在 master 本地跑会卡到 SSH 超时(~20 分钟)才跳过。
#     (2) 多余:output-scores 只要一开始建足 ≥ max(P) 分区,整批共用即可,无需每 P 重建。
#   - 去掉 docker-logs 抓 job_id + tsv:job_id 已由 run-experiment 写进每个 run 的
#     job-config.json(flink_local_job_id),分析脚本直接从结果目录读,无需 tsv。
#
# 前置(批量前一次性做好,本脚本不再动 topic):
#   - source-topic 保持 1 分区(SOURCE_PARTITIONS=1,保时序)。
#   - output-scores 建足分区 ≥ max(parallelisms)(如 6)。确认:
#       docker exec kafka-1 kafka-topics.sh --bootstrap-server <broker> --describe --topic output-scores
#     若不足,本环境 local 直跑可:
#       RUN_MODE=local bash deploy/scripts/clean-topics.sh --score-partitions 6
#   - .env JOB_LOAD_RATE 设吞吐档(如 50000,解 producer 限速)。
#
# 用法(在 master,RUN_MODE=local):
#   bash run-exp3-batch.sh
#   bash run-exp3-batch.sh --datasets "http" --parallelisms "1 2 4 6" --repeats 3
#   bash run-exp3-batch.sh --datasets "donors" --parallelisms "6" --repeats 3   # 补跑
#
# 产物:每 run 一个结果目录(scores.jsonl + job-config.json,后者含 flink_local_job_id)。
# 分析:python3 exp3_throughput_batch.py --prometheus http://localhost:9090
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

TOTAL=0
for ds in $DATASETS; do for p in $PARALLELISMS; do for r in $(seq 1 "$REPEATS"); do
    TOTAL=$((TOTAL+1))
done; done; done

MAXP=0; for p in $PARALLELISMS; do [[ "$p" -gt "$MAXP" ]] && MAXP=$p; done

echo "============================================================"
echo "EXP3 批量(吞吐):datasets=[$DATASETS] P=[$PARALLELISMS] repeats=$REPEATS  共 $TOTAL run"
echo "每 run detectionParallelism=P;source 单线(1 分区)。"
echo "[前置自检] output-scores 分区需 >= ${MAXP}。确认:"
echo "  docker exec kafka-1 kafka-topics.sh --bootstrap-server \$BROKER --describe --topic output-scores"
echo "本脚本不动 topic(已去掉 clean-topics preflight)。JOB_LOAD_RATE 取 .env 当前值。"
echo "============================================================"

i=0; ok=0; fail=0
for ds in $DATASETS; do
  for p in $PARALLELISMS; do
    for r in $(seq 1 "$REPEATS"); do
        i=$((i+1))
        echo ""
        echo "########## [$i/$TOTAL] $ds P=$p run=$r ##########"
        if RUN_MODE="${RUN_MODE:-local}" bash "${SCRIPT_DIR}/run-exp3-single.sh" "$ds" "$p" "$r"; then
            ok=$((ok+1))
            exp_id="${ds}_BACKLOG_THEN_NEW_FOREST_default_p${p}_r${r}_detectionParallelism-${p}_iksWindowSize-2000_iksPValue-0.001_aggK-2"
            echo "  [ok] $exp_id  (job_id 已在 job-config.json)"
        else
            fail=$((fail+1))
            echo "  [ERROR] $ds P=$p r=$r 失败,继续下一个"
        fi
    done
  done
done

echo ""
echo "============================================================"
echo "[完成] 成功 $ok / 失败 $fail / 共 $TOTAL"
echo ""
echo "[分析](跑完尽快,auto-window 回看窗口需覆盖整批跨度):"
echo "  python3 ${SCRIPT_DIR}/exp3_throughput_batch.py --prometheus http://localhost:9090"
echo "  # 只看某数据集: 加 --datasets donors"
echo "  # 批次跨度大、早期 run 被跳过时: 告知 planning 调大 auto-window 回看窗口"
echo "============================================================"
