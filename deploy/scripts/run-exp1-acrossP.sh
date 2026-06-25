#!/usr/bin/env bash
# ============================================================================
# run-exp1-acrossP.sh — 补跑 EXP1 across-P(坐实 per-feature P-invariance / Table 2b)
#
# 背景:exp1_iks plan 本就含 parallelism_grid [1,2,4],但此前只保留了 P=4 基线;
#       Table 2b 需 INSECTS abrupt/gradual × P{1,2,4} 的 overall_auc + n_committed
#       (主检测器 IKS,BACKLOG),证明 per-feature 检测对并行度 N 不变(消稀释)。
#
# 本脚本:用现成 exp1_iks plan,但只跑 BACKLOG(主模式,Table 2b 用),across P{1,2,4}。
#   规模 = 2 数据集 × P{1,2,4} × 3 repeats = 18 run(不跑 USE_OLD_FOREST,省一半)。
#
# 与 run-batch --plan exp1_iks 的区别:
#   exp1_iks plan 含 2 个 config(USE_OLD + BACKLOG),直接 run-batch 会跑 36 个。
#   本脚本循环 run-experiment,固定 BACKLOG_THEN_NEW_FOREST,只跑 18 个。
#   检测面:EXP1 不扩检测并行度(EXP1 测的是「全局并行度下漂移响应是否稀释」,
#   per-feature 检测天然按 featureId 分,不随全局 P 切分——这正是要验证的 P-invariance,
#   故不加 --detection-follow-parallelism,用默认 detectionParallelism)。
#
# 前置:
#   - 分支 feature/per-feature-hddm_w,jar 已打包上传(per-feature IKS,默认 detector=iks)。
#   - source 1 分区;.env 参数默认(iksWindowSize=2000 等由 plan_extras 带)。
#   - JOB_LOAD_RATE 用 EXP1 既有档(与原 P=4 基线一致,保证可比;不必用 EXP3 的 50000)。
#
# 用法(master,RUN_MODE=local):
#   bash run-exp1-acrossP.sh
#   bash run-exp1-acrossP.sh --datasets "insects_abrupt_imbalanced" --parallelisms "1 2"  # 补部分
# ============================================================================
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DATASETS="insects_abrupt_imbalanced insects_gradual_imbalanced"
PARALLELISMS="1 2 4"
REPEATS=3
CONFIG="BACKLOG_THEN_NEW_FOREST"
EXTRA="iksWindowSize=2000;iksPValue=0.001;aggK=2"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --datasets)     DATASETS="$2"; shift 2 ;;
        --parallelisms) PARALLELISMS="$2"; shift 2 ;;
        --repeats)      REPEATS="$2"; shift 2 ;;
        *) echo "未知参数: $1"; exit 1 ;;
    esac
done

TOTAL=0
for ds in $DATASETS; do for p in $PARALLELISMS; do for r in $(seq 1 "$REPEATS"); do TOTAL=$((TOTAL+1)); done; done; done

echo "============================================================"
echo "EXP1 across-P 补跑(P-invariance / Table 2b)"
echo "  datasets=[$DATASETS]  P=[$PARALLELISMS]  repeats=$REPEATS  config=$CONFIG"
echo "  detector=IKS(default)  共 $TOTAL run"
echo "  注:全局 parallelism 变,检测面按 featureId 分(不随 P 切分)——验证 P-invariance"
echo "============================================================"

i=0; ok=0; fail=0
for ds in $DATASETS; do
  for p in $PARALLELISMS; do
    for r in $(seq 1 "$REPEATS"); do
        i=$((i+1))
        echo ""
        echo "########## [$i/$TOTAL] $ds P=$p run=$r ##########"
        if RUN_MODE="${RUN_MODE:-local}" bash "${SCRIPT_DIR}/run-experiment.sh" \
            --dataset "$ds" --config-id "$CONFIG" --run-id "$r" \
            --parallelism "$p" --extra-param "$EXTRA"; then
            ok=$((ok+1)); echo "  [ok]"
        else
            fail=$((fail+1)); echo "  [ERROR] $ds P=$p r=$r 失败,继续"
        fi
    done
  done
done

echo ""
echo "============================================================"
echo "[完成] 成功 $ok / 失败 $fail / 共 $TOTAL"
echo ""
echo "[分析 1 — overall_auc / n_retrains across-P]:"
echo "  bash analysis/analyze-all.sh    # 产 drift-summary.csv + dilution-summary.csv"
echo "  dilution-summary.csv 按 (arm,parallelism,dataset,pauseMode) 聚合 overall_auc_med 等"
echo "  → 看 overall_auc 是否随 P{1,2,4} 保持(P-invariance 的 AUC 列)"
echo ""
echo "[分析 2 — n_committed across-P(Table 2b 第二列)]:"
echo "  python3 ${SCRIPT_DIR}/../../analysis/count_committed.py --results-dir /opt/fa-iforest/results"
echo "  (从各 run 的 drift-round-topic.jsonl 数 COMMITTED;脚本见同批交付)"
echo "============================================================"
