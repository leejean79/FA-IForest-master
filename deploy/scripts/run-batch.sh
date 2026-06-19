#!/usr/bin/env bash
# ============================================================================
# run-batch.sh - 批量跑一组实验
#
# 读 experiment-configs.yml 的 plan, 展开成实验列表, 依次串行执行.
# 断点续跑: run-experiment.sh 自己跳过已完成的, 这里再记 progress.log.
#
# 用法 / Usage:
#   bash run-batch.sh --plan exp1               # 跑实验 1 全部 120 次
#   bash run-batch.sh --plan exp2               # 实验 2 (自动 shuffle)
#   bash run-batch.sh --plan exp3               # 实验 3 (变 parallelism)
#   bash run-batch.sh --plan sensitivity        # 敏感性
#   bash run-batch.sh --plan exp1 --dry-run     # 只列要跑的实验, 不执行
# ============================================================================
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEPLOY_DIR="$(dirname "$SCRIPT_DIR")"
set -a; source "$DEPLOY_DIR/.env"; set +a

# RUN_MODE (remote=mac ssh / local=master本地) 传给子进程 run-experiment
export RUN_MODE="${RUN_MODE:-remote}"

CFG="python3 $SCRIPT_DIR/cfg_query.py"

PLAN=""; DRY_RUN=false
while [[ $# -gt 0 ]]; do
    case "$1" in
        --plan)    PLAN="$2"; shift 2 ;;
        --dry-run) DRY_RUN=true; shift ;;
        *) echo "Unknown arg: $1"; exit 1 ;;
    esac
done
[[ -z "$PLAN" ]] && { echo "ERROR: --plan 必填 (exp1|exp2|exp3|exp4|sensitivity)"; exit 1; }

# 实验 2 需要 shuffle (从 plan 的 shuffle 字段判断, 简化: exp2 固定 shuffle)
NEED_SHUFFLE=false
[[ "$PLAN" == "exp2" ]] && NEED_SHUFFLE=true

# 实验 3 扩检测面并行度 = 全局 parallelism (source 仍 1 分区单线; handover §0.2/1.2)
NEED_DETECTION_FOLLOW=false
[[ "$PLAN" == "exp3" ]] && NEED_DETECTION_FOLLOW=true

# 展开 plan → 实验列表
mapfile_compat() {
    # bash 3.2 兼容: 把 plan 展开读进数组
    EXPERIMENTS=()
    while IFS= read -r line; do
        [[ -n "$line" ]] && EXPERIMENTS+=("$line")
    done < <($CFG plan "$PLAN")
}
mapfile_compat

TOTAL=${#EXPERIMENTS[@]}
if [[ "$TOTAL" -eq 0 ]]; then
    echo "ERROR: plan '$PLAN' 展开为空 (可能 enabled=false 或不存在)"
    exit 1
fi

echo "════════════════════════════════════════════════════"
echo "BATCH: $PLAN  ($TOTAL experiments)"
echo "  shuffle: $NEED_SHUFFLE"
echo "  detection-follow-parallelism: $NEED_DETECTION_FOLLOW"
echo "════════════════════════════════════════════════════"

# dry-run: 只列
if $DRY_RUN; then
    i=0
    for exp in "${EXPERIMENTS[@]}"; do
        i=$((i+1))
        printf "  [%3d/%d] %s\n" "$i" "$TOTAL" "$exp"
    done
    echo "(--dry-run: 未执行)"
    exit 0
fi

PROGRESS_LOG="$DEPLOY_DIR/batch-progress-${PLAN}.log"
echo "# batch $PLAN started $(date)" >> "$PROGRESS_LOG"

# 逐个执行
i=0; ok=0; fail=0; skip=0
for exp in "${EXPERIMENTS[@]}"; do
    i=$((i+1))
    # exp 行格式: dataset config algo parallelism run [extra]
    read -r ds cfg algo par run extra <<< "$exp"

    echo ""
    echo "──────────────────────────────────────────"
    echo "[$i/$TOTAL] $ds $cfg algo=$algo p=$par run=$run ${extra:+extra=$extra}"
    echo "──────────────────────────────────────────"

    # 拼 run-experiment.sh 参数
    ARGS=(--dataset "$ds" --config-id "$cfg" --run-id "$run")
    [[ "$algo" != "_" ]] && ARGS+=(--algorithm "$algo")
    [[ "$par" != "_" ]] && ARGS+=(--parallelism "$par")
    $NEED_SHUFFLE && ARGS+=(--shuffle)
    $NEED_DETECTION_FOLLOW && ARGS+=(--detection-follow-parallelism)
    [[ -n "${extra:-}" ]] && ARGS+=(--extra-param "$extra")

    bash "$SCRIPT_DIR/run-experiment.sh" "${ARGS[@]}"
    rc=$?

    case $rc in
        0) ok=$((ok+1));   status="OK" ;;
        1) fail=$((fail+1)); status="TIMEOUT" ;;
        2) fail=$((fail+1)); status="JOBFAIL" ;;
        *) fail=$((fail+1)); status="ERROR($rc)" ;;
    esac
    echo "$(date '+%Y-%m-%d %H:%M:%S') [$i/$TOTAL] $exp → $status" >> "$PROGRESS_LOG"

    # 实验间隔 (让集群喘息, 避免上次残留影响)
    sleep 5
done

echo ""
echo "════════════════════════════════════════════════════"
echo "BATCH $PLAN DONE: ok=$ok fail=$fail (total $TOTAL)"
echo "  progress log: $PROGRESS_LOG"
echo "════════════════════════════════════════════════════"
[[ "$fail" -gt 0 ]] && exit 1 || exit 0
