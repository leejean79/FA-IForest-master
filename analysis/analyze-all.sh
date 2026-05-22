#!/usr/bin/env bash
# ============================================================================
# analyze-all.sh - 批量分析本地实验结果
#
# 扫 results-local/, 按 exp_id 模式归类, 分发给 analyze.py 对应 mode.
#   exp_id = {dataset}_{config}_{algo}_p{N}_r{run}
#
# 分析策略:
#   实验1/4 (有 driftspec): mode drift, 每个 run 一次, 汇总
#   实验2 (多 run 同数据集): mode stationary, --scores-dir
#   实验3 (多 parallelism): mode scalability, --runs-dir
#
# 用法 / Usage:
#   bash analyze-all.sh                          # 分析 ./results-local/
#   bash analyze-all.sh --results-dir <path>
#   bash analyze-all.sh --plan exp1              # 只分析某类实验
# ============================================================================
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEPLOY_DIR="$(dirname "$SCRIPT_DIR")"
PROJECT_ROOT="$(dirname "$DEPLOY_DIR")"

RESULTS_DIR="$PROJECT_ROOT/results-local"
ANALYZE="python3 $PROJECT_ROOT/analysis/analyze.py"
OUT_DIR="$PROJECT_ROOT/analysis-output"
PLAN_FILTER=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        --results-dir) RESULTS_DIR="$2"; shift 2 ;;
        --plan)        PLAN_FILTER="$2"; shift 2 ;;
        *) echo "Unknown arg: $1"; exit 1 ;;
    esac
done

[[ ! -d "$RESULTS_DIR" ]] && { echo "ERROR: $RESULTS_DIR 不存在, 先 pull-results.sh"; exit 1; }
mkdir -p "$OUT_DIR"

echo "=== analyze-all ==="
echo "  results: $RESULTS_DIR"
echo "  output:  $OUT_DIR"
echo ""

# 列出所有结果目录
RESULT_DIRS=()
while IFS= read -r d; do
    [[ -n "$d" ]] && RESULT_DIRS+=("$d")
done < <(find "$RESULTS_DIR" -maxdepth 1 -mindepth 1 -type d | sort)

echo "发现 ${#RESULT_DIRS[@]} 个结果目录"
echo ""

# ---------- 1. drift 分析 (有 driftspec 的 = 实验1/4) ----------
echo "[drift] 分析有 driftspec 的实验 (实验1/4) ..."
drift_count=0
DRIFT_SUMMARY="$OUT_DIR/drift-summary.csv"
echo "exp_id,n,overall_auc,n_versions" > "$DRIFT_SUMMARY"
for d in "${RESULT_DIRS[@]}"; do
    exp_id=$(basename "$d")
    if [[ -f "$d/scores.jsonl" && -f "$d/driftspec.json" ]]; then
        out_json="$OUT_DIR/drift_${exp_id}.json"
        if $ANALYZE --mode drift --scores "$d/scores.jsonl" \
            --driftspec "$d/driftspec.json" --out "$out_json" >/dev/null 2>&1; then
            # 用 analyze.py 真实输出字段: overall_auc, per_version
            python3 -c "
import json
try:
    d=json.load(open('$out_json'))
    auc=d.get('overall_auc','')
    nv=len(d.get('per_version',{})) if isinstance(d.get('per_version'),dict) else ''
    print(f\"$exp_id,{d.get('n','')},{auc},{nv}\")
except Exception:
    print(f'$exp_id,ERROR,,')
" >> "$DRIFT_SUMMARY"
            drift_count=$((drift_count+1))
        fi
    fi
done
echo "  分析了 $drift_count 个 drift 实验 → $DRIFT_SUMMARY"
echo ""

# ---------- 2. stationary 分析 (实验2: 同数据集多 run) ----------
echo "[stationary] 分析 stationary 实验 (实验2) ..."
# 按数据集分组: 把同一 dataset 的所有 run 的 scores 软链到临时目录
STAT_DATASETS=(donors http forestcover fraud mulcross)
STAT_SUMMARY="$OUT_DIR/stationary-summary.csv"
echo "dataset,n_runs,auc_mean,auc_std" > "$STAT_SUMMARY"
for ds in "${STAT_DATASETS[@]}"; do
    tmp_dir="$OUT_DIR/_stat_${ds}"
    rm -rf "$tmp_dir"; mkdir -p "$tmp_dir"
    n=0
    for d in "${RESULT_DIRS[@]}"; do
        exp_id=$(basename "$d")
        # 匹配 {ds}_C1_default_pN_rM (实验2 用 C1)
        if [[ "$exp_id" == ${ds}_* && -f "$d/scores.jsonl" ]]; then
            ln -sf "$d/scores.jsonl" "$tmp_dir/${exp_id}.jsonl"
            n=$((n+1))
        fi
    done
    if [[ "$n" -gt 0 ]]; then
        out_json="$OUT_DIR/stationary_${ds}.json"
        if $ANALYZE --mode stationary --scores-dir "$tmp_dir" \
            --out "$out_json" >/dev/null 2>&1; then
            python3 -c "
import json
try:
    d=json.load(open('$out_json')); s=d.get('summary',{})
    auc=s.get('auc',{})
    print(f\"$ds,$n,{auc.get('mean','')},{auc.get('std','')}\")
except: print(f'$ds,$n,ERROR,')
" >> "$STAT_SUMMARY"
            echo "  $ds: $n runs"
        fi
    fi
    rm -rf "$tmp_dir"
done
echo "  → $STAT_SUMMARY"
echo ""

# ---------- 3. scalability 分析 (实验3: 多 parallelism) ----------
echo "[scalability] 分析 scalability 实验 (实验3) ..."
SCALE_DATASETS=(donors http)
for ds in "${SCALE_DATASETS[@]}"; do
    runs_dir="$OUT_DIR/_scale_${ds}"
    rm -rf "$runs_dir"; mkdir -p "$runs_dir"
    found=false
    for d in "${RESULT_DIRS[@]}"; do
        exp_id=$(basename "$d")
        # 匹配 {ds}_C4_default_p{N}_r{M}, 提取 pN 作子目录
        if [[ "$exp_id" == ${ds}_C4_* && -f "$d/scores.jsonl" ]]; then
            p=$(echo "$exp_id" | grep -oE 'p[0-9]+' | head -1)
            mkdir -p "$runs_dir/$p"
            ln -sf "$d/scores.jsonl" "$runs_dir/$p/$(basename "$d").jsonl"
            found=true
        fi
    done
    if $found; then
        out_json="$OUT_DIR/scalability_${ds}.json"
        $ANALYZE --mode scalability --runs-dir "$runs_dir" \
            --out "$out_json" --out-csv "$OUT_DIR/scalability_${ds}.csv" >/dev/null 2>&1 \
            && echo "  $ds → scalability_${ds}.csv" || echo "  $ds: analyze failed"
    fi
    rm -rf "$runs_dir"
done
echo ""

echo "════════════════════════════════════════════════════"
echo "DONE. 分析结果在 $OUT_DIR/"
echo "  drift-summary.csv       (实验1/4: detection latency, AUC)"
echo "  stationary-summary.csv  (实验2: AUC mean/std)"
echo "  scalability_*.csv       (实验3: 吞吐 vs parallelism)"
echo "════════════════════════════════════════════════════"
