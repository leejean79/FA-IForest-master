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

# 探测 analyze.py 位置 (脚本放 analysis/ 或 deploy/scripts/ 都能用):
#   1. 同目录 (analyze-all.sh 和 analyze.py 都在 analysis/)
#   2. 上一层的 analysis/ (脚本在 deploy/scripts/, analyze.py 在项目根 analysis/)
#   3. 上两层的 analysis/
if [[ -f "$SCRIPT_DIR/analyze.py" ]]; then
    ANALYZE_PY="$SCRIPT_DIR/analyze.py"
    PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"          # analysis 的上层 = 项目根
elif [[ -f "$(dirname "$SCRIPT_DIR")/analysis/analyze.py" ]]; then
    PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
    ANALYZE_PY="$PROJECT_ROOT/analysis/analyze.py"
elif [[ -f "$(dirname "$(dirname "$SCRIPT_DIR")")/analysis/analyze.py" ]]; then
    PROJECT_ROOT="$(dirname "$(dirname "$SCRIPT_DIR")")"
    ANALYZE_PY="$PROJECT_ROOT/analysis/analyze.py"
else
    echo "ERROR: 找不到 analyze.py (查找了同目录/上层/上两层的 analysis/)"
    exit 1
fi

RESULTS_DIR="$PROJECT_ROOT/results-local"
ANALYZE="python $ANALYZE_PY"
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

# ---------- 1. drift 分析 (有 driftspec 的 = EXP1/EXP4) ----------
# drift-summary.csv 字段 (方向二(a) Phase 3 附录 D):
#   exp_id,dataset,pauseMode,arm,parallelism,n,overall_auc,n_retrains,
#   post_v1_auc,post_final_auc,delta_post_auc,recovery_latency,recovered
# pauseMode/arm/parallelism 从 exp_id 反解 (exp_id = {ds}_{cfg}_{algo}_p{N}_r{run}[_extra])
echo "[drift] 分析有 driftspec 的实验 (EXP1/EXP4) ..."
drift_count=0
DRIFT_SUMMARY="$OUT_DIR/drift-summary.csv"
echo "exp_id,dataset,pauseMode,arm,parallelism,n,overall_auc,n_retrains,post_v1_auc,post_final_auc,delta_post_auc,recovery_latency,recovered" > "$DRIFT_SUMMARY"
for d in "${RESULT_DIRS[@]}"; do
    exp_id=$(basename "$d")
    if [[ -f "$d/scores.jsonl" && -f "$d/driftspec.json" ]]; then
        out_json="$OUT_DIR/drift_${exp_id}.json"
        if $ANALYZE --mode drift --scores "$d/scores.jsonl" \
            --driftspec "$d/driftspec.json" --out "$out_json" >/dev/null 2>&1; then
            # 抽顶级字段 + 从 exp_id 反解 dataset/pauseMode/arm/parallelism
            python3 - <<PY >> "$DRIFT_SUMMARY"
import json, re
exp_id = "$exp_id"
# 反解 {ds}_{cfg}_{algo}_p{N}_r{run}[_extra]
# cfg ∈ {USE_OLD_FOREST, BACKLOG_THEN_NEW_FOREST};兼容旧 C1-C4 (历史结果)
m = re.match(r"^(.+?)_(USE_OLD_FOREST|BACKLOG_THEN_NEW_FOREST|C\d)_(.+?)_p(\d+)_r\d+", exp_id)
if m:
    ds, cfg, algo, p = m.group(1), m.group(2), m.group(3), int(m.group(4))
else:
    ds, cfg, algo, p = "?", "?", "?", -1
# arm: 直接用反解出的检测器名(default=per-feature IKS,HDDM_W=per-feature HDDM_W)。
# 原 new/old 是 HDDM_W 引入前的 build 新旧语义,现已不适用(两检测器同属新 build)。
arm = "iks" if algo in ("default", "?") else algo.lower()
try:
    d = json.load(open("$out_json"))
    fields = [
        exp_id, ds, cfg, arm, str(p),
        str(d.get("n", "")),
        str(d.get("overall_auc", "")),
        str(d.get("n_retrains", "")),
        str(d.get("post_v1_auc", "") or ""),
        str(d.get("post_final_auc", "") or ""),
        str(d.get("delta_post_auc", "") or ""),
        str(d.get("recovery_latency", "") or ""),
        str(d.get("recovered", "")),
    ]
    print(",".join(fields))
except Exception:
    print(f"{exp_id},{ds},{cfg},{arm},{p},ERROR,,,,,,,")
PY
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
        # 匹配 {ds}_USE_OLD_FOREST_default_pN_rM (EXP2 用 USE_OLD_FOREST;兼容旧 C1)
        if [[ ( "$exp_id" == ${ds}_USE_OLD_FOREST_* || "$exp_id" == ${ds}_C1_* ) && -f "$d/scores.jsonl" ]]; then
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
        # 匹配 {ds}_BACKLOG_THEN_NEW_FOREST_default_p{N}_r{M},提取 pN 作子目录;兼容旧 C4
        if [[ ( "$exp_id" == ${ds}_BACKLOG_THEN_NEW_FOREST_* || "$exp_id" == ${ds}_C4_* ) && -f "$d/scores.jsonl" ]]; then
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

# ---------- 4. 消稀释聚合 (方向二(a) Phase 3 附录 D headline) ----------
# 从 drift-summary.csv 按 (arm, parallelism, dataset, pauseMode) 聚合,
# 输出 EXP1 三观测项的跨 run 中位数/std。
# Headline:
#   - 旧法 P↑ 触发→0、新法 P-不变 → 坐实消稀释
#   - delta_post_auc ≤ 0 → COOLDOWN 池 (stale-score 选样) 拖后腿,触发「重训质量」workstream
DILUTION_SUMMARY="$OUT_DIR/dilution-summary.csv"
if [[ -s "$DRIFT_SUMMARY" ]] && [[ $(wc -l < "$DRIFT_SUMMARY") -gt 1 ]]; then
    echo "[dilution] 聚合 EXP1 消稀释指标 ..."
    python3 - <<PY
import csv, statistics
from collections import defaultdict

rows = list(csv.DictReader(open("$DRIFT_SUMMARY")))
# 跳过 ERROR 行
rows = [r for r in rows if r["overall_auc"] not in ("", "ERROR")]

def _f(v):
    try: return float(v)
    except: return None

# group by (arm, parallelism, dataset, pauseMode)
groups = defaultdict(list)
for r in rows:
    key = (r["arm"], r["parallelism"], r["dataset"], r["pauseMode"])
    groups[key].append(r)

def med(vals):
    vs = [v for v in vals if v is not None]
    return f"{statistics.median(vs):.4f}" if vs else ""

def std(vals):
    vs = [v for v in vals if v is not None]
    return f"{statistics.stdev(vs):.4f}" if len(vs) >= 2 else ""

out = open("$DILUTION_SUMMARY", "w")
w = csv.writer(out)
w.writerow([
    "arm", "parallelism", "dataset", "pauseMode", "n_runs",
    "recovered_frac",
    "recovery_latency_med", "recovery_latency_std",
    "n_retrains_med", "n_retrains_std",
    "post_v1_auc_med", "post_final_auc_med",
    "delta_post_auc_med", "delta_post_auc_std",
    "overall_auc_med",
])
for key in sorted(groups.keys()):
    rs = groups[key]
    arm, p, ds, cfg = key
    recovered_vals = [1 if r["recovered"].lower() == "true" else 0 for r in rs]
    recovered_frac = f"{sum(recovered_vals) / len(recovered_vals):.3f}"
    lat = [_f(r["recovery_latency"]) for r in rs]
    nr = [_f(r["n_retrains"]) for r in rs]
    pv1 = [_f(r["post_v1_auc"]) for r in rs]
    pfn = [_f(r["post_final_auc"]) for r in rs]
    dlt = [_f(r["delta_post_auc"]) for r in rs]
    oa  = [_f(r["overall_auc"]) for r in rs]
    w.writerow([
        arm, p, ds, cfg, len(rs),
        recovered_frac,
        med(lat), std(lat),
        med(nr), std(nr),
        med(pv1), med(pfn),
        med(dlt), std(dlt),
        med(oa),
    ])
out.close()
print(f"  {len(groups)} 组 (arm × P × dataset × pauseMode)")
PY
    echo "  → $DILUTION_SUMMARY"
else
    echo "[dilution] drift-summary.csv 为空或无数据行,跳过 dilution 聚合"
fi
echo ""

echo "════════════════════════════════════════════════════"
echo "DONE. 分析结果在 $OUT_DIR/"
echo "  drift-summary.csv       (EXP1/EXP4 per-run: AUC + n_retrains + recovery + v1/final)"
echo "  dilution-summary.csv    (EXP1 headline: pivot by arm × P × dataset × pauseMode)"
echo "  stationary-summary.csv  (EXP2: AUC mean/std)"
echo "  scalability_*.csv       (EXP3: 吞吐 vs parallelism)"
echo "════════════════════════════════════════════════════"
