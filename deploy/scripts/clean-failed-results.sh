#!/usr/bin/env bash
# ============================================================================
# clean-failed-results.sh - 检查并删除未完成的实验结果目录
#
# 未完成判据 (满足任一):
#   - 无 scores.jsonl
#   - scores.jsonl 为空 (0 行)
#   - scores.jsonl 行数 < job-config.json 里的 n_samples (数据不完整)
#
# 默认只列出 (dry-run), 加 --delete 才真删.
#
# 用法 / Usage:
#   bash clean-failed-results.sh                          # 只列出未完成的 (本地默认目录)
#   bash clean-failed-results.sh --results-dir <path>     # 指定目录
#   bash clean-failed-results.sh --delete                 # 确认后真删
#   RUN_MODE=local bash clean-failed-results.sh --results-dir /opt/fa-iforest/results [--delete]
#       (master 上直接查本地结果)
# ============================================================================
set -uo pipefail

RESULTS_DIR=""
DO_DELETE=false
RUN_MODE="${RUN_MODE:-local}"   # 这个脚本默认在结果所在机器本地跑

while [[ $# -gt 0 ]]; do
    case "$1" in
        --results-dir) RESULTS_DIR="$2"; shift 2 ;;
        --delete)      DO_DELETE=true; shift ;;
        *) echo "Unknown arg: $1"; exit 1 ;;
    esac
done

# 默认目录
if [[ -z "$RESULTS_DIR" ]]; then
    if [[ "$RUN_MODE" == "local" && -d /opt/fa-iforest/results ]]; then
        RESULTS_DIR=/opt/fa-iforest/results
    else
        RESULTS_DIR="./results-local"
    fi
fi

[[ ! -d "$RESULTS_DIR" ]] && { echo "ERROR: $RESULTS_DIR 不存在"; exit 1; }

echo "=== 检查未完成的实验结果 ==="
echo "  目录: $RESULTS_DIR"
echo "  模式: $([ "$DO_DELETE" = true ] && echo '删除' || echo '仅列出 (dry-run)')"
echo ""

incomplete=()
total=0; ok=0

for d in "$RESULTS_DIR"/*/; do
    [[ -d "$d" ]] || continue
    total=$((total+1))
    exp_id=$(basename "$d")
    scores="$d/scores.jsonl"

    # 判据 1: 无 scores.jsonl
    if [[ ! -f "$scores" ]]; then
        incomplete+=("$exp_id|无 scores.jsonl")
        continue
    fi

    # 判据 2: 行数
    lines=$(wc -l < "$scores" 2>/dev/null | tr -d ' ')
    lines=${lines:-0}
    if [[ "$lines" -eq 0 ]]; then
        incomplete+=("$exp_id|scores.jsonl 为空 (0 行)")
        continue
    fi

    # 判据 3: 行数 < n_samples (从 job-config.json 读期望行数)
    cfg="$d/job-config.json"
    if [[ -f "$cfg" ]]; then
        n_expected=$(python3 -c "import json; print(json.load(open('$cfg')).get('n_samples',0))" 2>/dev/null || echo 0)
        # 允许少量误差 (Flink 可能丢极少数), 低于 95% 视为不完整
        if [[ "$n_expected" -gt 0 ]]; then
            threshold=$(( n_expected * 95 / 100 ))
            if [[ "$lines" -lt "$threshold" ]]; then
                incomplete+=("$exp_id|行数不足: $lines / $n_expected")
                continue
            fi
        fi
    fi

    ok=$((ok+1))
done

echo "完整: $ok / $total"
echo "未完成: ${#incomplete[@]}"
echo ""

if [[ ${#incomplete[@]} -eq 0 ]]; then
    echo "没有未完成的结果, 无需清理."
    exit 0
fi

echo "=== 未完成列表 ==="
for item in "${incomplete[@]}"; do
    exp_id="${item%%|*}"
    reason="${item##*|}"
    printf "  %-45s %s\n" "$exp_id" "$reason"
done
echo ""

if ! $DO_DELETE; then
    echo "(dry-run: 未删除. 确认无误后加 --delete 真删)"
    exit 0
fi

# 真删 (再确认一次)
echo "即将删除以上 ${#incomplete[@]} 个未完成目录."
read -p "确认删除? [y/N] " confirm
if [[ "$confirm" =~ ^[Yy]$ ]]; then
    for item in "${incomplete[@]}"; do
        exp_id="${item%%|*}"
        rm -rf "${RESULTS_DIR:?}/$exp_id"
        echo "  deleted: $exp_id"
    done
    echo "完成. 删除 ${#incomplete[@]} 个."
else
    echo "取消, 未删除."
fi
