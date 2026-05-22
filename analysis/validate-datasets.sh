#!/usr/bin/env bash
# ============================================================================
# validate-datasets.sh
# 给指定目录下每个 CSV 打 fingerprint:
#   - 文件大小, 行数
#   - 前 3 行 (用来肉眼看 header / 第一列像不像 id)
#   - 列数 (sanity check 是否所有行一致)
#   - 最后一列分布 (推测 label)
#
# 用法 / Usage:
#   bash validate-datasets.sh /path/to/datasets/
#   bash validate-datasets.sh /path/to/datasets/ --single http.csv  # 只测一个
#
# 输出: stdout, 可重定向到文件后贴给 Claude 用于填 datasets.yml
# ============================================================================
set -uo pipefail   # 注意不用 -e, 让单文件出错不阻塞别的

if [[ $# -lt 1 ]]; then
    echo "Usage: $0 <datasets_dir> [--single <filename>]"
    exit 1
fi

DIR="$1"
SINGLE=""
if [[ "${2:-}" == "--single" ]]; then
    SINGLE="${3:-}"
fi

if [[ ! -d "$DIR" ]]; then
    echo "ERROR: $DIR is not a directory"
    exit 1
fi

# 收集 CSV 文件
FILES=()
if [[ -n "$SINGLE" ]]; then
    FILES=("$DIR/$SINGLE")
else
    # 注意: 文件名可能含空格 (你的 INSECTS 就是)
    # mac bash 3.2 没有 mapfile, 用 while read -d '' 兼容
    # 同时匹配 .csv 和 .txt (有些数据集如 mulcross 是 .txt)
    while IFS= read -r -d '' f; do
        FILES+=("$f")
    done < <(find "$DIR" -maxdepth 1 -type f \( -name "*.csv" -o -name "*.txt" \) -print0)
fi

if [[ ${#FILES[@]} -eq 0 ]]; then
    echo "ERROR: no .csv or .txt files in $DIR"
    exit 1
fi

# 检测分隔符: 看第二行 (避免 header 干扰), 哪个分隔符切出的列数最多
# 候选: 逗号 / tab / 空格 (按优先级)
detect_sep() {
    local f="$1"
    local sample
    sample=$(sed -n '2p' "$f")
    # 如果文件只有 1 行, fallback 到第 1 行
    if [[ -z "$sample" ]]; then
        sample=$(sed -n '1p' "$f")
    fi
    local n_comma n_tab n_space
    n_comma=$(echo "$sample" | awk -F',' '{print NF}')
    n_tab=$(echo "$sample" | awk -F'\t' '{print NF}')
    n_space=$(echo "$sample" | awk '{print NF}')   # 默认 FS 是 whitespace

    # 选切出列数最多的
    if [[ "$n_comma" -ge "$n_tab" && "$n_comma" -ge "$n_space" && "$n_comma" -gt 1 ]]; then
        echo ","
    elif [[ "$n_tab" -ge "$n_space" && "$n_tab" -gt 1 ]]; then
        echo $'\t'
    else
        # awk 默认 FS (任意空白)
        echo " "
    fi
}

# 显示分隔符名 (供输出阅读)
sep_name() {
    case "$1" in
        ",") echo "comma" ;;
        $'\t') echo "tab" ;;
        " ") echo "whitespace" ;;
        *) echo "unknown" ;;
    esac
}

# 单个文件的体检函数
check_one() {
    local f="$1"
    local basename
    basename=$(basename "$f")

    echo "========================================"
    echo "FILE: $basename"
    echo "========================================"

    # 大小
    local size
    size=$(stat -f%z "$f" 2>/dev/null || stat -c%s "$f" 2>/dev/null)
    local size_human
    size_human=$(ls -lh "$f" | awk '{print $5}')
    echo "Size: $size bytes ($size_human)"

    # 行数
    local lines
    lines=$(wc -l < "$f")
    echo "Lines: $lines"

    # 分隔符
    local sep
    sep=$(detect_sep "$f")
    echo "Delimiter: $(sep_name "$sep")"

    # 前 3 行
    echo "First 3 lines:"
    head -3 "$f" | sed 's/^/  /'

    # 列数统计 (用前 100 行抽样, 按检测到的分隔符)
    echo "Column count (sample 100 rows):"
    head -100 "$f" | awk -F"$sep" '{print NF}' | sort -u | sed 's/^/  /'

    # 推测 hasHeader: 看第一行是否含非数字字段
    echo "hasHeader guess (first row non-numeric ratio):"
    local first_row
    first_row=$(head -1 "$f")
    local total_fields
    total_fields=$(echo "$first_row" | awk -F"$sep" '{print NF}')
    local non_numeric
    non_numeric=$(echo "$first_row" | awk -F"$sep" '{
        n=0;
        for(i=1;i<=NF;i++) {
            f=$i; sub(/\r$/, "", f);  # 去掉 Windows CRLF 行尾的 \r
            if (f !~ /^-?[0-9]+(\.[0-9]+)?([eE][-+]?[0-9]+)?$/) n++;
        }
        print n
    }')
    if [[ "$non_numeric" -gt 0 && "$total_fields" -gt 0 ]]; then
        local ratio=$((non_numeric * 100 / total_fields))
        echo "  $non_numeric/$total_fields non-numeric fields ($ratio%) → likely hasHeader=true"
    else
        echo "  all numeric → likely hasHeader=false"
    fi

    # 最后一列分布 (label 候选)
    echo "Last column distribution (top 10):"
    awk -F"$sep" '{print $NF}' "$f" | sort | uniq -c | sort -rn | head -10 | sed 's/^/  /'

    # 第一列分布 (id? feature? label?)
    echo "First column sample (top 5 distinct):"
    awk -F"$sep" 'NR>1 {print $1}' "$f" | head -100 | sort -u | head -5 | sed 's/^/  /'

    echo ""
}

# 主循环
for f in "${FILES[@]}"; do
    if [[ ! -f "$f" ]]; then
        echo "WARN: $f not found, skip"
        continue
    fi
    check_one "$f"
done

echo "DONE. Paste output to Claude to fill datasets.yml."
