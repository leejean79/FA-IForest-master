#!/usr/bin/env bash
# ============================================================================
# 0-prepare-local.sh
# 本地准备工作 / Local preparation:
#   1. mvn clean package 生成 fat jar
#   2. 本地构建自定义 Flink 镜像(含 prometheus reporter)
#   3. 渲染 prometheus.yml(将 __NODE_*_IP__ 占位符替换为真实 IP)
#   4. 把镜像导出为 tar, 准备传到各节点
# ============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEPLOY_DIR="$(dirname "$SCRIPT_DIR")"
PROJECT_ROOT="$(dirname "$DEPLOY_DIR")"

# 加载 .env / Load .env
if [[ ! -f "$DEPLOY_DIR/.env" ]]; then
    echo "ERROR: $DEPLOY_DIR/.env not found. Copy .env.example first."
    exit 1
fi
set -a; source "$DEPLOY_DIR/.env"; set +a

# 工作目录(打包产物落在这里)
BUILD_DIR="$DEPLOY_DIR/.build"
mkdir -p "$BUILD_DIR"

echo "===================================="
echo "[0/4] Build summary"
echo "===================================="
echo "Project root:   $PROJECT_ROOT"
echo "Build output:   $BUILD_DIR"
echo "Flink version:  $FLINK_VERSION"
echo "Kafka version:  $KAFKA_VERSION"
echo "===================================="

# ---------- 1. mvn package ----------
echo "[1/4] mvn clean package ..."
cd "$PROJECT_ROOT"
mvn clean package -DskipTests -q
# 找到 shaded fat jar(maven-shade-plugin 输出在 target/, 通常带 -shaded 后缀或同名)
JAR_PATH="$(find "$PROJECT_ROOT/target" -maxdepth 1 -name '*.jar' ! -name '*sources*' ! -name '*javadoc*' ! -name 'original-*' | head -1)"
if [[ -z "$JAR_PATH" ]]; then
    echo "ERROR: no jar found under $PROJECT_ROOT/target/"
    exit 1
fi
cp "$JAR_PATH" "$BUILD_DIR/$JOB_JAR_NAME"
echo "  -> $BUILD_DIR/$JOB_JAR_NAME"

# ---------- 2. 构建自定义 Flink 镜像 ----------
echo "[2/4] docker build fa-iforest/flink:$FLINK_VERSION ..."
docker build \
    -t "fa-iforest/flink:$FLINK_VERSION" \
    -f "$DEPLOY_DIR/docker/Dockerfile.flink" \
    "$DEPLOY_DIR/docker/"

# ---------- 3. 渲染 prometheus.yml ----------
echo "[3/4] render prometheus.yml ..."
sed \
    -e "s|__NODE_MASTER_IP__|$NODE_MASTER_IP|g" \
    -e "s|__NODE_WORKER1_IP__|$NODE_WORKER1_IP|g" \
    -e "s|__NODE_WORKER2_IP__|$NODE_WORKER2_IP|g" \
    "$DEPLOY_DIR/compose/prometheus.yml.template" \
    > "$BUILD_DIR/prometheus.yml"
echo "  -> $BUILD_DIR/prometheus.yml"

# ---------- 4. 导出镜像为 tar(用于传到节点) ----------
echo "[4/4] save docker image to tar ..."
docker save "fa-iforest/flink:$FLINK_VERSION" -o "$BUILD_DIR/fa-iforest-flink.tar"
echo "  -> $BUILD_DIR/fa-iforest-flink.tar ($(du -h "$BUILD_DIR/fa-iforest-flink.tar" | cut -f1))"

echo ""
echo "===================================="
echo "DONE. Next step:"
echo "  bash $SCRIPT_DIR/1-sync-to-nodes.sh"
echo "===================================="
