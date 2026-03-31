#!/bin/sh

set -eu

ROOT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
DIST_DIR="$ROOT_DIR/dist"
PACKAGE_ROOT="$DIST_DIR/bin2sql"
PACKAGE_NAME="bin2sql-linux-amd64.tar.gz"

mkdir -p "$DIST_DIR"
rm -rf "$PACKAGE_ROOT"
mkdir -p "$PACKAGE_ROOT/bin"
mkdir -p "$PACKAGE_ROOT/config"
mkdir -p "$PACKAGE_ROOT/logs"
mkdir -p "$PACKAGE_ROOT/webui/assets"

echo "编译 Linux AMD64 CLI..."
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o "$PACKAGE_ROOT/bin/binlog-analyzer" ./main.go

echo "编译 Linux AMD64 Web..."
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o "$PACKAGE_ROOT/bin/binlog-web" ./cmd/binlog-web

echo "复制运行脚本..."
cp "$ROOT_DIR/start-web.sh" "$PACKAGE_ROOT/start-web.sh"
cp "$ROOT_DIR/stop-web.sh" "$PACKAGE_ROOT/stop-web.sh"
cp "$ROOT_DIR/webctl.sh" "$PACKAGE_ROOT/webctl.sh"
cp "$ROOT_DIR/README.md" "$PACKAGE_ROOT/README.md"
cp "$ROOT_DIR/config/README.md" "$PACKAGE_ROOT/config/README.md"
cp "$ROOT_DIR/logs/README.md" "$PACKAGE_ROOT/logs/README.md"
cp "$ROOT_DIR/internal/webui/assets/index.html" "$PACKAGE_ROOT/webui/assets/index.html"
cp "$ROOT_DIR/internal/webui/assets/app.js" "$PACKAGE_ROOT/webui/assets/app.js"
cp "$ROOT_DIR/internal/webui/assets/styles.css" "$PACKAGE_ROOT/webui/assets/styles.css"

chmod +x "$PACKAGE_ROOT/start-web.sh" "$PACKAGE_ROOT/stop-web.sh" "$PACKAGE_ROOT/webctl.sh"
chmod +x "$PACKAGE_ROOT/bin/binlog-analyzer" "$PACKAGE_ROOT/bin/binlog-web"

echo "清理旧压缩包..."
rm -f "$DIST_DIR/$PACKAGE_NAME"

echo "生成压缩包..."
(
  cd "$DIST_DIR"
  tar -czf "$PACKAGE_NAME" "bin2sql"
)

echo "打包完成:"
echo "目录: $PACKAGE_ROOT"
echo "压缩包: $DIST_DIR/$PACKAGE_NAME"
