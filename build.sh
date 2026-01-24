#!/bin/bash

# 确保脚本在错误时停止
set -e

echo "开始编译 Binlog 分析工具..."
mkdir -p bin

# 1. 编译 Linux AMD64 版本
echo "正在编译 Linux AMD64 版本..."
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o bin/binlog-analyzer-linux-amd64 cmd/binlog-analyzer/main.go
echo "✅ Linux AMD64 编译成功: bin/binlog-analyzer-linux-amd64"

# 2. 编译 Linux ARM64 版本
echo "正在编译 Linux ARM64 版本..."
CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -o bin/binlog-analyzer-linux-arm64 cmd/binlog-analyzer/main.go
echo "✅ Linux ARM64 编译成功: bin/binlog-analyzer-linux-arm64"

# 3. 编译 macOS ARM64 版本 (本地测试用)
echo "正在编译 macOS ARM64 版本 (方便本地测试)..."
CGO_ENABLED=0 GOOS=darwin GOARCH=arm64 go build -o bin/binlog-analyzer-darwin-arm64 cmd/binlog-analyzer/main.go
echo "✅ macOS ARM64 编译成功: bin/binlog-analyzer-darwin-arm64"

echo "所有编译任务完成！"
ls -lh bin/
