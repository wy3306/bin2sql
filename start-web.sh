#!/bin/sh

set -eu

ROOT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
BIN_DIR="$ROOT_DIR/bin"
LOG_DIR="$ROOT_DIR/logs"
PID_FILE="$LOG_DIR/binlog-web.pid"
LEGACY_PID_FILE="$BIN_DIR/binlog-web.pid"
LOG_FILE="$LOG_DIR/binlog-web.log"
WEB_BINARY="$BIN_DIR/binlog-web"
PORT=9000

mkdir -p "$BIN_DIR"
mkdir -p "$LOG_DIR"

if [ -f "$LEGACY_PID_FILE" ] && [ ! -f "$PID_FILE" ]; then
  mv "$LEGACY_PID_FILE" "$PID_FILE"
fi

if [ -f "$PID_FILE" ]; then
  OLD_PID=$(cat "$PID_FILE")
  if kill -0 "$OLD_PID" 2>/dev/null; then
    echo "Web 服务已在运行，PID: $OLD_PID"
    echo "访问地址: http://127.0.0.1:9000"
    exit 0
  fi
  rm -f "$PID_FILE"
fi

if command -v lsof >/dev/null 2>&1; then
  PORT_PID=$(lsof -ti tcp:"$PORT" -sTCP:LISTEN 2>/dev/null | head -n 1 || true)
  if [ -n "${PORT_PID:-}" ]; then
    CMDLINE=$(ps -p "$PORT_PID" -o command= 2>/dev/null || true)
    case "$CMDLINE" in
      *"$WEB_BINARY"*)
        echo "$PORT_PID" > "$PID_FILE"
        echo "Web 服务已在运行，PID: $PORT_PID"
        echo "访问地址: http://127.0.0.1:$PORT"
        echo "日志文件: $LOG_FILE"
        exit 0
        ;;
      *)
        echo "端口 $PORT 已被其他进程占用，PID: $PORT_PID"
        if [ -n "$CMDLINE" ]; then
          echo "占用进程: $CMDLINE"
        fi
        echo "请先释放端口，或停止占用端口的进程后再启动 Web 服务"
        exit 1
        ;;
    esac
  fi
fi

if [ ! -x "$WEB_BINARY" ]; then
  if [ -f "$ROOT_DIR/go.mod" ] && [ -d "$ROOT_DIR/cmd/binlog-web" ]; then
    echo "编译 Web 服务..."
    go build -o "$WEB_BINARY" ./cmd/binlog-web
  else
    echo "未找到可执行的 Web 二进制: $WEB_BINARY"
    echo "当前目录也不包含源码，无法现场编译"
    exit 1
  fi
fi

echo "启动 Web 服务..."
nohup "$WEB_BINARY" >"$LOG_FILE" 2>&1 &
NEW_PID=$!
echo "$NEW_PID" > "$PID_FILE"
rm -f "$LEGACY_PID_FILE"

sleep 1

if kill -0 "$NEW_PID" 2>/dev/null; then
  echo "Web 服务启动成功，PID: $NEW_PID"
  echo "访问地址: http://127.0.0.1:$PORT"
  echo "日志文件: $LOG_FILE"
else
  echo "Web 服务启动失败，请检查日志: $LOG_FILE"
  rm -f "$PID_FILE"
  exit 1
fi
