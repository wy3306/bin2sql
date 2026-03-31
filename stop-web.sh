#!/bin/sh

set -eu

ROOT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
PID_FILE="$ROOT_DIR/logs/binlog-web.pid"
LEGACY_PID_FILE="$ROOT_DIR/bin/binlog-web.pid"
WEB_BINARY="$ROOT_DIR/bin/binlog-web"
PORT=9000

stop_pid() {
  TARGET_PID="$1"

  if ! kill -0 "$TARGET_PID" 2>/dev/null; then
    return 0
  fi

  echo "停止 Web 服务，PID: $TARGET_PID"
  kill "$TARGET_PID"

  for _ in 1 2 3 4 5; do
    if ! kill -0 "$TARGET_PID" 2>/dev/null; then
      return 0
    fi
    sleep 1
  done

  echo "进程仍在运行，发送强制停止信号"
  kill -9 "$TARGET_PID"
}

if [ ! -f "$PID_FILE" ]; then
  if [ -f "$LEGACY_PID_FILE" ]; then
    PID_FILE="$LEGACY_PID_FILE"
  fi
fi

if [ ! -f "$PID_FILE" ]; then
  if command -v lsof >/dev/null 2>&1; then
    PORT_PID=$(lsof -ti tcp:"$PORT" -sTCP:LISTEN 2>/dev/null | head -n 1 || true)
    if [ -n "${PORT_PID:-}" ]; then
      CMDLINE=$(ps -p "$PORT_PID" -o command= 2>/dev/null || true)
      case "$CMDLINE" in
        *"$WEB_BINARY"*)
          stop_pid "$PORT_PID"
          echo "Web 服务已停止"
          exit 0
          ;;
      esac
    fi
  fi
  echo "未找到 PID 文件，Web 服务可能未启动"
  exit 0
fi

PID=$(cat "$PID_FILE")

if ! kill -0 "$PID" 2>/dev/null; then
  echo "PID $PID 不存在，清理残留 PID 文件"
  rm -f "$PID_FILE"
  exit 0
fi

stop_pid "$PID"
rm -f "$PID_FILE"
rm -f "$LEGACY_PID_FILE"
echo "Web 服务已停止"
