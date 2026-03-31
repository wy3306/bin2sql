#!/bin/sh

set -eu

ROOT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
LOG_FILE="$ROOT_DIR/logs/binlog-web.log"

usage() {
  cat <<'EOF'
用法:
  sh webctl.sh start
  sh webctl.sh stop
  sh webctl.sh restart
  sh webctl.sh status
  sh webctl.sh logs
EOF
}

status() {
  PID_FILE="$ROOT_DIR/logs/binlog-web.pid"
  LEGACY_PID_FILE="$ROOT_DIR/bin/binlog-web.pid"

  if [ -f "$PID_FILE" ]; then
    PID=$(cat "$PID_FILE")
    if kill -0 "$PID" 2>/dev/null; then
      echo "Web 服务运行中，PID: $PID"
      echo "日志文件: $LOG_FILE"
      exit 0
    fi
    echo "检测到失效 PID 文件: $PID_FILE"
    exit 1
  fi

  if [ -f "$LEGACY_PID_FILE" ]; then
    PID=$(cat "$LEGACY_PID_FILE")
    if kill -0 "$PID" 2>/dev/null; then
      echo "Web 服务运行中，PID: $PID"
      echo "日志文件: $LOG_FILE"
      exit 0
    fi
    echo "检测到失效 PID 文件: $LEGACY_PID_FILE"
    exit 1
  fi

  echo "Web 服务未运行"
}

case "${1:-}" in
  start)
    exec sh "$ROOT_DIR/start-web.sh"
    ;;
  stop)
    exec sh "$ROOT_DIR/stop-web.sh"
    ;;
  restart)
    sh "$ROOT_DIR/stop-web.sh" || true
    exec sh "$ROOT_DIR/start-web.sh"
    ;;
  status)
    status
    ;;
  logs)
    if [ ! -f "$LOG_FILE" ]; then
      echo "日志文件不存在: $LOG_FILE"
      exit 1
    fi
    exec tail -n 200 -f "$LOG_FILE"
    ;;
  *)
    usage
    exit 1
    ;;
esac
