#!/usr/bin/env bash
# Start the poller and the classifier consumer in parallel.
# Logs to logs/pipeline.log; Ctrl+C stops both cleanly.
#
# Run from the project root:
#   bash scripts/run_pipeline.sh

set -euo pipefail

cd "$(dirname "$0")/.."

LOG_DIR="${LOG_DIR:-logs}"
mkdir -p "$LOG_DIR"
LOG="$LOG_DIR/pipeline.log"

PYTHON="${PYTHON:-python}"
export PYTHONPATH="${PYTHONPATH:-$(pwd)}"

echo "[run_pipeline] starting poller..."        | tee -a "$LOG"
"$PYTHON" -m ingestion.open311_poller    >> "$LOG" 2>&1 &
POLLER_PID=$!

echo "[run_pipeline] starting classifier consumer..." | tee -a "$LOG"
"$PYTHON" -m classifier.consumer         >> "$LOG" 2>&1 &
CONSUMER_PID=$!

echo "[run_pipeline] poller=$POLLER_PID  consumer=$CONSUMER_PID  log=$LOG"
echo "[run_pipeline] follow with:  tail -f $LOG"

cleanup() {
    echo "[run_pipeline] shutting down..."
    kill "$POLLER_PID" "$CONSUMER_PID" 2>/dev/null || true
    wait "$POLLER_PID" "$CONSUMER_PID" 2>/dev/null || true
    echo "[run_pipeline] stopped."
}
trap cleanup SIGINT SIGTERM EXIT

wait
