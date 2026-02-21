#!/bin/bash
# Auto-start Yahoo fetch queue + CSV watcher/ingest workers

set -e

echo "=========================================="
echo "Auto-Starting Yahoo -> CSV -> DB Pipeline"
echo "=========================================="
echo ""

# Verify Redis and environment prerequisites
./scripts/start_pipeline.sh

echo ""
echo "Starting Yahoo CSV workers, DB workers, and watcher..."
echo ""

PYTHON_BIN="${PYTHON_BIN:-python3}"
YAHOO_DAYS="${YAHOO_DAYS:-120}"
YAHOO_BATCH_SIZE="${YAHOO_BATCH_SIZE:-500}"
YAHOO_WORKERS="${YAHOO_WORKERS:-4}"
DB_WORKERS="${DB_WORKERS:-4}"
YAHOO_QUEUE_NAME="${YAHOO_QUEUE_NAME:-yahoo_csv}"
PIPELINE_QUEUE_NAME="${PIPELINE_QUEUE_NAME:-ingest}"

# Function to cleanup on exit
cleanup() {
    echo ""
    echo "Shutting down Yahoo -> DB pipeline..."
    kill $(jobs -p) 2>/dev/null || true
    exit 0
}

trap cleanup SIGINT SIGTERM

# Start Yahoo CSV workers

echo "Starting ${YAHOO_WORKERS} Yahoo CSV worker(s) on queue '${YAHOO_QUEUE_NAME}'..."
"${PYTHON_BIN}" -m src.load_raw_yahoo worker --num-workers "${YAHOO_WORKERS}" --queue-name "${YAHOO_QUEUE_NAME}" &
YAHOO_WORKER_PID=$!

# Start DB ingest workers

echo "Starting ${DB_WORKERS} DB ingest worker(s) on queue '${PIPELINE_QUEUE_NAME}'..."
PIPELINE_QUEUE_NAME="${PIPELINE_QUEUE_NAME}" "${PYTHON_BIN}" -m src.pipeline.worker --num-workers "${DB_WORKERS}" &
DB_WORKER_PID=$!

# Give workers time to initialize
sleep 2

# Start watcher first so it can catch new CSV files

echo "Starting watcher (scan existing + watch new CSVs)..."
"${PYTHON_BIN}" -m src.pipeline.watch --scan-existing &
WATCHER_PID=$!

sleep 2

# Enqueue Yahoo jobs

echo "Enqueueing Yahoo fetch jobs from src/tickers.txt (days=${YAHOO_DAYS}, batch-size=${YAHOO_BATCH_SIZE})..."
"${PYTHON_BIN}" -m src.load_raw_yahoo enqueue --days "${YAHOO_DAYS}" --batch-size "${YAHOO_BATCH_SIZE}" --queue-name "${YAHOO_QUEUE_NAME}"

echo ""
echo "=========================================="
echo "Pipeline is running"
echo "  Yahoo workers PID root: ${YAHOO_WORKER_PID}"
echo "  DB workers PID root:    ${DB_WORKER_PID}"
echo "  Watcher PID:            ${WATCHER_PID}"
echo "Press Ctrl+C to stop all"
echo "=========================================="

# Keep script alive while watcher runs
wait "${WATCHER_PID}"
