#!/bin/bash
# Auto-start the complete pipeline with Redis check and workers

set -e

echo "=========================================="
echo "Auto-Starting Complete Pipeline"
echo "=========================================="
echo ""

# Run the prerequisite check script
./scripts/start_pipeline.sh

echo ""
echo "Starting pipeline workers and watcher..."
echo ""

# Function to cleanup on exit
cleanup() {
    echo ""
    echo "Shutting down pipeline..."
    kill $(jobs -p) 2>/dev/null
    exit 0
}

trap cleanup SIGINT SIGTERM

# Start workers in background
echo "Starting 4 pipeline workers..."
python3 -m src.pipeline.worker --num-workers 4 &
WORKER_PID=$!

# Give workers a moment to start
sleep 2

# Start watcher
echo "Starting file watcher..."
echo ""
python3 -m src.pipeline.watch --scan-existing

# If watcher exits, cleanup
kill $WORKER_PID 2>/dev/null
