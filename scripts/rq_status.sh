#!/bin/bash
# Show RQ queue progress for Yahoo CSV generation and DB ingestion.

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

if [[ -f .env ]]; then
  set -a
  source .env
  set +a
fi

WATCH_MODE=false
INTERVAL=2

while [[ $# -gt 0 ]]; do
  case "$1" in
    --watch)
      WATCH_MODE=true
      shift
      ;;
    --interval)
      INTERVAL="${2:-2}"
      shift 2
      ;;
    *)
      echo "Unknown argument: $1"
      echo "Usage: ./scripts/rq_status.sh [--watch] [--interval N]"
      exit 1
      ;;
  esac
done

render_status() {
  python3 - <<'PY'
import os
from redis import Redis
from rq import Queue
from rq.registry import (
    DeferredJobRegistry,
    FailedJobRegistry,
    FinishedJobRegistry,
    ScheduledJobRegistry,
    StartedJobRegistry,
)

redis_host = os.getenv("REDIS_HOST", "localhost")
redis_port = int(os.getenv("REDIS_PORT", 6379))
redis_password = os.getenv("REDIS_PASSWORD") or None
pipeline_queue = os.getenv("PIPELINE_QUEUE_NAME", "ingest")
queues = ["yahoo_csv", pipeline_queue]

redis_client = Redis(
    host=redis_host,
    port=redis_port,
    password=redis_password,
    decode_responses=False,
)

print(f"Redis: {redis_host}:{redis_port}")
print("-" * 72)

for queue_name in queues:
    queue = Queue(queue_name, connection=redis_client)
    queued = len(queue)
    started = StartedJobRegistry(queue_name, connection=redis_client).count
    finished = FinishedJobRegistry(queue_name, connection=redis_client).count
    failed = FailedJobRegistry(queue_name, connection=redis_client).count
    deferred = DeferredJobRegistry(queue_name, connection=redis_client).count
    scheduled = ScheduledJobRegistry(queue_name, connection=redis_client).count

    print(
        f"{queue_name:12} queued={queued:<6} started={started:<6} "
        f"finished={finished:<6} failed={failed:<6} deferred={deferred:<6} scheduled={scheduled:<6}"
    )
PY
}

if [[ "$WATCH_MODE" == "true" ]]; then
  while true; do
    clear
    echo "RQ status (refresh ${INTERVAL}s) - $(date '+%Y-%m-%d %H:%M:%S')"
    render_status
    sleep "$INTERVAL"
  done
else
  render_status
fi
