"""Watch raw folders and enqueue CSV processing jobs."""

import argparse
import logging
import os
import time
from pathlib import Path
from dotenv import load_dotenv

from redis import Redis
from rq import Queue
from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer

from src.pipeline.jobs import process_csv_job
from src.database.database import db_manager

# Load environment variables
load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def build_redis_client() -> Redis:
    return Redis(
        host=os.getenv("REDIS_HOST", "localhost"),
        port=int(os.getenv("REDIS_PORT", 6379)),
        password=os.getenv("REDIS_PASSWORD") or None,
        decode_responses=False,
    )


def enqueue_file(queue: Queue, file_path: Path) -> None:
    if file_path.suffix.lower() != ".csv":
        return
    if not file_path.exists():
        return

    queue.enqueue(
        process_csv_job,
        str(file_path),
        job_timeout=1800,
    )
    logger.info("Enqueued %s", file_path)


class CSVEventHandler(FileSystemEventHandler):
    def __init__(self, queue: Queue):
        self.queue = queue

    def on_created(self, event):
        if event.is_directory:
            return
        enqueue_file(self.queue, Path(event.src_path))

    def on_moved(self, event):
        if event.is_directory:
            return
        enqueue_file(self.queue, Path(event.dest_path))


def main() -> int:
    parser = argparse.ArgumentParser(description="Watch raw directories and enqueue CSV jobs for remote database")
    parser.add_argument(
        "--scan-existing",
        action="store_true",
        help="Enqueue existing CSV files on startup",
    )
    args = parser.parse_args()

    raw_dir = Path("data/raw").resolve()
    if not raw_dir.exists():
        logger.error(f"Raw directory '{raw_dir}' does not exist. Exiting.")
        return 1

    logger.info(f"Remote database: {db_manager.host}:{db_manager.port}")
    logger.info(f"Database name: {db_manager.database}")
    
    redis_client = build_redis_client()
    queue_name = os.getenv("PIPELINE_QUEUE_NAME", "ingest")
    queue = Queue(queue_name, connection=redis_client)

    if args.scan_existing:
        logger.info("Scanning for existing CSV files...")
        count = 0
        for csv_path in raw_dir.rglob("*.csv"):
            enqueue_file(queue, csv_path)
            count += 1
        logger.info(f"Enqueued {count} existing CSV files")

    event_handler = CSVEventHandler(queue)
    observer = Observer()

    observer.schedule(event_handler, str(raw_dir), recursive=True)

    logger.info("="*60)
    logger.info(f"Watching {raw_dir} for CSV files")
    logger.info(f"Files will be uploaded to: {db_manager.host}")
    logger.info("Press Ctrl+C to stop")
    logger.info("="*60)

    observer.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()

    observer.join()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
