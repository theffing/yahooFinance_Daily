"""RQ worker to process queued CSV ingest jobs."""

import argparse
import logging
import os
import multiprocessing
from dotenv import load_dotenv

from redis import Redis
from rq import Connection, Queue, Worker
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


def run_worker(queue_name: str, with_scheduler: bool) -> None:
    redis_client = build_redis_client()
    queue = Queue(queue_name, connection=redis_client)

    logger.info("="*60)
    logger.info(f"Starting worker for queue '{queue_name}'")
    logger.info(f"Remote database: {db_manager.host}:{db_manager.port}")
    logger.info(f"Database name: {db_manager.database}")
    logger.info("="*60)
    
    with Connection(redis_client):
        worker = Worker([queue])
        worker.work(with_scheduler=with_scheduler)


def main() -> int:
    parser = argparse.ArgumentParser(description="Run one or more RQ workers")
    parser.add_argument(
        "--num-workers",
        type=int,
        default=1,
        help="Number of worker processes to start",
    )
    args = parser.parse_args()

    queue_name = os.getenv("PIPELINE_QUEUE_NAME", "ingest")
    if args.num_workers <= 1:
        run_worker(queue_name, with_scheduler=True)
        return 0

    processes: list[multiprocessing.Process] = []
    for index in range(args.num_workers):
        process = multiprocessing.Process(
            target=run_worker,
            args=(queue_name, index == 0),
            daemon=False,
        )
        process.start()
        processes.append(process)

    try:
        for process in processes:
            process.join()
    except KeyboardInterrupt:
        for process in processes:
            process.terminate()
        for process in processes:
            process.join()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
