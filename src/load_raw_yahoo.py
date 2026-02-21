"""RQ-based Yahoo CSV generation with batch enqueueing and multi-worker processing.

Examples:
  python -m src.load_raw_yahoo enqueue --days 120
  python -m src.load_raw_yahoo worker --num-workers 4
"""

import argparse
import logging
import multiprocessing
import os
import random
import time
from pathlib import Path

import numpy as np
import pandas as pd
import yfinance as yf
from dotenv import load_dotenv
from redis import Redis
from rq import Connection, Queue, Worker


load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).resolve().parents[1]
RAW_DIR = BASE_DIR / "data" / "raw"
FAILED_LOG_FILE = Path(__file__).resolve().parent / "failed_tickers.txt"
TICKERS_FILE = Path(__file__).resolve().parent / "tickers.txt"


def build_redis_client() -> Redis:
    return Redis(
        host=os.getenv("REDIS_HOST", "localhost"),
        port=int(os.getenv("REDIS_PORT", 6379)),
        password=os.getenv("REDIS_PASSWORD") or None,
        decode_responses=False,
    )


def read_tickers(file_path: Path = TICKERS_FILE) -> list[str]:
    if not file_path.exists():
        raise FileNotFoundError(f"Tickers file not found: {file_path}")

    tickers: list[str] = []
    with file_path.open("r", encoding="utf-8") as file_handle:
        for line in file_handle:
            ticker = line.strip()
            if not ticker or ticker.startswith("#"):
                continue
            tickers.append(ticker.upper())

    if not tickers:
        raise ValueError(f"No tickers found in: {file_path}")

    deduped: list[str] = []
    seen: set[str] = set()
    for ticker in tickers:
        if ticker in seen:
            continue
        seen.add(ticker)
        deduped.append(ticker)
    return deduped


def fetch_yahoo_finance(ticker: str, days: int = 120) -> pd.DataFrame:
    ticker_obj = yf.Ticker(ticker)
    df = ticker_obj.history(period=f"{days}d", interval="1d", auto_adjust=False)

    if df.empty:
        raise ValueError(f"No data returned for {ticker}")

    df = df.reset_index()
    df = df.rename(
        columns={
            "Date": "date",
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Close": "close",
            "Adj Close": "adjClose",
            "Volume": "volume",
        }
    )

    df["date"] = pd.to_datetime(df["date"]).dt.date
    df["change_value"] = df["close"] - df["open"]
    df["changePercent"] = np.where(df["open"] != 0, (df["change_value"] / df["open"]) * 100.0, np.nan)
    df["vwap"] = np.where(df["volume"] > 0, (df["high"] + df["low"] + df["close"]) / 3.0, np.nan)
    df["unadjustedVolume"] = df["volume"]

    adj_factor = np.where(df["close"] != 0, df["adjClose"] / df["close"], np.nan)
    df["adjOpen"] = df["open"] * adj_factor
    df["adjHigh"] = df["high"] * adj_factor
    df["adjLow"] = df["low"] * adj_factor
    df["adjVolume"] = np.where(adj_factor != 0, df["volume"] / adj_factor, np.nan)
    df["symbol"] = ticker.upper()

    columns = [
        "date",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "vwap",
        "change_value",
        "changePercent",
        "unadjustedVolume",
        "adjOpen",
        "adjHigh",
        "adjLow",
        "adjClose",
        "adjVolume",
        "symbol",
    ]
    return df[columns].replace([np.inf, -np.inf], np.nan)


def save_to_csv(df: pd.DataFrame, ticker: str, raw_dir: Path = RAW_DIR) -> Path:
    raw_dir.mkdir(parents=True, exist_ok=True)
    out_path = raw_dir / f"{ticker.upper()}.csv"
    df.to_csv(out_path, index=False)
    return out_path


def append_failed_ticker_log(ticker: str, log_file: Path = FAILED_LOG_FILE) -> None:
    log_file.parent.mkdir(parents=True, exist_ok=True)
    with log_file.open("a", encoding="utf-8") as file_handle:
        file_handle.write(f"{ticker.upper()}\n")


def create_csv_job(ticker: str, days: int) -> bool:
    max_retries = max(1, int(os.getenv("YAHOO_MAX_RETRIES", "4")))
    base_delay = max(0.0, float(os.getenv("YAHOO_RETRY_BASE_DELAY", "2.0")))
    request_delay = max(0.0, float(os.getenv("YAHOO_REQUEST_DELAY", "0.25")))

    last_error: Exception | None = None
    for attempt in range(1, max_retries + 1):
        try:
            if request_delay > 0:
                time.sleep(request_delay + random.uniform(0, 0.2))

            df = fetch_yahoo_finance(ticker, days=days)
            out_path = save_to_csv(df, ticker)
            logger.info("Saved %s", out_path)
            return True
        except Exception as error:
            last_error = error
            if attempt < max_retries:
                sleep_seconds = base_delay * (2 ** (attempt - 1)) + random.uniform(0, 0.5)
                logger.warning(
                    "Retrying %s (%s/%s) in %.2fs after error: %s",
                    ticker,
                    attempt,
                    max_retries,
                    sleep_seconds,
                    error,
                )
                time.sleep(sleep_seconds)

    append_failed_ticker_log(ticker)
    logger.error("Failed %s after %s attempts: %s (logged in %s)", ticker, max_retries, last_error, FAILED_LOG_FILE)
    return False


def enqueue_jobs(days: int, batch_size: int, queue_name: str) -> int:
    tickers = read_tickers()
    redis_client = build_redis_client()
    queue = Queue(queue_name, connection=redis_client)
    job_target = "src.load_raw_yahoo.create_csv_job"

    enqueued = 0
    for index, ticker in enumerate(tickers, start=1):
        queue.enqueue(
            job_target,
            ticker,
            days,
            job_timeout=1800,
            result_ttl=86400,
        )
        enqueued += 1

        if index % batch_size == 0:
            logger.info("Enqueued %s/%s tickers", index, len(tickers))

    logger.info("Finished enqueue: %s jobs on queue '%s'", enqueued, queue_name)
    return enqueued


def run_worker(queue_name: str, with_scheduler: bool) -> None:
    redis_client = build_redis_client()
    queue = Queue(queue_name, connection=redis_client)

    logger.info("=" * 60)
    logger.info("Starting Yahoo CSV worker for queue '%s'", queue_name)
    logger.info("Raw output dir: %s", RAW_DIR)
    logger.info("Failed ticker log: %s", FAILED_LOG_FILE)
    logger.info("=" * 60)

    with Connection(redis_client):
        worker = Worker([queue])
        worker.work(with_scheduler=with_scheduler)


def start_workers(queue_name: str, num_workers: int) -> None:
    if num_workers <= 1:
        run_worker(queue_name, with_scheduler=True)
        return

    processes: list[multiprocessing.Process] = []
    for index in range(num_workers):
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


def main() -> int:
    parser = argparse.ArgumentParser(description="RQ-based Yahoo CSV producer/worker")
    subparsers = parser.add_subparsers(dest="command", required=True)

    enqueue_parser = subparsers.add_parser("enqueue", help="Enqueue ticker CSV jobs")
    enqueue_parser.add_argument("--days", type=int, default=120, help="Number of trailing days to fetch")
    enqueue_parser.add_argument("--batch-size", type=int, default=500, help="Progress log interval while enqueueing")
    enqueue_parser.add_argument(
        "--queue-name",
        default=os.getenv("YAHOO_QUEUE_NAME", "yahoo_csv"),
        help="Redis queue name",
    )

    worker_parser = subparsers.add_parser("worker", help="Run one or more CSV worker processes")
    worker_parser.add_argument("--num-workers", type=int, default=1, help="Number of worker processes")
    worker_parser.add_argument(
        "--queue-name",
        default=os.getenv("YAHOO_QUEUE_NAME", "yahoo_csv"),
        help="Redis queue name",
    )

    args = parser.parse_args()

    if args.command == "enqueue":
        enqueue_jobs(days=args.days, batch_size=args.batch_size, queue_name=args.queue_name)
        return 0

    if args.command == "worker":
        start_workers(queue_name=args.queue_name, num_workers=args.num_workers)
        return 0

    return 1


if __name__ == "__main__":
    raise SystemExit(main())
