# Yahoo Finance Daily → MySQL Pipeline

Fetch daily market data from Yahoo Finance, write one CSV per ticker, and ingest into a remote MySQL database using Redis + RQ workers.

## What this repo does

- Generates CSV files from Yahoo Finance for tickers in `src/tickers.txt`
- Watches `data/raw/` and queues CSV ingestion jobs
- Loads CSV rows into remote MySQL (`ticker_data` + `ticker_metadata`)
- Moves processed/failed CSVs to `data/processed/` and `data/failed/`
- Provides a FastAPI service for querying loaded data

## Directory structure

```text
yahooFinance_Daily/
├── .env
├── .env.example
├── .gitignore
├── LICENSE
├── README.md
├── data/
│   ├── raw/                  # Generated / incoming CSV files
│   ├── processed/            # CSV files successfully loaded into DB
│   └── failed/               # CSV files that failed DB ingestion
├── requirements.txt
├── scripts/
│   ├── start_pipeline.sh     # Checks Redis + env prerequisites
│   ├── run_pipeline.sh       # Starts DB workers + watcher
│   └── run_yahoo_to_db.sh    # Starts Yahoo workers + DB workers + watcher + enqueue
├── src/
│   ├── __init__.py
│   ├── api/
│   │   └── api.py
│   ├── database/
│   │   ├── database.py
│   │   └── sources.py
│   ├── loader/
│   │   └── loader.py
│   ├── pipeline/
│   │   ├── jobs.py
│   │   ├── watch.py
│   │   └── worker.py
│   ├── load_raw_yahoo.py     # Yahoo producer/worker for CSV generation
│   ├── failed_tickers.txt     # Yahoo fetch failures
│   └── tickers.txt
├── start_api.sh
├── start_pipeline.sh
└── start_yahoo_to_db.sh
```

## Quick start (recommended)

1. Create env + install dependencies:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
```

2. Update `.env` with your DB credentials and keep these pipeline paths:

```dotenv
PIPELINE_RAW_DIR=data/raw
PIPELINE_PROCESSED_DIR=data/processed
PIPELINE_FAILED_DIR=data/failed
```

3. Run everything (Yahoo fetch + CSV generation + watcher + DB workers):

First, a file 'tickers.txt' needs to be in the src/ directory, populated with tickers, one on each line.
Then the load_raw_yahoo.py reads the tickers and fetches the information via the api and creates the appropriate CSV file in raw, where the watcher sees it, and the worker enqueues it.

```bash
./scripts/run_yahoo_to_db.sh
```

This starts:
- Yahoo CSV workers (`src.load_raw_yahoo worker`)
- DB ingest workers (`src.pipeline.worker`)
- Watcher (`src.pipeline.watch --scan-existing`)
- Yahoo job enqueue from `src/tickers.txt`

## Other run modes

- CSV-only ingestion pipeline (no Yahoo generation):

```bash
./start_pipeline.sh
```

- API only:

```bash
./start_api.sh
```

## Useful commands

- Enqueue Yahoo jobs manually:

```bash
python -m src.load_raw_yahoo enqueue --days 120 --batch-size 500
```

- Start Yahoo workers manually:

```bash
python -m src.load_raw_yahoo worker --num-workers 4
```

- Start DB workers manually:

```bash
python -m src.pipeline.worker --num-workers 4
```

- Start watcher manually:

```bash
python -m src.pipeline.watch --scan-existing
```

## Notes

- Failed Yahoo fetches are logged to `failed_tickers.txt`.
- `src/database/database.py` is used for DB setup and runtime connections.
- Stop running processes with `Ctrl+C` in the terminal where you started them.