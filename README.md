# FMP Stock Data Insertion Pipeline

A high-performance stock market data ingestion system that loads CSV files from Financial Modeling Prep (FMP) into a remote MySQL database with automated processing, REST API access, and real-time file monitoring.

## What This Service Does

This pipeline system provides:

- **Automated CSV Processing**: Watches directories for new stock data CSV files and automatically processes them
- **High-Precision Storage**: Stores stock data with DECIMAL(50,25) precision in MySQL with yearly partitions (1990-2026)
- **Parallel Processing**: Uses Redis Queue (RQ) with multiple workers for concurrent file imports
- **REST API**: FastAPI-based API with 8 endpoints for querying stock data, metadata, and statistics
- **Remote Database**: Connects to MySQL at ai-api.umiuni.com for centralized data storage
- **Bulk Import**: Batch processing with 10,000 row chunks for optimal performance

## 📁 Directory Structure

```
fmp_stock_data_insertion/
├── src/                      # Main application source code
│   ├── __init__.py          # Package initialization
│   ├── api.py               # FastAPI REST API server
│   ├── database.py          # MySQL connection manager and schema setup
│   ├── loader.py            # CSV to MySQL data loader
│   ├── sources.py           # Table name constants and configuration
│   ├── pipeline_jobs.py     # RQ job definitions for async processing
│   ├── pipeline_watch.py    # File system watcher for auto-ingestion
│   └── pipeline_worker.py   # RQ worker processes
├── scripts/                  # Shell scripts for automation
│   ├── start_pipeline.sh    # Redis verification and prerequisite checks
│   └── run_pipeline.sh      # Auto-start complete pipeline with workers
├── data/                     # Data directories (ignored by git)
│   ├── raw/                 # Incoming CSV files to process
│   ├── processed/           # Successfully processed CSV files
│   └── failed/              # Failed CSV files with error logs
├── docs/                     # Documentation directory
├── .env                      # Environment variables (DO NOT COMMIT)
├── .env.example             # Template for environment configuration
├── .gitignore               # Git ignore rules (protects .env and data/)
├── requirements.txt         # Python dependencies
├── start_api.sh             # Start the REST API server
├── start_pipeline.sh        # Start the complete pipeline
├── LICENSE                  # Project license
└── README.md                # This file

Legacy directories (backward compatibility):
├── raw/                     # Original raw CSV directory (still monitored)
```

## 📄 File Descriptions

### Source Code (`src/`)

- **api.py** - FastAPI REST API with endpoints for stock data queries, health checks, and queue status
- **database.py** - DatabaseManager class for MySQL connections, schema creation, and partition management
- **loader.py** - CSVLoader class that processes FMP CSV files and bulk inserts into MySQL
- **sources.py** - Defines table name constants (DATA_TABLE, METADATA_TABLE) used across modules
- **pipeline_jobs.py** - RQ job function for async CSV processing with file movement to processed/failed directories
- **pipeline_watch.py** - Watchdog-based file monitor that queues CSV files for processing when detected
- **pipeline_worker.py** - RQ worker process manager that spawns multiple workers for parallel processing

### Scripts (`scripts/`)

- **start_pipeline.sh** - Verifies Redis is running, checks .env configuration, validates database connectivity
- **run_pipeline.sh** - Orchestrates startup of 4 workers + file watcher with automatic cleanup on exit

### Root Files

- **start_api.sh** - Convenience wrapper to start the FastAPI server (`python -m src.api`)
- **start_pipeline.sh** - Convenience wrapper to start the complete pipeline (`./scripts/run_pipeline.sh`)
- **requirements.txt** - Python package dependencies (FastAPI, MySQL, pandas, Redis, RQ, watchdog)
- **.env.example** - Template showing all required environment variables with placeholder values
- **.gitignore** - Protects sensitive files (.env, data/, *.sql, *.key) from being committed

## Environment Configuration (.env)

The `.env` file contains all configuration for database, API, and pipeline settings. **This file is gitignored and must be created locally.**

### Setup

1. Copy the example file:
```bash
cp .env.example .env
```

2. Edit `.env` with your actual credentials:
```bash
nano .env  # or use your preferred editor
```

### Environment Variables

**Database Configuration**
- `DB_HOST` - MySQL server hostname (e.g., ai-api.umiuni.com)
- `DB_PORT` - MySQL port (default: 3306)
- `DB_NAME` - Database name (e.g., fmp_api or stock_data)
- `DB_USER` - MySQL username with INSERT/SELECT permissions
- `DB_PASSWORD` - MySQL password (NEVER commit this!)

**API Configuration**
- `API_HOST` - API bind address (0.0.0.0 for all interfaces, 127.0.0.1 for localhost only)
- `API_PORT` - API port (default: 8000)

**Redis Configuration**
- `REDIS_HOST` - Redis server (localhost for pipeline mode, empty to disable caching)
- `REDIS_PORT` - Redis port (default: 6379)
- `REDIS_PASSWORD` - Redis password (leave empty if no auth)
- `CACHE_TTL` - Cache time-to-live in seconds (default: 300)

**Pipeline Configuration**
- `PIPELINE_QUEUE_NAME` - RQ queue name (default: ingest)
- `PIPELINE_RAW_DIR` - Directory to watch for new CSV files (default: raw)
- `PIPELINE_PROCESSED_DIR` - Directory for successfully processed files (default: processed)
- `PIPELINE_FAILED_DIR` - Directory for failed files (default: failed)

## How to Start the Service

### Prerequisites

1. **Python 3.8+** installed
2. **Redis server** installed and running
3. **MySQL database** accessible with credentials
4. **Virtual environment** (recommended)

### Initial Setup

```bash
# 1. Clone the repository
git clone <your-repo-url>
cd fmp_stock_data_insertion

# 2. Create virtual environment
python -m venv .venv
source .venv/bin/activate  # On Linux/Mac
# .venv\Scripts\activate   # On Windows

# 3. Install dependencies
pip install -r requirements.txt

# 4. Configure environment
cp .env.example .env
nano .env  # Edit with your credentials

# 5. Setup database (ONE TIME ONLY)
python -m src.database
```

### Starting Services

**Option 1: Start API Server Only**
```bash
./start_api.sh
# API will be available at http://localhost:8000
# Docs at http://localhost:8000/docs
```

**Option 2: Start Complete Pipeline (Recommended)**
```bash
./start_pipeline.sh
# This starts:
# - 4 RQ workers for parallel processing
# - File watcher monitoring data/raw/ directory
# - Automatic processing of CSV files
```

**Option 3: Manual Start (Advanced)**
```bash
# Terminal 1: Start workers
python -m src.pipeline_worker --num-workers 4

# Terminal 2: Start watcher
python -m src.pipeline_watch --scan-existing

# Terminal 3: Start API (optional)
python -m src.api
```

### Stopping Services

- Press `Ctrl+C` to stop the pipeline (gracefully shuts down workers)
- Workers will finish current jobs before exiting

## Usage

### Adding Data

1. Drop CSV files into `data/raw/` directory while Pipeline is running
2. Pipeline automatically detects and processes them
3. Successful files moved to `data/processed/`
4. Failed files moved to `data/failed/` with error logs

### API Endpoints

Once the API is running, access these endpoints:

- `GET /health` - Service health check
- `GET /stock/{ticker}` - Get all data for a specific ticker
- `GET /stock/{ticker}/range` - Get data for date range
- `GET /stocks` - Get data for multiple tickers
- `GET /tickers` - List all available tickers
- `GET /metadata/{ticker}` - Get ticker metadata
- `GET /stats` - Database statistics
- `GET /queue/status` - Pipeline queue status

**Example:**
```bash
# Get Apple stock data
curl http://localhost:8000/stock/AAPL

# Get date range
curl "http://localhost:8000/stock/AAPL/range?start_date=2025-01-01&end_date=2025-12-31"

# View API documentation
open http://localhost:8000/docs
```

## 🗄️ Database Schema

**Table: ticker_data**
- `symbol` VARCHAR(10) - Stock ticker symbol
- `date` DATE - Trading date
- `open`, `high`, `low`, `close` - DECIMAL(50,25) - Price data with ultra-high precision
- `adjClose` - DOUBLE - Adjusted close price
- `volume`, `unadjustedVolume` - BIGINT - Trading volumes
- `change_value`, `changePercent`, `vwap`, `changeOverTime` - DOUBLE - Calculated metrics
- `adjOpen`, `adjHigh`, `adjLow` - DOUBLE - Adjusted prices
- Partitioned by year (1990-2026) for query performance

**Table: ticker_metadata**
- `symbol` VARCHAR(10) PRIMARY KEY
- `first_date`, `last_date` - Date range of available data
- `row_count` - Total records for this ticker
- `last_updated` - Timestamp of last modification

## Features

-  High-precision DECIMAL(50,25) storage for price data
-  Yearly table partitioning (1990-2026) for optimized queries
-  Parallel processing with 4+ concurrent workers
-  Automatic file monitoring and ingestion
-  Batch inserts (10,000 rows) for performance
-  Error handling with failed file quarantine
-  RESTful API with OpenAPI/Swagger docs
-  Redis-based job queue for scalability
-  Metadata tracking per ticker
-  Environment-based configuration

## Notes

- **database.py** is a ONE-TIME setup script - only run during initial database creation
- The pipeline can process hundreds of CSV files concurrently
- Redis must be running for pipeline mode
- API can run standalone without Redis (caching disabled)
- All dates use FMP format: YYYY-MM-DD
- CSV files must follow FMP column naming (symbol, date, open, high, low, close, etc.)

## 📚 Dependencies

See [requirements.txt](requirements.txt) for full list. Key packages:
- **FastAPI** - Modern web framework for REST API
- **MySQL Connector** - Database driver
- **Pandas** - CSV processing and data manipulation
- **Redis & RQ** - Job queue for async processing
- **Watchdog** - File system monitoring
- **Uvicorn** - ASGI server for FastAPI

## 📄 License

See [LICENSE](LICENSE) file for details.