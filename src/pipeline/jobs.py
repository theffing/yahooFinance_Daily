"""Background jobs for the data pipeline."""

import logging
import os
import shutil
from pathlib import Path
from dotenv import load_dotenv

from src.loader.loader import CSVLoader
from src.database.database import db_manager
from src.database.sources import get_tables, DATA_TABLE, METADATA_TABLE

# Load environment variables
load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

RAW_DIR = Path(os.getenv("PIPELINE_RAW_DIR", "data/raw")).resolve()
PROCESSED_DIR = Path(os.getenv("PIPELINE_PROCESSED_DIR", "data/processed")).resolve()
FAILED_DIR = Path(os.getenv("PIPELINE_FAILED_DIR", "data/failed")).resolve()

logger.info(f"Pipeline configured for remote database: {db_manager.host}")
logger.info(f"Monitoring directory: {RAW_DIR}")


def process_csv_job(file_path: str) -> bool:
    """Process a single CSV file and upload to remote database."""
    path = Path(file_path)
    if not path.exists():
        logger.info("Skipping missing file (already moved/processed): %s", file_path)
        return True

    processed_dir = PROCESSED_DIR
    failed_dir = FAILED_DIR

    logger.info(f"Processing {path.name} for upload to {db_manager.host}...")
    
    loader = CSVLoader(
        csv_dir=str(path.parent),
        processed_dir=str(processed_dir),
        failed_dir=str(failed_dir),
    )

    if not loader.validate_csv(str(path)):
        logger.error("Validation failed for %s", path.name)
        _move_to_failed(path, failed_dir)
        return False

    success = loader.process_csv_file(str(path))
    if success:
        logger.info(f"✓ Successfully uploaded {path.name} to remote database")
    else:
        logger.error(f"✗ Failed to upload {path.name} to remote database")
        _move_to_failed(path, failed_dir)

        # If file was already moved to processed before failure was surfaced,
        # move it to failed for consistent triage behavior.
        processed_candidate = processed_dir / path.name
        _move_to_failed(processed_candidate, failed_dir)
    
    return success


def _move_to_failed(file_path: Path, failed_dir: Path) -> None:
    if not file_path.exists():
        return
    failed_dir.mkdir(parents=True, exist_ok=True)
    dest = failed_dir / file_path.name
    if dest.exists():
        dest.unlink()
    shutil.move(str(file_path), str(dest))
