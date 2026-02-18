"""Table names for stock data storage."""

# Table names (simplified from multi-source architecture)
DATA_TABLE = "ticker_data"
METADATA_TABLE = "ticker_metadata"


def get_tables() -> tuple[str, str]:
    """Return the data and metadata table names."""
    return DATA_TABLE, METADATA_TABLE
