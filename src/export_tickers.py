"""
Script to export all tickers from the database to a .txt file.
"""
import sys
import os
from database.database import db_manager

OUTPUT_FILE = "tickers.txt"

def main():
    tickers = db_manager.get_ticker_list()
    with open(OUTPUT_FILE, "w") as f:
        for ticker in tickers:
            f.write(f"{ticker}\n")
    print(f"Exported {len(tickers)} tickers to {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
