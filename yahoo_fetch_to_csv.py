"""
yahoo_fetch_to_csv.py - Fetches most recent Yahoo Finance data for a ticker and saves as CSV in raw/
"""

import os
import sys

import pandas as pd
import yfinance as yf
from datetime import datetime

# Usage: python yahoo_fetch_to_csv.py TICKER [DAYS]
# Example: python yahoo_fetch_to_csv.py AAPL 30

def fetch_yahoo_finance(ticker, days=1):
    """Fetch most recent Yahoo Finance data for a ticker (default: 1 day) using yfinance"""
    ticker_obj = yf.Ticker(ticker)
    df = ticker_obj.history(period=f"{days}d", interval="1d")
    if df.empty:
        raise ValueError(f"No data returned for {ticker}")
    df = df.reset_index()
    df = df.rename(columns={
        "Date": "date",
        "Open": "open",
        "High": "high",
        "Low": "low",
        "Close": "close",
        "Volume": "volume"
    })
    df = df[["date", "open", "high", "low", "close", "volume"]]
    df["symbol"] = ticker.upper()
    return df

def save_to_csv(df, ticker):
    raw_dir = os.path.join(os.path.dirname(__file__), "data", "raw")
    os.makedirs(raw_dir, exist_ok=True)
    out_path = os.path.join(raw_dir, f"{ticker.upper()}.csv")
    df.to_csv(out_path, index=False)
    print(f"Saved {out_path}")

def main():
    if len(sys.argv) < 2:
        print("Usage: python yahoo_fetch_to_csv.py TICKER [DAYS]")
        sys.exit(1)
    ticker = sys.argv[1]
    days = int(sys.argv[2]) if len(sys.argv) > 2 else 1
    df = fetch_yahoo_finance(ticker, days)
    save_to_csv(df, ticker)

if __name__ == "__main__":
    main()
