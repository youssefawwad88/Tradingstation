"""
Compact Intraday Data Update Job.

This script is optimized for frequent execution (e.g., every minute).
It fetches only the 'compact' (last 100 data points) from the API,
which is much faster and uses less data than a 'full' refresh.

The 'fetch_and_save_data' function will then intelligently merge this
small dataset, appending only the very latest candles to our existing CSV files.

This is the script that our orchestrator will call repeatedly during the
trading day.
"""

import sys
from pathlib import Path

# System path setup
project_root = Path(__file__).resolve().parent.parent
sys.path.append(str(project_root))

from utils import config
from utils import alpha_vantage_api

def run_compact_intraday_update():
    """
    Main function to run a compact update for intraday data.
    """
    print("--- Starting Compact Intraday Data Update ---")
    tickers = config.MASTER_TICKER_LIST
    if not tickers:
        print("!!! ERROR: The ticker list is empty.")
        return

    print(f"Processing {len(tickers)} tickers for compact update...")

    for ticker in tickers:
        print(f"\n--- Updating intraday data for {ticker} ---")

        # Fetch and save 30-Minute Intraday data using 'compact'
        alpha_vantage_api.fetch_and_save_data(
            ticker=ticker,
            data_dir=config.INTRADAY_30MIN_DIR,
            fetch_function=alpha_vantage_api.fetch_intraday_data,
            interval='30min',
            outputsize='compact' # The key change is here
        )

        # Fetch and save 1-Minute Intraday data using 'compact'
        alpha_vantage_api.fetch_and_save_data(
            ticker=ticker,
            data_dir=config.INTRADAY_1MIN_DIR,
            fetch_function=alpha_vantage_api.fetch_intraday_data,
            interval='1min',
            outputsize='compact' # The key change is here
        )

    print("\n--- Compact Intraday Data Update Complete ---")


if __name__ == "__main__":
    run_compact_intraday_update()
