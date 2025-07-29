"""
Data Fetching Job for the Trading System.

This script is responsible for the INITIAL population of market data.
It fetches the full history for each ticker and then trims it down to a
reasonable size as defined in the config file.

This should be run once to set up the data, or when you want to do a
complete refresh. For frequent updates, use 'update_intraday_compact.py'.
"""

import sys
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent
sys.path.append(str(project_root))

from utils import config
from utils import alpha_vantage_api

def run_all_data_updates():
    """
    Main function to update all data types for all tickers.
    """
    print("--- Starting Full Market Data Update ---")
    tickers = config.MASTER_TICKER_LIST
    if not tickers:
        print("!!! ERROR: The ticker list is empty. Please add tickers to 'tickerlist.txt'.")
        return

    print(f"Processing {len(tickers)} tickers: {tickers}")

    for ticker in tickers:
        print(f"\n--- Updating data for {ticker} ---")
        
        # 1. Fetch and save Daily data, applying the max_rows rule
        alpha_vantage_api.fetch_and_save_data(
            ticker=ticker,
            data_dir=config.DAILY_DIR,
            fetch_function=alpha_vantage_api.fetch_daily_data,
            outputsize='full',
            max_rows=config.DAILY_DATA_MAX_ROWS
        )

        # 2. Fetch and save 30-Minute Intraday data, applying the max_rows rule
        alpha_vantage_api.fetch_and_save_data(
            ticker=ticker,
            data_dir=config.INTRADAY_30MIN_DIR,
            fetch_function=alpha_vantage_api.fetch_intraday_data,
            interval='30min',
            outputsize='full',
            max_rows=config.INTRADAY_30MIN_MAX_ROWS
        )

        # 3. Fetch and save 1-Minute Intraday data, applying the max_rows rule
        alpha_vantage_api.fetch_and_save_data(
            ticker=ticker,
            data_dir=config.INTRADAY_1MIN_DIR,
            fetch_function=alpha_vantage_api.fetch_intraday_data,
            interval='1min',
            outputsize='full',
            max_rows=config.INTRADAY_1MIN_MAX_ROWS
        )

    print("\n--- Full Market Data Update Complete ---")


if __name__ == "__main__":
    run_all_data_updates()
