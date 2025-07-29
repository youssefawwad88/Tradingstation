import sys
import os
import time
import pandas as pd

# Adjust the path to include the parent directory (trading-system)
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from utils.helpers import read_tickerlist_from_s3, read_df_from_s3, save_df_to_s3
from utils.alpha_vantage_api import get_intraday_data

def merge_and_update_data(existing_df, new_df):
    """
    Intelligently merges the new compact data with existing data,
    avoiding duplicates and keeping it sorted.
    """
    if 'timestamp' not in existing_df.columns or 'timestamp' not in new_df.columns:
        print("Timestamp column missing, cannot merge. Returning new data.")
        return new_df.sort_values('timestamp', ascending=False).reset_index(drop=True)

    # Combine and drop duplicates, keeping the newest entry
    combined_df = pd.concat([new_df, existing_df]).drop_duplicates(subset=['timestamp'], keep='first')
    
    # Sort by timestamp descending (newest first) and reset index
    return combined_df.sort_values('timestamp', ascending=False).reset_index(drop=True)

def process_ticker_interval(ticker, interval, data_folder):
    """
    Processes a single ticker for a specific interval (e.g., '1min' or '30min').
    """
    print(f"--- Processing {ticker} for {interval} interval ---")
    
    # 1. Define the path for this ticker's data in the cloud
    file_path = f"data/{data_folder}/{ticker}_{interval}.csv"

    # 2. Read the existing data for this ticker from cloud storage
    existing_df = read_df_from_s3(file_path)

    # 3. Fetch the latest 'compact' data from the API
    new_compact_df = get_intraday_data(ticker, interval=interval, outputsize='compact')

    if new_compact_df.empty:
        print(f"No new data fetched for {ticker} ({interval}). Nothing to update.")
        return

    # 4. Merge the new data with the old data
    updated_df = merge_and_update_data(existing_df, new_compact_df)
    
    # 5. Save the newly merged DataFrame back to cloud storage
    save_df_to_s3(updated_df, file_path)
    
    print(f"Finished processing {ticker} for {interval}. Total rows: {len(updated_df)}")


def main():
    """
    Main function to run a compact update for all intraday data.
    """
    print("--- Starting Compact Intraday Data Update Job ---")
    
    tickers = read_tickerlist_from_s3('tickerlist.txt')
    if not tickers:
        print("Ticker list is empty. Nothing to do. Exiting.")
        return

    print(f"Processing {len(tickers)} tickers for compact update...")

    for ticker in tickers:
        # Process 1-minute data
        process_ticker_interval(ticker, '1min', 'intraday')
        
        # Add a delay to respect API rate limits
        print("Waiting 15 seconds before next API call...")
        time.sleep(15)

        # Process 30-minute data
        process_ticker_interval(ticker, '30min', 'intraday_30min')

        # Add another delay if there are more tickers in the loop
        if ticker != tickers[-1]:
            print("Waiting 15 seconds before next ticker...")
            time.sleep(15)

    print("\n--- Compact Intraday Data Update Complete ---")


if __name__ == "__main__":
    main()
