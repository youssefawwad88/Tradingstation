import sys
import os
import time

# Adjust the path to include the parent directory (trading-system)
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from utils.helpers import read_tickerlist_from_s3, save_df_to_s3
from utils.alpha_vantage_api import get_daily_data

def main():
    """
    Downloads full historical daily data for all tickers in the master list
    and saves it to cloud storage.
    """
    print("--- Starting Daily Data Update Job ---")

    # 1. Read the master ticker list from cloud storage
    tickers = read_tickerlist_from_s3('tickerlist.txt')
    if not tickers:
        print("Ticker list is empty. Nothing to do. Exiting.")
        return

    print(f"Found {len(tickers)} tickers to process.")

    # 2. Loop through each ticker, fetch data, and save to cloud storage
    for ticker in tickers:
        # Fetch the daily data using the API helper
        daily_df = get_daily_data(ticker)

        if not daily_df.empty:
            # Define the path in cloud storage where the file will be saved
            file_path = f"data/daily/{ticker}_daily.csv"
            
            # Save the DataFrame to our S3 Space
            save_df_to_s3(daily_df, file_path)
        else:
            print(f"Skipping save for {ticker} due to empty DataFrame.")

        # Alpha Vantage has a rate limit (e.g., 5 calls per minute).
        # We must add a delay to avoid being blocked. 15 seconds is safe.
        print("Waiting 15 seconds to respect API rate limit...")
        time.sleep(15)

    print("--- Daily Data Update Job Finished ---")


if __name__ == "__main__":
    main()
