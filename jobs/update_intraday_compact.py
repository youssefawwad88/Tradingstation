import pandas as pd
import sys
import os
import time

# Add project root to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from utils.helpers import read_tickerlist_from_s3, save_df_to_s3, read_df_from_s3, update_scheduler_status
from utils.alpha_vantage_api import get_intraday_data

def run_compact_append():
    """
    Runs the compact append process for intraday data.
    - Fetches the latest 'compact' data (100 rows).
    - Reads the existing full data from S3.
    - Appends new, unique rows and saves back.
    """
    print("--- Starting Compact Intraday Data Append Job ---")
    
    tickers = read_tickerlist_from_s3()
    if not tickers:
        print("No tickers found in tickerlist.txt. Exiting job.")
        return

    print(f"Processing {len(tickers)} tickers for compact update...")

    for ticker in tickers:
        for interval in ['1min', '30min']:
            print(f"\n--- Processing {ticker} for {interval} interval ---")
            try:
                # 1. Fetch the latest compact data from API
                # CORRECTED: Changed 'output_size' to 'outputsize'
                latest_df = get_intraday_data(ticker, interval=interval, outputsize='compact')
                if latest_df.empty:
                    print(f"No new {interval} data returned for {ticker}. Skipping.")
                    continue
                
                print(f"Successfully fetched {len(latest_df)} latest intraday data points for {ticker}.")

                # 2. Read the existing full data file from S3
                file_path = f'data/intraday/{ticker}_{interval}.csv'
                if interval == '30min':
                    file_path = f'data/intraday_30min/{ticker}_{interval}.csv'
                
                existing_df = read_df_from_s3(file_path)

                # 3. Combine, deduplicate, and sort
                if not existing_df.empty:
                    existing_df['timestamp'] = pd.to_datetime(existing_df['timestamp'])
                    latest_df['timestamp'] = pd.to_datetime(latest_df['timestamp'])
                    
                    combined_df = pd.concat([existing_df, latest_df], ignore_index=True)
                    combined_df.drop_duplicates(subset=['timestamp'], keep='last', inplace=True)
                else:
                    combined_df = latest_df

                combined_df.sort_values(by='timestamp', ascending=False, inplace=True)
                
                # 4. Save the updated file back to S3
                save_df_to_s3(combined_df, file_path)
                print(f"Finished processing {ticker} for {interval}. Total rows now: {len(combined_df)}")

            except Exception as e:
                print(f"ERROR processing {ticker} for {interval}: {e}")
        
        # Respect API rate limits
        time.sleep(1)

    print("\n--- Compact Intraday Data Append Job Finished ---")

if __name__ == "__main__":
    job_name = "update_intraday_compact"
    update_scheduler_status(job_name, "Running")
    try:
        run_compact_append()
        update_scheduler_status(job_name, "Success")
    except Exception as e:
        error_message = f"An unexpected error occurred: {e}"
        print(error_message)
        update_scheduler_status(job_name, "Fail", error_message)
