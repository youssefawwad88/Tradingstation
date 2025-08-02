import pandas as pd
import sys
import os
from datetime import datetime, timedelta

# Add project root to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from utils.helpers import read_tickerlist_from_s3, save_df_to_s3, update_scheduler_status
from utils.alpha_vantage_api import get_daily_data, get_intraday_data

def run_full_rebuild():
    """
    Runs the full data rebuild process once per day.
    - Fetches a clean, extended history for daily, 30-min, and 1-min data.
    - Overwrites existing files to ensure a clean slate.
    """
    print("--- Starting Daily Full Data Rebuild Job ---")
    
    # Read the master ticker list from S3
    tickers = read_tickerlist_from_s3()
    if not tickers:
        print("No tickers found in tickerlist.txt. Exiting job.")
        return

    print(f"Found {len(tickers)} tickers to process for full rebuild.")

    for ticker in tickers:
        print(f"\n--- Processing {ticker} for full rebuild ---")

        # 1. Daily Data (200 days)
        try:
            daily_df = get_daily_data(ticker, output_size='compact') # Compact gives ~100 rows, full gives years
            if not daily_df.empty:
                # Alpha Vantage 'compact' is 100 rows. Let's fetch 'full' and slice to 200 for more history.
                daily_df_full = get_daily_data(ticker, output_size='full')
                if not daily_df_full.empty:
                    daily_df_full = daily_df_full.head(200)
                    save_df_to_s3(daily_df_full, f'data/daily/{ticker}_daily.csv')
                    print(f"Successfully saved {len(daily_df_full)} rows of daily data for {ticker}.")
                else:
                    print(f"Warning: No 'full' daily data returned for {ticker}.")
            else:
                print(f"Warning: No 'compact' daily data returned for {ticker}.")
        except Exception as e:
            print(f"ERROR fetching or saving daily data for {ticker}: {e}")

        # 2. 30-Minute Intraday Data (500 rows)
        try:
            intraday_30min_df = get_intraday_data(ticker, interval='30min', output_size='full')
            if not intraday_30min_df.empty:
                intraday_30min_df = intraday_30min_df.head(500)
                save_df_to_s3(intraday_30min_df, f'data/intraday_30min/{ticker}_30min.csv')
                print(f"Successfully saved {len(intraday_30min_df)} rows of 30-min data for {ticker}.")
            else:
                print(f"Warning: No 30-min data returned for {ticker}.")
        except Exception as e:
            print(f"ERROR fetching or saving 30-min data for {ticker}: {e}")

        # 3. 1-Minute Intraday Data (Last 7 days)
        try:
            # Fetching 'full' for 1-min can be very large, let's process it carefully
            intraday_1min_df = get_intraday_data(ticker, interval='1min', output_size='full')
            if not intraday_1min_df.empty:
                # Convert timestamp to datetime and filter for the last 7 days
                intraday_1min_df['timestamp'] = pd.to_datetime(intraday_1min_df['timestamp'])
                seven_days_ago = datetime.now() - timedelta(days=7)
                intraday_1min_df = intraday_1min_df[intraday_1min_df['timestamp'] >= seven_days_ago]
                
                save_df_to_s3(intraday_1min_df, f'data/intraday/{ticker}_1min.csv')
                print(f"Successfully saved {len(intraday_1min_df)} rows of 1-min data for {ticker} (last 7 days).")
            else:
                print(f"Warning: No 1-min data returned for {ticker}.")
        except Exception as e:
            print(f"ERROR fetching or saving 1-min data for {ticker}: {e}")

    print("\n--- Daily Full Data Rebuild Job Finished ---")

if __name__ == "__main__":
    job_name = "update_all_data"
    update_scheduler_status(job_name, "Running")
    try:
        run_full_rebuild()
        update_scheduler_status(job_name, "Success")
    except Exception as e:
        error_message = f"An unexpected error occurred: {e}"
        print(error_message)
        update_scheduler_status(job_name, "Fail", error_message)
