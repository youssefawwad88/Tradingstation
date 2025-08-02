import os
import sys
import time
import pandas as pd

# Add parent directory to path to allow imports from utils
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils.alpha_vantage_api import AlphaVantageAPI
from utils.helpers import read_tickerlist_from_s3, read_df_from_s3, save_df_to_s3

def main():
    """
    Layer 2: Compact Mode Appender (Real-Time Updates)
    Only pulls the latest candle(s) and appends them to the existing data file.
    """
    print("--- Starting Compact Intraday Data Append Job ---")
    
    api_key = os.getenv('ALPHA_VANTAGE_API_KEY')
    if not api_key:
        print("FATAL: ALPHA_VANTAGE_API_KEY environment variable not set.")
        return
        
    av = AlphaVantageAPI(api_key)
    tickers = read_tickerlist_from_s3('tickerlist.txt')

    if not tickers:
        print("No tickers found in tickerlist.txt. Exiting job.")
        return

    print(f"Processing {len(tickers)} tickers for compact update...")

    for ticker in tickers:
        print(f"--- Processing {ticker} ---")
        
        # --- 1-Minute Data Append ---
        file_path_1min = f'data/intraday/{ticker}.csv'
        existing_1min_df = read_df_from_s3(file_path_1min)
        
        # Fetch compact data (last 100 points)
        new_1min_df = av.get_intraday(ticker, interval='1min', outputsize='compact')
        
        if not new_1min_df.empty:
            if not existing_1min_df.empty:
                # Combine and deduplicate
                combined_df = pd.concat([existing_1min_df, new_1min_df])
                combined_df.drop_duplicates(subset=['timestamp'], keep='last', inplace=True)
                combined_df.sort_values('timestamp', ascending=False, inplace=True)
                save_df_to_s3(combined_df, file_path_1min)
                print(f"Successfully updated 1-min data for {ticker}. Total rows: {len(combined_df)}")
            else:
                # If no existing file, save the new data
                save_df_to_s3(new_1min_df, file_path_1min)
                print(f"No existing 1-min data for {ticker}. Created new file.")
        else:
            print(f"Warning: No new 1-min data returned for {ticker}.")
            
        time.sleep(1) # 1 second delay for premium key

    print("\n--- Compact Intraday Data Append Job Finished ---")

if __name__ == "__main__":
    main()
