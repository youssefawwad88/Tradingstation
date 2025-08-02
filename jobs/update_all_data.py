import os
import sys
import time
import pandas as pd

# Add parent directory to path to allow imports from utils
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils.alpha_vantage_api import AlphaVantageAPI
from utils.helpers import read_tickerlist_from_s3, save_df_to_s3

def main():
    """
    Layer 1: Full Rebuild (Once per Day)
    Clears outdated data and pulls a fresh, clean slate for all data types.
    """
    print("--- Starting Full Data Rebuild Job ---")
    
    api_key = os.getenv('ALPHA_VANTAGE_API_KEY')
    if not api_key:
        print("FATAL: ALPHA_VANTAGE_API_KEY environment variable not set.")
        return
        
    av = AlphaVantageAPI(api_key)
    tickers = read_tickerlist_from_s3('tickerlist.txt')
    
    if not tickers:
        print("No tickers found in tickerlist.txt. Exiting job.")
        return

    print(f"Found {len(tickers)} tickers to process for full rebuild.")

    for ticker in tickers:
        print(f"\n--- Processing {ticker} ---")
        
        # 1. Daily Data (200 rows)
        print(f"Fetching daily data for {ticker}...")
        daily_df = av.get_daily_adjusted(ticker, outputsize='full')
        if not daily_df.empty:
            daily_df = daily_df.head(200) # Keep last 200 rows
            save_df_to_s3(daily_df, f'data/daily/{ticker}.csv')
            print(f"Successfully saved 200 daily rows for {ticker}.")
        else:
            print(f"Warning: No daily data returned for {ticker}.")
        time.sleep(1) # 1 second delay for premium key

        # 2. 30-Minute Data (500 rows)
        print(f"Fetching 30min intraday data for {ticker}...")
        intraday_30min_df = av.get_intraday(ticker, interval='30min', outputsize='full')
        if not intraday_30min_df.empty:
            intraday_30min_df = intraday_30min_df.head(500) # Keep last 500 rows
            save_df_to_s3(intraday_30min_df, f'data/intraday_30min/{ticker}.csv')
            print(f"Successfully saved 500 30-min rows for {ticker}.")
        else:
            print(f"Warning: No 30-min data returned for {ticker}.")
        time.sleep(1)

        # 3. 1-Minute Data (7 days)
        print(f"Fetching 1min intraday data for {ticker}...")
        # Alpha Vantage 'full' for 1-min is typically 7-10 days
        intraday_1min_df = av.get_intraday(ticker, interval='1min', outputsize='full')
        if not intraday_1min_df.empty:
            # No need to slice by rows, 'full' gives the desired amount
            save_df_to_s3(intraday_1min_df, f'data/intraday/{ticker}.csv')
            print(f"Successfully saved full 1-min intraday data for {ticker}.")
        else:
            print(f"Warning: No 1-min data returned for {ticker}.")
        time.sleep(1)

    print("\n--- Full Data Rebuild Job Finished ---")

if __name__ == "__main__":
    main()
