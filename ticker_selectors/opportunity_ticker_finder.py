import sys
import os
import pandas as pd
import time
from tqdm import tqdm

# Add project root to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from utils.helpers import read_df_from_s3, save_list_to_s3, update_scheduler_status
from utils.alpha_vantage_api import get_daily_data

def run_opportunity_finder():
    """
    Applies final price and volume filters to a pre-qualified list of stocks
    to generate the final, actionable watchlist for the day.
    This job is designed to be very fast.
    """
    print("--- Starting Daily Opportunity Finder Job ---")
    
    # 1. Load the pre-filtered universe created by the weekly job
    print("Loading pre-filtered universe from cloud storage...")
    pre_filtered_df = read_df_from_s3('data/universe/prefiltered_universe.csv')
    if pre_filtered_df.empty:
        print("Could not load the pre-filtered universe. The weekly scan may need to be run. Exiting.")
        return

    pre_filtered_tickers = pre_filtered_df['ticker'].tolist()
    print(f"Loaded {len(pre_filtered_tickers)} pre-filtered stocks to analyze.")
    
    final_watchlist = []
    
    # 2. Iterate through the SMALL list and apply final checks
    for ticker in tqdm(pre_filtered_tickers, desc="Finalizing Watchlist"):
        try:
            # Fetch just enough daily data to check price and volume
            daily_data = get_daily_data(ticker, outputsize='compact')
            if daily_data is None or daily_data.empty or len(daily_data) < 30:
                tqdm.write(f"Skipping {ticker}: Not enough daily data for final checks.")
                continue

            latest_price = daily_data['close'].iloc[0]
            avg_volume_30d = daily_data['volume'].head(30).mean()

            # Apply the final price and volume filters
            if not (2.00 <= latest_price <= 200.00):
                tqdm.write(f"Skipping {ticker}: Fails Price rule (${latest_price:.2f}).")
                continue
            
            if avg_volume_30d < 1_000_000:
                tqdm.write(f"Skipping {ticker}: Fails Avg Volume rule ({avg_volume_30d:,.0f} shares/day).")
                continue
            
            tqdm.write(f">>> {ticker} added to final daily watchlist! <<<")
            final_watchlist.append(ticker)

        except Exception as e:
            tqdm.write(f"An unexpected error occurred while processing {ticker}: {e}")
        
        time.sleep(1) # Pause briefly between API calls

    # 3. Save the final, filtered list to the master tickerlist.txt
    print(f"\nFinal filtering complete. Found {len(final_watchlist)} stocks for today's watchlist.")
    if final_watchlist:
        save_list_to_s3(final_watchlist, 'tickerlist.txt')
        print("Successfully saved the new master tickerlist for the day.")
    else:
        print("No tickers passed the final checks. The master list will be empty.")

    print("--- Daily Opportunity Finder Job Finished ---")

if __name__ == "__main__":
    job_name = "opportunity_ticker_finder"
    update_scheduler_status(job_name, "Running")
    try:
        run_opportunity_finder()
        update_scheduler_status(job_name, "Success")
    except Exception as e:
        error_message = f"An unexpected error occurred: {e}"
        print(error_message)
        update_scheduler_status(job_name, "Fail", error_message)
