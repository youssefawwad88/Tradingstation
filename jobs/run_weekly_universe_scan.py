import sys
import os
import time
from tqdm import tqdm
import pandas as pd

# Add project root to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from utils.helpers import read_tickerlist_from_s3, save_df_to_s3, update_scheduler_status
from utils.alpha_vantage_api import get_company_overview

def run_universe_scan():
    """
    Performs an intelligent scan of a large stock universe to create a pre-filtered list.
    Includes a "pre-flight check" and a "circuit breaker" for maximum stability.
    """
    print("--- Starting Weekly Universe Scan Job (with Pre-Flight Check) ---")
    
    # --- PRE-FLIGHT CHECK 1: Load Initial Universe ---
    print("[1/3] Loading S&P 500 list from cloud storage...")
    initial_universe = read_tickerlist_from_s3('data/universe/sp500.csv')
    if not initial_universe or len(initial_universe) < 100: # Check for a reasonable number of tickers
        error_msg = "CRITICAL: Could not load a valid S&P 500 list from 'data/universe/sp500.csv'. Exiting."
        print(error_msg)
        raise Exception(error_msg)
    print(f"  ✅  Successfully loaded {len(initial_universe)} stocks to scan.")

    # --- PRE-FLIGHT CHECK 2: Test API Key ---
    print("[2/3] Performing API pre-flight check...")
    test_overview = get_company_overview('IBM') # Use a reliable, well-known ticker
    if not test_overview or "MarketCapitalization" not in test_overview:
        error_msg = "CRITICAL: API pre-flight check failed. The API key may be invalid or the service is down. Exiting."
        print(error_msg)
        raise Exception(error_msg)
    print("  ✅  API key is working correctly.")
    
    print("[3/3] Pre-flight checks passed. Starting full scan...")
    pre_filtered_stocks = []
    consecutive_failures = 0
    FAILURE_THRESHOLD = 10 # Allow for a few more intermittent failures

    for ticker in tqdm(initial_universe, desc="Scanning Universe"):
        if consecutive_failures >= FAILURE_THRESHOLD:
            error_msg = f"Circuit breaker tripped after {consecutive_failures} consecutive API failures."
            print(f"\nCRITICAL ERROR: {error_msg}")
            raise Exception(error_msg)

        try:
            overview = get_company_overview(ticker)
            if not overview:
                tqdm.write(f"Warning: Could not fetch company overview for {ticker}.")
                consecutive_failures += 1
                time.sleep(5)
                continue
            
            consecutive_failures = 0
            market_cap = int(overview.get("MarketCapitalization", 0))
            shares_float = int(overview.get("SharesFloat", 0))
            exchange = overview.get("Exchange", "")
            
            if (500_000_000 <= market_cap < 100_000_000_000 and
                10_000_000 <= shares_float <= 150_000_000 and
                exchange in ["NASDAQ", "NYSE"]):
                
                pre_filtered_stocks.append({
                    'ticker': ticker, 'market_cap': market_cap,
                    'float': shares_float, 'exchange': exchange
                })
        except Exception as e:
            tqdm.write(f"An unexpected error occurred while processing {ticker}: {e}")
            consecutive_failures += 1
        
        time.sleep(1) # 1 sec sleep for 150/min premium key is safe

    if pre_filtered_stocks:
        pre_filtered_df = pd.DataFrame(pre_filtered_stocks)
        save_path = 'data/universe/prefiltered_universe.csv'
        save_df_to_s3(pre_filtered_df, save_path)
        print(f"\nScan complete. Saved {len(pre_filtered_df)} pre-filtered stocks to {save_path}.")
    else:
        print("\nScan complete. No stocks met the fundamental criteria.")

    print("--- Weekly Universe Scan Job Finished ---")

if __name__ == "__main__":
    job_name = "weekly_universe_scan"
    update_scheduler_status(job_name, "Running")
    try:
        run_universe_scan()
        update_scheduler_status(job_name, "Success")
    except Exception as e:
        error_message = f"An unexpected error occurred: {e}"
        print(error_message)
        update_scheduler_status(job_name, "Fail", str(e))
