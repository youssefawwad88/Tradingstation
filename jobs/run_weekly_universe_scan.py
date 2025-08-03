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
    Performs a robust, intelligent scan of a large stock universe.
    Includes a "pre-flight check" and a "circuit breaker".
    """
    print("--- Starting Weekly Universe Scan Job (Final Version) ---")
    
    print("[1/3] Loading S&P 500 list...")
    initial_universe = read_tickerlist_from_s3('data/universe/sp500.csv')
    if not initial_universe or len(initial_universe) < 100:
        raise Exception("CRITICAL: Could not load a valid S&P 500 list.")
    print(f"  ✅ Loaded {len(initial_universe)} stocks.")

    print("[2/3] Performing API pre-flight check...")
    if not get_company_overview('IBM'):
        raise Exception("CRITICAL: API pre-flight check failed. Check API key.")
    print("  ✅ API key is working.")
    
    print("[3/3] Pre-flight checks passed. Starting full scan...")
    pre_filtered_stocks = []
    consecutive_failures = 0
    FAILURE_THRESHOLD = 15

    for ticker in tqdm(initial_universe, desc="Scanning Universe"):
        if consecutive_failures >= FAILURE_THRESHOLD:
            raise Exception(f"Circuit breaker tripped after {consecutive_failures} consecutive API failures.")

        try:
            overview = get_company_overview(ticker)
            if not overview:
                tqdm.write(f"Warning: No overview for {ticker}.")
                consecutive_failures += 1
                continue
            
            consecutive_failures = 0
            market_cap = int(overview.get("MarketCapitalization", 0))
            shares_float = int(overview.get("SharesFloat", 0))
            exchange = overview.get("Exchange", "")
            
            if (500_000_000 <= market_cap < 100_000_000_000 and
                10_000_000 <= shares_float <= 150_000_000 and
                exchange in ["NASDAQ", "NYSE"]):
                pre_filtered_stocks.append({'ticker': ticker})
        except Exception as e:
            tqdm.write(f"Error processing {ticker}: {e}")
            consecutive_failures += 1
        
        # With a 150/min key, we can be more aggressive. 60s / 150 = 0.4s per call.
        time.sleep(0.5)

    if pre_filtered_stocks:
        pre_filtered_df = pd.DataFrame(pre_filtered_stocks)
        save_df_to_s3(pre_filtered_df, 'data/universe/prefiltered_universe.csv')
        print(f"\nScan complete. Saved {len(pre_filtered_df)} pre-filtered stocks.")
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
        update_scheduler_status(job_name, "Fail", str(e))
