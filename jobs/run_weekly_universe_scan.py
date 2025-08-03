import sys
import os
import time
from tqdm import tqdm

# Add project root to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from utils.helpers import read_tickerlist_from_s3, save_df_to_s3, update_scheduler_status
from utils.alpha_vantage_api import get_company_overview

def run_universe_scan():
    """
    Performs a slow, intensive scan of a large stock universe (e.g., S&P 500)
    to create a smaller, pre-filtered list of stocks that meet fundamental criteria.
    This job is designed to be run once per week during off-hours.
    """
    print("--- Starting Weekly Universe Scan Job ---")
    
    # Load the full S&P 500 list
    print("Loading S&P 500 list from cloud storage...")
    initial_universe = read_tickerlist_from_s3('data/universe/sp500.csv')
    if not initial_universe:
        print("Could not load S&P 500 universe. Exiting.")
        return

    print(f"Loaded {len(initial_universe)} stocks to scan.")
    
    pre_filtered_stocks = []
    
    for ticker in tqdm(initial_universe, desc="Scanning Universe"):
        try:
            overview = get_company_overview(ticker)
            if not overview:
                time.sleep(15) # Sleep longer if overview fails, it's a heavy API call
                continue

            # Apply the strict fundamental filters
            market_cap = int(overview.get("MarketCapitalization", 0))
            shares_float = int(overview.get("SharesFloat", 0))
            exchange = overview.get("Exchange", "")
            
            if not (500_000_000 <= market_cap < 100_000_000_000):
                continue
            
            if not (10_000_000 <= shares_float <= 150_000_000):
                continue

            if exchange not in ["NASDAQ", "NYSE"]:
                continue
            
            # If all fundamental checks pass, add it to our pre-filtered list
            pre_filtered_stocks.append({
                'ticker': ticker,
                'market_cap': market_cap,
                'float': shares_float,
                'exchange': exchange
            })
            tqdm.write(f"  ✅ {ticker} passed fundamental checks.")

        except Exception as e:
            tqdm.write(f"An unexpected error occurred while processing {ticker}: {e}")
        
        time.sleep(1.5) # Be extra polite to the API during this long-running job

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
        update_scheduler_status(job_name, "Fail", error_message)
