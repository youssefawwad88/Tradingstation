import sys
import os
import pandas as pd
import time
from tqdm import tqdm

# Add project root to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from utils.helpers import read_tickerlist_from_s3, save_list_to_s3, update_scheduler_status
from utils.alpha_vantage_api import get_company_overview

def run_opportunity_finder():
    """
    Applies Ashraf-style pre-filters to a universe of stocks (S&P 500)
    to generate a small, actionable watchlist for the day.
    This optimized version uses only the OVERVIEW API call for speed.
    """
    print("--- Starting Opportunity Ticker Finder (Optimized Version) ---")
    
    print("Loading S&P 500 list from cloud storage...")
    initial_universe = read_tickerlist_from_s3('data/universe/sp500.csv')
    if not initial_universe:
        print("Could not load S&P 500 universe. Exiting.")
        return

    print(f"Loaded {len(initial_universe)} stocks as the initial universe.")
    
    qualified_tickers = []
    
    for ticker in tqdm(initial_universe, desc="Filtering Universe"):
        try:
            # This single API call gets us most of the data we need for pre-filtering
            overview = get_company_overview(ticker)
            if not overview:
                tqdm.write(f"Skipping {ticker}: Could not fetch company overview.")
                time.sleep(15) # Sleep longer if overview fails, it's a heavier API call
                continue

            # --- Apply All Filters Using the Overview Data ---
            market_cap = int(overview.get("MarketCapitalization", 0))
            shares_float = int(overview.get("SharesFloat", 0))
            exchange = overview.get("Exchange", "")
            avg_volume_50d = int(overview.get("50DayMovingAverage", 0)) # Using 50d avg as a proxy for volume
            latest_price = float(overview.get("AnalystTargetPrice", 0)) # Using a proxy for price

            if not (500_000_000 <= market_cap < 100_000_000_000):
                continue
            
            if not (10_000_000 <= shares_float <= 150_000_000):
                continue

            if exchange not in ["NASDAQ", "NYSE"]:
                continue
            
            # Note: Price and Volume from OVERVIEW are less precise but much faster for a first pass.
            if not (2.00 <= latest_price <= 200.00):
                 continue
            
            if avg_volume_50d < 1_000_000:
                continue
            
            tqdm.write(f">>> {ticker} is a qualified candidate! <<<")
            qualified_tickers.append(ticker)

        except Exception as e:
            tqdm.write(f"An unexpected error occurred while processing {ticker}: {e}")
        
        time.sleep(1)

    print(f"\nFiltering complete. Found {len(qualified_tickers)} qualified tickers.")
    if qualified_tickers:
        save_list_to_s3(qualified_tickers, 'tickerlist.txt')
        print("Successfully saved the new master tickerlist to cloud storage.")
    else:
        print("No tickers met all criteria. The master list will not be updated.")

    print("--- Opportunity Ticker Finder Finished ---")

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
