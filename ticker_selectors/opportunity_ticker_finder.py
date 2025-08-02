import sys
import os
import pandas as pd
import time

# Add project root to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from utils.helpers import read_tickerlist_from_s3, save_list_to_s3, update_scheduler_status
from utils.alpha_vantage_api import get_company_overview, get_daily_data

def run_opportunity_finder():
    """
    Applies Ashraf-style pre-filters to a universe of stocks (S&P 500)
    to generate a small, actionable watchlist for the day.
    """
    print("--- Starting Opportunity Ticker Finder ---")
    
    # 1. Load the initial universe of stocks (S&P 500)
    print("Loading S&P 500 list from cloud storage...")
    # Assuming the S&P 500 list is stored as a text file at this path
    initial_universe = read_tickerlist_from_s3('data/universe/sp500.csv')
    if not initial_universe:
        print("Could not load S&P 500 universe. Exiting.")
        return

    print(f"Loaded {len(initial_universe)} stocks as the initial universe.")
    
    qualified_tickers = []
    
    # 2. Iterate and apply filters to each stock
    for i, ticker in enumerate(initial_universe):
        print(f"\n[{i+1}/{len(initial_universe)}] Filtering {ticker}...")
        
        try:
            # --- Filter 1: Fundamental Data (Market Cap, Float, Exchange) ---
            overview = get_company_overview(ticker)
            if not overview:
                print(f"Skipping {ticker}: Could not fetch company overview.")
                time.sleep(15) # Sleep longer if overview fails, it's a heavier API call
                continue

            market_cap = int(overview.get("MarketCapitalization", 0))
            shares_float = int(overview.get("SharesFloat", 0))
            exchange = overview.get("Exchange", "")

            if not (500_000_000 <= market_cap < 100_000_000_000):
                print(f"Skipping {ticker}: Fails Market Cap rule (${market_cap / 1_000_000_000:.2f}B).")
                continue
            
            if not (10_000_000 <= shares_float <= 150_000_000):
                print(f"Skipping {ticker}: Fails Float rule ({shares_float / 1_000_000:.2f}M shares).")
                continue

            if exchange not in ["NASDAQ", "NYSE"]:
                print(f"Skipping {ticker}: Fails Exchange rule ({exchange}).")
                continue
            
            print(f"  ✅ {ticker} passes fundamental checks.")

            # --- Filter 2: Price and Volume Data ---
            daily_data = get_daily_data(ticker, outputsize='compact')
            if daily_data is None or daily_data.empty or len(daily_data) < 30:
                print(f"Skipping {ticker}: Not enough daily data to analyze.")
                continue

            latest_price = daily_data['close'].iloc[0]
            avg_volume_30d = daily_data['volume'].head(30).mean()

            if not (2.00 <= latest_price <= 200.00):
                print(f"Skipping {ticker}: Fails Price rule (${latest_price:.2f}).")
                continue
            
            if avg_volume_30d < 1_000_000:
                print(f"Skipping {ticker}: Fails Avg Volume rule ({avg_volume_30d:,.0f} shares/day).")
                continue
            
            print(f"  ✅ {ticker} passes price/volume checks.")
            print(f"  >>> {ticker} is a qualified candidate! <<<")
            qualified_tickers.append(ticker)

        except Exception as e:
            print(f"An unexpected error occurred while processing {ticker}: {e}")
        
        # Respect API rate limits (premium keys can handle faster calls)
        time.sleep(1)

    # 3. Save the final, filtered list to S3
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
