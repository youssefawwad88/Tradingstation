"""
Opportunity Ticker Finder (Ashraf-Style Golden Criteria)

This script automatically detects explosive tickers each morning by combining
real-time Top Movers with the S&P 500 universe. It applies a multi-factor
scoring system based on Gap %, Volume, Catalysts, and technical alignment
to identify the highest-probability opportunities for the day.
"""

import pandas as pd
import sys
import os
from datetime import datetime
import pytz
import requests
import time

# --- System Path Setup ---
PROJECT_ROOT = '/content/drive/MyDrive/trading-system'
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from utils import config, helpers
from utils.sp500_loader import load_sp500_tickers

# --- Selector Configuration ---
MIN_GAP_PCT = 2.0
PREMARKET_VOLUME_SPIKE_RATIO = 1.25 # 125%
MIN_SCORE_TO_LIST = 3

# --- API Functions ---
def fetch_top_movers() -> list[str]:
    """Fetches top gainers and losers, returning a list of tickers."""
    print("--> Fetching Top Movers...")
    # This function is a placeholder for a real-time pre-market scanner API.
    # Alpha Vantage's free tier for Top Movers is not real-time pre-market.
    # In a live environment, you would use a paid API here.
    print("    - NOTE: Using placeholder tickers. A real pre-market API is needed for live data.")
    return ['TSLA', 'NVDA', 'AMD'] # Placeholder tickers

def fetch_premarket_data(ticker: str) -> pd.DataFrame:
    """Fetches extended hours intraday data to analyze pre-market action."""
    try:
        # NOTE: This requires a premium Alpha Vantage subscription for real-time data.
        url = f"https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY_EXTENDED&symbol={ticker}&interval=5min&slice=year1month1&apikey={config.ALPHA_VANTAGE_API_KEY}"
        r = requests.get(url)
        data = r.content
        df = pd.read_csv(pd.io.common.BytesIO(data))
        df['time'] = pd.to_datetime(df['time'])
        df.set_index('time', inplace=True)
        return df
    except Exception:
        return pd.DataFrame()

def fetch_earnings_catalyst(ticker: str) -> str:
    """Checks if a stock has earnings today or tomorrow."""
    # Placeholder for a real earnings API call
    return "Earnings Today" if ticker == "TSLA" else "N/A" # Example

def fetch_news_catalyst(ticker: str) -> str:
    """Checks for significant news catalysts."""
    # Placeholder for a real news API call
    return "Upgrade by Citi" if ticker == "TSLA" else "N/A" # Example

# --- Main Selector Logic ---
def run_opportunity_finder():
    """Main function to find, score, and save opportunity tickers."""
    print("\n--- Running Ashraf-Style Golden Opportunity Finder ---")
    
    # --- 1. Load Tickers and Anchor Data ---
    top_movers_tickers = fetch_top_movers()
    sp500_tickers = load_sp500_tickers()
    anchor_df = pd.read_csv(config.DATA_DIR / "avwap_anchors.csv").set_index('Ticker') if (config.DATA_DIR / "avwap_anchors.csv").exists() else pd.DataFrame()

    combined_tickers = sorted(list(set(top_movers_tickers + sp500_tickers)))
    print(f"\n--- Analyzing a combined universe of {len(combined_tickers)} unique tickers ---")

    # --- 2. Per-Ticker Analysis ---
    all_ticker_data = []
    for ticker in combined_tickers:
        print(f"  -> Analyzing {ticker}...")
        
        # --- Load Data ---
        daily_file = config.DAILY_DIR / f"{ticker}_daily.csv"
        intraday_file = config.INTRADAY_1MIN_DIR / f"{ticker}_1min.csv"
        if not daily_file.exists() or not intraday_file.exists(): continue
        
        daily_df = pd.read_csv(daily_file, index_col=0, parse_dates=True)
        intraday_df = pd.read_csv(intraday_file, index_col=0, parse_dates=True)
        if len(daily_df) < 50: continue

        # --- Pre-computation ---
        latest_daily = daily_df.iloc[-1]
        prev_close = daily_df.iloc[-2][config.CLOSE_COL]
        premarket_df = helpers.get_premarket_data(intraday_df)
        
        if premarket_df.empty: continue # Skip if no pre-market data
        
        premarket_price = premarket_df[config.CLOSE_COL].iloc[-1]
        gap_percent = ((premarket_price - prev_close) / prev_close) * 100
        premarket_volume = premarket_df[config.VOLUME_COL].sum()
        avg_early_volume = helpers.calculate_avg_early_volume(intraday_df, days=5)

        # --- 3. Strict Filtering (Ashraf's Golden Criteria) ---
        if abs(gap_percent) < MIN_GAP_PCT:
            print(f"     - SKIP: Gap % ({gap_percent:.2f}%) is not significant.")
            continue
        if avg_early_volume > 0 and premarket_volume < (avg_early_volume * PREMARKET_VOLUME_SPIKE_RATIO):
            print(f"     - SKIP: Low pre-market volume.")
            continue

        # --- 4. Scoring Module ---
        score = 0
        ticker_data = {'Ticker': ticker}
        
        # Score 1: Gap & Volume
        if abs(gap_percent) > MIN_GAP_PCT: score += 1
        if avg_early_volume > 0 and premarket_volume > (avg_early_volume * 1.3): score +=1 # 130%
        
        # Score 2: Catalyst
        catalyst_news = fetch_news_catalyst(ticker)
        catalyst_earnings = fetch_earnings_catalyst(ticker)
        catalyst_type = catalyst_earnings if catalyst_earnings != "N/A" else catalyst_news
        if catalyst_type != "N/A": score += 1
        ticker_data['Catalyst Type'] = catalyst_type

        # Score 3 & 4: Technical Alignment (AVWAP & EMA)
        avwap_reclaim, ema_confluence = "No", "No"
        daily_df['EMA21'] = daily_df[config.CLOSE_COL].ewm(span=21, adjust=False).mean()
        daily_df['EMA50'] = daily_df[config.CLOSE_COL].ewm(span=50, adjust=False).mean()
        
        if latest_daily[config.CLOSE_COL] > daily_df['EMA21'].iloc[-1] and latest_daily[config.CLOSE_COL] > daily_df['EMA50'].iloc[-1]:
            ema_confluence = "Yes"
            score += 1
            
        if not anchor_df.empty and ticker in anchor_df.index:
            anchor_date_str = anchor_df.loc[ticker, 'Anchor 1 Date']
            if pd.notna(anchor_date_str) and anchor_date_str != "N/A":
                avwap_df = daily_df[daily_df.index >= pd.to_datetime(anchor_date_str)].copy()
                avwap_df['AVWAP'] = helpers.calculate_vwap(avwap_df)
                if premarket_price > avwap_df['AVWAP'].iloc[-1]:
                    avwap_reclaim = "Yes"
                    score += 1
        
        ticker_data['AVWAP Reclaim'] = avwap_reclaim
        ticker_data['EMA Confluence'] = ema_confluence
        
        # --- 5. Finalize Data for Output ---
        if score < MIN_SCORE_TO_LIST:
            print(f"     - SKIP: Score ({score}) is below threshold.")
            continue

        ticker_data['Date'] = datetime.now(pytz.timezone('America/New_York')).strftime('%Y-%m-%d')
        ticker_data['Source'] = "Top Mover" if ticker in top_movers_tickers else "S&P 500"
        ticker_data['Price'] = helpers.format_to_two_decimal(premarket_price)
        ticker_data['Gap %'] = helpers.format_to_two_decimal(gap_percent)
        ticker_data['Volume Spike?'] = "Yes" if avg_early_volume > 0 and premarket_volume > (avg_early_volume * PREMARKET_VOLUME_SPIKE_RATIO) else "No"
        ticker_data['Score'] = score
        ticker_data['Setup Potential'] = "Gap & Go" if gap_percent > 0 and avwap_reclaim == "Yes" else "AVWAP Reclaim" if avwap_reclaim == "Yes" else "Pullback"
        ticker_data['Add to Watchlist?'] = '✅ Yes'
        
        all_ticker_data.append(ticker_data)

    # --- 6. Final Output ---
    if not all_ticker_data:
        print("\n--- No tickers met the golden criteria today. ---")
        return
        
    final_df = pd.DataFrame(all_ticker_data)
    
    final_columns = [
        "Date", "Ticker", "Source", "Price", "Gap %", "Volume Spike?", 
        "AVWAP Reclaim", "EMA Confluence", "Catalyst Type", "Score", 
        "Setup Potential", "Add to Watchlist?"
    ]
    final_df = final_df.reindex(columns=final_columns).fillna("N/A")
    final_df.sort_values(by="Score", ascending=False, inplace=True)

    helpers.save_signal_to_csv(final_df, 'opportunities_today')
    print("\n--- Tickerlist Update Skipped ---")
    print("Please manually review 'opportunities_today_signals.csv' and update tickerlist.txt.")


if __name__ == "__main__":
    run_opportunity_finder()
