import pandas as pd
import sys
import os
from datetime import datetime
import pytz
import numpy as np

# --- System Path Setup ---
# This makes sure the script can find the 'utils' directory
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from utils.helpers import (
    read_df_from_s3,
    save_df_to_s3,
    calculate_vwap,
    format_to_two_decimal
)

# --- Screener-Specific Configuration ---
VOLUME_SPIKE_THRESHOLD = 1.15  # 115%
STRONG_BODY_THRESHOLD = 60.0   # Min 60% body for a strong candle

def get_reclaim_quality(candle: pd.Series) -> str:
    """Analyzes a candle to determine the quality of a reclaim/rejection."""
    candle_range = candle['high'] - candle['low']
    if candle_range == 0:
        return "Weak"
    
    body_pct = (abs(candle['close'] - candle['open']) / candle_range) * 100
    
    if body_pct >= STRONG_BODY_THRESHOLD:
        return "Strong"
    elif body_pct >= 30:
        return "Moderate"
    else:
        return "Weak"

def run_avwap_screener():
    """Main function to execute the cloud-aware AVWAP screening logic."""
    print("\n--- Running Anchored VWAP (AVWAP) Screener (Cloud-Aware) ---")
    
    # --- 1. Load Prerequisite Data from Cloud Storage ---
    anchor_df = read_df_from_s3("data/avwap_anchors.csv")
    if anchor_df.empty:
        print("ERROR: Anchor file not found in cloud storage. Please run the find_avwap_anchors.py job first.")
        return
        
    anchor_df = anchor_df.set_index('Ticker')
    tickers = anchor_df.index.tolist()
    all_results = []
    ny_timezone = pytz.timezone('America/New_York')

    # --- 2. Process Each Ticker ---
    for ticker in tickers:
        try:
            # Load 30-minute intraday and daily data from cloud storage
            intraday_df = read_df_from_s3(f"data/intraday_30min/{ticker}_30min.csv")
            daily_df = read_df_from_s3(f"data/daily/{ticker}_daily.csv")

            if intraday_df.empty or daily_df.empty:
                continue
            
            # Convert timestamps to datetime objects and set index
            intraday_df['timestamp'] = pd.to_datetime(intraday_df['timestamp'])
            intraday_df = intraday_df.set_index('timestamp')
            daily_df['timestamp'] = pd.to_datetime(daily_df['timestamp'])
            daily_df = daily_df.set_index('timestamp')

            # --- 3. Get Live Data & Base Metrics ---
            latest_candle = intraday_df.iloc[-1]
            current_price = latest_candle['close']
            today_volume = intraday_df[intraday_df.index.date == datetime.now(ny_timezone).date()]['volume'].sum()
            avg_20d_volume = daily_df.tail(20)['volume'].mean()
            volume_vs_avg_pct = (today_volume / avg_20d_volume) * 100 if avg_20d_volume > 0 else 0

            avwap_results = {}

            # --- 4. Calculate AVWAP for each confirmed anchor ---
            ticker_anchors = anchor_df.loc[ticker]
            for i in [1, 2]:
                if ticker_anchors.get(f'Anchor {i} Confirmed?') != 'Yes':
                    continue

                anchor_date = pd.to_datetime(ticker_anchors[f'Anchor {i} Date'])
                avwap_df = intraday_df[intraday_df.index.date >= anchor_date.date()].copy()
                
                if avwap_df.empty: continue
                
                avwap_value = calculate_vwap(avwap_df)
                if avwap_value is None: continue
                
                avwap_results[i] = {
                    "value": avwap_value,
                    "reclaimed": current_price > avwap_value,
                }

            # --- 5. Determine Overall Setup & Signal ---
            reclaim_count = sum(1 for r in avwap_results.values() if r['reclaimed'])
            
            signal_direction = "None"
            if reclaim_count > 0:
                signal_direction = "Long"
            # Note: Short logic (rejection) can be added here if needed

            # --- 6. Final Validation & Scoring ---
            reclaim_quality = get_reclaim_quality(latest_candle)
            is_volume_ok = volume_vs_avg_pct >= (VOLUME_SPIKE_THRESHOLD * 100)
            
            setup_valid = (signal_direction == "Long" and is_volume_ok and reclaim_quality == "Strong")

            # --- 7. Populate Core Result Dictionary ---
            result = {
                "Date": latest_candle.name.strftime('%Y-%m-%d'),
                "Ticker": ticker,
                "Signal Direction": signal_direction,
                "Current Price": format_to_two_decimal(current_price),
                "Volume vs Avg %": format_to_two_decimal(volume_vs_avg_pct),
                "Reclaim Quality": reclaim_quality,
                "Setup Valid?": "TRUE" if setup_valid else "FALSE"
            }
            all_results.append(result)

        except Exception as e:
            print(f"   - ERROR processing {ticker} for AVWAP: {e}")

    # --- 8. Final Processing & Save to Cloud ---
    if not all_results:
        print("--- No AVWAP signals were generated. ---")
        return
        
    final_df = pd.DataFrame(all_results)
    final_df.sort_values(by=['Setup Valid?', 'Ticker'], ascending=[False, True], inplace=True)
    
    save_df_to_s3(final_df, 'data/signals/avwap_signals.csv')
    print("--- AVWAP Screener finished. Results saved to cloud. ---")

if __name__ == "__main__":
    run_avwap_screener()
