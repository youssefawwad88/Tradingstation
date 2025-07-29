import pandas as pd
import sys
import os
import numpy as np

# --- System Path Setup ---
# This makes sure the script can find the 'utils' directory
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from utils.helpers import (
    read_tickerlist_from_s3,
    read_df_from_s3,
    save_df_to_s3,
    format_to_two_decimal
)

# --- Screener-Specific Configuration ---
MIN_DROP_PCT = -7.0
MIN_GAP_DOWN_PCT = -1.5
VOLUME_SPIKE_RATIO = 1.3 # 130%
MIN_BODY_PCT = 65.0
MIN_RED_DAYS_IN_PREVIOUS_5 = 3

def run_exhaustion_screener():
    """Main function to execute the cloud-aware Exhaustion Reversal screening logic."""
    print("\n--- Running Exhaustion Reversal Screener (Cloud-Aware) ---")
    
    # --- 1. Load Prerequisite Data from Cloud Storage ---
    tickers = read_tickerlist_from_s3('tickerlist.txt')
    if not tickers:
        print("Ticker list from cloud is empty. Exiting.")
        return
        
    all_results = []

    for ticker in tickers:
        try:
            # --- 2. Load Data and Calculate Indicators ---
            df = read_df_from_s3(f"data/daily/{ticker}_daily.csv")
            if len(df) < 21: # Need at least 21 days for 20-day avg volume
                continue

            df['timestamp'] = pd.to_datetime(df['timestamp'])
            df = df.set_index('timestamp')

            latest = df.iloc[-1]
            previous = df.iloc[-2]
            last_5_candles = df.iloc[-6:-1]

            # --- 3. Evaluate Core Daily Conditions ---
            highest_close_in_5d = last_5_candles['close'].max()
            drop_percent = ((latest['close'] - highest_close_in_5d) / highest_close_in_5d) * 100
            is_significant_drop = drop_percent <= MIN_DROP_PCT

            gap_percent = ((latest['open'] - previous['close']) / previous['close']) * 100
            is_gap_down = gap_percent <= MIN_GAP_DOWN_PCT

            avg_20d_volume = df['volume'].iloc[-21:-1].mean()
            is_volume_spike = latest['volume'] >= (avg_20d_volume * VOLUME_SPIKE_RATIO)
            
            is_reclaimed = latest['close'] > previous['low']
            
            candle_range = latest['high'] - latest['low']
            body_pct = (abs(latest['close'] - latest['open']) / candle_range * 100) if candle_range > 0 else 0
            is_strong_green_candle = latest['close'] > latest['open'] and body_pct >= MIN_BODY_PCT

            red_days_count = (last_5_candles['close'] < last_5_candles['open']).sum()
            has_prior_weakness = red_days_count >= MIN_RED_DAYS_IN_PREVIOUS_5

            # --- 4. Final Validation ---
            conditions = {
                "Significant Drop": is_significant_drop,
                "Gap Down": is_gap_down,
                "Volume Spike": is_volume_spike,
                "Reclaimed Support": is_reclaimed,
                "Strong Green Candle": is_strong_green_candle,
                "Prior Weakness": has_prior_weakness
            }
            setup_valid = all(conditions.values())

            # --- 5. Populate Core Results ---
            result = {
                "Date": latest.name.strftime('%Y-%m-%d'),
                "Ticker": ticker,
                "Close": format_to_two_decimal(latest['close']),
                "Drop %": format_to_two_decimal(drop_percent),
                "Gap %": format_to_two_decimal(gap_percent),
                "Volume Spike?": "Yes" if is_volume_spike else "No",
                "Reclaimed Support?": "Yes" if is_reclaimed else "No",
                "Strong Green Candle?": "Yes" if is_strong_green_candle else "No",
                "Prior Weakness?": "Yes" if has_prior_weakness else "No",
                "Setup Valid?": "TRUE" if setup_valid else "FALSE"
            }
            all_results.append(result)

        except Exception as e:
            print(f"   - ERROR processing {ticker} for Exhaustion Reversal: {e}")

    # --- 6. Final Processing & Save to Cloud ---
    if not all_results:
        print("--- No Exhaustion Reversal signals were generated. ---")
        return

    final_df = pd.DataFrame(all_results)
    final_df.sort_values(by=["Setup Valid?", "Ticker"], ascending=[False, True], inplace=True)
    
    save_df_to_s3(final_df, 'data/signals/exhaustion_signals.csv')
    print("--- Exhaustion Reversal Screener finished. Results saved to cloud. ---")

if __name__ == "__main__":
    run_exhaustion_screener()
