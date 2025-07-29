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

# --- Anchor Finding Configuration ---
LOOKBACK_DAYS = 60
MAX_ANCHORS = 2
ANCHOR_GAP_UP_THRESHOLD = 1.5
ANCHOR_GAP_DOWN_THRESHOLD = -2.0
ANCHOR_BODY_THRESHOLD = 50
ANCHOR_VOLUME_RATIO = 1.5  # 150%

def run_anchor_finder():
    """Main function to find and save detailed AVWAP anchor data to cloud storage."""
    print("\n--- Running AVWAP Anchor Finder Job (Cloud-Aware) ---")
    
    tickers = read_tickerlist_from_s3('tickerlist.txt')
    if not tickers:
        print("Ticker list from cloud is empty. Exiting anchor finder.")
        return
        
    all_anchor_rows = []

    for ticker in tickers:
        try:
            # --- 1. Load Data from Cloud Storage ---
            daily_df = read_df_from_s3(f"data/daily/{ticker}_daily.csv")
            
            if len(daily_df) < LOOKBACK_DAYS:
                print(f"Not enough daily data for {ticker} to find anchors. Skipping.")
                continue

            # --- 2. Calculate Metrics for Anchor Identification ---
            df = daily_df.tail(LOOKBACK_DAYS).copy()
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            df = df.set_index('timestamp')

            df['prev_close'] = df['close'].shift(1)
            df['gap_%'] = ((df['open'] - df['prev_close']) / df['prev_close']) * 100
            
            candle_range = df['high'] - df['low']
            candle_body = abs(df['close'] - df['open'])
            df['body_%_of_range'] = np.where(candle_range > 0, (candle_body / candle_range) * 100, 0)
            
            df['avg_vol_20d'] = df['volume'].shift(1).rolling(window=20).mean()
            df['volume_vs_avg_%'] = (df['volume'] / df['avg_vol_20d']) * 100

            # --- 3. Identify All Potential Anchor Candles ---
            is_gap_valid = (df['gap_%'] >= ANCHOR_GAP_UP_THRESHOLD) | (df['gap_%'] <= ANCHOR_GAP_DOWN_THRESHOLD)
            is_body_valid = df['body_%_of_range'] >= ANCHOR_BODY_THRESHOLD
            is_volume_valid = df['volume_vs_avg_%'] >= (ANCHOR_VOLUME_RATIO * 100)
            
            confirmed_anchors = df[is_gap_valid & is_body_valid & is_volume_valid]
            confirmed_anchors = confirmed_anchors.sort_index(ascending=False)
            
            # --- 4. Build the Output Row ---
            output_row = {"Ticker": ticker}
            
            for i in range(1, MAX_ANCHORS + 1):
                anchor_prefix = f"Anchor {i}"
                if i - 1 < len(confirmed_anchors):
                    anchor_data = confirmed_anchors.iloc[i-1]
                    output_row[f"{anchor_prefix} Date"] = anchor_data.name.strftime('%Y-%m-%d')
                    output_row[f"{anchor_prefix} Gap %"] = format_to_two_decimal(anchor_data['gap_%'])
                    output_row[f"{anchor_prefix} Body %"] = format_to_two_decimal(anchor_data['body_%_of_range'])
                    output_row[f"{anchor_prefix} Vol %"] = format_to_two_decimal(anchor_data['volume_vs_avg_%'])
                    output_row[f"{anchor_prefix} Confirmed?"] = "Yes"
                else:
                    # Fill with N/A if no anchor is found for this slot
                    output_row[f"{anchor_prefix} Date"] = "N/A"
                    output_row[f"{anchor_prefix} Gap %"] = "N/A"
                    output_row[f"{anchor_prefix} Body %"] = "N/A"
                    output_row[f"{anchor_prefix} Vol %"] = "N/A"
                    output_row[f"{anchor_prefix} Confirmed?"] = "No"

            output_row["Manual Override"] = "FALSE"
            output_row["Data Status"] = "Auto-Detected"
            
            all_anchor_rows.append(output_row)

        except Exception as e:
            print(f"   - ERROR processing anchors for {ticker}: {e}")

    # --- 5. Save Anchors to Cloud Storage ---
    if not all_anchor_rows:
        print("--- No anchors found for any tickers. ---")
        return
        
    final_df = pd.DataFrame(all_anchor_rows)
    
    save_df_to_s3(final_df, 'data/avwap_anchors.csv')
    print(f"Successfully saved {len(final_df)} anchor sets to cloud storage.")

if __name__ == "__main__":
    run_anchor_finder()
