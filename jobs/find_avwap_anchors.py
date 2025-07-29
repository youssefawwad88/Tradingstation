"""
AVWAP Anchor Finder

This job analyzes daily data to identify significant "power candles" that will
serve as anchor points for the AVWAP screener. It calculates detailed metrics
for each potential anchor and saves them to a CSV file.
"""

import pandas as pd
import sys
import os
import numpy as np

# --- System Path Setup ---
PROJECT_ROOT = '/content/drive/MyDrive/trading-system'
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from utils import config, helpers

# --- Anchor Finding Configuration from Blueprint ---
LOOKBACK_DAYS = 60
MAX_ANCHORS = 2
ANCHOR_GAP_UP_THRESHOLD = 1.5
ANCHOR_GAP_DOWN_THRESHOLD = -2.0
ANCHOR_BODY_THRESHOLD = 50
ANCHOR_VOLUME_RATIO = 1.5  # 150%

def run_anchor_finder():
    """Main function to find and save detailed AVWAP anchor data."""
    print("\n--- Running AVWAP Anchor Finder Job ---")
    
    tickers = config.MASTER_TICKER_LIST
    all_anchor_rows = []

    for ticker in tickers:
        try:
            daily_file = config.DAILY_DIR / f"{ticker}_daily.csv"
            if not daily_file.exists():
                continue
            
            daily_df = pd.read_csv(daily_file, index_col=0, parse_dates=True)
            if len(daily_df) < LOOKBACK_DAYS:
                continue

            # --- 1. Calculate Metrics for Anchor Identification ---
            df = daily_df.tail(LOOKBACK_DAYS).copy()
            df['Prev_Close'] = df[config.CLOSE_COL].shift(1)
            df['Gap_%'] = ((df[config.OPEN_COL] - df['Prev_Close']) / df['Prev_Close']) * 100
            
            candle_range = df[config.HIGH_COL] - df[config.LOW_COL]
            candle_body = abs(df[config.CLOSE_COL] - df[config.OPEN_COL])
            df['Body_%_of_Range'] = np.where(candle_range > 0, (candle_body / candle_range) * 100, 0)
            
            df['Avg_Vol_20D'] = df[config.VOLUME_COL].shift(1).rolling(window=20).mean()
            df['Volume_vs_Avg_%'] = (df[config.VOLUME_COL] / df['Avg_Vol_20D']) * 100

            # --- 2. Identify All Potential Anchor Candles ---
            is_gap_valid = (df['Gap_%'] >= ANCHOR_GAP_UP_THRESHOLD) | (df['Gap_%'] <= ANCHOR_GAP_DOWN_THRESHOLD)
            is_body_valid = df['Body_%_of_Range'] >= ANCHOR_BODY_THRESHOLD
            is_volume_valid = df['Volume_vs_Avg_%'] >= (ANCHOR_VOLUME_RATIO * 100)
            
            confirmed_anchors = df[is_gap_valid & is_body_valid & is_volume_valid]
            confirmed_anchors = confirmed_anchors.sort_index(ascending=False)
            
            # --- 3. Build the Output Row ---
            output_row = {"Ticker": ticker}
            
            for i in range(1, MAX_ANCHORS + 1):
                anchor_prefix = f"Anchor {i}"
                if i - 1 < len(confirmed_anchors):
                    anchor_data = confirmed_anchors.iloc[i-1]
                    output_row[f"{anchor_prefix} Date"] = anchor_data.name.strftime('%Y-%m-%d')
                    output_row[f"{anchor_prefix} Gap %"] = helpers.format_to_two_decimal(anchor_data['Gap_%'])
                    output_row[f"{anchor_prefix} Body %"] = helpers.format_to_two_decimal(anchor_data['Body_%_of_Range'])
                    output_row[f"{anchor_prefix} Vol %"] = helpers.format_to_two_decimal(anchor_data['Volume_vs_Avg_%'])
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

    # --- 4. Save Anchors to File ---
    if not all_anchor_rows:
        print("--- No anchors found for any tickers. ---")
        return
        
    final_df = pd.DataFrame(all_anchor_rows)
    # Define the final column order as per the blueprint
    final_columns = [
        "Ticker", "Anchor 1 Date", "Manual Override", "Anchor 1 Gap %", "Anchor 1 Body %", "Anchor 1 Vol %", "Anchor 1 Confirmed?",
        "Anchor 2 Date", "Anchor 2 Gap %", "Anchor 2 Body %", "Anchor 2 Vol %", "Anchor 2 Confirmed?", "Data Status"
    ]
    # Reorder the dataframe and fill any potentially missing columns
    final_df = final_df.reindex(columns=final_columns, fill_value="N/A")

    output_path = config.DATA_DIR / "avwap_anchors.csv"
    final_df.to_csv(output_path, index=False)
    print(f"Successfully saved {len(final_df)} detailed anchor sets to {output_path}")

if __name__ == "__main__":
    run_anchor_finder()
