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
    calculate_vwap,
    format_to_two_decimal
)

# --- Screener-Specific Configuration ---
BB_PERIOD = 20
BB_STD_DEV = 2.0
VOLUME_SPIKE_THRESHOLD_PCT = 115  # 115%
BODY_THRESHOLD_PCT = 50
NEAR_BREAKOUT_PCT = 0.02 # 2% proximity

def run_breakout_screener():
    """Main function to execute the cloud-aware Daily Breakout screening logic."""
    print("\n--- Running Daily Breakout & Breakdown Screener (Cloud-Aware) ---")

    # --- 1. Load Prerequisite Data from Cloud Storage ---
    anchor_df = read_df_from_s3("data/avwap_anchors.csv")
    if anchor_df.empty:
        print("WARNING: Anchor file not found in cloud storage. AVWAP checks will be skipped.")
    else:
        anchor_df = anchor_df.set_index('Ticker')

    tickers = read_tickerlist_from_s3('tickerlist.txt')
    if not tickers:
        print("Ticker list from cloud is empty. Exiting.")
        return
        
    all_results = []

    for ticker in tickers:
        try:
            # --- 2. Load Data and Calculate Indicators ---
            df = read_df_from_s3(f"data/daily/{ticker}_daily.csv")
            if len(df) < BB_PERIOD + 1:
                continue

            df['timestamp'] = pd.to_datetime(df['timestamp'])
            df = df.set_index('timestamp')

            df['ema20'] = df['close'].ewm(span=BB_PERIOD, adjust=False).mean()
            df['std_dev'] = df['close'].rolling(window=BB_PERIOD).std()
            df['bb_upper'] = df['ema20'] + (df['std_dev'] * BB_STD_DEV)
            df['bb_lower'] = df['ema20'] - (df['std_dev'] * BB_STD_DEV)
            df['avg_vol_20d'] = df['volume'].shift(1).rolling(window=20).mean()
            df['volume_vs_avg_%'] = (df['volume'] / df['avg_vol_20d']) * 100

            # --- 3. Check AVWAP Reclaim Status ---
            avwap_reclaimed = "N/A"
            if not anchor_df.empty and ticker in anchor_df.index:
                anchor_date_str = anchor_df.loc[ticker, 'Anchor 1 Date']
                if pd.notna(anchor_date_str) and anchor_date_str != "N/A":
                    anchor_date = pd.to_datetime(anchor_date_str)
                    avwap_df = df[df.index >= anchor_date].copy()
                    if not avwap_df.empty:
                        avwap_df['avwap'] = calculate_vwap(avwap_df)
                        df = df.join(avwap_df['avwap'])
                        if 'avwap' in df.columns and pd.notna(df.iloc[-1]['avwap']):
                            avwap_reclaimed = "Yes" if df.iloc[-1]['close'] > df.iloc[-1]['avwap'] else "No"
            
            # --- 4. Core Breakout Logic ---
            latest = df.iloc[-1]
            direction = "None"
            
            if latest['close'] > latest['ema20'] and latest['close'] > latest['bb_upper']:
                direction = "Long"
            elif latest['close'] < latest['ema20'] and latest['close'] < latest['bb_lower']:
                direction = "Short"

            # --- 5. Validation Conditions ---
            volume_ok = latest.get('volume_vs_avg_%', 0) >= VOLUME_SPIKE_THRESHOLD_PCT
            
            breakout_from_base = False
            if len(df) >= 6:
                if direction == "Long":
                    breakout_from_base = latest['close'] > df['high'].iloc[-6:-1].max()
                elif direction == "Short":
                    breakout_from_base = latest['close'] < df['low'].iloc[-6:-1].min()

            setup_valid = False
            if direction == "Long":
                setup_valid = all([volume_ok, breakout_from_base, avwap_reclaimed == "Yes"])
            elif direction == "Short":
                setup_valid = all([volume_ok, breakout_from_base, avwap_reclaimed == "No"])

            # --- 6. Populate Results ---
            result = {
                "Date": latest.name.strftime('%Y-%m-%d'), 
                "Ticker": ticker,
                "Direction": direction,
                "Close": format_to_two_decimal(latest['close']),
                "Volume vs Avg %": format_to_two_decimal(latest.get('volume_vs_avg_%', 0)),
                "AVWAP Reclaimed?": avwap_reclaimed,
                "Breakout from Base?": "Yes" if breakout_from_base else "No",
                "Setup Valid?": "TRUE" if setup_valid else "FALSE"
            }
            all_results.append(result)

        except Exception as e:
            print(f"   - ERROR processing {ticker} for Breakout: {e}")

    # --- 7. Final Processing & Save to Cloud ---
    if not all_results:
        print("--- No Breakout signals were generated. ---")
        return

    final_df = pd.DataFrame(all_results)
    final_df.sort_values(by=["Setup Valid?", "Ticker"], ascending=[False, True], inplace=True)
    
    save_df_to_s3(final_df, 'data/signals/breakout_signals.csv')
    print("--- Daily Breakout Screener finished. Results saved to cloud. ---")

if __name__ == "__main__":
    run_breakout_screener()
