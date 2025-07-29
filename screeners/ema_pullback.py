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
EMA_LONG_PERIOD = 50
EMA_MEDIUM_PERIOD = 21
EMA_SHORT_PERIOD = 8
VOLUME_SPIKE_THRESHOLD_PCT = 115

def run_ema_pullback_screener():
    """Main function to execute the cloud-aware EMA Trend Pullback screening logic."""
    print("\n--- Running EMA Trend Pullback Screener (Cloud-Aware) ---")
    
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
            if len(df) < EMA_LONG_PERIOD + 1:
                continue

            df['timestamp'] = pd.to_datetime(df['timestamp'])
            df = df.set_index('timestamp')

            df['ema50'] = df['close'].ewm(span=EMA_LONG_PERIOD, adjust=False).mean()
            df['ema21'] = df['close'].ewm(span=EMA_MEDIUM_PERIOD, adjust=False).mean()
            df['ema8'] = df['close'].ewm(span=EMA_SHORT_PERIOD, adjust=False).mean()
            df['avg_vol_20d'] = df['volume'].shift(1).rolling(window=20).mean()
            df['volume_vs_avg_%'] = (df['volume'] / df['avg_vol_20d']) * 100

            # --- 3. Evaluate Pullback Conditions ---
            latest = df.iloc[-1]
            trend_vs_ema50 = "Above" if latest['close'] > latest['ema50'] else "Below"
            
            direction = "None"
            ema_reclaim_reject = "N/A"
            
            # Long condition: In an uptrend, pulls back and reclaims the 8 EMA
            if trend_vs_ema50 == "Above":
                if latest['open'] < latest['ema8'] and latest['close'] > latest['ema8']:
                    ema_reclaim_reject = "Reclaim EMA8"
                    direction = "Long"
            # Short condition: In a downtrend, pulls back and is rejected by the 21 EMA
            elif trend_vs_ema50 == "Below":
                if latest['open'] > latest['ema21'] and latest['close'] < latest['ema21']:
                    ema_reclaim_reject = "Reject EMA21"
                    direction = "Short"

            # --- 4. Final Validation ---
            is_volume_spike = latest.get('volume_vs_avg_%', 0) >= VOLUME_SPIKE_THRESHOLD_PCT
            
            # For the core logic, a valid setup requires a valid direction and a volume spike.
            setup_valid = (direction != "None" and is_volume_spike)

            # --- 5. Populate Core Results ---
            result = {
                "Date": latest.name.strftime('%Y-%m-%d'),
                "Ticker": ticker,
                "Direction": direction,
                "Trend vs EMA50": trend_vs_ema50,
                "Close Price": format_to_two_decimal(latest['close']),
                "EMA Reclaim/Reject": ema_reclaim_reject,
                "Volume Spike?": "Yes" if is_volume_spike else "No",
                "Setup Valid?": "TRUE" if setup_valid else "FALSE"
            }
            all_results.append(result)

        except Exception as e:
            print(f"   - ERROR processing {ticker} for EMA Pullback: {e}")

    # --- 6. Final Processing & Save to Cloud ---
    if not all_results:
        print("--- No EMA Pullback signals were generated. ---")
        return

    final_df = pd.DataFrame(all_results)
    final_df.sort_values(by=["Setup Valid?", "Ticker"], ascending=[False, True], inplace=True)
    
    save_df_to_s3(final_df, 'data/signals/ema_pullback_signals.csv')
    print("--- EMA Pullback Screener finished. Results saved to cloud. ---")

if __name__ == "__main__":
    run_ema_pullback_screener()
