"""
EMA Trend Pullback Screener

This screener identifies daily timeframe opportunities where a stock, in an
established trend (defined by the 50 EMA), pulls back to a key EMA (8 or 21)
and then shows a strong confirmation candle, signaling a potential trend continuation.
"""

import pandas as pd
import sys
import os
from datetime import datetime
import pytz
import numpy as np

# --- System Path Setup ---
PROJECT_ROOT = '/content/drive/MyDrive/trading-system'
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from utils import config, helpers

# --- Screener-Specific Configuration ---
EMA_LONG_PERIOD = 50
EMA_MEDIUM_PERIOD = 21
EMA_SHORT_PERIOD = 8
VOLUME_SPIKE_THRESHOLD_PCT = 115
BREAKOUT_POTENTIAL_LOOKBACK = 10
BREAKOUT_POTENTIAL_PROXIMITY_PCT = 1.5
AVWAP_CONFLUENCE_PROXIMITY_PCT = 1.0

def run_ema_pullback_screener():
    """Main function to execute the EMA Trend Pullback screening logic."""
    print("\n--- Running EMA Trend Pullback Screener (with advanced columns) ---")
    
    # --- 1. Load Prerequisite Data ---
    anchor_file = config.DATA_DIR / "avwap_anchors.csv"
    if not anchor_file.exists():
        print(f"WARNING: Anchor file not found at {anchor_file}. 'AVWAP Confluence?' will be N/A.")
        anchor_df = pd.DataFrame() # Create empty df to avoid errors
    else:
        anchor_df = pd.read_csv(anchor_file).set_index('Ticker')
        
    tickers = config.MASTER_TICKER_LIST
    all_results = []

    for ticker in tickers:
        try:
            daily_file = config.DAILY_DIR / f"{ticker}_daily.csv"
            if not daily_file.exists(): continue
            
            df = pd.read_csv(daily_file, index_col=0, parse_dates=True)
            if len(df) < EMA_LONG_PERIOD + 1: continue

            # --- 2. Calculate Indicators ---
            df['EMA50'] = df[config.CLOSE_COL].ewm(span=EMA_LONG_PERIOD, adjust=False).mean()
            df['EMA21'] = df[config.CLOSE_COL].ewm(span=EMA_MEDIUM_PERIOD, adjust=False).mean()
            df['EMA8'] = df[config.CLOSE_COL].ewm(span=EMA_SHORT_PERIOD, adjust=False).mean()
            df['Avg_Vol_20D'] = df[config.VOLUME_COL].shift(1).rolling(window=20).mean()
            df['Volume_vs_Avg_%'] = (df[config.VOLUME_COL] / df['Avg_Vol_20D']) * 100

            # --- 3. Get Latest & Previous Candles for Analysis ---
            latest = df.iloc[-1]
            previous = df.iloc[-2]
            
            # --- 4. Evaluate Trend and Candle Properties ---
            trend_vs_ema50 = "Above" if latest[config.CLOSE_COL] > latest['EMA50'] else "Below"
            candle_range = latest[config.HIGH_COL] - latest[config.LOW_COL]
            candle_body = abs(latest[config.CLOSE_COL] - latest[config.OPEN_COL])
            body_pct_of_candle = (candle_body / candle_range * 100) if candle_range > 0 else 0
            
            # --- 5. Evaluate Pullback Conditions ---
            direction, wick_confirmed, ema_reclaim_reject, signal_candle = "None", "No", "N/A", "No"
            
            if candle_range > 0 and (candle_range - candle_body) > (2 * candle_body): wick_confirmed = "Yes"
            if 20 <= body_pct_of_candle <= 60: signal_candle = "Yes"

            if trend_vs_ema50 == "Above":
                if latest[config.OPEN_COL] < latest['EMA8'] and latest[config.CLOSE_COL] > latest['EMA8']:
                    ema_reclaim_reject = "Reclaim EMA8"
                    direction = "Long"
            elif trend_vs_ema50 == "Below":
                if latest[config.OPEN_COL] > latest['EMA21'] and latest[config.CLOSE_COL] < latest['EMA21']:
                    ema_reclaim_reject = "Reject EMA21"
                    direction = "Short"

            # --- 6. Final Validation ---
            is_volume_spike = latest['Volume_vs_Avg_%'] >= VOLUME_SPIKE_THRESHOLD_PCT
            conditions = { "Wick not confirmed": wick_confirmed == "Yes", "No EMA Reclaim/Reject": ema_reclaim_reject != "N/A", "Not a Signal Candle": signal_candle == "Yes", "Low volume": is_volume_spike, "Incorrect Trend": direction != "None" }
            setup_valid = all(conditions.values())
            validation_failures = [k for k, v in conditions.items() if not v]

            # --- 7. Calculate NEW Optional Columns ---
            is_inside_candle = "Yes" if latest[config.HIGH_COL] < previous[config.HIGH_COL] and latest[config.LOW_COL] > previous[config.LOW_COL] else "No"
            
            ema_distance_pct = np.nan
            if direction == "Long": ema_distance_pct = ((latest[config.CLOSE_COL] - latest['EMA8']) / latest['EMA8']) * 100
            elif direction == "Short": ema_distance_pct = ((latest['EMA21'] - latest[config.CLOSE_COL]) / latest['EMA21']) * 100

            avwap_confluence = "N/A"
            if not anchor_df.empty and ticker in anchor_df.index:
                anchor_date_str = anchor_df.loc[ticker, 'Anchor 1 Date']
                if pd.notna(anchor_date_str) and anchor_date_str != "N/A":
                    anchor_date = pd.to_datetime(anchor_date_str)
                    avwap_df = df[df.index >= anchor_date].copy()
                    if not avwap_df.empty:
                        avwap_df['AVWAP'] = helpers.calculate_vwap(avwap_df)
                        latest_avwap = avwap_df['AVWAP'].iloc[-1]
                        if abs(latest[config.CLOSE_COL] - latest_avwap) / latest_avwap * 100 <= AVWAP_CONFLUENCE_PROXIMITY_PCT:
                            avwap_confluence = "Yes"

            # --- 8. Populate Full Result Dictionary ---
            result = {
                "Date": latest.name.strftime('%Y-%m-%d'), "Ticker": ticker, "Direction": direction, "Trend vs EMA50": trend_vs_ema50,
                "EMA50": helpers.format_to_two_decimal(latest['EMA50']), "EMA21": helpers.format_to_two_decimal(latest['EMA21']), "EMA8": helpers.format_to_two_decimal(latest['EMA8']),
                "Close Price": helpers.format_to_two_decimal(latest[config.CLOSE_COL]), "Wick Confirmed?": wick_confirmed, "EMA Reclaim/Reject": ema_reclaim_reject,
                "Signal Candle?": signal_candle, "Volume": f"{latest[config.VOLUME_COL]:,.0f}", "Avg 20D Volume": f"{latest['Avg_Vol_20D']:,.0f}" if pd.notna(latest['Avg_Vol_20D']) else "N/A",
                "Volume vs Avg %": helpers.format_to_two_decimal(latest['Volume_vs_Avg_%']), "Volume Spike?": "Yes" if is_volume_spike else "No",
                "Signal Direction": direction if setup_valid else "None", "Reason / Notes": ", ".join(validation_failures) if validation_failures else "N/A",
                "Setup Valid?": "TRUE" if setup_valid else "FALSE",
                # New Columns
                "Body % of Candle": helpers.format_to_two_decimal(body_pct_of_candle),
                "Inside Candle?": is_inside_candle,
                "EMA Distance %": helpers.format_to_two_decimal(ema_distance_pct),
                "AVWAP Confluence?": avwap_confluence
            }
            all_results.append(result)

        except Exception as e:
            print(f"   - ERROR processing {ticker} for EMA Pullback: {e}")

    # --- 9. Save ALL Results ---
    if not all_results:
        print("--- No tickers were processed. ---")
        return
        
    final_df = pd.DataFrame(all_results)
    
    final_columns = [
        "Date", "Ticker", "Direction", "Trend vs EMA50", "EMA50", "EMA21", "EMA8", "Close Price",
        "Wick Confirmed?", "EMA Reclaim/Reject", "Signal Candle?", "Volume", "Avg 20D Volume",
        "Volume vs Avg %", "Volume Spike?", "Signal Direction", "Reason / Notes", "Setup Valid?",
        "Body % of Candle", "Inside Candle?", "EMA Distance %", "AVWAP Confluence?"
    ]
    
    final_df = final_df.reindex(columns=final_columns).fillna("N/A")
    final_df.sort_values(by=["Setup Valid?", "Ticker"], ascending=[False, True], inplace=True)
    helpers.save_signal_to_csv(final_df, 'ema_pullback')

if __name__ == "__main__":
    run_ema_pullback_screener()
