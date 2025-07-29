"""
Daily Breakout & Breakdown Screener (Brian Shannon-Style)

This screener detects daily timeframe breakout and breakdown setups using a
rules-based technical approach. It incorporates Bollinger Bands, volume,
and Anchored VWAP for enhanced conviction on both Long and Short trades.
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
BB_PERIOD = 20
BB_STD_DEV = 2.0
VOLUME_SPIKE_THRESHOLD_PCT = 115  # 115%
BODY_THRESHOLD_PCT = 50  # 50%
NEAR_BREAKOUT_PCT = 0.02  # 2% proximity to breakout level

def calculate_signal_strength(row: pd.Series) -> str:
    """Calculates a 1-5 star rating for the signal strength."""
    score = 0
    if row.get('Volume vs Avg %', 0) >= 200: score += 2
    elif row.get('Volume vs Avg %', 0) >= 115: score += 1

    if row.get('Body % of Candle', 0) >= 60: score += 1

    # AVWAP condition check for scoring
    if row.get('Direction') == 'Long' and row.get('AVWAP Reclaimed?') == 'Yes': score += 1
    if row.get('Direction') == 'Short' and row.get('AVWAP Reclaimed?') == 'No': score += 1

    if row.get('Breakout from Base?') == 'Yes': score += 1

    return "★" * max(1, min(5, score))

def run_breakout_screener():
    print("\n--- Running Daily Breakout & Breakdown Screener (Advanced) ---")

    anchor_file = config.DATA_DIR / "avwap_anchors.csv"
    if not anchor_file.exists():
        print(f"ERROR: Anchor file not found at {anchor_file}. This screener requires it. Please run the find_avwap_anchors.py job first.")
        return
    anchor_df = pd.read_csv(anchor_file).set_index('Ticker')
    tickers = config.MASTER_TICKER_LIST
    all_results = []

    for ticker in tickers:
        try:
            daily_file = config.DAILY_DIR / f"{ticker}_daily.csv"
            if not daily_file.exists(): continue

            df = pd.read_csv(daily_file, index_col=0, parse_dates=True)
            if len(df) < BB_PERIOD + 1: continue

            df['EMA20'] = df[config.CLOSE_COL].ewm(span=BB_PERIOD, adjust=False).mean()
            df['STD_DEV'] = df[config.CLOSE_COL].rolling(window=BB_PERIOD).std()
            df['BB_Upper'] = df['EMA20'] + (df['STD_DEV'] * BB_STD_DEV)
            df['BB_Lower'] = df['EMA20'] - (df['STD_DEV'] * BB_STD_DEV)
            df['Avg_Vol_20D'] = df[config.VOLUME_COL].shift(1).rolling(window=20).mean()
            df['Volume_vs_Avg_%'] = (df[config.VOLUME_COL] / df['Avg_Vol_20D']) * 100

            avwap_reclaimed = "No"
            if ticker in anchor_df.index:
                anchor_date_str = anchor_df.loc[ticker, 'Anchor 1 Date']
                if pd.notna(anchor_date_str) and anchor_date_str != "N/A":
                    anchor_date = pd.to_datetime(anchor_date_str)
                    avwap_df = df[df.index >= anchor_date].copy()
                    if not avwap_df.empty:
                        avwap_df['AVWAP'] = helpers.calculate_vwap(avwap_df)
                        df = df.join(avwap_df['AVWAP'])
                        if 'AVWAP' in df.columns and pd.notna(df.iloc[-1]['AVWAP']):
                            avwap_reclaimed = "Yes" if df.iloc[-1][config.CLOSE_COL] > df.iloc[-1]['AVWAP'] else "No"
                else:
                    print(f"   - INFO: No confirmed anchor date found for {ticker}. AVWAP check will default to 'No'.")
            else:
                print(f"   - INFO: Ticker {ticker} not in anchor file. AVWAP check will default to 'No'.")

            latest = df.iloc[-1]
            direction = "None"
            near_breakout = False
            near_breakdown = False

            if latest[config.CLOSE_COL] > latest['EMA20'] and latest[config.CLOSE_COL] > latest['BB_Upper']:
                direction = "Long"
            elif latest[config.CLOSE_COL] < latest['EMA20'] and latest[config.CLOSE_COL] < latest['BB_Lower']:
                direction = "Short"
            else:
                # Check for near breakout/breakdown
                if latest[config.CLOSE_COL] > latest['BB_Upper'] * (1 - NEAR_BREAKOUT_PCT):
                    near_breakout = True
                if latest[config.CLOSE_COL] < latest['BB_Lower'] * (1 + NEAR_BREAKOUT_PCT):
                    near_breakdown = True

            candle_range = latest[config.HIGH_COL] - latest[config.LOW_COL]
            body_pct_of_candle = (abs(latest[config.CLOSE_COL] - latest[config.OPEN_COL]) / candle_range * 100) if candle_range > 0 else 0

            breakout_from_base = False
            if len(df) >= 6:
                if direction == "Long":
                    breakout_from_base = latest[config.CLOSE_COL] > df[config.HIGH_COL].iloc[-6:-1].max()
                elif direction == "Short":
                    breakout_from_base = latest[config.CLOSE_COL] < df[config.LOW_COL].iloc[-6:-1].min()

            setup_valid = False
            validation_failures = []
            if direction != "None":
                conditions = {
                    "Low Volume": latest.get('Volume_vs_Avg_%', 0) >= VOLUME_SPIKE_THRESHOLD_PCT,
                    "Small candle body": body_pct_of_candle >= BODY_THRESHOLD_PCT,
                    "No breakout from base": breakout_from_base
                }
                if direction == "Long":
                    conditions["Didn't reclaim AVWAP"] = avwap_reclaimed == "Yes"
                else:
                    conditions["Still above AVWAP"] = avwap_reclaimed == "No"

                if all(conditions.values()):
                    setup_valid = True
                else:
                    validation_failures = [key for key, value in conditions.items() if not value]
            else:
                validation_failures.append("Direction is None")

            result = {
                "Date": latest.name.strftime('%Y-%m-%d'), "Ticker": ticker,
                "Close": helpers.format_to_two_decimal(latest[config.CLOSE_COL]),
                "High": helpers.format_to_two_decimal(latest[config.HIGH_COL]),
                "Low": helpers.format_to_two_decimal(latest[config.LOW_COL]),
                "Volume": f"{latest[config.VOLUME_COL]:,.0f}",
                "EMA20": helpers.format_to_two_decimal(latest['EMA20']),
                "BB Upper": helpers.format_to_two_decimal(latest['BB_Upper']),
                "BB Lower": helpers.format_to_two_decimal(latest['BB_Lower']),
                "Volume vs Avg %": latest.get('Volume_vs_Avg_%', 0),
                "AVWAP Reclaimed?": avwap_reclaimed,
                "Direction": direction,
                "Setup Valid?": "TRUE" if setup_valid else "FALSE",
                "Body % of Candle": body_pct_of_candle,
                "Breakout from Base?": "Yes" if breakout_from_base else "No",
                "Why Not Valid?": ", ".join(validation_failures) if validation_failures else "N/A",
                "Near Breakout?": "Yes" if near_breakout else "No",
                "Near Breakdown?": "Yes" if near_breakdown else "No"
            }
            result["Signal Strength (★)"] = calculate_signal_strength(result)
            all_results.append(result)

        except Exception as e:
            print(f"   - ERROR processing {ticker} for Breakout: {e}")

    if not all_results:
        print("--- No tickers were processed. ---")
        return

    final_df = pd.DataFrame(all_results)
    final_df.sort_values(by=["Setup Valid?", "Ticker"], ascending=[False, True], inplace=True)
    helpers.save_signal_to_csv(final_df, 'breakout')

if __name__ == "__main__":
    run_breakout_screener()
