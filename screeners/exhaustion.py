"""
Exhaustion Reversal Screener (Advanced)

This screener identifies potential bullish reversal setups on the daily chart,
enhanced with intraday (30-min) confirmation signals like Hammers, Bullish
Engulfing patterns, and AVWAP reclaims for higher conviction.
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
MIN_DROP_PCT = -7.0
MIN_GAP_DOWN_PCT = -1.5
VOLUME_SPIKE_RATIO = 1.3 # 130%
MIN_BODY_PCT = 65.0
MIN_RED_DAYS_IN_PREVIOUS_5 = 3
AVWAP_CONFLUENCE_PROXIMITY_PCT = 1.0

def get_intraday_reversal_signal(intraday_df: pd.DataFrame, avwap_value: float) -> str:
    """Checks the last few 30-min candles for reversal patterns."""
    if len(intraday_df) < 2:
        return "N/A"
    
    last_candle = intraday_df.iloc[-1]
    prev_candle = intraday_df.iloc[-2]

    # AVWAP Reclaim Check
    if avwap_value and last_candle[config.CLOSE_COL] > avwap_value:
        return "AVWAP Reclaim"

    # Hammer Check
    body = abs(last_candle[config.CLOSE_COL] - last_candle[config.OPEN_COL])
    lower_wick = min(last_candle[config.OPEN_COL], last_candle[config.CLOSE_COL]) - last_candle[config.LOW_COL]
    if body > 0 and lower_wick >= (2 * body):
        return "Hammer"

    # Bullish Engulfing Check
    is_prev_red = prev_candle[config.CLOSE_COL] < prev_candle[config.OPEN_COL]
    is_last_green = last_candle[config.CLOSE_COL] > last_candle[config.OPEN_COL]
    if is_prev_red and is_last_green and \
       last_candle[config.CLOSE_COL] > prev_candle[config.OPEN_COL] and \
       last_candle[config.OPEN_COL] < prev_candle[config.CLOSE_COL]:
        return "Engulfing"
        
    return "None"

def run_exhaustion_screener():
    """Main function to execute the Exhaustion Reversal screening logic."""
    print("\n--- Running Exhaustion Reversal Screener (Advanced) ---")
    
    anchor_file = config.DATA_DIR / "avwap_anchors.csv"
    if not anchor_file.exists():
        print(f"WARNING: Anchor file not found. 'AVWAP Confluence' and 'Intraday Reversal' checks will be limited.")
        anchor_df = pd.DataFrame()
    else:
        anchor_df = pd.read_csv(anchor_file).set_index('Ticker')

    tickers = config.MASTER_TICKER_LIST
    all_results = []

    for ticker in tickers:
        try:
            daily_file = config.DAILY_DIR / f"{ticker}_daily.csv"
            intraday_file = config.INTRADAY_30MIN_DIR / f"{ticker}_30min.csv"
            if not daily_file.exists() or not intraday_file.exists(): continue
            
            df = pd.read_csv(daily_file, index_col=0, parse_dates=True)
            intraday_df = pd.read_csv(intraday_file, index_col=0, parse_dates=True)
            if len(df) < 21: continue

            # --- 1. Calculate Daily Indicators ---
            df['EMA50'] = df[config.CLOSE_COL].ewm(span=50, adjust=False).mean()
            
            latest = df.iloc[-1]
            previous = df.iloc[-2]
            last_5_candles = df.iloc[-6:-1]

            # --- 2. Evaluate Core Daily Conditions ---
            highest_close_in_5d = last_5_candles[config.CLOSE_COL].max()
            drop_percent = ((latest[config.CLOSE_COL] - highest_close_in_5d) / highest_close_in_5d) * 100
            is_significant_drop = drop_percent <= MIN_DROP_PCT

            gap_percent = ((latest[config.OPEN_COL] - previous[config.CLOSE_COL]) / previous[config.CLOSE_COL]) * 100
            is_gap_down = gap_percent <= MIN_GAP_DOWN_PCT

            avg_20d_volume = df[config.VOLUME_COL].iloc[-21:-1].mean()
            is_volume_spike = latest[config.VOLUME_COL] >= (avg_20d_volume * VOLUME_SPIKE_RATIO)
            
            is_reclaimed = latest[config.CLOSE_COL] > previous[config.LOW_COL]
            
            candle_range = latest[config.HIGH_COL] - latest[config.LOW_COL]
            body_pct = (abs(latest[config.CLOSE_COL] - latest[config.OPEN_COL]) / candle_range * 100) if candle_range > 0 else 0
            is_strong_green_candle = latest[config.CLOSE_COL] > latest[config.OPEN_COL] and body_pct >= MIN_BODY_PCT

            red_days_count = (last_5_candles[config.CLOSE_COL] < last_5_candles[config.OPEN_COL]).sum()
            has_prior_weakness = red_days_count >= MIN_RED_DAYS_IN_PREVIOUS_5

            # --- 3. Intraday & AVWAP Analysis ---
            intraday_reversal_signal = "N/A"
            avwap_confluence = "No"
            if not anchor_df.empty and ticker in anchor_df.index:
                anchor_date_str = anchor_df.loc[ticker, 'Anchor 1 Date']
                if pd.notna(anchor_date_str) and anchor_date_str != "N/A":
                    anchor_date = pd.to_datetime(anchor_date_str)
                    avwap_calc_df = df[df.index >= anchor_date].copy()
                    if not avwap_calc_df.empty:
                        avwap_calc_df['AVWAP'] = helpers.calculate_vwap(avwap_calc_df)
                        latest_avwap = avwap_calc_df['AVWAP'].iloc[-1]
                        
                        # Check for intraday reversal using this AVWAP
                        today_intraday_df = intraday_df[intraday_df.index.date == latest.name.date()]
                        intraday_reversal_signal = get_intraday_reversal_signal(today_intraday_df, latest_avwap)
                        
                        # Check for daily AVWAP confluence
                        if abs(latest[config.CLOSE_COL] - latest_avwap) / latest_avwap * 100 <= AVWAP_CONFLUENCE_PROXIMITY_PCT:
                            avwap_confluence = "Yes"

            # --- 4. Final Validation ---
            conditions = { "Drop %": is_significant_drop, "Gap %": is_gap_down, "Volume": is_volume_spike, "Reclaim": is_reclaimed, "Candle": is_strong_green_candle, "Prior Weakness": has_prior_weakness }
            setup_valid = all(conditions.values())
            validation_failures = [k for k, v in conditions.items() if not v]

            # --- 5. Populate Result Dictionary ---
            result = {
                "Date": latest.name.strftime('%Y-%m-%d'), "Ticker": ticker,
                "Close": helpers.format_to_two_decimal(latest[config.CLOSE_COL]),
                "Drop %": helpers.format_to_two_decimal(drop_percent),
                "Gap %": helpers.format_to_two_decimal(gap_percent),
                "Volume Spike?": "Yes" if is_volume_spike else "No",
                "Reclaimed?": "Yes" if is_reclaimed else "No",
                "Strong Green Candle?": "Yes" if is_strong_green_candle else "No",
                "5-Day High Close": helpers.format_to_two_decimal(highest_close_in_5d),
                "Lowest 5D Close": helpers.format_to_two_decimal(last_5_candles[config.CLOSE_COL].min()),
                "20-Day Volume Avg": f"{avg_20d_volume:,.0f}",
                "Strong Red Days?": "Yes" if has_prior_weakness else "No",
                "Intraday Reversal Signal?": intraday_reversal_signal,
                "Setup Valid?": "TRUE" if setup_valid else "FALSE",
                "Watch Note": "Daily setup confirmed." if setup_valid else "Failed: " + ", ".join(validation_failures),
                "Body % of Candle": helpers.format_to_two_decimal(body_pct),
                "Inside Candle?": "Yes" if latest[config.HIGH_COL] < previous[config.HIGH_COL] and latest[config.LOW_COL] > previous[config.LOW_COL] else "No",
                "EMA Distance %": helpers.format_to_two_decimal(((latest[config.CLOSE_COL] - latest['EMA50']) / latest['EMA50']) * 100),
                "AVWAP Confluence?": avwap_confluence
            }
            all_results.append(result)

        except Exception as e:
            print(f"   - ERROR processing {ticker} for Exhaustion Reversal: {e}")

    # --- 6. Save ALL Results ---
    if not all_results:
        print("--- No tickers were processed for Exhaustion Reversal. ---")
        return
        
    final_df = pd.DataFrame(all_results)
    final_df.sort_values(by=["Setup Valid?", "Ticker"], ascending=[False, True], inplace=True)
    helpers.save_signal_to_csv(final_df, 'exhaustion')

if __name__ == "__main__":
    run_exhaustion_screener()
