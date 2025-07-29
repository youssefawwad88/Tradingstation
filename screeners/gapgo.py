"""
Gap & Go Screener (Umar Ashraf Style) - Execution-Grade Logic

This script implements the full, corrected logic for identifying high-probability
Gap & Go intraday breakout opportunities for both LONG and SHORT directions.
It is fully integrated with the project's config and helper utilities and
includes all detailed analysis columns.
"""

import pandas as pd
import sys
import os
from datetime import datetime, time
import pytz
import numpy as np

# --- System Path Setup ---
PROJECT_ROOT = '/content/drive/MyDrive/trading-system'
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from utils import config, helpers

def run_gapgo_screener():
    """Main function to execute the Gap & Go screening logic."""
    print("\n--- Running Gap & Go Screener (Execution-Grade) ---")
    
    session = helpers.detect_market_session()
    print(f"Current Market Session Detected: {session}")
    
    if session == 'CLOSED':
        print("--- Market is closed. Skipping screener. ---")
        return

    tickers = config.MASTER_TICKER_LIST
    all_results = []
    ny_timezone = pytz.timezone('America/New_York')
    ny_time = datetime.now(ny_timezone)
    
    breakout_valid_time = time(9, 36)

    for ticker in tickers:
        try:
            # --- 1. Load Data ---
            daily_file = config.DAILY_DIR / f"{ticker}_daily.csv"
            intraday_1min_file = config.INTRADAY_1MIN_DIR / f"{ticker}_1min.csv"
            
            if not daily_file.exists() or not intraday_1min_file.exists():
                continue

            daily_df = pd.read_csv(daily_file, index_col=0, parse_dates=True)
            intraday_df = pd.read_csv(intraday_1min_file, index_col=0, parse_dates=True)
            today_intraday_df = intraday_df[intraday_df.index.date == ny_time.date()].copy()
            
            if today_intraday_df.empty:
                continue

            # --- 2. Calculate Base Metrics ---
            last_price = today_intraday_df[config.CLOSE_COL].iloc[-1]
            prev_close = helpers.get_previous_day_close(daily_df)
            premarket_df = helpers.get_premarket_data(today_intraday_df)
            premarket_high = premarket_df[config.HIGH_COL].max() if not premarket_df.empty else np.nan
            premarket_low = premarket_df[config.LOW_COL].min() if not premarket_df.empty else np.nan
            premarket_volume = premarket_df[config.VOLUME_COL].sum() if not premarket_df.empty else 0
            
            # --- 3. Session-Aware Calculations ---
            official_gap_percent, open_price_930, last_vwap = np.nan, np.nan, np.nan
            is_live_spike, today_early_volume = False, 0
            opening_range_pct, breakout_body_pct, breakout_vol_spike_pct = np.nan, np.nan, np.nan
            
            historical_intraday_df = intraday_df[intraday_df.index.date < ny_time.date()]
            avg_early_volume_5d = helpers.calculate_avg_early_volume(historical_intraday_df, days=5)
            avg_early_vol_complete = "Yes" if avg_early_volume_5d is not None and avg_early_volume_5d > 0 else "No"
            
            if session == 'REGULAR':
                opening_candle = today_intraday_df.loc[today_intraday_df.index.time == config.REGULAR_SESSION_START]
                if not opening_candle.empty and prev_close:
                    open_price_930 = opening_candle.iloc[0][config.OPEN_COL]
                    official_gap_percent = ((open_price_930 - prev_close) / prev_close) * 100

                if avg_early_volume_5d is not None and avg_early_volume_5d > 0:
                    today_early_volume = helpers.calculate_avg_early_volume(today_intraday_df, days=1)
                    is_live_spike = today_early_volume >= (avg_early_volume_5d * 1.15)

                regular_session_df = today_intraday_df.between_time(config.REGULAR_SESSION_START, config.REGULAR_SESSION_END)
                if not regular_session_df.empty:
                    regular_session_df['VWAP'] = helpers.calculate_vwap(regular_session_df)
                    last_vwap = regular_session_df['VWAP'].iloc[-1]
            
            live_gap_percent_pm = ((last_price - prev_close) / prev_close) * 100 if prev_close else np.nan

            # --- 4. Determine Direction & Gap Label ---
            direction, gap_label = "Flat", "Flat"
            gap_to_check = official_gap_percent if session == 'REGULAR' else live_gap_percent_pm
            if gap_to_check >= 1.5: direction = "Long"
            elif gap_to_check <= -1.5: direction = "Short"
            if gap_to_check >= 4.0: gap_label = "Huge Gap Up"
            elif gap_to_check >= 1.5: gap_label = "Large Gap Up"
            elif gap_to_check <= -4.0: gap_label = "Huge Gap Down"
            elif gap_to_check <= -1.5: gap_label = "Large Gap Down"

            # --- 5. Evaluate Core Conditions ---
            avg_daily_vol_10d = helpers.calculate_avg_daily_volume(daily_df, 10)
            is_pre_vol_spike = (avg_daily_vol_10d is not None and avg_daily_vol_10d > 0 and premarket_volume >= (avg_daily_vol_10d * 0.10))
            
            # Long conditions
            gap_valid_long = official_gap_percent >= 1.5
            vwap_reclaimed = last_price > last_vwap
            breakout_above = last_price > premarket_high
            breakout_candles = today_intraday_df[today_intraday_df[config.CLOSE_COL] > premarket_high]
            first_breakout_time = breakout_candles.index.min() if not breakout_candles.empty else None
            time_valid = first_breakout_time is not None and first_breakout_time.time() >= breakout_valid_time

            # Short conditions
            gap_valid_short = official_gap_percent <= -1.5
            vwap_rejected = last_price < last_vwap
            breakdown_below = last_price < premarket_low
            breakdown_candles = today_intraday_df[today_intraday_df[config.CLOSE_COL] < premarket_low]
            first_breakdown_time = breakdown_candles.index.min() if not breakdown_candles.empty else None
            time_valid_short = first_breakdown_time is not None and first_breakdown_time.time() >= breakout_valid_time

            # --- 6. Final Validation and Scoring ---
            setup_valid, setup_score = False, 0
            if direction == "Long":
                conditions = [gap_valid_long, vwap_reclaimed, breakout_above, is_live_spike, time_valid]
                setup_score = sum(conditions) * 20
                setup_valid = all(conditions)
            elif direction == "Short":
                conditions = [gap_valid_short, vwap_rejected, breakdown_below, is_live_spike, time_valid_short]
                setup_score = sum(conditions) * 20
                setup_valid = all(conditions)
            
            # --- 7. Calculate Quality Metrics if Setup is Valid ---
            if setup_valid:
                orb_5min_candles = today_intraday_df.between_time(config.REGULAR_SESSION_START, time(9, 34))
                if not orb_5min_candles.empty:
                    orb_high = orb_5min_candles[config.HIGH_COL].max()
                    orb_low = orb_5min_candles[config.LOW_COL].min()
                    if open_price_930 > 0:
                        opening_range_pct = ((orb_high - orb_low) / open_price_930) * 100
                    
                    avg_orb_vol = orb_5min_candles[config.VOLUME_COL].mean()
                    
                    breakout_candle = today_intraday_df.loc[first_breakout_time if direction == "Long" else first_breakdown_time]
                    bo_range = breakout_candle[config.HIGH_COL] - breakout_candle[config.LOW_COL]
                    if bo_range > 0:
                        breakout_body_pct = (abs(breakout_candle[config.CLOSE_COL] - breakout_candle[config.OPEN_COL]) / bo_range) * 100
                    if avg_orb_vol > 0:
                        breakout_vol_spike_pct = (breakout_candle[config.VOLUME_COL] / avg_orb_vol) * 100

            # --- 8. Determine Status ---
            status = "Flat"
            if setup_valid:
                status = "Entry"
            elif direction != "Flat" and is_pre_vol_spike:
                status = "Watch"
            
            # --- 9. Populate Full Result Dictionary ---
            result = {
                "Date": ny_time.strftime('%Y-%m-%d'), "US Time": ny_time.strftime('%H:%M:%S'),
                "Ticker": ticker, "Direction": direction, "Status": status,
                "Last Price": helpers.format_to_two_decimal(last_price),
                "Gap %": helpers.format_to_two_decimal(official_gap_percent if session == 'REGULAR' else live_gap_percent_pm),
                "Gap Label": gap_label,
                "Pre-Mkt High": helpers.format_to_two_decimal(premarket_high),
                "Pre-Mkt Low": helpers.format_to_two_decimal(premarket_low),
                "Open Price (9:30 AM)": helpers.format_to_two_decimal(open_price_930),
                "Prev Close": helpers.format_to_two_decimal(prev_close),
                "Pre-Mkt Volume": f"{premarket_volume:,.0f}",
                "Avg Early Volume (15min)": f"{avg_early_volume_5d:,.0f}" if avg_early_volume_5d else "N/A",
                "Avg Early Vol Complete?": avg_early_vol_complete,
                "Today Early Volume (9:30–9:44)": f"{today_early_volume:,.0f}" if session == 'REGULAR' else "N/A",
                "Pre Volume Spike?": "Yes" if is_pre_vol_spike else "No",
                "Live Volume Spike?": "Yes" if is_live_spike else "No" if session == 'REGULAR' else "N/A",
                "VWAP Reclaimed?": "Yes" if vwap_reclaimed else "No" if session == 'REGULAR' else "N/A",
                "Breakout Above Pre High?": "TRUE" if breakout_above else "FALSE",
                "Breakdown Below Pre Low?": "TRUE" if breakdown_below else "FALSE",
                "Setup Valid?": "TRUE" if setup_valid else "FALSE",
                "Avg Daily Vol (10-Day)": f"{avg_daily_vol_10d:,.0f}" if avg_daily_vol_10d else "N/A",
                "Setup Score %": setup_score,
                "Opening Range %": helpers.format_to_two_decimal(opening_range_pct),
                "Breakout Candle Body %": helpers.format_to_two_decimal(breakout_body_pct),
                "Breakout Volume Spike %": helpers.format_to_two_decimal(breakout_vol_spike_pct)
            }
            all_results.append(result)

        except Exception as e:
            print(f"   - ERROR processing {ticker}: {e}")

    # --- 10. Final Processing & Save ---
    if all_results:
        final_df = pd.DataFrame(all_results)
        final_df.sort_values(by=['Status', 'Setup Score %'], ascending=[True, False], inplace=True)
        helpers.save_signal_to_csv(final_df, 'gapgo')
    else:
        print("--- No tickers were processed. ---")

if __name__ == "__main__":
    run_gapgo_screener()
