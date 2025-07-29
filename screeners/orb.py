"""
Opening Range Breakout (ORB) Screener - Optimized Logic

This screener identifies breakout opportunities based on the 10-minute opening
range (9:30-9:39 AM EST). It is designed to run after the Gap & Go screener
and leverages its analysis to build a comprehensive setup score.
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

# --- ORB Screener Specific Configuration ---
ORB_WINDOW_END_MINUTE = 39
ORB_TRIGGER_MINUTE = 40
MIN_LAST_PRICE_THRESHOLD = 2.0

def run_orb_screener():
    """Main function to execute the optimized ORB screening logic."""
    print("\n--- Running Opening Range Breakout (ORB) Screener ---")
    
    session = helpers.detect_market_session()
    if session != 'REGULAR':
        print(f"Market session is {session}. ORB screener only runs during REGULAR session.")
        return

    ny_timezone = pytz.timezone('America/New_York')
    current_ny_time = datetime.now(ny_timezone).time()
    
    # Define the time after which this screener should run.
    orb_trigger_time = time(9, ORB_TRIGGER_MINUTE)
    if current_ny_time < orb_trigger_time:
        print(f"Waiting to run ORB screener. Trigger time is {orb_trigger_time.strftime('%H:%M:%S')} NY time.")
        return

    # --- 1. Load Prerequisite Data ---
    # The ORB screener DEPENDS on the output of the Gap & Go screener.
    gapgo_signals_file = config.SIGNALS_DIR / "gapgo_signals.csv"
    if not gapgo_signals_file.exists():
        print(f"ERROR: Gap & Go signals file not found at {gapgo_signals_file}. Please run the Gap & Go screener first.")
        return
        
    gapgo_df = pd.read_csv(gapgo_signals_file)
    tickers = gapgo_df['Ticker'].tolist()
    all_results = []

    # --- 2. Process Each Ticker ---
    for ticker in tickers:
        try:
            # Get the pre-calculated data for this ticker from the GapGo results.
            gapgo_row = gapgo_df[gapgo_df['Ticker'] == ticker].iloc[0]
            
            # Load intraday data to get the latest price and calculate the opening range.
            intraday_file = config.INTRADAY_1MIN_DIR / f"{ticker}_1min.csv"
            if not intraday_file.exists():
                continue
            
            intraday_df = pd.read_csv(intraday_file, index_col=0, parse_dates=True)
            today_intraday_df = intraday_df[intraday_df.index.date == datetime.now(ny_timezone).date()].copy()
            
            if today_intraday_df.empty:
                continue

            last_price = today_intraday_df[config.CLOSE_COL].iloc[-1]

            # --- 3. Calculate the Opening Range (9:30 - 9:39) ---
            orb_start_time = config.REGULAR_SESSION_START
            orb_end_time = time(9, ORB_WINDOW_END_MINUTE)
            
            opening_range_candles = today_intraday_df.between_time(orb_start_time, orb_end_time)
            
            if opening_range_candles.empty:
                continue
            
            or_high = opening_range_candles[config.HIGH_COL].max()
            or_low = opening_range_candles[config.LOW_COL].min()

            # --- 4. Core ORB Breakout/Breakdown Logic ---
            orb_breakout = last_price > or_high
            orb_breakdown = last_price < or_low

            # --- 5. Calculate Setup Score based on combined signals ---
            setup_score = 0
            
            # Condition 1: VWAP Reclaimed? (from GapGo data)
            is_vwap_reclaimed = gapgo_row.get("VWAP Reclaimed?") == "Yes"
            if is_vwap_reclaimed:
                setup_score += 25
                
            # Condition 2: Breakout or Breakdown Triggered?
            is_breakout_or_breakdown_triggered = orb_breakout or orb_breakdown
            if is_breakout_or_breakdown_triggered:
                setup_score += 25
                
            # Condition 3: Volume Spike? (Pre-market or Live, from GapGo data)
            has_volume_spike = gapgo_row.get("Pre Volume Spike?") == "Yes" or gapgo_row.get("Live Volume Spike?") == "Yes"
            if has_volume_spike:
                setup_score += 25
                
            # Condition 4: Valid Gap Direction? (from GapGo data)
            has_valid_gap_direction = gapgo_row.get("Direction") in ["Long", "Short"]
            if has_valid_gap_direction:
                setup_score += 25

            # --- 6. Final Validation and Status ---
            setup_valid = (setup_score == 100 and last_price > MIN_LAST_PRICE_THRESHOLD)
            
            status = "Flat"
            if setup_valid:
                status = "Entry"
            elif setup_score >= 50:
                status = "Watch"

            # --- 7. Populate Result Dictionary ---
            result = {
                "Date": datetime.now(ny_timezone).strftime('%Y-%m-%d'),
                "US Time": datetime.now(ny_timezone).strftime('%H:%M:%S'),
                "Ticker": ticker,
                "Direction": gapgo_row.get("Direction", "N/A"),
                "Status": status,
                "Last Price": helpers.format_to_two_decimal(last_price),
                "Gap %": gapgo_row.get("Gap %", "N/A"),
                "Pre-Mkt High": gapgo_row.get("Pre-Mkt High", "N/A"),
                "Pre-Mkt Low": gapgo_row.get("Pre-Mkt Low", "N/A"),
                "Open Price (9:30 AM)": gapgo_row.get("Open Price (9:30 AM)", "N/A"),
                "Prev Close": gapgo_row.get("Prev Close", "N/A"),
                "Opening Range High": helpers.format_to_two_decimal(or_high),
                "Opening Range Low": helpers.format_to_two_decimal(or_low),
                "VWAP Reclaimed?": "Yes" if is_vwap_reclaimed else "No",
                "Pre Volume Spike?": gapgo_row.get("Pre Volume Spike?", "N/A"),
                "Live Volume Spike?": gapgo_row.get("Live Volume Spike?", "N/A"),
                "ORB Breakout?": "TRUE" if orb_breakout else "FALSE",
                "ORB Breakdown?": "TRUE" if orb_breakdown else "FALSE",
                "Setup Score %": setup_score,
                "Setup Valid?": "TRUE" if setup_valid else "FALSE",
                "Why Consider This Trade?": "" # Placeholder for future logic
            }
            all_results.append(result)

        except Exception as e:
            print(f"   - ERROR processing {ticker} for ORB: {e}")

    # --- 8. Final Processing & Save ---
    if not all_results:
        print("--- No tickers were processed for ORB. ---")
        return
        
    final_df = pd.DataFrame(all_results)
    final_df['Status'] = pd.Categorical(final_df['Status'], ["Entry", "Watch", "Flat"])
    final_df.sort_values(by=['Status', 'Setup Score %'], ascending=[True, False], inplace=True)
    
    helpers.save_signal_to_csv(final_df, 'orb')

if __name__ == "__main__":
    run_orb_screener()
