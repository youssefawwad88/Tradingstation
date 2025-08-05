import pandas as pd
import sys
import os
from datetime import datetime, time
import pytz
import numpy as np
import logging

# --- System Path Setup ---
# This makes sure the script can find the 'utils' directory
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from utils.helpers import (
    read_df_from_s3, 
    save_df_to_s3,
    detect_market_session,
    format_to_two_decimal
)

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- ORB Screener Specific Configuration ---
ORB_WINDOW_END_MINUTE = 39
ORB_TRIGGER_MINUTE = 40
MIN_LAST_PRICE_THRESHOLD = 2.0

def run_orb_screener():
    """Main function to execute the cloud-aware ORB screening logic."""
    logger.info("Running Opening Range Breakout (ORB) Screener")
    
    session = detect_market_session()
    if session != 'REGULAR':
        logger.info(f"Market session is {session} - ORB screener only runs during REGULAR session")
        return

    ny_timezone = pytz.timezone('America/New_York')
    current_ny_time = datetime.now(ny_timezone).time()
    
    # Define the time after which this screener should run.
    orb_trigger_time = time(9, ORB_TRIGGER_MINUTE)
    if current_ny_time < orb_trigger_time:
        logger.info(f"Waiting to run ORB screener - trigger time is {orb_trigger_time.strftime('%H:%M:%S')} NY time")
        return

    # --- 1. Load Prerequisite Data from Cloud Storage ---
    logger.info("Loading prerequisite Gap & Go signals from cloud")
    gapgo_df = read_df_from_s3('data/signals/gapgo_signals.csv')
    
    if gapgo_df.empty:
        logger.error("Gap & Go signals file not found in cloud storage - cannot run ORB screener")
        return
        
    tickers = gapgo_df['Ticker'].tolist()
    all_results = []

    # --- 2. Process Each Ticker ---
    for ticker in tickers:
        try:
            # Get the pre-calculated data for this ticker from the GapGo results.
            gapgo_row = gapgo_df[gapgo_df['Ticker'] == ticker].iloc[0]
            
            # Load intraday data from cloud to get the latest price and calculate the opening range.
            intraday_df = read_df_from_s3(f"data/intraday/{ticker}_1min.csv")
            
            if intraday_df.empty:
                continue
            
            intraday_df.index = pd.to_datetime(intraday_df['timestamp'])
            today_intraday_df = intraday_df[intraday_df.index.date == datetime.now(ny_timezone).date()].copy()
            
            if today_intraday_df.empty:
                continue

            last_price = today_intraday_df['close'].iloc[-1]

            # --- 3. Calculate the Opening Range (9:30 - 9:39) ---
            orb_start_time = time(9, 30)
            orb_end_time = time(9, ORB_WINDOW_END_MINUTE)
            
            opening_range_candles = today_intraday_df.between_time(orb_start_time, orb_end_time)
            
            if opening_range_candles.empty:
                continue
            
            or_high = opening_range_candles['high'].max()
            or_low = opening_range_candles['low'].min()

            # --- 4. Core ORB Breakout/Breakdown Logic ---
            orb_breakout = last_price > or_high
            orb_breakdown = last_price < or_low

            # --- 5. Calculate Setup Score based on combined signals ---
            setup_score = 0
            is_vwap_reclaimed = gapgo_row.get("VWAP Reclaimed?") == "Yes"
            if is_vwap_reclaimed:
                setup_score += 25
                
            is_breakout_or_breakdown_triggered = orb_breakout or orb_breakdown
            if is_breakout_or_breakdown_triggered:
                setup_score += 25
                
            has_volume_spike = gapgo_row.get("Pre Volume Spike?") == "Yes" or gapgo_row.get("Live Volume Spike?") == "Yes"
            if has_volume_spike:
                setup_score += 25
                
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
                "Ticker": ticker,
                "Direction": gapgo_row.get("Direction", "N/A"),
                "Status": status,
                "Last Price": format_to_two_decimal(last_price),
                "Opening Range High": format_to_two_decimal(or_high),
                "Opening Range Low": format_to_two_decimal(or_low),
                "ORB Breakout?": "TRUE" if orb_breakout else "FALSE",
                "ORB Breakdown?": "TRUE" if orb_breakdown else "FALSE",
                "Setup Score %": setup_score,
                "Setup Valid?": "TRUE" if setup_valid else "FALSE",
            }
            all_results.append(result)

        except Exception as e:
            logger.error(f"Error processing {ticker} for ORB: {e}")

    # --- 8. Final Processing & Save to Cloud ---
    if not all_results:
        logger.info("No tickers processed for ORB")
        return
        
    final_df = pd.DataFrame(all_results)
    final_df['Status'] = pd.Categorical(final_df['Status'], ["Entry", "Watch", "Flat"])
    final_df.sort_values(by=['Status', 'Setup Score %'], ascending=[True, False], inplace=True)
    
    save_df_to_s3(final_df, 'data/signals/orb_signals.csv')
    logger.info(f"ORB screener finished - {len(all_results)} signals saved")

if __name__ == "__main__":
    run_orb_screener()
