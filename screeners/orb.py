import logging
import os
import sys
from datetime import datetime, time

import numpy as np
import pandas as pd
import pytz

# --- System Path Setup ---
# This makes sure the script can find the 'utils' directory
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from utils.helpers import (
    calculate_avg_daily_volume,
    calculate_avg_early_volume,
    calculate_vwap,
    detect_market_session,
    format_to_two_decimal,
    get_premarket_data,
    get_previous_day_close,
    read_df_from_s3,
    read_tickerlist_from_s3,
    save_df_to_s3,
)

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- ORB Screener Specific Configuration ---
ORB_WINDOW_END_MINUTE = 39
ORB_TRIGGER_MINUTE = 40
MIN_LAST_PRICE_THRESHOLD = 2.0


def run_orb_screener():
    """Main function to execute the decoupled ORB screening logic."""
    logger.info("Running Opening Range Breakout (ORB) Screener")

    session = detect_market_session()
    if session != "REGULAR":
        logger.info(
            f"Market session is {session} - ORB screener only runs during REGULAR session"
        )
        return

    ny_timezone = pytz.timezone("America/New_York")
    current_ny_time = datetime.now(ny_timezone).time()

    # Define the time after which this screener should run.
    orb_trigger_time = time(9, ORB_TRIGGER_MINUTE)
    if current_ny_time < orb_trigger_time:
        logger.info(
            f"Waiting to run ORB screener - trigger time is {orb_trigger_time.strftime('%H:%M:%S')} NY time"
        )
        return

    # --- 1. Load Tickers from Cloud Storage (DECOUPLED) ---
    logger.info("Loading tickers from master ticker list")
    tickers = read_tickerlist_from_s3("tickerlist.txt")

    if not tickers:
        logger.warning("No tickers found in tickerlist.txt - cannot run ORB screener")
        return

    all_results = []
    ny_date = datetime.now(ny_timezone).date()

    # --- 2. Process Each Ticker Independently ---
    for ticker in tickers:
        try:
            # Load intraday and daily data from cloud
            intraday_df = read_df_from_s3(f"data/intraday/{ticker}_1min.csv")
            daily_df = read_df_from_s3(f"data/daily/{ticker}_daily.csv")

            if intraday_df.empty:
                continue

            intraday_df.index = pd.to_datetime(intraday_df["timestamp"])
            today_intraday_df = intraday_df[intraday_df.index.date == ny_date].copy()

            if today_intraday_df.empty:
                continue

            last_price = today_intraday_df["close"].iloc[-1]

            # --- 3. Calculate the Opening Range (9:30 - 9:39) ---
            orb_start_time = time(9, 30)
            orb_end_time = time(9, ORB_WINDOW_END_MINUTE)

            opening_range_candles = today_intraday_df.between_time(
                orb_start_time, orb_end_time
            )

            if opening_range_candles.empty:
                continue

            or_high = opening_range_candles["high"].max()
            or_low = opening_range_candles["low"].min()

            # --- 4. Core ORB Breakout/Breakdown Logic ---
            orb_breakout = last_price > or_high
            orb_breakdown = last_price < or_low

            # --- 5. Determine Direction from ORB State (DECOUPLED) ---
            direction = "None"
            if orb_breakout:
                direction = "Long"
            elif orb_breakdown:
                direction = "Short"

            # --- 6. Calculate VWAP Internally (DECOUPLED) ---
            regular_session_df = today_intraday_df.between_time(
                time(9, 30), time(16, 0)
            )
            vwap_reclaimed = "No"
            if not regular_session_df.empty:
                regular_session_df["vwap"] = calculate_vwap(regular_session_df)
                last_vwap = regular_session_df["vwap"].iloc[-1]
                vwap_reclaimed = "Yes" if last_price > last_vwap else "No"

            # --- 7. Calculate Volume Spikes Internally (DECOUPLED) ---
            # Pre Volume Spike calculation
            prev_close = (
                get_previous_day_close(daily_df) if not daily_df.empty else None
            )
            premarket_df = get_premarket_data(today_intraday_df)
            premarket_volume = (
                premarket_df["volume"].sum() if not premarket_df.empty else 0
            )
            avg_daily_vol_10d = (
                calculate_avg_daily_volume(daily_df, 10) if not daily_df.empty else None
            )
            pre_volume_spike = "No"
            if avg_daily_vol_10d and avg_daily_vol_10d > 0:
                pre_volume_spike = (
                    "Yes" if premarket_volume >= (avg_daily_vol_10d * 0.10) else "No"
                )

            # Live Volume Spike calculation
            historical_intraday_df = intraday_df[intraday_df.index.date < ny_date]
            avg_early_volume_5d = calculate_avg_early_volume(
                historical_intraday_df, days=5
            )
            live_volume_spike = "No"
            if avg_early_volume_5d and avg_early_volume_5d > 0:
                today_early_volume = calculate_avg_early_volume(
                    today_intraday_df, days=1
                )
                live_volume_spike = (
                    "Yes"
                    if today_early_volume >= (avg_early_volume_5d * 1.15)
                    else "No"
                )

            # --- 8. Calculate Gap % for additional context (DECOUPLED) ---
            gap_percent = np.nan
            if prev_close and not opening_range_candles.empty:
                open_price_930 = opening_range_candles.iloc[0]["open"]
                gap_percent = ((open_price_930 - prev_close) / prev_close) * 100

            # --- 9. Calculate Setup Score based on internal conditions ---
            setup_score = 0

            # VWAP Reclaimed? (25 points)
            if vwap_reclaimed == "Yes":
                setup_score += 25

            # Triggered? (25 points)
            if orb_breakout or orb_breakdown:
                setup_score += 25

            # Volume Spike? (25 points)
            if pre_volume_spike == "Yes" or live_volume_spike == "Yes":
                setup_score += 25

            # Valid Gap Direction? (25 points)
            if direction in ["Long", "Short"]:
                setup_score += 25

            # --- 10. Final Validation and Status ---
            setup_valid = setup_score == 100 and last_price > MIN_LAST_PRICE_THRESHOLD

            status = "Flat"
            if setup_valid:
                status = "Entry"
            elif setup_score >= 50:
                status = "Watch"

            # --- 11. Populate Result Dictionary with Required Columns ---
            result = {
                "Date": ny_date.strftime("%Y-%m-%d"),
                "US Time": current_ny_time.strftime("%H:%M:%S"),
                "Ticker": ticker,
                "Direction": direction,
                "Status": status,
                "Last Price": format_to_two_decimal(last_price),
                "Gap %": format_to_two_decimal(gap_percent),
                "Pre-Mkt High": format_to_two_decimal(
                    premarket_df["high"].max() if not premarket_df.empty else np.nan
                ),
                "Pre-Mkt Low": format_to_two_decimal(
                    premarket_df["low"].min() if not premarket_df.empty else np.nan
                ),
                "Open Price (9:30 AM)": format_to_two_decimal(
                    opening_range_candles.iloc[0]["open"]
                    if not opening_range_candles.empty
                    else np.nan
                ),
                "Prev Close": format_to_two_decimal(prev_close),
                "Opening Range High": format_to_two_decimal(or_high),
                "Opening Range Low": format_to_two_decimal(or_low),
                "VWAP Reclaimed?": vwap_reclaimed,
                "Pre Volume Spike?": pre_volume_spike,
                "Live Volume Spike?": live_volume_spike,
                "ORB Breakout?": "TRUE" if orb_breakout else "FALSE",
                "ORB Breakdown?": "TRUE" if orb_breakdown else "FALSE",
                "Setup Score %": setup_score,
                "Setup Valid?": "TRUE" if setup_valid else "FALSE",
                "Why Consider This Trade?": "",  # Keep blank for now as specified
            }
            all_results.append(result)

        except Exception as e:
            logger.error(f"Error processing {ticker} for ORB: {e}")

    # --- 12. Final Processing & Save to Cloud ---
    if not all_results:
        logger.info("No tickers processed for ORB")
        return

    final_df = pd.DataFrame(all_results)
    final_df["Status"] = pd.Categorical(final_df["Status"], ["Entry", "Watch", "Flat"])
    final_df.sort_values(
        by=["Status", "Setup Score %"], ascending=[True, False], inplace=True
    )

    save_df_to_s3(final_df, "data/signals/orb_signals.csv")
    logger.info(f"ORB screener finished - {len(all_results)} signals saved")


if __name__ == "__main__":
    run_orb_screener()
