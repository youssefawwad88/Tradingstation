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


def run_gapgo_screener():
    """Main function to execute the FULL, cloud-aware Gap & Go screening logic."""
    logger.info("Running Gap & Go Screener")

    session = detect_market_session()
    logger.info(f"Market Session: {session}")

    if session == "CLOSED":
        logger.info("Market is closed - skipping screener")
        return

    tickers = read_tickerlist_from_s3("tickerlist.txt")
    if not tickers:
        logger.warning("Ticker list from cloud is empty")
        return

    all_results = []
    ny_timezone = pytz.timezone("America/New_York")
    ny_time = datetime.now(ny_timezone)

    breakout_valid_time = time(9, 36)

    for ticker in tickers:
        try:
            # --- 1. Load Data from Cloud Storage ---
            daily_df = read_df_from_s3(f"data/daily/{ticker}_daily.csv")
            intraday_df = read_df_from_s3(f"data/intraday/{ticker}_1min.csv")

            if daily_df.empty or intraday_df.empty:
                logger.debug(f"Data missing for {ticker} - skipping")
                continue

            # Convert index to datetime objects
            daily_df.index = pd.to_datetime(daily_df["timestamp"])
            intraday_df.index = pd.to_datetime(intraday_df["timestamp"])

            today_intraday_df = intraday_df[
                intraday_df.index.date == ny_time.date()
            ].copy()

            if today_intraday_df.empty:
                continue

            # --- 2. Calculate Base Metrics ---
            last_price = today_intraday_df["close"].iloc[-1]
            prev_close = get_previous_day_close(daily_df)
            premarket_df = get_premarket_data(today_intraday_df)
            premarket_high = (
                premarket_df["high"].max() if not premarket_df.empty else np.nan
            )
            premarket_low = (
                premarket_df["low"].min() if not premarket_df.empty else np.nan
            )
            premarket_volume = (
                premarket_df["volume"].sum() if not premarket_df.empty else 0
            )

            # --- 3. Session-Aware Calculations ---
            official_gap_percent, open_price_930, last_vwap = np.nan, np.nan, np.nan
            is_live_spike, today_early_volume = False, 0
            opening_range_pct, breakout_body_pct, breakout_vol_spike_pct = (
                np.nan,
                np.nan,
                np.nan,
            )

            historical_intraday_df = intraday_df[
                intraday_df.index.date < ny_time.date()
            ]
            avg_early_volume_5d = calculate_avg_early_volume(
                historical_intraday_df, days=5
            )
            avg_early_vol_complete = (
                "Yes"
                if avg_early_volume_5d is not None and avg_early_volume_5d > 0
                else "No"
            )

            if session == "REGULAR":
                opening_candle = today_intraday_df.loc[
                    today_intraday_df.index.time == time(9, 30)
                ]
                if not opening_candle.empty and prev_close is not None:
                    open_price_930 = opening_candle.iloc[0]["open"]
                    official_gap_percent = (
                        (open_price_930 - prev_close) / prev_close
                    ) * 100

                if avg_early_volume_5d is not None and avg_early_volume_5d > 0:
                    today_early_volume = calculate_avg_early_volume(
                        today_intraday_df, days=1
                    )
                    is_live_spike = today_early_volume >= (avg_early_volume_5d * 1.15)

                regular_session_df = today_intraday_df.between_time(
                    time(9, 30), time(16, 0)
                )
                if not regular_session_df.empty:
                    regular_session_df["vwap"] = calculate_vwap(regular_session_df)
                    last_vwap = regular_session_df["vwap"].iloc[-1]

            live_gap_percent_pm = (
                ((last_price - prev_close) / prev_close) * 100
                if prev_close is not None
                else np.nan
            )

            # --- 4. Determine Direction & Gap Label ---
            direction, gap_label = "Flat", "Flat"
            gap_to_check = (
                official_gap_percent if session == "REGULAR" else live_gap_percent_pm
            )
            if not np.isnan(gap_to_check):
                if gap_to_check >= 1.5:
                    direction = "Long"
                elif gap_to_check <= -1.5:
                    direction = "Short"
                if gap_to_check >= 4.0:
                    gap_label = "Huge Gap Up"
                elif gap_to_check >= 1.5:
                    gap_label = "Large Gap Up"
                elif gap_to_check <= -4.0:
                    gap_label = "Huge Gap Down"
                elif gap_to_check <= -1.5:
                    gap_label = "Large Gap Down"

            # --- 5. Evaluate Core Conditions ---
            avg_daily_vol_10d = calculate_avg_daily_volume(daily_df, 10)
            is_pre_vol_spike = (
                avg_daily_vol_10d is not None
                and avg_daily_vol_10d > 0
                and premarket_volume >= (avg_daily_vol_10d * 0.10)
            )

            # Long conditions
            gap_valid_long = (
                not np.isnan(official_gap_percent) and official_gap_percent >= 1.5
            )
            vwap_reclaimed = not np.isnan(last_vwap) and last_price > last_vwap
            breakout_above = (
                not np.isnan(premarket_high) and last_price > premarket_high
            )
            breakout_candles = (
                today_intraday_df[today_intraday_df["close"] > premarket_high]
                if not np.isnan(premarket_high)
                else pd.DataFrame()
            )
            first_breakout_time = (
                breakout_candles.index.min() if not breakout_candles.empty else None
            )
            time_valid = (
                first_breakout_time is not None
                and first_breakout_time.time() >= breakout_valid_time
            )

            # Short conditions
            gap_valid_short = (
                not np.isnan(official_gap_percent) and official_gap_percent <= -1.5
            )
            vwap_rejected = not np.isnan(last_vwap) and last_price < last_vwap
            breakdown_below = not np.isnan(premarket_low) and last_price < premarket_low
            breakdown_candles = (
                today_intraday_df[today_intraday_df["close"] < premarket_low]
                if not np.isnan(premarket_low)
                else pd.DataFrame()
            )
            first_breakdown_time = (
                breakdown_candles.index.min() if not breakdown_candles.empty else None
            )
            time_valid_short = (
                first_breakdown_time is not None
                and first_breakdown_time.time() >= breakout_valid_time
            )

            # --- 6. Final Validation and Scoring ---
            setup_valid, setup_score = False, 0
            if direction == "Long":
                conditions = [
                    gap_valid_long,
                    vwap_reclaimed,
                    breakout_above,
                    is_live_spike,
                    time_valid,
                ]
                setup_score = sum(conditions) * 20
                setup_valid = all(conditions)
            elif direction == "Short":
                conditions = [
                    gap_valid_short,
                    vwap_rejected,
                    breakdown_below,
                    is_live_spike,
                    time_valid_short,
                ]
                setup_score = sum(conditions) * 20
                setup_valid = all(conditions)

            # --- 7. Calculate Quality Metrics if Setup is Valid ---
            if setup_valid and session == "REGULAR":
                orb_5min_candles = today_intraday_df.between_time(
                    time(9, 30), time(9, 34)
                )
                if not orb_5min_candles.empty:
                    orb_high = orb_5min_candles["high"].max()
                    orb_low = orb_5min_candles["low"].min()
                    if open_price_930 > 0:
                        opening_range_pct = (
                            (orb_high - orb_low) / open_price_930
                        ) * 100

                    avg_orb_vol = orb_5min_candles["volume"].mean()

                    breakout_candle_time = (
                        first_breakout_time
                        if direction == "Long"
                        else first_breakdown_time
                    )
                    breakout_candle = today_intraday_df.loc[breakout_candle_time]
                    bo_range = breakout_candle["high"] - breakout_candle["low"]
                    if bo_range > 0:
                        breakout_body_pct = (
                            abs(breakout_candle["close"] - breakout_candle["open"])
                            / bo_range
                        ) * 100
                    if avg_orb_vol > 0:
                        breakout_vol_spike_pct = (
                            breakout_candle["volume"] / avg_orb_vol
                        ) * 100

            # --- 8. Determine Status ---
            status = "Flat"
            if setup_valid:
                status = "Entry"
            elif direction != "Flat" and is_pre_vol_spike:
                status = "Watch"

            # --- 8.5. Calculate Breakout Time Valid for Master Dashboard ---
            breakout_time_valid = "N/A"
            if direction == "Long" and time_valid and first_breakout_time is not None:
                breakout_time_valid = first_breakout_time.strftime("%H:%M")
            elif (
                direction == "Short"
                and time_valid_short
                and first_breakdown_time is not None
            ):
                breakout_time_valid = first_breakdown_time.strftime("%H:%M")

            # --- 9. Populate Full Result Dictionary ---
            result = {
                "Date": ny_time.strftime("%Y-%m-%d"),
                "US Time": ny_time.strftime("%H:%M:%S"),
                "Ticker": ticker,
                "Direction": direction,
                "Status": status,
                "Last Price": format_to_two_decimal(last_price),
                "Gap %": format_to_two_decimal(gap_to_check),
                "Gap Label": gap_label,
                "Pre-Mkt High": format_to_two_decimal(premarket_high),
                "Pre-Mkt Low": format_to_two_decimal(premarket_low),
                "Open Price (9:30 AM)": format_to_two_decimal(open_price_930),
                "Prev Close": format_to_two_decimal(prev_close),
                "Pre-Mkt Volume": f"{premarket_volume:,.0f}",
                "Avg Early Volume (15min)": (
                    f"{avg_early_volume_5d:,.0f}" if avg_early_volume_5d else "N/A"
                ),
                "Avg Early Vol Complete?": avg_early_vol_complete,
                "Today Early Volume (9:30–9:44)": (
                    f"{today_early_volume:,.0f}" if session == "REGULAR" else "N/A"
                ),
                "Pre Volume Spike?": "Yes" if is_pre_vol_spike else "No",
                "Live Volume Spike?": (
                    "Yes" if is_live_spike else "No" if session == "REGULAR" else "N/A"
                ),
                "VWAP Reclaimed?": (
                    "Yes" if vwap_reclaimed else "No" if session == "REGULAR" else "N/A"
                ),
                "Breakout Above Pre High?": "TRUE" if breakout_above else "FALSE",
                "Breakdown Below Pre Low?": "TRUE" if breakdown_below else "FALSE",
                "Setup Valid?": "TRUE" if setup_valid else "FALSE",
                "Avg Daily Vol (10-Day)": (
                    f"{avg_daily_vol_10d:,.0f}" if avg_daily_vol_10d else "N/A"
                ),
                "Setup Score %": setup_score,
                "Opening Range %": format_to_two_decimal(opening_range_pct),
                "Breakout Candle Body %": format_to_two_decimal(breakout_body_pct),
                "Breakout Volume Spike %": format_to_two_decimal(
                    breakout_vol_spike_pct
                ),
                "Breakout Time Valid?": breakout_time_valid,
            }
            all_results.append(result)

        except Exception as e:
            logger.error(f"Error processing {ticker}: {e}")

    # --- 10. Final Processing & Save to Cloud ---
    if all_results:
        final_df = pd.DataFrame(all_results)
        final_df.sort_values(
            by=["Status", "Setup Score %"], ascending=[True, False], inplace=True
        )

        # Save the results to a CSV file in our S3 bucket
        save_df_to_s3(final_df, "data/signals/gapgo_signals.csv")
        logger.info(f"Gap & Go screener finished - {len(all_results)} signals saved")
    else:
        logger.info("No Gap & Go signals generated")


if __name__ == "__main__":
    run_gapgo_screener()
