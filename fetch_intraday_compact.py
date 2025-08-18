#!/usr/bin/env python3
"""
Intraday Compact Fetcher (Every Minute)

Fetches only today's 1-minute data and appends new candles only.
Used for live price action and setup monitoring.
Reads from master_tickerlist.csv.

Updated with intelligent fetch strategy based on 10KB file size rule:
- Files ≤ 10KB trigger full historical fetch (outputsize='full')
- Files > 10KB use compact fetch (outputsize='compact') for real-time updates
- Supports both 1min and 30min intervals through DATA_INTERVAL configuration
"""

import logging
import os
import sys
import time
from datetime import datetime, timedelta

import pandas as pd
import pytz

# CONFIGURATION: Data interval (can be "1min" or "30min")
DATA_INTERVAL = "1min"  # Change this to "30min" for 30-minute interval processing

# Add project root to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from utils.alpha_vantage_api import get_intraday_data
from utils.helpers import (
    detect_market_session,
    is_today_present,
    read_df_from_s3,
    read_master_tickerlist,
    save_df_to_s3,
    update_scheduler_status,
)
from utils.market_time import is_market_open_on_date

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def append_new_candles_smart(existing_df, new_df):
    """
    Smart append function that only adds truly new candles.

    Args:
        existing_df (pandas.DataFrame): Existing data
        new_df (pandas.DataFrame): New data to append

    Returns:
        pandas.DataFrame: Combined DataFrame with only new candles added
    """
    if existing_df.empty:
        return new_df

    if new_df.empty:
        return existing_df

    try:
        # Ensure timestamp columns exist and are datetime
        timestamp_col = "timestamp" if "timestamp" in new_df.columns else "Date"
        existing_df[timestamp_col] = pd.to_datetime(existing_df[timestamp_col])
        new_df[timestamp_col] = pd.to_datetime(new_df[timestamp_col])

        # Get the latest timestamp in existing data
        latest_existing = existing_df[timestamp_col].max()

        # Only keep new data that's newer than existing
        truly_new = new_df[new_df[timestamp_col] > latest_existing]

        if not truly_new.empty:
            combined = pd.concat([existing_df, truly_new], ignore_index=True)
            combined = combined.drop_duplicates(subset=[timestamp_col], keep="last")
            combined = combined.sort_values(timestamp_col)
            logger.info(f"   Added {len(truly_new)} new candles")
            return combined
        else:
            logger.info(f"   No new candles to add")
            return existing_df

    except Exception as e:
        logger.error(f"Error in smart append: {e}")
        return existing_df


def fetch_intraday_compact():
    """
    Fetch intraday data for all tickers in master_tickerlist.csv.
    Appends only new candles to existing data.

    Uses intelligent fetch strategy based on 10KB file size rule:
    - If file ≤ 10KB or missing: use outputsize='full' for complete historical data
    - If file > 10KB: use outputsize='compact' for efficient real-time updates

    Supports both 1min and 30min intervals via DATA_INTERVAL configuration.
    
    ENHANCED with extensive logging for diagnostic purposes as per problem statement Phase 1.2.
    """
    logger.info(f"🚀 Starting Intraday Compact Fetch Job - {DATA_INTERVAL} interval")

    # Check API key availability
    from utils.config import ALPHA_VANTAGE_API_KEY, SPACES_BUCKET_NAME

    if not ALPHA_VANTAGE_API_KEY:
        logger.warning("⚠️ ALPHA_VANTAGE_API_KEY not configured")
        logger.warning("💡 Running in TEST MODE - no new data will be fetched")
        logger.warning(
            "🔧 Set ALPHA_VANTAGE_API_KEY environment variable to enable data fetching"
        )
        logger.warning(
            "📝 For production use, ensure API credentials are properly configured"
        )
        # Don't return False here - let it continue for weekend testing

    if not SPACES_BUCKET_NAME:
        logger.warning(
            "⚠️ DigitalOcean Spaces not configured - using local storage only"
        )
        logger.warning(
            "💡 CSV files will be saved locally but NOT uploaded to cloud storage"
        )
        logger.warning("🔧 Set SPACES credentials to enable cloud storage uploads")

    # Load tickers from master_tickerlist.csv
    tickers = read_master_tickerlist()

    if not tickers:
        logger.error("❌ No tickers to process. Exiting.")
        return False

    logger.info(f"📊 Processing {len(tickers)} tickers from master_tickerlist.csv")
    logger.info(f"🕐 Using {DATA_INTERVAL} interval")

    # Check market session
    market_session = detect_market_session()
    logger.info(f"🕐 Current market session: {market_session}")

    successful_fetches = 0
    new_candles_added = 0
    total_tickers = len(tickers)

    # PHASE 1.2: Enhanced logging for each ticker as specified in problem statement
    for ticker in tickers:
        logger.info(f"🔄 Processing {ticker}")

        try:
            # Determine file path based on interval
            if DATA_INTERVAL == "30min":
                file_path = f"data/intraday_30min/{ticker}_30min.csv"
            else:
                file_path = f"data/intraday/{ticker}_1min.csv"

            # LOGGING: Log the exact file path being processed
            logger.info(f"📂 TICKER {ticker}: Target file path: {file_path}")

            # Get existing data
            existing_df = read_df_from_s3(file_path)
            logger.info(f"📊 TICKER {ticker}: Existing data rows: {len(existing_df)}")

            # CORE LOGIC: Intelligent Fetch Strategy based on 10KB file size rule
            # Exactly as specified in the problem statement

            # Get the local file path for size checking
            local_file_path = os.path.join(
                os.path.dirname(os.path.abspath(__file__)), file_path
            )

            file_exists = os.path.exists(local_file_path)
            should_do_full_fetch = True  # Assume we need a full fetch by default

            if file_exists:
                file_size_kb = os.path.getsize(local_file_path) / 1024
                logger.info(
                    f"💡 File '{file_path}' exists. Size: {file_size_kb:.2f} KB"
                )
                if file_size_kb >= 10:  # Check against the 10KB threshold
                    should_do_full_fetch = False
                    logger.info(
                        "✅ Skipping full historical data fetch. File size is sufficient."
                    )
                else:
                    logger.info(
                        "⚠️ File size is below threshold. Performing full fetch."
                    )
            else:
                logger.info(f"💡 File '{file_path}' not found. Performing full fetch.")

            # Determine outputsize strategy based on file size check
            if should_do_full_fetch:
                outputsize_strategy = "full"
                logger.info(
                    f"   {ticker}: Using outputsize='full' for complete historical fetch"
                )
            else:
                outputsize_strategy = "compact"
                logger.info(
                    f"   {ticker}: Using outputsize='compact' for real-time updates"
                )

            # LOGGING: Construct and log the exact API URL being called (Phase 1.2 requirement)
            api_url_params = {
                "function": "TIME_SERIES_INTRADAY",
                "symbol": ticker,
                "interval": DATA_INTERVAL,
                "outputsize": outputsize_strategy,
                "apikey": "***API_KEY***",  # Don't log actual API key
                "datatype": "csv",
            }
            api_url = "https://www.alphavantage.co/query?" + "&".join([f"{k}={v}" for k, v in api_url_params.items()])
            logger.info(f"🌐 TICKER {ticker}: API URL being called: {api_url}")

            # Fetch data using determined strategy and configured interval
            latest_df = get_intraday_data(
                ticker, interval=DATA_INTERVAL, outputsize=outputsize_strategy
            )

            # LOGGING: Log the number of rows received from API (Phase 1.2 requirement)
            logger.info(f"📊 TICKER {ticker}: API response rows received: {len(latest_df)}")

            if not latest_df.empty:
                # LOGGING: Log timestamp of last candle received (Phase 1.2 requirement)
                if "timestamp" in latest_df.columns:
                    latest_timestamp = latest_df["timestamp"].iloc[-1]
                    logger.info(f"📅 TICKER {ticker}: Last candle timestamp: {latest_timestamp}")
                elif len(latest_df.columns) >= 6:
                    logger.info(f"⚠️ TICKER {ticker}: No timestamp column found, will normalize columns")

                # Normalize column names if needed
                if "timestamp" not in latest_df.columns and len(latest_df.columns) >= 6:
                    latest_df.columns = [
                        "timestamp",
                        "open",
                        "high",
                        "low",
                        "close",
                        "volume",
                    ]
                    logger.info(f"🔄 TICKER {ticker}: Columns normalized to standard format")

                # Handle data combination based on fetch strategy
                if outputsize_strategy == "full":
                    # For full fetch, we have complete historical data
                    # Combine with existing if available, but prioritize new full dataset
                    combined_df = append_new_candles_smart(existing_df, latest_df)
                    logger.debug(f"   {ticker}: Full fetch completed, merged data")
                else:
                    # For compact fetch, append only new candles to existing data
                    combined_df = append_new_candles_smart(existing_df, latest_df)
                    logger.debug(
                        f"   {ticker}: Compact fetch completed, appended new candles"
                    )

                # LOGGING: Log combined data information
                logger.info(f"📊 TICKER {ticker}: Combined data rows: {len(combined_df)}")

                # Keep only last 7 days of data (rolling window)
                # Use timezone-aware datetime for proper comparison
                ny_tz = pytz.timezone("America/New_York")
                seven_days_ago = datetime.now(ny_tz) - timedelta(days=7)
                timestamp_col = (
                    "timestamp" if "timestamp" in combined_df.columns else "Date"
                )
                combined_df[timestamp_col] = pd.to_datetime(combined_df[timestamp_col])

                # Ensure timestamps are timezone-aware for proper comparison
                if combined_df[timestamp_col].dt.tz is None:
                    # If naive, localize to NY timezone first
                    combined_df[timestamp_col] = combined_df[
                        timestamp_col
                    ].dt.tz_localize(ny_tz)
                elif combined_df[timestamp_col].dt.tz != ny_tz:
                    # If different timezone, convert to NY timezone
                    combined_df[timestamp_col] = combined_df[
                        timestamp_col
                    ].dt.tz_convert(ny_tz)

                # Now we can safely compare timezone-aware datetimes
                combined_df = combined_df[combined_df[timestamp_col] >= seven_days_ago]
                logger.info(f"📊 TICKER {ticker}: After 7-day trim: {len(combined_df)} rows")

                # LOGGING: Log the final file path where script is attempting to save (Phase 1.2 requirement)
                logger.info(f"💾 TICKER {ticker}: Attempting to save to final path: {file_path}")

                # CRITICAL VALIDATION: Check if today's data is present before declaring success
                # This is the core fix for the compact fetch issue described in the problem statement
                ny_tz = pytz.timezone("America/New_York")
                today_et = datetime.now(ny_tz).date()
                
                # Validate that final combined data contains today's candles
                today_data_present = False
                if not combined_df.empty and timestamp_col in combined_df.columns:
                    try:
                        # Convert timestamps to ET for today's data check
                        df_timestamps = pd.to_datetime(combined_df[timestamp_col])
                        if df_timestamps.dt.tz is None:
                            df_timestamps_et = df_timestamps.dt.tz_localize(ny_tz)
                        else:
                            df_timestamps_et = df_timestamps.dt.tz_convert(ny_tz)
                        
                        today_rows = (df_timestamps_et.dt.date == today_et).sum()
                        today_data_present = today_rows > 0
                        
                        logger.info(f"🗓️ TICKER {ticker}: Today's data validation:")
                        logger.info(f"   📅 Today's date (ET): {today_et}")
                        logger.info(f"   📊 Rows with today's data: {today_rows}")
                        logger.info(f"   ✅ Today's data present: {today_data_present}")
                        
                        if today_data_present:
                            # Log the time range of today's data for debugging
                            today_data = df_timestamps_et[df_timestamps_et.dt.date == today_et]
                            if not today_data.empty:
                                logger.info(f"   🕐 Today's data range: {today_data.min()} to {today_data.max()}")
                        
                    except Exception as e:
                        logger.error(f"❌ TICKER {ticker}: Error validating today's data: {e}")
                        today_data_present = False

                # HARDENED VALIDATION: Only declare success if today's data is present OR the market is closed (weekend/holiday)
                market_closed = not is_market_open_on_date()  # Comprehensive check including holidays
                
                if not today_data_present and not market_closed:
                    # FAIL THE FETCH: Today's data is missing during market hours
                    logger.error(f"❌ TICKER {ticker}: COMPACT FETCH VALIDATION FAILED")
                    logger.error(f"   💡 This is the exact issue described in the problem statement!")
                    logger.error(f"   📊 Final data contains {len(combined_df)} rows but NO today's candles")
                    logger.error(f"   🗓️ Expected data for: {today_et}")
                    logger.error(f"   ⚠️ Script will NOT declare this ticker as successful")
                    # Do not increment successful_fetches - this is a failed fetch
                    continue
                elif not today_data_present and market_closed:
                    logger.info(f"⚠️ TICKER {ticker}: No today's data, but market is closed (weekend/holiday)")
                    logger.info(f"   📅 This is acceptable during non-market hours")

                # Save updated data
                upload_success = save_df_to_s3(combined_df, file_path)

                if upload_success:
                    successful_fetches += 1
                    new_candles_count = (
                        len(combined_df) - len(existing_df)
                        if not existing_df.empty
                        else len(combined_df)
                    )
                    if new_candles_count > 0:
                        new_candles_added += new_candles_count
                    
                    # LOGGING: Confirm successful save with detailed information
                    logger.info(f"✅ TICKER {ticker}: Successfully saved to {file_path}")
                    logger.info(f"📊 TICKER {ticker}: Total rows in final file: {len(combined_df)}")
                    logger.info(f"🆕 TICKER {ticker}: New candles added: {new_candles_count}")
                    if today_data_present:
                        logger.info(f"🎯 TICKER {ticker}: VALIDATION PASSED - Today's data confirmed present")
                else:
                    logger.error(f"❌ TICKER {ticker}: Failed to upload to Spaces at {file_path}")
            else:
                # CRITICAL: No new data from API is now treated as a failure during market hours
                market_closed = not is_market_open_on_date()  # Comprehensive check including holidays
                
                if not market_closed:
                    logger.error(f"❌ TICKER {ticker}: No new data from API during market hours")
                    logger.error(f"   💡 This indicates a potential API or connectivity issue")
                    logger.error(f"   ⚠️ Script will NOT declare this ticker as successful")
                    # Do not increment successful_fetches - this is a failed fetch during market hours
                else:
                    logger.warning(f"⚠️ TICKER {ticker}: No new data from API (acceptable during market closure)")
                    successful_fetches += 1  # Acceptable during market closure

        except Exception as e:
            logger.error(f"❌ TICKER {ticker}: Error processing - {e}")
            # LOGGING: Add detailed error information for debugging
            import traceback
            logger.error(f"❌ TICKER {ticker}: Full error traceback: {traceback.format_exc()}")

        # Small delay to respect API limits (can be faster for compact fetches)
        time.sleep(0.5)

    logger.info(f"📋 Intraday Compact Fetch Job Completed")
    logger.info(f"   Interval: {DATA_INTERVAL}")
    logger.info(f"   Processed: {successful_fetches}/{total_tickers} tickers")
    logger.info(f"   New candles added: {new_candles_added}")
    logger.info(f"   Market session: {market_session}")

    return successful_fetches > 0


def run_final_logic():
    """
    Main run function implementing the intelligent data fetching logic
    based on the 10KB file size rule and interval compatibility.

    This function demonstrates the exact logic requested:
    - Dynamic fetch strategy based on file size analysis
    - Support for both 1min and 30min intervals
    - Proper API integration with outputsize parameter
    """
    logger.info("=" * 80)
    logger.info(f"🚀 RUNNING FINAL LOGIC - {DATA_INTERVAL} INTERVAL")
    logger.info("=" * 80)

    return fetch_intraday_compact()


if __name__ == "__main__":
    job_name = "fetch_intraday_compact"
    update_scheduler_status(job_name, "Running")

    try:
        # Check for interval configuration from command line
        if len(sys.argv) > 1:
            if sys.argv[1] == "--30min":
                DATA_INTERVAL = "30min"
                logger.info("🔧 Command line override: Using 30-minute interval")
            elif sys.argv[1] == "--1min":
                DATA_INTERVAL = "1min"
                logger.info("🔧 Command line override: Using 1-minute interval")

        success = run_final_logic()
        if success:
            update_scheduler_status(job_name, "Success")
            logger.info("✅ Intraday compact fetch completed successfully")
        else:
            update_scheduler_status(
                job_name, "Fail", "No tickers processed successfully"
            )
            logger.error("❌ Intraday compact fetch failed")
    except Exception as e:
        error_message = f"An unexpected error occurred: {e}"
        logger.error(error_message)
        update_scheduler_status(job_name, "Fail", error_message)
