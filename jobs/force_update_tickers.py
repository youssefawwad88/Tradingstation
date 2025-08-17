#!/usr/bin/env python3
"""
Force Update Tickers Script - Phase 1 Implementation

This script forces live processing of all tickers in tickerlist.txt and ensures:
- Data is written to correct paths: data/intraday/{TICKER}_1min.csv, data/intraday_30min/{TICKER}_30min.csv, data/daily/{TICKER}_daily.csv
- TODAY'S DATA is included in all files
- Every successful write operation is logged
- Validates file exists and logs its contents
- Processes each ticker in sequence with detailed logging
"""

import logging
import os
import sys
import time
from datetime import datetime, timedelta

import pandas as pd

# Add project root to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from utils.alpha_vantage_api import get_daily_data, get_intraday_data
from utils.config import ALPHA_VANTAGE_API_KEY, DEBUG_MODE
from utils.helpers import (
    is_today_present_enhanced,
    log_detailed_operation,
    save_df_to_s3,
)

# Set up logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def load_manual_tickers_with_validation():
    """
    Read manual tickers directly from /workspace/tickerlist.txt with validation and logging.
    This function implements the diagnostic requirements from Phase 1.

    Returns:
        list: List of ticker symbols from tickerlist.txt
    """
    # Multiple possible locations as specified in requirements
    paths_to_check = [
        "/workspace/tickerlist.txt",
        "/workspace/data/tickerlist.txt",
        os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            "tickerlist.txt",
        ),
        "tickerlist.txt",
        "../tickerlist.txt",
    ]

    logger.info("🔍 SEARCHING for tickerlist.txt in multiple locations...")

    for path in paths_to_check:
        logger.info(f"   Checking path: {path}")
        if os.path.exists(path):
            logger.info(f"✅ FOUND tickerlist.txt at: {path}")
            try:
                with open(path, "r") as f:
                    content = f.read().strip()
                    tickers = [
                        line.strip() for line in content.split("\n") if line.strip()
                    ]

                # Log file contents as required
                logger.info(f"📋 Tickerlist.txt contents ({len(tickers)} tickers):")
                logger.info(f"   First 5 tickers: {tickers[:5]}")
                if len(tickers) > 5:
                    logger.info(f"   Remaining tickers: {tickers[5:]}")

                logger.info(
                    f"✅ Successfully loaded {len(tickers)} tickers from {path}"
                )
                return tickers
            except Exception as e:
                logger.error(f"❌ Error reading {path}: {e}")
        else:
            logger.warning(f"   ❌ Not found: {path}")

    logger.error("❌ CRITICAL: tickerlist.txt not found in any expected location!")
    logger.error("   Please ensure tickerlist.txt exists in one of the checked paths")
    return []


def fetch_and_validate_data(ticker, data_type, interval=None):
    """
    Fetch data and validate that TODAY'S data is included.

    Args:
        ticker (str): Ticker symbol
        data_type (str): 'daily', '30min', or '1min'
        interval (str): Optional interval for intraday data

    Returns:
        tuple: (DataFrame, bool) - data and success flag
    """
    start_time = datetime.now()

    try:
        if data_type == "daily":
            logger.info(f"📊 Fetching DAILY data for {ticker}...")
            df = get_daily_data(ticker, outputsize="compact")
            expected_path = f"data/daily/{ticker}_daily.csv"
        elif data_type == "30min":
            logger.info(f"📊 Fetching 30-MIN data for {ticker}...")
            df = get_intraday_data(ticker, interval="30min", outputsize="compact")
            expected_path = f"data/intraday_30min/{ticker}_30min.csv"
        elif data_type == "1min":
            logger.info(f"📊 Fetching 1-MIN data for {ticker}...")
            df = get_intraday_data(ticker, interval="1min", outputsize="compact")
            expected_path = f"data/intraday/{ticker}_1min.csv"
        else:
            logger.error(f"❌ Unknown data type: {data_type}")
            return pd.DataFrame(), False

        if df is None or df.empty:
            logger.warning(f"⚠️ No data returned for {ticker} {data_type}")
            return pd.DataFrame(), False

        # Log data details
        row_count = len(df)
        date_col = (
            "Date"
            if "Date" in df.columns
            else "datetime" if "datetime" in df.columns else "timestamp"
        )

        if date_col in df.columns:
            min_date = pd.to_datetime(df[date_col]).min()
            max_date = pd.to_datetime(df[date_col]).max()
            logger.info(f"   📅 Data range: {min_date} to {max_date}")
            logger.info(f"   📈 Row count: {row_count}")

            # Check for TODAY'S data as required
            today_present = is_today_present_enhanced(df, date_col)
            if today_present:
                logger.info(f"   ✅ TODAY'S DATA confirmed present")
            else:
                logger.warning(f"   ⚠️ TODAY'S DATA missing - may be weekend/holiday")

        logger.info(f"   🎯 Target path: {expected_path}")

        log_detailed_operation(
            ticker,
            f"{data_type.upper()} fetch complete",
            start_time,
            row_count_after=row_count,
        )

        return df, True

    except Exception as e:
        logger.error(f"❌ Error fetching {data_type} data for {ticker}: {e}")
        return pd.DataFrame(), False


def save_and_verify(df, ticker, data_type):
    """
    Save data and verify the operation with detailed logging.

    Args:
        df (DataFrame): Data to save
        ticker (str): Ticker symbol
        data_type (str): Type of data ('daily', '30min', '1min')

    Returns:
        bool: Success flag
    """
    if df.empty:
        logger.warning(f"⚠️ Cannot save empty DataFrame for {ticker} {data_type}")
        return False

    # Determine correct path as specified in requirements
    if data_type == "daily":
        file_path = f"data/daily/{ticker}_daily.csv"
    elif data_type == "30min":
        file_path = f"data/intraday_30min/{ticker}_30min.csv"
    elif data_type == "1min":
        file_path = f"data/intraday/{ticker}_1min.csv"
    else:
        logger.error(f"❌ Unknown data type: {data_type}")
        return False

    save_start_time = datetime.now()
    logger.info(f"💾 Saving {ticker} {data_type} to: {file_path}")

    try:
        # Use the existing save function
        success = save_df_to_s3(df, file_path)

        if success:
            logger.info(
                f"✅ SUCCESSFUL WRITE: {ticker} {data_type} saved to {file_path}"
            )
            logger.info(f"   📊 Saved {len(df)} rows")

            # Verify TODAY'S data is included in saved file
            date_col = (
                "Date"
                if "Date" in df.columns
                else "datetime" if "datetime" in df.columns else "timestamp"
            )
            if date_col in df.columns:
                today_present = is_today_present_enhanced(df, date_col)
                if today_present:
                    logger.info(f"   ✅ TODAY'S DATA confirmed in saved file")
                else:
                    logger.info(f"   📅 No today's data (may be weekend/holiday)")

            log_detailed_operation(
                ticker,
                f"{data_type.upper()} save complete",
                save_start_time,
                details=f"Path: {file_path}",
            )
            return True
        else:
            logger.error(
                f"❌ FAILED WRITE: {ticker} {data_type} failed to save to {file_path}"
            )
            return False

    except Exception as e:
        logger.error(f"❌ ERROR during save for {ticker} {data_type}: {e}")
        return False


def force_update_manual_tickers(verbose=False):
    """
    Main function to force update all manual tickers.
    Implements all Phase 1 requirements.

    Args:
        verbose (bool): Enable verbose logging
    """
    if verbose:
        logger.setLevel(logging.DEBUG)
        logger.info("🔊 VERBOSE MODE enabled")

    logger.info("🚀 FORCE UPDATE MANUAL TICKERS - Starting Process")
    logger.info("=" * 60)

    # Check API key
    if not ALPHA_VANTAGE_API_KEY:
        logger.error("❌ CRITICAL: ALPHA_VANTAGE_API_KEY not set!")
        logger.error("   Cannot proceed without API key")
        return False
    else:
        logger.info("✅ Alpha Vantage API key configured")

    # Load manual tickers with validation
    tickers = load_manual_tickers_with_validation()

    if not tickers:
        logger.error("❌ CRITICAL: No tickers found to process!")
        return False

    logger.info(f"🎯 Processing {len(tickers)} manual tickers:")
    logger.info(f"   Tickers: {tickers}")
    logger.info("=" * 60)

    overall_start_time = datetime.now()
    success_count = 0
    error_count = 0

    for i, ticker in enumerate(tickers, 1):
        logger.info(f"📈 PROCESSING TICKER {i}/{len(tickers)}: {ticker}")
        logger.info("-" * 40)

        ticker_start_time = datetime.now()
        ticker_success = True

        # Process each data type as specified in requirements
        data_types = [("daily", None), ("30min", "30min"), ("1min", "1min")]

        for data_type, interval in data_types:
            # Fetch data
            df, fetch_success = fetch_and_validate_data(ticker, data_type, interval)

            if fetch_success:
                # Save and verify
                save_success = save_and_verify(df, ticker, data_type)
                if not save_success:
                    ticker_success = False
                    error_count += 1
            else:
                ticker_success = False
                error_count += 1

            # Respect API rate limits
            time.sleep(0.5)

        if ticker_success:
            success_count += 1
            log_detailed_operation(
                ticker, "TICKER COMPLETE - All data types processed", ticker_start_time
            )
        else:
            logger.error(f"❌ TICKER FAILED: {ticker} had one or more errors")

        logger.info("-" * 40)

        # Rate limiting between tickers
        time.sleep(1)

    # Final summary
    overall_duration = (datetime.now() - overall_start_time).total_seconds()
    logger.info("=" * 60)
    logger.info("🏁 FORCE UPDATE COMPLETE - Final Summary:")
    logger.info(f"   ✅ Successful tickers: {success_count}/{len(tickers)}")
    logger.info(f"   ❌ Failed operations: {error_count}")
    logger.info(f"   ⏱️ Total duration: {overall_duration:.2f} seconds")
    logger.info(f"   📅 Completed at: {datetime.now()}")

    if success_count == len(tickers):
        logger.info("🎉 ALL MANUAL TICKERS PROCESSED SUCCESSFULLY!")
        return True
    else:
        logger.warning(f"⚠️ {len(tickers) - success_count} tickers had issues")
        return False


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Force update manual tickers from tickerlist.txt"
    )
    parser.add_argument("--verbose", action="store_true", help="Enable verbose logging")
    args = parser.parse_args()

    try:
        success = force_update_manual_tickers(verbose=args.verbose)
        if success:
            logger.info("✅ Script completed successfully")
            sys.exit(0)
        else:
            logger.error("❌ Script completed with errors")
            sys.exit(1)
    except Exception as e:
        logger.error(f"💥 CRITICAL ERROR: {e}")
        sys.exit(1)
