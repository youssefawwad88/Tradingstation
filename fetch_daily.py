#!/usr/bin/env python3
"""
Daily Data Fetcher

Fetches daily data (200 rows) for all tickers in master_tickerlist.csv.
Used for AVWAP anchors and swing analysis.
"""

import logging
import os
import sys
import time
from datetime import datetime, timedelta

import pandas as pd

# Add project root to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from utils.alpha_vantage_api import get_daily_data
from utils.helpers import read_master_tickerlist, save_df_to_s3, update_scheduler_status

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def fetch_daily_data():
    """
    Fetch daily data for all tickers in master_tickerlist.csv.
    Fetches exactly 200 rows per ticker as specified.
    """
    logger.info("🚀 Starting Daily Data Fetch Job")

    # Load tickers from master_tickerlist.csv
    tickers = read_master_tickerlist()

    if not tickers:
        logger.error("❌ No tickers to process. Exiting.")
        return False

    logger.info(
        f"📊 Processing {len(tickers)} tickers from master_tickerlist.csv for daily data"
    )

    successful_fetches = 0
    total_tickers = len(tickers)

    for ticker in tickers:
        logger.info(f"🔄 Fetching daily data for {ticker}")

        try:
            # Fetch daily data with full history
            daily_df = get_daily_data(ticker, outputsize="full")

            if not daily_df.empty:
                # Take exactly 200 rows as specified
                daily_df = daily_df.head(200)

                # Save to Spaces with correct path format
                file_path = f"data/daily/{ticker}_daily.csv"
                upload_success = save_df_to_s3(daily_df, file_path)

                if upload_success:
                    successful_fetches += 1
                    logger.info(
                        f"✅ {ticker}: Saved {len(daily_df)} rows of daily data"
                    )
                else:
                    logger.error(f"❌ {ticker}: Failed to upload daily data to Spaces")
            else:
                logger.warning(f"⚠️ {ticker}: No daily data returned from API")

        except Exception as e:
            logger.error(f"❌ {ticker}: Error fetching daily data - {e}")

        # Respect API rate limits (150 requests/minute = ~2.5 seconds between requests)
        time.sleep(1)

    logger.info(f"📋 Daily Data Fetch Job Completed")
    logger.info(f"   Success: {successful_fetches}/{total_tickers} tickers")
    logger.info(f"   Success Rate: {(successful_fetches/total_tickers*100):.1f}%")

    return successful_fetches > 0


if __name__ == "__main__":
    job_name = "fetch_daily"
    update_scheduler_status(job_name, "Running")

    try:
        success = fetch_daily_data()
        if success:
            update_scheduler_status(job_name, "Success")
            logger.info("✅ Daily data fetch completed successfully")
        else:
            update_scheduler_status(
                job_name, "Fail", "No tickers processed successfully"
            )
            logger.error("❌ Daily data fetch failed")
    except Exception as e:
        error_message = f"An unexpected error occurred: {e}"
        logger.error(error_message)
        update_scheduler_status(job_name, "Fail", error_message)
