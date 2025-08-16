#!/usr/bin/env python3
"""
30-Minute Data Fetcher

Fetches 30-minute intraday data (500 rows) for all tickers in master_tickerlist.csv.
Used for breakout windows, ORB, and EMA pulls.
"""

import pandas as pd
import sys
import os
from datetime import datetime, timedelta
import time
import logging

# Add project root to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from utils.helpers import read_master_tickerlist, save_df_to_s3, update_scheduler_status
from utils.alpha_vantage_api import get_intraday_data
from jobs.full_fetch import trim_data_to_requirements

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def fetch_30min_data():
    """
    Fetch 30-minute intraday data for all tickers in master_tickerlist.csv.
    Fetches exactly 500 rows per ticker as specified.
    """
    logger.info("🚀 Starting 30-Minute Data Fetch Job")
    
    # Check API key availability
    from utils.config import ALPHA_VANTAGE_API_KEY, SPACES_BUCKET_NAME
    if not ALPHA_VANTAGE_API_KEY:
        logger.error("❌ ALPHA_VANTAGE_API_KEY not configured")
        logger.error("💡 Cannot fetch 30-minute data without API key")
        logger.error("🔧 Set ALPHA_VANTAGE_API_KEY environment variable to enable data fetching")
        logger.error("📝 For production use, ensure API credentials are properly configured")
        return False
    
    if not SPACES_BUCKET_NAME:
        logger.warning("⚠️ DigitalOcean Spaces not configured - using local storage only")
        logger.warning("💡 CSV files will be saved locally but NOT uploaded to cloud storage")
        logger.warning("🔧 Set SPACES credentials to enable cloud storage uploads")
    
    # Load tickers from master_tickerlist.csv
    tickers = read_master_tickerlist()
    
    if not tickers:
        logger.error("❌ No tickers to process. Exiting.")
        return False

    logger.info(f"📊 Processing {len(tickers)} tickers from master_tickerlist.csv for 30-min data")

    successful_fetches = 0
    total_tickers = len(tickers)

    for ticker in tickers:
        logger.info(f"🔄 Fetching 30-min data for {ticker}")

        try:
            # Fetch 30-minute intraday data with full history
            intraday_30min_df = get_intraday_data(ticker, interval='30min', outputsize='full')
            
            if not intraday_30min_df.empty:
                # Apply proper 30-minute data trimming (most recent 500 rows, properly sorted)
                intraday_30min_df = trim_data_to_requirements(intraday_30min_df, '30min')
                
                # Save to Spaces with correct path format
                file_path = f'data/intraday_30min/{ticker}_30min.csv'
                upload_success = save_df_to_s3(intraday_30min_df, file_path)
                
                if upload_success:
                    successful_fetches += 1
                    logger.info(f"✅ {ticker}: Saved {len(intraday_30min_df)} rows of 30-min data")
                else:
                    logger.error(f"❌ {ticker}: Failed to upload 30-min data to Spaces")
            else:
                logger.warning(f"⚠️ {ticker}: No 30-min data returned from API")
                
        except Exception as e:
            logger.error(f"❌ {ticker}: Error fetching 30-min data - {e}")
        
        # Respect API rate limits (150 requests/minute = ~2.5 seconds between requests)
        time.sleep(1)

    logger.info(f"📋 30-Minute Data Fetch Job Completed")
    logger.info(f"   Success: {successful_fetches}/{total_tickers} tickers")
    logger.info(f"   Success Rate: {(successful_fetches/total_tickers*100):.1f}%")
    
    return successful_fetches > 0

if __name__ == "__main__":
    job_name = "fetch_30min"
    update_scheduler_status(job_name, "Running")
    
    try:
        success = fetch_30min_data()
        if success:
            update_scheduler_status(job_name, "Success")
            logger.info("✅ 30-minute data fetch completed successfully")
        else:
            update_scheduler_status(job_name, "Fail", "No tickers processed successfully")
            logger.error("❌ 30-minute data fetch failed")
    except Exception as e:
        error_message = f"An unexpected error occurred: {e}"
        logger.error(error_message)
        update_scheduler_status(job_name, "Fail", error_message)