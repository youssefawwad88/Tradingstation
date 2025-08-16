#!/usr/bin/env python3
"""
Intraday Compact Fetcher (Every Minute)

Fetches only today's 1-minute data and appends new candles only.
Used for live price action and setup monitoring.
Reads from master_tickerlist.csv.
"""

import pandas as pd
import sys
import os
from datetime import datetime, timedelta
import time
import logging
import pytz

# Add project root to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from utils.helpers import (
    read_master_tickerlist, save_df_to_s3, read_df_from_s3, update_scheduler_status,
    is_today_present, detect_market_session
)
from utils.alpha_vantage_api import get_intraday_data

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
        timestamp_col = 'timestamp' if 'timestamp' in new_df.columns else 'Date'
        existing_df[timestamp_col] = pd.to_datetime(existing_df[timestamp_col])
        new_df[timestamp_col] = pd.to_datetime(new_df[timestamp_col])
        
        # Get the latest timestamp in existing data
        latest_existing = existing_df[timestamp_col].max()
        
        # Only keep new data that's newer than existing
        truly_new = new_df[new_df[timestamp_col] > latest_existing]
        
        if not truly_new.empty:
            combined = pd.concat([existing_df, truly_new], ignore_index=True)
            combined = combined.drop_duplicates(subset=[timestamp_col], keep='last')
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
    Fetch today's 1-minute data for all tickers in master_tickerlist.csv.
    Appends only new candles to existing data.
    """
    logger.info("🚀 Starting Intraday Compact Fetch Job (Every Minute)")
    
    # Check API key availability
    from utils.config import ALPHA_VANTAGE_API_KEY, SPACES_BUCKET_NAME
    if not ALPHA_VANTAGE_API_KEY:
        logger.warning("⚠️ ALPHA_VANTAGE_API_KEY not configured")
        logger.warning("💡 Running in TEST MODE - no new data will be fetched")
        logger.warning("🔧 Set ALPHA_VANTAGE_API_KEY environment variable to enable data fetching")
        logger.warning("📝 For production use, ensure API credentials are properly configured")
        # Don't return False here - let it continue for weekend testing
    
    if not SPACES_BUCKET_NAME:
        logger.warning("⚠️ DigitalOcean Spaces not configured - using local storage only")
        logger.warning("💡 CSV files will be saved locally but NOT uploaded to cloud storage")
        logger.warning("🔧 Set SPACES credentials to enable cloud storage uploads")
    
    # Load tickers from master_tickerlist.csv
    tickers = read_master_tickerlist()
    
    if not tickers:
        logger.error("❌ No tickers to process. Exiting.")
        return False

    logger.info(f"📊 Processing {len(tickers)} tickers from master_tickerlist.csv")
    
    # Check market session
    market_session = detect_market_session()
    logger.info(f"🕐 Current market session: {market_session}")

    successful_fetches = 0
    new_candles_added = 0
    total_tickers = len(tickers)

    for ticker in tickers:
        logger.debug(f"🔄 Processing {ticker}")

        try:
            # Get existing data
            file_path = f'data/intraday/{ticker}_1min.csv'
            existing_df = read_df_from_s3(file_path)
            
            # Fetch latest compact data (today's data)
            latest_df = get_intraday_data(ticker, interval='1min', outputsize='compact')
            
            if not latest_df.empty:
                # Normalize column names if needed
                if 'timestamp' not in latest_df.columns and len(latest_df.columns) >= 6:
                    latest_df.columns = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
                
                # Combine with existing data
                combined_df = append_new_candles_smart(existing_df, latest_df)
                
                # Keep only last 7 days of data (rolling window)
                # CRITICAL FIX: Use timezone-aware datetime for proper comparison
                ny_tz = pytz.timezone('America/New_York')
                seven_days_ago = datetime.now(ny_tz) - timedelta(days=7)
                timestamp_col = 'timestamp' if 'timestamp' in combined_df.columns else 'Date'
                combined_df[timestamp_col] = pd.to_datetime(combined_df[timestamp_col])
                
                # Ensure timestamps are timezone-aware for proper comparison
                if combined_df[timestamp_col].dt.tz is None:
                    # If naive, localize to NY timezone first
                    combined_df[timestamp_col] = combined_df[timestamp_col].dt.tz_localize(ny_tz)
                elif combined_df[timestamp_col].dt.tz != ny_tz:
                    # If different timezone, convert to NY timezone
                    combined_df[timestamp_col] = combined_df[timestamp_col].dt.tz_convert(ny_tz)
                
                # Now we can safely compare timezone-aware datetimes
                combined_df = combined_df[combined_df[timestamp_col] >= seven_days_ago]
                
                # Save updated data
                upload_success = save_df_to_s3(combined_df, file_path)
                
                if upload_success:
                    successful_fetches += 1
                    new_candles_count = len(combined_df) - len(existing_df) if not existing_df.empty else len(combined_df)
                    if new_candles_count > 0:
                        new_candles_added += new_candles_count
                    logger.debug(f"✅ {ticker}: Updated with {len(combined_df)} total rows")
                else:
                    logger.error(f"❌ {ticker}: Failed to upload to Spaces")
            else:
                logger.debug(f"⚠️ {ticker}: No new data from API")
                successful_fetches += 1  # Not an error if no new data
                
        except Exception as e:
            logger.error(f"❌ {ticker}: Error processing - {e}")
        
        # Small delay to respect API limits (can be faster for compact fetches)
        time.sleep(0.5)

    logger.info(f"📋 Intraday Compact Fetch Job Completed")
    logger.info(f"   Processed: {successful_fetches}/{total_tickers} tickers")
    logger.info(f"   New candles added: {new_candles_added}")
    logger.info(f"   Market session: {market_session}")
    
    return successful_fetches > 0

if __name__ == "__main__":
    job_name = "fetch_intraday_compact"
    update_scheduler_status(job_name, "Running")
    
    try:
        success = fetch_intraday_compact()
        if success:
            update_scheduler_status(job_name, "Success")
            logger.info("✅ Intraday compact fetch completed successfully")
        else:
            update_scheduler_status(job_name, "Fail", "No tickers processed successfully")
            logger.error("❌ Intraday compact fetch failed")
    except Exception as e:
        error_message = f"An unexpected error occurred: {e}"
        logger.error(error_message)
        update_scheduler_status(job_name, "Fail", error_message)