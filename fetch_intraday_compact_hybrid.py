#!/usr/bin/env python3
"""
Hybrid Compact Intraday Fetcher - BEST OF BOTH WORLDS

This combines the timezone fixes from fetch_intraday_compact.py with
the robust error handling and structure from jobs/compact_update.py,
while using the correct TIME_SERIES_INTRADAY API endpoint.

Key Features:
- Uses TIME_SERIES_INTRADAY with outputsize='compact' (real intraday data)
- Fixed timezone handling (no more tz-aware vs tz-naive comparison errors)
- Robust error handling and health checks
- Smart append logic that only adds new candles
- Proper today's data detection
- 7-day rolling window that preserves today's data
"""

import pandas as pd
import sys
import os
from datetime import datetime, timedelta
import time
import logging
import pytz

# Add project root to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '.')))

from utils.helpers import (
    read_master_tickerlist, save_df_to_s3, read_df_from_s3, update_scheduler_status,
    is_today_present, detect_market_session
)
from utils.alpha_vantage_api import get_intraday_data
from utils.config import ALPHA_VANTAGE_API_KEY, TIMEZONE

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def append_new_candles_smart_fixed(existing_df, new_df):
    """
    FIXED: Smart append function that only adds truly new candles.
    Now with proper timezone handling.
    """
    if existing_df.empty:
        return new_df
    
    if new_df.empty:
        return existing_df
    
    try:
        # Ensure timestamp columns exist and are datetime
        timestamp_col = 'timestamp' if 'timestamp' in new_df.columns else 'Date'
        
        # Convert to datetime with timezone awareness
        ny_tz = pytz.timezone(TIMEZONE)
        
        existing_df[timestamp_col] = pd.to_datetime(existing_df[timestamp_col])
        new_df[timestamp_col] = pd.to_datetime(new_df[timestamp_col])
        
        # Ensure both are timezone-aware for proper comparison
        if existing_df[timestamp_col].dt.tz is None:
            existing_df[timestamp_col] = existing_df[timestamp_col].dt.tz_localize(ny_tz)
        
        if new_df[timestamp_col].dt.tz is None:
            new_df[timestamp_col] = new_df[timestamp_col].dt.tz_localize(ny_tz)
        
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

def apply_7day_filter_fixed(df):
    """
    FIXED: Apply 7-day rolling window filter with proper timezone handling.
    This ensures today's data is preserved.
    """
    if df.empty:
        return df
    
    try:
        ny_tz = pytz.timezone(TIMEZONE)
        
        # CRITICAL FIX: Use timezone-aware datetime for proper comparison
        seven_days_ago = datetime.now(ny_tz) - timedelta(days=7)
        timestamp_col = 'timestamp' if 'timestamp' in df.columns else 'Date'
        
        df[timestamp_col] = pd.to_datetime(df[timestamp_col])
        
        # Ensure timestamps are timezone-aware for proper comparison
        if df[timestamp_col].dt.tz is None:
            # If naive, localize to NY timezone first
            df[timestamp_col] = df[timestamp_col].dt.tz_localize(ny_tz)
        elif df[timestamp_col].dt.tz != ny_tz:
            # If different timezone, convert to NY timezone
            df[timestamp_col] = df[timestamp_col].dt.tz_convert(ny_tz)
        
        # Now we can safely compare timezone-aware datetimes
        filtered_df = df[df[timestamp_col] >= seven_days_ago]
        
        # Verify today's data is preserved
        today_count = len(filtered_df[filtered_df[timestamp_col].dt.date == datetime.now(ny_tz).date()])
        logger.info(f"7-day filter applied: {len(df)} -> {len(filtered_df)} rows, today's data: {today_count} rows")
        
        return filtered_df
        
    except Exception as e:
        logger.error(f"Error in 7-day filter: {e}")
        logger.warning("Returning original data to avoid data loss")
        return df

def check_ticker_data_health(ticker):
    """Check if ticker has healthy data, trigger self-healing if needed."""
    try:
        file_path = f'data/intraday/{ticker}_1min.csv'
        existing_df = read_df_from_s3(file_path)
        
        if existing_df.empty or len(existing_df) < 100:
            logger.warning(f"⚠️ {ticker} has insufficient data ({len(existing_df)} rows), needs refresh")
            return False
        
        # Check if we have recent data (within last 2 hours)
        ny_tz = pytz.timezone(TIMEZONE)
        two_hours_ago = datetime.now(ny_tz) - timedelta(hours=2)
        
        timestamp_col = 'timestamp' if 'timestamp' in existing_df.columns else 'Date'
        existing_df[timestamp_col] = pd.to_datetime(existing_df[timestamp_col])
        
        if existing_df[timestamp_col].dt.tz is None:
            existing_df[timestamp_col] = existing_df[timestamp_col].dt.tz_localize(ny_tz)
        
        recent_data = existing_df[existing_df[timestamp_col] >= two_hours_ago]
        
        if recent_data.empty:
            logger.warning(f"⚠️ {ticker} has no recent data (last 2 hours)")
            return False
        
        logger.debug(f"✅ {ticker} data health check passed")
        return True
        
    except Exception as e:
        logger.warning(f"⚠️ {ticker} health check failed: {e}")
        return False

def fetch_intraday_compact_hybrid():
    """
    HYBRID: Fetch today's intraday data using the correct TIME_SERIES_INTRADAY API
    with all timezone fixes and robust error handling.
    """
    logger.info("🚀 Starting Hybrid Compact Intraday Fetch Job")
    
    # Check API key
    if not ALPHA_VANTAGE_API_KEY:
        logger.error("❌ ALPHA_VANTAGE_API_KEY not configured")
        return False
    
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
        logger.info(f"🔄 Processing {ticker}")

        try:
            # Health check
            if not check_ticker_data_health(ticker):
                logger.info(f"ℹ️ {ticker} needs data refresh - fetching full compact data")
            
            # Get existing data
            file_path = f'data/intraday/{ticker}_1min.csv'
            existing_df = read_df_from_s3(file_path)
            
            # Fetch latest compact data (TIME_SERIES_INTRADAY with outputsize=compact)
            logger.debug(f"📥 Fetching compact intraday data for {ticker}...")
            latest_df = get_intraday_data(ticker, interval='1min', outputsize='compact')
            
            if not latest_df.empty:
                logger.info(f"📊 Received {len(latest_df)} rows of compact data for {ticker}")
                
                # Normalize column names if needed
                if 'timestamp' not in latest_df.columns and len(latest_df.columns) >= 6:
                    latest_df.columns = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
                
                # Smart append with FIXED timezone handling
                combined_df = append_new_candles_smart_fixed(existing_df, latest_df)
                
                # Apply FIXED 7-day filter that preserves today's data
                combined_df = apply_7day_filter_fixed(combined_df)
                
                # Save updated data
                upload_success = save_df_to_s3(combined_df, file_path)
                
                if upload_success:
                    successful_fetches += 1
                    new_candles_count = len(combined_df) - len(existing_df) if not existing_df.empty else len(combined_df)
                    if new_candles_count > 0:
                        new_candles_added += new_candles_count
                    
                    # Verify today's data is present
                    today_present = is_today_present(combined_df, 'timestamp')
                    logger.info(f"✅ {ticker}: Updated with {len(combined_df)} total rows, today's data: {today_present}")
                else:
                    logger.error(f"❌ {ticker}: Failed to upload to Spaces")
            else:
                logger.debug(f"⚠️ {ticker}: No new data from API")
                successful_fetches += 1  # Not an error if no new data
                
        except Exception as e:
            logger.error(f"❌ {ticker}: Error processing - {e}")
        
        # Rate limiting
        time.sleep(0.5)

    logger.info(f"📋 Hybrid Compact Fetch Job Completed")
    logger.info(f"   Processed: {successful_fetches}/{total_tickers} tickers")
    logger.info(f"   New candles added: {new_candles_added}")
    logger.info(f"   Market session: {market_session}")
    
    return successful_fetches > 0

if __name__ == "__main__":
    job_name = "fetch_intraday_compact_hybrid"
    update_scheduler_status(job_name, "Running")
    
    try:
        success = fetch_intraday_compact_hybrid()
        if success:
            update_scheduler_status(job_name, "Success")
            logger.info("✅ Hybrid compact fetch completed successfully")
        else:
            update_scheduler_status(job_name, "Fail", "No tickers processed successfully")
            logger.error("❌ Hybrid compact fetch failed")
    except Exception as e:
        error_message = f"An unexpected error occurred: {e}"
        logger.error(error_message)
        update_scheduler_status(job_name, "Fail", error_message)
