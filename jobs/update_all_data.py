#!/usr/bin/env python3
"""
Full Rebuild Engine - Daily Data Refresh
========================================

This script performs a complete daily data refresh for all tickers in the watchlist.
Implements the two-layer data management model as specified:

1. Reads master watchlist from tickerlist.txt
2. Performs full historical data fetch for all three timeframes (daily, 30-min, 1-min)
3. Applies rigorous cleanup and trimming rules:
   - Daily Data: 200 rows (most recent)
   - 30-Minute Data: 500 rows (most recent) 
   - 1-Minute Data: 7 days (most recent)
4. Performs timestamp standardization (America/New_York -> UTC)
5. Saves clean datasets to DigitalOcean Spaces

This is the foundational data layer that runs once per day.
"""

import os
import sys
import pandas as pd
import logging
from datetime import datetime, timedelta
import time
import pytz

# Add project root to Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Import core utilities
from utils.config import ALPHA_VANTAGE_API_KEY, SPACES_BUCKET_NAME, TIMEZONE
from utils.alpha_vantage_api import get_daily_data, get_intraday_data
from utils.helpers import read_master_tickerlist, save_df_to_s3, update_scheduler_status
from utils.timestamp_standardizer import apply_timestamp_standardization_to_api_data

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def standardize_timestamps(df, data_type):
    """
    Apply rigorous timestamp standardization to dataframe.
    
    Process:
    1. Parse timestamps from API data
    2. Localize to America/New_York timezone
    3. Convert to UTC for storage
    
    Args:
        df (DataFrame): Input dataframe with timestamp column
        data_type (str): Type of data ('daily', '30min', '1min')
        
    Returns:
        DataFrame: Dataframe with standardized UTC timestamps
    """
    if df.empty:
        return df
    
    try:
        # Apply the centralized timestamp standardization
        standardized_df = apply_timestamp_standardization_to_api_data(df, data_type=data_type)
        logger.debug(f"Timestamp standardization applied for {data_type} data: {len(standardized_df)} rows")
        return standardized_df
    except Exception as e:
        logger.error(f"Error in timestamp standardization for {data_type}: {e}")
        return df


def trim_data_to_requirements(df, data_type):
    """
    Trim datasets according to specified requirements.
    
    Args:
        df (DataFrame): Input dataframe
        data_type (str): 'daily' (200 rows), '30min' (500 rows), '1min' (7 days)
        
    Returns:
        DataFrame: Trimmed dataframe
    """
    if df.empty:
        return df
    
    try:
        # Sort by timestamp descending (newest first) for proper trimming
        timestamp_col = 'timestamp' if 'timestamp' in df.columns else 'Date'
        df_sorted = df.sort_values(by=timestamp_col, ascending=False)
        
        if data_type == 'daily':
            # Keep most recent 200 rows
            trimmed_df = df_sorted.head(200)
            logger.debug(f"Daily data trimmed to {len(trimmed_df)} rows (target: 200)")
            
        elif data_type == '30min':
            # Keep most recent 500 rows
            trimmed_df = df_sorted.head(500)
            logger.debug(f"30-minute data trimmed to {len(trimmed_df)} rows (target: 500)")
            
        elif data_type == '1min':
            # Keep most recent 7 days
            cutoff_date = datetime.now(pytz.timezone(TIMEZONE)) - timedelta(days=7)
            df_sorted[timestamp_col] = pd.to_datetime(df_sorted[timestamp_col])
            trimmed_df = df_sorted[df_sorted[timestamp_col] >= cutoff_date]
            logger.debug(f"1-minute data trimmed to {len(trimmed_df)} rows (last 7 days)")
            
        else:
            trimmed_df = df_sorted
            
        # Sort back to chronological order (oldest first) for storage
        return trimmed_df.sort_values(by=timestamp_col, ascending=True)
        
    except Exception as e:
        logger.error(f"Error trimming {data_type} data: {e}")
        return df


def fetch_and_process_ticker(ticker):
    """
    Fetch and process all timeframes for a single ticker.
    
    Args:
        ticker (str): Stock ticker symbol
        
    Returns:
        dict: Results for each timeframe
    """
    results = {'daily': None, '30min': None, '1min': None}
    
    logger.info(f"🔄 Processing ticker: {ticker}")
    
    # 1. Daily Data
    try:
        logger.info(f"📈 Fetching daily data for {ticker}...")
        daily_df = get_daily_data(ticker, outputsize='full')
        
        if not daily_df.empty:
            # Apply timestamp standardization
            daily_df = standardize_timestamps(daily_df, 'daily')
            
            # Trim to 200 rows
            daily_df = trim_data_to_requirements(daily_df, 'daily')
            
            results['daily'] = daily_df
            logger.info(f"✅ Daily data processed: {len(daily_df)} rows")
        else:
            logger.warning(f"⚠️ No daily data received for {ticker}")
            
    except Exception as e:
        logger.error(f"❌ Error fetching daily data for {ticker}: {e}")
    
    # 2. 30-Minute Data
    try:
        logger.info(f"📊 Fetching 30-minute data for {ticker}...")
        min_30_df = get_intraday_data(ticker, interval='30min', outputsize='full')
        
        if not min_30_df.empty:
            # Apply timestamp standardization
            min_30_df = standardize_timestamps(min_30_df, '30min')
            
            # Trim to 500 rows
            min_30_df = trim_data_to_requirements(min_30_df, '30min')
            
            results['30min'] = min_30_df
            logger.info(f"✅ 30-minute data processed: {len(min_30_df)} rows")
        else:
            logger.warning(f"⚠️ No 30-minute data received for {ticker}")
            
    except Exception as e:
        logger.error(f"❌ Error fetching 30-minute data for {ticker}: {e}")
    
    # 3. 1-Minute Data
    try:
        logger.info(f"⏱️ Fetching 1-minute data for {ticker}...")
        min_1_df = get_intraday_data(ticker, interval='1min', outputsize='full')
        
        if not min_1_df.empty:
            # Apply timestamp standardization
            min_1_df = standardize_timestamps(min_1_df, '1min')
            
            # Trim to 7 days
            min_1_df = trim_data_to_requirements(min_1_df, '1min')
            
            results['1min'] = min_1_df
            logger.info(f"✅ 1-minute data processed: {len(min_1_df)} rows")
        else:
            logger.warning(f"⚠️ No 1-minute data received for {ticker}")
            
    except Exception as e:
        logger.error(f"❌ Error fetching 1-minute data for {ticker}: {e}")
    
    return results


def save_ticker_data(ticker, results):
    """
    Save processed data to DigitalOcean Spaces.
    
    Args:
        ticker (str): Stock ticker symbol
        results (dict): Processed data for each timeframe
        
    Returns:
        bool: True if all saves successful
    """
    save_success = True
    
    # Save daily data
    if results['daily'] is not None and not results['daily'].empty:
        daily_path = f'data/daily/{ticker}_daily.csv'
        if save_df_to_s3(results['daily'], daily_path):
            logger.info(f"✅ Saved daily data: {daily_path}")
        else:
            logger.error(f"❌ Failed to save daily data for {ticker}")
            save_success = False
    
    # Save 30-minute data
    if results['30min'] is not None and not results['30min'].empty:
        min_30_path = f'data/intraday_30min/{ticker}_30min.csv'
        if save_df_to_s3(results['30min'], min_30_path):
            logger.info(f"✅ Saved 30-minute data: {min_30_path}")
        else:
            logger.error(f"❌ Failed to save 30-minute data for {ticker}")
            save_success = False
    
    # Save 1-minute data
    if results['1min'] is not None and not results['1min'].empty:
        min_1_path = f'data/intraday/{ticker}_1min.csv'
        if save_df_to_s3(results['1min'], min_1_path):
            logger.info(f"✅ Saved 1-minute data: {min_1_path}")
        else:
            logger.error(f"❌ Failed to save 1-minute data for {ticker}")
            save_success = False
    
    return save_success


def run_full_rebuild():
    """
    Execute the full rebuild process.
    
    This is the main function that orchestrates the complete daily data refresh:
    1. Read master watchlist
    2. Fetch full historical data for all timeframes
    3. Apply cleanup and trimming rules
    4. Standardize timestamps
    5. Save to DigitalOcean Spaces
    """
    logger.info("=" * 60)
    logger.info("🚀 STARTING FULL REBUILD ENGINE")
    logger.info("=" * 60)
    
    # Check environment setup
    if not ALPHA_VANTAGE_API_KEY:
        logger.error("❌ ALPHA_VANTAGE_API_KEY not configured")
        return False
    
    if not SPACES_BUCKET_NAME:
        logger.warning("⚠️ DigitalOcean Spaces not configured - using local storage only")
    
    # Read master watchlist
    tickers = read_master_tickerlist()
    if not tickers:
        logger.error("❌ No tickers found in master watchlist")
        return False
    
    logger.info(f"📋 Processing {len(tickers)} tickers: {tickers}")
    
    # Track progress
    processed_count = 0
    success_count = 0
    failed_tickers = []
    
    for i, ticker in enumerate(tickers, 1):
        logger.info(f"\n📍 Processing ticker {i}/{len(tickers)}: {ticker}")
        
        try:
            # Fetch and process all timeframes
            results = fetch_and_process_ticker(ticker)
            
            # Save processed data
            if save_ticker_data(ticker, results):
                success_count += 1
                logger.info(f"✅ Successfully processed {ticker}")
            else:
                failed_tickers.append(ticker)
                logger.error(f"❌ Failed to save data for {ticker}")
            
            processed_count += 1
            
            # Rate limiting - respect API limits
            if i < len(tickers):  # Don't sleep after last ticker
                time.sleep(1)
                
        except Exception as e:
            logger.error(f"❌ Critical error processing {ticker}: {e}")
            failed_tickers.append(ticker)
            processed_count += 1
    
    # Final summary
    logger.info("\n" + "=" * 60)
    logger.info("📊 FULL REBUILD SUMMARY")
    logger.info("=" * 60)
    logger.info(f"📋 Total tickers: {len(tickers)}")
    logger.info(f"✅ Successfully processed: {success_count}")
    logger.info(f"❌ Failed: {len(failed_tickers)}")
    
    if failed_tickers:
        logger.warning(f"⚠️ Failed tickers: {failed_tickers}")
    
    success_rate = (success_count / len(tickers)) * 100 if tickers else 0
    logger.info(f"📈 Success rate: {success_rate:.1f}%")
    
    if success_rate >= 80:
        logger.info("🎉 Full rebuild completed successfully!")
        return True
    else:
        logger.error("💥 Full rebuild failed - too many errors")
        return False


if __name__ == "__main__":
    job_name = "update_all_data"
    update_scheduler_status(job_name, "Running")
    
    try:
        success = run_full_rebuild()
        
        if success:
            update_scheduler_status(job_name, "Success")
            logger.info("✅ Full rebuild job completed successfully")
        else:
            update_scheduler_status(job_name, "Fail", "Too many ticker processing failures")
            logger.error("❌ Full rebuild job failed")
            
    except Exception as e:
        error_message = f"Critical error in full rebuild: {e}"
        logger.error(error_message)
        update_scheduler_status(job_name, "Fail", error_message)
        sys.exit(1)