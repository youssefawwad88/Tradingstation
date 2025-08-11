#!/usr/bin/env python3
"""
Live Update Engine - Real-time Intraday Updates
===============================================

This script keeps intraday data up-to-date in real-time during market hours.
Implements the two-layer data management model as specified:

1. Reads master watchlist from master_tickerlist.csv (SINGLE SOURCE OF TRUTH)
2. Fetches compact data (latest 100 candles) for 1-min and 30-min timeframes
3. Reads existing data from DigitalOcean Spaces
4. Intelligently appends only new, unique candles (no duplicates)
5. Performs timestamp standardization (America/New_York -> UTC)
6. Saves updated datasets back to Spaces

This is the live update layer that runs frequently during market hours.
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
from utils.alpha_vantage_api import get_intraday_data
from utils.helpers import read_master_tickerlist, save_df_to_s3, read_df_from_s3, update_scheduler_status
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
        data_type (str): Type of data ('30min', '1min')
        
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


def merge_new_candles(existing_df, new_df):
    """
    Intelligently merge new candles with existing data.
    
    Ensures no duplicates by comparing timestamps and only adding truly new candles.
    
    Args:
        existing_df (DataFrame): Current data from storage
        new_df (DataFrame): New data from API
        
    Returns:
        DataFrame: Merged dataframe with new unique candles appended
    """
    if existing_df.empty:
        logger.debug("No existing data - returning all new data")
        return new_df
    
    if new_df.empty:
        logger.debug("No new data - returning existing data unchanged")
        return existing_df
    
    try:
        # Ensure both dataframes have consistent timestamp column
        timestamp_col = 'timestamp'
        
        # Normalize column names if needed
        if 'Date' in existing_df.columns and 'timestamp' not in existing_df.columns:
            existing_df = existing_df.rename(columns={'Date': 'timestamp'})
            logger.debug("Normalized existing data: renamed 'Date' to 'timestamp'")
        if 'Date' in new_df.columns and 'timestamp' not in new_df.columns:
            new_df = new_df.rename(columns={'Date': 'timestamp'})
            logger.debug("Normalized new data: renamed 'Date' to 'timestamp'")
        
        # Validate timestamp column exists
        if timestamp_col not in existing_df.columns:
            logger.error(f"Timestamp column '{timestamp_col}' not found in existing data")
            return existing_df  # Return existing data as fallback
        if timestamp_col not in new_df.columns:
            logger.error(f"Timestamp column '{timestamp_col}' not found in new data")
            return existing_df  # Return existing data as fallback
        
        # Convert timestamps to datetime for comparison
        existing_df[timestamp_col] = pd.to_datetime(existing_df[timestamp_col])
        new_df[timestamp_col] = pd.to_datetime(new_df[timestamp_col])
        
        # Find new candles that don't exist in current data
        existing_timestamps = set(existing_df[timestamp_col])
        new_candles = new_df[~new_df[timestamp_col].isin(existing_timestamps)]
        
        logger.debug(f"Merge analysis: {len(existing_df)} existing, {len(new_df)} new, {len(new_candles)} unique new candles")
        
        if not new_candles.empty:
            # Append new candles to existing data
            merged_df = pd.concat([existing_df, new_candles], ignore_index=True)
            
            # Sort by timestamp (chronological order)
            merged_df = merged_df.sort_values(by=timestamp_col, ascending=True)
            
            # Remove any potential duplicates (safety check)
            original_count = len(merged_df)
            merged_df = merged_df.drop_duplicates(subset=[timestamp_col], keep='last')
            deduplicated_count = len(merged_df)
            
            if original_count != deduplicated_count:
                logger.warning(f"Removed {original_count - deduplicated_count} duplicate timestamps during merge")
            
            logger.info(f"✅ Merge successful: added {len(new_candles)} new candles, total: {len(merged_df)}")
            return merged_df
        else:
            logger.debug("No new candles found to append - data is up to date")
            return existing_df
            
    except Exception as e:
        logger.error(f"❌ Error merging candles: {e}")
        logger.warning("🔄 Returning existing data as fallback to avoid data loss")
        # On error, return existing data to avoid data loss
        return existing_df


def process_ticker_interval(ticker, interval):
    """
    Process updates for a single ticker and interval.
    
    Args:
        ticker (str): Stock ticker symbol
        interval (str): '1min' or '30min'
        
    Returns:
        bool: True if processing successful
    """
    try:
        logger.info(f"📊 Processing {ticker} ({interval})")
        
        # Determine file path based on interval
        if interval == '1min':
            file_path = f'data/intraday/{ticker}_1min.csv'
        elif interval == '30min':
            file_path = f'data/intraday_30min/{ticker}_30min.csv'
        else:
            logger.error(f"❌ Invalid interval: {interval}")
            return False
        
        # Read existing data from Spaces
        logger.debug(f"📂 Reading existing data: {file_path}")
        existing_df = read_df_from_s3(file_path)
        existing_count = len(existing_df) if not existing_df.empty else 0
        logger.debug(f"📊 Existing data for {ticker} ({interval}): {existing_count} rows")
        
        # Fetch latest compact data (100 candles)
        logger.debug(f"🔄 Fetching compact {interval} data for {ticker}...")
        new_df = get_intraday_data(ticker, interval=interval, outputsize='compact')
        
        if new_df.empty:
            logger.warning(f"⚠️ No new data received for {ticker} ({interval}) - API may have failed or market closed")
            # Not necessarily an error - market might be closed or no new data available
            return True  # Consider this a success since it's not a processing failure
        
        new_count = len(new_df)
        logger.info(f"📥 Received {new_count} new candles for {ticker} ({interval})")
        
        # Standardize timestamps for new data
        logger.debug(f"🕐 Standardizing timestamps for {ticker} ({interval})...")
        new_df = standardize_timestamps(new_df, interval)
        logger.debug(f"✅ Timestamps standardized for {ticker} ({interval})")
        
        # Merge with existing data (intelligent deduplication)
        logger.debug(f"🔀 Merging new data with existing for {ticker} ({interval})...")
        merged_df = merge_new_candles(existing_df, new_df)
        
        # Calculate new candles added
        final_count = len(merged_df)
        new_candles_count = final_count - existing_count
        
        if new_candles_count > 0:
            logger.info(f"✅ Added {new_candles_count} new candles for {ticker} ({interval}) (total: {final_count})")
        else:
            logger.debug(f"📊 No new candles for {ticker} ({interval}) - data up to date (total: {final_count})")
        
        # Save updated data back to Spaces
        logger.debug(f"💾 Saving updated data for {ticker} ({interval})...")
        if save_df_to_s3(merged_df, file_path):
            logger.info(f"✅ Saved updated data: {file_path} ({final_count} total rows)")
            return True
        else:
            logger.error(f"❌ Failed to save data for {ticker} ({interval})")
            return False
            
    except Exception as e:
        logger.error(f"❌ Critical error processing {ticker} ({interval}): {e}")
        logger.error(f"❌ Ticker {ticker} ({interval}) processing failed - continuing with next")
        return False


def run_live_updates():
    """
    Execute the live update process.
    
    This is the main function that orchestrates real-time intraday updates:
    1. Read master watchlist
    2. For each ticker, fetch compact data for 1min and 30min
    3. Merge with existing data (intelligent deduplication)  
    4. Standardize timestamps
    5. Save back to DigitalOcean Spaces
    """
    logger.info("=" * 60)
    logger.info("⚡ STARTING LIVE UPDATE ENGINE")
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
    total_operations = len(tickers) * 2  # 1min + 30min for each ticker
    success_count = 0
    failed_operations = []
    ticker_summaries = []
    
    for i, ticker in enumerate(tickers, 1):
        logger.info(f"\n📍 Processing ticker {i}/{len(tickers)}: {ticker}")
        
        # Process 1-minute interval
        logger.debug(f"🔄 Starting 1min processing for {ticker}...")
        success_1min = process_ticker_interval(ticker, '1min')
        if success_1min:
            success_count += 1
            logger.debug(f"✅ 1min processing successful for {ticker}")
        else:
            failed_operations.append(f"{ticker}:1min")
            logger.warning(f"❌ 1min processing failed for {ticker}")
        
        # Process 30-minute interval
        logger.debug(f"🔄 Starting 30min processing for {ticker}...")
        success_30min = process_ticker_interval(ticker, '30min')
        if success_30min:
            success_count += 1
            logger.debug(f"✅ 30min processing successful for {ticker}")
        else:
            failed_operations.append(f"{ticker}:30min")
            logger.warning(f"❌ 30min processing failed for {ticker}")
        
        # Log ticker completion
        overall_success = success_1min and success_30min
        if overall_success:
            status = "🎉 COMPLETE SUCCESS"
            ticker_summaries.append(f"{ticker}: ✅")
        elif success_1min or success_30min:
            status = "⚠️ PARTIAL SUCCESS"
            ticker_summaries.append(f"{ticker}: ⚠️")
        else:
            status = "💥 COMPLETE FAILURE"
            ticker_summaries.append(f"{ticker}: ❌")
            
        logger.info(f"📊 {ticker}: {status} (1min: {'✅' if success_1min else '❌'}, 30min: {'✅' if success_30min else '❌'})")
        
        # Rate limiting - respect API limits
        if i < len(tickers):  # Don't sleep after last ticker
            logger.debug(f"⏳ Rate limiting: sleeping 0.5 seconds before next ticker...")
            time.sleep(0.5)  # Lighter sleep for live updates
    
    # Final summary
    logger.info("\n" + "=" * 60)
    logger.info("📊 LIVE UPDATE SUMMARY")
    logger.info("=" * 60)
    logger.info(f"📋 Total tickers processed: {len(tickers)}")
    logger.info(f"🔢 Total operations: {total_operations}")
    logger.info(f"✅ Successful operations: {success_count}")
    logger.info(f"❌ Failed operations: {len(failed_operations)}")
    
    # Count ticker-level results
    complete_success = ticker_summaries.count(lambda x: "✅" in x)
    partial_success = ticker_summaries.count(lambda x: "⚠️" in x)
    complete_failures = ticker_summaries.count(lambda x: "❌" in x)
    
    # Calculate success by tickers
    complete_success = sum(1 for summary in ticker_summaries if "✅" in summary)
    partial_success = sum(1 for summary in ticker_summaries if "⚠️" in summary)
    complete_failures = sum(1 for summary in ticker_summaries if "❌" in summary)
    
    logger.info(f"🎉 Complete ticker success: {complete_success}")
    logger.info(f"⚠️ Partial ticker success: {partial_success}")
    logger.info(f"💥 Complete ticker failures: {complete_failures}")
    
    if failed_operations:
        logger.warning(f"⚠️ Failed operations: {failed_operations}")
    
    # Show ticker summary
    logger.info(f"📊 Ticker results: {' '.join(ticker_summaries)}")
    
    operation_success_rate = (success_count / total_operations) * 100 if total_operations else 0
    ticker_success_rate = ((complete_success + partial_success) / len(tickers)) * 100 if tickers else 0
    
    logger.info(f"📈 Operation success rate: {operation_success_rate:.1f}% ({success_count}/{total_operations})")
    logger.info(f"🎯 Ticker success rate: {ticker_success_rate:.1f}% ({complete_success + partial_success}/{len(tickers)})")
    
    # Determine overall success
    if complete_success == len(tickers):
        logger.info("🌟 PERFECT LIVE UPDATE - All tickers updated completely!")
        return True
    elif ticker_success_rate >= 80:
        logger.info("🎉 SUCCESSFUL LIVE UPDATE - Most tickers updated!")
        return True
    else:
        logger.error("💥 FAILED LIVE UPDATE - Too many ticker failures")
        return False


if __name__ == "__main__":
    job_name = "update_intraday_compact"
    update_scheduler_status(job_name, "Running")
    
    try:
        success = run_live_updates()
        
        if success:
            update_scheduler_status(job_name, "Success")
            logger.info("✅ Live update job completed successfully")
        else:
            update_scheduler_status(job_name, "Fail", "Too many operation failures")
            logger.error("❌ Live update job failed")
            
    except Exception as e:
        error_message = f"Critical error in live updates: {e}"
        logger.error(error_message)
        update_scheduler_status(job_name, "Fail", error_message)
        sys.exit(1)