#!/usr/bin/env python3
"""
Compact Update Engine - Real-time Data Updates
==============================================

This script keeps intraday data up-to-date in real-time during market hours.
UPDATED: Now uses GLOBAL_QUOTE endpoint for true real-time data fetching.

Implements the requirements specified in the problem statement:

1. Reads entire ticker column from master_tickerlist.csv (SINGLE SOURCE OF TRUTH)
2. Loops through EVERY single ticker (fixes incomplete ticker processing)
3. Fetches real-time data using GLOBAL_QUOTE endpoint (latest live quote)
4. Reads existing data from DigitalOcean Spaces
5. Intelligently merges new real-time data with existing files, appending only new candles
6. Performs mandatory timestamp standardization (America/New_York -> UTC)
7. Saves updated datasets back to Spaces

This is the live update layer that runs frequently during market hours AFTER full fetch completes.
Now uses the correct real-time API endpoint to fix the bug where current day's data was not being fetched.
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
from utils.alpha_vantage_api import get_real_time_price
from utils.helpers import read_master_tickerlist, save_df_to_s3, read_df_from_s3, update_scheduler_status
from utils.timestamp_standardizer import apply_timestamp_standardization_to_api_data

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def convert_global_quote_to_dataframe(quote_data, ticker):
    """
    Convert GLOBAL_QUOTE API response to DataFrame format matching existing data structure.
    
    Args:
        quote_data (dict): Global quote data from Alpha Vantage API
        ticker (str): Stock ticker symbol
        
    Returns:
        DataFrame: Single-row DataFrame with current real-time data
    """
    if not quote_data:
        return pd.DataFrame()
    
    try:
        # Create DataFrame with structure matching existing historical data
        df = pd.DataFrame({
            'timestamp': [quote_data['latest_trading_day']],
            'open': [quote_data['open']],
            'high': [quote_data['high']], 
            'low': [quote_data['low']],
            'close': [quote_data['price']],  # Use current price as close
            'volume': [quote_data['volume']]
        })
        
        logger.debug(f"Converted GLOBAL_QUOTE to DataFrame for {ticker}: {len(df)} row")
        return df
        
    except Exception as e:
        logger.error(f"Error converting GLOBAL_QUOTE data to DataFrame for {ticker}: {e}")
        return pd.DataFrame()


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
    Intelligently merge new candles with existing data with enhanced transparency logging.
    
    Ensures no duplicates by comparing timestamps and only adding truly new candles.
    This is critical for correctly updating the current day's files.
    
    Args:
        existing_df (DataFrame): Current data from storage
        new_df (DataFrame): New data from API
        
    Returns:
        DataFrame: Merged dataframe with new unique candles appended
    """
    if existing_df.empty:
        logger.info("📊 Merge analysis: No existing data - returning all new data")
        logger.info(f"   Original: 0 rows, New: {len(new_df)} rows, Final: {len(new_df)} rows")
        return new_df
    
    if new_df.empty:
        logger.info("📊 Merge analysis: No new data - returning existing data unchanged")
        logger.info(f"   Original: {len(existing_df)} rows, New: 0 rows, Final: {len(existing_df)} rows")
        return existing_df
    
    try:
        # Enhanced logging: Show initial counts
        original_count = len(existing_df)
        new_count = len(new_df)
        logger.info(f"📊 Merge analysis: Starting with {original_count} existing rows, {new_count} new rows")
        
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
        
        # ROBUST 4-STEP MERGE PROCESS (as per problem statement requirements):
        
        # Step 1: Combine - Concatenate both DataFrames into one
        logger.debug("Step 1: Combining existing and new DataFrames")
        combined_df = pd.concat([existing_df, new_df], ignore_index=True)
        combined_count = len(combined_df)
        logger.debug(f"   Combined DataFrame: {combined_count} rows")
        
        # Step 2: Sort - Sort the combined DataFrame by timestamp, from oldest to newest
        logger.debug("Step 2: Sorting combined DataFrame by timestamp (oldest to newest)")
        combined_df = combined_df.sort_values(by=timestamp_col, ascending=True)
        
        # Step 3: Deduplicate - Remove duplicate rows based on timestamp, keeping the last entry
        logger.debug("Step 3: Deduplicating by timestamp, keeping last entry")
        pre_dedup_count = len(combined_df)
        merged_df = combined_df.drop_duplicates(subset=[timestamp_col], keep='last')
        final_count = len(merged_df)
        
        # Calculate how many unique new rows were actually added
        unique_new_count = final_count - original_count
        duplicates_removed = pre_dedup_count - final_count
        
        # Step 4: Log the Result - Crystal clear logging about the merge analysis
        logger.info(f"📊 Merge analysis:")
        logger.info(f"   • Original file: {original_count} rows")
        logger.info(f"   • Fetched from API: {new_count} rows")
        logger.info(f"   • Combined total: {combined_count} rows")
        logger.info(f"   • Duplicates removed: {duplicates_removed} rows")
        logger.info(f"   • Unique new rows added: {unique_new_count} rows")
        logger.info(f"   • Final result: {final_count} rows")
        
        if unique_new_count > 0:
            logger.info(f"✅ Merge successful: Added {unique_new_count} new candles")
            logger.info(f"   Original: {original_count} rows → New: {new_count} rows → Final: {final_count} rows")
            return merged_df
        else:
            logger.info("ℹ️ No new candles found to append - data is current and up to date")
            logger.info(f"   Original: {original_count} rows → New: {new_count} rows → Final: {original_count} rows (no changes)")
            return existing_df
            
    except Exception as e:
        logger.error(f"❌ Error merging candles: {e}")
        logger.warning(f"🔄 Returning existing data as fallback to avoid data loss ({len(existing_df)} rows)")
        # On error, return existing data to avoid data loss
        return existing_df


def process_ticker_realtime(ticker):
    """
    Process real-time updates for a single ticker using GLOBAL_QUOTE endpoint.
    
    New real-time logic as per requirements:
    1. Call GLOBAL_QUOTE endpoint for the ticker
    2. Transform single quote into one-row DataFrame matching existing structure  
    3. Load existing data file from DigitalOcean Spaces
    4. Append new one-row DataFrame to end of existing data
    5. Save updated DataFrame back to DigitalOcean Spaces
    
    Args:
        ticker (str): Stock ticker symbol
        
    Returns:
        bool: True if processing successful
    """
    try:
        logger.info(f"📊 Processing real-time data for {ticker}")
        
        # Step 1: Call GLOBAL_QUOTE endpoint for the ticker
        logging.info(f"[{ticker}] Fetching real-time quote...")
        logger.debug(f"🔄 Fetching GLOBAL_QUOTE data for {ticker}...")
        quote_data = get_real_time_price(ticker)
        
        if not quote_data:
            logging.info(f"[{ticker}] API returned no real-time data. Skipping.")
            logger.warning(f"⚠️ No real-time data received for {ticker} - API may have failed or market closed")
            # Not necessarily an error - market might be closed or no new data available
            return True  # Consider this a success since it's not a processing failure
        
        logging.info(f"[{ticker}] Real-time quote received.")
        logger.info(f"📥 Received real-time quote for {ticker}")
        
        # Step 2: Transform single quote into one-row DataFrame matching existing structure
        logger.debug(f"🔄 Converting GLOBAL_QUOTE to DataFrame for {ticker}...")
        new_df = convert_global_quote_to_dataframe(quote_data, ticker)
        
        if new_df.empty:
            logger.error(f"❌ Failed to convert GLOBAL_QUOTE data to DataFrame for {ticker}")
            return False
        
        logger.debug(f"✅ Real-time data converted to DataFrame for {ticker}: {len(new_df)} row")
        
        # Standardize timestamps for new data
        logger.debug(f"🕐 Standardizing timestamps for {ticker}...")
        new_df = standardize_timestamps(new_df, '1min')  # Use 1min as data type for real-time
        logger.debug(f"✅ Timestamps standardized for {ticker}")
        
        # Process for 1-minute data file (most granular real-time updates)
        file_path = f'data/intraday/{ticker}_1min.csv'
        
        # Step 3: Load existing data file from DigitalOcean Spaces
        logger.debug(f"📂 Reading existing 1min data: {file_path}")
        existing_df = read_df_from_s3(file_path)
        existing_count = len(existing_df) if not existing_df.empty else 0
        logging.info(f"[{ticker}] Loaded existing 1min file... It has {existing_count} rows.")
        logger.debug(f"📊 Existing 1min data for {ticker}: {existing_count} rows")
        
        # Step 4: Append new one-row DataFrame to end of existing data  
        logger.debug(f"🔀 Merging new real-time data with existing for {ticker}...")
        merged_df = merge_new_candles(existing_df, new_df)
        logging.info(f"[{ticker}] Merge complete. Combined DataFrame now has {len(merged_df)} rows.")
        
        # Calculate new candles added
        final_count = len(merged_df)
        new_candles_count = final_count - existing_count
        
        if new_candles_count > 0:
            logging.info(f"[{ticker}] Real-time data has changed. Preparing to write...")
            # Required logging format: "INFO: Merge successful: Added X new candles for TICKER (real-time)"
            logger.info(f"INFO: Merge successful: Added {new_candles_count} new candles for {ticker} (real-time)")
        else:
            logging.info(f"[{ticker}] No real-time data change detected... Skipping cloud write.")
            logger.debug(f"📊 No new real-time candles for {ticker} - data up to date (total: {final_count})")
            return True  # Skip writing if no new data
        
        # Step 5: Save updated DataFrame back to DigitalOcean Spaces
        logger.info(f"💾 Preparing to save updated real-time data for {ticker}...")
        
        # Enhanced logging: Show exact data being saved
        data_size = len(merged_df.to_csv(index=False).encode('utf-8'))
        logger.info(f"   Data size: {data_size} bytes, Total rows: {final_count}")
        
        if save_df_to_s3(merged_df, file_path):
            # Enhanced logging: Confirm successful write with details
            logger.info(f"✅ Successfully wrote {data_size} bytes to S3 path: {file_path}")
            logger.info(f"   Updated real-time data for {ticker}: {final_count} total rows saved")
            return True
        else:
            logger.error(f"❌ Failed to save real-time data for {ticker} to path: {file_path}")
            return False
            
    except Exception as e:
        logger.error(f"❌ Critical error processing real-time data for {ticker}: {e}")
        logger.error(f"❌ Ticker {ticker} real-time processing failed - continuing with next")
        return False


def run_compact_update():
    """
    Execute the compact update process.
    
    This is the main function that orchestrates real-time intraday updates:
    1. Check if we're within extended trading hours (4:00 AM - 8:00 PM ET)
    2. Read master watchlist (ALL tickers)
    3. For each ticker, fetch compact data for 1min and 30min
    4. Merge with existing data (intelligent deduplication)  
    5. Standardize timestamps
    6. Save back to DigitalOcean Spaces
    """
    print("!!!! DEPLOYMENT TEST v5: compact_update IS RUNNING NEW CODE !!!!")
    logging.info("--- COMPACT UPDATE JOB STARTING ---")
    logger.info("=" * 60)
    logger.info("⚡ STARTING COMPACT UPDATE ENGINE")
    logger.info("=" * 60)
    
    # Check if we're within extended trading hours (4:00 AM - 8:00 PM ET)
    now_utc = datetime.now(pytz.utc)
    eastern = pytz.timezone('America/New_York')
    now_eastern = now_utc.astimezone(eastern)
    
    # Define trading window (4:00 AM to 8:00 PM ET)
    trading_start = now_eastern.replace(hour=4, minute=0, second=0, microsecond=0)
    trading_end = now_eastern.replace(hour=20, minute=0, second=0, microsecond=0)
    
    if not (trading_start <= now_eastern <= trading_end):
        logger.info(f"INFO: Outside of all trading hours (4:00 AM - 8:00 PM ET). Current time: {now_eastern.strftime('%H:%M:%S ET')}. Skipping real-time update.")
        sys.exit(0)
    
    logging.info(f"Market hours check PASSED...")
    logger.info(f"✅ Within trading hours ({now_eastern.strftime('%H:%M:%S ET')}). Proceeding with real-time update.")
    
    # Check environment setup
    if not ALPHA_VANTAGE_API_KEY:
        logger.error("❌ ALPHA_VANTAGE_API_KEY not configured")
        return False
    
    if not SPACES_BUCKET_NAME:
        logger.warning("⚠️ DigitalOcean Spaces not configured - using local storage only")
    
    # Read master watchlist - CRITICAL: Read entire ticker column
    tickers = read_master_tickerlist()
    if not tickers:
        logger.error("❌ No tickers found in master watchlist")
        return False
    
    logger.info(f"📋 Processing {len(tickers)} tickers from master_tickerlist.csv: {tickers}")
    logger.info("🔄 This engine will process EVERY single ticker to fix incomplete processing")
    
    # Track progress
    total_operations = len(tickers)  # One real-time operation per ticker
    success_count = 0
    failed_operations = []
    ticker_summaries = []
    
    # CRITICAL: Loop through EVERY ticker (fixes incomplete ticker processing)
    for i, ticker in enumerate(tickers, 1):
        logging.info(f"--- Processing Ticker: {ticker} ---")
        logger.info(f"\n📍 Processing ticker {i}/{len(tickers)}: {ticker}")
        
        # Process real-time data using GLOBAL_QUOTE endpoint
        logger.debug(f"🔄 Starting real-time processing for {ticker}...")
        success_realtime = process_ticker_realtime(ticker)
        if success_realtime:
            success_count += 1
            logger.debug(f"✅ Real-time processing successful for {ticker}")
            status = "🎉 COMPLETE SUCCESS"
            ticker_summaries.append(f"{ticker}: ✅")
        else:
            failed_operations.append(f"{ticker}:realtime")
            logger.warning(f"❌ Real-time processing failed for {ticker}")
            status = "💥 COMPLETE FAILURE"
            ticker_summaries.append(f"{ticker}: ❌")
            
        logger.info(f"📊 {ticker}: {status} (real-time: {'✅' if success_realtime else '❌'})")
        
        # Rate limiting - respect API limits
        if i < len(tickers):  # Don't sleep after last ticker
            logger.debug(f"⏳ Rate limiting: sleeping 0.5 seconds before next ticker...")
            time.sleep(0.5)  # Lighter sleep for live updates
    
    # Final summary
    logger.info("\n" + "=" * 60)
    logger.info("📊 COMPACT UPDATE ENGINE SUMMARY")
    logger.info("=" * 60)
    logger.info(f"📋 Total tickers processed: {len(tickers)}")
    logger.info(f"🔢 Total operations: {total_operations}")
    logger.info(f"✅ Successful operations: {success_count}")
    logger.info(f"❌ Failed operations: {len(failed_operations)}")
    
    # Calculate ticker-level results
    complete_success = sum(1 for summary in ticker_summaries if "✅" in summary)
    complete_failures = sum(1 for summary in ticker_summaries if "❌" in summary)
    
    logger.info(f"🎉 Complete ticker success: {complete_success}")
    logger.info(f"💥 Complete ticker failures: {complete_failures}")
    
    if failed_operations:
        logger.warning(f"⚠️ Failed operations: {failed_operations}")
    
    # Show ticker summary
    logger.info(f"📊 Ticker results: {' '.join(ticker_summaries)}")
    
    operation_success_rate = (success_count / total_operations) * 100 if total_operations else 0
    ticker_success_rate = (complete_success / len(tickers)) * 100 if tickers else 0
    
    logger.info(f"📈 Operation success rate: {operation_success_rate:.1f}% ({success_count}/{total_operations})")
    logger.info(f"🎯 Ticker success rate: {ticker_success_rate:.1f}% ({complete_success}/{len(tickers)})")
    
    # Determine overall success
    if complete_success == len(tickers):
        logger.info("🌟 PERFECT COMPACT UPDATE - All tickers updated completely!")
        logging.info("--- COMPACT UPDATE JOB FINISHED ---")
        return True
    elif ticker_success_rate >= 80:
        logger.info("🎉 SUCCESSFUL COMPACT UPDATE - Most tickers updated!")
        logging.info("--- COMPACT UPDATE JOB FINISHED ---")
        return True
    else:
        logger.error("💥 FAILED COMPACT UPDATE - Too many ticker failures")
        logging.info("--- COMPACT UPDATE JOB FINISHED ---")
        return False


if __name__ == "__main__":
    job_name = "compact_update"
    update_scheduler_status(job_name, "Running")
    
    try:
        success = run_compact_update()
        
        if success:
            update_scheduler_status(job_name, "Success")
            logger.info("✅ Compact update job completed successfully")
        else:
            update_scheduler_status(job_name, "Fail", "Too many operation failures")
            logger.error("❌ Compact update job failed")
            
    except Exception as e:
        error_message = f"Critical error in compact updates: {e}"
        logger.error(error_message)
        update_scheduler_status(job_name, "Fail", error_message)
        sys.exit(1)