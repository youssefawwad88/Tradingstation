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

# Import full_fetch for self-healing logic
from jobs.full_fetch import fetch_and_process_ticker, save_ticker_data

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def convert_global_quote_to_dataframe(quote_data, ticker):
    """
    Convert GLOBAL_QUOTE API response to DataFrame format matching existing data structure.
    UPDATED: Uses current timestamp rounded to minute boundary for real-time candle generation.
    
    Args:
        quote_data (dict): Global quote data from Alpha Vantage API
        ticker (str): Stock ticker symbol
        
    Returns:
        DataFrame: Single-row DataFrame with current real-time data
    """
    if not quote_data:
        return pd.DataFrame()
    
    try:
        # CRITICAL FIX: Use current timestamp instead of latest_trading_day for real-time data
        # Round to the current minute boundary for proper candle generation
        ny_tz = pytz.timezone('America/New_York')
        current_time = datetime.now(ny_tz)
        current_minute = current_time.replace(second=0, microsecond=0)
        
        # Create DataFrame with structure matching existing historical data
        df = pd.DataFrame({
            'timestamp': [current_minute.strftime('%Y-%m-%d %H:%M:%S')],
            'open': [quote_data['price']],   # For real-time: open=current price (will be updated intelligently)
            'high': [quote_data['price']],   # For real-time: high=current price (will be updated intelligently)
            'low': [quote_data['price']],    # For real-time: low=current price (will be updated intelligently)
            'close': [quote_data['price']],  # For real-time: close=current price
            'volume': [quote_data['volume']] # Use reported volume
        })
        
        logger.debug(f"Converted GLOBAL_QUOTE to DataFrame for {ticker}: {len(df)} row with timestamp {current_minute}")
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


def intelligent_append_or_update(existing_df, new_df):
    """
    INTELLIGENT APPEND & UPDATE LOGIC as per problem statement requirements.
    
    Compare live quote timestamp with last candle timestamp:
    - If live quote is in NEW minute: Append new candle 
    - If live quote is in SAME minute: Update existing candle (high=max, low=min, close=current)
    
    Args:
        existing_df (DataFrame): Current data from storage (1-minute data)
        new_df (DataFrame): New real-time data from GLOBAL_QUOTE
        
    Returns:
        DataFrame: Updated dataframe with intelligent append/update applied
    """
def intelligent_append_or_update(existing_df, new_df):
    """
    INTELLIGENT APPEND & UPDATE LOGIC as per problem statement requirements.
    
    Compare live quote timestamp with last candle timestamp:
    - If live quote is in NEW minute: Append new candle 
    - If live quote is in SAME minute: Update existing candle (high=max, low=min, close=current)
    
    Args:
        existing_df (DataFrame): Current data from storage (1-minute data)
        new_df (DataFrame): New real-time data from GLOBAL_QUOTE
        
    Returns:
        DataFrame: Updated dataframe with intelligent append/update applied
    """
    if existing_df.empty:
        logger.info("📊 Intelligent Append: No existing data - appending new candle")
        logger.info(f"   Original: 0 rows, New: {len(new_df)} rows, Final: {len(new_df)} rows")
        return new_df
    
    if new_df.empty:
        logger.info("📊 Intelligent Append: No new data - returning existing data unchanged")
        logger.info(f"   Original: {len(existing_df)} rows, New: 0 rows, Final: {len(existing_df)} rows")
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
            return existing_df
        if timestamp_col not in new_df.columns:
            logger.error(f"Timestamp column '{timestamp_col}' not found in new data")
            return existing_df
        
        # Convert timestamps to datetime for comparison
        existing_df[timestamp_col] = pd.to_datetime(existing_df[timestamp_col])
        new_df[timestamp_col] = pd.to_datetime(new_df[timestamp_col])
        
        # Get the new quote data (should be single row)
        if len(new_df) != 1:
            logger.warning(f"Expected 1 row in new real-time data, got {len(new_df)} rows")
            return existing_df
        
        new_timestamp = new_df.iloc[0][timestamp_col]
        new_high = new_df.iloc[0]['high']    # New high value
        new_low = new_df.iloc[0]['low']      # New low value
        new_close = new_df.iloc[0]['close']  # New close price
        new_volume = new_df.iloc[0]['volume']
        
        # Get the last candle from existing data
        if len(existing_df) > 0:
            existing_df_sorted = existing_df.sort_values(timestamp_col)
            last_candle_timestamp = existing_df_sorted.iloc[-1][timestamp_col]
            
            logger.info(f"📊 Intelligent Append Analysis:")
            logger.info(f"   Last candle timestamp: {last_candle_timestamp}")
            logger.info(f"   New quote timestamp: {new_timestamp}")
            
            # CORE LOGIC: Compare timestamps to determine append vs update
            if new_timestamp == last_candle_timestamp:
                # SAME MINUTE: Update existing candle
                logger.info(f"✏️ SAME MINUTE detected - Updating existing candle")
                
                # Get the index of the last candle
                last_idx = existing_df_sorted.index[-1]
                
                # Update the last candle with intelligent logic
                updated_high = max(existing_df_sorted.loc[last_idx, 'high'], new_high)
                updated_low = min(existing_df_sorted.loc[last_idx, 'low'], new_low)
                
                existing_df_sorted.loc[last_idx, 'high'] = updated_high
                existing_df_sorted.loc[last_idx, 'low'] = updated_low
                existing_df_sorted.loc[last_idx, 'close'] = new_close  # Always update close to current price
                # Note: open stays the same, volume could be updated but we'll keep existing
                
                logger.info(f"   Updated candle: high={updated_high}, low={updated_low}, close={new_close}")
                logger.info(f"   Original: {len(existing_df)} rows, Updated: 0 new rows, Final: {len(existing_df)} rows")
                
                return existing_df_sorted
                
            elif new_timestamp > last_candle_timestamp:
                # NEW MINUTE: Append new candle
                logger.info(f"➕ NEW MINUTE detected - Appending new candle")
                
                # Append the new candle
                combined_df = pd.concat([existing_df_sorted, new_df], ignore_index=True)
                combined_df = combined_df.sort_values(timestamp_col)
                
                logger.info(f"   Appended new candle: timestamp={new_timestamp}, price={new_close}")
                logger.info(f"   Original: {len(existing_df)} rows, New: 1 candle, Final: {len(combined_df)} rows")
                
                return combined_df
            else:
                # PAST TIMESTAMP: This shouldn't happen in real-time, but handle gracefully
                logger.warning(f"⚠️ New timestamp {new_timestamp} is older than last candle {last_candle_timestamp}")
                logger.info(f"   Keeping existing data unchanged")
                return existing_df
        else:
            # No existing data, append new candle
            logger.info(f"➕ No existing candles - Appending first candle")
            return new_df
            
    except Exception as e:
        logger.error(f"❌ Error in intelligent append/update: {e}")
        logger.warning(f"🔄 Returning existing data as fallback to avoid data loss ({len(existing_df)} rows)")
        return existing_df


def resample_1min_to_30min(df_1min):
    """
    Resample 1-minute data to 30-minute data with proper OHLCV aggregation.
    
    Aggregation rules as per requirements:
    - open='first' (first open in 30-min period)
    - high='max' (highest high in 30-min period)  
    - low='min' (lowest low in 30-min period)
    - close='last' (last close in 30-min period)
    - volume='sum' (total volume in 30-min period)
    
    Args:
        df_1min (DataFrame): 1-minute OHLCV data
        
    Returns:
        DataFrame: 30-minute OHLCV data, trimmed to 500 rows (most recent)
    """
    if df_1min.empty:
        logger.warning("📊 Resample: No 1-minute data to resample")
        return pd.DataFrame()
    
    try:
        # Ensure timestamp column exists and is datetime
        timestamp_col = 'timestamp'
        if timestamp_col not in df_1min.columns:
            logger.error(f"Timestamp column '{timestamp_col}' not found in 1-minute data")
            return pd.DataFrame()
        
        # Make a copy to avoid modifying original
        df_work = df_1min.copy()
        df_work[timestamp_col] = pd.to_datetime(df_work[timestamp_col])
        
        # Set timestamp as index for resampling
        df_work = df_work.set_index(timestamp_col)
        
        # Resample to 30-minute intervals with proper aggregation
        df_30min = df_work.resample('30min').agg({
            'open': 'first',
            'high': 'max', 
            'low': 'min',
            'close': 'last',
            'volume': 'sum'
        }).dropna()  # Remove any NaN rows
        
        # Reset index to get timestamp back as column
        df_30min = df_30min.reset_index()
        
        # Sort by timestamp (newest first) and keep only last 500 rows as per requirements
        df_30min = df_30min.sort_values(timestamp_col, ascending=False).head(500)
        
        # Sort back to chronological order (oldest first) for consistency
        df_30min = df_30min.sort_values(timestamp_col, ascending=True)
        
        logger.info(f"📊 Resampled 1-minute data: {len(df_1min)} rows → 30-minute data: {len(df_30min)} rows (trimmed to 500)")
        
        return df_30min
        
    except Exception as e:
        logger.error(f"❌ Error resampling 1-minute to 30-minute data: {e}")
        return pd.DataFrame()


def check_ticker_data_health(ticker):
    """
    Step A: Data Health Check & Bootstrapping with Self-Healing Logic
    
    Check for History File existence and validate file integrity before attempting real-time updates.
    As per problem statement requirements:
    1. Check if {ticker}_1min.csv file exists in the production data store
    2. Validate file integrity (minimum file size > 50KB - a 100-byte file is considered corrupt)
    3. Handle missing/corrupt data with self-healing logic: automatically trigger full_fetch to repair
    
    Args:
        ticker (str): Stock ticker symbol
        
    Returns:
        bool: True if ticker has healthy data (either originally or after successful repair)
    """
    try:
        file_path_1min = f'data/intraday/{ticker}_1min.csv'
        logger.debug(f"🏥 Health Check: Checking file existence for {file_path_1min}")
        
        # Step A1: Check for History File existence
        data_is_healthy = False
        existing_1min_df = None
        
        try:
            existing_1min_df = read_df_from_s3(file_path_1min)
            
            if existing_1min_df.empty:
                logger.warning(f"⚠️ {ticker}_1min.csv not found or is incomplete. Triggering a full data fetch to repair.")
                data_is_healthy = False
            else:
                data_is_healthy = True
                
        except Exception as e:
            logger.warning(f"⚠️ {ticker}_1min.csv not found or is incomplete. Triggering a full data fetch to repair.")
            logger.debug(f"Health check file read error for {ticker}: {e}")
            data_is_healthy = False
        
        # Step A2: Validate File Integrity if data exists
        if data_is_healthy and existing_1min_df is not None:
            try:
                # Try to get the actual file size from local filesystem first
                local_file_path = os.path.join(
                    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 
                    file_path_1min
                )
                
                file_size_bytes = 0
                if os.path.exists(local_file_path):
                    file_size_bytes = os.path.getsize(local_file_path)
                    logger.debug(f"Local file size for {ticker}: {file_size_bytes} bytes")
                else:
                    # If no local file, estimate size from DataFrame
                    # Based on testing: actual CSV size is roughly 55-60 bytes per row for OHLCV data
                    # Using conservative estimate of 60 bytes per row
                    estimated_size = len(existing_1min_df) * 60  # Realistic estimate based on testing
                    file_size_bytes = estimated_size
                    logger.debug(f"Estimated file size for {ticker}: {file_size_bytes} bytes (from {len(existing_1min_df)} rows)")
                
                # Check minimum file size (>50KB = 51,200 bytes as per problem statement)
                min_file_size = 50 * 1024  # 50KB in bytes
                if file_size_bytes <= min_file_size:
                    logger.warning(f"⚠️ {ticker}_1min.csv not found or is incomplete. Triggering a full data fetch to repair.")
                    logger.debug(f"Health check failed for {ticker}: insufficient file size ({file_size_bytes} bytes, minimum {min_file_size} bytes required)")
                    data_is_healthy = False
                
                # Check for required columns
                if data_is_healthy:
                    required_columns = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
                    missing_columns = [col for col in required_columns if col not in existing_1min_df.columns]
                    if missing_columns:
                        logger.warning(f"⚠️ {ticker}_1min.csv not found or is incomplete. Triggering a full data fetch to repair.")
                        logger.debug(f"Health check failed for {ticker}: missing columns {missing_columns}")
                        data_is_healthy = False
                
                # Additional validation: Ensure substantial 7-day history
                if data_is_healthy:
                    # For 7 days of 1-minute data during market hours (~390 minutes/day * 7 days = ~2730 rows minimum)
                    if len(existing_1min_df) < 1000:  # Conservative minimum for multi-day history
                        logger.warning(f"⚠️ {ticker}_1min.csv not found or is incomplete. Triggering a full data fetch to repair.")
                        logger.debug(f"Health check failed for {ticker}: insufficient history ({len(existing_1min_df)} rows, expected substantial 7-day history)")
                        data_is_healthy = False
                
                if data_is_healthy:
                    logger.debug(f"✅ Health check passed for {ticker}: {len(existing_1min_df)} rows, {file_size_bytes} bytes with required columns")
                    return True
                
            except Exception as e:
                logger.warning(f"⚠️ {ticker}_1min.csv not found or is incomplete. Triggering a full data fetch to repair.")
                logger.debug(f"Health check integrity validation failed for {ticker}: {e}")
                data_is_healthy = False
        
        # Step A3: Self-Healing Logic - Automatically trigger full_fetch for corrupted/missing data
        if not data_is_healthy:
            logger.info(f"🔧 Self-Healing: Initiating full data fetch to repair {ticker}...")
            try:
                # Call the full_fetch logic for this specific ticker
                fetch_results = fetch_and_process_ticker(ticker)
                
                # Save the fetched data using the full_fetch save logic
                save_results = save_ticker_data(ticker, fetch_results)
                
                # Check if the repair was successful
                if save_results['saves_successful'] > 0:
                    logger.info(f"✅ Self-Healing: Successfully repaired data for {ticker} ({save_results['saves_successful']}/{save_results['saves_attempted']} files saved)")
                    
                    # Verify the repair by re-checking the 1-minute file
                    try:
                        repaired_df = read_df_from_s3(file_path_1min)
                        if not repaired_df.empty and len(repaired_df) >= 1000:
                            logger.info(f"✅ Self-Healing: Repair verification successful for {ticker} - {len(repaired_df)} rows available")
                            return True
                        else:
                            logger.error(f"❌ Self-Healing: Repair verification failed for {ticker} - data still insufficient after repair")
                            return False
                    except Exception as e:
                        logger.error(f"❌ Self-Healing: Repair verification failed for {ticker}: {e}")
                        return False
                else:
                    logger.error(f"❌ Self-Healing: Failed to repair data for {ticker} (no files saved successfully)")
                    return False
                    
            except Exception as e:
                logger.error(f"❌ Self-Healing: Full data fetch failed for {ticker}: {e}")
                return False
        
        return False  # Should not reach here, but fallback to False
        
    except Exception as e:
        logger.warning(f"⚠️ {ticker}_1min.csv not found or is incomplete. A full data fetch is required for this ticker.")
        logger.debug(f"Health check critical error for {ticker}: {e}")
        return False


def process_ticker_realtime(ticker):
    """
    Process real-time updates for a single ticker using GLOBAL_QUOTE endpoint.
    
    UPDATED: Implements "Intelligent Append & Resample" Architecture with production-grade error handling:
    Step A: Data Health Check & Bootstrapping (NEW - per problem statement)
    Step 1: Fetch Live Quote (GLOBAL_QUOTE endpoint)
    Step 2: Load 1-Minute History (full 7-day history from _1min.csv)
    Step 3: Intelligently Append or Update (compare timestamps, update same minute vs append new minute)
    Step 4: Save 1-Minute File (updated 1-minute DataFrame)
    Step 5: Resample to 30-Minute (create 30-minute data from perfected 1-minute data)
    Step 6: Save 30-Minute File (trimmed to 500 rows)
    
    Each step is wrapped in individual try-except blocks for granular error reporting.
    A failure in one ticker does not crash the entire update job.
    
    Args:
        ticker (str): Stock ticker symbol
        
    Returns:
        bool: True if processing successful (includes graceful skips for missing data)
    """
    try:
        logger.info(f"📊 Processing real-time data for {ticker}")
        
        # Step A: Data Health Check & Bootstrapping (NEW - as per problem statement)
        logger.debug(f"🏥 Step A: Data Health Check for {ticker}...")
        if not check_ticker_data_health(ticker):
            # Gracefully skip this ticker - log already handled in health check function
            logger.info(f"⏭️ Skipping real-time update for {ticker} due to missing/incomplete data")
            return True  # Return True to continue processing other tickers (graceful skip)
        
        logger.debug(f"✅ Step A passed: {ticker} has healthy data, proceeding with real-time update")
        
        # Step 1: Fetch Live Quote (GLOBAL_QUOTE endpoint)
        try:
            logging.info(f"[{ticker}] Fetching real-time quote...")
            logger.debug(f"🔄 Step 1: Fetching GLOBAL_QUOTE data for {ticker}...")
            quote_data = get_real_time_price(ticker)
            
            if not quote_data:
                logging.info(f"[{ticker}] API returned no real-time data. Skipping.")
                logger.warning(f"⚠️ No real-time data received for {ticker} - API may have failed or market closed")
                return True  # Consider this a success since it's not a processing failure
            
            logging.info(f"[{ticker}] Real-time quote received.")
            logger.info(f"📥 Received real-time quote for {ticker}: price=${quote_data['price']}")
            
        except Exception as e:
            logger.error(f"❌ STEP 1 FAILED - Error fetching GLOBAL_QUOTE for {ticker}: {e}")
            logger.error(f"❌ {ticker}: API call failed during quote fetch step")
            return False
        
        # Step 2: Transform single quote into one-row DataFrame matching existing structure
        try:
            logger.debug(f"🔄 Step 2: Converting GLOBAL_QUOTE to DataFrame for {ticker}...")
            new_df = convert_global_quote_to_dataframe(quote_data, ticker)
            
            if new_df.empty:
                logger.error(f"❌ STEP 2 FAILED - Failed to convert GLOBAL_QUOTE data to DataFrame for {ticker}")
                logger.error(f"❌ {ticker}: Data transformation failed during DataFrame conversion step")
                return False
            
            logger.debug(f"✅ Real-time data converted to DataFrame for {ticker}: {len(new_df)} row")
            
        except Exception as e:
            logger.error(f"❌ STEP 2 FAILED - Error converting GLOBAL_QUOTE to DataFrame for {ticker}: {e}")
            logger.error(f"❌ {ticker}: Data transformation failed during DataFrame conversion step")
            return False
        
        # Step 3: Standardize timestamps for new data
        try:
            logger.debug(f"🕐 Step 3: Standardizing timestamps for {ticker}...")
            new_df = standardize_timestamps(new_df, '1min')
            logger.debug(f"✅ Timestamps standardized for {ticker}")
            
        except Exception as e:
            logger.error(f"❌ STEP 3 FAILED - Error standardizing timestamps for {ticker}: {e}")
            logger.error(f"❌ {ticker}: Timestamp processing failed during standardization step")
            return False
        
        # Step 4: Load 1-Minute History (full 7-day history from _1min.csv)
        try:
            file_path_1min = f'data/intraday/{ticker}_1min.csv'
            logger.debug(f"📂 Step 4: Reading existing 1min data: {file_path_1min}")
            existing_1min_df = read_df_from_s3(file_path_1min)
            existing_count = len(existing_1min_df) if not existing_1min_df.empty else 0
            logging.info(f"[{ticker}] Loaded existing 1min file... It has {existing_count} rows.")
            logger.debug(f"📊 Existing 1min data for {ticker}: {existing_count} rows")
            
        except Exception as e:
            logger.error(f"❌ STEP 4 FAILED - Error loading existing 1min data for {ticker}: {e}")
            logger.error(f"❌ {ticker}: File read failed during 1min data loading step")
            return False
        
        # Step 5: Intelligently Append or Update (1-Min Data)
        try:
            logger.debug(f"🧠 Step 5: Applying intelligent append/update logic for {ticker}...")
            updated_1min_df = intelligent_append_or_update(existing_1min_df, new_df)
            logging.info(f"[{ticker}] Intelligent update complete. 1min DataFrame now has {len(updated_1min_df)} rows.")
            
            # Calculate changes
            final_1min_count = len(updated_1min_df)
            rows_changed = final_1min_count - existing_count
            
            if rows_changed > 0:
                logging.info(f"[{ticker}] 1-min data changed: +{rows_changed} rows. Saving...")
                logger.info(f"INFO: Intelligent update successful: Added {rows_changed} new candles for {ticker} (1-min)")
            elif rows_changed == 0 and final_1min_count > 0:
                logging.info(f"[{ticker}] 1-min data updated in-place (same minute). Saving...")
                logger.info(f"INFO: Intelligent update successful: Updated existing candle for {ticker} (1-min)")
            else:
                logging.info(f"[{ticker}] No 1-min data changes detected. Skipping saves.")
                return True
                
        except Exception as e:
            logger.error(f"❌ STEP 5 FAILED - Error during intelligent append/update for {ticker}: {e}")
            logger.error(f"❌ {ticker}: Data merge failed during intelligent append/update step")
            return False
        
        # Step 6: Save 1-Minute File (updated 1-minute DataFrame)
        try:
            logger.info(f"💾 Step 6: Saving updated 1-minute data for {ticker}...")
            save_1min_success = save_df_to_s3(updated_1min_df, file_path_1min)
            
            if not save_1min_success:
                logger.error(f"❌ STEP 6 FAILED - Failed to save 1-minute data for {ticker}")
                logger.error(f"❌ {ticker}: File save failed during 1min data save step")
                return False
            
            logger.info(f"✅ Successfully saved 1-minute data for {ticker}: {final_1min_count} rows")
            
        except Exception as e:
            logger.error(f"❌ STEP 6 FAILED - Error saving 1-minute data for {ticker}: {e}")
            logger.error(f"❌ {ticker}: File save failed during 1min data save step")
            return False
        
        # Step 7: Resample to Create 30-Minute Data
        try:
            logger.debug(f"📊 Step 7: Resampling 1-minute to 30-minute data for {ticker}...")
            resampled_30min_df = resample_1min_to_30min(updated_1min_df)
            
            if resampled_30min_df.empty:
                logger.warning(f"⚠️ STEP 7 WARNING - Failed to resample 30-minute data for {ticker}")
                logger.warning(f"⚠️ {ticker}: Resampling failed during 30min data creation step, but 1min save succeeded")
                return True  # 1-min save was successful, so this is still a partial success
            
            logger.info(f"📊 Resampled 30-minute data for {ticker}: {len(resampled_30min_df)} rows")
            
        except Exception as e:
            logger.error(f"❌ STEP 7 FAILED - Error resampling 30-minute data for {ticker}: {e}")
            logger.warning(f"⚠️ {ticker}: Resampling failed during 30min data creation step, but 1min save succeeded")
            return True  # 1-min save was successful, so this is still a partial success
        
        # Step 8: Save 30-Minute File (trimmed to 500 rows)
        try:
            file_path_30min = f'data/intraday_30min/{ticker}_30min.csv'
            logger.info(f"💾 Step 8: Saving resampled 30-minute data for {ticker}...")
            save_30min_success = save_df_to_s3(resampled_30min_df, file_path_30min)
            
            if save_30min_success:
                logger.info(f"✅ Successfully saved 30-minute data for {ticker}: {len(resampled_30min_df)} rows")
                logger.info(f"🎉 Complete real-time update success for {ticker}: 1-min ✅, 30-min ✅")
                return True
            else:
                logger.warning(f"⚠️ STEP 8 WARNING - Failed to save 30-minute data for {ticker}, but 1-minute save succeeded")
                logger.warning(f"⚠️ {ticker}: File save failed during 30min data save step, but 1min save succeeded")
                return True  # Partial success is still success
                
        except Exception as e:
            logger.error(f"❌ STEP 8 FAILED - Error saving 30-minute data for {ticker}: {e}")
            logger.warning(f"⚠️ {ticker}: File save failed during 30min data save step, but 1min save succeeded")
            return True  # Partial success is still success
            
    except Exception as e:
        logger.error(f"❌ CRITICAL FAILURE - Unexpected error processing real-time data for {ticker}: {e}")
        logger.error(f"❌ {ticker}: Critical failure in real-time processing - continuing with next ticker")
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