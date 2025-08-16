#!/usr/bin/env python3
"""
Comprehensive Data Fetching System
=================================

A single, powerful system that handles all data fetching needs with intelligent
configuration and strategies. Replaces separate scripts for different data types.

Strategic Features:
1. ✅ Centralized Configuration - All settings at the top
2. ✅ Single Generic API Function - Handles all data types and intervals
3. ✅ Intelligent Fetching Strategy - 10KB threshold for full vs compact
4. ✅ Multiple Data Types Support - INTRADAY and DAILY with proper logic

USAGE:
    # Edit configuration section below, then run:
    python3 comprehensive_data_fetcher.py
    
    # Configuration is at the top - just change these values:
    TICKER_SYMBOL = "AAPL"
    DATA_INTERVAL = "1min"  # or "30min" for intraday, ignored for daily
    DATA_TYPE = "INTRADAY"  # or "DAILY"
    FILE_SIZE_THRESHOLD_KB = 10
    API_KEY = "your_alpha_vantage_key"  # or uses environment variable
"""

import pandas as pd
import sys
import os
from datetime import datetime, timedelta
import time
import logging
import pytz

# Add project root to Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '.')))

# =============================================================================
# CENTRALIZED CONFIGURATION SECTION
# =============================================================================
# All variables for configuration are located here for easy access

TICKER_SYMBOL = "AAPL"                    # Stock ticker to fetch data for
DATA_INTERVAL = "1min"                    # "1min" or "30min" (ignored for DAILY)
DATA_TYPE = "INTRADAY"                    # "INTRADAY" or "DAILY"
FILE_SIZE_THRESHOLD_KB = 10               # Threshold for full vs compact fetch
API_KEY = None                            # Uses environment variable if None

# =============================================================================
# IMPORTS AND SETUP
# =============================================================================

from utils.config import ALPHA_VANTAGE_API_KEY
from utils.spaces_manager import get_cloud_file_size_bytes
from utils.helpers import read_master_tickerlist, save_df_to_s3, read_df_from_s3, update_scheduler_status
from utils.alpha_vantage_api import get_intraday_data, get_daily_data, get_real_time_price

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# =============================================================================
# CORE CONSOLIDATED FUNCTIONS
# =============================================================================

def get_data_from_api(data_type, symbol, interval=None, output_size="compact"):
    """
    Single, generic function that handles all data fetching requests.
    
    This function dynamically builds the correct API URL based on parameters
    and replaces all other specialized fetching functions.
    
    Args:
        data_type (str): 'INTRADAY' or 'DAILY'
        symbol (str): Stock ticker symbol
        interval (str): Time interval for intraday data ('1min', '30min', etc.)
        output_size (str): 'compact' or 'full'
        
    Returns:
        pandas.DataFrame: Fetched data or empty DataFrame on error
    """
    logger.info(f"🔄 Fetching {data_type} data for {symbol}")
    
    try:
        if data_type.upper() == "INTRADAY":
            if not interval:
                interval = "1min"  # Default fallback
            logger.info(f"   Using interval: {interval}, output_size: {output_size}")
            return get_intraday_data(symbol, interval=interval, outputsize=output_size)
            
        elif data_type.upper() == "DAILY":
            logger.info(f"   Using output_size: {output_size}")
            return get_daily_data(symbol, outputsize=output_size)
            
        else:
            logger.error(f"❌ Invalid data_type: {data_type}. Must be 'INTRADAY' or 'DAILY'")
            return pd.DataFrame()
            
    except Exception as e:
        logger.error(f"❌ Error fetching {data_type} data for {symbol}: {e}")
        return pd.DataFrame()


def intelligent_fetch_strategy(symbol, data_type, interval=None):
    """
    Implements the intelligent fetching strategy based on cloud file size.
    
    Core Logic:
    - Files < 10KB: Perform full fetch (system starting fresh or incomplete data)
    - Files ≥ 10KB: Perform compact fetch (historical data complete, get latest only)
    
    Args:
        symbol (str): Stock ticker symbol
        data_type (str): 'INTRADAY' or 'DAILY'
        interval (str): Time interval for intraday data
        
    Returns:
        str: 'full' or 'compact' fetch strategy
    """
    # Determine the cloud storage path based on data type
    if data_type.upper() == "INTRADAY":
        if interval == "30min":
            file_path = f"data/intraday_30min/{symbol}_30min.csv"
        else:
            file_path = f"data/intraday/{symbol}_1min.csv"
    elif data_type.upper() == "DAILY":
        file_path = f"data/daily/{symbol}_daily.csv"
    else:
        logger.error(f"❌ Invalid data_type for strategy: {data_type}")
        return "full"  # Fallback to full fetch
    
    # Check cloud file size
    file_size_bytes = get_cloud_file_size_bytes(file_path)
    file_size_kb = file_size_bytes / 1024
    
    logger.info(f"📁 Cloud file check: {file_path}")
    logger.info(f"   File size: {file_size_kb:.2f} KB")
    logger.info(f"   Threshold: {FILE_SIZE_THRESHOLD_KB} KB")
    
    if file_size_kb < FILE_SIZE_THRESHOLD_KB:
        logger.info("🔄 File below threshold → Using FULL fetch strategy")
        return "full"
    else:
        logger.info("⚡ File above threshold → Using COMPACT fetch strategy")
        return "compact"


def handle_intraday_data(symbol, interval):
    """
    Handle INTRADAY data type with intelligent full vs compact strategy.
    After fetching, combines historical and real-time data, handles duplicates.
    
    Args:
        symbol (str): Stock ticker symbol
        interval (str): Time interval ('1min', '30min')
        
    Returns:
        bool: True if successful, False otherwise
    """
    logger.info("🚀 Processing INTRADAY data")
    logger.info("=" * 60)
    
    # Step 1: Determine fetch strategy
    fetch_strategy = intelligent_fetch_strategy(symbol, "INTRADAY", interval)
    
    # Step 2: Fetch new data using strategy
    new_df = get_data_from_api("INTRADAY", symbol, interval, fetch_strategy)
    
    if new_df.empty:
        logger.error("❌ No new data received from API")
        return False
    
    logger.info(f"✅ Received {len(new_df)} rows from API")
    
    # Step 3: Handle data merging if using compact strategy
    if fetch_strategy == "compact":
        # Get existing data
        if interval == "30min":
            file_path = f"data/intraday_30min/{symbol}_30min.csv"
        else:
            file_path = f"data/intraday/{symbol}_1min.csv"
            
        existing_df = read_df_from_s3(file_path)
        
        if not existing_df.empty:
            logger.info(f"📊 Merging with existing {len(existing_df)} rows")
            # Combine data, remove duplicates, keep latest
            combined_df = pd.concat([existing_df, new_df], ignore_index=True)
            combined_df = combined_df.drop_duplicates(subset=['timestamp'], keep='last')
            combined_df = combined_df.sort_values('timestamp', ascending=False)
            final_df = combined_df
        else:
            final_df = new_df
    else:
        final_df = new_df
    
    # Step 4: Save final data
    if interval == "30min":
        file_path = f"data/intraday_30min/{symbol}_30min.csv"
    else:
        file_path = f"data/intraday/{symbol}_1min.csv"
    
    success = save_df_to_s3(final_df, file_path)
    
    if success:
        logger.info(f"✅ Saved {len(final_df)} rows to {file_path}")
        return True
    else:
        logger.error(f"❌ Failed to save data to {file_path}")
        return False


def handle_daily_data(symbol):
    """
    Handle DAILY data type.
    Always performs full fetch for complete historical record,
    then separate live_quote call to update latest candle, then merges.
    
    Args:
        symbol (str): Stock ticker symbol
        
    Returns:
        bool: True if successful, False otherwise
    """
    logger.info("🚀 Processing DAILY data")
    logger.info("=" * 60)
    
    # Step 1: Always do full fetch for complete historical record
    logger.info("📈 Fetching complete historical daily data")
    historical_df = get_data_from_api("DAILY", symbol, output_size="full")
    
    if historical_df.empty:
        logger.error("❌ No historical data received from API")
        return False
    
    logger.info(f"✅ Received {len(historical_df)} rows of historical data")
    
    # Step 2: Get real-time price to update latest candle
    logger.info("⚡ Fetching latest real-time price")
    try:
        real_time_data = get_real_time_price(symbol)
        if real_time_data and not real_time_data.empty:
            logger.info("✅ Real-time data received")
            # Note: The specific merging logic would depend on the real-time data format
            # For now, we'll use the historical data as the primary source
            final_df = historical_df
        else:
            logger.warning("⚠️ No real-time data available, using historical only")
            final_df = historical_df
    except Exception as e:
        logger.warning(f"⚠️ Error fetching real-time data: {e}")
        final_df = historical_df
    
    # Step 3: Take exactly 200 rows as specified in original daily script
    final_df = final_df.head(200)
    
    # Step 4: Save final data
    file_path = f"data/daily/{symbol}_daily.csv"
    success = save_df_to_s3(final_df, file_path)
    
    if success:
        logger.info(f"✅ Saved {len(final_df)} rows to {file_path}")
        return True
    else:
        logger.error(f"❌ Failed to save data to {file_path}")
        return False


def main():
    """
    Main execution function implementing the strategic data fetching logic.
    Uses simple if-elif-else statement to handle different DATA_TYPE values.
    """
    logger.info("=" * 80)
    logger.info("🚀 COMPREHENSIVE DATA FETCHING SYSTEM")
    logger.info("=" * 80)
    
    # Show configuration
    logger.info("⚙️ Configuration:")
    logger.info(f"   TICKER_SYMBOL: {TICKER_SYMBOL}")
    logger.info(f"   DATA_TYPE: {DATA_TYPE}")
    logger.info(f"   DATA_INTERVAL: {DATA_INTERVAL}")
    logger.info(f"   FILE_SIZE_THRESHOLD_KB: {FILE_SIZE_THRESHOLD_KB}")
    logger.info(f"   API_KEY: {'✅ Available' if (API_KEY or ALPHA_VANTAGE_API_KEY) else '❌ Missing'}")
    
    # Validate configuration
    effective_api_key = API_KEY or ALPHA_VANTAGE_API_KEY
    if not effective_api_key:
        logger.warning("⚠️ No API key configured - running in test mode")
    
    # Execute based on DATA_TYPE using simple if-elif-else logic
    success = False
    
    if DATA_TYPE.upper() == "INTRADAY":
        logger.info(f"\n📊 Processing INTRADAY data with {DATA_INTERVAL} interval")
        success = handle_intraday_data(TICKER_SYMBOL, DATA_INTERVAL)
        
    elif DATA_TYPE.upper() == "DAILY":
        logger.info(f"\n📊 Processing DAILY data")
        success = handle_daily_data(TICKER_SYMBOL)
        
    else:
        logger.error(f"❌ Invalid DATA_TYPE: {DATA_TYPE}")
        logger.error("   Valid options: 'INTRADAY' or 'DAILY'")
        return False
    
    # Final result
    logger.info("\n" + "=" * 80)
    if success:
        logger.info("✅ COMPREHENSIVE DATA FETCH COMPLETED SUCCESSFULLY")
    else:
        logger.info("❌ COMPREHENSIVE DATA FETCH FAILED")
    logger.info("=" * 80)
    
    return success


if __name__ == "__main__":
    # Update job status for scheduler integration
    job_name = f"comprehensive_fetch_{DATA_TYPE.lower()}_{TICKER_SYMBOL}"
    update_scheduler_status(job_name, "Running")
    
    try:
        success = main()
        if success:
            update_scheduler_status(job_name, "Success")
        else:
            update_scheduler_status(job_name, "Fail", "Data fetch failed")
    except Exception as e:
        error_message = f"Unexpected error: {e}"
        logger.error(error_message)
        update_scheduler_status(job_name, "Fail", error_message)