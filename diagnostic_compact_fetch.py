#!/usr/bin/env python3
"""
Comprehensive Diagnostic Test for Compact Intraday Data Fetch Issues

This script performs multiple checks to identify why compact data fetching 
is not generating today's data.
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
    read_master_tickerlist, save_df_to_s3, read_df_from_s3, 
    is_today_present, detect_market_session
)
from utils.alpha_vantage_api import get_intraday_data
from utils.config import ALPHA_VANTAGE_API_KEY, TIMEZONE

# Set up logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_api_key_configuration():
    """Test if API key is properly configured."""
    logger.info("=== Testing API Key Configuration ===")
    
    if ALPHA_VANTAGE_API_KEY:
        logger.info(f"✅ API key found: {ALPHA_VANTAGE_API_KEY[:8]}...")
        return True
    else:
        logger.error("❌ API key not found in environment variables")
        return False

def test_master_tickerlist():
    """Test if master tickerlist can be read."""
    logger.info("=== Testing Master Tickerlist ===")
    
    try:
        tickers = read_master_tickerlist()
        if tickers:
            logger.info(f"✅ Master tickerlist loaded: {len(tickers)} tickers")
            logger.info(f"Tickers: {tickers[:5]}...")  # Show first 5
            return tickers
        else:
            logger.error("❌ No tickers found in master tickerlist")
            return []
    except Exception as e:
        logger.error(f"❌ Error reading master tickerlist: {e}")
        return []

def test_market_session():
    """Test market session detection."""
    logger.info("=== Testing Market Session Detection ===")
    
    try:
        session = detect_market_session()
        logger.info(f"Current market session: {session}")
        
        ny_tz = pytz.timezone(TIMEZONE)
        current_time = datetime.now(ny_tz)
        logger.info(f"Current time in {TIMEZONE}: {current_time}")
        
        return session
    except Exception as e:
        logger.error(f"❌ Error detecting market session: {e}")
        return "unknown"

def test_raw_api_response(ticker="AAPL"):
    """Test raw API response for a single ticker."""
    logger.info(f"=== Testing Raw API Response for {ticker} ===")
    
    if not ALPHA_VANTAGE_API_KEY:
        logger.warning("Skipping API test - no API key")
        return None
    
    try:
        # Test compact data
        logger.info("Fetching compact intraday data...")
        compact_df = get_intraday_data(ticker, interval='1min', outputsize='compact')
        
        if not compact_df.empty:
            logger.info(f"✅ Compact data received: {len(compact_df)} rows")
            logger.info(f"Columns: {list(compact_df.columns)}")
            logger.info(f"Date range: {compact_df['timestamp'].min()} to {compact_df['timestamp'].max()}")
            
            # Check if today's data is present
            today_present = is_today_present(compact_df)
            logger.info(f"Today's data present: {today_present}")
            
            # Show sample data
            logger.info("Sample data (first 3 rows):")
            print(compact_df.head(3))
            
            return compact_df
        else:
            logger.error("❌ Compact data is empty")
            return pd.DataFrame()
            
    except Exception as e:
        logger.error(f"❌ Error fetching compact data: {e}")
        return None

def test_today_data_detection(df):
    """Test today's data detection logic."""
    logger.info("=== Testing Today's Data Detection ===")
    
    if df is None or df.empty:
        logger.warning("No data to test")
        return False
    
    try:
        ny_tz = pytz.timezone(TIMEZONE)
        today = datetime.now(ny_tz).date()
        logger.info(f"Today's date: {today}")
        
        # Convert timestamps to datetime if they aren't already
        df_copy = df.copy()
        if 'timestamp' in df_copy.columns:
            df_copy['timestamp'] = pd.to_datetime(df_copy['timestamp'])
            df_copy['date'] = df_copy['timestamp'].dt.date
            
            today_data = df_copy[df_copy['date'] == today]
            logger.info(f"Rows with today's date: {len(today_data)}")
            
            if not today_data.empty:
                logger.info("Sample today's data:")
                print(today_data.head(3))
                return True
            else:
                logger.warning("❌ No data found for today's date")
                
                # Show what dates are available
                unique_dates = df_copy['date'].unique()
                logger.info(f"Available dates: {sorted(unique_dates)}")
                return False
        else:
            logger.error("No timestamp column found")
            return False
            
    except Exception as e:
        logger.error(f"❌ Error in today's data detection: {e}")
        return False

def test_append_logic():
    """Test the smart append logic."""
    logger.info("=== Testing Smart Append Logic ===")
    
    try:
        from fetch_intraday_compact import append_new_candles_smart
        
        # Create test data - existing data from yesterday
        ny_tz = pytz.timezone(TIMEZONE)
        yesterday = datetime.now(ny_tz) - timedelta(days=1)
        
        existing_data = pd.DataFrame({
            'timestamp': [yesterday.replace(hour=9, minute=30), yesterday.replace(hour=10, minute=0)],
            'open': [100.0, 101.0],
            'high': [102.0, 103.0],
            'low': [99.0, 100.0],
            'close': [101.0, 102.0],
            'volume': [1000000, 1100000]
        })
        
        # Create new data - today's data
        today = datetime.now(ny_tz)
        new_data = pd.DataFrame({
            'timestamp': [today.replace(hour=9, minute=30), today.replace(hour=10, minute=0)],
            'open': [102.0, 103.0],
            'high': [104.0, 105.0],
            'low': [101.0, 102.0],
            'close': [103.0, 104.0],
            'volume': [1200000, 1300000]
        })
        
        logger.info(f"Testing append with {len(existing_data)} existing rows and {len(new_data)} new rows")
        
        result = append_new_candles_smart(existing_data, new_data)
        
        logger.info(f"Result: {len(result)} total rows")
        logger.info("Combined data:")
        print(result)
        
        return len(result) == len(existing_data) + len(new_data)
        
    except Exception as e:
        logger.error(f"❌ Error testing append logic: {e}")
        return False

def test_full_compact_flow(ticker="AAPL"):
    """Test the complete compact fetch flow."""
    logger.info(f"=== Testing Full Compact Flow for {ticker} ===")
    
    if not ALPHA_VANTAGE_API_KEY:
        logger.warning("Skipping full flow test - no API key")
        return False
    
    try:
        # Step 1: Get existing data (will be empty in test)
        file_path = f'data/intraday/{ticker}_1min.csv'
        logger.info(f"Checking for existing data at: {file_path}")
        
        existing_df = pd.DataFrame()  # Simulate no existing data
        logger.info(f"Existing data: {len(existing_df)} rows")
        
        # Step 2: Fetch compact data
        logger.info("Fetching compact data...")
        latest_df = get_intraday_data(ticker, interval='1min', outputsize='compact')
        
        if latest_df.empty:
            logger.error("❌ No data returned from API")
            return False
        
        logger.info(f"✅ Fetched {len(latest_df)} rows")
        
        # Step 3: Test column normalization
        if 'timestamp' not in latest_df.columns and len(latest_df.columns) >= 6:
            latest_df.columns = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
            logger.info("Normalized column names")
        
        # Step 4: Test append logic
        from fetch_intraday_compact import append_new_candles_smart
        combined_df = append_new_candles_smart(existing_df, latest_df)
        logger.info(f"After append: {len(combined_df)} rows")
        
        # Step 5: Test 7-day filter
        seven_days_ago = datetime.now() - timedelta(days=7)
        timestamp_col = 'timestamp' if 'timestamp' in combined_df.columns else 'Date'
        combined_df[timestamp_col] = pd.to_datetime(combined_df[timestamp_col])
        filtered_df = combined_df[combined_df[timestamp_col] >= seven_days_ago]
        logger.info(f"After 7-day filter: {len(filtered_df)} rows")
        
        # Step 6: Check today's data in final result
        today_in_result = is_today_present(filtered_df)
        logger.info(f"Today's data in final result: {today_in_result}")
        
        if not today_in_result:
            logger.error("❌ CRITICAL: Today's data missing from final result!")
            logger.info("Date range in final result:")
            logger.info(f"From: {filtered_df[timestamp_col].min()}")
            logger.info(f"To: {filtered_df[timestamp_col].max()}")
        
        return today_in_result
        
    except Exception as e:
        logger.error(f"❌ Error in full flow test: {e}")
        return False

def run_comprehensive_diagnostics():
    """Run all diagnostic tests."""
    logger.info("🚀 Starting Comprehensive Compact Fetch Diagnostics")
    logger.info("=" * 60)
    
    results = {}
    
    # Test 1: API Key Configuration
    results['api_key'] = test_api_key_configuration()
    
    # Test 2: Master Tickerlist
    tickers = test_master_tickerlist()
    results['tickerlist'] = len(tickers) > 0
    
    # Test 3: Market Session
    session = test_market_session()
    results['market_session'] = session != "unknown"
    
    # Test 4: Raw API Response (if API key available)
    if results['api_key'] and tickers:
        test_ticker = tickers[0] if tickers else "AAPL"
        api_data = test_raw_api_response(test_ticker)
        results['api_response'] = api_data is not None and not api_data.empty
        
        # Test 5: Today's Data Detection
        if api_data is not None and not api_data.empty:
            results['today_detection'] = test_today_data_detection(api_data)
        else:
            results['today_detection'] = False
            
        # Test 6: Full Flow
        results['full_flow'] = test_full_compact_flow(test_ticker)
    else:
        logger.warning("Skipping API-dependent tests due to missing API key or tickers")
        results['api_response'] = None
        results['today_detection'] = None
        results['full_flow'] = None
    
    # Test 7: Append Logic (independent test)
    results['append_logic'] = test_append_logic()
    
    # Summary
    logger.info("=" * 60)
    logger.info("🔍 DIAGNOSTIC SUMMARY")
    logger.info("=" * 60)
    
    for test_name, result in results.items():
        if result is None:
            status = "⚠️ SKIPPED"
        elif result:
            status = "✅ PASSED"
        else:
            status = "❌ FAILED"
        logger.info(f"{test_name.upper().replace('_', ' ')}: {status}")
    
    # Recommendations
    logger.info("\n📋 RECOMMENDATIONS:")
    
    if not results['api_key']:
        logger.info("1. Set ALPHA_VANTAGE_API_KEY environment variable")
    
    if not results['tickerlist']:
        logger.info("2. Ensure master_tickerlist.csv exists and contains tickers")
    
    if results['api_response'] is False:
        logger.info("3. Check API connectivity and rate limits")
    
    if results['today_detection'] is False:
        logger.info("4. Investigate timestamp processing and timezone handling")
    
    if results['full_flow'] is False:
        logger.info("5. Debug the complete compact fetch workflow")
    
    if not results['append_logic']:
        logger.info("6. Fix the smart append logic for combining data")
    
    return results

if __name__ == "__main__":
    run_comprehensive_diagnostics()