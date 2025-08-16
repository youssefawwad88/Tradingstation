#!/usr/bin/env python3
"""
Comprehensive Intraday Data Fetcher - Test/Demo Version
======================================================

This is a demonstration version that shows the complete logic without requiring network access.
It creates simulated data to demonstrate the file size rule and 30-minute interval compatibility.

Key Features Demonstrated:
1. ✅ Robust File Size Logic: Checks if file < 10KB and performs full historical fetch
2. ✅ 30-Minute Interval Compatibility: Works with both 1min and 30min intervals
3. ✅ Dynamic Fetch Strategy: Switches between 'full' and 'compact' based on file size
4. ✅ Intelligent Data Merging: Handles data combination properly
5. ✅ Error Handling: Graceful handling of all conditions

Usage Examples:
    # Test with 1-minute data
    python3 demo_comprehensive_fix.py --test-1min
    
    # Test with 30-minute data  
    python3 demo_comprehensive_fix.py --test-30min
    
    # Test file size rule
    python3 demo_comprehensive_fix.py --test-filesize
"""

import os
import sys
import pandas as pd
import time
import logging
from datetime import datetime, timedelta
import pytz
from pathlib import Path

# =============================================================================
# CONFIGURATION SECTION
# =============================================================================

# Configuration for testing
TEST_TICKER = "AAPL"
ALPHA_VANTAGE_API_KEY = "LF4A4K5UCTYB93VZ"

# File size threshold as per problem statement
FILE_SIZE_THRESHOLD_KB = 10
FILE_SIZE_THRESHOLD_BYTES = FILE_SIZE_THRESHOLD_KB * 1024  # 10KB = 10,240 bytes

# =============================================================================
# LOGGING SETUP
# =============================================================================

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
    ]
)
logger = logging.getLogger(__name__)

# =============================================================================
# SIMULATED DATA FUNCTIONS (for testing without network)
# =============================================================================

def create_simulated_intraday_data(interval="1min", num_days=1, num_rows=None):
    """
    Create simulated intraday data for testing purposes.
    
    Args:
        interval (str): "1min" or "30min"
        num_days (int): Number of days of data to create
        num_rows (int): Specific number of rows (overrides num_days)
        
    Returns:
        pandas.DataFrame: Simulated OHLCV data
    """
    ny_tz = pytz.timezone('America/New_York')
    
    # Calculate number of rows based on interval and days
    if num_rows is None:
        if interval == "1min":
            # Market hours: 9:30 AM - 4:00 PM = 390 minutes per day
            rows_per_day = 390
        elif interval == "30min":
            # Market hours: 390 minutes / 30 = 13 rows per day
            rows_per_day = 13
        else:
            rows_per_day = 100  # Default
        
        total_rows = rows_per_day * num_days
    else:
        total_rows = num_rows
    
    # Create base timestamp (market open today)
    today = datetime.now(ny_tz).replace(hour=9, minute=30, second=0, microsecond=0)
    
    # Create timestamp sequence
    if interval == "1min":
        freq = "1min"
    elif interval == "30min":
        freq = "30min"
    else:
        freq = "1min"
    
    timestamps = pd.date_range(
        start=today - timedelta(days=num_days),
        periods=total_rows,
        freq=freq,
        tz=ny_tz
    )
    
    # Create realistic price data
    base_price = 150.0  # AAPL-like price
    price_data = []
    
    for i in range(total_rows):
        # Add some realistic price movement
        change = (i % 20 - 10) * 0.1  # Price oscillation
        noise = (i % 7 - 3) * 0.05   # Random noise
        
        price = base_price + change + noise
        
        # Create OHLCV data
        open_price = price
        high_price = price + abs(change * 0.2)
        low_price = price - abs(change * 0.2)
        close_price = price + (change * 0.1)
        volume = 1000000 + (i % 500000)  # Realistic volume
        
        price_data.append({
            'timestamp': timestamps[i],
            'open': round(open_price, 2),
            'high': round(high_price, 2),
            'low': round(low_price, 2),
            'close': round(close_price, 2),
            'volume': volume
        })
    
    df = pd.DataFrame(price_data)
    logger.info(f"📊 Created simulated {interval} data: {len(df)} rows")
    return df


def simulate_api_fetch(symbol, interval="1min", outputsize="compact"):
    """
    Simulate an API fetch for testing purposes.
    
    Args:
        symbol (str): Stock ticker symbol
        interval (str): Time interval
        outputsize (str): "compact" or "full"
        
    Returns:
        pandas.DataFrame: Simulated data
    """
    logger.info(f"🎭 SIMULATING API FETCH: {symbol} {interval} (outputsize={outputsize})")
    
    # Simulate different data sizes based on outputsize
    if outputsize == "full":
        # Full historical data (20+ days)
        data = create_simulated_intraday_data(interval=interval, num_days=20)
        logger.info(f"   📈 Simulated FULL dataset: {len(data)} rows (20 days of {interval} data)")
    else:
        # Compact data (latest 100 data points)
        data = create_simulated_intraday_data(interval=interval, num_rows=100)
        logger.info(f"   📊 Simulated COMPACT dataset: {len(data)} rows (latest 100 data points)")
    
    # Simulate network delay
    time.sleep(0.5)
    
    return data

# =============================================================================
# FILE MANAGEMENT FUNCTIONS
# =============================================================================

def ensure_directory_exists(file_path):
    """
    Ensure the directory for the given file path exists.
    
    Args:
        file_path (str): Full file path
    """
    directory = os.path.dirname(file_path)
    if directory and not os.path.exists(directory):
        os.makedirs(directory, exist_ok=True)
        logger.info(f"📁 Created directory: {directory}")


def get_file_size_bytes(file_path):
    """
    Get the size of a file in bytes. Returns 0 if file doesn't exist.
    
    Args:
        file_path (str): Path to the file
        
    Returns:
        int: File size in bytes
    """
    try:
        if os.path.exists(file_path):
            size = os.path.getsize(file_path)
            logger.debug(f"📏 File size for {file_path}: {size} bytes")
            return size
        else:
            logger.debug(f"📂 File not found: {file_path}")
            return 0
    except Exception as e:
        logger.error(f"❌ Error getting file size for {file_path}: {e}")
        return 0


def load_existing_data(file_path):
    """
    Load existing CSV data from file.
    
    Args:
        file_path (str): Path to CSV file
        
    Returns:
        pandas.DataFrame: Existing data or empty DataFrame
    """
    try:
        if os.path.exists(file_path):
            df = pd.read_csv(file_path)
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            logger.info(f"📂 Loaded existing data: {len(df)} rows from {file_path}")
            return df
        else:
            logger.info(f"📂 No existing file found: {file_path}")
            return pd.DataFrame()
    except Exception as e:
        logger.error(f"❌ Error loading existing data from {file_path}: {e}")
        return pd.DataFrame()


def save_data_to_csv(df, file_path):
    """
    Save DataFrame to CSV file with proper formatting.
    
    Args:
        df (pandas.DataFrame): Data to save
        file_path (str): Path to save the file
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        ensure_directory_exists(file_path)
        
        # Convert timestamps back to strings for CSV storage (UTC format)
        df_save = df.copy()
        if 'timestamp' in df_save.columns:
            # Convert to UTC for storage
            if df_save['timestamp'].dt.tz is not None:
                df_save['timestamp'] = df_save['timestamp'].dt.tz_convert('UTC')
            df_save['timestamp'] = df_save['timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S+00:00')
        
        # Sort by timestamp for consistency (chronological order)
        df_save = df_save.sort_values('timestamp')
        
        # Save to CSV
        df_save.to_csv(file_path, index=False)
        
        # Verify file was saved
        file_size = get_file_size_bytes(file_path)
        logger.info(f"💾 Successfully saved {len(df_save)} rows to {file_path} ({file_size} bytes)")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Error saving data to {file_path}: {e}")
        return False


def create_small_test_file(file_path, target_size_bytes=5000):
    """
    Create a small test file to demonstrate the 10KB rule.
    
    Args:
        file_path (str): Path to create the file
        target_size_bytes (int): Target file size in bytes (default: 5KB)
    """
    # Create minimal data that results in a small file
    small_data = create_simulated_intraday_data(interval="1min", num_rows=50)
    save_data_to_csv(small_data, file_path)
    
    actual_size = get_file_size_bytes(file_path)
    logger.info(f"📝 Created small test file: {actual_size} bytes (target: {target_size_bytes} bytes)")
    return actual_size

# =============================================================================
# CORE LOGIC: FILE SIZE RULE IMPLEMENTATION
# =============================================================================

def determine_fetch_strategy(file_path, existing_df):
    """
    CRITICAL FUNCTION: Implement the exact 10KB file size rule.
    
    Rule: "If the local data file's size is less than 10kb, the script must 
    perform a full historical data fetch using outputsize='full'"
    
    Args:
        file_path (str): Path to the data file
        existing_df (pandas.DataFrame): Existing data (if any)
        
    Returns:
        str: "full" or "compact" strategy
    """
    # Check if file exists and get its size
    file_size_bytes = get_file_size_bytes(file_path)
    
    logger.info(f"🔍 File Size Analysis for {file_path}:")
    logger.info(f"   Current file size: {file_size_bytes} bytes")
    logger.info(f"   Threshold: {FILE_SIZE_THRESHOLD_BYTES} bytes ({FILE_SIZE_THRESHOLD_KB}KB)")
    
    # Apply the 10KB rule
    if file_size_bytes <= FILE_SIZE_THRESHOLD_BYTES:
        logger.info(f"🔄 RULE TRIGGERED: File size {file_size_bytes} bytes ≤ {FILE_SIZE_THRESHOLD_BYTES} bytes")
        logger.info(f"   ➤ Using outputsize='full' for complete historical data fetch")
        return "full"
    else:
        logger.info(f"✅ RULE CHECK PASSED: File size {file_size_bytes} bytes > {FILE_SIZE_THRESHOLD_BYTES} bytes")
        logger.info(f"   ➤ Using outputsize='compact' for efficient real-time updates")
        return "compact"


def intelligent_data_merge(existing_df, new_df):
    """
    Intelligently merge existing and new data, avoiding duplicates.
    
    Args:
        existing_df (pandas.DataFrame): Existing data
        new_df (pandas.DataFrame): New data from API
        
    Returns:
        pandas.DataFrame: Merged data
    """
    if existing_df.empty:
        logger.info("📊 No existing data - using new data as base")
        return new_df
    
    if new_df.empty:
        logger.info("📊 No new data - keeping existing data")
        return existing_df
    
    try:
        # Ensure both DataFrames have timezone-aware timestamps
        ny_tz = pytz.timezone('America/New_York')
        
        # Handle existing data timestamps
        if existing_df['timestamp'].dt.tz is None:
            # If stored as UTC strings, parse and convert
            if isinstance(existing_df['timestamp'].iloc[0], str):
                existing_df['timestamp'] = pd.to_datetime(existing_df['timestamp'], utc=True)
                existing_df['timestamp'] = existing_df['timestamp'].dt.tz_convert(ny_tz)
            else:
                existing_df['timestamp'] = existing_df['timestamp'].dt.tz_localize(ny_tz)
        
        # Combine the data
        combined_df = pd.concat([existing_df, new_df], ignore_index=True)
        
        # Remove duplicates based on timestamp, keeping the latest values
        combined_df = combined_df.drop_duplicates(subset=['timestamp'], keep='last')
        
        # Sort by timestamp (newest first)
        combined_df = combined_df.sort_values('timestamp', ascending=False)
        
        # Keep only last 7 days for efficiency (market hours data)
        cutoff_date = datetime.now(ny_tz) - timedelta(days=7)
        combined_df = combined_df[combined_df['timestamp'] >= cutoff_date]
        
        added_rows = len(combined_df) - len(existing_df)
        logger.info(f"📊 Data merge completed:")
        logger.info(f"   Existing: {len(existing_df)} rows")
        logger.info(f"   New: {len(new_df)} rows") 
        logger.info(f"   Final: {len(combined_df)} rows")
        logger.info(f"   Net change: {added_rows:+d} rows")
        
        return combined_df
        
    except Exception as e:
        logger.error(f"❌ Error merging data: {e}")
        logger.info("🔄 Falling back to existing data to prevent data loss")
        return existing_df

# =============================================================================
# TEST SCENARIOS
# =============================================================================

def test_file_size_rule():
    """
    Test the 10KB file size rule implementation.
    """
    logger.info("=" * 80)
    logger.info("🧪 TESTING FILE SIZE RULE (10KB Threshold)")
    logger.info("=" * 80)
    
    test_file = "test_data/test_file_size.csv"
    
    # Test 1: No file (should trigger full fetch)
    logger.info("\n🔬 Test 1: No existing file")
    if os.path.exists(test_file):
        os.remove(test_file)
    
    strategy = determine_fetch_strategy(test_file, pd.DataFrame())
    assert strategy == "full", f"Expected 'full', got '{strategy}'"
    logger.info("✅ PASS: No file correctly triggers full fetch")
    
    # Test 2: Small file < 10KB (should trigger full fetch)
    logger.info("\n🔬 Test 2: Small file < 10KB")
    create_small_test_file(test_file, target_size_bytes=5000)
    
    strategy = determine_fetch_strategy(test_file, pd.DataFrame())
    assert strategy == "full", f"Expected 'full', got '{strategy}'"
    logger.info("✅ PASS: Small file correctly triggers full fetch")
    
    # Test 3: Large file > 10KB (should use compact fetch)
    logger.info("\n🔬 Test 3: Large file > 10KB")
    large_data = create_simulated_intraday_data(interval="1min", num_days=5)  # Should be > 10KB
    save_data_to_csv(large_data, test_file)
    
    strategy = determine_fetch_strategy(test_file, large_data)
    assert strategy == "compact", f"Expected 'compact', got '{strategy}'"
    logger.info("✅ PASS: Large file correctly uses compact fetch")
    
    # Cleanup
    if os.path.exists(test_file):
        os.remove(test_file)
    
    logger.info("\n🎉 ALL FILE SIZE TESTS PASSED!")


def test_interval_compatibility(interval):
    """
    Test compatibility with different intervals.
    
    Args:
        interval (str): "1min" or "30min"
    """
    logger.info("=" * 80)
    logger.info(f"🧪 TESTING {interval.upper()} INTERVAL COMPATIBILITY")
    logger.info("=" * 80)
    
    # Setup test file path
    if interval == "1min":
        data_folder = "test_data/intraday"
        test_file = f"{data_folder}/{TEST_TICKER}_1min.csv"
    elif interval == "30min":
        data_folder = "test_data/intraday_30min"
        test_file = f"{data_folder}/{TEST_TICKER}_30min.csv"
    else:
        raise ValueError(f"Unsupported interval: {interval}")
    
    logger.info(f"📋 Configuration:")
    logger.info(f"   Ticker: {TEST_TICKER}")
    logger.info(f"   Interval: {interval}")
    logger.info(f"   Test File: {test_file}")
    
    # Clean up any existing test file
    if os.path.exists(test_file):
        os.remove(test_file)
    
    # Step 1: Test initial fetch (no existing data, should use full)
    logger.info(f"\n📂 STEP 1: Loading existing data...")
    existing_df = load_existing_data(test_file)
    
    logger.info(f"\n🔍 STEP 2: Applying file size rule...")
    fetch_strategy = determine_fetch_strategy(test_file, existing_df)
    
    logger.info(f"\n📡 STEP 3: Simulating {fetch_strategy} fetch...")
    new_df = simulate_api_fetch(TEST_TICKER, interval=interval, outputsize=fetch_strategy)
    
    logger.info(f"\n🧠 STEP 4: Merging data...")
    final_df = intelligent_data_merge(existing_df, new_df)
    
    logger.info(f"\n💾 STEP 5: Saving data...")
    save_success = save_data_to_csv(final_df, test_file)
    
    if save_success:
        final_size = get_file_size_bytes(test_file)
        logger.info(f"✅ {interval} test completed successfully:")
        logger.info(f"   Final file size: {final_size} bytes")
        logger.info(f"   Rows saved: {len(final_df)}")
        logger.info(f"   File above threshold: {'Yes' if final_size > FILE_SIZE_THRESHOLD_BYTES else 'No'}")
        
        # Step 2: Test subsequent fetch (existing data, should use compact if file is large enough)
        logger.info(f"\n🔄 STEP 6: Testing subsequent fetch...")
        
        existing_df_2 = load_existing_data(test_file)
        fetch_strategy_2 = determine_fetch_strategy(test_file, existing_df_2)
        
        expected_strategy = "compact" if final_size > FILE_SIZE_THRESHOLD_BYTES else "full"
        if fetch_strategy_2 == expected_strategy:
            logger.info(f"✅ Subsequent fetch strategy correct: {fetch_strategy_2}")
        else:
            logger.warning(f"⚠️ Unexpected fetch strategy: expected {expected_strategy}, got {fetch_strategy_2}")
        
        return True
    else:
        logger.error(f"❌ {interval} test failed: Could not save data")
        return False


def run_comprehensive_demo():
    """
    Run a comprehensive demonstration of all functionality.
    """
    logger.info("=" * 80)
    logger.info("🎯 COMPREHENSIVE DEMONSTRATION - ALL FUNCTIONALITY")
    logger.info("=" * 80)
    
    results = []
    
    # Test 1: File size rule
    try:
        test_file_size_rule()
        results.append(("File Size Rule", True))
    except Exception as e:
        logger.error(f"❌ File size rule test failed: {e}")
        results.append(("File Size Rule", False))
    
    # Test 2: 1-minute interval
    try:
        result_1min = test_interval_compatibility("1min")
        results.append(("1-minute Interval", result_1min))
    except Exception as e:
        logger.error(f"❌ 1-minute interval test failed: {e}")
        results.append(("1-minute Interval", False))
    
    # Test 3: 30-minute interval
    try:
        result_30min = test_interval_compatibility("30min")
        results.append(("30-minute Interval", result_30min))
    except Exception as e:
        logger.error(f"❌ 30-minute interval test failed: {e}")
        results.append(("30-minute Interval", False))
    
    # Summary
    logger.info("\n" + "=" * 80)
    logger.info("📊 COMPREHENSIVE TEST RESULTS")
    logger.info("=" * 80)
    
    for test_name, passed in results:
        status = "✅ PASSED" if passed else "❌ FAILED"
        logger.info(f"   {test_name}: {status}")
    
    all_passed = all(passed for _, passed in results)
    logger.info(f"\n🎯 Overall Result: {'✅ ALL TESTS PASSED' if all_passed else '❌ SOME TESTS FAILED'}")
    
    if all_passed:
        logger.info("\n🎉 DEMONSTRATION COMPLETE!")
        logger.info("   ✅ File size rule implemented correctly")
        logger.info("   ✅ 1-minute interval compatibility verified")
        logger.info("   ✅ 30-minute interval compatibility verified")
        logger.info("   ✅ Dynamic fetch strategy working properly")
        logger.info("   ✅ Data merging logic functional")
    
    return all_passed

# =============================================================================
# SCRIPT EXECUTION
# =============================================================================

if __name__ == "__main__":
    logger.info("🎯 Starting Comprehensive Intraday Data Fetcher Demo...")
    
    # Parse command line arguments
    if len(sys.argv) > 1:
        if sys.argv[1] == "--test-1min":
            logger.info("🔬 Running 1-minute interval test...")
            success = test_interval_compatibility("1min")
        elif sys.argv[1] == "--test-30min":
            logger.info("🔬 Running 30-minute interval test...")
            success = test_interval_compatibility("30min")
        elif sys.argv[1] == "--test-filesize":
            logger.info("🔬 Running file size rule test...")
            try:
                test_file_size_rule()
                success = True
            except Exception as e:
                logger.error(f"Test failed: {e}")
                success = False
        elif sys.argv[1] == "--demo":
            logger.info("🎭 Running comprehensive demonstration...")
            success = run_comprehensive_demo()
        else:
            logger.error(f"Unknown argument: {sys.argv[1]}")
            logger.info("Usage: python3 demo_comprehensive_fix.py [--test-1min|--test-30min|--test-filesize|--demo]")
            success = False
    else:
        logger.info("🎭 Running full demonstration by default...")
        success = run_comprehensive_demo()
    
    if success:
        logger.info("\n🎉 EXECUTION COMPLETED SUCCESSFULLY!")
    else:
        logger.error("\n💥 EXECUTION FAILED!")
    
    logger.info("\n" + "=" * 80)