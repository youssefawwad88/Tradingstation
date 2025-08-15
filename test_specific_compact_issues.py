#!/usr/bin/env python3
"""
Specific Test to Identify Compact Fetch Issues

This test focuses on identifying why compact data is not generating today's data.
We'll test both implementations and compare them.
"""

import pandas as pd
import sys
import os
from datetime import datetime, timedelta
import logging
import pytz

# Add project root to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '.')))

from utils.config import TIMEZONE

# Set up logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_today_presence_logic():
    """Test the logic for detecting today's data."""
    logger.info("=== Testing Today's Data Presence Logic ===")
    
    ny_tz = pytz.timezone(TIMEZONE)
    
    # Test 1: Create test data with today's date
    today = datetime.now(ny_tz)
    yesterday = today - timedelta(days=1)
    
    # Create test dataframe with mixed dates
    test_data = pd.DataFrame({
        'timestamp': [
            yesterday.replace(hour=9, minute=30),
            yesterday.replace(hour=15, minute=30),
            today.replace(hour=9, minute=30),  # This should be detected as "today"
            today.replace(hour=15, minute=30)   # This should be detected as "today"
        ],
        'open': [100, 101, 102, 103],
        'high': [101, 102, 103, 104],
        'low': [99, 100, 101, 102],
        'close': [100.5, 101.5, 102.5, 103.5],
        'volume': [1000000, 1100000, 1200000, 1300000]
    })
    
    logger.info(f"Created test data with {len(test_data)} rows")
    logger.info(f"Today's date: {today.date()}")
    
    # Test with different column names and formats
    for timestamp_col in ['timestamp', 'Date']:
        test_df = test_data.copy()
        if timestamp_col != 'timestamp':
            test_df = test_df.rename(columns={'timestamp': timestamp_col})
        
        logger.info(f"\nTesting with column name: {timestamp_col}")
        
        # Test the is_today_present function
        try:
            from utils.helpers import is_today_present
            result = is_today_present(test_df, timestamp_col)
            logger.info(f"is_today_present result: {result}")
            
            # Manual check
            df_copy = test_df.copy()
            df_copy[timestamp_col] = pd.to_datetime(df_copy[timestamp_col])
            df_copy['date'] = df_copy[timestamp_col].dt.date
            today_rows = df_copy[df_copy['date'] == today.date()]
            logger.info(f"Manual check - rows with today's date: {len(today_rows)}")
            
        except Exception as e:
            logger.error(f"Error testing is_today_present: {e}")

def test_7_day_filter_logic():
    """Test the 7-day filtering logic that might be removing today's data."""
    logger.info("=== Testing 7-Day Filter Logic ===")
    
    ny_tz = pytz.timezone(TIMEZONE)
    today = datetime.now(ny_tz)
    
    # Create test data spanning 10 days
    test_data = []
    for i in range(10, 0, -1):  # 10 days ago to today
        date = today - timedelta(days=i-1)  # i-1 so that i=1 gives today
        test_data.append({
            'timestamp': date.replace(hour=9, minute=30),
            'open': 100 + i,
            'high': 105 + i,
            'low': 95 + i,
            'close': 102 + i,
            'volume': 1000000 + i * 10000
        })
    
    df = pd.DataFrame(test_data)
    logger.info(f"Created test data with {len(df)} rows spanning 10 days")
    logger.info(f"Date range: {df['timestamp'].min()} to {df['timestamp'].max()}")
    
    # Test the 7-day filter like in fetch_intraday_compact.py
    seven_days_ago = datetime.now() - timedelta(days=7)  # Note: not timezone-aware
    logger.info(f"Seven days ago threshold (naive): {seven_days_ago}")
    
    # Convert to datetime (like the real code does)
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    
    # Apply filter
    filtered_df = df[df['timestamp'] >= seven_days_ago]
    logger.info(f"After 7-day filter: {len(filtered_df)} rows")
    logger.info(f"Filtered date range: {filtered_df['timestamp'].min()} to {filtered_df['timestamp'].max()}")
    
    # Check if today's data is preserved
    today_date = today.date()
    filtered_df['date'] = filtered_df['timestamp'].dt.date
    today_in_filtered = filtered_df[filtered_df['date'] == today_date]
    logger.info(f"Today's data in filtered result: {len(today_in_filtered)} rows")
    
    if len(today_in_filtered) == 0:
        logger.error("❌ CRITICAL: 7-day filter is removing today's data!")
        logger.info("Investigating timezone mismatch...")
        
        # Test with timezone-aware threshold
        seven_days_ago_tz = today - timedelta(days=7)  # timezone-aware
        logger.info(f"Seven days ago threshold (timezone-aware): {seven_days_ago_tz}")
        
        # Convert df timestamps to timezone-aware for proper comparison
        df_tz = df.copy()
        df_tz['timestamp'] = df_tz['timestamp'].dt.tz_localize(ny_tz)
        filtered_df_tz = df_tz[df_tz['timestamp'] >= seven_days_ago_tz]
        logger.info(f"With timezone-aware filter: {len(filtered_df_tz)} rows")
        
        today_in_filtered_tz = filtered_df_tz[filtered_df_tz['timestamp'].dt.date == today_date]
        logger.info(f"Today's data with timezone-aware filter: {len(today_in_filtered_tz)} rows")

def test_timestamp_column_detection():
    """Test timestamp column detection logic."""
    logger.info("=== Testing Timestamp Column Detection ===")
    
    # Test different column names that might be used
    test_columns = ['timestamp', 'Date', 'datetime', 'time']
    
    for col_name in test_columns:
        test_data = pd.DataFrame({
            col_name: [datetime.now()],
            'open': [100],
            'high': [105],
            'low': [95], 
            'close': [102],
            'volume': [1000000]
        })
        
        logger.info(f"\nTesting with column name: {col_name}")
        
        # Test the logic from fetch_intraday_compact.py
        timestamp_col = 'timestamp' if 'timestamp' in test_data.columns else 'Date'
        logger.info(f"Detected timestamp column: {timestamp_col}")
        
        if timestamp_col not in test_data.columns:
            logger.error(f"❌ Column detection failed for {col_name}")
        else:
            logger.info(f"✅ Column detection succeeded for {col_name}")

def test_api_response_simulation():
    """Simulate API responses to test data processing."""
    logger.info("=== Testing API Response Simulation ===")
    
    ny_tz = pytz.timezone(TIMEZONE)
    now = datetime.now(ny_tz)
    
    # Simulate what Alpha Vantage compact API would return (last 100 data points)
    # This would typically include recent data including today
    
    simulated_compact_data = []
    
    # Generate 100 data points going back from now
    for i in range(100, 0, -1):
        # Go back i minutes from now
        timestamp = now - timedelta(minutes=i)
        
        # Only include market hours (9:30 AM - 4:00 PM ET)
        if 9.5 <= timestamp.hour + timestamp.minute/60 <= 16:
            simulated_compact_data.append({
                'timestamp': timestamp,
                'open': 100 + i * 0.1,
                'high': 105 + i * 0.1,
                'low': 95 + i * 0.1,
                'close': 102 + i * 0.1,
                'volume': 100000 + i * 1000
            })
    
    if simulated_compact_data:
        compact_df = pd.DataFrame(simulated_compact_data)
        logger.info(f"Simulated compact API response: {len(compact_df)} rows")
        logger.info(f"Time range: {compact_df['timestamp'].min()} to {compact_df['timestamp'].max()}")
        
        # Check if today's data is in the simulation
        today_date = now.date()
        compact_df['date'] = compact_df['timestamp'].dt.date
        today_data = compact_df[compact_df['date'] == today_date]
        logger.info(f"Today's data in simulated compact response: {len(today_data)} rows")
        
        if len(today_data) > 0:
            logger.info("✅ Today's data would be present in compact API response")
            logger.info(f"Sample today's data: {today_data.head(1)}")
        else:
            logger.warning("⚠️ No today's data in simulated compact response (might be outside market hours)")
    else:
        logger.warning("⚠️ No data generated (might be outside market hours)")

def test_append_logic_edge_cases():
    """Test edge cases in the append logic."""
    logger.info("=== Testing Append Logic Edge Cases ===")
    
    from fetch_intraday_compact import append_new_candles_smart
    
    ny_tz = pytz.timezone(TIMEZONE)
    now = datetime.now(ny_tz)
    
    # Test Case 1: Empty existing data
    logger.info("\nTest Case 1: Empty existing data")
    empty_df = pd.DataFrame()
    new_data = pd.DataFrame({
        'timestamp': [now],
        'open': [100], 'high': [105], 'low': [95], 'close': [102], 'volume': [1000000]
    })
    
    result1 = append_new_candles_smart(empty_df, new_data)
    logger.info(f"Result: {len(result1)} rows (expected: 1)")
    
    # Test Case 2: New data is empty
    logger.info("\nTest Case 2: New data is empty")
    existing_data = pd.DataFrame({
        'timestamp': [now - timedelta(hours=1)],
        'open': [100], 'high': [105], 'low': [95], 'close': [102], 'volume': [1000000]
    })
    empty_new = pd.DataFrame()
    
    result2 = append_new_candles_smart(existing_data, empty_new)
    logger.info(f"Result: {len(result2)} rows (expected: 1)")
    
    # Test Case 3: New data is older than existing
    logger.info("\nTest Case 3: New data is older than existing")
    old_new_data = pd.DataFrame({
        'timestamp': [now - timedelta(hours=2)],  # Older than existing
        'open': [99], 'high': [104], 'low': [94], 'close': [101], 'volume': [900000]
    })
    
    result3 = append_new_candles_smart(existing_data, old_new_data)
    logger.info(f"Result: {len(result3)} rows (expected: 1 - should not append old data)")
    
    # Test Case 4: Normal append (new data is newer)
    logger.info("\nTest Case 4: Normal append (new data is newer)")
    newer_data = pd.DataFrame({
        'timestamp': [now],  # Newer than existing
        'open': [103], 'high': [108], 'low': [98], 'close': [105], 'volume': [1200000]
    })
    
    result4 = append_new_candles_smart(existing_data, newer_data)
    logger.info(f"Result: {len(result4)} rows (expected: 2)")

def run_specific_compact_tests():
    """Run all specific compact fetch tests."""
    logger.info("🚀 Starting Specific Compact Fetch Issue Tests")
    logger.info("=" * 60)
    
    # Test 1: Today's data presence detection
    test_today_presence_logic()
    
    # Test 2: 7-day filter logic  
    test_7_day_filter_logic()
    
    # Test 3: Timestamp column detection
    test_timestamp_column_detection()
    
    # Test 4: API response simulation
    test_api_response_simulation()
    
    # Test 5: Append logic edge cases
    test_append_logic_edge_cases()
    
    logger.info("=" * 60)
    logger.info("🔍 Specific tests completed - check logs for issues")

if __name__ == "__main__":
    run_specific_compact_tests()