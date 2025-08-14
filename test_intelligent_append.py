#!/usr/bin/env python3
"""
Test script for the new "Intelligent Append & Resample" architecture.

This script tests the core logic without requiring API keys or network access.
"""

import os
import sys
import pandas as pd
from datetime import datetime, timedelta
import pytz

# Add project root to Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '.')))

# Import the functions we want to test
from jobs.compact_update import convert_global_quote_to_dataframe, intelligent_append_or_update, resample_1min_to_30min

def test_intelligent_append_logic():
    """Test the intelligent append vs update logic."""
    
    print("🧪 TESTING INTELLIGENT APPEND & UPDATE LOGIC")
    print("=" * 60)
    
    # Setup timezone for testing
    ny_tz = pytz.timezone('America/New_York')
    base_time = datetime(2025, 8, 14, 14, 30, 0)  # 2:30 PM ET
    base_time_ny = ny_tz.localize(base_time)
    
    # Test 1: Create existing 1-minute data (last candle at 2:30 PM)
    print("\n📊 TEST 1: Creating sample existing 1-minute data")
    print("-" * 50)
    
    existing_data = pd.DataFrame({
        'timestamp': [
            base_time_ny.replace(minute=28).strftime('%Y-%m-%d %H:%M:%S'),  # 2:28 PM
            base_time_ny.replace(minute=29).strftime('%Y-%m-%d %H:%M:%S'),  # 2:29 PM
            base_time_ny.replace(minute=30).strftime('%Y-%m-%d %H:%M:%S'),  # 2:30 PM (last candle)
        ],
        'open': [100.0, 101.0, 102.0],
        'high': [100.5, 101.5, 102.5],
        'low': [99.5, 100.5, 101.5],
        'close': [100.2, 101.2, 102.2],
        'volume': [1000, 1100, 1200]
    })
    
    print("Existing 1-minute data:")
    print(existing_data.to_string(index=False))
    
    # Test 2A: SAME MINUTE - Update existing candle
    print("\n\n📊 TEST 2A: SAME MINUTE - Should update existing candle")
    print("-" * 50)
    
    # Create new quote data for same minute (2:30 PM) with higher price
    # Manually create the dataframe with the same timestamp as last existing candle
    new_df_same = pd.DataFrame({
        'timestamp': [base_time_ny.replace(minute=30).strftime('%Y-%m-%d %H:%M:%S')],  # Same as last candle
        'open': [103.0],
        'high': [103.5],  # Higher than existing high (102.5)
        'low': [102.8],   # Higher than existing low (101.5)
        'close': [103.0], # Higher than existing close (102.2)
        'volume': [500]
    })
    
    print("New quote data (same minute):")
    print(new_df_same.to_string(index=False))
    
    # Apply intelligent append logic
    result_same = intelligent_append_or_update(existing_data, new_df_same)
    print("\nResult after intelligent update (same minute):")
    print(result_same.to_string(index=False))
    
    # Validate: Should have same number of rows, but last candle should be updated
    print(f"\nValidation:")
    print(f"  Original rows: {len(existing_data)}, Result rows: {len(result_same)}")
    if len(result_same) == len(existing_data):
        last_candle = result_same.iloc[-1]
        expected_high = max(102.5, 103.5)  # max of existing high and new high
        expected_low = min(101.5, 102.8)   # min of existing low and new low  
        expected_close = 103.0              # new close price
        print(f"  Last candle high: {last_candle['high']} (should be max of 102.5 and 103.5 = {expected_high})")
        print(f"  Last candle low: {last_candle['low']} (should be min of 101.5 and 102.8 = {expected_low})")
        print(f"  Last candle close: {last_candle['close']} (should be current price = {expected_close})")
        
        high_correct = last_candle['high'] == expected_high
        low_correct = last_candle['low'] == expected_low
        close_correct = last_candle['close'] == expected_close
        
        print(f"  ✅ SAME MINUTE test: {'PASSED' if high_correct and low_correct and close_correct else 'FAILED'}")
        if not (high_correct and low_correct and close_correct):
            print(f"    High: {'✅' if high_correct else '❌'}, Low: {'✅' if low_correct else '❌'}, Close: {'✅' if close_correct else '❌'}")
    else:
        print(f"  ❌ SAME MINUTE test: FAILED - row count changed")
    
    # Test 2B: NEW MINUTE - Append new candle
    print("\n\n📊 TEST 2B: NEW MINUTE - Should append new candle")
    print("-" * 50)
    
    # Create mock GLOBAL_QUOTE data for new minute (2:31 PM)
    new_minute_time = base_time_ny.replace(minute=31)
    mock_quote_new_minute = {
        'price': 104.0,
        'open': 103.5,
        'high': 104.0,
        'low': 103.5,
        'volume': 800
    }
    
    # Manually create new_df for new minute to simulate the time difference
    new_df_new = pd.DataFrame({
        'timestamp': [new_minute_time.strftime('%Y-%m-%d %H:%M:%S')],
        'open': [mock_quote_new_minute['price']],
        'high': [mock_quote_new_minute['price']],
        'low': [mock_quote_new_minute['price']],
        'close': [mock_quote_new_minute['price']],
        'volume': [mock_quote_new_minute['volume']]
    })
    
    print("New quote data (new minute):")
    print(new_df_new.to_string(index=False))
    
    # Apply intelligent append logic
    result_new = intelligent_append_or_update(existing_data, new_df_new)
    print("\nResult after intelligent update (new minute):")
    print(result_new.to_string(index=False))
    
    # Validate: Should have one more row
    print(f"\nValidation:")
    print(f"  Original rows: {len(existing_data)}, Result rows: {len(result_new)}")
    if len(result_new) == len(existing_data) + 1:
        print(f"  ✅ NEW MINUTE test: PASSED - new candle appended")
    else:
        print(f"  ❌ NEW MINUTE test: FAILED - incorrect row count")
    
    # Test 3: Resampling to 30-minute data
    print("\n\n📊 TEST 3: Resampling 1-minute to 30-minute data")
    print("-" * 50)
    
    # Use the result with new minute for resampling test
    resampled_30min = resample_1min_to_30min(result_new)
    print("Resampled 30-minute data:")
    print(resampled_30min.to_string(index=False))
    
    # Validate resampling
    print(f"\nValidation:")
    print(f"  1-minute rows: {len(result_new)}, 30-minute rows: {len(resampled_30min)}")
    if not resampled_30min.empty:
        print(f"  ✅ RESAMPLING test: PASSED - 30-minute data created")
    else:
        print(f"  ❌ RESAMPLING test: FAILED - no 30-minute data")
    
    print("\n" + "=" * 60)
    print("🧪 INTELLIGENT APPEND & UPDATE TESTS COMPLETED")
    print("=" * 60)


if __name__ == "__main__":
    test_intelligent_append_logic()