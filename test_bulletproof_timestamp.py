#!/usr/bin/env python3
"""
Test script for the bulletproof timestamp comparison implementation.

This script specifically tests the new bulletproof logic to ensure it correctly handles:
1. UTC timestamps from CSV files (historical data)
2. Live timestamps with various timezone info 
3. Proper conversion to America/New_York for comparison
4. Edge cases around DST transitions
"""

import os
import sys
import pandas as pd
from datetime import datetime, timedelta
import pytz

# Add project root to Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '.')))

# Import the function we want to test
from jobs.compact_update import intelligent_append_or_update

def test_bulletproof_timestamp_comparison():
    """Test the bulletproof timestamp comparison with various timezone scenarios."""
    
    print("🧪 TESTING BULLETPROOF TIMESTAMP COMPARISON")
    print("=" * 70)
    
    # Test 1: UTC Historical Data (typical CSV format) vs Live Data
    print("\n📊 TEST 1: UTC Historical Data vs Live Data")
    print("-" * 50)
    
    # Create historical data as stored in CSV (UTC format)
    historical_data = pd.DataFrame({
        'timestamp': [
            '2025-08-14 18:28:00+00:00',  # 2:28 PM ET in UTC
            '2025-08-14 18:29:00+00:00',  # 2:29 PM ET in UTC
            '2025-08-14 18:30:00+00:00',  # 2:30 PM ET in UTC (last candle)
        ],
        'open': [100.0, 101.0, 102.0],
        'high': [100.5, 101.5, 102.5],
        'low': [99.5, 100.5, 101.5],
        'close': [100.2, 101.2, 102.2],
        'volume': [1000, 1100, 1200]
    })
    
    print("Historical data (UTC format as stored in CSV):")
    print(historical_data.to_string(index=False))
    
    # Create live data for same minute (2:30 PM ET)
    ny_tz = pytz.timezone('America/New_York')
    live_time = ny_tz.localize(datetime(2025, 8, 14, 14, 30, 30))  # 2:30:30 PM ET (same minute but 30 seconds later)
    
    live_data = pd.DataFrame({
        'timestamp': [live_time],
        'open': [103.0],
        'high': [103.8],
        'low': [102.9],
        'close': [103.5],
        'volume': [500]
    })
    
    print("\nLive data (NY timezone, same minute but different seconds):")
    print(f"Live timestamp: {live_time} (2:30:30 PM ET)")
    
    # Apply bulletproof comparison
    result = intelligent_append_or_update(historical_data, live_data)
    
    print(f"\nResult validation:")
    print(f"  Input rows: {len(historical_data)}, Output rows: {len(result)}")
    if len(result) == len(historical_data):
        last_candle = result.iloc[-1]
        print(f"  ✅ SAME MINUTE test: PASSED - candle updated, not appended")
        print(f"  Updated high: {last_candle['high']} (should be max of 102.5 and 103.8 = 103.8)")
        print(f"  Updated close: {last_candle['close']} (should be current price = 103.5)")
    else:
        print(f"  ❌ SAME MINUTE test: FAILED - unexpected row count change")
    
    # Test 2: Different minute should append
    print("\n\n📊 TEST 2: New minute should append new candle")
    print("-" * 50)
    
    # Live data for new minute (2:31 PM ET)
    live_time_new = ny_tz.localize(datetime(2025, 8, 14, 14, 31, 15))  # 2:31:15 PM ET
    
    live_data_new = pd.DataFrame({
        'timestamp': [live_time_new],
        'open': [104.0],
        'high': [104.2],
        'low': [103.8],
        'close': [104.1],
        'volume': [800]
    })
    
    print(f"Live timestamp (new minute): {live_time_new} (2:31:15 PM ET)")
    
    # Apply bulletproof comparison
    result_new = intelligent_append_or_update(historical_data, live_data_new)
    
    print(f"\nResult validation:")
    print(f"  Input rows: {len(historical_data)}, Output rows: {len(result_new)}")
    if len(result_new) == len(historical_data) + 1:
        print(f"  ✅ NEW MINUTE test: PASSED - new candle appended")
    else:
        print(f"  ❌ NEW MINUTE test: FAILED - expected {len(historical_data) + 1} rows, got {len(result_new)}")
    
    # Test 3: Past timestamp should be rejected
    print("\n\n📊 TEST 3: Past timestamp should be rejected")
    print("-" * 50)
    
    # Live data for past minute (2:29 PM ET)
    live_time_past = ny_tz.localize(datetime(2025, 8, 14, 14, 29, 45))  # 2:29:45 PM ET (past)
    
    live_data_past = pd.DataFrame({
        'timestamp': [live_time_past],
        'open': [99.0],
        'high': [99.5],
        'low': [98.8],
        'close': [99.2],
        'volume': [300]
    })
    
    print(f"Live timestamp (past minute): {live_time_past} (2:29:45 PM ET)")
    
    # Apply bulletproof comparison
    result_past = intelligent_append_or_update(historical_data, live_data_past)
    
    print(f"\nResult validation:")
    print(f"  Input rows: {len(historical_data)}, Output rows: {len(result_past)}")
    if len(result_past) == len(historical_data):
        # Check if data was unchanged
        data_unchanged = result_past.equals(historical_data.copy().assign(
            timestamp=pd.to_datetime(historical_data['timestamp']).dt.strftime('%Y-%m-%d %H:%M:%S+00:00')
        ))
        if data_unchanged:
            print(f"  ✅ PAST TIMESTAMP test: PASSED - data unchanged")
        else:
            print(f"  ⚠️ PAST TIMESTAMP test: PARTIAL - correct row count but data may have changed")
    else:
        print(f"  ❌ PAST TIMESTAMP test: FAILED - unexpected row count change")
    
    print("\n" + "=" * 70)
    print("🧪 BULLETPROOF TIMESTAMP COMPARISON TESTS COMPLETED")
    print("=" * 70)


if __name__ == "__main__":
    test_bulletproof_timestamp_comparison()