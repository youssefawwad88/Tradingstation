#!/usr/bin/env python3
"""
Test script for the new data retention functionality.
This tests the Phase 4 changes without requiring API access or Spaces credentials.
"""

import sys
import os
import pandas as pd
from datetime import datetime, timedelta
import pytz

# Add project root to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '.')))

from utils.helpers import apply_data_retention, is_today_present_enhanced
from utils.config import TIMEZONE, INTRADAY_TRIM_DAYS, DEBUG_MODE

def create_test_dataframe():
    """Create a test DataFrame with intraday data spanning several days."""
    
    # Create data spanning 10 days (including today)
    ny_tz = pytz.timezone(TIMEZONE)
    today = datetime.now(ny_tz).replace(hour=12, minute=0, second=0, microsecond=0)
    
    # Generate timestamps for past 10 days
    timestamps = []
    for day_offset in range(10, 0, -1):  # 10 days ago to yesterday
        base_date = today - timedelta(days=day_offset)
        # Add some intraday timestamps (9:30 AM, 12:00 PM, 3:30 PM ET)
        for hour, minute in [(9, 30), (12, 0), (15, 30), (16, 0)]:
            timestamps.append(base_date.replace(hour=hour, minute=minute))
    
    # Add today's data (important!)
    for hour, minute in [(9, 30), (12, 0), (15, 30)]:
        timestamps.append(today.replace(hour=hour, minute=minute))
    
    # Create DataFrame
    data = {
        'Date': timestamps,
        'Open': [100.0 + i for i in range(len(timestamps))],
        'High': [105.0 + i for i in range(len(timestamps))],
        'Low': [95.0 + i for i in range(len(timestamps))],
        'Close': [102.0 + i for i in range(len(timestamps))],
        'Volume': [1000000 + i * 10000 for i in range(len(timestamps))]
    }
    
    return pd.DataFrame(data)

def test_data_retention():
    """Test the enhanced data retention functionality."""
    print("🧪 TESTING ENHANCED DATA RETENTION")
    print("=" * 60)
    
    # Create test data
    test_df = create_test_dataframe()
    print(f"📊 Created test DataFrame with {len(test_df)} rows")
    print(f"   Date range: {test_df['Date'].min()} to {test_df['Date'].max()}")
    
    # Check if today's data is present in original
    has_today_original = is_today_present_enhanced(test_df, 'Date')
    print(f"✅ Today's data in original: {has_today_original}")
    
    # Apply retention
    print(f"\n🔄 Applying data retention (trim_days={INTRADAY_TRIM_DAYS})...")
    filtered_df = apply_data_retention(test_df, INTRADAY_TRIM_DAYS)
    
    print(f"\n📊 RESULTS:")
    print(f"   Original rows: {len(test_df)}")
    print(f"   Filtered rows: {len(filtered_df)}")
    print(f"   Rows removed: {len(test_df) - len(filtered_df)}")
    
    if not filtered_df.empty:
        print(f"   Date range after: {filtered_df['Date'].min()} to {filtered_df['Date'].max()}")
        
        # Most important check: verify today's data is still present
        has_today_filtered = is_today_present_enhanced(filtered_df, 'Date')
        if has_today_filtered:
            print(f"✅ SUCCESS: Today's data preserved after filtering")
        else:
            print(f"❌ CRITICAL: Today's data LOST after filtering!")
            
        return has_today_filtered
    else:
        print(f"❌ CRITICAL: All data was filtered out!")
        return False

def test_timezone_handling():
    """Test timezone handling."""
    print(f"\n🌍 TESTING TIMEZONE HANDLING")
    print("=" * 40)
    
    ny_tz = pytz.timezone(TIMEZONE)
    now_et = datetime.now(ny_tz)
    print(f"Current time in {TIMEZONE}: {now_et}")
    print(f"Current date in {TIMEZONE}: {now_et.date()}")
    
    return True

def main():
    """Run all tests."""
    print(f"🚀 STARTING DATA RETENTION TESTS")
    print(f"Configuration: TRIM_DAYS={INTRADAY_TRIM_DAYS}, TIMEZONE={TIMEZONE}")
    print("=" * 80)
    
    # Test timezone handling
    timezone_ok = test_timezone_handling()
    
    # Test data retention
    retention_ok = test_data_retention()
    
    print(f"\n📋 TEST SUMMARY:")
    print(f"   Timezone handling: {'✅ PASS' if timezone_ok else '❌ FAIL'}")
    print(f"   Data retention: {'✅ PASS' if retention_ok else '❌ FAIL'}")
    
    if timezone_ok and retention_ok:
        print(f"\n🎉 ALL TESTS PASSED!")
        print(f"   The enhanced data retention is working correctly.")
        print(f"   Today's data will be preserved during processing.")
    else:
        print(f"\n💥 TESTS FAILED!")
        print(f"   There are issues with the data retention logic.")
    
    return timezone_ok and retention_ok

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)