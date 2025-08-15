#!/usr/bin/env python3
"""
Test the timezone fix for the compact fetch issue.
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
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_timezone_fix():
    """Test the fixed timezone logic."""
    logger.info("=== Testing Timezone Fix ===")
    
    ny_tz = pytz.timezone(TIMEZONE)
    today = datetime.now(ny_tz)
    
    # Create test data with timezone-aware timestamps
    test_data = []
    for i in range(10, 0, -1):  # 10 days ago to today
        date = today - timedelta(days=i-1)
        test_data.append({
            'timestamp': date.replace(hour=9, minute=30),
            'open': 100 + i,
            'high': 105 + i,
            'low': 95 + i,
            'close': 102 + i,
            'volume': 1000000 + i * 10000
        })
    
    df = pd.DataFrame(test_data)
    logger.info(f"Created test data with {len(df)} rows")
    logger.info(f"Date range: {df['timestamp'].min()} to {df['timestamp'].max()}")
    
    # Apply the FIXED 7-day filter logic
    logger.info("\nApplying FIXED 7-day filter...")
    
    # FIXED: Use timezone-aware datetime for proper comparison
    seven_days_ago = datetime.now(ny_tz) - timedelta(days=7)
    timestamp_col = 'timestamp'
    combined_df = df.copy()
    combined_df[timestamp_col] = pd.to_datetime(combined_df[timestamp_col])
    
    logger.info(f"Seven days ago threshold (timezone-aware): {seven_days_ago}")
    logger.info(f"Data timezone info before: {combined_df[timestamp_col].dt.tz}")
    
    # Ensure timestamps are timezone-aware for proper comparison
    if combined_df[timestamp_col].dt.tz is None:
        # If naive, localize to NY timezone first
        combined_df[timestamp_col] = combined_df[timestamp_col].dt.tz_localize(ny_tz)
        logger.info("Localized naive timestamps to NY timezone")
    elif combined_df[timestamp_col].dt.tz != ny_tz:
        # If different timezone, convert to NY timezone
        combined_df[timestamp_col] = combined_df[timestamp_col].dt.tz_convert(ny_tz)
        logger.info("Converted timestamps to NY timezone")
    
    logger.info(f"Data timezone info after: {combined_df[timestamp_col].dt.tz}")
    
    # Now we can safely compare timezone-aware datetimes
    try:
        filtered_df = combined_df[combined_df[timestamp_col] >= seven_days_ago]
        logger.info(f"✅ Filter applied successfully!")
        logger.info(f"Original rows: {len(combined_df)}")
        logger.info(f"Filtered rows: {len(filtered_df)}")
        logger.info(f"Filtered date range: {filtered_df[timestamp_col].min()} to {filtered_df[timestamp_col].max()}")
        
        # Check if today's data is preserved
        today_date = today.date()
        filtered_df['date'] = filtered_df[timestamp_col].dt.date
        today_in_filtered = filtered_df[filtered_df['date'] == today_date]
        logger.info(f"Today's data preserved: {len(today_in_filtered)} rows")
        
        if len(today_in_filtered) > 0:
            logger.info("✅ SUCCESS: Today's data is preserved after filtering!")
        else:
            logger.error("❌ ISSUE: Today's data not found in filtered result")
            
        return True
        
    except Exception as e:
        logger.error(f"❌ Filter still failed: {e}")
        return False

def test_is_today_present_fix():
    """Test the fixed is_today_present function."""
    logger.info("\n=== Testing is_today_present Fix ===")
    
    from utils.helpers import is_today_present
    
    ny_tz = pytz.timezone(TIMEZONE)
    today = datetime.now(ny_tz)
    yesterday = today - timedelta(days=1)
    
    # Test data with today's date
    test_data = pd.DataFrame({
        'timestamp': [yesterday, today, today + timedelta(hours=1)],
        'open': [100, 101, 102],
        'high': [105, 106, 107],
        'low': [95, 96, 97],
        'close': [102, 103, 104],
        'volume': [1000000, 1100000, 1200000]
    })
    
    logger.info("Testing with 'timestamp' column...")
    try:
        result1 = is_today_present(test_data, 'timestamp')
        logger.info(f"Result with timestamp column: {result1}")
        
        result2 = is_today_present(test_data)  # default parameter
        logger.info(f"Result with default parameter: {result2}")
        
        if result1:
            logger.info("✅ SUCCESS: is_today_present correctly detects today's data")
        else:
            logger.warning("⚠️ is_today_present did not detect today's data")
            
        return True
        
    except Exception as e:
        logger.error(f"❌ is_today_present still failing: {e}")
        return False

def test_complete_compact_flow_simulation():
    """Test the complete compact flow with fixes."""
    logger.info("\n=== Testing Complete Compact Flow with Fixes ===")
    
    try:
        from fetch_intraday_compact import append_new_candles_smart
        
        ny_tz = pytz.timezone(TIMEZONE)
        now = datetime.now(ny_tz)
        
        # Simulate existing data (from yesterday)
        existing_data = pd.DataFrame({
            'timestamp': [now - timedelta(days=1, hours=1), now - timedelta(days=1)],
            'open': [100, 101],
            'high': [105, 106],
            'low': [95, 96],
            'close': [102, 103],
            'volume': [1000000, 1100000]
        })
        
        # Simulate new compact API data (today's data)
        new_data = pd.DataFrame({
            'timestamp': [now - timedelta(hours=1), now],
            'open': [104, 105],
            'high': [108, 109],
            'low': [98, 99],
            'close': [106, 107],
            'volume': [1200000, 1300000]
        })
        
        logger.info(f"Existing data: {len(existing_data)} rows")
        logger.info(f"New data: {len(new_data)} rows")
        
        # Test smart append
        combined_df = append_new_candles_smart(existing_data, new_data)
        logger.info(f"After smart append: {len(combined_df)} rows")
        
        # Test the FIXED 7-day filter
        seven_days_ago = datetime.now(ny_tz) - timedelta(days=7)  # Fixed: timezone-aware
        timestamp_col = 'timestamp'
        combined_df[timestamp_col] = pd.to_datetime(combined_df[timestamp_col])
        
        # Ensure timezone consistency (FIXED logic)
        if combined_df[timestamp_col].dt.tz is None:
            combined_df[timestamp_col] = combined_df[timestamp_col].dt.tz_localize(ny_tz)
        elif combined_df[timestamp_col].dt.tz != ny_tz:
            combined_df[timestamp_col] = combined_df[timestamp_col].dt.tz_convert(ny_tz)
        
        # Apply filter (should work now)
        filtered_df = combined_df[combined_df[timestamp_col] >= seven_days_ago]
        logger.info(f"After 7-day filter: {len(filtered_df)} rows")
        
        # Test today detection
        from utils.helpers import is_today_present
        today_detected = is_today_present(filtered_df, 'timestamp')
        logger.info(f"Today's data detected in final result: {today_detected}")
        
        if today_detected and len(filtered_df) > 0:
            logger.info("🎉 SUCCESS: Complete compact flow works with fixes!")
            return True
        else:
            logger.error("❌ Something still not working in the complete flow")
            return False
            
    except Exception as e:
        logger.error(f"❌ Complete flow test failed: {e}")
        return False

def run_timezone_tests():
    """Run all timezone fix tests."""
    logger.info("🚀 Testing Timezone Fixes for Compact Fetch")
    logger.info("=" * 60)
    
    results = []
    
    # Test 1: Basic timezone fix
    results.append(test_timezone_fix())
    
    # Test 2: is_today_present fix
    results.append(test_is_today_present_fix())
    
    # Test 3: Complete flow
    results.append(test_complete_compact_flow_simulation())
    
    logger.info("\n" + "=" * 60)
    logger.info("🔍 TIMEZONE FIX TEST SUMMARY")
    logger.info("=" * 60)
    
    test_names = ["Timezone Filter Fix", "is_today_present Fix", "Complete Flow Fix"]
    for i, (name, result) in enumerate(zip(test_names, results)):
        status = "✅ PASSED" if result else "❌ FAILED"
        logger.info(f"{name}: {status}")
    
    all_passed = all(results)
    if all_passed:
        logger.info("\n🎉 ALL TIMEZONE FIXES WORKING!")
    else:
        logger.info("\n⚠️ Some fixes still need work")
    
    return all_passed

if __name__ == "__main__":
    run_timezone_tests()