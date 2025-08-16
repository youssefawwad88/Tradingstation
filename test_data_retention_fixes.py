#!/usr/bin/env python3
"""
Test Data Retention Fixes
==========================

This test verifies that both the 1-minute and 30-minute data retention fixes work correctly:

1. 7-day retention fix for 1-minute data (timezone issue)
2. Proper trimming logic for 30-minute data

"""

import os
import sys
import pandas as pd
import logging
from datetime import datetime, timedelta
import pytz

# Add project root to Python path
sys.path.append(os.path.abspath(os.path.dirname(__file__)))

from utils.config import TIMEZONE
from jobs.full_fetch import trim_data_to_requirements

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def test_1min_timezone_fix():
    """Test the 1-minute data timezone comparison fix."""
    logger.info("🧪 Testing 1-Minute Timezone Fix")
    logger.info("=" * 50)
    
    # Create timezone-naive test data that would previously cause issues
    ny_tz = pytz.timezone(TIMEZONE)
    now = datetime.now(ny_tz)
    
    # Create test data spanning 10 days (some should be filtered out)
    test_data = []
    for days_back in range(10, 0, -1):  # 10 days ago to yesterday
        test_date = now - timedelta(days=days_back)
        # Create timezone-naive timestamp string (like from CSV)
        timestamp_str = test_date.strftime('%Y-%m-%d %H:%M:%S')
        
        test_data.append({
            'timestamp': timestamp_str,
            'open': 100.0,
            'high': 101.0,
            'low': 99.0,
            'close': 100.5,
            'volume': 1000000
        })
    
    df = pd.DataFrame(test_data)
    logger.info(f"📊 Created test data: {len(df)} rows")
    logger.info(f"   Date range: {df['timestamp'].min()} to {df['timestamp'].max()}")
    
    # Test the trimming function
    try:
        trimmed_df = trim_data_to_requirements(df, '1min')
        
        logger.info(f"✅ Trimming successful: {len(df)} → {len(trimmed_df)} rows")
        
        # Verify that data older than 7 days was filtered out
        trimmed_dates = pd.to_datetime(trimmed_df['timestamp']).dt.date
        oldest_date = trimmed_dates.min()
        newest_date = trimmed_dates.max()
        
        cutoff_date = (now - timedelta(days=7)).date()
        
        logger.info(f"   Oldest kept date: {oldest_date}")
        logger.info(f"   Newest kept date: {newest_date}")
        logger.info(f"   Expected cutoff: {cutoff_date}")
        
        # Verify cutoff logic
        if oldest_date >= cutoff_date:
            logger.info("✅ Cutoff logic working correctly")
            return True
        else:
            logger.error(f"❌ Data older than cutoff was kept: {oldest_date} < {cutoff_date}")
            return False
            
    except Exception as e:
        logger.error(f"❌ Trimming failed with error: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_30min_trimming_logic():
    """Test the 30-minute data trimming logic."""
    logger.info("\n🧪 Testing 30-Minute Trimming Logic")
    logger.info("=" * 50)
    
    # Create test data with more than 500 rows
    test_data = []
    base_time = datetime(2025, 8, 15, 9, 30)  # Start at 9:30 AM
    
    # Create 600 rows of 30-minute data (should be trimmed to 500)
    for i in range(600):
        timestamp = base_time + timedelta(minutes=30 * i)
        test_data.append({
            'timestamp': timestamp.strftime('%Y-%m-%d %H:%M:%S'),
            'open': 100.0 + (i * 0.1),
            'high': 101.0 + (i * 0.1),
            'low': 99.0 + (i * 0.1),
            'close': 100.5 + (i * 0.1),
            'volume': 1000000
        })
    
    df = pd.DataFrame(test_data)
    logger.info(f"📊 Created test data: {len(df)} rows")
    logger.info(f"   Date range: {df['timestamp'].min()} to {df['timestamp'].max()}")
    
    # Test the trimming function
    try:
        trimmed_df = trim_data_to_requirements(df, '30min')
        
        logger.info(f"✅ Trimming successful: {len(df)} → {len(trimmed_df)} rows")
        
        # Verify exactly 500 rows
        if len(trimmed_df) == 500:
            logger.info("✅ Correct number of rows retained (500)")
        else:
            logger.error(f"❌ Wrong number of rows: expected 500, got {len(trimmed_df)}")
            return False
        
        # Verify that the most recent data was kept
        original_newest = pd.to_datetime(df['timestamp']).max()
        trimmed_newest = pd.to_datetime(trimmed_df['timestamp']).max()
        
        if original_newest == trimmed_newest:
            logger.info("✅ Most recent data preserved")
            return True
        else:
            logger.error(f"❌ Most recent data not preserved: {original_newest} vs {trimmed_newest}")
            return False
            
    except Exception as e:
        logger.error(f"❌ Trimming failed with error: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_edge_cases():
    """Test edge cases that might cause issues."""
    logger.info("\n🧪 Testing Edge Cases")
    logger.info("=" * 50)
    
    # Test 1: Empty DataFrame
    empty_df = pd.DataFrame()
    try:
        result = trim_data_to_requirements(empty_df, '1min')
        if result.empty:
            logger.info("✅ Empty DataFrame handled correctly")
        else:
            logger.error("❌ Empty DataFrame not handled correctly")
            return False
    except Exception as e:
        logger.error(f"❌ Empty DataFrame test failed: {e}")
        return False
    
    # Test 2: DataFrame with less than required rows
    small_df = pd.DataFrame({
        'timestamp': ['2025-08-15 15:30:00', '2025-08-15 16:00:00'],
        'open': [100.0, 101.0],
        'high': [100.5, 101.5],
        'low': [99.5, 100.5],
        'close': [100.2, 101.2],
        'volume': [1000000, 1000000]
    })
    
    try:
        result = trim_data_to_requirements(small_df, '30min')
        if len(result) == 2:  # Should keep all 2 rows since less than 500
            logger.info("✅ Small DataFrame handled correctly")
        else:
            logger.error(f"❌ Small DataFrame not handled correctly: {len(result)} rows")
            return False
    except Exception as e:
        logger.error(f"❌ Small DataFrame test failed: {e}")
        return False
    
    return True


def main():
    """Run all data retention fix tests."""
    logger.info("🚀 Testing Data Retention Fixes")
    logger.info("=" * 60)
    
    tests_passed = 0
    total_tests = 3
    
    # Test 1: 1-minute timezone fix
    if test_1min_timezone_fix():
        tests_passed += 1
    
    # Test 2: 30-minute trimming logic
    if test_30min_trimming_logic():
        tests_passed += 1
    
    # Test 3: Edge cases
    if test_edge_cases():
        tests_passed += 1
    
    # Summary
    logger.info("\n" + "=" * 60)
    logger.info("📋 TEST SUMMARY")
    logger.info("=" * 60)
    logger.info(f"Tests passed: {tests_passed}/{total_tests}")
    
    if tests_passed == total_tests:
        logger.info("🎉 ALL TESTS PASSED - Data retention fixes are working correctly!")
        return True
    else:
        logger.error(f"❌ {total_tests - tests_passed} test(s) failed")
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)