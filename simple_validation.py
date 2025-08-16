#!/usr/bin/env python3
"""
Simple Validation: Core Fixes Work
===================================

This script validates that the core issues have been resolved:
1. Timezone comparison now works (no more TypeError)
2. 30-minute trimming now uses proper logic

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


def test_timezone_comparison_fix():
    """Test that timezone comparison no longer causes TypeError."""
    logger.info("🧪 Testing Timezone Comparison Fix")
    logger.info("=" * 50)
    
    # Create timezone-naive data (like from CSV) - this used to cause TypeError
    test_data = pd.DataFrame({
        'timestamp': [
            '2025-08-15 15:30:00',  # Recent
            '2025-08-08 15:30:00',  # Older (should be filtered)
        ],
        'open': [100.0, 100.0],
        'high': [101.0, 101.0],
        'low': [99.0, 99.0],
        'close': [100.5, 100.5],
        'volume': [1000000, 1000000]
    })
    
    logger.info("   Testing timezone-naive timestamps (previously caused TypeError)...")
    
    try:
        # This used to fail with: TypeError: can't compare offset-naive and offset-aware datetimes
        result = trim_data_to_requirements(test_data, '1min')
        logger.info(f"   ✅ SUCCESS: No timezone error! {len(test_data)} → {len(result)} rows")
        return True
    except TypeError as e:
        if "offset-naive and offset-aware" in str(e):
            logger.error(f"   ❌ FAILED: Timezone comparison error still exists: {e}")
            return False
        else:
            raise e
    except Exception as e:
        logger.error(f"   ❌ FAILED: Unexpected error: {e}")
        return False


def test_30min_trimming_improvement():
    """Test that 30-minute trimming now preserves most recent data."""
    logger.info("\n🧪 Testing 30-Minute Trimming Improvement")
    logger.info("=" * 50)
    
    # Create 600 rows of data in chronological order
    base_time = datetime(2025, 8, 15, 9, 30)
    test_data = []
    
    for i in range(600):
        timestamp = base_time + timedelta(minutes=30 * i)
        test_data.append({
            'timestamp': timestamp.strftime('%Y-%m-%d %H:%M:%S'),
            'open': 100.0 + i,  # Incrementing value to track order
            'high': 101.0 + i,
            'low': 99.0 + i,
            'close': 100.5 + i,
            'volume': 1000000
        })
    
    df = pd.DataFrame(test_data)
    logger.info(f"   Created 600 rows of 30-minute test data")
    logger.info(f"   Data range: {df['timestamp'].min()} to {df['timestamp'].max()}")
    
    try:
        result = trim_data_to_requirements(df, '30min')
        
        # Check that exactly 500 rows are returned
        if len(result) != 500:
            logger.error(f"   ❌ FAILED: Expected 500 rows, got {len(result)}")
            return False
        
        # Check that the most recent data is preserved
        original_max_close = df['close'].max()  # Should be 100.5 + 599 = 700.0
        result_max_close = result['close'].max()
        
        if original_max_close == result_max_close:
            logger.info(f"   ✅ SUCCESS: Most recent 500 rows preserved (latest close: {result_max_close})")
            return True
        else:
            logger.error(f"   ❌ FAILED: Most recent data not preserved ({result_max_close} vs {original_max_close})")
            return False
            
    except Exception as e:
        logger.error(f"   ❌ FAILED: Error during trimming: {e}")
        return False


def main():
    """Run simple validation tests."""
    logger.info("🎯 SIMPLE VALIDATION: Core Fixes Work")
    logger.info("=" * 60)
    
    tests_passed = 0
    total_tests = 2
    
    # Test 1: Timezone comparison fix
    if test_timezone_comparison_fix():
        tests_passed += 1
    
    # Test 2: 30-minute trimming improvement
    if test_30min_trimming_improvement():
        tests_passed += 1
    
    # Summary
    logger.info("\n" + "=" * 60)
    logger.info("📋 VALIDATION SUMMARY")
    logger.info("=" * 60)
    
    if tests_passed == total_tests:
        logger.info("🎉 ALL CORE FIXES VALIDATED!")
        logger.info("   ✅ Timezone comparison: FIXED (no more TypeError)")
        logger.info("   ✅ 30-minute trimming: IMPROVED (preserves recent data)")
        logger.info("\n🚀 Ready for production deployment!")
        return True
    else:
        logger.error(f"❌ {total_tests - tests_passed} validation(s) failed")
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)