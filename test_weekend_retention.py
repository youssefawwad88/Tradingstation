#!/usr/bin/env python3
"""
Test weekend/market-closed data retention to ensure testing works properly.
"""

import pandas as pd
import sys
import os
from datetime import datetime, timedelta
import pytz
import logging

# Add project root to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '.')))

from utils.helpers import cleanup_data_retention
from utils.config import TIMEZONE

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_weekend_data_retention():
    """Test data retention logic works properly on weekends/market-closed days."""
    logger.info("🧪 Testing Weekend/Market-Closed Data Retention")
    
    ny_tz = pytz.timezone(TIMEZONE)
    
    # Simulate it's Saturday (market closed)
    saturday_time = ny_tz.localize(datetime(2025, 8, 16, 14, 0, 0))  # Saturday 2 PM
    
    # Create test data for the past 10 days including weekends
    test_data = []
    for day_offset in range(-9, 1):  # 9 days ago to today
        test_date = saturday_time + timedelta(days=day_offset)
        
        # Add some data points for each day (simulate market data even on weekends for testing)
        for hour in [9, 12, 15]:
            test_data.append({
                'timestamp': test_date.replace(hour=hour, minute=30),
                'open': 100.0,
                'high': 101.0,
                'low': 99.0,
                'close': 100.5,
                'volume': 1000000
            })
    
    test_df = pd.DataFrame(test_data)
    
    logger.info(f"Created weekend test data with {len(test_df)} rows")
    logger.info(f"Simulated current time: {saturday_time} (Saturday)")
    
    # Show unique dates
    test_df_temp = test_df.copy()
    test_df_temp['date'] = test_df_temp['timestamp'].dt.date
    unique_dates_before = sorted(test_df_temp['date'].unique())
    logger.info(f"Dates before cleanup: {[str(d) for d in unique_dates_before]}")
    
    # Apply cleanup retention
    _, _, cleaned_1min = cleanup_data_retention("TEST_WEEKEND", pd.DataFrame(), pd.DataFrame(), test_df)
    
    if not cleaned_1min.empty:
        cleaned_1min['date'] = cleaned_1min['timestamp'].dt.date
        unique_dates_after = sorted(cleaned_1min['date'].unique())
        
        logger.info(f"Dates after cleanup: {[str(d) for d in unique_dates_after]}")
        logger.info(f"Rows before: {len(test_df)}, after: {len(cleaned_1min)}")
        
        # Check if Saturday (today) is preserved
        saturday_date = saturday_time.date()
        has_saturday = saturday_date in unique_dates_after
        logger.info(f"Saturday data preserved: {has_saturday}")
        
        # Check if Friday data is preserved
        friday_date = (saturday_time - timedelta(days=1)).date()
        has_friday = friday_date in unique_dates_after
        logger.info(f"Friday data preserved: {has_friday}")
        
        return has_saturday, has_friday, len(unique_dates_after)
    else:
        logger.error("No data after cleanup!")
        return False, False, 0

def test_market_holiday_scenario():
    """Test data retention during market holidays."""
    logger.info("\n🎯 Testing Market Holiday Scenario")
    
    ny_tz = pytz.timezone(TIMEZONE)
    
    # Simulate it's a Monday holiday
    monday_holiday = ny_tz.localize(datetime(2025, 8, 18, 10, 0, 0))  # Monday
    
    # Create data including the holiday and surrounding days
    test_data = []
    for day_offset in range(-10, 1):  # 10 days ago to today
        test_date = monday_holiday + timedelta(days=day_offset)
        
        # Add intraday data
        test_data.append({
            'timestamp': test_date.replace(hour=10, minute=0),
            'open': 100.0,
            'high': 101.0,
            'low': 99.0,
            'close': 100.5,
            'volume': 1000000
        })
    
    test_df = pd.DataFrame(test_data)
    
    # Apply cleanup
    _, _, cleaned_1min = cleanup_data_retention("TEST_HOLIDAY", pd.DataFrame(), pd.DataFrame(), test_df)
    
    if not cleaned_1min.empty:
        cleaned_1min['date'] = cleaned_1min['timestamp'].dt.date
        unique_dates = sorted(cleaned_1min['date'].unique())
        
        logger.info(f"Holiday test - dates preserved: {[str(d) for d in unique_dates]}")
        
        # Check if today (holiday) is preserved
        holiday_date = monday_holiday.date()
        has_holiday = holiday_date in unique_dates
        logger.info(f"Holiday data preserved: {has_holiday}")
        
        return has_holiday, len(unique_dates)
    else:
        logger.error("No data after holiday cleanup!")
        return False, 0

if __name__ == "__main__":
    logger.info("🚀 Testing Weekend/Market-Closed Data Retention Logic")
    
    # Test weekend scenario
    has_saturday, has_friday, weekend_days = test_weekend_data_retention()
    
    # Test holiday scenario  
    has_holiday, holiday_days = test_market_holiday_scenario()
    
    # Summary
    logger.info("\n=== WEEKEND/HOLIDAY TEST SUMMARY ===")
    logger.info(f"Weekend Saturday data preserved: {has_saturday}")
    logger.info(f"Weekend Friday data preserved: {has_friday}")
    logger.info(f"Weekend days retained: {weekend_days}")
    logger.info(f"Holiday data preserved: {has_holiday}")
    logger.info(f"Holiday days retained: {holiday_days}")
    
    all_tests_passed = has_saturday and has_friday and has_holiday
    
    if all_tests_passed:
        logger.info("✅ ALL TESTS PASSED - Weekend/holiday data retention works correctly!")
        logger.info("✅ Testing will work properly when market is closed")
    else:
        logger.error("❌ SOME TESTS FAILED - Weekend/holiday data retention needs attention")