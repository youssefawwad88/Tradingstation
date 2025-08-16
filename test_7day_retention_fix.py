#!/usr/bin/env python3
"""
Test script to validate 7-day retention logic for 1-minute intraday data.
This test validates the fix for intraday data retention issues.
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
from utils.config import TIMEZONE, ONE_MIN_REQUIRED_DAYS

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def create_test_intraday_data():
    """Create test 1-minute intraday data spanning 10 days to test filtering."""
    logger.info("Creating test 1-minute intraday data...")
    
    ny_tz = pytz.timezone(TIMEZONE)
    now = datetime.now(ny_tz)
    
    # Create data spanning 10 days (including today)
    start_date = now - timedelta(days=9)  # 10 days ago
    
    timestamps = []
    
    # Generate 1-minute data for each day (including weekends for testing)
    for day_offset in range(10):
        day_start = start_date + timedelta(days=day_offset)
            
        # Generate market hours data (9:30 AM to 4:00 PM ET = 390 minutes)
        day_current = day_start.replace(hour=9, minute=30, second=0, microsecond=0)
        for minute in range(0, 390, 30):  # Sample every 30 minutes to reduce data size
            timestamps.append(day_current + timedelta(minutes=minute))
    
    # IMPORTANT: Add today's data explicitly
    today_start = now.replace(hour=9, minute=30, second=0, microsecond=0)
    for minute in range(0, 390, 30):
        timestamps.append(today_start + timedelta(minutes=minute))
    
    # Create DataFrame with OHLCV data
    data = []
    for i, ts in enumerate(timestamps):
        data.append({
            'timestamp': ts,
            'open': 100.0 + (i % 10),
            'high': 101.0 + (i % 10),
            'low': 99.0 + (i % 10),
            'close': 100.5 + (i % 10),
            'volume': 1000000 + (i * 1000)
        })
    
    df = pd.DataFrame(data)
    
    # Show unique dates in test data
    df_temp = df.copy()
    df_temp['date'] = df_temp['timestamp'].dt.date
    unique_dates = sorted(df_temp['date'].unique())
    
    logger.info(f"Created test data with {len(df)} rows")
    logger.info(f"Date range: {df['timestamp'].min()} to {df['timestamp'].max()}")
    logger.info(f"Unique dates in test data: {[str(d) for d in unique_dates]}")
    
    return df

def test_current_cleanup_logic():
    """Test the current cleanup_data_retention logic."""
    logger.info("\n=== Testing Current Cleanup Logic ===")
    
    # Create test data
    test_df = create_test_intraday_data()
    
    # Apply current cleanup logic
    _, _, cleaned_1min = cleanup_data_retention("TEST", pd.DataFrame(), pd.DataFrame(), test_df)
    
    logger.info(f"Original data: {len(test_df)} rows")
    logger.info(f"After cleanup: {len(cleaned_1min)} rows")
    
    if not cleaned_1min.empty:
        logger.info(f"Cleaned data range: {cleaned_1min['timestamp'].min()} to {cleaned_1min['timestamp'].max()}")
        
        # Check if we have expected 7 days
        ny_tz = pytz.timezone(TIMEZONE)
        now = datetime.now(ny_tz)
        seven_days_ago = now - timedelta(days=7)
        
        # Count how many days we actually have
        cleaned_1min['date'] = cleaned_1min['timestamp'].dt.date
        unique_dates = cleaned_1min['date'].unique()
        logger.info(f"Unique dates in cleaned data: {len(unique_dates)}")
        logger.info(f"Expected cutoff (7 days ago): {seven_days_ago.strftime('%Y-%m-%d')}")
        
        # Check if today's date equivalent is preserved
        today_date = now.date()
        has_today = today_date in unique_dates
        logger.info(f"Has today's date ({today_date}): {has_today}")
        
        return len(unique_dates), has_today
    else:
        logger.error("Cleaned data is empty!")
        return 0, False

def test_specific_date_retention():
    """Test if specific dates (like 15/08) are properly retained."""
    logger.info("\n=== Testing Specific Date Retention ===")
    
    ny_tz = pytz.timezone(TIMEZONE)
    now = datetime.now(ny_tz)
    
    # Create test data that includes August 15th
    aug_15 = ny_tz.localize(datetime(2025, 8, 15, 10, 0, 0))
    aug_16 = ny_tz.localize(datetime(2025, 8, 16, 10, 0, 0))
    
    # Calculate days difference
    days_diff_15 = (now - aug_15).days
    days_diff_16 = (now - aug_16).days
    
    logger.info(f"Days from Aug 15th to now: {days_diff_15}")
    logger.info(f"Days from Aug 16th to now: {days_diff_16}")
    
    # Create targeted test data
    test_data = []
    for day_offset in range(-10, 1):  # 10 days ago to today
        test_date = now + timedelta(days=day_offset)
        for hour in [9, 12, 15]:  # Sample hours
            test_data.append({
                'timestamp': test_date.replace(hour=hour, minute=30),
                'open': 100.0,
                'high': 101.0,
                'low': 99.0,
                'close': 100.5,
                'volume': 1000000
            })
    
    test_df = pd.DataFrame(test_data)
    
    # Apply cleanup
    _, _, cleaned_1min = cleanup_data_retention("TEST", pd.DataFrame(), pd.DataFrame(), test_df)
    
    if not cleaned_1min.empty:
        cleaned_1min['date'] = cleaned_1min['timestamp'].dt.date
        unique_dates = sorted(cleaned_1min['date'].unique())
        
        logger.info(f"Dates in cleaned data: {[str(d) for d in unique_dates]}")
        
        # Check if Aug 15 is present
        aug_15_date = aug_15.date()
        has_aug_15 = aug_15_date in unique_dates
        logger.info(f"Has August 15th data: {has_aug_15}")
        
        return has_aug_15, unique_dates
    else:
        logger.error("No data after cleanup!")
        return False, []

if __name__ == "__main__":
    logger.info("🚀 Testing 7-Day Retention Logic for 1-Minute Intraday Data")
    
    # Test current logic
    unique_days, has_today = test_current_cleanup_logic()
    
    # Test specific date retention
    has_aug_15, dates = test_specific_date_retention()
    
    # Summary
    logger.info("\n=== TEST SUMMARY ===")
    logger.info(f"Unique days retained: {unique_days}")
    logger.info(f"Has today's data: {has_today}")
    logger.info(f"Has August 15th data: {has_aug_15}")
    
    if unique_days < 7:
        logger.error(f"❌ ISSUE: Only {unique_days} days retained, expected at least 7")
    else:
        logger.info(f"✅ Good: {unique_days} days retained")
    
    if not has_today:
        logger.error("❌ ISSUE: Today's data missing")
    else:
        logger.info("✅ Good: Today's data preserved")
    
    if not has_aug_15:
        logger.warning("⚠️ August 15th data missing (may be expected if > 7 days ago)")
    else:
        logger.info("✅ Good: August 15th data preserved")