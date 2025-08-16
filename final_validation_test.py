#!/usr/bin/env python3
"""
Final validation test demonstrating that the 1-minute intraday data retention issues are fixed.
This addresses all three issues mentioned in the problem statement.
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

def test_problem_statement_issues():
    """Test that all three issues from the problem statement are resolved."""
    logger.info("🎯 FINAL VALIDATION - Problem Statement Issues")
    logger.info("Problem: 1. No historical data - should have 7 days history besides today")
    logger.info("Problem: 2. Missing 15/08 data") 
    logger.info("Problem: 3. Testing when market is closed")
    
    ny_tz = pytz.timezone(TIMEZONE)
    now = datetime.now(ny_tz)
    
    logger.info(f"Current time: {now}")
    logger.info(f"Required lookback days: {ONE_MIN_REQUIRED_DAYS}")
    
    # Create realistic test data that includes Aug 15th and covers 10 days
    test_data = []
    
    # Start from 10 days ago and create data up to today
    for day_offset in range(-9, 1):  # -9 to 0 = 10 days
        test_date = now + timedelta(days=day_offset)
        
        # Create multiple data points per day to simulate intraday data
        for hour in [9, 10, 11, 12, 13, 14, 15, 16]:  # Market hours
            for minute in [30, 45]:  # Sample minutes
                timestamp = test_date.replace(hour=hour, minute=minute, second=0, microsecond=0)
                test_data.append({
                    'timestamp': timestamp,
                    'open': 150.0 + (day_offset % 5),
                    'high': 152.0 + (day_offset % 5),
                    'low': 148.0 + (day_offset % 5),
                    'close': 151.0 + (day_offset % 5),
                    'volume': 2000000 + (abs(day_offset) * 10000)
                })
    
    original_df = pd.DataFrame(test_data)
    
    # Show the original data range
    original_df_temp = original_df.copy()
    original_df_temp['date'] = original_df_temp['timestamp'].dt.date
    original_dates = sorted(original_df_temp['date'].unique())
    
    logger.info(f"\n📊 ORIGINAL TEST DATA:")
    logger.info(f"   Total rows: {len(original_df)}")
    logger.info(f"   Date range: {original_df['timestamp'].min()} to {original_df['timestamp'].max()}")
    logger.info(f"   Unique dates: {[str(d) for d in original_dates]}")
    
    # Check if Aug 15th is in original data
    aug_15_date = datetime(2025, 8, 15).date()
    has_aug_15_original = aug_15_date in original_dates
    logger.info(f"   Contains Aug 15th: {has_aug_15_original}")
    
    # Apply the fixed cleanup retention logic
    logger.info(f"\n🔧 APPLYING FIXED CLEANUP LOGIC...")
    _, _, cleaned_df = cleanup_data_retention("FINAL_TEST", pd.DataFrame(), pd.DataFrame(), original_df)
    
    # Analyze the results
    if not cleaned_df.empty:
        cleaned_df_temp = cleaned_df.copy()
        cleaned_df_temp['date'] = cleaned_df_temp['timestamp'].dt.date
        cleaned_dates = sorted(cleaned_df_temp['date'].unique())
        
        logger.info(f"\n📈 CLEANED DATA RESULTS:")
        logger.info(f"   Total rows: {len(cleaned_df)}")
        logger.info(f"   Date range: {cleaned_df['timestamp'].min()} to {cleaned_df['timestamp'].max()}")
        logger.info(f"   Unique dates: {[str(d) for d in cleaned_dates]}")
        logger.info(f"   Days retained: {len(cleaned_dates)}")
        
        # Test Issue #1: Historical data (7 days)
        has_sufficient_history = len(cleaned_dates) >= ONE_MIN_REQUIRED_DAYS
        logger.info(f"\n✅ ISSUE #1 - Historical Data:")
        logger.info(f"   Required: {ONE_MIN_REQUIRED_DAYS} days minimum")
        logger.info(f"   Actual: {len(cleaned_dates)} days")
        logger.info(f"   Status: {'✅ FIXED' if has_sufficient_history else '❌ STILL BROKEN'}")
        
        # Test Issue #2: Aug 15th data preservation
        has_aug_15_cleaned = aug_15_date in cleaned_dates
        logger.info(f"\n✅ ISSUE #2 - August 15th Data:")
        logger.info(f"   Aug 15th in original data: {has_aug_15_original}")
        logger.info(f"   Aug 15th preserved after cleanup: {has_aug_15_cleaned}")
        logger.info(f"   Status: {'✅ FIXED' if has_aug_15_cleaned else '❌ STILL BROKEN'}")
        
        # Test Issue #3: Today's data when market closed
        today_date = now.date()
        has_today = today_date in cleaned_dates
        is_weekend = now.weekday() >= 5
        logger.info(f"\n✅ ISSUE #3 - Market Closed Testing:")
        logger.info(f"   Today's date: {today_date}")
        logger.info(f"   Is weekend: {is_weekend}")
        logger.info(f"   Today's data preserved: {has_today}")
        logger.info(f"   Status: {'✅ FIXED' if has_today else '❌ STILL BROKEN'}")
        
        # Overall assessment
        all_fixed = has_sufficient_history and has_aug_15_cleaned and has_today
        
        logger.info(f"\n🎯 FINAL ASSESSMENT:")
        logger.info(f"   Issue #1 (Historical data): {'✅ FIXED' if has_sufficient_history else '❌ BROKEN'}")
        logger.info(f"   Issue #2 (Aug 15th data): {'✅ FIXED' if has_aug_15_cleaned else '❌ BROKEN'}")
        logger.info(f"   Issue #3 (Market closed): {'✅ FIXED' if has_today else '❌ BROKEN'}")
        logger.info(f"   Overall status: {'🎉 ALL ISSUES FIXED' if all_fixed else '⚠️ SOME ISSUES REMAIN'}")
        
        return has_sufficient_history, has_aug_15_cleaned, has_today
    else:
        logger.error("❌ CRITICAL: No data after cleanup!")
        return False, False, False

if __name__ == "__main__":
    logger.info("🚀 FINAL VALIDATION TEST FOR 1-MINUTE INTRADAY DATA RETENTION")
    logger.info("=" * 80)
    
    # Run the comprehensive test
    issue1_fixed, issue2_fixed, issue3_fixed = test_problem_statement_issues()
    
    logger.info("\n" + "=" * 80)
    if issue1_fixed and issue2_fixed and issue3_fixed:
        logger.info("🎉 SUCCESS: All problem statement issues have been resolved!")
        logger.info("✅ The 1-minute intraday data retention logic now works correctly")
        logger.info("✅ Historical data (7+ days) is preserved")
        logger.info("✅ Specific dates (like 15/08) are preserved within the 7-day window")
        logger.info("✅ Testing works properly even when market is closed")
    else:
        logger.error("❌ FAILURE: Some issues remain unresolved")
        
    logger.info("=" * 80)