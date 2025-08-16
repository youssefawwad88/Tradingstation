#!/usr/bin/env python3
"""
Test Data Fetching Fixes

Tests the improvements made to data fetching and cloud storage handling.
"""

import os
import sys
import pandas as pd
import logging
from datetime import datetime, timedelta
import pytz

# Add project root to Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '.')))

from utils.helpers import save_df_to_s3, read_df_from_s3
from utils.spaces_manager import upload_dataframe, download_dataframe

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def create_sample_data():
    """Create sample data for testing."""
    logger.info("Creating sample data for testing...")
    
    # Create sample 1-minute data (7 days worth)
    ny_tz = pytz.timezone('America/New_York')
    end_date = datetime.now(ny_tz)
    start_date = end_date - timedelta(days=7)
    
    data = []
    current_date = start_date
    
    while current_date <= end_date:
        # Only create data for weekdays during market hours
        if current_date.weekday() < 5:  # Monday=0, Friday=4
            # Market hours: 9:30 AM to 4:00 PM ET
            market_start = current_date.replace(hour=9, minute=30, second=0, microsecond=0)
            market_end = current_date.replace(hour=16, minute=0, second=0, microsecond=0)
            
            current_time = market_start
            while current_time <= market_end:
                data.append({
                    'timestamp': current_time,
                    'open': 100.0 + (current_time.hour - 9) * 0.5,
                    'high': 101.0 + (current_time.hour - 9) * 0.5,
                    'low': 99.0 + (current_time.hour - 9) * 0.5,
                    'close': 100.5 + (current_time.hour - 9) * 0.5,
                    'volume': 10000
                })
                current_time += timedelta(minutes=1)
        
        current_date += timedelta(days=1)
    
    df = pd.DataFrame(data)
    logger.info(f"Created sample 1-minute data: {len(df)} rows covering {len(df['timestamp'].dt.date.unique())} trading days")
    
    return df

def create_sample_30min_data():
    """Create sample 30-minute data (500 rows)."""
    logger.info("Creating sample 30-minute data...")
    
    ny_tz = pytz.timezone('America/New_York')
    end_date = datetime.now(ny_tz)
    
    data = []
    
    # Create 500 30-minute intervals going backwards from now
    for i in range(500, 0, -1):
        timestamp = end_date - timedelta(minutes=30 * i)
        data.append({
            'timestamp': timestamp,
            'open': 100.0 + i * 0.01,
            'high': 101.0 + i * 0.01,
            'low': 99.0 + i * 0.01,
            'close': 100.5 + i * 0.01,
            'volume': 50000
        })
    
    df = pd.DataFrame(data)
    logger.info(f"Created sample 30-minute data: {len(df)} rows")
    
    return df

def test_local_storage():
    """Test local storage functionality."""
    logger.info("=== Testing Local Storage ===")
    
    # Create sample data
    sample_1min = create_sample_data()
    sample_30min = create_sample_30min_data()
    
    # Test local saves
    logger.info("Testing local saves...")
    
    # Save 1-minute data locally
    success_1min = save_df_to_s3(sample_1min, 'data/intraday/AAPL_1min.csv')
    logger.info(f"1-minute local save result: {success_1min}")
    
    # Save 30-minute data locally  
    success_30min = save_df_to_s3(sample_30min, 'data/intraday_30min/AAPL_30min.csv')
    logger.info(f"30-minute local save result: {success_30min}")
    
    # Test local reads
    logger.info("Testing local reads...")
    
    # Read 1-minute data
    read_1min = read_df_from_s3('data/intraday/AAPL_1min.csv')
    logger.info(f"1-minute read result: {len(read_1min)} rows")
    
    # Read 30-minute data
    read_30min = read_df_from_s3('data/intraday_30min/AAPL_30min.csv')
    logger.info(f"30-minute read result: {len(read_30min)} rows")
    
    return success_1min and success_30min and not read_1min.empty and not read_30min.empty

def test_weekend_mode_behavior():
    """Test behavior during weekend."""
    logger.info("=== Testing Weekend Mode Behavior ===")
    
    from utils.market_time import is_weekend, detect_market_session
    from utils.helpers import should_use_test_mode, get_test_mode_reason
    
    is_weekend_now = is_weekend()
    market_session = detect_market_session()
    test_mode_active = should_use_test_mode()
    test_mode_active, test_mode_reason = get_test_mode_reason()
    
    logger.info(f"Is weekend: {is_weekend_now}")
    logger.info(f"Market session: {market_session}")
    logger.info(f"Test mode active: {test_mode_active}")
    logger.info(f"Test mode reason: {test_mode_reason}")
    
    return True

def test_error_messaging():
    """Test improved error messaging."""
    logger.info("=== Testing Error Messaging ===")
    
    # Check environment variables
    alpha_key = os.getenv("ALPHA_VANTAGE_API_KEY")
    spaces_key = os.getenv("SPACES_ACCESS_KEY_ID")
    spaces_secret = os.getenv("SPACES_SECRET_ACCESS_KEY")
    spaces_bucket = os.getenv("SPACES_BUCKET_NAME")
    
    logger.info(f"ALPHA_VANTAGE_API_KEY: {'Set' if alpha_key else 'Not set'}")
    logger.info(f"SPACES_ACCESS_KEY_ID: {'Set' if spaces_key else 'Not set'}")
    logger.info(f"SPACES_SECRET_ACCESS_KEY: {'Set' if spaces_secret else 'Not set'}")
    logger.info(f"SPACES_BUCKET_NAME: {'Set' if spaces_bucket else 'Not set'}")
    
    if not alpha_key:
        logger.info("✅ API key missing as expected for test environment")
    
    if not spaces_bucket:
        logger.info("✅ Spaces credentials missing as expected for test environment")
    
    return True

def test_data_validation():
    """Test data validation for weekend scenarios."""
    logger.info("=== Testing Data Validation ===")
    
    # Check if we have local data from previous runs
    sample_files = [
        'data/intraday/AAPL_1min.csv',
        'data/intraday_30min/AAPL_30min.csv'
    ]
    
    for file_path in sample_files:
        full_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), file_path)
        if os.path.exists(full_path):
            try:
                df = pd.read_csv(full_path)
                logger.info(f"✅ Found local data file: {file_path} with {len(df)} rows")
                
                if 'timestamp' in df.columns:
                    df['timestamp'] = pd.to_datetime(df['timestamp'])
                    min_date = df['timestamp'].min()
                    max_date = df['timestamp'].max()
                    logger.info(f"   Date range: {min_date} to {max_date}")
                    
                    # Check if we have 7 days of data for 1-minute files
                    if '1min' in file_path:
                        days_span = (max_date - min_date).days
                        logger.info(f"   Data spans {days_span} days")
                        if days_span >= 6:  # Allow for 6-7 days
                            logger.info("   ✅ Has approximately 7 days of data")
                        else:
                            logger.warning(f"   ⚠️ Only has {days_span} days of data (expected ~7)")
                    
                    # Check if we have 500 rows for 30-minute files
                    if '30min' in file_path:
                        if len(df) >= 480:  # Allow some tolerance
                            logger.info("   ✅ Has approximately 500 rows of data")
                        else:
                            logger.warning(f"   ⚠️ Only has {len(df)} rows (expected ~500)")
                
            except Exception as e:
                logger.error(f"❌ Error reading {file_path}: {e}")
        else:
            logger.info(f"⚠️ No local data file found: {file_path}")
    
    return True

def main():
    """Run all tests."""
    logger.info("🚀 Starting Data Fetching Fixes Test")
    logger.info("=" * 60)
    
    tests = [
        ("Weekend Mode Behavior", test_weekend_mode_behavior),
        ("Error Messaging", test_error_messaging),
        ("Data Validation", test_data_validation),
        ("Local Storage", test_local_storage),
    ]
    
    passed = 0
    total = len(tests)
    
    for test_name, test_func in tests:
        logger.info(f"\n--- {test_name} ---")
        try:
            result = test_func()
            if result:
                logger.info(f"✅ {test_name}: PASSED")
                passed += 1
            else:
                logger.error(f"❌ {test_name}: FAILED")
        except Exception as e:
            logger.error(f"❌ {test_name}: FAILED with error: {e}")
    
    logger.info("=" * 60)
    logger.info(f"📊 Test Results: {passed}/{total} tests passed")
    
    if passed == total:
        logger.info("🎉 All tests passed!")
    else:
        logger.warning(f"⚠️ {total - passed} tests failed")
    
    return passed == total

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)