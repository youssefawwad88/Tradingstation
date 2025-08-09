#!/usr/bin/env python3
"""
Test script to validate the force_update_tickers.py implementation
without requiring API keys. Uses mock data to simulate the full workflow.
"""

import sys
import os
import pandas as pd
import logging
from datetime import datetime, timedelta

# Add project root to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def create_mock_data(ticker, data_type):
    """Create mock data for testing purposes."""
    today = datetime.now()
    
    if data_type == 'daily':
        # Create 30 days of daily data including today
        dates = pd.date_range(end=today.date(), periods=30, freq='D')
        df = pd.DataFrame({
            'Date': dates,
            'open': 100.0 + pd.Series(range(len(dates))),
            'high': 105.0 + pd.Series(range(len(dates))),
            'low': 95.0 + pd.Series(range(len(dates))),
            'close': 102.0 + pd.Series(range(len(dates))),
            'adjusted_close': 102.0 + pd.Series(range(len(dates))),
            'volume': 1000000 + pd.Series(range(len(dates))) * 10000,
            'dividend_amount': 0.0,
            'split_coefficient': 1.0
        })
    elif data_type == '30min':
        # Create last 3 days of 30-min data including today
        start_time = today - timedelta(days=3)
        timestamps = pd.date_range(start=start_time, end=today, freq='30min')
        df = pd.DataFrame({
            'timestamp': timestamps,
            'open': 100.0 + pd.Series(range(len(timestamps))) * 0.1,
            'high': 105.0 + pd.Series(range(len(timestamps))) * 0.1,
            'low': 95.0 + pd.Series(range(len(timestamps))) * 0.1,
            'close': 102.0 + pd.Series(range(len(timestamps))) * 0.1,
            'volume': 10000 + pd.Series(range(len(timestamps))) * 100
        })
    elif data_type == '1min':
        # Create last day of 1-min data including current time
        start_time = today - timedelta(hours=8)
        timestamps = pd.date_range(start=start_time, end=today, freq='1min')
        df = pd.DataFrame({
            'timestamp': timestamps,
            'open': 100.0 + pd.Series(range(len(timestamps))) * 0.01,
            'high': 105.0 + pd.Series(range(len(timestamps))) * 0.01,
            'low': 95.0 + pd.Series(range(len(timestamps))) * 0.01,
            'close': 102.0 + pd.Series(range(len(timestamps))) * 0.01,
            'volume': 1000 + pd.Series(range(len(timestamps))) * 10
        })
    
    return df

def test_load_manual_tickers():
    """Test the enhanced load_manual_tickers function."""
    logger.info("🧪 TEST 1: Testing load_manual_tickers function")
    
    from utils.helpers import load_manual_tickers
    
    try:
        tickers = load_manual_tickers()
        
        if tickers and len(tickers) > 0:
            logger.info(f"✅ PASS: Loaded {len(tickers)} tickers")
            logger.info(f"   Tickers: {tickers}")
            return True, tickers
        else:
            logger.error("❌ FAIL: No tickers loaded")
            return False, []
    except Exception as e:
        logger.error(f"❌ FAIL: Exception in load_manual_tickers: {e}")
        return False, []

def test_save_function():
    """Test the enhanced save_df_to_s3 function."""
    logger.info("🧪 TEST 2: Testing save_df_to_s3 function")
    
    from utils.helpers import save_df_to_s3
    
    try:
        # Create test data
        test_df = create_mock_data('TEST', 'daily')
        
        # Test saving
        result = save_df_to_s3(test_df, 'data/daily/TEST_daily.csv')
        
        if result:
            logger.info("✅ PASS: Save function works correctly")
            
            # Verify file exists
            test_file = '/home/runner/work/Tradingstation/Tradingstation/data/daily/TEST_daily.csv'
            if os.path.exists(test_file):
                logger.info("✅ PASS: File verification successful")
                
                # Clean up
                os.remove(test_file)
                logger.info("🧹 Cleanup: Test file removed")
                
                return True
            else:
                logger.error("❌ FAIL: File verification failed")
                return False
        else:
            logger.error("❌ FAIL: Save function returned False")
            return False
    except Exception as e:
        logger.error(f"❌ FAIL: Exception in save function test: {e}")
        return False

def test_verification_functions():
    """Test the verification functions."""
    logger.info("🧪 TEST 3: Testing verification functions")
    
    from utils.helpers import verify_data_storage_and_retention, check_spaces_connectivity
    
    try:
        # Create test data first
        test_df = create_mock_data('TESTVERIFY', 'daily')
        from utils.helpers import save_df_to_s3
        save_df_to_s3(test_df, 'data/daily/TESTVERIFY_daily.csv')
        
        # Test verification
        result = verify_data_storage_and_retention('TESTVERIFY', check_today=True)
        
        if result and 'daily' in result:
            if result['daily']['exists']:
                logger.info("✅ PASS: Data verification works correctly")
                
                # Test connectivity check
                connectivity = check_spaces_connectivity()
                if 'credentials_configured' in connectivity:
                    logger.info("✅ PASS: Connectivity check works correctly")
                    
                    # Clean up
                    test_file = '/home/runner/work/Tradingstation/Tradingstation/data/daily/TESTVERIFY_daily.csv'
                    if os.path.exists(test_file):
                        os.remove(test_file)
                        logger.info("🧹 Cleanup: Test file removed")
                    
                    return True
                else:
                    logger.error("❌ FAIL: Connectivity check malformed")
                    return False
            else:
                logger.error("❌ FAIL: Data verification failed to find test file")
                return False
        else:
            logger.error("❌ FAIL: Verification function returned invalid result")
            return False
    except Exception as e:
        logger.error(f"❌ FAIL: Exception in verification test: {e}")
        return False

def test_force_update_functions():
    """Test the key functions from force_update_tickers.py."""
    logger.info("🧪 TEST 4: Testing force update functions")
    
    try:
        # Import the functions
        sys.path.append('/home/runner/work/Tradingstation/Tradingstation/jobs')
        from force_update_tickers import load_manual_tickers_with_validation, save_and_verify
        
        # Test ticker loading
        tickers = load_manual_tickers_with_validation()
        if not tickers:
            logger.error("❌ FAIL: Force update ticker loading failed")
            return False
        
        logger.info(f"✅ PASS: Force update ticker loading works ({len(tickers)} tickers)")
        
        # Test save and verify with mock data
        test_df = create_mock_data('TESTFORCE', 'daily')
        
        if not test_df.empty:
            result = save_and_verify(test_df, 'TESTFORCE', 'daily')
            
            if result:
                logger.info("✅ PASS: Force update save and verify works")
                
                # Clean up
                test_file = '/home/runner/work/Tradingstation/Tradingstation/data/daily/TESTFORCE_daily.csv'
                if os.path.exists(test_file):
                    os.remove(test_file)
                    logger.info("🧹 Cleanup: Test file removed")
                
                return True
            else:
                logger.error("❌ FAIL: Force update save and verify failed")
                return False
        else:
            logger.error("❌ FAIL: Mock data creation failed")
            return False
            
    except Exception as e:
        logger.error(f"❌ FAIL: Exception in force update test: {e}")
        return False

def run_all_tests():
    """Run all tests and report results."""
    logger.info("🚀 STARTING COMPREHENSIVE TEST SUITE")
    logger.info("=" * 60)
    
    tests = [
        ("Load Manual Tickers", test_load_manual_tickers),
        ("Save Function", test_save_function), 
        ("Verification Functions", test_verification_functions),
        ("Force Update Functions", test_force_update_functions)
    ]
    
    results = []
    
    for test_name, test_func in tests:
        logger.info(f"\n📋 Running: {test_name}")
        logger.info("-" * 40)
        
        try:
            if test_name == "Load Manual Tickers":
                success, tickers = test_func()
                results.append((test_name, success))
                if success:
                    logger.info(f"   📊 Result: {len(tickers)} tickers loaded")
            else:
                success = test_func()
                results.append((test_name, success))
        except Exception as e:
            logger.error(f"💥 CRITICAL ERROR in {test_name}: {e}")
            results.append((test_name, False))
    
    # Final report
    logger.info("\n" + "=" * 60)
    logger.info("🏁 TEST SUITE RESULTS:")
    
    passed = 0
    total = len(results)
    
    for test_name, success in results:
        status = "✅ PASS" if success else "❌ FAIL"
        logger.info(f"   {status}: {test_name}")
        if success:
            passed += 1
    
    logger.info(f"\n📊 SUMMARY: {passed}/{total} tests passed")
    
    if passed == total:
        logger.info("🎉 ALL TESTS PASSED! Implementation is working correctly.")
        return True
    else:
        logger.warning(f"⚠️ {total - passed} test(s) failed. Review implementation.")
        return False

if __name__ == "__main__":
    try:
        success = run_all_tests()
        sys.exit(0 if success else 1)
    except Exception as e:
        logger.error(f"💥 CRITICAL ERROR: {e}")
        sys.exit(1)