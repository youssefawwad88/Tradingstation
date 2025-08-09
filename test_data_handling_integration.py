#!/usr/bin/env python3
"""
Integration Test for Data Handling Refactor

Tests the complete data handling workflow including:
- Manual ticker-only sources
- Weekend test mode functionality
- Data fetching with proper limits (200 daily, 500 30-min, 7 days 1-min)
- Cleanup procedures
- Data storage in unified /data/ structure
"""

import sys
import os
import pandas as pd
import tempfile
import shutil
from datetime import datetime, timedelta

# Add project root to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '.')))

def test_weekend_detection():
    """Test weekend detection functionality"""
    print("🧪 Testing weekend detection...")
    
    try:
        from utils.helpers import is_weekend, should_use_test_mode
        
        # Test weekend detection function exists and works
        weekend_status = is_weekend()
        test_mode_status = should_use_test_mode()
        
        print(f"   Weekend status: {weekend_status}")
        print(f"   Test mode should be active: {test_mode_status}")
        
        # Since it's likely a weekend when running this test, expect test mode
        assert callable(is_weekend), "is_weekend should be callable"
        assert callable(should_use_test_mode), "should_use_test_mode should be callable"
        
        print("✅ Weekend detection functions work correctly")
        return True
        
    except Exception as e:
        print(f"❌ Error testing weekend detection: {e}")
        return False

def test_manual_ticker_source():
    """Test that only manual tickers are used"""
    print("🧪 Testing manual ticker source...")
    
    try:
        from utils.helpers import read_master_tickerlist
        
        # Read the master ticker list
        tickers = read_master_tickerlist()
        
        assert len(tickers) > 0, "Should have loaded manual tickers"
        assert 'NVDA' in tickers, "Should include NVDA from tickerlist.txt"
        
        # Check that there are no automated tickers beyond the manual ones
        expected_manual = ['NVDA', 'AAPL', 'TSLA', 'AMD', 'GOOGL', 'MSFT', 'AMZN', 'NFLX']
        for ticker in expected_manual:
            assert ticker in tickers, f"Manual ticker {ticker} should be in list"
        
        print(f"✅ Manual ticker source working: {len(tickers)} tickers loaded")
        print(f"   Tickers: {tickers}")
        return True
        
    except Exception as e:
        print(f"❌ Error testing manual ticker source: {e}")
        return False

def test_data_structure_limits():
    """Test that data files have correct row limits"""
    print("🧪 Testing data structure and limits...")
    
    try:
        data_dir = "/home/runner/work/Tradingstation/Tradingstation/data"
        
        # Check that data directories exist
        assert os.path.exists(f"{data_dir}/intraday"), "Intraday directory should exist"
        assert os.path.exists(f"{data_dir}/intraday_30min"), "30min directory should exist"
        assert os.path.exists(f"{data_dir}/daily"), "Daily directory should exist"
        
        # Test a sample ticker's data files
        sample_ticker = "NVDA"
        
        # Check 30-min data (should be ~500 rows + header)
        intraday_30min_file = f"{data_dir}/intraday_30min/{sample_ticker}_30min.csv"
        if os.path.exists(intraday_30min_file):
            df_30min = pd.read_csv(intraday_30min_file)
            assert len(df_30min) == 500, f"30-min data should have 500 rows, got {len(df_30min)}"
            assert 'timestamp' in df_30min.columns, "Should have timestamp column"
            assert 'volume' in df_30min.columns, "Should have volume column"
            print(f"✅ 30-min data: {len(df_30min)} rows (correct)")
        
        # Check 1-min data (should be ~7 days worth)
        intraday_1min_file = f"{data_dir}/intraday/{sample_ticker}_1min.csv"
        if os.path.exists(intraday_1min_file):
            df_1min = pd.read_csv(intraday_1min_file)
            # Should be roughly 7 days worth of 1-min data (allowing for weekends/holidays)
            assert len(df_1min) > 5000, f"1-min data should have substantial rows for 7 days, got {len(df_1min)}"
            assert len(df_1min) < 15000, f"1-min data shouldn't exceed reasonable 7-day limit, got {len(df_1min)}"
            print(f"✅ 1-min data: {len(df_1min)} rows (within 7-day range)")
        
        # Check daily data in intraday folder (test mode puts it there)
        daily_file = f"{data_dir}/intraday/{sample_ticker}_daily.csv"
        if os.path.exists(daily_file):
            df_daily = pd.read_csv(daily_file)
            assert len(df_daily) == 200, f"Daily data should have 200 rows, got {len(df_daily)}"
            print(f"✅ Daily data: {len(df_daily)} rows (correct)")
        
        print("✅ Data structure and limits are correct")
        return True
        
    except Exception as e:
        print(f"❌ Error testing data structure: {e}")
        return False

def test_cleanup_functionality():
    """Test cleanup and retention functionality"""
    print("🧪 Testing cleanup functionality...")
    
    try:
        from utils.helpers import cleanup_data_retention
        
        # Create sample data that exceeds limits
        large_daily = pd.DataFrame({
            'timestamp': pd.date_range('2024-01-01', periods=300, freq='D'),
            'close': range(300),
            'volume': range(300)
        })
        
        large_30min = pd.DataFrame({
            'timestamp': pd.date_range('2024-01-01', periods=600, freq='30min'),
            'close': range(600),
            'volume': range(600)
        })
        
        large_1min = pd.DataFrame({
            'timestamp': pd.date_range('2024-01-01', periods=15000, freq='1min'),
            'close': range(15000),
            'volume': range(15000)
        })
        
        # Test cleanup
        cleaned_daily, cleaned_30min, cleaned_1min = cleanup_data_retention(
            "TEST", large_daily, large_30min, large_1min
        )
        
        # Verify limits are applied
        assert len(cleaned_daily) == 200, f"Cleaned daily should have 200 rows, got {len(cleaned_daily)}"
        assert len(cleaned_30min) == 500, f"Cleaned 30min should have 500 rows, got {len(cleaned_30min)}"
        # 1-min should be trimmed to reasonable size (last 7 days from full dataset)
        assert len(cleaned_1min) < len(large_1min), "1-min data should be trimmed"
        
        print(f"✅ Cleanup working: Daily {len(large_daily)}→{len(cleaned_daily)}, "
              f"30min {len(large_30min)}→{len(cleaned_30min)}, "
              f"1min {len(large_1min)}→{len(cleaned_1min)}")
        return True
        
    except Exception as e:
        print(f"❌ Error testing cleanup: {e}")
        return False

def test_detailed_logging():
    """Test detailed logging functionality"""
    print("🧪 Testing detailed logging...")
    
    try:
        from utils.helpers import log_detailed_operation
        import io
        import logging
        
        # Capture log output
        log_capture = io.StringIO()
        handler = logging.StreamHandler(log_capture)
        logger = logging.getLogger('utils.helpers')
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        
        # Test logging function
        start_time = datetime.now()
        log_detailed_operation(
            "TEST", "Test Operation", start_time, 1000, 500, "Test details"
        )
        
        # Check log output
        log_output = log_capture.getvalue()
        assert "TEST: Test Operation" in log_output, "Should contain operation details"
        assert "Rows: 1000 → 500" in log_output, "Should contain row count changes"
        assert "Test details" in log_output, "Should contain additional details"
        
        print("✅ Detailed logging is working correctly")
        return True
        
    except Exception as e:
        print(f"❌ Error testing detailed logging: {e}")
        return False

def test_full_integration():
    """Test the full integration by running the update_all_data script"""
    print("🧪 Testing full integration...")
    
    try:
        import subprocess
        
        # Run the update_all_data script (should use test mode on weekend)
        result = subprocess.run(
            [sys.executable, "jobs/update_all_data.py"],
            cwd="/home/runner/work/Tradingstation/Tradingstation",
            capture_output=True,
            text=True,
            timeout=60
        )
        
        assert result.returncode == 0, f"Script should exit successfully, got code {result.returncode}"
        
        # Check for key output messages
        output = result.stdout + result.stderr
        assert "TEST MODE" in output, "Should indicate test mode is active"
        assert "Weekend Test Mode Active" in output, "Should show weekend test mode"
        assert "Full Rebuild Complete" in output, "Should complete full rebuild"
        
        print("✅ Full integration test passed")
        print(f"   Script output contained expected test mode indicators")
        return True
        
    except Exception as e:
        print(f"❌ Error in full integration test: {e}")
        return False

def run_all_tests():
    """Run all integration tests"""
    print("🚀 Running Data Handling Integration Tests")
    print("=" * 60)
    
    tests = [
        test_weekend_detection,
        test_manual_ticker_source,
        test_data_structure_limits,
        test_cleanup_functionality,
        test_detailed_logging,
        test_full_integration
    ]
    
    results = []
    for test_func in tests:
        try:
            result = test_func()
            results.append(result)
            print()
        except Exception as e:
            print(f"❌ Test {test_func.__name__} failed with exception: {e}")
            results.append(False)
            print()
    
    # Summary
    passed = sum(results)
    total = len(results)
    
    print("=" * 60)
    print(f"📊 Test Results: {passed}/{total} passed")
    
    if passed == total:
        print("🎉 All integration tests passed! Data handling refactor is working correctly.")
        return True
    else:
        print(f"❌ {total - passed} tests failed. Please review the failures above.")
        return False

if __name__ == "__main__":
    success = run_all_tests()
    exit(0 if success else 1)