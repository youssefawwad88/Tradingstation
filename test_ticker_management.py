#!/usr/bin/env python3
"""
Test Suite for Ticker Management System

Tests the new ticker management functionality without requiring API keys.
"""

import sys
import os
import pandas as pd
import tempfile
import shutil
from datetime import datetime

# Add project root to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '.')))

def test_master_ticker_list_reading():
    """Test reading master ticker list"""
    print("🧪 Testing master ticker list reading...")
    
    try:
        from utils.helpers import read_master_tickerlist
        
        # This should read the existing master_tickerlist.csv we created earlier
        tickers = read_master_tickerlist()
        
        assert len(tickers) > 0, "Should have loaded at least some tickers"
        assert 'NVDA' in tickers, "Should include manual ticker NVDA"
        assert 'AAPL' in tickers, "Should include manual ticker AAPL"
        
        print(f"✅ Successfully loaded {len(tickers)} tickers from master list")
        print(f"   First 5 tickers: {tickers[:5]}")
        return True
        
    except Exception as e:
        print(f"❌ Error testing master ticker list reading: {e}")
        return False

def test_real_time_price_function():
    """Test the real-time price function structure (without API call)"""
    print("🧪 Testing real-time price function structure...")
    
    try:
        from utils.alpha_vantage_api import get_real_time_price
        
        # Check that the function exists and is callable
        assert callable(get_real_time_price), "get_real_time_price should be callable"
        
        print("✅ Real-time price function is properly defined")
        return True
        
    except Exception as e:
        print(f"❌ Error testing real-time price function: {e}")
        return False

def test_fetch_scripts_structure():
    """Test that all required fetch scripts exist"""
    print("🧪 Testing fetch scripts structure...")
    
    required_scripts = [
        'generate_master_tickerlist.py',
        'fetch_daily.py', 
        'fetch_30min.py',
        'fetch_intraday_compact.py'
    ]
    
    missing_scripts = []
    for script in required_scripts:
        if not os.path.exists(script):
            missing_scripts.append(script)
    
    if missing_scripts:
        print(f"❌ Missing scripts: {missing_scripts}")
        return False
    
    print(f"✅ All required scripts exist: {required_scripts}")
    return True

def test_data_path_structure():
    """Test that data directory structure matches requirements"""
    print("🧪 Testing data directory structure...")
    
    required_dirs = [
        'data/daily',
        'data/intraday',
        'data/intraday_30min'
    ]
    
    missing_dirs = []
    for dir_path in required_dirs:
        if not os.path.exists(dir_path):
            missing_dirs.append(dir_path)
    
    if missing_dirs:
        print(f"❌ Missing directories: {missing_dirs}")
        return False
    
    print(f"✅ All required directories exist: {required_dirs}")
    return True

def test_ticker_source_priorities():
    """Test that manual tickers have priority over S&P 500 tickers"""
    print("🧪 Testing ticker source priorities...")
    
    try:
        from utils.helpers import read_master_tickerlist
        
        tickers = read_master_tickerlist()
        
        # Manual tickers should appear first in the list
        manual_tickers = ['NVDA', 'AAPL', 'TSLA', 'AMD', 'GOOGL', 'MSFT', 'AMZN', 'NFLX']
        
        for i, manual_ticker in enumerate(manual_tickers):
            if manual_ticker in tickers:
                manual_index = tickers.index(manual_ticker)
                # Check that manual tickers appear early in the list
                assert manual_index < 20, f"Manual ticker {manual_ticker} should appear early in list, found at index {manual_index}"
        
        print("✅ Manual tickers have proper priority in master list")
        return True
        
    except Exception as e:
        print(f"❌ Error testing ticker priorities: {e}")
        return False

def test_file_format_compliance():
    """Test that generated files follow the expected format"""
    print("🧪 Testing file format compliance...")
    
    try:
        # Check master_tickerlist.csv format
        if os.path.exists('master_tickerlist.csv'):
            df = pd.read_csv('master_tickerlist.csv')
            
            required_columns = ['ticker', 'source', 'generated_at']
            for col in required_columns:
                assert col in df.columns, f"master_tickerlist.csv should have column: {col}"
            
            # Check that sources are correct
            valid_sources = ['manual', 'sp500_filtered']
            for source in df['source'].unique():
                assert source in valid_sources, f"Invalid source found: {source}"
            
            print("✅ master_tickerlist.csv format is correct")
        else:
            print("⚠️ master_tickerlist.csv not found (will be created when generate_master_tickerlist.py runs)")
        
        return True
        
    except Exception as e:
        print(f"❌ Error testing file format: {e}")
        return False

def run_all_tests():
    """Run all tests and report results"""
    print("🚀 Running Ticker Management System Tests")
    print("=" * 60)
    
    tests = [
        test_master_ticker_list_reading,
        test_real_time_price_function,
        test_fetch_scripts_structure,
        test_data_path_structure,
        test_ticker_source_priorities,
        test_file_format_compliance
    ]
    
    passed = 0
    failed = 0
    
    for test in tests:
        try:
            if test():
                passed += 1
            else:
                failed += 1
        except Exception as e:
            print(f"❌ Test {test.__name__} failed with exception: {e}")
            failed += 1
        print()
    
    print("=" * 60)
    print(f"📊 Test Results: {passed} passed, {failed} failed")
    
    if failed == 0:
        print("🎉 All tests passed! Ticker management system is ready.")
        return True
    else:
        print("⚠️ Some tests failed. Please review the issues above.")
        return False

if __name__ == "__main__":
    success = run_all_tests()
    if success:
        print("\n✅ Ticker management system validation completed successfully")
        exit(0)
    else:
        print("\n❌ Ticker management system validation failed")
        exit(1)