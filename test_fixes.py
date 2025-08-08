#!/usr/bin/env python3
"""
Test script to verify all the TradingStation fixes work correctly.
This script tests the import fixes and basic functionality without requiring API credentials.
"""

import sys
import os

def test_imports():
    """Test that all previously failing imports now work."""
    print("🧪 Testing Import Fixes...")
    
    try:
        # Test 1: spaces_manager import (was failing in master_dashboard.py)
        from utils.spaces_manager import spaces_manager
        print("✅ spaces_manager import: FIXED")
        
        # Test 2: master_dashboard import
        from dashboard.master_dashboard import run_master_dashboard_consolidation
        print("✅ master_dashboard import: FIXED")
        
        # Test 3: breakout screener import
        from screeners.breakout import run_breakout_screener
        print("✅ breakout screener import: FIXED") 
        
        # Test 4: update_intraday_compact import
        from jobs.update_intraday_compact import run_compact_append
        print("✅ update_intraday_compact import: FIXED")
        
        return True
        
    except Exception as e:
        print(f"❌ Import test failed: {e}")
        return False

def test_function_calls():
    """Test that the specific function calls that were failing now work."""
    print("\n🧪 Testing Function Call Fixes...")
    
    try:
        # Test the specific function call that was failing in breakout.py
        from utils.helpers import read_tickerlist_from_s3
        result = read_tickerlist_from_s3('tickerlist.txt')
        print(f"✅ read_tickerlist_from_s3('tickerlist.txt'): FIXED - returned {len(result)} tickers")
        
        # Test spaces_manager functionality
        from utils.spaces_manager import spaces_manager
        if hasattr(spaces_manager, 'list_objects'):
            print("✅ spaces_manager.list_objects method: AVAILABLE")
        else:
            print("❌ spaces_manager.list_objects method: MISSING")
            return False
            
        return True
        
    except Exception as e:
        print(f"❌ Function call test failed: {e}")
        return False

def test_diagnostic_logging():
    """Test that the enhanced diagnostic logging works."""
    print("\n🧪 Testing Diagnostic Logging...")
    
    try:
        from utils.helpers import read_master_tickerlist, load_manual_tickers
        
        # Test master ticker loading
        master_tickers = read_master_tickerlist()
        print(f"✅ Master tickers loaded: {len(master_tickers)} tickers")
        
        # Test manual ticker loading  
        manual_tickers = load_manual_tickers()
        print(f"✅ Manual tickers loaded: {len(manual_tickers)} tickers")
        
        # Test overlap analysis
        if master_tickers and manual_tickers:
            overlap = set(master_tickers) & set(manual_tickers)
            print(f"✅ Manual/Master overlap: {len(overlap)} tickers will be processed as manual")
            
            missing = set(manual_tickers) - set(master_tickers)
            if missing:
                print(f"⚠️  Manual tickers not in master: {len(missing)} ({list(missing)})")
            
        return True
        
    except Exception as e:
        print(f"❌ Diagnostic logging test failed: {e}")
        return False

def main():
    """Run all tests and provide summary."""
    print("=" * 60)
    print("TradingStation Fix Verification Tests")
    print("=" * 60)
    
    tests = [
        ("Import Fixes", test_imports),
        ("Function Call Fixes", test_function_calls), 
        ("Diagnostic Logging", test_diagnostic_logging)
    ]
    
    results = []
    for test_name, test_func in tests:
        try:
            result = test_func()
            results.append((test_name, result))
        except Exception as e:
            print(f"❌ {test_name} test crashed: {e}")
            results.append((test_name, False))
    
    print("\n" + "=" * 60)
    print("Test Summary:")
    print("=" * 60)
    
    all_passed = True
    for test_name, passed in results:
        status = "✅ PASSED" if passed else "❌ FAILED"
        print(f"{test_name}: {status}")
        if not passed:
            all_passed = False
    
    print("=" * 60)
    if all_passed:
        print("🎉 ALL TESTS PASSED - TradingStation fixes are working!")
        print("\nNext steps:")
        print("1. Test with actual API credentials to verify full functionality")
        print("2. Monitor logs when running update_intraday_compact.py to identify processing issues")
        print("3. Check if manual ticker processing rate improves from 0/7 to 7/7")
        print("4. Investigate why only 4/13 master tickers process successfully")
    else:
        print("❌ SOME TESTS FAILED - Review errors above")
        return 1
    
    return 0

if __name__ == "__main__":
    sys.exit(main())