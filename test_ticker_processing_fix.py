#!/usr/bin/env python3
"""
Test script to validate the ticker processing fix.
This test verifies that the update_intraday_compact job works correctly
even when the ALPHA_VANTAGE_API_KEY is not available.
"""

import os
import sys
import subprocess

def test_ticker_processing_without_api_key():
    """Test that ticker processing works without API key."""
    print("🧪 Testing ticker processing without API key...")
    
    # Ensure API key is not set for this test
    if 'ALPHA_VANTAGE_API_KEY' in os.environ:
        del os.environ['ALPHA_VANTAGE_API_KEY']
    
    # Run the job
    try:
        result = subprocess.run([
            sys.executable, 'jobs/update_intraday_compact.py'
        ], capture_output=True, text=True, timeout=60)
        
        output = result.stdout + result.stderr
        
        # Check for the orchestrator summary
        summary_lines = [line for line in output.split('\n') if 'ORCHESTRATOR SUMMARY' in line]
        
        if not summary_lines:
            print("❌ FAIL: No orchestrator summary found")
            return False
        
        summary = summary_lines[0]
        print(f"📋 Found summary: {summary}")
        
        # Validate the summary format and content
        if "Processed 0/" in summary:
            print("❌ FAIL: Still processing 0 tickers")
            return False
        
        if "Processed 8/13 tickers" in summary:
            print("✅ PASS: Processing 8/13 tickers as expected")
        else:
            print(f"⚠️  WARNING: Expected 8/13 tickers, got different count")
        
        if "Manual tickers: 7/7 OK" in summary:
            print("✅ PASS: All 7 manual tickers processed successfully")
        else:
            print("❌ FAIL: Manual tickers not processed correctly")
            return False
        
        if "Storage: Local only" in summary:
            print("✅ PASS: Using local storage as expected")
        else:
            print("❌ FAIL: Storage configuration incorrect")
            return False
        
        return True
        
    except subprocess.TimeoutExpired:
        print("❌ FAIL: Job timed out")
        return False
    except Exception as e:
        print(f"❌ FAIL: Exception occurred: {e}")
        return False

def test_api_key_warning():
    """Test that API key warning is displayed correctly."""
    print("\n🧪 Testing API key warning message...")
    
    # Import the alpha_vantage_api module
    sys.path.append('.')
    from utils.alpha_vantage_api import get_intraday_data
    
    # Ensure API key is not set
    if 'ALPHA_VANTAGE_API_KEY' in os.environ:
        del os.environ['ALPHA_VANTAGE_API_KEY']
    
    # Capture the output
    from io import StringIO
    import contextlib
    
    captured_output = StringIO()
    
    with contextlib.redirect_stdout(captured_output):
        # This should print a warning and return empty DataFrame
        result = get_intraday_data('AAPL', '1min', 'compact')
    
    output = captured_output.getvalue()
    
    if "WARNING: ALPHA_VANTAGE_API_KEY environment variable not set" in output:
        print("✅ PASS: API key warning displayed correctly")
        return True
    else:
        print("❌ FAIL: API key warning not displayed")
        return False

def main():
    """Run all tests."""
    print("🚀 Starting ticker processing fix validation tests\n")
    
    test_results = []
    
    # Test 1: Ticker processing without API key
    test_results.append(test_ticker_processing_without_api_key())
    
    # Test 2: API key warning
    test_results.append(test_api_key_warning())
    
    # Summary
    passed = sum(test_results)
    total = len(test_results)
    
    print(f"\n📊 Test Results: {passed}/{total} tests passed")
    
    if passed == total:
        print("🎉 All tests passed! The ticker processing fix is working correctly.")
        return True
    else:
        print("❌ Some tests failed. Please review the output above.")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)