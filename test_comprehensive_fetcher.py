#!/usr/bin/env python3
"""
Test script for the comprehensive data fetcher to validate core functionality.
"""

import sys
import os
import pandas as pd

# Add project root to Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '.')))

def test_comprehensive_data_fetcher():
    """Test the core functions of the comprehensive data fetcher"""
    print("🧪 Testing Comprehensive Data Fetcher")
    print("=" * 50)
    
    # Import the functions from our comprehensive script
    import comprehensive_data_fetcher as cdf
    
    # Test 1: Test get_data_from_api function signature and error handling
    print("📋 Test 1: get_data_from_api function...")
    
    # Test with invalid data type
    result = cdf.get_data_from_api("INVALID", "AAPL")
    assert result.empty, "Should return empty DataFrame for invalid data type"
    print("   ✅ Invalid data type handling: PASS")
    
    # Test with valid parameters (will fail due to missing API key, but should not crash)
    try:
        result = cdf.get_data_from_api("INTRADAY", "AAPL", "1min", "compact")
        print("   ✅ INTRADAY call without crashing: PASS")
    except Exception as e:
        print(f"   ❌ INTRADAY call crashed: {e}")
        return False
    
    try:
        result = cdf.get_data_from_api("DAILY", "AAPL", output_size="full")
        print("   ✅ DAILY call without crashing: PASS")
    except Exception as e:
        print(f"   ❌ DAILY call crashed: {e}")
        return False
    
    # Test 2: Test intelligent_fetch_strategy function
    print("\n📋 Test 2: intelligent_fetch_strategy function...")
    
    # Test strategy determination (will return 'full' due to no cloud access)
    strategy = cdf.intelligent_fetch_strategy("AAPL", "INTRADAY", "1min")
    assert strategy in ["full", "compact"], f"Strategy should be 'full' or 'compact', got: {strategy}"
    print(f"   ✅ Strategy determination: {strategy} - PASS")
    
    strategy = cdf.intelligent_fetch_strategy("AAPL", "DAILY")
    assert strategy in ["full", "compact"], f"Strategy should be 'full' or 'compact', got: {strategy}"
    print(f"   ✅ DAILY strategy determination: {strategy} - PASS")
    
    # Test 3: Configuration validation
    print("\n📋 Test 3: Configuration validation...")
    
    valid_data_types = ["INTRADAY", "DAILY"]
    assert cdf.DATA_TYPE in valid_data_types, f"DATA_TYPE should be one of {valid_data_types}"
    print(f"   ✅ DATA_TYPE '{cdf.DATA_TYPE}' is valid: PASS")
    
    valid_intervals = ["1min", "30min"]
    assert cdf.DATA_INTERVAL in valid_intervals, f"DATA_INTERVAL should be one of {valid_intervals}"
    print(f"   ✅ DATA_INTERVAL '{cdf.DATA_INTERVAL}' is valid: PASS")
    
    assert isinstance(cdf.FILE_SIZE_THRESHOLD_KB, (int, float)), "FILE_SIZE_THRESHOLD_KB should be a number"
    assert cdf.FILE_SIZE_THRESHOLD_KB > 0, "FILE_SIZE_THRESHOLD_KB should be positive"
    print(f"   ✅ FILE_SIZE_THRESHOLD_KB '{cdf.FILE_SIZE_THRESHOLD_KB}' is valid: PASS")
    
    print("\n🎉 All tests passed!")
    return True

def test_configuration_changes():
    """Test that configuration can be easily changed"""
    print("\n🔧 Testing Configuration Changes")
    print("=" * 50)
    
    # Read the script content
    with open('comprehensive_data_fetcher.py', 'r') as f:
        content = f.read()
    
    # Test that all required configuration variables are at the top
    config_lines = content.split('\n')[:50]  # Check first 50 lines
    config_section_found = False
    
    for line in config_lines:
        if "CENTRALIZED CONFIGURATION SECTION" in line:
            config_section_found = True
            break
    
    assert config_section_found, "Configuration section should be clearly marked and at the top"
    print("   ✅ Configuration section is clearly marked: PASS")
    
    # Check that all required variables are present
    required_vars = ["TICKER_SYMBOL", "DATA_INTERVAL", "DATA_TYPE", "FILE_SIZE_THRESHOLD_KB"]
    
    for var in required_vars:
        assert f"{var} = " in content, f"Required variable {var} should be in the script"
        print(f"   ✅ Variable '{var}' found in script: PASS")
    
    print("\n🎉 Configuration tests passed!")
    return True

if __name__ == "__main__":
    print("🚀 Starting Comprehensive Data Fetcher Tests")
    print("=" * 60)
    
    try:
        # Run tests
        test1_passed = test_comprehensive_data_fetcher()
        test2_passed = test_configuration_changes()
        
        if test1_passed and test2_passed:
            print("\n" + "=" * 60)
            print("🎉 ALL TESTS PASSED - Comprehensive Data Fetcher is working correctly!")
            print("=" * 60)
            exit(0)
        else:
            print("\n" + "=" * 60)
            print("❌ SOME TESTS FAILED")
            print("=" * 60)
            exit(1)
            
    except Exception as e:
        print(f"\n❌ Test execution failed: {e}")
        exit(1)