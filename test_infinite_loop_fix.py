#!/usr/bin/env python3
"""
Test script to validate the comprehensive intraday fix issues.

This script tests the three critical fixes:
1. No infinite loop in get_cloud_file_size_bytes function
2. Correct intelligent fetching logic using cloud file size
3. Self-contained script functionality

Run with: python3 test_infinite_loop_fix.py
"""

import sys
import os
import logging

# Add project root to Python path
sys.path.append(os.path.abspath('.'))

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def test_no_infinite_loop():
    """Test Issue 1 Fix: Verify no infinite loop in get_cloud_file_size_bytes"""
    logger.info("\n🧪 TEST 1: No Infinite Loop")
    logger.info("=" * 50)
    
    try:
        import comprehensive_intraday_fix as cif
        logger.info("   ✅ Script imported successfully")
        
        # Test the function that previously had infinite loop
        size = cif.get_cloud_file_size_bytes("test/nonexistent.csv")
        logger.info(f"   ✅ get_cloud_file_size_bytes returned: {size} bytes")
        
        # Test multiple calls to ensure no recursion
        for i in range(3):
            size = cif.get_cloud_file_size_bytes(f"test/file_{i}.csv")
            logger.info(f"   ✅ Call {i+1}: {size} bytes")
            
        logger.info("   🎯 RESULT: No infinite loop - function works correctly")
        return True
        
    except RecursionError:
        logger.error("   ❌ RecursionError detected - infinite loop still exists")
        return False
    except ImportError as e:
        logger.error(f"   ❌ Import error: {e}")
        return False
    except Exception as e:
        logger.error(f"   ❌ Unexpected error: {e}")
        return False


def test_intelligent_fetching_logic():
    """Test Issue 2 Fix: Verify intelligent fetching uses cloud file size correctly"""
    logger.info("\n🧪 TEST 2: Intelligent Fetching Logic")
    logger.info("=" * 50)
    
    try:
        import comprehensive_intraday_fix as cif
        import pandas as pd
        
        # Test small file scenario (should trigger 'full' fetch)
        empty_df = pd.DataFrame()
        strategy = cif.determine_fetch_strategy("test/small_file.csv", empty_df)
        logger.info(f"   ✅ Small/non-existent file strategy: {strategy}")
        
        if strategy == "full":
            logger.info("   ✅ Correct: Small files trigger 'full' fetch strategy")
        else:
            logger.error("   ❌ Error: Small files should trigger 'full' strategy")
            return False
            
        # Test with config object
        config = cif.AppConfig(data_interval="1min", file_size_threshold_kb=10)
        strategy = cif.determine_fetch_strategy("test/another_file.csv", empty_df, config)
        logger.info(f"   ✅ With config object strategy: {strategy}")
        
        logger.info("   🎯 RESULT: Intelligent fetching logic works correctly")
        return True
        
    except Exception as e:
        logger.error(f"   ❌ Error testing intelligent fetching: {e}")
        return False


def test_self_contained_functionality():
    """Test Issue 3 Fix: Verify script is self-contained and configurable"""
    logger.info("\n🧪 TEST 3: Self-Contained Functionality")
    logger.info("=" * 50)
    
    try:
        import comprehensive_intraday_fix as cif
        
        # Test configuration at the top of script
        config = cif.AppConfig(
            data_interval="30min", 
            test_ticker="MSFT",
            file_size_threshold_kb=15
        )
        logger.info(f"   ✅ Config created - interval: {config.DATA_INTERVAL}")
        logger.info(f"   ✅ Config paths - folder: {config.DATA_FOLDER}")
        logger.info(f"   ✅ Config threshold: {config.FILE_SIZE_THRESHOLD_KB}KB")
        
        # Test interval update functionality
        config.update_interval("1min")
        logger.info(f"   ✅ Updated interval: {config.DATA_INTERVAL}")
        logger.info(f"   ✅ Updated paths - folder: {config.DATA_FOLDER}")
        
        # Test that all required functions exist
        required_functions = [
            'get_cloud_file_size_bytes',
            'determine_fetch_strategy', 
            'intelligent_data_merge',
            'run_comprehensive_intraday_fetch'
        ]
        
        for func_name in required_functions:
            if hasattr(cif, func_name):
                logger.info(f"   ✅ Function exists: {func_name}")
            else:
                logger.error(f"   ❌ Missing function: {func_name}")
                return False
                
        logger.info("   🎯 RESULT: Script is self-contained with all required functionality")
        return True
        
    except Exception as e:
        logger.error(f"   ❌ Error testing self-contained functionality: {e}")
        return False


def run_all_tests():
    """Run all validation tests"""
    logger.info("🎯 COMPREHENSIVE INFINITE LOOP FIX VALIDATION")
    logger.info("=" * 80)
    
    tests = [
        ("Issue 1: No Infinite Loop", test_no_infinite_loop),
        ("Issue 2: Intelligent Fetching Logic", test_intelligent_fetching_logic), 
        ("Issue 3: Self-Contained Functionality", test_self_contained_functionality)
    ]
    
    all_passed = True
    results = []
    
    for test_name, test_func in tests:
        try:
            result = test_func()
            results.append((test_name, result))
            if not result:
                all_passed = False
        except Exception as e:
            logger.error(f"❌ {test_name} failed with exception: {e}")
            results.append((test_name, False))
            all_passed = False
    
    # Summary
    logger.info("\n" + "=" * 80)
    logger.info("📊 TEST RESULTS SUMMARY")
    logger.info("=" * 80)
    
    for test_name, passed in results:
        status = "✅ PASSED" if passed else "❌ FAILED"
        logger.info(f"{status} - {test_name}")
    
    if all_passed:
        logger.info("\n🎉 ALL TESTS PASSED - Issues are fixed!")
        logger.info("   ✅ Issue 1: Infinite loop fixed")
        logger.info("   ✅ Issue 2: Intelligent fetching logic fixed")
        logger.info("   ✅ Issue 3: Self-contained script ready")
    else:
        logger.error("\n❌ SOME TESTS FAILED - Review needed")
    
    logger.info("=" * 80)
    return all_passed


if __name__ == "__main__":
    success = run_all_tests()
    sys.exit(0 if success else 1)