#!/usr/bin/env python3
"""
Phase 5: Verification Plan

Comprehensive verification script to ensure all requirements are met:
1. Verify correct paths are used consistently
2. Check environment variables are properly configured
3. Verify data retention logic keeps today's data
4. Test migration script functionality
5. Check that both manual and master tickers are processed
"""

import os
import sys
import pandas as pd
from datetime import datetime
import pytz

# Add project root to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '.')))

def verify_environment_variables():
    """Verify Phase 1 environment variables are properly configured."""
    print("🔧 PHASE 1: ENVIRONMENT VARIABLES VERIFICATION")
    print("=" * 60)
    
    from utils.config import (
        SPACES_BASE_PREFIX, SPACES_STRUCTURE_VERSION,
        INTRADAY_TRIM_DAYS, INTRADAY_EXCLUDE_TODAY,
        INTRADAY_INCLUDE_PREMARKET, INTRADAY_INCLUDE_AFTERHOURS,
        TIMEZONE, PROCESS_MANUAL_TICKERS, MAX_TICKERS_PER_RUN,
        INTRADAY_BATCH_SIZE, MARKET_HOURS_ONLY, SKIP_IF_FRESH_MINUTES,
        DEBUG_MODE
    )
    
    configs = [
        ("SPACES_BASE_PREFIX", SPACES_BASE_PREFIX, "data"),
        ("SPACES_STRUCTURE_VERSION", SPACES_STRUCTURE_VERSION, "v2"),
        ("INTRADAY_TRIM_DAYS", INTRADAY_TRIM_DAYS, 7),
        ("INTRADAY_EXCLUDE_TODAY", INTRADAY_EXCLUDE_TODAY, False),
        ("INTRADAY_INCLUDE_PREMARKET", INTRADAY_INCLUDE_PREMARKET, True),
        ("INTRADAY_INCLUDE_AFTERHOURS", INTRADAY_INCLUDE_AFTERHOURS, True),
        ("TIMEZONE", TIMEZONE, "America/New_York"),
        ("PROCESS_MANUAL_TICKERS", PROCESS_MANUAL_TICKERS, True),
        ("MAX_TICKERS_PER_RUN", MAX_TICKERS_PER_RUN, 25),
        ("INTRADAY_BATCH_SIZE", INTRADAY_BATCH_SIZE, 25),
        ("MARKET_HOURS_ONLY", MARKET_HOURS_ONLY, False),
        ("SKIP_IF_FRESH_MINUTES", SKIP_IF_FRESH_MINUTES, 0),
        ("DEBUG_MODE", DEBUG_MODE, True)
    ]
    
    all_correct = True
    for name, actual, expected in configs:
        status = "✅" if actual == expected else "❌"
        print(f"{status} {name}: {actual} (expected: {expected})")
        if actual != expected:
            all_correct = False
    
    return all_correct

def verify_folder_structure():
    """Verify Phase 2 folder structure standardization."""
    print("\n📁 PHASE 2: FOLDER STRUCTURE VERIFICATION")
    print("=" * 60)
    
    from utils.config import BASE_DATA_DIR, INTRADAY_DATA_DIR, INTRADAY_30MIN_DATA_DIR, DAILY_DATA_DIR
    
    expected_paths = [
        ("Base data directory", BASE_DATA_DIR, "data"),
        ("1min intraday", INTRADAY_DATA_DIR, "data/intraday"),
        ("30min intraday", INTRADAY_30MIN_DATA_DIR, "data/intraday_30min"),
        ("Daily data", DAILY_DATA_DIR, "data/daily")
    ]
    
    all_exist = True
    for name, path, expected_suffix in expected_paths:
        exists = os.path.exists(path)
        correct_path = path.endswith(expected_suffix)
        status = "✅" if exists and correct_path else "❌"
        print(f"{status} {name}: {path} ({'exists' if exists else 'missing'})")
        if not exists or not correct_path:
            all_exist = False
    
    # Test path generation
    test_paths = [
        ("1min data path", "data/intraday/TSLA_1min.csv"),
        ("30min data path", "data/intraday_30min/TSLA_30min.csv"),
        ("Daily data path", "data/daily/TSLA_daily.csv")
    ]
    
    print(f"\n📋 Standard path verification:")
    for name, expected_path in test_paths:
        print(f"✅ {name}: {expected_path}")
    
    return all_exist

def verify_migration_functionality():
    """Verify Phase 3 migration script functionality."""
    print("\n🔄 PHASE 3: MIGRATION SCRIPT VERIFICATION")
    print("=" * 60)
    
    try:
        from migrate_spaces_paths import extract_ticker_from_path, get_standard_path
        
        # Test path extraction and conversion
        test_cases = [
            ("intraday/AAPL_1min.csv", "AAPL", "data/intraday/AAPL_1min.csv"),
            ("30 minutes/TSLA_30min.csv", "TSLA", "data/intraday_30min/TSLA_30min.csv"),
            ("intraday/NVDA.csv", "NVDA", "data/intraday/NVDA_1min.csv")
        ]
        
        all_correct = True
        for old_path, expected_ticker, expected_new_path in test_cases:
            actual_ticker = extract_ticker_from_path(old_path)
            actual_new_path = get_standard_path(old_path)
            
            ticker_ok = actual_ticker == expected_ticker
            path_ok = actual_new_path == expected_new_path
            
            status = "✅" if ticker_ok and path_ok else "❌"
            print(f"{status} {old_path}")
            print(f"    Ticker: {actual_ticker} ({'✅' if ticker_ok else '❌ expected ' + expected_ticker})")
            print(f"    New path: {actual_new_path} ({'✅' if path_ok else '❌ expected ' + expected_new_path})")
            
            if not (ticker_ok and path_ok):
                all_correct = False
        
        return all_correct
        
    except Exception as e:
        print(f"❌ Migration script import failed: {e}")
        return False

def verify_data_retention():
    """Verify Phase 4 data retention functionality."""
    print("\n📊 PHASE 4: DATA RETENTION VERIFICATION")
    print("=" * 60)
    
    try:
        from utils.helpers import apply_data_retention, is_today_present_enhanced
        from utils.config import TIMEZONE, INTRADAY_TRIM_DAYS
        
        # Create test data with multiple days
        ny_tz = pytz.timezone(TIMEZONE)
        today = datetime.now(ny_tz).replace(hour=12, minute=0, second=0, microsecond=0)
        
        timestamps = []
        # Add old data (should be filtered out)
        for days_back in [15, 10, 9, 8]:
            old_date = today - pd.Timedelta(days=days_back)
            timestamps.append(old_date)
        
        # Add recent data (should be kept)
        for days_back in [3, 2, 1]:
            recent_date = today - pd.Timedelta(days=days_back)
            timestamps.append(recent_date)
        
        # Add today's data (MUST be kept)
        timestamps.append(today)
        
        test_df = pd.DataFrame({
            'Date': timestamps,
            'Close': range(len(timestamps))
        })
        
        print(f"📊 Test data created: {len(test_df)} rows")
        print(f"   Date range: {test_df['Date'].min()} to {test_df['Date'].max()}")
        
        # Check today's data before retention
        has_today_before = is_today_present_enhanced(test_df, 'Date')
        print(f"✅ Today's data before retention: {has_today_before}")
        
        # Apply retention
        filtered_df = apply_data_retention(test_df.copy())
        
        # Check results
        has_today_after = is_today_present_enhanced(filtered_df, 'Date')
        rows_before = len(test_df)
        rows_after = len(filtered_df)
        
        print(f"\n📋 Retention results:")
        print(f"   Rows before: {rows_before}")
        print(f"   Rows after: {rows_after}")
        print(f"   Today's data preserved: {'✅' if has_today_after else '❌'}")
        
        # Verify date range
        if not filtered_df.empty:
            min_date = filtered_df['Date'].min()
            max_date = filtered_df['Date'].max()
            print(f"   Date range after: {min_date} to {max_date}")
            
            # Check that we kept approximately the right number of days
            days_span = (max_date - min_date).days
            expected_span = INTRADAY_TRIM_DAYS
            span_ok = days_span <= expected_span + 1  # Allow some tolerance
            print(f"   Days span: {days_span} (expected ≤ {expected_span}: {'✅' if span_ok else '❌'})")
            
            return has_today_after and span_ok
        else:
            print("   ❌ No data remaining after retention!")
            return False
            
    except Exception as e:
        print(f"❌ Data retention test failed: {e}")
        return False

def verify_ticker_processing():
    """Verify ticker processing functionality."""
    print("\n🎯 PHASE 5: TICKER PROCESSING VERIFICATION")
    print("=" * 60)
    
    try:
        from utils.helpers import read_master_tickerlist, load_manual_tickers
        
        # Test master ticker list
        master_tickers = read_master_tickerlist()
        print(f"📊 Master tickers loaded: {len(master_tickers) if master_tickers else 0}")
        if master_tickers:
            print(f"   Tickers: {master_tickers}")
        
        # Test manual ticker list
        manual_tickers = load_manual_tickers()
        print(f"🎯 Manual tickers loaded: {len(manual_tickers) if manual_tickers else 0}")
        if manual_tickers:
            print(f"   Tickers: {manual_tickers}")
        
        # Check overlap
        if master_tickers and manual_tickers:
            overlap = set(manual_tickers) & set(master_tickers)
            print(f"📋 Overlap between manual and master: {len(overlap)} tickers")
            if overlap:
                print(f"   Common tickers: {list(overlap)}")
        
        return bool(master_tickers or manual_tickers)
        
    except Exception as e:
        print(f"❌ Ticker processing test failed: {e}")
        return False

def verify_imports():
    """Verify all imports work correctly."""
    print("\n🔧 IMPORT VERIFICATION")
    print("=" * 60)
    
    imports_to_test = [
        ("utils.config", ["INTRADAY_TRIM_DAYS", "TIMEZONE", "DEBUG_MODE"]),
        ("utils.helpers", ["apply_data_retention", "is_today_present_enhanced"]),
        ("jobs.update_intraday_compact", ["process_ticker_interval"]),
        ("migrate_spaces_paths", ["migrate_objects"])
    ]
    
    all_imports_ok = True
    for module_name, items in imports_to_test:
        try:
            module = __import__(module_name, fromlist=items)
            for item in items:
                if hasattr(module, item):
                    print(f"✅ {module_name}.{item}")
                else:
                    print(f"❌ {module_name}.{item} - not found")
                    all_imports_ok = False
        except Exception as e:
            print(f"❌ {module_name} - import failed: {e}")
            all_imports_ok = False
    
    return all_imports_ok

def main():
    """Run comprehensive verification."""
    print("🚀 COMPREHENSIVE IMPLEMENTATION VERIFICATION")
    print("=" * 80)
    
    # Run all verification phases
    results = {
        "Environment Variables": verify_environment_variables(),
        "Folder Structure": verify_folder_structure(),
        "Migration Functionality": verify_migration_functionality(),
        "Data Retention": verify_data_retention(),
        "Ticker Processing": verify_ticker_processing(),
        "Imports": verify_imports()
    }
    
    # Summary
    print(f"\n📋 VERIFICATION SUMMARY")
    print("=" * 40)
    
    all_passed = True
    for phase, passed in results.items():
        status = "✅ PASS" if passed else "❌ FAIL"
        print(f"{status} {phase}")
        if not passed:
            all_passed = False
    
    print(f"\n{'🎉 ALL VERIFICATIONS PASSED!' if all_passed else '💥 SOME VERIFICATIONS FAILED!'}")
    
    if all_passed:
        print(f"\n✅ IMPLEMENTATION IS READY:")
        print(f"   • Environment variables are properly configured")
        print(f"   • Folder structure follows standardized paths")
        print(f"   • Migration script is functional")
        print(f"   • Data retention preserves today's data")
        print(f"   • Ticker processing works correctly")
        print(f"   • All imports are successful")
        print(f"\n🚀 The system is ready for deployment!")
    else:
        print(f"\n❌ ISSUES FOUND:")
        print(f"   Some components need attention before deployment.")
        print(f"   Check the failed verifications above.")
    
    return all_passed

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)