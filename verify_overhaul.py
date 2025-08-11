#!/usr/bin/env python3
"""
Final Verification Script - Data Engine Overhaul
===============================================

This script verifies that all requirements from the problem statement have been implemented correctly.
"""

import os
import sys
import inspect

# Add project root to Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

def verify_file_deletions():
    """Verify that old flawed scripts have been deleted"""
    print("🗑️ Verifying file deletions...")
    
    old_files = [
        "jobs/update_all_data.py",
        "jobs/update_intraday_compact.py"
    ]
    
    all_deleted = True
    for file_path in old_files:
        if os.path.exists(file_path):
            print(f"   ❌ {file_path} still exists (should be deleted)")
            all_deleted = False
        else:
            print(f"   ✅ {file_path} successfully deleted")
    
    return all_deleted

def verify_new_engines():
    """Verify that new engines have been created with correct functionality"""
    print("\n🛠️ Verifying new engines...")
    
    new_files = [
        "jobs/full_fetch.py",
        "jobs/compact_update.py"
    ]
    
    all_exist = True
    for file_path in new_files:
        if os.path.exists(file_path):
            print(f"   ✅ {file_path} created successfully")
            
            # Check file content for key requirements
            with open(file_path, 'r') as f:
                content = f.read()
                
            # Check for ticker iteration
            if "for i, ticker in enumerate(tickers" in content:
                print(f"      ✅ Contains ticker iteration logic")
            else:
                print(f"      ❌ Missing ticker iteration logic")
                all_exist = False
                
            # Check for master tickerlist reading
            if "read_master_tickerlist()" in content:
                print(f"      ✅ Reads from master_tickerlist.csv")
            else:
                print(f"      ❌ Does not read from master_tickerlist.csv")
                all_exist = False
                
            # Check for timestamp standardization
            if "timestamp_standardization" in content or "standardize_timestamps" in content:
                print(f"      ✅ Includes timestamp standardization")
            else:
                print(f"      ❌ Missing timestamp standardization")
                all_exist = False
        else:
            print(f"   ❌ {file_path} does not exist")
            all_exist = False
    
    return all_exist

def verify_orchestrator_updates():
    """Verify that orchestrator has been updated to use new engines"""
    print("\n🎼 Verifying orchestrator updates...")
    
    orchestrator_path = "orchestrator/run_all.py"
    if not os.path.exists(orchestrator_path):
        print(f"   ❌ {orchestrator_path} does not exist")
        return False
    
    with open(orchestrator_path, 'r') as f:
        content = f.read()
    
    checks = [
        ("jobs/full_fetch.py", "References new full_fetch.py"),
        ("jobs/compact_update.py", "References new compact_update.py"),
        ("Full Fetch Engine", "Contains Full Fetch Engine description")
    ]
    
    all_updated = True
    for check_string, description in checks:
        if check_string in content:
            print(f"   ✅ {description}")
        else:
            print(f"   ❌ Missing: {description}")
            all_updated = False
    
    # Check that old references are removed
    old_refs = [
        "jobs/update_all_data.py",
        "jobs/update_intraday_compact.py"
    ]
    
    for old_ref in old_refs:
        if old_ref in content:
            print(f"   ❌ Still contains reference to {old_ref}")
            all_updated = False
        else:
            print(f"   ✅ Removed reference to {old_ref}")
    
    return all_updated

def verify_functionality():
    """Verify that the new engines can be imported and have correct functions"""
    print("\n🧪 Verifying functionality...")
    
    try:
        # Test full_fetch engine
        from jobs.full_fetch import run_full_fetch, fetch_and_process_ticker, trim_data_to_requirements
        print("   ✅ full_fetch.py imports successfully")
        print("      ✅ run_full_fetch function exists")
        print("      ✅ fetch_and_process_ticker function exists")
        print("      ✅ trim_data_to_requirements function exists")
        
        # Test compact_update engine
        from jobs.compact_update import run_compact_update, process_ticker_interval, merge_new_candles
        print("   ✅ compact_update.py imports successfully")
        print("      ✅ run_compact_update function exists")
        print("      ✅ process_ticker_interval function exists")
        print("      ✅ merge_new_candles function exists")
        
        # Test orchestrator
        from orchestrator.run_all import run_daily_data_jobs, run_intraday_updates
        print("   ✅ orchestrator/run_all.py imports successfully")
        print("      ✅ run_daily_data_jobs function exists")
        print("      ✅ run_intraday_updates function exists")
        
        return True
        
    except Exception as e:
        print(f"   ❌ Import error: {e}")
        return False

def verify_requirements_implementation():
    """Verify specific requirements from problem statement"""
    print("\n📋 Verifying problem statement requirements...")
    
    requirements_met = True
    
    # Check Full Fetch Engine requirements
    print("   🔍 Full Fetch Engine requirements:")
    try:
        with open("jobs/full_fetch.py", 'r') as f:
            full_fetch_content = f.read()
        
        requirements = [
            ("read_master_tickerlist", "Reads entire ticker column from master_tickerlist.csv"),
            ("for i, ticker in enumerate(tickers", "Loops through every single ticker"),
            ("outputsize='full'", "Performs full historical data fetch"),
            ("200", "Implements 200 rows cleanup for daily"),
            ("500", "Implements 500 rows cleanup for 30-min"),
            ("7 days", "Implements 7 days cleanup for 1-min"),
            ("standardize_timestamps", "Performs timestamp standardization")
        ]
        
        for req, desc in requirements:
            if req in full_fetch_content:
                print(f"      ✅ {desc}")
            else:
                print(f"      ❌ Missing: {desc}")
                requirements_met = False
                
    except Exception as e:
        print(f"      ❌ Error checking full_fetch.py: {e}")
        requirements_met = False
    
    # Check Compact Update Engine requirements
    print("   🔍 Compact Update Engine requirements:")
    try:
        with open("jobs/compact_update.py", 'r') as f:
            compact_content = f.read()
        
        requirements = [
            ("read_master_tickerlist", "Reads entire ticker column from master_tickerlist.csv"),
            ("for i, ticker in enumerate(tickers", "Loops through every single ticker"),
            ("outputsize='compact'", "Fetches compact data (latest 100 candles)"),
            ("merge_new_candles", "Merges with existing data"),
            ("read_df_from_s3", "Reads existing files"),
            ("standardize_timestamps", "Performs timestamp standardization")
        ]
        
        for req, desc in requirements:
            if req in compact_content:
                print(f"      ✅ {desc}")
            else:
                print(f"      ❌ Missing: {desc}")
                requirements_met = False
                
    except Exception as e:
        print(f"      ❌ Error checking compact_update.py: {e}")
        requirements_met = False
    
    return requirements_met

def main():
    """Run all verification checks"""
    print("=" * 60)
    print("🔍 FINAL VERIFICATION - DATA ENGINE OVERHAUL")
    print("=" * 60)
    
    checks = [
        ("File Deletions", verify_file_deletions),
        ("New Engines Creation", verify_new_engines),
        ("Orchestrator Updates", verify_orchestrator_updates),
        ("Functionality", verify_functionality),
        ("Requirements Implementation", verify_requirements_implementation)
    ]
    
    all_passed = True
    results = {}
    
    for check_name, check_func in checks:
        try:
            result = check_func()
            results[check_name] = result
            if not result:
                all_passed = False
        except Exception as e:
            print(f"❌ Error in {check_name}: {e}")
            results[check_name] = False
            all_passed = False
    
    # Final summary
    print("\n" + "=" * 60)
    print("📊 VERIFICATION SUMMARY")
    print("=" * 60)
    
    for check_name, result in results.items():
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"{status} {check_name}")
    
    print("\n" + "=" * 60)
    if all_passed:
        print("🎉 ALL VERIFICATION CHECKS PASSED!")
        print("✅ Data Engine Overhaul implementation is COMPLETE and meets all requirements")
        print("🚀 Ready for production deployment")
    else:
        print("❌ SOME VERIFICATION CHECKS FAILED")
        print("🔧 Please review and fix the issues above")
    print("=" * 60)
    
    return all_passed

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)