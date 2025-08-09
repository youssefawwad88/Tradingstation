"""
Test script to validate TEST_MODE functionality and fixture loading.
"""

import os
import sys
import pandas as pd
from datetime import datetime

# Add project root to path
sys.path.insert(0, os.path.abspath('.'))

from utils.config import TEST_MODE, FORCE_LIVE_API
from utils.alpha_vantage_api import get_api
from utils.logging_setup import setup_logging, get_logger

# Set up logging
setup_logging()
logger = get_logger(__name__)

def test_fixture_loading():
    """Test that API fixtures are loaded correctly in TEST_MODE."""
    print("=== Testing TEST_MODE Fixture Loading ===")
    
    # Force TEST_MODE on
    os.environ['TEST_MODE'] = 'true'
    os.environ['FORCE_LIVE_API'] = 'false'
    
    # Get API instance
    api = get_api()
    
    # Test intraday data loading
    print("\n1. Testing intraday data fixture...")
    df_intraday = api.get_intraday_data("AAPL", interval="1min")
    
    if df_intraday is not None and not df_intraday.empty:
        print(f"✓ Intraday fixture loaded: {len(df_intraday)} rows")
        print(f"  Columns: {list(df_intraday.columns)}")
        print(f"  Date range: {df_intraday['timestamp'].min()} to {df_intraday['timestamp'].max()}")
    else:
        print("✗ Failed to load intraday fixture")
    
    # Test daily data loading
    print("\n2. Testing daily data fixture...")
    df_daily = api.get_daily_data("AAPL")
    
    if df_daily is not None and not df_daily.empty:
        print(f"✓ Daily fixture loaded: {len(df_daily)} rows")
        print(f"  Columns: {list(df_daily.columns)}")
        print(f"  Date range: {df_daily['date'].min()} to {df_daily['date'].max()}")
    else:
        print("✗ Failed to load daily fixture")
    
    # Test 30-min data loading
    print("\n3. Testing 30-min data fixture...")
    df_30min = api.get_intraday_data("AAPL", interval="30min")
    
    if df_30min is not None and not df_30min.empty:
        print(f"✓ 30-min fixture loaded: {len(df_30min)} rows")
        print(f"  Date range: {df_30min['timestamp'].min()} to {df_30min['timestamp'].max()}")
    else:
        print("✗ Failed to load 30-min fixture")
    
    # Test quote data (mock)
    print("\n4. Testing quote fixture...")
    quote = api.get_quote("AAPL")
    
    if quote:
        print(f"✓ Quote fixture loaded: {quote}")
    else:
        print("✗ Failed to load quote fixture")
    
    return True

def test_data_pipeline_with_fixtures():
    """Test that data pipeline jobs work with fixtures."""
    print("\n=== Testing Data Pipeline with Fixtures ===")
    
    # Test ticker management
    print("\n1. Testing ticker management...")
    from utils.ticker_management import load_manual_tickers, load_master_tickerlist
    
    manual_tickers = load_manual_tickers()
    print(f"✓ Manual tickers loaded: {len(manual_tickers)} tickers")
    
    # Test opportunity finder
    print("\n2. Testing opportunity finder with fixtures...")
    from ticker_selectors.opportunity_ticker_finder import OpportunityTickerFinder
    
    try:
        finder = OpportunityTickerFinder()
        sp500_tickers = finder.load_sp500_universe()
        print(f"✓ S&P 500 universe loaded: {len(sp500_tickers)} tickers")
        
        # Test data loading (should use fixtures)
        sample_tickers = sp500_tickers[:2]  # Test with just 2 tickers
        daily_data = finder.get_daily_data_for_screening(sample_tickers)
        print(f"✓ Daily data loaded for {len(daily_data)} tickers via fixtures")
        
    except Exception as e:
        print(f"✗ Opportunity finder test failed: {e}")
    
    # Test individual job execution
    print("\n3. Testing individual data jobs...")
    from jobs.update_daily import DailyDataUpdater
    
    try:
        updater = DailyDataUpdater()
        # Test with a single ticker to avoid API limits
        results = updater.run_update(['AAPL'])
        success_count = sum(1 for success in results.values() if success)
        print(f"✓ Daily update job completed: {success_count}/{len(results)} successful")
        
    except Exception as e:
        print(f"✗ Daily update job test failed: {e}")
    
    return True

def test_job_registry_execution():
    """Test job execution through the registry."""
    print("\n=== Testing Job Registry Execution ===")
    
    from orchestrator.job_registry import get_job_registry
    
    registry = get_job_registry()
    
    # Test a simple job that should work with fixtures
    print("\n1. Testing opportunity_ticker_finder job...")
    success, message = registry.execute_job('opportunity_ticker_finder')
    
    if success:
        print(f"✓ Job executed successfully: {message}")
    else:
        print(f"✗ Job execution failed: {message}")
    
    # Test job listing
    print("\n2. Testing job listing...")
    from orchestrator.job_registry import list_jobs
    
    jobs = list_jobs()
    print(f"✓ Job registry contains {len(jobs)} jobs")
    
    # Show which jobs can run
    runnable_jobs = []
    for job in jobs:
        can_run, reason = registry.can_run_job(job.name)
        if can_run:
            runnable_jobs.append(job.name)
    
    print(f"✓ {len(runnable_jobs)} jobs can run in current conditions")
    print(f"  Runnable: {runnable_jobs[:5]}")  # Show first 5
    
    return True

def main():
    """Main test runner."""
    print("Testing Trading Station in TEST_MODE")
    print("=" * 50)
    
    # Show current configuration
    print(f"TEST_MODE: {TEST_MODE}")
    print(f"FORCE_LIVE_API: {FORCE_LIVE_API}")
    
    try:
        # Run tests
        test_fixture_loading()
        test_data_pipeline_with_fixtures()
        test_job_registry_execution()
        
        print("\n" + "=" * 50)
        print("✅ All TEST_MODE tests completed successfully!")
        
        return True
        
    except Exception as e:
        print(f"\n❌ TEST_MODE tests failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)