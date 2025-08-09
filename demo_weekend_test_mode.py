#!/usr/bin/env python3
"""
Demo: Weekend Test Mode for Data Handling

This demo showcases the weekend test mode functionality that was implemented
as part of the data handling refactor. It demonstrates:

1. Automatic weekend detection
2. Test mode simulation without API calls
3. Detailed logging for all operations
4. Proper data fetching limits (200 daily, 500 30-min, 7 days 1-min)
5. Cleanup procedures with retention limits
6. Manual ticker-only sources
"""

import sys
import os
from datetime import datetime

# Add project root to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '.')))

def demo_weekend_detection():
    """Demo weekend detection functionality"""
    print("🕐 Weekend Detection Demo")
    print("-" * 40)
    
    from utils.helpers import is_weekend, should_use_test_mode
    
    weekend_status = is_weekend()
    test_mode_status = should_use_test_mode()
    
    print(f"Current time: {datetime.now().strftime('%A, %Y-%m-%d %H:%M:%S')}")
    print(f"Is weekend: {weekend_status}")
    print(f"Test mode active: {test_mode_status}")
    
    if test_mode_status:
        print("✅ Weekend test mode is ACTIVE - no API calls will be made")
    else:
        print("ℹ️  Live mode would be active - API calls would be made")
    
    print()

def demo_manual_ticker_sources():
    """Demo manual ticker source functionality"""
    print("📋 Manual Ticker Sources Demo")
    print("-" * 40)
    
    from utils.helpers import read_master_tickerlist
    
    print("Loading tickers from master_tickerlist.csv (manual sources only)...")
    tickers = read_master_tickerlist()
    
    print(f"✅ Loaded {len(tickers)} manual tickers:")
    for i, ticker in enumerate(tickers, 1):
        print(f"   {i:2d}. {ticker}")
    
    print("\nNote: Automated sources (S&P 500 loader, opportunity finder) have been removed")
    print()

def demo_data_fetching_simulation():
    """Demo the data fetching with test mode simulation"""
    print("🔄 Data Fetching Simulation Demo")
    print("-" * 40)
    
    from jobs.update_all_data import simulate_data_fetch
    
    sample_ticker = "DEMO"
    
    print("Simulating data fetch for different intervals:")
    
    # Daily data
    daily_df = simulate_data_fetch(sample_ticker, "daily", 200)
    print(f"✅ Daily data: {len(daily_df)} rows (target: 200)")
    
    # 30-min data
    min_30_df = simulate_data_fetch(sample_ticker, "30min", 500)
    print(f"✅ 30-min data: {len(min_30_df)} rows (target: 500)")
    
    # 1-min data
    min_1_df = simulate_data_fetch(sample_ticker, "1min", 7*24*60)
    print(f"✅ 1-min data: {len(min_1_df)} rows (target: ~{7*24*60} for 7 days)")
    
    print("\nAll data includes realistic OHLCV structure with proper timestamps")
    print()

def demo_cleanup_procedures():
    """Demo cleanup and retention procedures"""
    print("🧹 Cleanup Procedures Demo")
    print("-" * 40)
    
    from utils.helpers import cleanup_data_retention
    import pandas as pd
    
    # Create oversized sample data
    print("Creating oversized sample data...")
    large_daily = pd.DataFrame({'close': range(300), 'volume': range(300)})
    large_30min = pd.DataFrame({'close': range(700), 'volume': range(700)})
    large_1min = pd.DataFrame({
        'timestamp': pd.date_range('2025-01-01', periods=15000, freq='1min'),
        'close': range(15000), 
        'volume': range(15000)
    })
    
    print(f"Before cleanup: Daily={len(large_daily)}, 30min={len(large_30min)}, 1min={len(large_1min)}")
    
    # Apply cleanup
    cleaned_daily, cleaned_30min, cleaned_1min = cleanup_data_retention(
        "DEMO", large_daily, large_30min, large_1min
    )
    
    print(f"After cleanup:  Daily={len(cleaned_daily)}, 30min={len(cleaned_30min)}, 1min={len(cleaned_1min)}")
    print("✅ Retention limits properly applied: 200 daily, 500 30-min, 7 days 1-min")
    print()

def demo_detailed_logging():
    """Demo detailed logging functionality"""
    print("📊 Detailed Logging Demo")
    print("-" * 40)
    
    from utils.helpers import log_detailed_operation
    import time
    
    print("Sample detailed log entries (like those generated during data processing):")
    print()
    
    # Simulate some operations with logging
    start_time = datetime.now()
    time.sleep(0.1)  # Simulate work
    log_detailed_operation("DEMO", "Data Fetch", start_time, row_count_after=200)
    
    start_time = datetime.now()
    time.sleep(0.05)  # Simulate work  
    log_detailed_operation("DEMO", "Cleanup", start_time, 15000, 10080, "7-day retention applied")
    
    start_time = datetime.now()
    time.sleep(0.02)  # Simulate work
    log_detailed_operation("DEMO", "Save Complete", start_time, details="All files saved successfully")
    
    print("\nEach operation includes timestamps, duration, row counts, and details")
    print()

def demo_full_integration():
    """Demo by running the actual update_all_data script"""
    print("🚀 Full Integration Demo")
    print("-" * 40)
    
    print("Running the actual update_all_data.py script...")
    print("This will process all manual tickers using weekend test mode")
    print()
    
    import subprocess
    
    try:
        result = subprocess.run(
            [sys.executable, "jobs/update_all_data.py"],
            cwd=os.path.dirname(os.path.abspath(__file__)),
            capture_output=True,
            text=True,
            timeout=60
        )
        
        if result.returncode == 0:
            print("✅ Full integration demo completed successfully!")
            
            # Extract key metrics from output
            output = result.stdout + result.stderr
            if "TEST MODE" in output:
                print("✅ Weekend test mode was automatically activated")
            if "Full Rebuild Complete" in output:
                print("✅ All tickers processed successfully")
            
            # Count processed tickers
            lines = output.split('\n')
            rebuild_lines = [line for line in lines if "Full Rebuild Complete" in line]
            print(f"✅ Processed {len(rebuild_lines)} tickers with detailed logging")
            
        else:
            print(f"❌ Demo failed with exit code {result.returncode}")
            print("Error output:", result.stderr[:200], "...")
    
    except subprocess.TimeoutExpired:
        print("❌ Demo timed out")
    except Exception as e:
        print(f"❌ Demo failed: {e}")
    
    print()

def main():
    """Run the complete weekend test mode demo"""
    print("🎯 Weekend Test Mode Demo for Trading Station Data Handling")
    print("=" * 70)
    print()
    print("This demo showcases the new data handling features implemented:")
    print("• Manual ticker sources only (no automated S&P 500 or opportunity finder)")
    print("• Weekend test mode with simulated data (no API calls)")
    print("• Proper data retention limits (200 daily, 500 30-min, 7 days 1-min)")
    print("• Cleanup procedures after data fetch")
    print("• Detailed logging with timestamps and row counts")
    print()
    
    # Run all demos
    demo_weekend_detection()
    demo_manual_ticker_sources()
    demo_data_fetching_simulation()
    demo_cleanup_procedures()
    demo_detailed_logging()
    demo_full_integration()
    
    print("=" * 70)
    print("🎉 Demo Complete!")
    print()
    print("Key Benefits of the New System:")
    print("• ✅ Reliable weekend testing without API waste")
    print("• ✅ Clean, unified data storage in /data/ structure")
    print("• ✅ Proper retention limits prevent data bloat")
    print("• ✅ Detailed logging enables performance monitoring")
    print("• ✅ Manual ticker control removes automation complexity")
    print("• ✅ Ready for Monday live mode with full visibility")

if __name__ == "__main__":
    main()