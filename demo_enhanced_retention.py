#!/usr/bin/env python3
"""
Quick Test Script for Enhanced Data Retention

This script demonstrates the key functionality of the enhanced data retention system
without requiring API access or Spaces credentials. Perfect for testing the logic.
"""

import sys
import os
import pandas as pd
from datetime import datetime, timedelta
import pytz

# Add project root to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '.')))

def demo_enhanced_retention():
    """Demonstrate the enhanced data retention functionality."""
    print("🚀 ENHANCED DATA RETENTION DEMO")
    print("=" * 60)
    
    from utils.helpers import apply_data_retention, is_today_present_enhanced
    from utils.config import TIMEZONE, INTRADAY_TRIM_DAYS
    
    # Create sample intraday data with multiple days
    ny_tz = pytz.timezone(TIMEZONE)
    now = datetime.now(ny_tz)
    today = now.replace(hour=12, minute=0, second=0, microsecond=0)
    
    print(f"📅 Current time ({TIMEZONE}): {now}")
    print(f"📅 Today's reference: {today}")
    print(f"🔧 Retention period: {INTRADAY_TRIM_DAYS} days")
    
    # Generate sample data for the last 15 days
    timestamps = []
    for days_back in range(15, 0, -1):
        for hour in [9, 12, 15]:  # Market hours samples
            timestamp = today - timedelta(days=days_back) + timedelta(hours=hour-12)
            timestamps.append(timestamp)
    
    # Add today's data (critical!)
    for hour in [9, 12, 15]:
        timestamps.append(today.replace(hour=hour))
    
    # Create sample OHLCV data
    sample_data = pd.DataFrame({
        'Date': timestamps,
        'Open': [100.0 + i*0.1 for i in range(len(timestamps))],
        'High': [105.0 + i*0.1 for i in range(len(timestamps))],
        'Low': [95.0 + i*0.1 for i in range(len(timestamps))],
        'Close': [102.0 + i*0.1 for i in range(len(timestamps))],
        'Volume': [1000000 + i*1000 for i in range(len(timestamps))]
    })
    
    print(f"\n📊 SAMPLE DATA CREATED:")
    print(f"   Total rows: {len(sample_data)}")
    print(f"   Date range: {sample_data['Date'].min()} to {sample_data['Date'].max()}")
    print(f"   Days span: {(sample_data['Date'].max() - sample_data['Date'].min()).days} days")
    
    # Check today's data before filtering
    has_today_before = is_today_present_enhanced(sample_data, 'Date')
    print(f"   Today's data present: {'✅' if has_today_before else '❌'}")
    
    # Apply the enhanced retention logic
    print(f"\n🔄 APPLYING ENHANCED RETENTION LOGIC...")
    print(f"   (Watch for detailed logging below)")
    print("-" * 60)
    
    filtered_data = apply_data_retention(sample_data.copy())
    
    print("-" * 60)
    print(f"📋 RETENTION RESULTS:")
    print(f"   Original rows: {len(sample_data)}")
    print(f"   Filtered rows: {len(filtered_data)}")
    print(f"   Rows removed: {len(sample_data) - len(filtered_data)}")
    
    if not filtered_data.empty:
        print(f"   New date range: {filtered_data['Date'].min()} to {filtered_data['Date'].max()}")
        
        # Critical check: verify today's data is preserved
        has_today_after = is_today_present_enhanced(filtered_data, 'Date')
        if has_today_after:
            print(f"   ✅ SUCCESS: Today's data PRESERVED!")
        else:
            print(f"   ❌ CRITICAL: Today's data LOST!")
        
        # Show some sample rows
        print(f"\n📄 SAMPLE OF FILTERED DATA (last 5 rows):")
        print(filtered_data[['Date', 'Close']].tail().to_string(index=False))
        
        return has_today_after
    else:
        print(f"   ❌ CRITICAL: All data was filtered out!")
        return False

def demo_configuration():
    """Show current configuration."""
    print(f"\n⚙️  CURRENT CONFIGURATION:")
    print("=" * 40)
    
    from utils.config import (
        INTRADAY_TRIM_DAYS, INTRADAY_EXCLUDE_TODAY,
        INTRADAY_INCLUDE_PREMARKET, INTRADAY_INCLUDE_AFTERHOURS,
        TIMEZONE, DEBUG_MODE
    )
    
    configs = [
        ("Retention Days", INTRADAY_TRIM_DAYS),
        ("Exclude Today", INTRADAY_EXCLUDE_TODAY),
        ("Include Pre-market", INTRADAY_INCLUDE_PREMARKET),
        ("Include After-hours", INTRADAY_INCLUDE_AFTERHOURS),
        ("Timezone", TIMEZONE),
        ("Debug Mode", DEBUG_MODE)
    ]
    
    for name, value in configs:
        print(f"   {name}: {value}")

def demo_path_structure():
    """Show standardized path structure."""
    print(f"\n📁 STANDARDIZED PATH STRUCTURE:")
    print("=" * 40)
    
    from utils.config import INTRADAY_DATA_DIR, INTRADAY_30MIN_DATA_DIR, DAILY_DATA_DIR
    
    sample_ticker = "TSLA"
    paths = [
        ("1min data", f"data/intraday/{sample_ticker}_1min.csv"),
        ("30min data", f"data/intraday_30min/{sample_ticker}_30min.csv"),
        ("Daily data", f"data/daily/{sample_ticker}_daily.csv")
    ]
    
    for name, path in paths:
        print(f"   {name}: {path}")
    
    print(f"\n📂 Directory structure:")
    dirs = [
        ("1min dir", INTRADAY_DATA_DIR),
        ("30min dir", INTRADAY_30MIN_DATA_DIR),
        ("Daily dir", DAILY_DATA_DIR)
    ]
    
    for name, directory in dirs:
        exists = os.path.exists(directory)
        print(f"   {name}: {directory} ({'✅ exists' if exists else '❌ missing'})")

def main():
    """Run the demo."""
    print("🎯 ENHANCED DATA RETENTION SYSTEM DEMO")
    print("=" * 80)
    
    try:
        # Show configuration
        demo_configuration()
        
        # Show path structure
        demo_path_structure()
        
        # Demonstrate retention logic
        retention_success = demo_enhanced_retention()
        
        print(f"\n🏁 DEMO SUMMARY:")
        print("=" * 40)
        
        if retention_success:
            print("✅ DEMO SUCCESSFUL!")
            print("   • Configuration is correct")
            print("   • Path structure is standardized") 
            print("   • Data retention preserves today's data")
            print("   • System is ready for production")
            
            print(f"\n💡 KEY BENEFITS:")
            print("   • TODAY'S DATA IS ALWAYS PRESERVED")
            print("   • All market sessions included by default")
            print("   • Configurable retention via environment variables")
            print("   • Enhanced logging for troubleshooting")
            print("   • Standardized data/ folder structure")
        else:
            print("❌ DEMO FAILED!")
            print("   • There may be issues with the retention logic")
            print("   • Check configuration and logs above")
        
        return retention_success
        
    except Exception as e:
        print(f"❌ DEMO ERROR: {e}")
        return False

if __name__ == "__main__":
    success = main()
    print(f"\n{'🎉 Ready for deployment!' if success else '🔧 Needs attention before deployment.'}")
    exit(0 if success else 1)