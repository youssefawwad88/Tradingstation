#!/usr/bin/env python3
"""
Test script to verify timestamp standardization is working correctly.

This script tests the rigorous timestamp standardization process:
1. Parse Timestamps: Read the raw timestamp string from the API
2. Localize to New York Time: Convert to timezone-aware object using 'America/New_York' timezone  
3. Standardize to UTC for Storage: Save the final timestamp in UTC format
"""

import sys
import os
import pandas as pd
import pytz
from datetime import datetime

# Add project root to Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.timestamp_standardizer import (
    apply_timestamp_standardization_to_api_data,
    validate_timestamp_standardization,
    standardize_timestamp_column
)

def test_timestamp_standardization():
    """Test the timestamp standardization process with sample data."""
    
    print("🧪 TESTING TIMESTAMP STANDARDIZATION")
    print("=" * 60)
    
    # Test 1: Intraday data with Eastern Time timestamps (current format)
    print("\n📊 TEST 1: Intraday Data with Eastern Time Timestamps")
    print("-" * 50)
    
    # Create sample data that mimics current Alpha Vantage format
    sample_intraday = pd.DataFrame({
        'timestamp': [
            '2025-08-10 09:30:00',  # Market open
            '2025-08-10 12:00:00',  # Midday
            '2025-08-10 16:00:00',  # Market close
            '2025-08-10 20:00:00'   # After hours
        ],
        'open': [100.0, 101.0, 102.0, 103.0],
        'high': [100.5, 101.5, 102.5, 103.5],
        'low': [99.5, 100.5, 101.5, 102.5],
        'close': [100.2, 101.2, 102.2, 103.2],
        'volume': [10000, 15000, 12000, 8000]
    })
    
    print("📥 Original data:")
    print(sample_intraday[['timestamp', 'close']].to_string(index=False))
    
    # Apply standardization
    standardized_intraday = apply_timestamp_standardization_to_api_data(
        sample_intraday, data_type='intraday'
    )
    
    print("\n📤 Standardized data (UTC):")
    print(standardized_intraday[['timestamp', 'close']].to_string(index=False))
    
    # Validate
    is_valid = validate_timestamp_standardization(standardized_intraday)
    print(f"\n✅ Validation: {'PASSED' if is_valid else 'FAILED'}")
    
    # Test 2: Daily data 
    print("\n\n📊 TEST 2: Daily Data Standardization")
    print("-" * 50)
    
    sample_daily = pd.DataFrame({
        'timestamp': [
            '2025-08-08',  # Friday
            '2025-08-09',  # Saturday (weekend)
            '2025-08-10'   # Sunday (weekend)
        ],
        'open': [100.0, 101.0, 102.0],
        'high': [100.5, 101.5, 102.5], 
        'low': [99.5, 100.5, 101.5],
        'close': [100.2, 101.2, 102.2],
        'volume': [1000000, 800000, 900000]
    })
    
    print("📥 Original daily data:")
    print(sample_daily[['timestamp', 'close']].to_string(index=False))
    
    # Apply standardization
    standardized_daily = apply_timestamp_standardization_to_api_data(
        sample_daily, data_type='daily'
    )
    
    print("\n📤 Standardized daily data (UTC @ 4PM ET):")
    print(standardized_daily[['timestamp', 'close']].to_string(index=False))
    
    # Validate
    is_valid_daily = validate_timestamp_standardization(standardized_daily)
    print(f"\n✅ Validation: {'PASSED' if is_valid_daily else 'FAILED'}")
    
    # Test 3: Demonstrate timezone conversion
    print("\n\n🌍 TEST 3: Timezone Conversion Demonstration")
    print("-" * 50)
    
    ny_tz = pytz.timezone('America/New_York')
    utc_tz = pytz.UTC
    
    # Create a timestamp in Eastern Time 
    et_time = ny_tz.localize(datetime(2025, 8, 10, 14, 30))  # 2:30 PM ET
    utc_time = et_time.astimezone(utc_tz)
    
    print(f"📅 Original Eastern Time: {et_time}")
    print(f"🌍 Converted to UTC: {utc_time}")
    print(f"📝 ISO Format (for CSV): {utc_time.strftime('%Y-%m-%d %H:%M:%S+00:00')}")
    
    # Test 4: Verify CSV storage format
    print("\n\n💾 TEST 4: CSV Storage Format Verification")
    print("-" * 50)
    
    # Show what the standardized data looks like when saved to CSV
    csv_content = standardized_intraday.to_csv(index=False, float_format='%.2f')
    print("📄 CSV content preview:")
    print(csv_content[:200] + "...")
    
    print("\n" + "=" * 60)
    print("🎯 TIMESTAMP STANDARDIZATION TEST SUMMARY:")
    print(f"   ✅ Intraday standardization: {'PASSED' if is_valid else 'FAILED'}")
    print(f"   ✅ Daily standardization: {'PASSED' if is_valid_daily else 'FAILED'}")
    print(f"   ✅ All timestamps in UTC format: {'+00:00' in csv_content}")
    print(f"   ✅ Timezone conversion working: {utc_time.tzinfo == utc_tz}")
    
    overall_success = is_valid and is_valid_daily and ('+00:00' in csv_content)
    print(f"\n🏆 OVERALL RESULT: {'SUCCESS' if overall_success else 'FAILED'}")
    
    if overall_success:
        print("✅ Timestamp standardization is working correctly!")
        print("   All data will be stored with consistent UTC timestamps.")
    else:
        print("❌ Timestamp standardization needs attention!")
    
    return overall_success


if __name__ == "__main__":
    success = test_timestamp_standardization()
    sys.exit(0 if success else 1)