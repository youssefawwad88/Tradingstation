#!/usr/bin/env python3
"""
Test script for basic ticker list generation without API calls
"""

import sys
import os
import pandas as pd

# Add project root to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '.')))

def test_basic_ticker_generation():
    """Test basic ticker generation without API calls"""
    print("🧪 Testing basic ticker list generation...")
    
    # Test manual ticker loading
    manual_tickers = []
    manual_file = "tickerlist.txt"
    if os.path.exists(manual_file):
        with open(manual_file, 'r') as f:
            for line in f.readlines():
                line = line.strip()
                if line:
                    # Remove number prefix if present (e.g., "1.NVDA" -> "NVDA")
                    if '.' in line and line.split('.')[0].isdigit():
                        ticker = line.split('.', 1)[1]
                    else:
                        ticker = line
                    manual_tickers.append(ticker)
    
    print(f"✅ Manual tickers loaded: {manual_tickers}")
    
    # Test S&P 500 loading
    sp500_tickers = []
    sp500_file = "data/universe/sp500.csv"
    if os.path.exists(sp500_file):
        df = pd.read_csv(sp500_file)
        sp500_tickers = df['Symbol'].tolist()
    
    print(f"✅ S&P 500 tickers loaded: {len(sp500_tickers)} tickers")
    print(f"   First 10: {sp500_tickers[:10]}")
    
    # Simulate filtered S&P 500 (for testing, just take first 5)
    filtered_sp500 = sp500_tickers[:5]
    print(f"📊 Simulated filtered S&P 500: {filtered_sp500}")
    
    # Combine and create master list
    all_tickers = manual_tickers + filtered_sp500
    
    # Remove duplicates while preserving order
    master_tickers = []
    seen = set()
    for ticker in all_tickers:
        if ticker not in seen:
            master_tickers.append(ticker)
            seen.add(ticker)
    
    print(f"🎯 Master ticker list: {master_tickers}")
    
    # Create DataFrame and save
    df = pd.DataFrame({
        'ticker': master_tickers,
        'source': ['manual' if t in manual_tickers else 'sp500_filtered' for t in master_tickers],
        'generated_at': pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')
    })
    
    output_file = "master_tickerlist.csv"
    df.to_csv(output_file, index=False)
    print(f"💾 Master ticker list saved: {output_file}")
    
    # Display the final result
    print("\n📋 Final Master Ticker List:")
    for i, row in df.iterrows():
        print(f"   {i+1:2d}. {row['ticker']} ({row['source']})")
    
    return True

if __name__ == "__main__":
    success = test_basic_ticker_generation()
    if success:
        print("\n✅ Basic ticker generation test completed successfully")
    else:
        print("\n❌ Basic ticker generation test failed")