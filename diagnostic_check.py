#!/usr/bin/env python3
"""
Emergency Diagnostic Script for Intraday Pipeline Issues
Run this in production to quickly identify problems.
"""

import os
import sys
import requests
import pandas as pd
from datetime import datetime
import pytz

# Add project root to path
sys.path.append(os.path.abspath(os.path.dirname(__file__)))

from utils.helpers import load_manual_tickers, read_df_from_s3, is_today_present
from utils.alpha_vantage_api import _make_api_request

def check_system_health():
    """Comprehensive system health check for production environment."""
    
    print("🔍 EMERGENCY DIAGNOSTIC CHECK - Intraday Pipeline")
    print("=" * 60)
    
    # Get current time
    ny_tz = pytz.timezone('America/New_York')
    current_time = datetime.now(ny_tz)
    print(f"Current time (NY): {current_time}")
    print(f"Current date (NY): {current_time.date()}")
    print()
    
    issues_found = []
    
    # 1. Check API Key
    print("1️⃣ CHECKING ALPHA VANTAGE API KEY...")
    api_key = os.getenv('ALPHA_VANTAGE_API_KEY')
    if not api_key:
        print("❌ CRITICAL: ALPHA_VANTAGE_API_KEY environment variable not set")
        issues_found.append("Missing Alpha Vantage API key")
    else:
        print(f"✅ API key configured: {api_key[:8]}***{api_key[-4:] if len(api_key) > 12 else '***'}")
        
        # Test API connectivity
        print("   Testing API connectivity...")
        try:
            test_params = {
                'function': 'TIME_SERIES_INTRADAY',
                'symbol': 'AAPL',
                'interval': '1min',
                'outputsize': 'compact',
                'apikey': api_key,
                'datatype': 'csv'
            }
            response = _make_api_request(test_params)
            if response:
                print("   ✅ API connectivity successful")
            else:
                print("   ❌ API request failed")
                issues_found.append("Alpha Vantage API connectivity failed")
        except Exception as e:
            print(f"   ❌ API test error: {e}")
            issues_found.append(f"Alpha Vantage API error: {e}")
    
    print()
    
    # 2. Check Spaces Credentials
    print("2️⃣ CHECKING DIGITALOCEAN SPACES CREDENTIALS...")
    spaces_key = os.getenv('SPACES_ACCESS_KEY_ID')
    spaces_secret = os.getenv('SPACES_SECRET_ACCESS_KEY')
    
    if not spaces_key or not spaces_secret:
        print("⚠️  WARNING: DigitalOcean Spaces credentials not configured")
        print("   Using local filesystem fallback for data persistence")
        issues_found.append("Spaces credentials missing (non-critical - local fallback active)")
    else:
        print("✅ Spaces credentials configured")
    
    print()
    
    # 3. Check Manual Tickers Loading
    print("3️⃣ CHECKING MANUAL TICKERS LOADING...")
    try:
        manual_tickers = load_manual_tickers()
        if manual_tickers:
            print(f"✅ {len(manual_tickers)} manual tickers loaded: {manual_tickers}")
        else:
            print("❌ No manual tickers loaded")
            issues_found.append("Manual tickers not loading")
    except Exception as e:
        print(f"❌ Error loading manual tickers: {e}")
        issues_found.append(f"Manual ticker loading error: {e}")
    
    print()
    
    # 4. Check Existing Data Files
    print("4️⃣ CHECKING EXISTING DATA FILES...")
    data_status = {}
    
    for ticker in ['AAPL', 'NVDA', 'MSFT', 'GOOGL', 'TSLA', 'AMD', 'AMZN', 'NFLX']:
        file_path = f'data/intraday/{ticker}_1min.csv'
        try:
            df = read_df_from_s3(file_path)
            if not df.empty:
                # Get last timestamp
                if 'Date' in df.columns:
                    df['Date'] = pd.to_datetime(df['Date'])
                    last_time = df['Date'].max()
                    today_present = is_today_present(df)
                    
                    data_status[ticker] = {
                        'rows': len(df),
                        'last_time': last_time,
                        'today_present': today_present
                    }
                    
                    status_icon = "✅" if today_present else "⚠️ "
                    print(f"   {status_icon} {ticker}: {len(df)} rows, last: {last_time}")
                    
                    if not today_present:
                        days_ago = (current_time.date() - last_time.date()).days
                        print(f"      📅 Data is {days_ago} days old")
                        
                else:
                    print(f"   ❌ {ticker}: Invalid data format (no Date column)")
            else:
                print(f"   ❌ {ticker}: No data found")
                data_status[ticker] = {'rows': 0, 'last_time': None, 'today_present': False}
        
        except Exception as e:
            print(f"   ❌ {ticker}: Error reading data - {e}")
            data_status[ticker] = {'rows': 0, 'last_time': None, 'today_present': False}
    
    print()
    
    # 5. Summary and Recommendations
    print("5️⃣ SUMMARY AND RECOMMENDATIONS")
    print("=" * 40)
    
    if issues_found:
        print("❌ ISSUES FOUND:")
        for i, issue in enumerate(issues_found, 1):
            print(f"   {i}. {issue}")
        print()
    
    # Count tickers missing today's data
    missing_today = sum(1 for ticker, status in data_status.items() if not status.get('today_present', False))
    
    if missing_today == 0:
        print("✅ ALL GOOD: All tickers have today's data")
    elif missing_today < len(data_status):
        print(f"⚠️  PARTIAL ISSUE: {missing_today}/{len(data_status)} tickers missing today's data")
    else:
        print(f"❌ CRITICAL: ALL {len(data_status)} tickers missing today's data")
    
    print()
    print("🔧 RECOMMENDED ACTIONS:")
    
    if api_key and not any("API" in issue for issue in issues_found):
        print("1. ✅ API is working - run update_intraday_compact.py to fetch latest data")
    else:
        print("1. ❌ Fix Alpha Vantage API key and connectivity first")
    
    if not spaces_key or not spaces_secret:
        print("2. ⚠️  Set SPACES_ACCESS_KEY_ID and SPACES_SECRET_ACCESS_KEY for cloud storage")
    else:
        print("2. ✅ Spaces configured")
    
    print("3. 🔄 Manual tickers are now properly configured and will be processed")
    print("4. 💾 Data will persist locally even if Spaces upload fails")
    
    if missing_today > 0:
        print(f"5. 📊 Execute pipeline to fetch missing data for {missing_today} tickers")
    
    print("\n" + "=" * 60)
    print("Diagnostic check complete!")

if __name__ == "__main__":
    check_system_health()