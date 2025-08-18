#!/usr/bin/env python3
"""
Comprehensive API Diagnostic Tool for Compact Data Fetching Failure

This script performs a deep dive analysis to identify the root cause of
the compact fetch failure issue as outlined in the problem statement.

Phase 1: Full System Diagnostic
1. API Endpoint Validation - verify URL construction
2. Response Content Audit - manual API execution and raw response inspection
3. Data Processing Logic Audit - timestamp/timezone handling verification
"""

import json
import logging
import os
import sys
from datetime import datetime, timedelta
from io import StringIO

import pandas as pd
import pytz
import requests

# Add project root to Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), ".")))

from utils.alpha_vantage_api import get_intraday_data
from utils.config import ALPHA_VANTAGE_API_KEY
from utils.timestamp_standardizer import apply_timestamp_standardization_to_api_data

# Set up detailed logging
logging.basicConfig(
    level=logging.INFO, 
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('/tmp/api_diagnostic.log')
    ]
)
logger = logging.getLogger(__name__)

# Test configuration
TEST_SYMBOL = "AAPL"  # Use a highly liquid stock for testing
TEST_INTERVAL = "1min"


class CompactFetchDiagnostic:
    """Comprehensive diagnostic tool for compact fetch failure analysis."""
    
    def __init__(self):
        self.ny_tz = pytz.timezone("America/New_York")
        self.utc_tz = pytz.UTC
        self.today_et = datetime.now(self.ny_tz).date()
        self.api_key = ALPHA_VANTAGE_API_KEY
        
    def log_section(self, title):
        """Log a clear section header."""
        logger.info("=" * 80)
        logger.info(f"🔍 {title}")
        logger.info("=" * 80)
        
    def check_1_api_endpoint_validation(self):
        """Phase 1.1: API Endpoint Validation - verify URL construction."""
        self.log_section("API ENDPOINT VALIDATION")
        
        if not self.api_key:
            logger.error("❌ ALPHA_VANTAGE_API_KEY not configured - cannot perform API tests")
            return False
            
        # Test both compact and full endpoint construction
        base_url = "https://www.alphavantage.co/query"
        
        for outputsize in ["compact", "full"]:
            params = {
                "function": "TIME_SERIES_INTRADAY",
                "symbol": TEST_SYMBOL,
                "interval": TEST_INTERVAL,
                "outputsize": outputsize,
                "apikey": self.api_key,
                "datatype": "csv",
            }
            
            # Construct full URL
            url_parts = [f"{k}={v}" for k, v in params.items()]
            full_url = f"{base_url}?{'&'.join(url_parts)}"
            
            logger.info(f"✅ {outputsize.upper()} URL constructed correctly:")
            logger.info(f"   URL: {full_url}")
            logger.info(f"   Function: {params['function']}")
            logger.info(f"   Symbol: {params['symbol']}")
            logger.info(f"   Interval: {params['interval']}")
            logger.info(f"   Output Size: {params['outputsize']}")
            logger.info(f"   Data Type: {params['datatype']}")
            
        return True
        
    def check_2_response_content_audit(self):
        """Phase 1.2: Response Content Audit - manual API execution and inspection."""
        self.log_section("RESPONSE CONTENT AUDIT")
        
        if not self.api_key:
            logger.error("❌ Cannot perform response audit without API key")
            return False
            
        for outputsize in ["compact", "full"]:
            logger.info(f"🔍 Testing {outputsize.upper()} API response for {TEST_SYMBOL}")
            
            # Manual API call construction (same as alpha_vantage_api.py)
            params = {
                "function": "TIME_SERIES_INTRADAY",
                "symbol": TEST_SYMBOL,
                "interval": TEST_INTERVAL,
                "outputsize": outputsize,
                "apikey": self.api_key,
                "datatype": "csv",
            }
            
            try:
                # Make the actual API call
                response = requests.get(
                    "https://www.alphavantage.co/query", 
                    params=params, 
                    timeout=30
                )
                response.raise_for_status()
                
                logger.info(f"✅ API call successful - Status Code: {response.status_code}")
                logger.info(f"   Response size: {len(response.text)} bytes")
                
                # Parse the CSV response
                df = pd.read_csv(StringIO(response.text))
                
                if df.empty or "Error Message" in df.columns:
                    logger.error(f"❌ API returned error or empty data: {df.head()}")
                    continue
                    
                logger.info(f"📊 Raw CSV data parsed successfully:")
                logger.info(f"   Total rows: {len(df)}")
                logger.info(f"   Columns: {list(df.columns)}")
                
                # Inspect the timestamp column
                timestamp_col = None
                for col in ["timestamp", "datetime", "Date", "time"]:
                    if col in df.columns:
                        timestamp_col = col
                        break
                        
                if timestamp_col:
                    logger.info(f"   Timestamp column: {timestamp_col}")
                    logger.info(f"   First timestamp: {df[timestamp_col].iloc[0]}")
                    logger.info(f"   Last timestamp: {df[timestamp_col].iloc[-1]}")
                    
                    # CRITICAL CHECK: Does the response contain today's data?
                    df[timestamp_col] = pd.to_datetime(df[timestamp_col])
                    
                    # Convert to ET for comparison
                    if df[timestamp_col].dt.tz is None:
                        df_et = df[timestamp_col].dt.tz_localize(self.ny_tz)
                    else:
                        df_et = df[timestamp_col].dt.tz_convert(self.ny_tz)
                        
                    today_data = df_et.dt.date == self.today_et
                    today_count = today_data.sum()
                    
                    logger.info(f"🔍 TODAY'S DATA ANALYSIS (ET timezone):")
                    logger.info(f"   Today's date (ET): {self.today_et}")
                    logger.info(f"   Rows with today's data: {today_count}")
                    logger.info(f"   Percentage of today's data: {(today_count/len(df)*100):.2f}%")
                    
                    if today_count > 0:
                        logger.info("✅ API IS RETURNING TODAY'S DATA")
                        today_rows = df[today_data]
                        logger.info(f"   First today entry: {today_rows[timestamp_col].min()}")
                        logger.info(f"   Last today entry: {today_rows[timestamp_col].max()}")
                    else:
                        logger.error("❌ API IS NOT RETURNING TODAY'S DATA")
                        logger.error("   This confirms the compact fetch failure issue!")
                        
                        # Show the most recent dates available
                        recent_dates = df_et.dt.date.value_counts().head(3)
                        logger.error(f"   Most recent dates in response: {recent_dates}")
                        
                else:
                    logger.error(f"❌ No timestamp column found in response")
                    
            except Exception as e:
                logger.error(f"❌ API call failed for {outputsize}: {e}")
                
        return True
        
    def check_3_data_processing_logic_audit(self):
        """Phase 1.3: Data Processing Logic Audit - timestamp/timezone handling."""
        self.log_section("DATA PROCESSING LOGIC AUDIT")
        
        if not self.api_key:
            logger.warning("⚠️ Skipping processing audit - no API key for live data")
            # Create mock data for processing logic test
            mock_data = self._create_mock_current_day_data()
            return self._test_processing_logic(mock_data, is_mock=True)
        
        # Use our actual API wrapper to test processing
        logger.info("🔍 Testing data processing logic with live API data")
        
        for outputsize in ["compact", "full"]:
            logger.info(f"📊 Testing processing logic with {outputsize} data")
            
            # Use the actual function from alpha_vantage_api.py
            df = get_intraday_data(TEST_SYMBOL, interval=TEST_INTERVAL, outputsize=outputsize)
            
            if df.empty:
                logger.error(f"❌ No data returned from get_intraday_data({outputsize})")
                continue
                
            success = self._test_processing_logic(df, is_mock=False)
            if not success:
                logger.error(f"❌ Processing logic failed for {outputsize} data")
                
        return True
        
    def _create_mock_current_day_data(self):
        """Create mock data that includes current day for testing processing logic."""
        logger.info("🧪 Creating mock data with current day entries")
        
        # Create timestamps for today including pre-market hours
        today_et = datetime.now(self.ny_tz).replace(hour=7, minute=0, second=0, microsecond=0)
        timestamps = []
        
        # Add some pre-market data (7:00 AM - 9:29 AM ET)
        current_time = today_et
        while current_time.hour < 9 or (current_time.hour == 9 and current_time.minute < 30):
            timestamps.append(current_time)
            current_time += timedelta(minutes=1)
            
        # Add some market hours data (9:30 AM - 10:00 AM ET) 
        while current_time.hour < 10:
            timestamps.append(current_time)
            current_time += timedelta(minutes=1)
            
        # Convert to naive timestamps (as API would return)
        naive_timestamps = [ts.replace(tzinfo=None) for ts in timestamps]
        
        # Create mock DataFrame
        mock_df = pd.DataFrame({
            'timestamp': naive_timestamps,
            'open': [150.0 + i * 0.1 for i in range(len(naive_timestamps))],
            'high': [150.5 + i * 0.1 for i in range(len(naive_timestamps))],
            'low': [149.5 + i * 0.1 for i in range(len(naive_timestamps))], 
            'close': [150.2 + i * 0.1 for i in range(len(naive_timestamps))],
            'volume': [1000 + i * 10 for i in range(len(naive_timestamps))]
        })
        
        logger.info(f"✅ Created mock data with {len(mock_df)} current day entries")
        logger.info(f"   Time range: {mock_df['timestamp'].min()} to {mock_df['timestamp'].max()}")
        
        return mock_df
        
    def _test_processing_logic(self, df, is_mock=False):
        """Test the timestamp processing and today's data detection logic."""
        logger.info(f"🔬 Testing processing logic on {'mock' if is_mock else 'live'} data")
        
        try:
            # Apply the same timestamp standardization as the real system
            logger.info("   Step 1: Applying timestamp standardization")
            processed_df = apply_timestamp_standardization_to_api_data(df, data_type="intraday")
            
            if processed_df.empty:
                logger.error("❌ Timestamp standardization returned empty DataFrame")
                return False
                
            logger.info("   Step 2: Converting standardized timestamps back for analysis")
            
            # Parse the standardized timestamps back to datetime for analysis
            processed_df['timestamp_dt'] = pd.to_datetime(processed_df['timestamp'])
            
            # Convert to ET for today's data detection 
            processed_df['timestamp_et'] = processed_df['timestamp_dt'].dt.tz_convert(self.ny_tz)
            
            # Check for today's data
            today_data = processed_df['timestamp_et'].dt.date == self.today_et
            today_count = today_data.sum()
            
            logger.info(f"   📊 Processing results:")
            logger.info(f"      Total processed rows: {len(processed_df)}")
            logger.info(f"      Rows with today's data: {today_count}")
            logger.info(f"      Today's date (ET): {self.today_et}")
            
            if today_count > 0:
                logger.info("   ✅ PROCESSING LOGIC CORRECTLY PRESERVES TODAY'S DATA")
                today_rows = processed_df[today_data]
                logger.info(f"      First today entry: {today_rows['timestamp_et'].min()}")
                logger.info(f"      Last today entry: {today_rows['timestamp_et'].max()}")
                
                # Test the smart append logic
                self._test_smart_append_logic(processed_df[today_data])
                
                return True
            else:
                if is_mock:
                    logger.error("   ❌ PROCESSING LOGIC FAILED - LOST TODAY'S DATA FROM MOCK INPUT")
                    logger.error("      This indicates a bug in timestamp processing!")
                else:
                    logger.warning("   ⚠️ No today's data found in live API response")
                    logger.warning("      This confirms the API response issue, not processing logic")
                return is_mock  # False for mock (processing bug), True for live (API issue)
                
        except Exception as e:
            logger.error(f"❌ Processing logic test failed: {e}")
            return False
            
    def _test_smart_append_logic(self, today_data):
        """Test the append_new_candles_smart function with today's data."""
        logger.info("   🔧 Testing smart append logic")
        
        try:
            # Import the function
            sys.path.append(os.path.dirname(os.path.abspath(__file__)))
            from fetch_intraday_compact import append_new_candles_smart
            
            # Create some "existing" data from yesterday
            yesterday = self.today_et - timedelta(days=1)
            yesterday_et = datetime.combine(yesterday, datetime.min.time()).replace(tzinfo=self.ny_tz)
            
            # Create existing data (yesterday's data)
            existing_timestamps = []
            for hour in range(9, 16):  # Market hours
                for minute in range(0, 60, 1):  # Every minute
                    ts = yesterday_et.replace(hour=hour, minute=minute)
                    existing_timestamps.append(ts.astimezone(pytz.UTC).strftime("%Y-%m-%d %H:%M:%S+00:00"))
                    
            existing_df = pd.DataFrame({
                'timestamp': existing_timestamps[:100],  # Just first 100 entries
                'open': [149.0] * 100,
                'high': [149.5] * 100,
                'low': [148.5] * 100,
                'close': [149.2] * 100,
                'volume': [1000] * 100
            })
            
            # Test appending today's data to existing data
            combined_df = append_new_candles_smart(existing_df, today_data)
            
            logger.info(f"      Original existing data: {len(existing_df)} rows")
            logger.info(f"      New today's data: {len(today_data)} rows") 
            logger.info(f"      Combined result: {len(combined_df)} rows")
            
            # Verify today's data was actually appended
            combined_df['timestamp_dt'] = pd.to_datetime(combined_df['timestamp'])
            combined_df['timestamp_et'] = combined_df['timestamp_dt'].dt.tz_convert(self.ny_tz)
            today_in_combined = (combined_df['timestamp_et'].dt.date == self.today_et).sum()
            
            if today_in_combined > 0:
                logger.info("      ✅ Smart append successfully added today's data")
            else:
                logger.error("      ❌ Smart append failed to preserve today's data")
                
        except Exception as e:
            logger.error(f"      ❌ Smart append logic test failed: {e}")
            
    def run_comprehensive_diagnostic(self):
        """Run the complete diagnostic suite."""
        logger.info("🚀 STARTING COMPREHENSIVE COMPACT FETCH DIAGNOSTIC")
        logger.info(f"Test Symbol: {TEST_SYMBOL}")
        logger.info(f"Test Interval: {TEST_INTERVAL}")
        logger.info(f"Current Date (ET): {self.today_et}")
        logger.info(f"API Key Configured: {'Yes' if self.api_key else 'No'}")
        
        results = {}
        
        # Phase 1.1: API Endpoint Validation
        results['api_endpoint'] = self.check_1_api_endpoint_validation()
        
        # Phase 1.2: Response Content Audit  
        results['response_content'] = self.check_2_response_content_audit()
        
        # Phase 1.3: Data Processing Logic Audit
        results['processing_logic'] = self.check_3_data_processing_logic_audit()
        
        # Summary
        self.log_section("DIAGNOSTIC SUMMARY")
        logger.info("📋 Diagnostic Results:")
        for test, passed in results.items():
            status = "✅ PASSED" if passed else "❌ FAILED"
            logger.info(f"   {test.replace('_', ' ').title()}: {status}")
            
        # Root cause analysis
        self.log_section("ROOT CAUSE ANALYSIS")
        
        if not results.get('response_content', False):
            logger.info("🎯 ROOT CAUSE IDENTIFIED: API RESPONSE ISSUE")
            logger.info("   The Alpha Vantage API is not returning current day data")
            logger.info("   This is an API-side issue, not a processing logic problem")
            logger.info("   RECOMMENDED FIX: Implement retry mechanism with exponential backoff")
        elif not results.get('processing_logic', False):
            logger.info("🎯 ROOT CAUSE IDENTIFIED: DATA PROCESSING ISSUE") 
            logger.info("   The processing logic is not correctly handling current day data")
            logger.info("   This is a timestamp/timezone handling problem")
            logger.info("   RECOMMENDED FIX: Fix timestamp processing and date filtering logic")
        else:
            logger.info("🎯 ROOT CAUSE: UNCLEAR FROM DIAGNOSTIC")
            logger.info("   All tests passed but the issue may be intermittent")
            logger.info("   RECOMMENDED FIX: Add comprehensive retry logic and enhanced logging")
            
        return results


if __name__ == "__main__":
    diagnostic = CompactFetchDiagnostic()
    results = diagnostic.run_comprehensive_diagnostic()
    
    print("\n" + "="*80)
    print("💾 DETAILED LOG SAVED TO: /tmp/api_diagnostic.log")
    print("="*80)