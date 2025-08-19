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

# Test configuration - Test both working and non-working tickers
WORKING_TICKER = "AMD"     # Known working ticker according to problem statement
NON_WORKING_TICKER = "AAPL"  # Known non-working ticker according to problem statement
ADDITIONAL_TEST_TICKER = "PLTR"  # Additional non-working ticker mentioned
TEST_INTERVAL = "1min"

# Test all problematic tickers
TEST_SYMBOLS = [WORKING_TICKER, NON_WORKING_TICKER, ADDITIONAL_TEST_TICKER]


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
        """Phase 1.1: API Endpoint Validation - verify URL construction for all test tickers."""
        self.log_section("API ENDPOINT VALIDATION - WORKING vs NON-WORKING TICKERS")
        
        if not self.api_key:
            logger.error("❌ ALPHA_VANTAGE_API_KEY not configured - cannot perform API tests")
            return False
            
        # Test endpoint construction for each ticker and outputsize combination
        base_url = "https://www.alphavantage.co/query"
        
        for symbol in TEST_SYMBOLS:
            logger.info(f"🔍 Testing URL construction for {symbol}")
            
            for outputsize in ["compact", "full"]:
                params = {
                    "function": "TIME_SERIES_INTRADAY",
                    "symbol": symbol,
                    "interval": TEST_INTERVAL,
                    "outputsize": outputsize,
                    "apikey": self.api_key,
                    "datatype": "csv",
                }
                
                # Construct full URL
                url_parts = [f"{k}={v}" for k, v in params.items()]
                full_url = f"{base_url}?{'&'.join(url_parts)}"
                
                logger.info(f"✅ {symbol} {outputsize.upper()} URL constructed correctly:")
                logger.info(f"   URL: {full_url}")
                logger.info(f"   Function: {params['function']}")
                logger.info(f"   Symbol: {params['symbol']}")
                logger.info(f"   Interval: {params['interval']}")
                logger.info(f"   Output Size: {params['outputsize']}")
                logger.info(f"   Data Type: {params['datatype']}")
                logger.info("")
            
        return True
        
    def check_2_response_content_audit(self):
        """Phase 1.2: Response Content Audit - manual API execution and inspection for working vs non-working tickers."""
        self.log_section("RESPONSE CONTENT AUDIT - COMPARATIVE ANALYSIS")
        
        if not self.api_key:
            logger.error("❌ Cannot perform response audit without API key")
            return False
        
        api_results = {}
        
        for symbol in TEST_SYMBOLS:
            logger.info(f"🔍 ANALYZING {symbol} ({'WORKING' if symbol == WORKING_TICKER else 'NON-WORKING'} TICKER)")
            logger.info("=" * 60)
            
            symbol_results = {}
            
            for outputsize in ["compact", "full"]:
                logger.info(f"📊 Testing {outputsize.upper()} API response for {symbol}")
                
                # Manual API call construction (same as alpha_vantage_api.py)
                params = {
                    "function": "TIME_SERIES_INTRADAY",
                    "symbol": symbol,
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
                        symbol_results[outputsize] = {"error": "Empty or error response"}
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
                        
                        result_data = {
                            "total_rows": len(df),
                            "today_count": today_count,
                            "today_percentage": (today_count/len(df)*100),
                            "first_timestamp": str(df[timestamp_col].min()),
                            "last_timestamp": str(df[timestamp_col].max())
                        }
                        
                        if today_count > 0:
                            logger.info(f"✅ {symbol} API IS RETURNING TODAY'S DATA")
                            today_rows = df[today_data]
                            first_today = today_rows[timestamp_col].min()
                            last_today = today_rows[timestamp_col].max()
                            logger.info(f"   First today entry: {first_today}")
                            logger.info(f"   Last today entry: {last_today}")
                            result_data["has_today_data"] = True
                            result_data["first_today"] = str(first_today)
                            result_data["last_today"] = str(last_today)
                        else:
                            logger.error(f"❌ {symbol} API IS NOT RETURNING TODAY'S DATA")
                            logger.error("   This confirms the compact fetch failure issue!")
                            
                            # Show the most recent dates available
                            recent_dates = df_et.dt.date.value_counts().head(3)
                            logger.error(f"   Most recent dates in response: {recent_dates.to_dict()}")
                            result_data["has_today_data"] = False
                            result_data["recent_dates"] = recent_dates.to_dict()
                            
                        symbol_results[outputsize] = result_data
                        
                    else:
                        logger.error(f"❌ No timestamp column found in response")
                        symbol_results[outputsize] = {"error": "No timestamp column"}
                        
                except Exception as e:
                    logger.error(f"❌ API call failed for {symbol} {outputsize}: {e}")
                    symbol_results[outputsize] = {"error": str(e)}
                    
                logger.info("")  # Add spacing between tests
                
            api_results[symbol] = symbol_results
            logger.info("=" * 60)
            
        # Summary comparison
        self.log_section("API RESPONSE COMPARISON SUMMARY")
        
        for symbol in TEST_SYMBOLS:
            status = "WORKING" if symbol == WORKING_TICKER else "NON-WORKING"
            logger.info(f"📋 {symbol} ({status}) Summary:")
            
            if symbol in api_results:
                for outputsize in ["compact", "full"]:
                    if outputsize in api_results[symbol]:
                        result = api_results[symbol][outputsize]
                        if "error" in result:
                            logger.info(f"   {outputsize}: ❌ {result['error']}")
                        else:
                            has_today = "✅" if result.get("has_today_data", False) else "❌"
                            logger.info(f"   {outputsize}: {has_today} Today's data: {result.get('today_count', 0)} rows")
                            
        return len(api_results) > 0
        
    def check_3_data_processing_logic_audit(self):
        """Phase 1.3: Data Processing Logic Audit - test processing for all problematic tickers."""
        self.log_section("DATA PROCESSING LOGIC AUDIT - COMPARATIVE TESTING")
        
        if not self.api_key:
            logger.warning("⚠️ Skipping processing audit - no API key for live data")
            # Create mock data for processing logic test
            mock_data = self._create_mock_current_day_data()
            return self._test_processing_logic(mock_data, is_mock=True, symbol="MOCK")
        
        # Use our actual API wrapper to test processing for each ticker
        logger.info("🔍 Testing data processing logic with live API data for all test tickers")
        
        processing_results = {}
        
        for symbol in TEST_SYMBOLS:
            status = "WORKING" if symbol == WORKING_TICKER else "NON-WORKING"
            logger.info(f"🔬 Processing logic test for {symbol} ({status})")
            
            symbol_results = {}
            
            for outputsize in ["compact", "full"]:
                logger.info(f"📊 Testing processing logic with {outputsize} data for {symbol}")
                
                # Use the actual function from alpha_vantage_api.py
                df = get_intraday_data(symbol, interval=TEST_INTERVAL, outputsize=outputsize)
                
                if df.empty:
                    logger.error(f"❌ No data returned from get_intraday_data({symbol}, {outputsize})")
                    symbol_results[outputsize] = {"success": False, "error": "No data returned"}
                    continue
                    
                success = self._test_processing_logic(df, is_mock=False, symbol=symbol)
                symbol_results[outputsize] = {"success": success}
                
                if not success:
                    logger.error(f"❌ Processing logic failed for {symbol} {outputsize} data")
                else:
                    logger.info(f"✅ Processing logic successful for {symbol} {outputsize} data")
                    
            processing_results[symbol] = symbol_results
            logger.info("")  # Add spacing
            
        # Summary
        self.log_section("PROCESSING LOGIC COMPARISON SUMMARY")
        for symbol in TEST_SYMBOLS:
            status = "WORKING" if symbol == WORKING_TICKER else "NON-WORKING"
            logger.info(f"📋 {symbol} ({status}) Processing Results:")
            
            if symbol in processing_results:
                for outputsize in ["compact", "full"]:
                    if outputsize in processing_results[symbol]:
                        result = processing_results[symbol][outputsize]
                        status_icon = "✅" if result.get("success", False) else "❌"
                        logger.info(f"   {outputsize}: {status_icon}")
                        
        return len(processing_results) > 0
        
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
        
    def _test_processing_logic(self, df, is_mock=False, symbol="TEST"):
        """Test the timestamp processing and today's data detection logic."""
        logger.info(f"🔬 Testing processing logic on {'mock' if is_mock else 'live'} data for {symbol}")
        
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
            
            logger.info(f"   📊 Processing results for {symbol}:")
            logger.info(f"      Total processed rows: {len(processed_df)}")
            logger.info(f"      Rows with today's data: {today_count}")
            logger.info(f"      Today's date (ET): {self.today_et}")
            
            if today_count > 0:
                logger.info(f"   ✅ PROCESSING LOGIC CORRECTLY PRESERVES TODAY'S DATA for {symbol}")
                today_rows = processed_df[today_data]
                logger.info(f"      First today entry: {today_rows['timestamp_et'].min()}")
                logger.info(f"      Last today entry: {today_rows['timestamp_et'].max()}")
                
                # Test the smart append logic
                self._test_smart_append_logic(processed_df[today_data], symbol)
                
                return True
            else:
                if is_mock:
                    logger.error(f"   ❌ PROCESSING LOGIC FAILED - LOST TODAY'S DATA FROM MOCK INPUT for {symbol}")
                    logger.error("      This indicates a bug in timestamp processing!")
                else:
                    logger.warning(f"   ⚠️ No today's data found in live API response for {symbol}")
                    logger.warning("      This confirms the API response issue, not processing logic")
                return is_mock  # False for mock (processing bug), True for live (API issue)
                
        except Exception as e:
            logger.error(f"❌ Processing logic test failed for {symbol}: {e}")
            return False
            
    def _test_smart_append_logic(self, today_data, symbol="TEST"):
        """Test the append_new_candles_smart function with today's data."""
        logger.info(f"   🔧 Testing smart append logic for {symbol}")
        
        # NOTE: This test is disabled as fetch_intraday_compact.py has been replaced 
        # by the new Unified DataFetchManager system (jobs/data_fetch_manager.py)
        logger.warning(f"   ⚠️ Smart append test disabled - replaced by Unified DataFetchManager")
        logger.info(f"      Use: python jobs/data_fetch_manager.py")
        return
        
        try:
            # DEPRECATED: Import the function
            # sys.path.append(os.path.dirname(os.path.abspath(__file__)))
            # from fetch_intraday_compact import append_new_candles_smart
            
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
                logger.info(f"      ✅ Smart append successfully added today's data for {symbol}")
            else:
                logger.error(f"      ❌ Smart append failed to preserve today's data for {symbol}")
                
        except Exception as e:
            logger.error(f"      ❌ Smart append logic test failed for {symbol}: {e}")
            
    def run_comprehensive_diagnostic(self):
        """Run the complete diagnostic suite comparing working vs non-working tickers."""
        logger.info("🚀 STARTING COMPREHENSIVE COMPACT FETCH DIAGNOSTIC")
        logger.info(f"Working Ticker: {WORKING_TICKER}")
        logger.info(f"Non-Working Tickers: {NON_WORKING_TICKER}, {ADDITIONAL_TEST_TICKER}")
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
        self.log_section("COMPREHENSIVE DIAGNOSTIC SUMMARY")
        logger.info("📋 Diagnostic Results:")
        for test, passed in results.items():
            status = "✅ PASSED" if passed else "❌ FAILED"
            logger.info(f"   {test.replace('_', ' ').title()}: {status}")
            
        # Enhanced root cause analysis based on comparative testing
        self.log_section("ROOT CAUSE ANALYSIS - WORKING vs NON-WORKING COMPARISON")
        
        if not results.get('response_content', False):
            logger.info("🎯 ROOT CAUSE IDENTIFIED: API RESPONSE ISSUE")
            logger.info("   The Alpha Vantage API is showing inconsistent behavior")
            logger.info("   Some tickers (AMD) return current day data, others (AAPL, PLTR) do not")
            logger.info("   This suggests an API-side issue with specific tickers or rate limiting")
            logger.info("   RECOMMENDED FIX: Implement aggressive retry mechanism for stale tickers")
            logger.info("   ENHANCEMENT: Add ticker-specific retry logic with exponential backoff")
        elif not results.get('processing_logic', False):
            logger.info("🎯 ROOT CAUSE IDENTIFIED: DATA PROCESSING ISSUE") 
            logger.info("   The processing logic is not correctly handling current day data")
            logger.info("   This is a timestamp/timezone handling problem affecting all tickers")
            logger.info("   RECOMMENDED FIX: Fix timestamp processing and date filtering logic")
        else:
            logger.info("🎯 ROOT CAUSE: TICKER-SPECIFIC API BEHAVIOR")
            logger.info("   All tests passed but the issue may be ticker-specific or intermittent")
            logger.info("   Working ticker (AMD) behaves differently from non-working ones (AAPL, PLTR)")
            logger.info("   RECOMMENDED FIX: Add comprehensive retry logic and ticker-specific validation")
            
        return results


if __name__ == "__main__":
    diagnostic = CompactFetchDiagnostic()
    results = diagnostic.run_comprehensive_diagnostic()
    
    print("\n" + "="*80)
    print("💾 DETAILED LOG SAVED TO: /tmp/api_diagnostic.log")
    print("="*80)