#!/usr/bin/env python3
"""
Comprehensive Test for Compact Data Fetching Fix

This test validates that the enhanced retry mechanism and current day data
validation properly addresses the compact fetch failure issue.

Tests both the fix implementation and provides examples of expected behavior.
"""

import logging
import os
import sys
import unittest
from datetime import datetime, timedelta
from io import StringIO
from unittest.mock import Mock, patch

import pandas as pd
import pytz
import requests

# Add project root to Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), ".")))

from utils.alpha_vantage_api import (
    _make_api_request_with_retry,
    _validate_current_day_data,
    get_intraday_data,
)

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TestCompactFetchFix(unittest.TestCase):
    """Test suite for the compact data fetching fix."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.ny_tz = pytz.timezone("America/New_York")
        self.today_et = datetime.now(self.ny_tz).date()
        self.test_symbol = "AAPL"
        
    def create_mock_csv_response(self, include_today=True, include_premarket=True):
        """Create a mock CSV response for testing."""
        timestamps = []
        
        if include_today:
            # Create today's data
            today_start = datetime.combine(self.today_et, datetime.min.time())
            today_start = today_start.replace(tzinfo=self.ny_tz)
            
            if include_premarket:
                # Pre-market data (7:00 AM - 9:29 AM ET)
                current = today_start.replace(hour=7)
                while current.hour < 9 or (current.hour == 9 and current.minute < 30):
                    timestamps.append(current.strftime("%Y-%m-%d %H:%M:%S"))
                    current += timedelta(minutes=1)
                    if len(timestamps) >= 50:  # Limit for testing
                        break
                        
            # Market hours data (9:30 AM onwards)
            if len(timestamps) < 100:
                current = today_start.replace(hour=9, minute=30)
                while len(timestamps) < 100:
                    timestamps.append(current.strftime("%Y-%m-%d %H:%M:%S"))
                    current += timedelta(minutes=1)
        else:
            # Create yesterday's data only (simulating stale API response)
            yesterday = self.today_et - timedelta(days=1)
            yesterday_start = datetime.combine(yesterday, datetime.min.time())
            yesterday_start = yesterday_start.replace(tzinfo=self.ny_tz)
            
            current = yesterday_start.replace(hour=9, minute=30)
            for i in range(100):
                timestamps.append(current.strftime("%Y-%m-%d %H:%M:%S"))
                current += timedelta(minutes=1)
                
        # Create CSV data
        csv_data = ["timestamp,open,high,low,close,volume"]
        for i, ts in enumerate(timestamps):
            csv_data.append(f"{ts},{150.0 + i*0.01},{150.5 + i*0.01},{149.5 + i*0.01},{150.2 + i*0.01},{1000 + i*10}")
            
        return "\n".join(csv_data)
        
    def test_validate_current_day_data_with_today(self):
        """Test current day validation with today's data present."""
        logger.info("🧪 Testing current day validation with today's data")
        
        # Create mock response with today's data
        csv_content = self.create_mock_csv_response(include_today=True)
        mock_response = Mock()
        mock_response.text = csv_content
        
        # Test validation
        is_valid = _validate_current_day_data(mock_response, self.today_et, self.test_symbol)
        
        self.assertTrue(is_valid, "Validation should pass when today's data is present")
        logger.info("✅ Validation correctly identified today's data")
        
    def test_validate_current_day_data_without_today(self):
        """Test current day validation with stale data (no today's data)."""
        logger.info("🧪 Testing current day validation with stale data")
        
        # Create mock response without today's data
        csv_content = self.create_mock_csv_response(include_today=False)
        mock_response = Mock()
        mock_response.text = csv_content
        
        # Test validation
        is_valid = _validate_current_day_data(mock_response, self.today_et, self.test_symbol)
        
        self.assertFalse(is_valid, "Validation should fail when today's data is missing")
        logger.info("✅ Validation correctly identified stale data")
        
    @patch('utils.alpha_vantage_api.API_KEY', 'test_api_key')
    @patch('requests.get')
    def test_retry_mechanism_success_on_second_attempt(self, mock_get):
        """Test retry mechanism succeeds on second attempt."""
        logger.info("🧪 Testing retry mechanism - success on second attempt")
        
        # First response: stale data (no today's data)
        stale_response = Mock()
        stale_response.text = self.create_mock_csv_response(include_today=False)
        stale_response.raise_for_status = Mock()
        
        # Second response: fresh data (with today's data) 
        fresh_response = Mock()
        fresh_response.text = self.create_mock_csv_response(include_today=True)
        fresh_response.raise_for_status = Mock()
        
        # Mock requests.get to return stale first, then fresh
        mock_get.side_effect = [stale_response, fresh_response]
        
        params = {
            'function': 'TIME_SERIES_INTRADAY',
            'symbol': self.test_symbol,
            'interval': '1min',
            'outputsize': 'compact',
            'apikey': 'test_api_key',
            'datatype': 'csv'
        }
        
        # Test retry mechanism
        with patch('time.sleep'):  # Speed up test by mocking sleep
            response = _make_api_request_with_retry(params, max_retries=2, base_delay=0.1)
            
        self.assertIsNotNone(response, "Retry mechanism should return a response")
        self.assertEqual(response, fresh_response, "Should return the fresh response")
        self.assertEqual(mock_get.call_count, 2, "Should have made exactly 2 API calls")
        logger.info("✅ Retry mechanism worked correctly")
        
    @patch('utils.alpha_vantage_api.API_KEY', 'test_api_key')
    @patch('requests.get')
    def test_retry_mechanism_all_attempts_fail(self, mock_get):
        """Test retry mechanism when all attempts return stale data."""
        logger.info("🧪 Testing retry mechanism - all attempts fail")
        
        # All responses return stale data
        stale_response = Mock()
        stale_response.text = self.create_mock_csv_response(include_today=False)
        stale_response.raise_for_status = Mock()
        
        mock_get.return_value = stale_response
        
        params = {
            'function': 'TIME_SERIES_INTRADAY',
            'symbol': self.test_symbol,
            'interval': '1min',
            'outputsize': 'compact',
            'apikey': 'test_api_key',
            'datatype': 'csv'
        }
        
        # Test retry mechanism
        with patch('time.sleep'):  # Speed up test
            response = _make_api_request_with_retry(params, max_retries=2, base_delay=0.1)
            
        # Should still return response (let processing logic handle stale data)
        self.assertIsNotNone(response, "Should return response even if stale")
        
        # PHASE 2 ENHANCEMENT: AAPL is a problematic ticker, so it should get more aggressive retries
        # Expected attempts: 8 retries + 1 initial = 9 total for AAPL
        expected_attempts = 9  # Updated for problematic ticker aggressive retry
        self.assertEqual(mock_get.call_count, expected_attempts, 
                        f"Should have made {expected_attempts} attempts for problematic ticker AAPL (aggressive retry)")
        logger.info(f"✅ Retry mechanism attempted all {expected_attempts} retries correctly for problematic ticker")
        
    @patch('utils.alpha_vantage_api.API_KEY', 'test_api_key')
    @patch('requests.get')
    def test_retry_mechanism_regular_ticker(self, mock_get):
        """Test retry mechanism for non-problematic ticker."""
        logger.info("🧪 Testing retry mechanism - regular ticker")
        
        # All responses return stale data
        stale_response = Mock()
        stale_response.text = self.create_mock_csv_response(include_today=False)
        stale_response.raise_for_status = Mock()
        
        mock_get.return_value = stale_response
        
        # Use a regular ticker (not AAPL/PLTR)
        regular_ticker = "AMD"
        params = {
            'function': 'TIME_SERIES_INTRADAY',
            'symbol': regular_ticker,
            'interval': '1min',
            'outputsize': 'compact',
            'apikey': 'test_api_key',
            'datatype': 'csv'
        }
        
        # Test retry mechanism with regular ticker
        with patch('time.sleep'):  # Speed up test
            response = _make_api_request_with_retry(params, max_retries=2, base_delay=0.1)
            
        # Should still return response (let processing logic handle stale data)
        self.assertIsNotNone(response, "Should return response even if stale")
        
        # For regular tickers: enhanced retry but not as aggressive as problematic ones
        # Expected attempts: 6 retries + 1 initial = 7 total for regular compact fetches
        expected_attempts = 7  # Enhanced but not as aggressive as problematic tickers
        self.assertEqual(mock_get.call_count, expected_attempts, 
                        f"Should have made {expected_attempts} attempts for regular ticker (enhanced retry)")
        logger.info(f"✅ Retry mechanism attempted {expected_attempts} retries correctly for regular ticker")
        
    @patch('utils.alpha_vantage_api.API_KEY', 'test_api_key')
    @patch('utils.alpha_vantage_api._make_api_request_with_retry')
    def test_get_intraday_data_integration(self, mock_api_request):
        """Test the complete get_intraday_data function with fix."""
        logger.info("🧪 Testing complete get_intraday_data integration")
        
        # Mock successful API response with today's data
        mock_response = Mock()
        mock_response.text = self.create_mock_csv_response(include_today=True, include_premarket=True)
        mock_api_request.return_value = mock_response
        
        # Test the function
        df = get_intraday_data(self.test_symbol, interval="1min", outputsize="compact")
        
        self.assertFalse(df.empty, "Should return non-empty DataFrame")
        self.assertIn('timestamp', df.columns, "Should have timestamp column")
        
        # Verify today's data is present in the result
        timestamps = pd.to_datetime(df['timestamp'])
        timestamps_et = timestamps.dt.tz_convert(self.ny_tz)
        today_count = (timestamps_et.dt.date == self.today_et).sum()
        
        self.assertGreater(today_count, 0, "Should contain today's data")
        logger.info(f"✅ Integration test passed - {today_count} today's data points found")
        
    def test_premarket_data_handling(self):
        """Test that pre-market data is properly handled."""
        logger.info("🧪 Testing pre-market data handling")
        
        # Create data with pre-market hours (7:00 AM - 9:29 AM ET)
        csv_content = self.create_mock_csv_response(include_today=True, include_premarket=True)
        mock_response = Mock()
        mock_response.text = csv_content
        
        # Validate pre-market data is recognized as today's data
        is_valid = _validate_current_day_data(mock_response, self.today_et, self.test_symbol)
        
        self.assertTrue(is_valid, "Pre-market data should be recognized as today's data")
        
        # Parse the data and verify pre-market timestamps
        df = pd.read_csv(StringIO(csv_content))
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df_et = df['timestamp'].dt.tz_localize(self.ny_tz)
        
        # Check for pre-market data (before 9:30 AM)
        premarket_data = df_et[(df_et.dt.hour < 9) | ((df_et.dt.hour == 9) & (df_et.dt.minute < 30))]
        
        self.assertGreater(len(premarket_data), 0, "Should contain pre-market data")
        logger.info(f"✅ Pre-market data handling verified - {len(premarket_data)} pre-market data points")


def run_comprehensive_test():
    """Run all tests and provide a comprehensive report."""
    logger.info("🚀 STARTING COMPREHENSIVE TEST SUITE FOR COMPACT FETCH FIX")
    logger.info("=" * 80)
    
    # Create test suite
    suite = unittest.TestLoader().loadTestsFromTestCase(TestCompactFetchFix)
    
    # Run tests with detailed output
    runner = unittest.TextTestRunner(verbosity=2, stream=sys.stdout)
    result = runner.run(suite)
    
    # Provide summary
    logger.info("=" * 80)
    logger.info("📋 TEST SUMMARY")
    logger.info("=" * 80)
    logger.info(f"Tests run: {result.testsRun}")
    logger.info(f"Failures: {len(result.failures)}")
    logger.info(f"Errors: {len(result.errors)}")
    
    if result.wasSuccessful():
        logger.info("✅ ALL TESTS PASSED - Compact fetch fix is working correctly")
        logger.info("🎯 The retry mechanism and current day validation are functioning as expected")
        logger.info("📈 Pre-market data handling is properly implemented") 
        logger.info("🔄 API retry logic with exponential backoff is operational")
        return True
    else:
        logger.error("❌ SOME TESTS FAILED - Fix needs attention")
        for failure in result.failures:
            logger.error(f"FAILURE: {failure[0]} - {failure[1]}")
        for error in result.errors:
            logger.error(f"ERROR: {error[0]} - {error[1]}")
        return False


if __name__ == "__main__":
    success = run_comprehensive_test()
    
    print("\n" + "="*80)
    print("💾 FINAL VALIDATION COMPLETE")
    print(f"🎯 Result: {'SUCCESS' if success else 'FAILED'}")
    print("="*80)
    
    # Exit with appropriate code
    sys.exit(0 if success else 1)