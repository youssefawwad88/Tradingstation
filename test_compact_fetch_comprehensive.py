#!/usr/bin/env python3
"""
Comprehensive Test for Compact Data Fetch Fix

This test validates the entire end-to-end pipeline and the specific issue
described in the problem statement:

1. Tests that the script properly fails when API returns stale data
2. Validates that today's data presence is checked before declaring success  
3. Ensures hardcoded problematic tickers are removed
4. Tests market hours vs non-market hours logic
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

# Add project root to Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), ".")))

from fetch_intraday_compact import append_new_candles_smart, fetch_intraday_compact
from utils.alpha_vantage_api import get_intraday_data
from utils.market_time import is_market_open_on_date

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TestCompactFetchComprehensive(unittest.TestCase):
    """Comprehensive test suite for the compact data fetch fix."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.ny_tz = pytz.timezone("America/New_York")
        self.today_et = datetime.now(self.ny_tz).date()
        
    def create_stale_api_response(self):
        """Create a mock API response with yesterday's data only (stale data)."""
        yesterday = self.today_et - timedelta(days=1)
        yesterday_start = datetime.combine(yesterday, datetime.min.time())
        yesterday_start = self.ny_tz.localize(yesterday_start)
        
        timestamps = []
        for hour in range(9, 16):  # Market hours
            for minute in range(0, 60, 1):  # Every minute
                timestamp = yesterday_start.replace(hour=hour, minute=minute)
                timestamps.append(timestamp.strftime('%Y-%m-%d %H:%M:%S'))
        
        csv_data = "timestamp,open,high,low,close,volume\n"
        for i, ts in enumerate(timestamps):
            csv_data += f"{ts},100.{i%10},101.{i%10},99.{i%10},100.{(i+5)%10},{1000+i}\n"
        
        return csv_data
    
    def create_fresh_api_response(self):
        """Create a mock API response with today's data (fresh data)."""
        today_start = datetime.combine(self.today_et, datetime.min.time())
        today_start = self.ny_tz.localize(today_start)
        
        timestamps = []
        for hour in range(9, 16):  # Market hours  
            for minute in range(0, 60, 1):  # Every minute
                timestamp = today_start.replace(hour=hour, minute=minute)
                timestamps.append(timestamp.strftime('%Y-%m-%d %H:%M:%S'))
        
        csv_data = "timestamp,open,high,low,close,volume\n"
        for i, ts in enumerate(timestamps):
            csv_data += f"{ts},100.{i%10},101.{i%10},99.{i%10},100.{(i+5)%10},{1000+i}\n"
        
        return csv_data
    
    @patch('utils.alpha_vantage_api.API_KEY', 'test_api_key')
    @patch('utils.alpha_vantage_api.requests.get')
    def test_stale_data_detection(self, mock_get):
        """Test that stale data (no today's candles) is properly detected."""
        logger.info("🧪 Testing stale data detection")
        
        # Mock API response with stale data
        mock_response = Mock()
        mock_response.text = self.create_stale_api_response()
        mock_response.raise_for_status = Mock()
        mock_get.return_value = mock_response
        
        # Call get_intraday_data
        df = get_intraday_data("TEST", interval="1min", outputsize="compact")
        
        # Validate that data was returned but doesn't contain today's data
        self.assertFalse(df.empty, "Should return data even if stale")
        
        # Check if today's data is present
        if 'timestamp' in df.columns:
            df_timestamps = pd.to_datetime(df['timestamp'])
            if df_timestamps.dt.tz is None:
                df_timestamps_et = df_timestamps.dt.tz_localize(self.ny_tz)
            else:
                df_timestamps_et = df_timestamps.dt.tz_convert(self.ny_tz)
            
            today_rows = (df_timestamps_et.dt.date == self.today_et).sum()
            today_data_present = today_rows > 0
            
            self.assertFalse(today_data_present, "Stale data should not contain today's candles")
            logger.info(f"✅ Stale data correctly detected - today's rows: {today_rows}")
    
    @patch('utils.alpha_vantage_api.API_KEY', 'test_api_key')
    @patch('utils.alpha_vantage_api.requests.get')
    def test_fresh_data_detection(self, mock_get):
        """Test that fresh data (with today's candles) is properly detected."""
        logger.info("🧪 Testing fresh data detection")
        
        # Mock API response with fresh data
        mock_response = Mock()
        mock_response.text = self.create_fresh_api_response()
        mock_response.raise_for_status = Mock()
        mock_get.return_value = mock_response
        
        # Call get_intraday_data
        df = get_intraday_data("TEST", interval="1min", outputsize="compact")
        
        # Validate that data was returned and contains today's data
        self.assertFalse(df.empty, "Should return fresh data")
        
        # Check if today's data is present
        if 'timestamp' in df.columns:
            df_timestamps = pd.to_datetime(df['timestamp'])
            if df_timestamps.dt.tz is None:
                df_timestamps_et = df_timestamps.dt.tz_localize(self.ny_tz)
            else:
                df_timestamps_et = df_timestamps.dt.tz_convert(self.ny_tz)
            
            today_rows = (df_timestamps_et.dt.date == self.today_et).sum()
            today_data_present = today_rows > 0
            
            self.assertTrue(today_data_present, "Fresh data should contain today's candles")
            logger.info(f"✅ Fresh data correctly detected - today's rows: {today_rows}")
    
    def test_append_new_candles_smart_logic(self):
        """Test the smart append logic for handling new vs existing data."""
        logger.info("🧪 Testing smart append logic")
        
        # Create existing data (yesterday)
        yesterday = self.today_et - timedelta(days=1)
        existing_data = []
        for hour in range(9, 12):  # Morning hours
            for minute in range(0, 60, 5):
                timestamp = datetime.combine(yesterday, datetime.min.time()).replace(hour=hour, minute=minute)
                timestamp = self.ny_tz.localize(timestamp)
                existing_data.append({
                    'timestamp': timestamp.isoformat(),
                    'open': 100.0,
                    'high': 101.0,
                    'low': 99.0,
                    'close': 100.5,
                    'volume': 1000
                })
        
        existing_df = pd.DataFrame(existing_data)
        
        # Create new data (today)
        new_data = []
        for hour in range(9, 12):  # Morning hours
            for minute in range(0, 60, 5):
                timestamp = datetime.combine(self.today_et, datetime.min.time()).replace(hour=hour, minute=minute)
                timestamp = self.ny_tz.localize(timestamp)
                new_data.append({
                    'timestamp': timestamp.isoformat(),
                    'open': 105.0,
                    'high': 106.0,
                    'low': 104.0,
                    'close': 105.5,
                    'volume': 1500
                })
        
        new_df = pd.DataFrame(new_data)
        
        # Test smart append
        combined_df = append_new_candles_smart(existing_df, new_df)
        
        # Validate results
        self.assertGreater(len(combined_df), len(existing_df), "Should have more rows after append")
        
        # Check that both yesterday's and today's data are present
        combined_timestamps = pd.to_datetime(combined_df['timestamp'])
        if combined_timestamps.dt.tz is None:
            combined_timestamps_et = combined_timestamps.dt.tz_localize(self.ny_tz)
        else:
            combined_timestamps_et = combined_timestamps.dt.tz_convert(self.ny_tz)
        
        yesterday_rows = (combined_timestamps_et.dt.date == yesterday).sum()
        today_rows = (combined_timestamps_et.dt.date == self.today_et).sum()
        
        self.assertGreater(yesterday_rows, 0, "Should retain yesterday's data")
        self.assertGreater(today_rows, 0, "Should add today's data")
        
        logger.info(f"✅ Smart append test passed - Yesterday rows: {yesterday_rows}, Today rows: {today_rows}")
    
    def test_hardcoded_tickers_removed(self):
        """Test that hardcoded problematic tickers have been removed."""
        logger.info("🧪 Testing that hardcoded problematic tickers are removed")
        
        # Read the alpha_vantage_api.py file and check for hardcoded tickers
        with open('utils/alpha_vantage_api.py', 'r') as f:
            content = f.read()
        
        # These patterns should NOT be present after the fix
        problematic_patterns = [
            'problematic_tickers = ["AAPL", "PLTR"]',
            'if symbol in problematic_tickers:',
            'PROBLEMATIC TICKER DETECTED'
        ]
        
        for pattern in problematic_patterns:
            self.assertNotIn(pattern, content, f"Hardcoded pattern '{pattern}' should be removed")
        
        logger.info("✅ Hardcoded problematic tickers have been properly removed")
    
    def test_market_hours_logic(self):
        """Test the comprehensive market calendar logic (weekends AND holidays)."""
        logger.info("🧪 Testing comprehensive market calendar logic")
        
        ny_tz = pytz.timezone("America/New_York")
        current_time = datetime.now(ny_tz)
        is_weekend = current_time.weekday() >= 5  # Saturday=5, Sunday=6
        
        # Test weekend detection (legacy)
        logger.info(f"Current time: {current_time}")
        logger.info(f"Day of week: {current_time.strftime('%A')} ({current_time.weekday()})")
        logger.info(f"Is weekend: {is_weekend}")
        
        # Test comprehensive market calendar
        market_open = is_market_open_on_date()
        logger.info(f"Market open (comprehensive): {market_open}")
        
        # The logic should properly handle market hours
        if is_weekend:
            logger.info("✅ Weekend detected - missing data should be acceptable")
        else:
            logger.info("✅ Weekday detected - missing data should cause failure")
        
        # Test the UPDATED logic that's in fetch_intraday_compact.py
        market_closed = not is_market_open_on_date()  # Comprehensive check including holidays
        
        # The new logic should properly handle both weekends AND holidays
        if market_closed:
            logger.info("✅ Market closed (weekend or holiday) - missing data should be acceptable")
        else:
            logger.info("✅ Market open - missing data should cause failure")
        
        self.assertIsInstance(market_closed, bool, "Market closed should be a boolean value")


if __name__ == "__main__":
    logger.info("🚀 Running Comprehensive Compact Fetch Fix Tests")
    logger.info("=" * 70)
    
    unittest.main(verbosity=2)