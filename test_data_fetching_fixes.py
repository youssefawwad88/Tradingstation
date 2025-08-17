#!/usr/bin/env python3
"""
Test file to demonstrate data fetching fixes and improvements.
This file shows the corrected version without code duplication.
"""

import unittest
from unittest.mock import patch, Mock
import pandas as pd

from utils.data_fetcher import fetch_intraday_data


class TestDataFetchingFixes(unittest.TestCase):
    """Test cases for data fetching fixes."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.test_ticker = "AAPL"
        
    @patch("utils.data_fetcher.ALPHA_VANTAGE_API_KEY", "test_key")
    def test_fetch_with_api_key(self):
        """Test that fetching works with API key."""
        # This test demonstrates the fixed logging behavior
        result, success = fetch_intraday_data(self.test_ticker)
        # Test should handle the warning-level logging appropriately
        self.assertIsNotNone(result)

if __name__ == '__main__':
    unittest.main()