"""
Unit tests for data_fetcher module.
"""

from unittest.mock import Mock, patch
import pandas as pd
import requests

from utils.data_fetcher import (
    fetch_intraday_data,
    fetch_daily_data,
    validate_api_response,
    get_api_rate_limit_delay,
)


class TestDataFetcher:
    """Test cases for data fetcher functions."""

    def test_get_api_rate_limit_delay(self):
        """Test API rate limit delay calculation."""
        delay = get_api_rate_limit_delay()
        assert delay == 12.0

    def test_validate_api_response_success(self):
        """Test successful API response validation."""
        valid_response = {
            "Time Series (1min)": {
                "2025-01-01 09:30:00": {
                    "1. open": "100.0",
                    "2. high": "101.0",
                    "3. low": "99.0",
                    "4. close": "100.5",
                    "5. volume": "1000",
                }
            }
        }
        assert validate_api_response(valid_response, "AAPL") is True

    def test_validate_api_response_error(self):
        """Test API response validation with error."""
        error_response = {"Error Message": "Invalid API call"}
        assert validate_api_response(error_response, "AAPL") is False

    def test_validate_api_response_rate_limit(self):
        """Test API response validation with rate limit."""
        rate_limit_response = {
            "Note": "Thank you for using Alpha Vantage! Our standard API call frequency is 5 calls per minute"
        }
        assert validate_api_response(rate_limit_response, "AAPL") is False

    @patch("utils.data_fetcher.ALPHA_VANTAGE_API_KEY", None)
    def test_fetch_intraday_data_no_api_key(self):
        """Test fetch_intraday_data without API key."""
        df, success = fetch_intraday_data("AAPL")
        assert df is None
        assert success is False

    @patch("utils.data_fetcher.ALPHA_VANTAGE_API_KEY", "test_key")
    @patch("utils.data_fetcher.requests.get")
    def test_fetch_intraday_data_success(self, mock_get, mock_alpha_vantage_response):
        """Test successful intraday data fetch."""
        # Mock the API response
        mock_response = Mock()
        mock_response.json.return_value = mock_alpha_vantage_response
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        df, success = fetch_intraday_data("AAPL")

        assert success is True
        assert df is not None
        assert isinstance(df, pd.DataFrame)
        assert "ticker" in df.columns
        assert "datetime" in df.columns
        assert df["ticker"].iloc[0] == "AAPL"

    @patch("utils.data_fetcher.ALPHA_VANTAGE_API_KEY", "test_key")
    @patch("utils.data_fetcher.requests.get")
    def test_fetch_intraday_data_api_error(self, mock_get):
        """Test intraday data fetch with API error."""
        # Mock API error response
        mock_response = Mock()
        mock_response.json.return_value = {"Error Message": "Invalid API call"}
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        df, success = fetch_intraday_data("INVALID")

        assert success is False
        assert df is None

    @patch("utils.data_fetcher.ALPHA_VANTAGE_API_KEY", "test_key")
    @patch("utils.data_fetcher.requests.get")
    def test_fetch_intraday_data_network_error(self, mock_get):
        """Test intraday data fetch with network error."""
        mock_get.side_effect = requests.RequestException("Network error")

        df, success = fetch_intraday_data("AAPL")

        assert success is False
        assert df is None

    @patch("utils.data_fetcher.ALPHA_VANTAGE_API_KEY", "test_key")
    @patch("utils.data_fetcher.requests.get")
    def test_fetch_daily_data_success(self, mock_get):
        """Test successful daily data fetch."""
        # Mock the API response
        mock_response = Mock()
        mock_response.json.return_value = {
            "Time Series (Daily)": {
                "2025-01-01": {
                    "1. open": "100.0",
                    "2. high": "101.0",
                    "3. low": "99.0",
                    "4. close": "100.5",
                    "5. volume": "1000000",
                }
            }
        }
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        df, success = fetch_daily_data("AAPL")

        assert success is True
        assert df is not None
        assert isinstance(df, pd.DataFrame)
        assert "ticker" in df.columns
        assert "Date" in df.columns
        assert df["ticker"].iloc[0] == "AAPL"
