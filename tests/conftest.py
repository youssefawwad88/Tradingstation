"""
Pytest configuration and shared fixtures for all tests.
"""

import os
import tempfile
from typing import Generator, Dict, Any
from unittest.mock import Mock

import pandas as pd
import pytest


@pytest.fixture
def sample_ticker_data() -> pd.DataFrame:
    """Sample ticker data for testing."""
    return pd.DataFrame(
        {
            "datetime": pd.date_range("2025-01-01", periods=10, freq="1min"),
            "open": [
                100.0,
                101.0,
                102.0,
                103.0,
                104.0,
                105.0,
                106.0,
                107.0,
                108.0,
                109.0,
            ],
            "high": [
                101.0,
                102.0,
                103.0,
                104.0,
                105.0,
                106.0,
                107.0,
                108.0,
                109.0,
                110.0,
            ],
            "low": [
                99.0,
                100.0,
                101.0,
                102.0,
                103.0,
                104.0,
                105.0,
                106.0,
                107.0,
                108.0,
            ],
            "close": [
                101.0,
                102.0,
                103.0,
                104.0,
                105.0,
                106.0,
                107.0,
                108.0,
                109.0,
                110.0,
            ],
            "volume": [1000, 1100, 1200, 1300, 1400, 1500, 1600, 1700, 1800, 1900],
            "ticker": ["AAPL"] * 10,
        }
    )


@pytest.fixture
def mock_config() -> Dict[str, Any]:
    """Mock configuration for testing."""
    return {
        "ALPHA_VANTAGE_API_KEY": "test_api_key",
        "SPACES_ACCESS_KEY_ID": "test_access_key",
        "SPACES_SECRET_ACCESS_KEY": "test_secret_key",
        "SPACES_BUCKET_NAME": "test_bucket",
        "DEBUG_MODE": True,
        "TEST_MODE": "enabled",
    }


@pytest.fixture
def temp_data_dir() -> Generator[str, None, None]:
    """Create a temporary directory for test data."""
    with tempfile.TemporaryDirectory() as temp_dir:
        yield temp_dir


@pytest.fixture
def mock_alpha_vantage_response() -> Dict[str, Any]:
    """Mock Alpha Vantage API response."""
    return {
        "Time Series (1min)": {
            "2025-01-01 09:30:00": {
                "1. open": "100.0",
                "2. high": "101.0",
                "3. low": "99.0",
                "4. close": "101.0",
                "5. volume": "1000",
            },
            "2025-01-01 09:31:00": {
                "1. open": "101.0",
                "2. high": "102.0",
                "3. low": "100.0",
                "4. close": "102.0",
                "5. volume": "1100",
            },
        }
    }


@pytest.fixture
def mock_spaces_client() -> Mock:
    """Mock DigitalOcean Spaces client."""
    client = Mock()
    client.upload_fileobj.return_value = None
    client.download_fileobj.return_value = None
    client.delete_object.return_value = None
    return client


@pytest.fixture(autouse=True)
def setup_test_environment(
    monkeypatch: pytest.MonkeyPatch, mock_config: Dict[str, Any]
) -> None:
    """Set up test environment variables."""
    for key, value in mock_config.items():
        monkeypatch.setenv(key, str(value))


@pytest.fixture
def sample_tickers() -> list[str]:
    """Sample ticker list for testing."""
    return ["AAPL", "GOOGL", "MSFT", "TSLA", "NVDA"]
