"""
Unit tests for async_client module.
"""

import asyncio
from unittest.mock import AsyncMock, Mock, patch

import pytest

from utils.async_client import (
    AsyncAlphaVantageClient,
    RateLimiter,
    async_fetch_multiple_tickers,
)


class TestRateLimiter:
    """Test cases for rate limiter."""

    @pytest.mark.asyncio
    async def test_rate_limiter_basic(self):
        """Test basic rate limiting functionality."""
        limiter = RateLimiter(calls_per_minute=60)  # 1 call per second

        # First call should be immediate
        start_time = asyncio.get_event_loop().time()
        await limiter.acquire()
        first_call_time = asyncio.get_event_loop().time()

        # Second call should be delayed
        await limiter.acquire()
        second_call_time = asyncio.get_event_loop().time()

        # Should be at least 1 second apart
        assert second_call_time - first_call_time >= 0.9

    def test_rate_limiter_backoff(self):
        """Test backoff mechanism."""
        limiter = RateLimiter()

        initial_backoff = limiter.backoff_factor

        # Rate limit hit should increase backoff
        limiter.on_rate_limit()
        assert limiter.backoff_factor > initial_backoff

        # Success should decrease backoff
        limiter.on_success()
        assert limiter.backoff_factor < initial_backoff * 2.0


class TestAsyncAlphaVantageClient:
    """Test cases for async Alpha Vantage client."""

    @pytest.mark.asyncio
    async def test_client_context_manager(self):
        """Test client as async context manager."""
        async with AsyncAlphaVantageClient(api_key="test_key") as client:
            assert client.session is not None

        # Session should be closed after context exit
        assert client.session is None

    @pytest.mark.asyncio
    @patch("utils.async_client.aiohttp.ClientSession")
    async def test_make_request_success(self, mock_session_class):
        """Test successful API request."""
        # Mock response
        mock_response = AsyncMock()
        mock_response.json.return_value = {"test": "data"}
        mock_response.raise_for_status.return_value = None

        # Mock session
        mock_session = AsyncMock()
        mock_session.get.return_value.__aenter__.return_value = mock_response
        mock_session_class.return_value = mock_session

        client = AsyncAlphaVantageClient(api_key="test_key")
        await client.start()

        result = await client._make_request({"test": "params"})

        assert result == {"test": "data"}
        await client.close()

    @pytest.mark.asyncio
    @patch("utils.async_client.aiohttp.ClientSession")
    async def test_make_request_rate_limit(self, mock_session_class):
        """Test API request with rate limit response."""
        # Mock rate limit response
        mock_response = AsyncMock()
        mock_response.json.return_value = {"Note": "Rate limit exceeded"}
        mock_response.raise_for_status.return_value = None

        # Mock session
        mock_session = AsyncMock()
        mock_session.get.return_value.__aenter__.return_value = mock_response
        mock_session_class.return_value = mock_session

        client = AsyncAlphaVantageClient(api_key="test_key")
        await client.start()

        result = await client._make_request({"test": "params"})

        # Should return None for rate limit
        assert result is None
        await client.close()

    @pytest.mark.asyncio
    async def test_fetch_intraday_data_no_api_key(self):
        """Test intraday fetch without API key."""
        with pytest.raises(ValueError, match="Alpha Vantage API key is required"):
            AsyncAlphaVantageClient(api_key=None)

    @pytest.mark.asyncio
    @patch("utils.async_client.AsyncAlphaVantageClient._make_request")
    async def test_fetch_intraday_data_success(self, mock_make_request):
        """Test successful intraday data fetch."""
        # Mock API response
        mock_make_request.return_value = {
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

        client = AsyncAlphaVantageClient(api_key="test_key")
        df, success = await client.fetch_intraday_data("AAPL")

        assert success is True
        assert df is not None
        assert len(df) == 1
        assert df["ticker"].iloc[0] == "AAPL"

    @pytest.mark.asyncio
    @patch("utils.async_client.AsyncAlphaVantageClient._make_request")
    async def test_fetch_multiple_tickers(self, mock_make_request):
        """Test fetching multiple tickers concurrently."""
        # Mock API response
        mock_make_request.return_value = {
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

        client = AsyncAlphaVantageClient(api_key="test_key")
        results = await client.fetch_multiple_tickers(
            tickers=["AAPL", "MSFT"], max_concurrent=2
        )

        assert len(results) == 2
        assert "AAPL" in results
        assert "MSFT" in results

        for ticker, (df, success) in results.items():
            assert success is True
            assert df is not None
            assert df["ticker"].iloc[0] == ticker


@pytest.mark.asyncio
async def test_async_fetch_multiple_tickers():
    """Test convenience function for async multiple ticker fetching."""
    with patch("utils.async_client.AsyncAlphaVantageClient") as mock_client_class:
        mock_client = AsyncMock()
        mock_client.fetch_multiple_tickers.return_value = {
            "AAPL": (Mock(), True),
            "MSFT": (Mock(), True),
        }
        mock_client_class.return_value.__aenter__.return_value = mock_client

        result = await async_fetch_multiple_tickers(["AAPL", "MSFT"])

        assert len(result) == 2
        assert "AAPL" in result
        assert "MSFT" in result
