"""
Asynchronous API client for Alpha Vantage with connection pooling and rate limiting.

This module provides high-performance async operations for concurrent data fetching
with proper rate limiting and error handling.
"""

import asyncio
import aiohttp
import logging
from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime, timedelta
import json
import time

import pandas as pd

from .config import ALPHA_VANTAGE_API_KEY

logger = logging.getLogger(__name__)


class RateLimiter:
    """Rate limiter for API calls with exponential backoff."""

    def __init__(self, calls_per_minute: int = 5):
        self.calls_per_minute = calls_per_minute
        self.min_interval = 60.0 / calls_per_minute  # seconds between calls
        self.last_call_time = 0.0
        self.call_count = 0
        self.backoff_factor = 1.0

    async def acquire(self) -> None:
        """Acquire permission to make an API call."""
        current_time = time.time()
        time_since_last = current_time - self.last_call_time

        if time_since_last < self.min_interval * self.backoff_factor:
            sleep_time = (self.min_interval * self.backoff_factor) - time_since_last
            logger.debug(f"Rate limiting: sleeping for {sleep_time:.2f} seconds")
            await asyncio.sleep(sleep_time)

        self.last_call_time = time.time()
        self.call_count += 1

    def on_success(self) -> None:
        """Called after successful API call to reduce backoff."""
        self.backoff_factor = max(1.0, self.backoff_factor * 0.9)

    def on_rate_limit(self) -> None:
        """Called when rate limit is hit to increase backoff."""
        self.backoff_factor = min(10.0, self.backoff_factor * 2.0)
        logger.warning(
            f"Rate limit hit, increasing backoff to {self.backoff_factor:.2f}x"
        )


class AsyncAlphaVantageClient:
    """Async Alpha Vantage API client with connection pooling."""

    def __init__(
        self,
        api_key: Optional[str] = None,
        max_connections: int = 10,
        timeout: int = 30,
        retry_attempts: int = 3,
    ):
        self.api_key = api_key or ALPHA_VANTAGE_API_KEY
        self.base_url = "https://www.alphavantage.co/query"
        self.max_connections = max_connections
        self.timeout = aiohttp.ClientTimeout(total=timeout)
        self.retry_attempts = retry_attempts
        self.session: Optional[aiohttp.ClientSession] = None
        self.rate_limiter = RateLimiter()

        if not self.api_key:
            raise ValueError("Alpha Vantage API key is required")

    async def __aenter__(self):
        """Async context manager entry."""
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.close()

    async def start(self) -> None:
        """Initialize the HTTP session with connection pooling."""
        if self.session is None:
            connector = aiohttp.TCPConnector(
                limit=self.max_connections,
                limit_per_host=self.max_connections,
                ttl_dns_cache=300,
                use_dns_cache=True,
            )
            self.session = aiohttp.ClientSession(
                connector=connector, timeout=self.timeout
            )
            logger.info(f"Started async client with {self.max_connections} connections")

    async def close(self) -> None:
        """Close the HTTP session."""
        if self.session:
            await self.session.close()
            self.session = None
            logger.info("Closed async client session")

    async def _make_request(
        self, params: Dict[str, str], retries: int = 0
    ) -> Optional[Dict[str, Any]]:
        """Make an async HTTP request with retry logic."""
        if not self.session:
            await self.start()

        await self.rate_limiter.acquire()

        try:
            async with self.session.get(self.base_url, params=params) as response:
                response.raise_for_status()
                data = await response.json()

                # Check for API errors
                if "Error Message" in data:
                    logger.error(f"API error: {data['Error Message']}")
                    return None

                if "Note" in data:
                    # Rate limit hit
                    logger.warning(f"Rate limit: {data['Note']}")
                    self.rate_limiter.on_rate_limit()

                    if retries < self.retry_attempts:
                        await asyncio.sleep(60)  # Wait before retry
                        return await self._make_request(params, retries + 1)
                    return None

                self.rate_limiter.on_success()
                return data

        except aiohttp.ClientError as e:
            logger.error(f"HTTP error: {e}")
            if retries < self.retry_attempts:
                await asyncio.sleep(2**retries)  # Exponential backoff
                return await self._make_request(params, retries + 1)
            return None

        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            return None

    async def fetch_intraday_data(
        self, ticker: str, interval: str = "1min", outputsize: str = "compact"
    ) -> Tuple[Optional[pd.DataFrame], bool]:
        """
        Fetch intraday data asynchronously.

        Args:
            ticker: Stock ticker symbol
            interval: Time interval (1min, 5min, 15min, 30min, 60min)
            outputsize: 'compact' or 'full'

        Returns:
            Tuple of (DataFrame or None, success boolean)
        """
        params = {
            "function": "TIME_SERIES_INTRADAY",
            "symbol": ticker,
            "interval": interval,
            "outputsize": outputsize,
            "apikey": self.api_key,
        }

        data = await self._make_request(params)
        if not data:
            return None, False

        time_series_key = f"Time Series ({interval})"
        if time_series_key not in data:
            logger.error(f"No time series data for {ticker}")
            return None, False

        try:
            # Convert to DataFrame
            time_series = data[time_series_key]
            df = pd.DataFrame.from_dict(time_series, orient="index")

            # Rename columns
            df.columns = [col.split(". ")[1] for col in df.columns]

            # Convert values to float
            for col in df.columns:
                df[col] = df[col].astype(float)

            # Add date and ticker columns
            df.index = pd.to_datetime(df.index)
            df = df.reset_index()
            df.rename(columns={"index": "datetime"}, inplace=True)
            df["ticker"] = ticker

            logger.info(
                f"Successfully fetched {len(df)} {interval} records for {ticker}"
            )
            return df, True

        except Exception as e:
            logger.error(f"Error processing data for {ticker}: {e}")
            return None, False

    async def fetch_daily_data(
        self, ticker: str, outputsize: str = "compact"
    ) -> Tuple[Optional[pd.DataFrame], bool]:
        """
        Fetch daily data asynchronously.

        Args:
            ticker: Stock ticker symbol
            outputsize: 'compact' or 'full'

        Returns:
            Tuple of (DataFrame or None, success boolean)
        """
        params = {
            "function": "TIME_SERIES_DAILY",
            "symbol": ticker,
            "outputsize": outputsize,
            "apikey": self.api_key,
        }

        data = await self._make_request(params)
        if not data:
            return None, False

        time_series_key = "Time Series (Daily)"
        if time_series_key not in data:
            logger.error(f"No daily data for {ticker}")
            return None, False

        try:
            # Convert to DataFrame
            time_series = data[time_series_key]
            df = pd.DataFrame.from_dict(time_series, orient="index")

            # Rename columns
            df.columns = [col.split(". ")[1] for col in df.columns]

            # Convert values to float
            for col in df.columns:
                df[col] = df[col].astype(float)

            # Add date and ticker columns
            df.index = pd.to_datetime(df.index)
            df = df.reset_index()
            df.rename(columns={"index": "Date"}, inplace=True)
            df["ticker"] = ticker

            logger.info(f"Successfully fetched {len(df)} daily records for {ticker}")
            return df, True

        except Exception as e:
            logger.error(f"Error processing daily data for {ticker}: {e}")
            return None, False

    async def fetch_multiple_tickers(
        self,
        tickers: List[str],
        data_type: str = "intraday",
        interval: str = "1min",
        outputsize: str = "compact",
        max_concurrent: int = 5,
    ) -> Dict[str, Tuple[Optional[pd.DataFrame], bool]]:
        """
        Fetch data for multiple tickers concurrently.

        Args:
            tickers: List of ticker symbols
            data_type: 'intraday' or 'daily'
            interval: Time interval for intraday data
            outputsize: 'compact' or 'full'
            max_concurrent: Maximum concurrent requests

        Returns:
            Dictionary mapping ticker to (DataFrame, success) tuples
        """
        semaphore = asyncio.Semaphore(max_concurrent)
        results = {}

        async def fetch_single(ticker: str):
            async with semaphore:
                if data_type == "daily":
                    return await self.fetch_daily_data(ticker, outputsize)
                else:
                    return await self.fetch_intraday_data(ticker, interval, outputsize)

        # Create tasks for all tickers
        tasks = {ticker: fetch_single(ticker) for ticker in tickers}

        # Execute tasks concurrently
        logger.info(
            f"Fetching {data_type} data for {len(tickers)} tickers "
            f"with {max_concurrent} concurrent connections"
        )

        completed_tasks = await asyncio.gather(*tasks.values(), return_exceptions=True)

        # Collect results
        for ticker, result in zip(tickers, completed_tasks):
            if isinstance(result, Exception):
                logger.error(f"Error fetching data for {ticker}: {result}")
                results[ticker] = (None, False)
            else:
                results[ticker] = result

        successful = sum(1 for _, success in results.values() if success)
        logger.info(
            f"Completed fetching: {successful}/{len(tickers)} tickers successful"
        )

        return results


# Convenience function for backward compatibility
async def async_fetch_multiple_tickers(
    tickers: List[str],
    data_type: str = "intraday",
    interval: str = "1min",
    api_key: Optional[str] = None,
) -> Dict[str, Tuple[Optional[pd.DataFrame], bool]]:
    """
    Convenience function to fetch multiple tickers asynchronously.

    Args:
        tickers: List of ticker symbols
        data_type: 'intraday' or 'daily'
        interval: Time interval for intraday data
        api_key: Optional API key override

    Returns:
        Dictionary mapping ticker to (DataFrame, success) tuples
    """
    async with AsyncAlphaVantageClient(api_key=api_key) as client:
        return await client.fetch_multiple_tickers(
            tickers=tickers, data_type=data_type, interval=interval
        )


# Synchronous wrapper for backward compatibility
def fetch_multiple_tickers_sync(
    tickers: List[str],
    data_type: str = "intraday",
    interval: str = "1min",
    api_key: Optional[str] = None,
) -> Dict[str, Tuple[Optional[pd.DataFrame], bool]]:
    """
    Synchronous wrapper for async multiple ticker fetching.

    Args:
        tickers: List of ticker symbols
        data_type: 'intraday' or 'daily'
        interval: Time interval for intraday data
        api_key: Optional API key override

    Returns:
        Dictionary mapping ticker to (DataFrame, success) tuples
    """
    return asyncio.run(
        async_fetch_multiple_tickers(
            tickers=tickers, data_type=data_type, interval=interval, api_key=api_key
        )
    )
