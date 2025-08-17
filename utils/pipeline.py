"""
High-performance data processing pipeline with async operations and caching.

This module provides optimized data processing operations that leverage
async operations, caching, and parallel processing for maximum performance.
"""

import asyncio
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Optional, Tuple

import pandas as pd

from .async_client import AsyncAlphaVantageClient, fetch_multiple_tickers_sync
from .cache import cache_key_for_ticker_data, cached_fetch_wrapper, get_cache
from .data_storage import save_df_to_s3
from .ticker_manager import clean_ticker_list, read_master_tickerlist

logger = logging.getLogger(__name__)


class DataPipeline:
    """High-performance data processing pipeline."""

    def __init__(
        self,
        max_concurrent_api: int = 5,
        max_concurrent_storage: int = 10,
        use_cache: bool = True,
        cache_ttl: int = 300,  # 5 minutes
    ):
        self.max_concurrent_api = max_concurrent_api
        self.max_concurrent_storage = max_concurrent_storage
        self.use_cache = use_cache
        self.cache_ttl = cache_ttl
        self.cache = get_cache() if use_cache else None

    async def fetch_data_batch_async(
        self,
        tickers: List[str],
        data_type: str = "intraday",
        interval: str = "1min",
        outputsize: str = "compact",
    ) -> Dict[str, Tuple[Optional[pd.DataFrame], bool]]:
        """
        Fetch data for multiple tickers using async operations.

        Args:
            tickers: List of ticker symbols
            data_type: 'intraday' or 'daily'
            interval: Time interval for intraday data
            outputsize: 'compact' or 'full'

        Returns:
            Dictionary mapping ticker to (DataFrame, success) tuples
        """
        start_time = time.time()

        # Check cache first if enabled
        if self.use_cache:
            cached_results = {}
            uncached_tickers = []

            for ticker in tickers:
                cache_key = cache_key_for_ticker_data(
                    ticker, data_type, interval, outputsize
                )
                cached_data = self.cache.get(cache_key)
                if cached_data is not None:
                    cached_results[ticker] = cached_data
                else:
                    uncached_tickers.append(ticker)

            logger.info(
                f"Cache check: {len(cached_results)} hits, "
                f"{len(uncached_tickers)} misses"
            )
        else:
            cached_results = {}
            uncached_tickers = tickers

        # Fetch uncached data
        if uncached_tickers:
            async with AsyncAlphaVantageClient() as client:
                fresh_results = await client.fetch_multiple_tickers(
                    tickers=uncached_tickers,
                    data_type=data_type,
                    interval=interval,
                    outputsize=outputsize,
                    max_concurrent=self.max_concurrent_api,
                )

            # Cache successful results
            if self.use_cache:
                for ticker, (df, success) in fresh_results.items():
                    if success and df is not None:
                        cache_key = cache_key_for_ticker_data(
                            ticker, data_type, interval, outputsize
                        )
                        self.cache.set(cache_key, (df, success), self.cache_ttl)
        else:
            fresh_results = {}

        # Combine cached and fresh results
        all_results = {**cached_results, **fresh_results}

        elapsed = time.time() - start_time
        successful = sum(1 for _, success in all_results.values() if success)

        logger.info(
            f"Fetched {len(tickers)} tickers in {elapsed:.2f}s "
            f"({successful} successful, {len(cached_results)} from cache)"
        )

        return all_results

    def fetch_data_batch_sync(
        self,
        tickers: List[str],
        data_type: str = "intraday",
        interval: str = "1min",
    ) -> Dict[str, Tuple[Optional[pd.DataFrame], bool]]:
        """
        Synchronous wrapper for batch data fetching.

        Args:
            tickers: List of ticker symbols
            data_type: 'intraday' or 'daily'
            interval: Time interval for intraday data

        Returns:
            Dictionary mapping ticker to (DataFrame, success) tuples
        """
        return asyncio.run(
            self.fetch_data_batch_async(
                tickers=tickers, data_type=data_type, interval=interval
            )
        )

    def save_data_batch(
        self, data_results: Dict[str, Tuple[Optional[pd.DataFrame], bool]]
    ) -> Dict[str, bool]:
        """
        Save multiple DataFrames concurrently.

        Args:
            data_results: Dictionary mapping ticker to (DataFrame, success) tuples

        Returns:
            Dictionary mapping ticker to save success status
        """
        start_time = time.time()
        save_results = {}

        # Filter out failed fetches
        valid_data = {
            ticker: df
            for ticker, (df, success) in data_results.items()
            if success and df is not None
        }

        if not valid_data:
            logger.warning("No valid data to save")
            return save_results

        # Use ThreadPoolExecutor for concurrent I/O operations
        with ThreadPoolExecutor(max_workers=self.max_concurrent_storage) as executor:
            # Submit save tasks
            future_to_ticker = {
                executor.submit(
                    save_df_to_s3,
                    df,
                    f"data/intraday/{ticker}_1min.csv",  # Standardized path
                ): ticker
                for ticker, df in valid_data.items()
            }

            # Collect results
            for future in as_completed(future_to_ticker):
                ticker = future_to_ticker[future]
                try:
                    success = future.result()
                    save_results[ticker] = success
                except Exception as e:
                    logger.error(f"Error saving data for {ticker}: {e}")
                    save_results[ticker] = False

        elapsed = time.time() - start_time
        successful_saves = sum(save_results.values())

        logger.info(
            f"Saved {len(valid_data)} datasets in {elapsed:.2f}s "
            f"({successful_saves} successful)"
        )

        return save_results

    def process_full_universe(
        self,
        data_type: str = "intraday",
        interval: str = "1min",
        batch_size: int = 25,
    ) -> Dict[str, Dict[str, bool]]:
        """
        Process the full ticker universe in optimized batches.

        Args:
            data_type: 'intraday' or 'daily'
            interval: Time interval for intraday data
            batch_size: Number of tickers per batch

        Returns:
            Dictionary with 'fetch' and 'save' results for each ticker
        """
        start_time = time.time()

        # Get ticker universe
        all_tickers = read_master_tickerlist()
        clean_tickers = clean_ticker_list(all_tickers)

        if not clean_tickers:
            logger.error("No valid tickers found")
            return {"fetch": {}, "save": {}}

        logger.info(
            f"Processing {len(clean_tickers)} tickers in batches of {batch_size}"
        )

        # Split into batches
        batches = [
            clean_tickers[i : i + batch_size]
            for i in range(0, len(clean_tickers), batch_size)
        ]

        all_fetch_results = {}
        all_save_results = {}

        # Process each batch
        for i, batch in enumerate(batches, 1):
            logger.info(f"Processing batch {i}/{len(batches)} ({len(batch)} tickers)")

            try:
                # Fetch data for batch
                fetch_results = self.fetch_data_batch_sync(
                    tickers=batch, data_type=data_type, interval=interval
                )
                all_fetch_results.update(
                    {ticker: success for ticker, (_, success) in fetch_results.items()}
                )

                # Save data for batch
                save_results = self.save_data_batch(fetch_results)
                all_save_results.update(save_results)

                # Brief pause between batches to avoid overwhelming the API
                if i < len(batches):
                    asyncio.sleep(1)

            except Exception as e:
                logger.error(f"Error processing batch {i}: {e}")
                # Mark all tickers in this batch as failed
                for ticker in batch:
                    all_fetch_results[ticker] = False
                    all_save_results[ticker] = False

        elapsed = time.time() - start_time
        successful_fetches = sum(all_fetch_results.values())
        successful_saves = sum(all_save_results.values())

        logger.info(
            f"Universe processing complete in {elapsed:.2f}s: "
            f"{successful_fetches}/{len(clean_tickers)} fetched, "
            f"{successful_saves}/{len(clean_tickers)} saved"
        )

        return {"fetch": all_fetch_results, "save": all_save_results}

    def get_performance_stats(self) -> Dict[str, any]:
        """Get performance statistics for the pipeline."""
        stats = {"cache": None, "concurrent_limits": {}}

        if self.cache:
            stats["cache"] = self.cache.stats()

        stats["concurrent_limits"] = {
            "max_api_concurrent": self.max_concurrent_api,
            "max_storage_concurrent": self.max_concurrent_storage,
        }

        return stats


# Global pipeline instance
_global_pipeline: Optional[DataPipeline] = None


def get_pipeline() -> DataPipeline:
    """Get or create the global data pipeline instance."""
    global _global_pipeline
    if _global_pipeline is None:
        _global_pipeline = DataPipeline()
    return _global_pipeline


def configure_pipeline(
    max_concurrent_api: int = 5,
    max_concurrent_storage: int = 10,
    use_cache: bool = True,
    cache_ttl: int = 300,
) -> DataPipeline:
    """Configure the global data pipeline."""
    global _global_pipeline
    _global_pipeline = DataPipeline(
        max_concurrent_api=max_concurrent_api,
        max_concurrent_storage=max_concurrent_storage,
        use_cache=use_cache,
        cache_ttl=cache_ttl,
    )
    return _global_pipeline


# Convenience functions for backward compatibility
def fetch_multiple_tickers_optimized(
    tickers: List[str], data_type: str = "intraday", interval: str = "1min"
) -> Dict[str, Tuple[Optional[pd.DataFrame], bool]]:
    """
    Optimized function to fetch multiple tickers with caching and async operations.

    Args:
        tickers: List of ticker symbols
        data_type: 'intraday' or 'daily'
        interval: Time interval for intraday data

    Returns:
        Dictionary mapping ticker to (DataFrame, success) tuples
    """
    pipeline = get_pipeline()
    return pipeline.fetch_data_batch_sync(
        tickers=tickers, data_type=data_type, interval=interval
    )


def process_ticker_universe_optimized(
    data_type: str = "intraday", interval: str = "1min", batch_size: int = 25
) -> Dict[str, Dict[str, bool]]:
    """
    Optimized function to process the full ticker universe.

    Args:
        data_type: 'intraday' or 'daily'
        interval: Time interval for intraday data
        batch_size: Number of tickers per batch

    Returns:
        Dictionary with 'fetch' and 'save' results for each ticker
    """
    pipeline = get_pipeline()
    return pipeline.process_full_universe(
        data_type=data_type, interval=interval, batch_size=batch_size
    )
