"""
Unified Data Fetch Manager - The core data pipeline for the trading system.

This module handles all market data fetching, merging, validation, and storage
with self-healing capabilities, retention policies, and atomic operations.
"""

import json
import os
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

import pandas as pd

from utils.providers.router import get_candles, health_check
from utils.config import config
from utils.helpers import get_test_mode_reason
from utils.logging_setup import get_logger
from utils.spaces_io import spaces_io
from utils.time_utils import (
    convert_to_utc,
    filter_market_hours,
    get_market_time,
    get_trading_days_back,
    is_weekend,
    utc_now,
)
from utils.validation import clean_dataframe, validate_dataframe

logger = get_logger(__name__)


class DataFetchManager:
    """Unified data fetch manager with self-healing and retention policies."""

    def __init__(self) -> None:
        """Initialize the data fetch manager."""
        self.universe_tickers: List[str] = []
        self.load_universe()

    def load_universe(self) -> None:
        """Load the master ticker list from Spaces."""
        try:
            universe_key = config.get_spaces_path(*config.MASTER_TICKERLIST_PATH)
            df = spaces_io.download_dataframe(universe_key)
            
            if df is not None and not df.empty:
                # Filter for active tickers
                active_tickers = df[df["active"] == 1]["symbol"].tolist()
                self.universe_tickers = active_tickers
                logger.info(f"Loaded {len(active_tickers)} active tickers from universe")
            else:
                # Fallback to default tickers
                self.universe_tickers = config.FALLBACK_TICKERS
                logger.warning("Universe not found, using fallback tickers")
                
        except Exception as e:
            logger.error(f"Error loading universe: {e}")
            self.universe_tickers = config.FALLBACK_TICKERS

    def run_full_data_update(self) -> bool:
        """
        Run complete data update for all intervals and tickers.
        
        Returns:
            True if successful, False otherwise
        """
        logger.job_start("DataFetchManager.run_full_data_update")
        start_time = time.time()
        
        try:
            is_test_mode, test_reason = get_test_mode_reason()
            if is_test_mode:
                logger.info(f"Running in test mode: {test_reason}")
            
            # Update daily data first (longer retention, less frequent)
            daily_success = self.run_daily_updates()
            
            # Update intraday data
            intraday_1min_success = self.run_intraday_updates("1min")
            intraday_30min_success = self.run_intraday_updates("30min")
            
            # Overall success if at least one succeeded
            overall_success = daily_success or intraday_1min_success or intraday_30min_success
            
            duration = time.time() - start_time
            logger.job_complete(
                "DataFetchManager.run_full_data_update",
                duration_seconds=duration,
                success=overall_success,
                daily_success=daily_success,
                intraday_1min_success=intraday_1min_success,
                intraday_30min_success=intraday_30min_success,
            )
            
            return overall_success
            
        except Exception as e:
            duration = time.time() - start_time
            logger.job_complete(
                "DataFetchManager.run_full_data_update",
                duration_seconds=duration,
                success=False,
                error=str(e),
            )
            return False

    def run_daily_updates(self) -> bool:
        """
        Update daily data for all tickers.
        
        Returns:
            True if successful, False otherwise
        """
        logger.info("Starting daily data updates")
        start_time = time.time()
        
        successful_tickers = 0
        
        for ticker in self.universe_tickers:
            try:
                if self.fetch_daily_data(ticker):
                    successful_tickers += 1
                
                # Brief pause between tickers
                time.sleep(0.5)
                
            except Exception as e:
                logger.error(f"Error updating daily data for {ticker}: {e}")
        
        duration = time.time() - start_time
        success_rate = successful_tickers / len(self.universe_tickers) if self.universe_tickers else 0
        
        logger.info(
            f"Daily updates completed in {duration:.1f}s",
            successful_tickers=successful_tickers,
            total_tickers=len(self.universe_tickers),
            success_rate=success_rate,
        )
        
        return success_rate > 0.5  # Consider successful if >50% of tickers updated

    def run_intraday_updates(self, interval: str) -> bool:
        """
        Update intraday data for specified interval.
        
        Args:
            interval: Time interval (1min or 30min)
            
        Returns:
            True if successful, False otherwise
        """
        logger.info(f"Starting {interval} intraday updates")
        start_time = time.time()
        
        successful_tickers = 0
        
        for ticker in self.universe_tickers:
            try:
                if self.fetch_intraday_data(ticker, interval):
                    successful_tickers += 1
                
                # Brief pause between tickers
                time.sleep(0.2)
                
            except Exception as e:
                logger.error(f"Error updating {interval} data for {ticker}: {e}")
        
        duration = time.time() - start_time
        success_rate = successful_tickers / len(self.universe_tickers) if self.universe_tickers else 0
        
        logger.info(
            f"{interval} updates completed in {duration:.1f}s",
            successful_tickers=successful_tickers,
            total_tickers=len(self.universe_tickers),
            success_rate=success_rate,
        )
        
        return success_rate > 0.5

    def fetch_daily_data(self, ticker: str) -> bool:
        """
        Fetch and update daily data for a ticker.
        
        Args:
            ticker: Stock symbol
            
        Returns:
            True if successful, False otherwise
        """
        try:
            data_key = config.get_spaces_path("data", "daily", f"{ticker}.csv")
            
            # Determine fetch mode
            existing_df = spaces_io.download_dataframe(data_key)
            mode = self._determine_fetch_mode(existing_df, "daily")
            
            outputsize = "full" if mode == "FULL" else "compact"
            logger.debug(f"Fetching daily data for {ticker} (mode={mode}, outputsize={outputsize})")
            
            # Fetch new data using MarketData provider
            new_df = get_candles(
                symbol=ticker,
                resolution="D",
                countback=220,  # Fetch more than needed, will be trimmed
                adjustsplits=True,
            )
            if new_df is None or new_df.empty:
                logger.error(f"Failed to fetch daily data for {ticker}")
                return False
            
            # Merge with existing data
            merged_df = self._merge_data(existing_df, new_df, "daily")
            if merged_df is None:
                logger.error(f"Failed to merge daily data for {ticker}")
                return False
            
            # Apply retention policy
            trimmed_df = self._apply_daily_retention(merged_df)
            
            # Validate and clean
            is_valid, errors = validate_dataframe(trimmed_df, "DAILY", ticker)
            if not is_valid:
                logger.warning(f"Daily data validation failed for {ticker}: {errors}")
                # Continue with cleaned data
                trimmed_df = clean_dataframe(trimmed_df, "DAILY")
            
            # Upload to Spaces with atomic writes
            metadata = {
                "symbol": ticker,
                "interval": "daily",
                "mode": mode,
                "rows": str(len(trimmed_df)),
                "managed-by": "data_fetch_manager",
                "provider": "marketdata",
                "deployment": config.DEPLOYMENT_TAG or "unknown",
            }
            
            success = spaces_io.upload_dataframe(trimmed_df, data_key, metadata=metadata)
            if success:
                # Log per-run details as required
                latest_timestamp = trimmed_df["timestamp"].max() if not trimmed_df.empty else None
                latest_ts_utc = latest_timestamp.strftime("%Y-%m-%dT%H:%M:%SZ") if latest_timestamp else "none"
                
                # Calculate rows added (new rows only, not total)
                if existing_df is not None and not existing_df.empty:
                    existing_count = len(existing_df)
                    appended_rows = len(trimmed_df) - existing_count
                else:
                    appended_rows = len(trimmed_df)
                
                logger.info(
                    f"✅ {ticker} (daily) update complete - "
                    f"provider=marketdata interval=daily appended={appended_rows} "
                    f"final_rows={len(trimmed_df)} latest_ts_utc={latest_ts_utc}"
                )
                
                # Update manifest
                self._update_manifest(ticker, "daily", trimmed_df, mode)
                logger.data_operation(
                    "daily_update",
                    ticker,
                    rows_processed=len(trimmed_df),
                    mode=mode,
                    provider="marketdata",
                )
            
            return success
            
        except Exception as e:
            logger.error(f"Error fetching daily data for {ticker}: {e}")
            return False

    def fetch_intraday_data(self, ticker: str, interval: str) -> bool:
        """
        Fetch and update intraday data for a ticker.
        
        Args:
            ticker: Stock symbol
            interval: Time interval (1min or 30min)
            
        Returns:
            True if successful, False otherwise
        """
        try:
            if interval == "1min":
                data_key = config.get_spaces_path("data", "intraday", "1min", f"{ticker}.csv")
                schema_name = "INTRADAY_1MIN"
            else:
                data_key = config.get_spaces_path("data", "intraday", "30min", f"{ticker}.csv")
                schema_name = "INTRADAY_30MIN"
            
            # Determine fetch mode
            existing_df = spaces_io.download_dataframe(data_key)
            mode = self._determine_fetch_mode(existing_df, interval)
            
            outputsize = "full" if mode == "FULL" else "compact"
            logger.debug(f"Fetching {interval} data for {ticker} (mode={mode}, outputsize={outputsize})")
            
            # Check provider health before intraday operations  
            if interval == "1min":
                healthy, health_msg = health_check()
                if not healthy:
                    degraded_allowed = os.getenv("PROVIDER_DEGRADED_ALLOWED", "true").lower() == "true"
                    if degraded_allowed:
                        logger.info(f"Provider degraded, skipping 1min intraday update: {health_msg}")
                        return True  # Skip with success
                    else:
                        logger.error(f"Provider health check failed: {health_msg}")
                        return False
                        
            # Determine fetch strategy based on interval
            if interval == "1min":
                # Fetch from 8 days ago to now for 1min data
                now_utc = utc_now()
                from_utc = now_utc - timedelta(days=8)
                
                new_df = get_candles(
                    symbol=ticker,
                    resolution="1",
                    from_iso=from_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
                    to_iso=now_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
                    extended=config.INTRADAY_EXTENDED,
                )
            else:  # 30min
                # Fetch last 520 bars for 30min data
                new_df = get_candles(
                    symbol=ticker,
                    resolution="30",
                    countback=520,
                    extended=config.INTRADAY_EXTENDED,
                )
            if new_df is None or new_df.empty:
                logger.error(f"Failed to fetch {interval} data for {ticker}")
                return False
            
            # Merge with existing data
            merged_df = self._merge_data(existing_df, new_df, interval)
            if merged_df is None:
                logger.error(f"Failed to merge {interval} data for {ticker}")
                return False
            
            # Apply retention policy
            if interval == "1min":
                trimmed_df = self._apply_1min_retention(merged_df)
            else:
                trimmed_df = self._apply_30min_retention(merged_df)
            
            # Filter to market hours if configured
            if not config.INTRADAY_EXTENDED:
                trimmed_df = filter_market_hours(
                    trimmed_df,
                    include_premarket=False,
                    include_afterhours=False,
                )
            
            # Validate and clean
            is_valid, errors = validate_dataframe(trimmed_df, schema_name, ticker)
            if not is_valid:
                logger.warning(f"{interval} data validation failed for {ticker}: {errors}")
                # Continue with cleaned data
                trimmed_df = clean_dataframe(trimmed_df, schema_name)
            
            # Upload to Spaces with atomic writes
            metadata = {
                "symbol": ticker,
                "interval": interval,
                "mode": mode,
                "rows": str(len(trimmed_df)),
                "managed-by": "data_fetch_manager",
                "provider": "marketdata",
                "deployment": config.DEPLOYMENT_TAG or "unknown",
            }
            
            success = spaces_io.upload_dataframe(trimmed_df, data_key, metadata=metadata)
            if success:
                # Log per-run details as required
                latest_timestamp = trimmed_df["timestamp"].max() if not trimmed_df.empty else None
                latest_ts_utc = latest_timestamp.strftime("%Y-%m-%dT%H:%M:%SZ") if latest_timestamp else "none"
                
                # Calculate rows added (new rows only, not total)
                if existing_df is not None and not existing_df.empty:
                    existing_count = len(existing_df)
                    appended_rows = len(trimmed_df) - existing_count
                else:
                    appended_rows = len(trimmed_df)
                
                logger.info(
                    f"✅ {ticker} ({interval}) update complete - "
                    f"provider=marketdata interval={interval} appended={appended_rows} "
                    f"final_rows={len(trimmed_df)} latest_ts_utc={latest_ts_utc}"
                )
                
                # Update manifest
                self._update_manifest(ticker, interval, trimmed_df, mode)
                logger.data_operation(
                    f"{interval}_update",
                    ticker,
                    interval=interval,
                    rows_processed=len(trimmed_df),
                    mode=mode,
                    provider="marketdata",
                )
            
            return success
            
        except Exception as e:
            logger.error(f"Error fetching {interval} data for {ticker}: {e}")
            return False

    def _determine_fetch_mode(self, existing_df: Optional[pd.DataFrame], interval: str) -> str:
        """
        Determine whether to use full or compact fetch mode.
        
        Args:
            existing_df: Existing DataFrame or None
            interval: Data interval
            
        Returns:
            "FULL" or "COMPACT"
        """
        # If no existing data, use full mode
        if existing_df is None or existing_df.empty:
            return "FULL"
        
        # Check file size/row count thresholds
        if interval == "daily":
            min_rows = 50  # Expect at least 50 days
        elif interval == "1min":
            min_rows = 1000  # Expect substantial intraday data
        else:  # 30min
            min_rows = 100  # Expect reasonable amount of 30min data
        
        if len(existing_df) < min_rows:
            return "FULL"
        
        # Check data freshness - if data is very old, do full refresh
        if "timestamp" in existing_df.columns:
            # Both timestamps need to be timezone-aware UTC for proper comparison
            latest_timestamp = pd.to_datetime(existing_df["timestamp"].max(), utc=True)
            now_utc = utc_now()
            days_old = (now_utc - latest_timestamp).days
            
            if interval == "daily" and days_old > 7:
                return "FULL"
            elif interval in ["1min", "30min"] and days_old > 3:
                return "FULL"
        
        return "COMPACT"

    def _merge_data(
        self,
        existing_df: Optional[pd.DataFrame],
        new_df: pd.DataFrame,
        interval: str,
    ) -> Optional[pd.DataFrame]:
        """
        Merge existing and new data, only adding newer rows as per MarketData requirements.
        
        Args:
            existing_df: Existing DataFrame or None
            new_df: New DataFrame to merge
            interval: Data interval for timestamp column
            
        Returns:
            Merged DataFrame or None if error
        """
        try:
            if existing_df is None or existing_df.empty:
                # Ensure timestamps are UTC aware
                new_df_copy = new_df.copy()
                new_df_copy["timestamp"] = pd.to_datetime(new_df_copy["timestamp"], utc=True)
                return new_df_copy
            
            # All data uses "timestamp" column with UTC timestamps
            if "timestamp" not in new_df.columns:
                logger.error("Missing timestamp column in new data")
                return None
            
            if "timestamp" not in existing_df.columns:
                logger.error("Missing timestamp column in existing data")
                return None
            
            # Ensure both DataFrames have UTC-aware timestamps
            existing_df_copy = existing_df.copy()
            new_df_copy = new_df.copy()
            
            existing_df_copy["timestamp"] = pd.to_datetime(existing_df_copy["timestamp"], utc=True)
            new_df_copy["timestamp"] = pd.to_datetime(new_df_copy["timestamp"], utc=True)
            
            # Get the maximum timestamp from existing data
            existing_max = existing_df_copy["timestamp"].max()
            
            # Filter new data to only include rows newer than existing max
            newer_rows = new_df_copy[new_df_copy["timestamp"] > existing_max]
            
            if newer_rows.empty:
                logger.debug("No newer data to merge")
                return existing_df_copy
            
            # Combine existing data with only newer rows
            combined_df = pd.concat([existing_df_copy, newer_rows], ignore_index=True)
            
            # Sort by timestamp and remove any potential duplicates
            combined_df = combined_df.sort_values("timestamp")
            combined_df = combined_df.drop_duplicates(subset=["timestamp"], keep="last")
            combined_df = combined_df.reset_index(drop=True)
            
            logger.debug(
                f"Merged data: existing={len(existing_df)}, new={len(new_df)}, "
                f"newer_rows={len(newer_rows)}, final={len(combined_df)}"
            )
            
            return combined_df
            
        except Exception as e:
            logger.error(f"Error merging data: {e}")
            return None

    def _apply_daily_retention(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply retention policy for daily data."""
        if df.empty:
            return df
        
        # Keep last ~210-220 rows for daily data
        max_rows = config.DAILY_RETENTION_ROWS
        
        # Ensure timestamp is UTC-aware datetime for proper sorting
        df = df.copy()
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
        df = df.sort_values("timestamp")
        
        if len(df) > max_rows:
            df = df.tail(max_rows).reset_index(drop=True)
        
        logger.debug(f"Daily retention: kept {len(df)} rows (max: {max_rows})")
        return df

    def _apply_1min_retention(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply retention policy for 1-minute data using UTC timestamps only."""
        if df.empty:
            return df
        
        # Keep data from last N days using UTC timestamps only (no ET conversion)
        retention_days = config.INTRADAY_1MIN_RETENTION_DAYS
        now_utc = utc_now()
        cutoff_utc = now_utc - timedelta(days=retention_days)
        
        # Ensure timestamp is UTC-aware datetime
        df = df.copy()
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
        
        # Filter to retention window (UTC only)
        df = df[df["timestamp"] >= cutoff_utc].reset_index(drop=True)
        
        logger.debug(f"1min retention: kept {len(df)} rows after {retention_days} day cutoff")
        return df

    def _apply_30min_retention(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply retention policy for 30-minute data."""
        if df.empty:
            return df
        
        # Keep last N rows, then sort by timestamp to maintain order
        max_rows = config.INTRADAY_30MIN_RETENTION_ROWS
        
        # Ensure timestamp is UTC-aware datetime for proper sorting
        df = df.copy()
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
        df = df.sort_values("timestamp")
        
        if len(df) > max_rows:
            df = df.tail(max_rows).reset_index(drop=True)
        
        logger.debug(f"30min retention: kept {len(df)} rows (max: {max_rows})")
        return df

    def _update_manifest(
        self,
        ticker: str,
        interval: str,
        df: pd.DataFrame,
        mode: str,
    ) -> None:
        """
        Update the fetch status manifest.
        
        Args:
            ticker: Stock symbol
            interval: Data interval
            df: Processed DataFrame
            mode: Fetch mode used
        """
        try:
            manifest_key = config.get_spaces_path("data", "manifest", "fetch_status.json")
            
            # Load existing manifest
            manifest = spaces_io.download_json(manifest_key) or {}
            
            # Create entry key
            entry_key = f"{ticker}:{interval}"
            
            # Determine timestamp column
            timestamp_col = "date" if interval == "daily" else "timestamp"
            
            # Create manifest entry
            if not df.empty and timestamp_col in df.columns:
                timestamps = pd.to_datetime(df[timestamp_col])
                first_ts = timestamps.min().isoformat()
                last_ts = timestamps.max().isoformat()
            else:
                first_ts = last_ts = None
            
            manifest[entry_key] = {
                "last_fetch_utc": utc_now().isoformat(),
                "rows": len(df),
                "first_ts": first_ts,
                "last_ts": last_ts,
                "mode_used": mode,
                "degraded": False,  # TODO: Add degradation logic
                "file_size_bytes": 0,  # TODO: Calculate actual size
                "api_calls_used": 1,
                "commit": config.DEPLOYMENT_TAG or "unknown",
            }
            
            # Upload updated manifest
            spaces_io.upload_json(manifest, manifest_key)
            
        except Exception as e:
            logger.error(f"Error updating manifest for {ticker}:{interval}: {e}")


def main():
    """Main entry point for the data fetch manager."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Unified Data Fetch Manager - Single Authority for Market Data",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --job daily
  %(prog)s --job intraday --interval 1min
  %(prog)s --job intraday --interval 30min --force-full
  %(prog)s --job intraday --interval all --tickers AAPL,TSLA
  
Legacy (deprecated):
  %(prog)s --job intraday_1min   # Use: --job intraday --interval 1min
  %(prog)s --job intraday_30min  # Use: --job intraday --interval 30min
        """
    )
    
    # Canonical flags
    parser.add_argument(
        "--job",
        choices=["daily", "intraday", "intraday_1min", "intraday_30min"],  # Include legacy
        required=True,
        help="Job type to run: 'daily' or 'intraday' with --interval",
    )
    parser.add_argument(
        "--interval",
        choices=["1min", "30min", "all"],
        help="Required for --job intraday: {1min|30min|all}",
    )
    parser.add_argument(
        "--tickers",
        help="Comma-separated list of tickers (overrides universe)",
    )
    parser.add_argument(
        "--force-full",
        action="store_true",
        help="Force full refresh of all data",
    )
    
    # Test mode flags
    test_group = parser.add_mutually_exclusive_group()
    test_group.add_argument(
        "--test-mode",
        action="store_true",
        help="Run in test mode (no API calls)",
    )
    test_group.add_argument(
        "--no-test-mode",
        action="store_true",
        help="Explicitly disable test mode",
    )
    
    args = parser.parse_args()
    
    # Handle legacy flags with deprecation warnings
    if args.job == "intraday_1min":
        logger.warning("DEPRECATED: --job intraday_1min. Use: --job intraday --interval 1min")
        args.job = "intraday"
        args.interval = "1min"
    elif args.job == "intraday_30min":
        logger.warning("DEPRECATED: --job intraday_30min. Use: --job intraday --interval 30min")
        args.job = "intraday"
        args.interval = "30min"
    
    # Validate canonical flag combinations
    if args.job == "intraday" and not args.interval:
        parser.error("--job intraday requires --interval {1min|30min|all}")
    elif args.job == "daily" and args.interval:
        parser.error("--job daily does not accept --interval (intervals are for intraday only)")
    
    # Validate interval choices
    if args.interval and args.interval not in ["1min", "30min", "all"]:
        parser.error(f"Invalid --interval '{args.interval}'. Use: 1min, 30min, or all")
    
    manager = DataFetchManager()
    
    # Override tickers if specified
    if args.tickers:
        manager.universe_tickers = [t.strip().upper() for t in args.tickers.split(",")]
        logger.info(f"Using custom ticker list: {manager.universe_tickers}")
    
    # Log deployment info
    from utils.config import get_deployment_info
    deployment_info = get_deployment_info()
    
    # Execute based on job type
    success = False
    
    if args.job == "daily":
        logger.info(f"--- Running Daily Data Update --- {deployment_info}")
        success = manager.run_daily_updates()
    elif args.job == "intraday":
        if args.interval == "all":
            logger.info(f"--- Running All Intraday Updates --- {deployment_info}")
            success_1min = manager.run_intraday_updates("1min")
            success_30min = manager.run_intraday_updates("30min")
            success = success_1min or success_30min
        else:
            logger.info(f"--- Running {args.interval} Intraday Update --- {deployment_info}")
            success = manager.run_intraday_updates(args.interval)
    
    return success


if __name__ == "__main__":
    import sys
    import traceback
    
    try:
        success = main()
        exit(0 if success else 1)
    except Exception as e:
        logger.error(f"Fatal in data_fetch_manager: {e}")
        traceback.print_exc(file=sys.stderr)  # Ensure orchestrator shows it
        sys.exit(1)