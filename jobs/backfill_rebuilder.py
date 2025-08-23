"""Backfill and Data Rebuilder.

This module handles complete data reconstruction, gap filling, and recovery
operations for the trading data lake when self-healing is insufficient.
"""

import time
from datetime import datetime
from typing import List, Optional

from jobs.data_fetch_manager import DataFetchManager
from utils.config import config
from utils.logging_setup import get_logger
from utils.paths import daily_key, intraday_key
from utils.spaces_io import spaces_io

logger = get_logger(__name__)


class BackfillRebuilder:
    """Complete data rebuilder for recovery operations."""

    def __init__(self) -> None:
        """Initialize the backfill rebuilder."""
        self.data_manager = DataFetchManager()

    def rebuild_all_data(
        self,
        force_full: bool = True,
        skip_existing: bool = False,
    ) -> bool:
        """Rebuild all data from scratch.
        
        Args:
            force_full: Force full API calls for all data
            skip_existing: Skip tickers that already have recent data
            
        Returns:
            True if successful, False otherwise
        """
        logger.job_start("BackfillRebuilder.rebuild_all_data")
        start_time = time.time()

        try:
            # Rebuild daily data first
            daily_success = self.rebuild_daily_data(force_full, skip_existing)

            # Rebuild intraday data
            intraday_1min_success = self.rebuild_intraday_data("1min", force_full, skip_existing)
            intraday_30min_success = self.rebuild_intraday_data("30min", force_full, skip_existing)

            overall_success = daily_success and intraday_1min_success and intraday_30min_success

            duration = time.time() - start_time
            logger.job_complete(
                "BackfillRebuilder.rebuild_all_data",
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
                "BackfillRebuilder.rebuild_all_data",
                duration_seconds=duration,
                success=False,
                error=str(e),
            )
            return False

    def rebuild_daily_data(
        self,
        force_full: bool = True,
        skip_existing: bool = False,
    ) -> bool:
        """Rebuild daily data for all tickers.
        
        Args:
            force_full: Force full API calls
            skip_existing: Skip tickers with recent data
            
        Returns:
            True if successful, False otherwise
        """
        logger.info("Starting daily data rebuild")
        start_time = time.time()

        successful_tickers = 0
        skipped_tickers = 0

        for ticker in self.data_manager.universe_tickers:
            try:
                # Check if we should skip this ticker
                if skip_existing and self._has_recent_daily_data(ticker):
                    logger.debug(f"Skipping {ticker} - has recent daily data")
                    skipped_tickers += 1
                    continue

                # Force full mode if requested
                if force_full:
                    # Clear existing data to force full fetch
                    data_key = daily_key(ticker)
                    # Note: We don't actually delete, just let the manager detect and do full

                # Fetch data
                if self.data_manager.fetch_daily_data(ticker):
                    successful_tickers += 1
                    logger.info(f"✅ Rebuilt daily data for {ticker}")
                else:
                    logger.error(f"❌ Failed to rebuild daily data for {ticker}")

                # Longer pause for rebuilds to respect API limits
                time.sleep(1.0)

            except Exception as e:
                logger.error(f"Error rebuilding daily data for {ticker}: {e}")

        duration = time.time() - start_time
        total_processed = successful_tickers + len(self.data_manager.universe_tickers) - skipped_tickers
        success_rate = successful_tickers / total_processed if total_processed > 0 else 0

        logger.info(
            f"Daily rebuild completed in {duration:.1f}s",
            successful_tickers=successful_tickers,
            skipped_tickers=skipped_tickers,
            total_tickers=len(self.data_manager.universe_tickers),
            success_rate=success_rate,
        )

        return success_rate > 0.7  # Consider successful if >70% rebuilt

    def rebuild_intraday_data(
        self,
        interval: str,
        force_full: bool = True,
        skip_existing: bool = False,
    ) -> bool:
        """Rebuild intraday data for specified interval.
        
        Args:
            interval: Time interval (1min or 30min)
            force_full: Force full API calls
            skip_existing: Skip tickers with recent data
            
        Returns:
            True if successful, False otherwise
        """
        logger.info(f"Starting {interval} intraday data rebuild")
        start_time = time.time()

        successful_tickers = 0
        skipped_tickers = 0

        for ticker in self.data_manager.universe_tickers:
            try:
                # Check if we should skip this ticker
                if skip_existing and self._has_recent_intraday_data(ticker, interval):
                    logger.debug(f"Skipping {ticker} {interval} - has recent data")
                    skipped_tickers += 1
                    continue

                # Force full mode if requested
                if force_full:
                    # Clear existing data to force full fetch
                    data_key = intraday_key(ticker, interval)
                    # Note: We don't actually delete, just let the manager detect and do full

                # Fetch data
                if self.data_manager.fetch_intraday_data(ticker, interval):
                    successful_tickers += 1
                    logger.info(f"✅ Rebuilt {interval} data for {ticker}")
                else:
                    logger.error(f"❌ Failed to rebuild {interval} data for {ticker}")

                # Longer pause for rebuilds
                time.sleep(0.5)

            except Exception as e:
                logger.error(f"Error rebuilding {interval} data for {ticker}: {e}")

        duration = time.time() - start_time
        total_processed = successful_tickers + len(self.data_manager.universe_tickers) - skipped_tickers
        success_rate = successful_tickers / total_processed if total_processed > 0 else 0

        logger.info(
            f"{interval} rebuild completed in {duration:.1f}s",
            successful_tickers=successful_tickers,
            skipped_tickers=skipped_tickers,
            total_tickers=len(self.data_manager.universe_tickers),
            success_rate=success_rate,
        )

        return success_rate > 0.7

    def _has_recent_daily_data(self, ticker: str, max_days_old: int = 3) -> bool:
        """Check if ticker has recent daily data."""
        try:
            data_key = daily_key(ticker)
            df = spaces_io.download_dataframe(data_key)

            if df is None or df.empty:
                return False

            # Check if data is recent
            latest_date = df["date"].max()
            if isinstance(latest_date, str):
                latest_date = datetime.strptime(latest_date, "%Y-%m-%d").date()

            days_old = (datetime.now().date() - latest_date).days
            return days_old <= max_days_old

        except Exception:
            return False

    def _has_recent_intraday_data(self, ticker: str, interval: str, max_days_old: int = 1) -> bool:
        """Check if ticker has recent intraday data."""
        try:
            data_key = intraday_key(ticker, interval)

            df = spaces_io.download_dataframe(data_key)

            if df is None or df.empty:
                return False

            # Check if data is recent
            import pandas as pd
            latest_timestamp = pd.to_datetime(df["timestamp"].max())
            hours_old = (datetime.now() - latest_timestamp.to_pydatetime()).total_seconds() / 3600

            return hours_old <= (max_days_old * 24)

        except Exception:
            return False

    def rebuild_specific_tickers(
        self,
        tickers: List[str],
        intervals: Optional[List[str]] = None,
        force_full: bool = True,
    ) -> bool:
        """Rebuild data for specific tickers.
        
        Args:
            tickers: List of ticker symbols
            intervals: List of intervals to rebuild (default: all)
            force_full: Force full API calls
            
        Returns:
            True if successful, False otherwise
        """
        if intervals is None:
            intervals = ["daily", "1min", "30min"]

        logger.info(f"Rebuilding data for {len(tickers)} tickers: {tickers}")

        # Override universe temporarily
        original_tickers = self.data_manager.universe_tickers
        self.data_manager.universe_tickers = tickers

        try:
            success = True

            for interval in intervals:
                if interval == "daily":
                    result = self.rebuild_daily_data(force_full, skip_existing=False)
                elif interval in ["1min", "30min"]:
                    result = self.rebuild_intraday_data(interval, force_full, skip_existing=False)
                else:
                    logger.error(f"Unknown interval: {interval}")
                    result = False

                success = success and result

            return success

        finally:
            # Restore original universe
            self.data_manager.universe_tickers = original_tickers

    def verify_data_integrity(self) -> dict:
        """Verify data integrity across all tickers and intervals.
        
        Returns:
            Dictionary with integrity report
        """
        logger.info("Verifying data integrity")

        report = {
            "tickers_checked": 0,
            "daily_issues": [],
            "intraday_1min_issues": [],
            "intraday_30min_issues": [],
            "manifest_issues": [],
        }

        for ticker in self.data_manager.universe_tickers:
            report["tickers_checked"] += 1

            # Check daily data
            daily_issues = self._check_daily_integrity(ticker)
            if daily_issues:
                report["daily_issues"].extend(daily_issues)

            # Check 1min data
            intraday_1min_issues = self._check_intraday_integrity(ticker, "1min")
            if intraday_1min_issues:
                report["intraday_1min_issues"].extend(intraday_1min_issues)

            # Check 30min data
            intraday_30min_issues = self._check_intraday_integrity(ticker, "30min")
            if intraday_30min_issues:
                report["intraday_30min_issues"].extend(intraday_30min_issues)

        # Summary
        total_issues = (len(report["daily_issues"]) +
                       len(report["intraday_1min_issues"]) +
                       len(report["intraday_30min_issues"]) +
                       len(report["manifest_issues"]))

        report["total_issues"] = total_issues
        report["integrity_score"] = max(0, 100 - (total_issues * 2))  # Rough scoring

        logger.info(f"Integrity check complete: {total_issues} issues found, score: {report['integrity_score']}")

        return report

    def _check_daily_integrity(self, ticker: str) -> List[str]:
        """Check daily data integrity for a ticker."""
        issues = []

        try:
            data_key = daily_key(ticker)
            df = spaces_io.download_dataframe(data_key)

            if df is None or df.empty:
                issues.append(f"{ticker}: No daily data found")
                return issues

            # Check row count
            if len(df) < 50:
                issues.append(f"{ticker}: Insufficient daily data ({len(df)} rows)")

            # Check for missing required columns
            required_cols = ["date", "open", "high", "low", "close", "volume"]
            missing_cols = set(required_cols) - set(df.columns)
            if missing_cols:
                issues.append(f"{ticker}: Missing daily columns: {missing_cols}")

            # Check for recent data
            if not self._has_recent_daily_data(ticker, max_days_old=7):
                issues.append(f"{ticker}: Daily data is stale (>7 days old)")

        except Exception as e:
            issues.append(f"{ticker}: Error checking daily integrity: {e}")

        return issues

    def _check_intraday_integrity(self, ticker: str, interval: str) -> List[str]:
        """Check intraday data integrity for a ticker."""
        issues = []

        try:
            data_key = intraday_key(ticker, interval)
            min_rows = 1000 if interval == "1min" else 100

            df = spaces_io.download_dataframe(data_key)

            if df is None or df.empty:
                issues.append(f"{ticker}: No {interval} data found")
                return issues

            # Check row count
            if len(df) < min_rows:
                issues.append(f"{ticker}: Insufficient {interval} data ({len(df)} rows)")

            # Check for missing required columns
            required_cols = ["timestamp", "open", "high", "low", "close", "volume"]
            missing_cols = set(required_cols) - set(df.columns)
            if missing_cols:
                issues.append(f"{ticker}: Missing {interval} columns: {missing_cols}")

            # Check for recent data
            if not self._has_recent_intraday_data(ticker, interval, max_days_old=2):
                issues.append(f"{ticker}: {interval} data is stale (>2 days old)")

        except Exception as e:
            issues.append(f"{ticker}: Error checking {interval} integrity: {e}")

        return issues


def main():
    """Main entry point for backfill rebuilder."""
    import argparse

    parser = argparse.ArgumentParser(description="Backfill and Data Rebuilder")
    parser.add_argument(
        "--operation",
        choices=["rebuild-all", "rebuild-daily", "rebuild-intraday", "verify", "rebuild-tickers"],
        default="rebuild-all",
        help="Operation to perform",
    )
    parser.add_argument(
        "--interval",
        choices=["1min", "30min", "all"],
        help="Interval for intraday operations",
    )
    parser.add_argument(
        "--tickers",
        help="Comma-separated list of tickers for specific rebuilds",
    )
    parser.add_argument(
        "--force-full",
        action="store_true",
        default=True,
        help="Force full API calls (default: True)",
    )
    parser.add_argument(
        "--skip-existing",
        action="store_true",
        help="Skip tickers with recent data",
    )

    args = parser.parse_args()

    rebuilder = BackfillRebuilder()

    # Log deployment info
    from utils.config import get_deployment_info
    deployment_info = get_deployment_info()

    logger.info(f"--- Running Backfill Rebuilder: {args.operation} --- {deployment_info}")

    success = False

    if args.operation == "rebuild-all":
        success = rebuilder.rebuild_all_data(args.force_full, args.skip_existing)

    elif args.operation == "rebuild-daily":
        success = rebuilder.rebuild_daily_data(args.force_full, args.skip_existing)

    elif args.operation == "rebuild-intraday":
        if args.interval == "all":
            success_1min = rebuilder.rebuild_intraday_data("1min", args.force_full, args.skip_existing)
            success_30min = rebuilder.rebuild_intraday_data("30min", args.force_full, args.skip_existing)
            success = success_1min and success_30min
        elif args.interval:
            success = rebuilder.rebuild_intraday_data(args.interval, args.force_full, args.skip_existing)
        else:
            logger.error("Intraday rebuild requires --interval parameter")
            return False

    elif args.operation == "rebuild-tickers":
        if not args.tickers:
            logger.error("rebuild-tickers operation requires --tickers parameter")
            return False

        tickers = [t.strip().upper() for t in args.tickers.split(",")]
        intervals = None
        if args.interval and args.interval != "all":
            intervals = [args.interval]

        success = rebuilder.rebuild_specific_tickers(tickers, intervals, args.force_full)

    elif args.operation == "verify":
        report = rebuilder.verify_data_integrity()
        logger.info(f"Integrity report: {report}")
        success = report["total_issues"] == 0

    return success


if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)
