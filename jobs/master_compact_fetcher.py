#!/usr/bin/env python3
"""
Master Compact Fetcher - Unified Intelligent Data Fetching System
================================================================

This is the single, powerful script that handles all intraday data fetching needs
for the Tradingstation platform. It represents a complete consolidation and cleanup
of all previous fetching logic into one elegant, production-grade automation system.

CORE CAPABILITIES:
- 🏛️ Hardened market calendar checking using pandas_market_calendars  
- 🔍 Aggressive data validation to ensure today's data is present
- 🧠 Intelligent fetch strategy (full vs compact based on file size)
- 🕐 Universal handling of both 1min and 30min data intervals
- 🔄 Self-healing mechanism for incomplete data detection
- 🚀 Auto-trigger full fetch to rebuild incomplete historical data
- ☁️ Cloud storage gap detection for missing candles/timestamps
- 📅 Designed for scheduled execution (cron-compatible)
- 🛡️ Graceful API error handling with exponential backoff retry
- 📊 Robust logging for immediate alerting of persistent issues

USAGE:
    # Default 1-minute interval processing
    python3 jobs/master_compact_fetcher.py
    
    # 30-minute interval processing
    python3 jobs/master_compact_fetcher.py --interval 30min
    
    # Force full rebuild mode
    python3 jobs/master_compact_fetcher.py --force-full
    
    # Test mode (single ticker validation)
    python3 jobs/master_compact_fetcher.py --test AAPL

This script replaces:
- fetch_intraday_compact.py
- fetch_30min.py  
- All previous compact fetch test scripts
"""

import argparse
import logging
import os
import sys
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

import pandas as pd
import pytz

# Add project root to Python path for imports
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# Import core utilities
from utils.alpha_vantage_api import get_intraday_data
from utils.config import ALPHA_VANTAGE_API_KEY, SPACES_BUCKET_NAME
from utils.data_storage import save_df_to_s3
from utils.helpers import read_master_tickerlist, update_scheduler_status, save_df_to_s3
from utils.market_time import is_market_open_on_date, detect_market_session
from utils.spaces_manager import get_cloud_file_size_bytes
from utils.timestamp_standardizer import apply_timestamp_standardization_to_api_data

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("data/logs/master_compact_fetcher.log", mode="a")
    ]
)
logger = logging.getLogger(__name__)

# Configuration constants
FILE_SIZE_THRESHOLD_KB = 10  # Files <= 10KB trigger full fetch
MAX_RETRIES = 3
RETRY_DELAY = 2.0
DEFAULT_INTERVAL = "1min"


class MasterCompactFetcher:
    """
    The unified, intelligent data fetching system for Tradingstation.
    
    This class encapsulates all the logic needed for robust, self-healing
    data fetching with comprehensive error handling and validation.
    """
    
    def __init__(self, interval: str = DEFAULT_INTERVAL):
        """Initialize the master compact fetcher."""
        self.interval = interval
        self.ny_tz = pytz.timezone("America/New_York")
        self.today_et = datetime.now(self.ny_tz).date()
        self.session_stats = {
            "total_processed": 0,
            "successful_fetches": 0,
            "failed_fetches": 0,
            "full_fetches_triggered": 0,
            "compact_fetches": 0,
            "self_healing_actions": 0
        }
        
        # Validate interval
        if interval not in ["1min", "30min"]:
            raise ValueError(f"Unsupported interval: {interval}. Must be '1min' or '30min'")
            
        # Determine data paths based on interval
        self.data_dir = "data/intraday" if interval == "1min" else "data/30min"
        
        logger.info(f"🚀 Master Compact Fetcher initialized for {interval} interval")
        logger.info(f"📂 Data directory: {self.data_dir}")
        logger.info(f"📅 Today (ET): {self.today_et}")
    
    def is_market_open_now(self) -> bool:
        """
        Check if market is currently open using hardened market calendar.
        
        This is the central function that replaces all oversimplified weekend-only checks.
        Uses pandas_market_calendars for proper US stock market holiday handling.
        """
        try:
            current_time = datetime.now(self.ny_tz)
            
            # First check if it's a trading day (accounts for holidays)
            if not is_market_open_on_date(current_time):
                logger.info(f"📅 Market is closed today - not a trading day (weekend/holiday)")
                return False
            
            # Check current market session
            session = detect_market_session()
            logger.info(f"🕒 Current market session: {session}")
            
            # Consider market "open" during regular hours and extended hours for data collection
            return session in ["PRE-MARKET", "REGULAR", "AFTER-HOURS"]
            
        except Exception as e:
            logger.warning(f"⚠️ Market calendar check failed, falling back to session detection: {e}")
            session = detect_market_session()
            return session != "CLOSED"
    
    def get_intelligent_outputsize(self, ticker: str) -> str:
        """
        Determine whether to use 'full' or 'compact' fetch based on cloud file size.
        
        The 10KB rule:
        - Files ≤ 10KB: Trigger full historical fetch (incomplete data)
        - Files > 10KB: Use compact fetch for real-time updates
        """
        try:
            file_path = f"{self.data_dir}/{ticker}.csv"
            file_size_bytes = get_cloud_file_size_bytes(file_path)
            
            if file_size_bytes is None:
                logger.info(f"📄 {ticker}: No existing file found, using FULL fetch")
                return "full"
            
            file_size_kb = file_size_bytes / 1024
            logger.info(f"📊 {ticker}: Cloud file size: {file_size_kb:.1f} KB")
            
            if file_size_kb <= FILE_SIZE_THRESHOLD_KB:
                logger.info(f"🔄 {ticker}: File ≤ {FILE_SIZE_THRESHOLD_KB}KB, triggering FULL fetch")
                self.session_stats["full_fetches_triggered"] += 1
                return "full"
            else:
                logger.info(f"⚡ {ticker}: File > {FILE_SIZE_THRESHOLD_KB}KB, using COMPACT fetch")
                self.session_stats["compact_fetches"] += 1
                return "compact"
                
        except Exception as e:
            logger.warning(f"⚠️ {ticker}: Error checking file size, defaulting to FULL fetch: {e}")
            return "full"
    
    def validate_data_completeness(self, df: pd.DataFrame, ticker: str, outputsize: str) -> bool:
        """
        Aggressive data validation to ensure today's data is present during market hours.
        
        This addresses the core issue where compact fetches would return stale data
        without today's current candles.
        """
        if df.empty:
            logger.warning(f"❌ {ticker}: Empty dataset received")
            return False
        
        # Check for required columns
        if 'timestamp' not in df.columns:
            logger.warning(f"❌ {ticker}: No timestamp column found")
            return False
        
        try:
            # Convert timestamps to Eastern Time for validation
            df_copy = df.copy()
            df_copy['timestamp'] = pd.to_datetime(df_copy['timestamp'])
            
            # Handle timezone conversion
            if df_copy['timestamp'].dt.tz is None:
                df_et = df_copy['timestamp'].dt.tz_localize(self.ny_tz)
            else:
                df_et = df_copy['timestamp'].dt.tz_convert(self.ny_tz)
            
            # Count today's data
            today_data_count = (df_et.dt.date == self.today_et).sum()
            total_rows = len(df_copy)
            
            logger.info(f"📊 {ticker}: Data validation - Total: {total_rows}, Today: {today_data_count}")
            
            # During market hours, we MUST have today's data for compact fetches
            if self.is_market_open_now() and outputsize == "compact" and today_data_count == 0:
                logger.error(f"❌ {ticker}: CRITICAL - No today's data during market hours (compact fetch)")
                logger.error(f"   This indicates stale data - triggering self-healing")
                return False
            
            # For full fetches, we're more lenient but still check data freshness
            if today_data_count == 0:
                if not df_et.empty:
                    latest_date = df_et.max().date()
                    days_old = (self.today_et - latest_date).days
                    logger.warning(f"⚠️ {ticker}: No today's data, latest is {days_old} days old")
                    
                    # If data is more than 5 days old, consider it incomplete
                    if days_old > 5:
                        logger.warning(f"⚠️ {ticker}: Data is stale ({days_old} days), marking as incomplete")
                        return False
                        
            logger.info(f"✅ {ticker}: Data validation passed")
            return True
            
        except Exception as e:
            logger.error(f"❌ {ticker}: Error during data validation: {e}")
            return False
    
    def detect_data_gaps(self, df: pd.DataFrame, ticker: str) -> List[str]:
        """
        Detect gaps in timestamp data that indicate incomplete historical records.
        
        This is part of the self-healing mechanism to ensure data integrity.
        """
        gaps = []
        
        if df.empty or 'timestamp' not in df.columns:
            return gaps
            
        try:
            df_copy = df.copy()
            df_copy['timestamp'] = pd.to_datetime(df_copy['timestamp'])
            df_sorted = df_copy.sort_values('timestamp')
            
            # Check for significant gaps (more than expected for the interval)
            time_diffs = df_sorted['timestamp'].diff()
            
            if self.interval == "1min":
                expected_diff = timedelta(minutes=1)
                max_acceptable_gap = timedelta(hours=2)  # Allow for market breaks
            else:  # 30min
                expected_diff = timedelta(minutes=30)
                max_acceptable_gap = timedelta(hours=8)  # Allow for overnight gaps
            
            large_gaps = time_diffs[time_diffs > max_acceptable_gap]
            
            for idx, gap in large_gaps.items():
                gap_start = df_sorted.iloc[idx-1]['timestamp'] if idx > 0 else "Start"
                gap_end = df_sorted.iloc[idx]['timestamp']
                gaps.append(f"Gap: {gap_start} to {gap_end} ({gap})")
                
            if gaps:
                logger.warning(f"🕳️ {ticker}: Detected {len(gaps)} significant data gaps")
                for gap in gaps:
                    logger.warning(f"   {gap}")
                    
        except Exception as e:
            logger.warning(f"⚠️ {ticker}: Error detecting data gaps: {e}")
            
        return gaps
    
    def fetch_with_retry(self, ticker: str, outputsize: str) -> Optional[pd.DataFrame]:
        """
        Fetch data with exponential backoff retry mechanism.
        
        Implements graceful API error handling as specified in the requirements.
        """
        for attempt in range(MAX_RETRIES):
            try:
                logger.info(f"🔄 {ticker}: Fetch attempt {attempt + 1}/{MAX_RETRIES} ({outputsize})")
                
                # Use the existing robust API function
                df = get_intraday_data(ticker, interval=self.interval, outputsize=outputsize)
                
                if df is not None and not df.empty:
                    logger.info(f"✅ {ticker}: Successfully fetched {len(df)} rows")
                    return df
                else:
                    logger.warning(f"⚠️ {ticker}: API returned empty data on attempt {attempt + 1}")
                    
            except Exception as e:
                logger.error(f"❌ {ticker}: Fetch error on attempt {attempt + 1}: {e}")
                
            # Exponential backoff if not the last attempt
            if attempt < MAX_RETRIES - 1:
                wait_time = RETRY_DELAY * (2 ** attempt)
                logger.info(f"⏳ {ticker}: Waiting {wait_time:.1f}s before retry...")
                time.sleep(wait_time)
        
        logger.error(f"❌ {ticker}: All fetch attempts failed")
        return None
    
    def self_healing_fetch(self, ticker: str) -> bool:
        """
        Self-healing mechanism that automatically fixes incomplete data.
        
        When compact fetch fails validation, automatically trigger a full fetch
        to rebuild the historical data.
        """
        logger.warning(f"🔧 {ticker}: Initiating self-healing process")
        self.session_stats["self_healing_actions"] += 1
        
        # Force a full fetch to rebuild the dataset
        logger.info(f"🔄 {ticker}: Self-healing - forcing FULL fetch to rebuild data")
        df = self.fetch_with_retry(ticker, "full")
        
        if df is not None and not df.empty:
            # Enhanced logging: Self-healing data details
            self_heal_api_rows = len(df)
            logger.info(f"🔧 {ticker}: Self-healing API returned {self_heal_api_rows} candles")
            
            # Validate the full fetch results
            if self.validate_data_completeness(df, ticker, "full"):
                # Check for and log any data gaps
                gaps = self.detect_data_gaps(df, ticker)
                if gaps:
                    logger.warning(f"⚠️ {ticker}: Self-healing detected gaps but proceeding with save")
                
                # Save the rebuilt dataset
                file_path = f"{self.data_dir}/{ticker}.csv"
                success = save_df_to_s3(df, file_path)
                
                if success:
                    logger.info(f"✅ {ticker}: Self-healing successful - rebuilt dataset with {self_heal_api_rows} rows")
                    return True
                else:
                    logger.error(f"❌ {ticker}: Self-healing failed - could not save rebuilt data ({self_heal_api_rows} rows)")
            else:
                logger.error(f"❌ {ticker}: Self-healing failed - full fetch also incomplete ({self_heal_api_rows} rows)")
        else:
            logger.error(f"❌ {ticker}: Self-healing failed - full fetch returned no data")
            
        return False
    
    def process_ticker(self, ticker: str, force_full: bool = False) -> bool:
        """
        Process a single ticker with intelligent fetch strategy and self-healing.
        
        This is the core method that orchestrates all the logic for each ticker.
        """
        # Enhanced logging: Start timestamp for each ticker
        start_time = datetime.now()
        logger.info(f"\n{'='*60}")
        logger.info(f"🎯 Processing {ticker} ({self.interval} interval)")
        logger.info(f"⏰ Start time: {start_time.strftime('%H:%M:%S')}")
        logger.info(f"{'='*60}")
        
        self.session_stats["total_processed"] += 1
        
        try:
            # Enhanced logging: Determine and log fetch strategy
            if force_full:
                outputsize = "full"
                logger.info(f"🔧 {ticker}: FETCH MODE: FULL (force-full enabled)")
            else:
                outputsize = self.get_intelligent_outputsize(ticker)
                fetch_mode_reason = "full (small file or missing)" if outputsize == "full" else "compact (existing large file)"
                logger.info(f"📊 {ticker}: FETCH MODE: {outputsize.upper()} ({fetch_mode_reason})")
            
            # Attempt initial fetch with enhanced logging
            logger.info(f"🚀 {ticker}: Starting API fetch...")
            df = self.fetch_with_retry(ticker, outputsize)
            
            if df is None or df.empty:
                logger.error(f"❌ {ticker}: FAILED - No data received from API")
                self.session_stats["failed_fetches"] += 1
                self._log_ticker_completion(ticker, start_time, success=False, reason="No API data")
                return False
            
            # Enhanced logging: Log API data received
            api_row_count = len(df)
            logger.info(f"📥 {ticker}: API returned {api_row_count} candles")
            
            # Validate data completeness
            if not self.validate_data_completeness(df, ticker, outputsize):
                logger.warning(f"⚠️ {ticker}: Data validation failed")
                
                # Try self-healing if this was a compact fetch
                if outputsize == "compact":
                    logger.info(f"🔧 {ticker}: Attempting self-healing...")
                    if self.self_healing_fetch(ticker):
                        logger.info(f"✅ {ticker}: SUCCESS - Self-healing completed")
                        self.session_stats["successful_fetches"] += 1
                        self._log_ticker_completion(ticker, start_time, success=True, reason="Self-healing successful")
                        return True
                
                # If self-healing failed or this was already a full fetch, mark as failed
                logger.error(f"❌ {ticker}: FAILED - Data validation failed, self-healing not possible")
                self.session_stats["failed_fetches"] += 1
                self._log_ticker_completion(ticker, start_time, success=False, reason="Data validation failed")
                return False
            
            # Data passed validation, check for gaps
            gaps = self.detect_data_gaps(df, ticker)
            if gaps:
                logger.warning(f"⚠️ {ticker}: Data gaps detected but proceeding with save")
            
            # Enhanced logging: Pre-save data processing
            final_row_count = len(df)
            logger.info(f"💾 {ticker}: Saving {final_row_count} candles (API: {api_row_count} → Final: {final_row_count})")
            
            # Save the data
            file_path = f"{self.data_dir}/{ticker}.csv"
            success = save_df_to_s3(df, file_path)
            
            if success:
                logger.info(f"✅ {ticker}: SUCCESS - Processed and saved {final_row_count} rows")
                self.session_stats["successful_fetches"] += 1
                self._log_ticker_completion(ticker, start_time, success=True, 
                                          reason=f"{final_row_count} rows saved", 
                                          api_rows=api_row_count, final_rows=final_row_count)
                return True
            else:
                logger.error(f"❌ {ticker}: FAILED - Could not save data to storage")
                self.session_stats["failed_fetches"] += 1
                self._log_ticker_completion(ticker, start_time, success=False, reason="Save failed")
                return False
                
        except Exception as e:
            logger.error(f"❌ {ticker}: FAILED - Unexpected error during processing: {e}")
            import traceback
            logger.debug(f"Full traceback: {traceback.format_exc()}")
            self.session_stats["failed_fetches"] += 1
            self._log_ticker_completion(ticker, start_time, success=False, reason=f"Exception: {str(e)}")
            return False

    def _log_ticker_completion(self, ticker: str, start_time: datetime, success: bool, 
                             reason: str, api_rows: int = None, final_rows: int = None):
        """
        Log ticker completion with timing and status details.
        
        This function provides comprehensive logging for ticker processing completion,
        including timing information, success status, and data flow statistics.
        
        Args:
            ticker (str): The ticker symbol that was processed
            start_time (datetime): The timestamp when processing started for this ticker
            success (bool): Whether the ticker processing completed successfully
            reason (str): Detailed explanation of the completion status or failure reason
            api_rows (int, optional): Number of rows received from the API. Defaults to None.
            final_rows (int, optional): Number of rows in the final processed dataset. Defaults to None.
        """
        end_time = datetime.now()
        duration = end_time - start_time
        status = "SUCCESS" if success else "FAILED"
        
        logger.info(f"🏁 {ticker}: {status}")
        logger.info(f"⏱️ {ticker}: Duration: {duration.total_seconds():.2f}s")
        logger.info(f"📋 {ticker}: Reason: {reason}")
        if api_rows is not None and final_rows is not None:
            logger.info(f"📊 {ticker}: Data flow: API={api_rows} → Final={final_rows}")
        logger.info(f"{'='*60}")
    
    def run_batch_processing(self, tickers: List[str], force_full: bool = False) -> Dict:
        """
        Run batch processing for multiple tickers with comprehensive statistics.
        """
        start_time = datetime.now()
        logger.info(f"\n{'='*80}")
        logger.info(f"🚀 Master Compact Fetcher - Batch Processing Started")
        logger.info(f"⏰ Start time: {start_time}")
        logger.info(f"📋 Tickers to process: {len(tickers)}")
        logger.info(f"📊 Interval: {self.interval}")
        logger.info(f"🔧 Force full mode: {force_full}")
        logger.info(f"📅 Market open today: {is_market_open_on_date()}")
        logger.info(f"🕒 Current session: {detect_market_session()}")
        logger.info(f"{'='*80}")
        
        # Process each ticker
        for i, ticker in enumerate(tickers, 1):
            logger.info(f"\n📍 Progress: {i}/{len(tickers)} ({i/len(tickers)*100:.1f}%)")
            self.process_ticker(ticker, force_full)
            
            # Small delay between tickers to be respectful to the API
            if i < len(tickers):
                time.sleep(0.5)
        
        # Final statistics
        end_time = datetime.now()
        duration = end_time - start_time
        
        logger.info(f"\n{'='*80}")
        logger.info(f"📊 MASTER COMPACT FETCHER - BATCH COMPLETE")
        logger.info(f"{'='*80}")
        logger.info(f"⏰ Duration: {duration}")
        logger.info(f"📋 Total processed: {self.session_stats['total_processed']}")
        logger.info(f"✅ Successful: {self.session_stats['successful_fetches']}")
        logger.info(f"❌ Failed: {self.session_stats['failed_fetches']}")
        logger.info(f"🔄 Full fetches triggered: {self.session_stats['full_fetches_triggered']}")
        logger.info(f"⚡ Compact fetches: {self.session_stats['compact_fetches']}")
        logger.info(f"🔧 Self-healing actions: {self.session_stats['self_healing_actions']}")
        
        success_rate = (self.session_stats['successful_fetches'] / 
                       self.session_stats['total_processed'] * 100) if self.session_stats['total_processed'] > 0 else 0
        logger.info(f"📈 Success rate: {success_rate:.1f}%")
        logger.info(f"{'='*80}")
        
        # Update scheduler status
        try:
            status_data = {
                'job_name': f'master_compact_fetcher_{self.interval}',
                'last_run': end_time.isoformat(),
                'status': 'SUCCESS' if success_rate >= 80 else 'PARTIAL',
                'tickers_processed': self.session_stats['total_processed'],
                'success_rate': success_rate,
                'duration_minutes': duration.total_seconds() / 60
            }
            update_scheduler_status(**status_data)
        except Exception as e:
            logger.warning(f"⚠️ Failed to update scheduler status: {e}")
        
        return self.session_stats


def main():
    """Main entry point for the Master Compact Fetcher."""
    parser = argparse.ArgumentParser(
        description="Master Compact Fetcher - Unified Intelligent Data Fetching System",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python3 jobs/master_compact_fetcher.py                    # Default 1min processing
  python3 jobs/master_compact_fetcher.py --interval 30min   # 30min processing  
  python3 jobs/master_compact_fetcher.py --force-full       # Force full rebuild
  python3 jobs/master_compact_fetcher.py --test AAPL        # Test single ticker
        """
    )
    
    parser.add_argument(
        "--interval", 
        choices=["1min", "30min"], 
        default=DEFAULT_INTERVAL,
        help="Data interval to process (default: 1min)"
    )
    
    parser.add_argument(
        "--force-full", 
        action="store_true",
        help="Force full fetch for all tickers (ignore file size logic)"
    )
    
    parser.add_argument(
        "--test", 
        type=str, 
        metavar="TICKER",
        help="Test mode - process only the specified ticker"
    )
    
    args = parser.parse_args()
    
    # Validate API key
    if not ALPHA_VANTAGE_API_KEY:
        logger.error("❌ ALPHA_VANTAGE_API_KEY environment variable not set")
        logger.error("   Please set your Alpha Vantage API key before running")
        sys.exit(1)
    
    # Initialize the fetcher
    try:
        fetcher = MasterCompactFetcher(interval=args.interval)
    except ValueError as e:
        logger.error(f"❌ Configuration error: {e}")
        sys.exit(1)
    
    # Determine tickers to process
    if args.test:
        tickers = [args.test.upper()]
        logger.info(f"🧪 Test mode: Processing single ticker {args.test}")
    else:
        try:
            tickers = read_master_tickerlist()
            if not tickers:
                logger.error("❌ No tickers found in master_tickerlist.csv")
                sys.exit(1)
        except Exception as e:
            logger.error(f"❌ Failed to read master tickerlist: {e}")
            sys.exit(1)
    
    # Run the batch processing
    try:
        stats = fetcher.run_batch_processing(tickers, force_full=args.force_full)
        
        # Exit code based on success rate
        success_rate = (stats['successful_fetches'] / stats['total_processed'] * 100) if stats['total_processed'] > 0 else 0
        
        if success_rate >= 80:
            logger.info("🎉 Batch processing completed successfully")
            sys.exit(0)
        elif success_rate >= 50:
            logger.warning("⚠️ Batch processing completed with some failures")  
            sys.exit(1)
        else:
            logger.error("❌ Batch processing failed - too many errors")
            sys.exit(2)
            
    except KeyboardInterrupt:
        logger.info("⚠️ Processing interrupted by user")
        sys.exit(130)
    except Exception as e:
        logger.error(f"❌ Fatal error during batch processing: {e}")
        import traceback
        logger.debug(f"Full traceback: {traceback.format_exc()}")
        sys.exit(1)


if __name__ == "__main__":
    main()