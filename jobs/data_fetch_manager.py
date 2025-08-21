#!/usr/bin/env python3
"""
Unified DataFetchManager - The Single Authority for Market Data
============================================================

This script consolidates ALL data fetching logic into a single, intelligent, 
self-healing Python script that replaces all previous data fetching implementations.

This is the sole authority for fetching, cleaning, and managing financial market data
stored as CSV files in DigitalOcean Spaces cloud storage.

Key Features:
- Single source of truth using master_tickerlist.csv from cloud storage
- Intelligent fetch strategy (full vs compact based on file state)
- Self-healing mechanism with gap detection and auto-remediation
- Maintains three data types: Daily (200 rows), 30-min (500 rows), 1-min (7 days)
- Professional error handling with exponential backoff
- Comprehensive logging for production monitoring
"""

import io
import logging
import os
import sys
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

import pandas as pd
import pytz

# Add project root to Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# Import core utilities
from utils.alpha_vantage_api import get_daily_data, get_intraday_data, get_real_time_price
from utils.config import (
    ALPHA_VANTAGE_API_KEY,
    SPACES_ACCESS_KEY_ID,
    SPACES_SECRET_ACCESS_KEY, 
    SPACES_BUCKET_NAME,
    SPACES_REGION,
    TIMEZONE,
    INTRADAY_1MIN_COMPACT_COUNTBACK,
    INTRADAY_1MIN_HEAL_EVERY_MINUTES,
    INTRADAY_1MIN_HEAL_COUNTBACK,
    INTRADAY_EXTENDED
)
from utils.spaces_manager import (
    get_spaces_credentials_status,
    get_spaces_client,
    download_dataframe,
    upload_dataframe
)

# Setup comprehensive logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def get_deployment_info():
    """Get deployment version information for debugging silent failures."""
    try:
        import subprocess
        # Get git commit hash
        commit_hash = subprocess.check_output(
            ["git", "rev-parse", "HEAD"], 
            cwd=os.path.dirname(__file__)
        ).decode().strip()[:8]
        
        # Get current timestamp
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S UTC")
        
        return f"[DEPLOYMENT v{commit_hash} @ {timestamp}]"
    except Exception:
        return "[DEPLOYMENT unknown]"

# Print enhanced version info immediately when script starts
try:
    deployment_info = get_deployment_info()
    print(f"--- DATA FETCH MANAGER VERSION 2.0 RUNNING {deployment_info} ---")
except Exception:
    print("--- DATA FETCH MANAGER VERSION 2.0 RUNNING ---")


class DataFetchManager:
    """
    Unified Data Fetch Manager - The single authority for all market data operations.
    
    Implements intelligent fetch strategy, self-healing data integrity, and
    complete automation of the data pipeline from API to cloud storage.
    """
    
    def __init__(self):
        """Initialize the DataFetchManager with all required configurations."""
        # Log deployment version for debugging silent failures
        deployment_info = get_deployment_info()
        logger.info(f"🚀 DataFetchManager {deployment_info} - Initialization Starting")
        
        self.ny_tz = pytz.timezone("America/New_York")
        self.utc_tz = pytz.timezone("UTC")
        
        # Data retention specifications
        self.DAILY_ROWS = 200
        self.INTRADAY_30MIN_ROWS = 500
        self.INTRADAY_1MIN_DAYS = 7
        
        # File size threshold for decision logic (10KB)
        self.FILE_SIZE_THRESHOLD = 10 * 1024
        
        # Gap detection thresholds (in minutes)
        self.GAP_THRESHOLD_1MIN = 5  # minutes
        self.GAP_THRESHOLD_30MIN = 35  # minutes
        
        self.master_tickers = []
        self.spaces_client = None
        
        # Validate credentials
        self._validate_credentials()
        
    def _validate_credentials(self):
        """Validate all required credentials and log status."""
        logger.info("🔍 DataFetchManager Initialization - Validating Credentials")
        
        # Define required environment variables
        required_env_vars = [
            'SPACES_ACCESS_KEY_ID',
            'SPACES_SECRET_ACCESS_KEY', 
            'SPACES_BUCKET_NAME',
            'SPACES_REGION',
            'ALPHA_VANTAGE_API_KEY'
        ]
        
        missing_vars = []
        for var in required_env_vars:
            if not os.environ.get(var):
                missing_vars.append(var)
        
        if missing_vars:
            logger.error(f"❌ Missing required environment variables: {', '.join(missing_vars)}")
            logger.error("Required environment variables for full functionality:")
            for var in missing_vars:
                logger.error(f"   {var}: ❌ Missing")
            # Note: We don't exit here anymore, we continue with degraded functionality
        
        # Check Alpha Vantage API Key
        if not ALPHA_VANTAGE_API_KEY:
            logger.error("❌ ALPHA_VANTAGE_API_KEY not found - API calls will fail")
        else:
            logger.info("✅ Alpha Vantage API key configured")
            
        # Check Spaces credentials
        spaces_status = get_spaces_credentials_status()
        if spaces_status["all_present"]:
            logger.info("✅ All DigitalOcean Spaces credentials configured")
            self.spaces_client = get_spaces_client()
        else:
            logger.error(f"❌ Missing Spaces credentials: {spaces_status['missing']}")
            for var, status in spaces_status["status_details"].items():
                logger.info(f"   {var}: {status}")
                
    def download_master_tickerlist(self) -> bool:
        """
        Download and read master_tickerlist.csv from Spaces cloud storage.
        This is the single source of truth for all tickers to process.
        With fallback to default tickers if cloud storage is unavailable.
        
        Returns:
            bool: Success status
        """
        logger.info("📥 Downloading master_tickerlist.csv from Spaces cloud storage")
        
        if not self.spaces_client:
            logger.warning("⚠️ No Spaces client available - using fallback ticker list")
            # Fallback to default tickers as specified in problem statement
            fallback_tickers = ["NVDA", "AAPL", "TSLA"]
            logger.warning(f"🔄 Using fallback tickers: {fallback_tickers}")
            self.master_tickers = fallback_tickers
            return True
            
        try:
            # Download master_tickerlist.csv from root of bucket
            df = download_dataframe("master_tickerlist.csv")
            
            if df is None or df.empty:
                logger.critical("❌ CRITICAL: master_tickerlist.csv is empty or not found")
                return False
                
            # Extract ticker column
            if 'Symbol' in df.columns:
                self.master_tickers = df['Symbol'].dropna().unique().tolist()
            elif 'symbol' in df.columns:
                self.master_tickers = df['symbol'].dropna().unique().tolist()
            elif 'ticker' in df.columns:
                self.master_tickers = df['ticker'].dropna().unique().tolist()
            else:
                # Try first column
                self.master_tickers = df.iloc[:, 0].dropna().unique().tolist()
                
            logger.info(f"✅ Successfully loaded {len(self.master_tickers)} tickers from master list")
            logger.info(f"📋 Sample tickers: {self.master_tickers[:5]}")
            return True
            
        except Exception as e:
            logger.critical(f"❌ CRITICAL ERROR downloading master_tickerlist.csv: {e}")
            return False
            
    def check_cloud_file_state(self, ticker: str, directory: str) -> Tuple[bool, int]:
        """
        Check if file exists in cloud and get its size for decision logic.
        
        Args:
            ticker: Stock ticker symbol
            directory: Cloud directory (daily/, intraday_1min/, intraday_30min/)
            
        Returns:
            Tuple of (file_exists, file_size_bytes)
        """
        if not self.spaces_client:
            return False, 0
            
        try:
            file_key = f"{directory}/{ticker}.csv"
            
            response = self.spaces_client.head_object(
                Bucket=SPACES_BUCKET_NAME,
                Key=file_key
            )
            
            file_size = response.get('ContentLength', 0)
            logger.info(f"📁 {ticker} ({directory}): File exists, size: {file_size} bytes")
            return True, file_size
            
        except Exception:
            logger.info(f"📁 {ticker} ({directory}): File does not exist")
            return False, 0
            
    def fetch_daily_data(self, ticker: str) -> bool:
        """
        Fetch and process daily data for a ticker.
        
        Strategy: Always perform full fetch + real-time update for daily data.
        
        Args:
            ticker: Stock ticker symbol
            
        Returns:
            bool: Success status
        """
        logger.info(f"📈 Processing daily data for {ticker}")
        
        try:
            # Step 1: Full historical fetch
            df_daily = get_daily_data(ticker, outputsize='full')
            if df_daily is None or df_daily.empty:
                logger.error(f"❌ {ticker}: Failed to fetch daily data")
                return False
                
            # Step 2: Trim to 200 most recent rows
            df_daily = df_daily.tail(self.DAILY_ROWS)
            
            # Step 3: Get real-time quote for current day
            real_time_data = get_real_time_price(ticker)
            if real_time_data:
                # Update or append latest row with real-time data
                today = datetime.now(self.ny_tz).date()
                today_str = today.strftime('%Y-%m-%d')
                
                # Create or update today's row
                if today_str in df_daily.index:
                    # Update existing row
                    df_daily.loc[today_str, 'close'] = real_time_data.get('price', df_daily.loc[today_str, 'close'])
                else:
                    # Append new row for today
                    new_row = {
                        'open': real_time_data.get('price'),
                        'high': real_time_data.get('price'),
                        'low': real_time_data.get('price'),
                        'close': real_time_data.get('price'),
                        'volume': 0
                    }
                    df_daily.loc[today_str] = new_row
                    
            # Step 4: Save to cloud
            success = upload_dataframe(
                df_daily,
                f"daily/{ticker}.csv"
            )
            
            if success:
                logger.info(f"✅ {ticker}: Daily data saved ({len(df_daily)} rows)")
                return True
            else:
                logger.error(f"❌ {ticker}: Failed to save daily data")
                return False
                
        except Exception as e:
            logger.error(f"❌ {ticker}: Error processing daily data: {e}")
            return False
            
    def fetch_intraday_data(self, ticker: str, interval: str) -> bool:
        """
        Fetch and process intraday data with intelligent strategy.
        
        Phase 2.1 Runtime Tuning Implementation:
        - For 1min interval: Uses compact fetch + periodic healing strategy
        - For 30min interval: Uses existing logic (unchanged)
        
        Args:
            ticker: Stock ticker symbol
            interval: '1min' or '30min'
            
        Returns:
            bool: Success status
        """
        # Phase 2.1: Special handling for 1-minute intervals
        if interval == '1min':
            return self._fetch_1min_intraday_data(ticker)
        else:
            # Keep existing logic for 30min unchanged
            return self._fetch_30min_intraday_data(ticker, interval)
    
    def _fetch_1min_intraday_data(self, ticker: str) -> bool:
        """
        Phase 2.1: Optimized 1-minute intraday data fetch with compact + healing strategy.
        
        Strategy:
        - Default: Compact fetch with configurable countback
        - Periodic healing: Every N minutes, do a heal fetch
        - Enhanced logging with all metrics
        """
        start_time = time.time()
        interval = '1min'
        directory = "intraday_1min"
        
        try:
            # Step 1: Determine fetch mode (compact or heal)
            now_minute = int(datetime.utcnow().strftime("%M"))
            is_heal_cycle = (now_minute % INTRADAY_1MIN_HEAL_EVERY_MINUTES == 0)
            
            if is_heal_cycle:
                mode = "heal"
                countback = INTRADAY_1MIN_HEAL_COUNTBACK
                outputsize = 'full'  # Use full to get enough data, then trim
                logger.info(f"🔧 {ticker} ({interval}): HEAL FETCH - every {INTRADAY_1MIN_HEAL_EVERY_MINUTES} min cycle")
            else:
                mode = "compact" 
                countback = INTRADAY_1MIN_COMPACT_COUNTBACK
                outputsize = 'compact'  # Alpha Vantage compact gives ~100 bars (~180 min)
                logger.info(f"⚡ {ticker} ({interval}): COMPACT FETCH - regular update")

            # Step 2: Check if we have existing data for merge logic
            file_exists, file_size = self.check_cloud_file_state(ticker, directory)
            existing_df = None
            existing_max_timestamp = None
            
            if file_exists:
                try:
                    existing_df = download_dataframe(f"{directory}/{ticker}.csv")
                    if not existing_df.empty:
                        existing_df['timestamp'] = pd.to_datetime(existing_df['timestamp'])
                        existing_max_timestamp = existing_df['timestamp'].max()
                except Exception as e:
                    logger.warning(f"⚠️ {ticker}: Could not load existing data: {e}")
            
            # Step 3: Fetch new data
            new_df = get_intraday_data(ticker, interval=interval, outputsize=outputsize)
            if new_df is None or new_df.empty:
                logger.error(f"❌ {ticker} ({interval}): Failed to fetch intraday data")
                return False
            
            # Step 4: Process new data and apply countback limit
            new_df['timestamp'] = pd.to_datetime(new_df['timestamp'])
            new_df = new_df.sort_values('timestamp')
            
            # For heal mode, limit to countback rows to avoid excessive data
            if mode == "heal" and len(new_df) > countback:
                new_df = new_df.tail(countback)
            
            # Step 5: Apply merge rule - append only rows with new.timestamp > existing_max
            appended_count = 0
            if existing_df is not None and not existing_df.empty and existing_max_timestamp is not None:
                # Filter new data to only include timestamps newer than existing max
                new_rows = new_df[new_df['timestamp'] > existing_max_timestamp].copy()
                appended_count = len(new_rows)
                
                if appended_count > 0:
                    # Combine DataFrames - existing + new rows only
                    combined_df = pd.concat([existing_df, new_rows], ignore_index=True)
                    combined_df = combined_df.sort_values('timestamp')
                    # Remove any potential duplicates, keeping last
                    combined_df = combined_df.drop_duplicates(subset=['timestamp'], keep='last')
                else:
                    # No new data to append
                    combined_df = existing_df.copy()
                    logger.info(f"📊 {ticker} ({mode}): No new timestamps to append")
            else:
                # No existing data, use all new data
                combined_df = new_df.copy()
                appended_count = len(combined_df)
            
            # Step 6: Apply pruning - keep rows with timestamp >= now_utc - 8d (7d + today)
            now_utc = datetime.utcnow()
            cutoff_date = now_utc - timedelta(days=8)  # 8 days to ensure 7 days + today coverage
            
            pre_prune_count = len(combined_df)
            combined_df = combined_df[combined_df['timestamp'] >= cutoff_date]
            pruned_count = pre_prune_count - len(combined_df)
            
            # Step 7: Calculate metrics
            elapsed_ms = int((time.time() - start_time) * 1000)
            final_rows = len(combined_df)
            latest_ts_utc = combined_df['timestamp'].max().isoformat() if not combined_df.empty else None
            
            # Step 8: Save to cloud  
            success = upload_dataframe(combined_df, f"{directory}/{ticker}.csv")
            
            if success:
                # Enhanced logging as specified
                logger.info(
                    f"✅ Update 1min Intraday Data completed in {elapsed_ms/1000:.1f}s "
                    f"provider=marketdata mode={mode} countback={countback} "
                    f"appended={appended_count} final_rows={final_rows} "
                    f"latest_ts_utc={latest_ts_utc} elapsed_ms={elapsed_ms}"
                )
                return True
            else:
                logger.error(f"❌ {ticker} ({interval}): Failed to save intraday data")
                return False
                
        except Exception as e:
            elapsed_ms = int((time.time() - start_time) * 1000)
            logger.error(f"❌ {ticker} ({interval}): Error processing intraday data: {e} elapsed_ms={elapsed_ms}")
            return False
    
    def _fetch_30min_intraday_data(self, ticker: str, interval: str) -> bool:
        """
        30-minute intraday data fetch - UNCHANGED from original implementation.
        
        Phase 2.1: Keep existing 30-min logic exactly as is.
        """
        directory = f"intraday_{interval}"
        logger.info(f"📊 Processing {interval} intraday data for {ticker}")
        
        try:
            # Step 1: Check cloud state
            file_exists, file_size = self.check_cloud_file_state(ticker, directory)
            
            # Step 2: Decision point
            if not file_exists or file_size < self.FILE_SIZE_THRESHOLD:
                logger.info(f"🔄 {ticker} ({interval}): Triggering FULL FETCH (recovery/initial)")
                outputsize = 'full'
            else:
                logger.info(f"⚡ {ticker} ({interval}): Triggering COMPACT FETCH (standard update)")
                outputsize = 'compact'
                
            # Step 3: Fetch new data
            new_df = get_intraday_data(ticker, interval=interval, outputsize=outputsize)
            if new_df is None or new_df.empty:
                logger.error(f"❌ {ticker} ({interval}): Failed to fetch intraday data")
                return False
                
            # Step 4: Load existing data if it exists
            existing_df = None
            if file_exists:
                try:
                    existing_df = download_dataframe(
                        f"{directory}/{ticker}.csv"
                    )
                except Exception as e:
                    logger.warning(f"⚠️ {ticker} ({interval}): Could not load existing data: {e}")
                    
            # Step 5: Merge data
            if existing_df is not None and not existing_df.empty:
                # Combine DataFrames
                combined_df = pd.concat([existing_df, new_df], ignore_index=True)
                
                # Sort by timestamp
                combined_df['timestamp'] = pd.to_datetime(combined_df['timestamp'])
                combined_df = combined_df.sort_values('timestamp')
                
                # Remove duplicates, keeping newest
                combined_df = combined_df.drop_duplicates(subset=['timestamp'], keep='last')
            else:
                combined_df = new_df.copy()
                combined_df['timestamp'] = pd.to_datetime(combined_df['timestamp'])
                combined_df = combined_df.sort_values('timestamp')
                
            # Step 6: Trim to specification - 30min keeps last 500 rows
            combined_df = combined_df.tail(self.INTRADAY_30MIN_ROWS)
                
            # Step 7: Self-healing gap detection (existing logic)
            gap_detected = self._detect_gaps(combined_df, interval, ticker)
            if gap_detected:
                logger.warning(f"⚠️ {ticker} ({interval}): Data gap detected - triggering auto-remediation")
                # Trigger full fetch to fix gaps
                remediation_df = get_intraday_data(ticker, interval=interval, outputsize='full')
                if remediation_df is not None and not remediation_df.empty:
                    combined_df = remediation_df.copy()
                    combined_df['timestamp'] = pd.to_datetime(combined_df['timestamp'])
                    combined_df = combined_df.sort_values('timestamp')
                    
                    # Re-apply trimming after remediation
                    combined_df = combined_df.tail(self.INTRADAY_30MIN_ROWS)
                        
                    logger.info(f"✅ {ticker} ({interval}): Auto-remediation completed")
                else:
                    logger.error(f"❌ {ticker} ({interval}): Auto-remediation failed")
                    
            # Step 8: Save to cloud
            success = upload_dataframe(
                combined_df,
                f"{directory}/{ticker}.csv"
            )
            
            if success:
                logger.info(f"✅ {ticker} ({interval}): Intraday data saved ({len(combined_df)} rows)")
                return True
            else:
                logger.error(f"❌ {ticker} ({interval}): Failed to save intraday data")
                return False
                
        except Exception as e:
            logger.error(f"❌ {ticker} ({interval}): Error processing intraday data: {e}")
            return False
            
    def _detect_gaps(self, df: pd.DataFrame, interval: str, ticker: str) -> bool:
        """
        Detect gaps in timestamp data for self-healing mechanism.
        
        Args:
            df: DataFrame with timestamp column
            interval: '1min' or '30min'
            ticker: Stock ticker symbol for logging
            
        Returns:
            bool: True if gap detected
        """
        if df.empty or len(df) < 2:
            return False
            
        try:
            # Ensure timestamp column is datetime
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            
            # Calculate time differences between consecutive rows
            time_diffs = df['timestamp'].diff().dt.total_seconds() / 60  # Convert to minutes
            
            # Set gap threshold based on interval
            if interval == '1min':
                threshold = self.GAP_THRESHOLD_1MIN
            else:  # 30min
                threshold = self.GAP_THRESHOLD_30MIN
                
            # Check for gaps during market hours
            gaps = time_diffs > threshold
            
            if gaps.any():
                gap_indices = gaps[gaps].index.tolist()
                for idx in gap_indices:
                    if idx > 0:
                        gap_start = df.iloc[idx-1]['timestamp']
                        gap_end = df.iloc[idx]['timestamp']
                        gap_duration = time_diffs.iloc[idx]
                        
                        logger.warning(
                            f"⚠️ GAP DETECTED: {ticker} {interval} between "
                            f"{gap_start} and {gap_end} ({gap_duration:.1f} minutes)"
                        )
                        
                return True
                
            return False
            
        except Exception as e:
            logger.error(f"❌ Error detecting gaps for {ticker} ({interval}): {e}")
            return False
            
    def process_all_tickers(self) -> Dict[str, Dict[str, bool]]:
        """
        Process all tickers from master list for all data types.
        
        Returns:
            Dict: Results summary with ticker -> {daily, 1min, 30min} -> bool
        """
        if not self.master_tickers:
            logger.error("❌ No tickers to process - master tickerlist not loaded")
            return {}
            
        logger.info(f"🚀 Starting complete data fetch for {len(self.master_tickers)} tickers")
        start_time = time.time()
        
        results = {}
        successful_tickers = 0
        
        for i, ticker in enumerate(self.master_tickers, 1):
            logger.info(f"📈 Processing ticker {i}/{len(self.master_tickers)}: {ticker}")
            
            ticker_results = {
                'daily': False,
                '1min': False,
                '30min': False
            }
            
            # Process daily data
            ticker_results['daily'] = self.fetch_daily_data(ticker)
            
            # Process 1-minute intraday data
            ticker_results['1min'] = self.fetch_intraday_data(ticker, '1min')
            
            # Process 30-minute intraday data
            ticker_results['30min'] = self.fetch_intraday_data(ticker, '30min')
            
            results[ticker] = ticker_results
            
            # Check if ticker was successful
            if any(ticker_results.values()):
                successful_tickers += 1
                
            # Brief pause between tickers to respect API limits
            time.sleep(0.2)
            
        # Final summary
        elapsed_time = time.time() - start_time
        logger.info(f"🏁 Data fetch completed in {elapsed_time:.1f} seconds")
        logger.info(f"📊 Successfully processed {successful_tickers}/{len(self.master_tickers)} tickers")
        
        return results
    
    def run_daily_updates(self) -> bool:
        """
        Run only daily data updates for all tickers.
        
        Returns:
            bool: Success status
        """
        if not self.master_tickers:
            logger.info("📥 Loading master tickerlist for daily updates")
            if not self.download_master_tickerlist():
                logger.error("❌ Failed to load master tickerlist")
                return False
            
        logger.info(f"🚀 Running DAILY updates for {len(self.master_tickers)} tickers")
        start_time = time.time()
        
        successful_tickers = 0
        for i, ticker in enumerate(self.master_tickers, 1):
            logger.info(f"📈 Processing daily data for ticker {i}/{len(self.master_tickers)}: {ticker}")
            
            if self.fetch_daily_data(ticker):
                successful_tickers += 1
                
            # Brief pause between tickers to respect API limits
            time.sleep(0.2)
            
        elapsed_time = time.time() - start_time
        logger.info(f"🏁 Daily updates completed in {elapsed_time:.1f} seconds")
        logger.info(f"📊 Successfully processed {successful_tickers}/{len(self.master_tickers)} tickers")
        
        return successful_tickers > 0
    
    def run_intraday_updates(self, interval: str) -> bool:
        """
        Run only intraday data updates for a specific interval.
        
        Args:
            interval: '1min' or '30min'
            
        Returns:
            bool: Success status
        """
        if interval not in ['1min', '30min']:
            logger.error(f"❌ Invalid interval: {interval}. Must be '1min' or '30min'")
            return False
            
        if not self.master_tickers:
            logger.info("📥 Loading master tickerlist for intraday updates")
            if not self.download_master_tickerlist():
                logger.error("❌ Failed to load master tickerlist")
                return False
            
        logger.info(f"🚀 Running {interval.upper()} intraday updates for {len(self.master_tickers)} tickers")
        start_time = time.time()
        
        successful_tickers = 0
        for i, ticker in enumerate(self.master_tickers, 1):
            logger.info(f"📊 Processing {interval} data for ticker {i}/{len(self.master_tickers)}: {ticker}")
            
            if self.fetch_intraday_data(ticker, interval):
                successful_tickers += 1
                
            # Brief pause between tickers to respect API limits
            time.sleep(0.2)
            
        elapsed_time = time.time() - start_time
        per_symbol_ms = int((elapsed_time * 1000) / len(self.master_tickers)) if self.master_tickers else 0
        
        # Enhanced logging for Phase 2.1
        logger.info(f"🏁 {interval.upper()} updates completed in {elapsed_time:.1f} seconds")
        logger.info(f"📊 Successfully processed {successful_tickers}/{len(self.master_tickers)} tickers")
        logger.info(f"⏱️ Performance: per_symbol_ms={per_symbol_ms}")
        
        return successful_tickers > 0
    
    def run_all_data_updates(self) -> bool:
        """
        Run complete data updates for all intervals (original behavior).
        
        Returns:
            bool: Success status
        """
        logger.info("🌟 Running COMPLETE data updates for all intervals")
        return self.run()
        
    def run(self):
        """
        Main execution method - The Grand Unified Data Fetch Process.
        """
        logger.info("🌟 DataFetchManager Started - The Unified Data Pipeline")
        logger.info("=" * 60)
        
        try:
            # Step 1: Download master tickerlist (single source of truth)
            if not self.download_master_tickerlist():
                logger.critical("❌ CRITICAL: Failed to download master tickerlist - EXITING")
                return False
                
            # Step 2: Process all tickers for all data types
            results = self.process_all_tickers()
            
            # Step 3: Generate final report
            self._generate_completion_report(results)
            
            logger.info("🎯 DataFetchManager Completed Successfully")
            return True
            
        except Exception as e:
            logger.critical(f"❌ CRITICAL ERROR in DataFetchManager: {e}")
            return False
        finally:
            logger.info("=" * 60)
            logger.info("🌟 DataFetchManager Session Ended")
            
    def _generate_completion_report(self, results: Dict[str, Dict[str, bool]]):
        """Generate a comprehensive completion report."""
        if not results:
            return
            
        total_tickers = len(results)
        daily_success = sum(1 for r in results.values() if r['daily'])
        min1_success = sum(1 for r in results.values() if r['1min'])
        min30_success = sum(1 for r in results.values() if r['30min'])
        
        logger.info("📊 COMPLETION REPORT")
        logger.info("-" * 40)
        logger.info(f"📈 Daily Data:     {daily_success}/{total_tickers} successful")
        logger.info(f"⚡ 1-Min Data:     {min1_success}/{total_tickers} successful")
        logger.info(f"📊 30-Min Data:    {min30_success}/{total_tickers} successful")
        logger.info("-" * 40)
        
        # List any failed tickers
        failed_tickers = []
        for ticker, ticker_results in results.items():
            if not any(ticker_results.values()):
                failed_tickers.append(ticker)
                
        if failed_tickers:
            logger.warning(f"⚠️ Failed tickers: {failed_tickers}")


if __name__ == "__main__":
    import argparse
    
    # Wrap main execution in try/catch for unhandled exceptions
    try:
        parser = argparse.ArgumentParser(description="Unified Data Fetch Manager for the Trading System.")
        
        # Canonical interface arguments
        parser.add_argument(
            '--job',
            type=str,
            choices=['intraday', 'daily'],
            help="Job type: 'intraday' for intraday data, 'daily' for daily data"
        )
        parser.add_argument(
            '--interval',
            type=str,
            choices=['1min', '30min', 'all', 'daily'],
            help="For intraday jobs: '1min', '30min', or 'all'. For daily jobs: 'daily' (or omit)"
        )
        parser.add_argument(
            '--tickers',
            type=str,
            help="Comma-separated list of tickers to process (optional, defaults to master tickerlist)"
        )
        parser.add_argument(
            '--force-full',
            action='store_true',
            help="Force full fetch instead of incremental update"
        )
        parser.add_argument(
            '--test-mode',
            action='store_true',
            help="Run in test mode (simulation)"
        )
        parser.add_argument(
            '--verbose',
            action='store_true',
            help="Enable verbose logging"
        )
        
        args = parser.parse_args()
        
        # Handle backward compatibility and canonical interface validation
        if args.job:
            # Canonical interface
            if args.job == 'intraday':
                if not args.interval or args.interval not in ['1min', '30min', 'all']:
                    parser.error("For --job intraday, --interval must be '1min', '30min', or 'all'")
            elif args.job == 'daily':
                if args.interval and args.interval != 'daily':
                    parser.error("For --job daily, --interval should be 'daily' or omitted")
        elif args.interval:
            # Backward compatibility shim - deprecated but supported
            if args.interval in ['1min', '30min']:
                print(f"⚠️ DEPRECATED: Use '--job intraday --interval {args.interval}' instead of '--interval {args.interval}'")
                args.job = 'intraday'
            elif args.interval == 'daily':
                print(f"⚠️ DEPRECATED: Use '--job daily' instead of '--interval daily'")
                args.job = 'daily'
        else:
            # No arguments - default behavior
            pass

        # Instantiate the main class that contains the fetching logic
        manager = DataFetchManager() 
        
        # Get deployment info for tracking
        deployment_info = get_deployment_info()
        
        # Print startup banner
        import os
        deployment_tag = os.environ.get('DEPLOYMENT_TAG', 'unknown')
        app_env = os.environ.get('APP_ENV', 'unknown')
        bucket = os.environ.get('SPACES_BUCKET_NAME', 'unknown')
        
        # Count tickers
        ticker_count = len(manager.master_tickers) if hasattr(manager, 'master_tickers') else 0
        if args.tickers:
            custom_tickers = [t.strip().upper() for t in args.tickers.split(',')]
            ticker_count = len(custom_tickers)
        
        print(f"🚀 DATA FETCH MANAGER STARTUP BANNER")
        print(f"deployment={deployment_tag} env={app_env} job={args.job or 'all'} interval={args.interval or 'all'} tickers={ticker_count} bucket={bucket} universe=data/universe/master_tickerlist.csv")
        
        # Execute based on parsed arguments
        if args.job == 'intraday':
            if args.interval == '1min':
                print(f"--- Triggering 1-Minute Intraday Update Only --- {deployment_info}")
                success = manager.run_intraday_updates(interval='1min')
            elif args.interval == '30min':
                print(f"--- Triggering 30-Minute Intraday Update Only --- {deployment_info}")
                success = manager.run_intraday_updates(interval='30min')
            elif args.interval == 'all':
                print(f"--- Triggering All Intraday Updates --- {deployment_info}")
                success1 = manager.run_intraday_updates(interval='1min')
                success2 = manager.run_intraday_updates(interval='30min')
                success = success1 and success2
            else:
                print(f"--- Triggering 30-Minute Intraday Update Only (default) --- {deployment_info}")
                success = manager.run_intraday_updates(interval='30min')
        elif args.job == 'daily':
            print(f"--- Triggering Daily Update Only --- {deployment_info}")
            success = manager.run_daily_updates()
        else:
            # Default behavior: If no job specified, run everything
            print(f"--- No specific job provided. Running full update for ALL intervals. --- {deployment_info}")
            success = manager.run_all_data_updates()
        
        # Exit with appropriate code
        if not success:
            print("❌ Data fetch manager completed with errors")
            sys.exit(1)
        else:
            print("✅ Data fetch manager completed successfully")
            sys.exit(0)
            
    except Exception as e:
        import traceback
        print(f"❌ UNHANDLED EXCEPTION in data_fetch_manager: {e}", file=sys.stderr)
        traceback.print_exc(file=sys.stderr)
        sys.exit(1)