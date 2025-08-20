print("--- DATA FETCH MANAGER VERSION 2.0 RUNNING ---")
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
    TIMEZONE
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
        
        Returns:
            bool: Success status
        """
        logger.info("📥 Downloading master_tickerlist.csv from Spaces cloud storage")
        
        if not self.spaces_client:
            logger.error("❌ Spaces client not available - cannot download master tickerlist")
            return False
            
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
        
        Decision Logic:
        - If file doesn't exist OR size < 10KB: Full fetch (recovery/initial)
        - If file exists and size >= 10KB: Compact fetch (standard update)
        
        Args:
            ticker: Stock ticker symbol
            interval: '1min' or '30min'
            
        Returns:
            bool: Success status
        """
        directory = f"intraday_{interval}" if interval == "30min" else "intraday_1min"
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
                
            # Step 6: Trim to specification
            if interval == '1min':
                # Keep last 7 days
                cutoff_date = datetime.now(self.ny_tz) - timedelta(days=self.INTRADAY_1MIN_DAYS)
                combined_df = combined_df[combined_df['timestamp'] >= cutoff_date]
            else:  # 30min
                # Keep last 500 rows
                combined_df = combined_df.tail(self.INTRADAY_30MIN_ROWS)
                
            # Step 7: Self-healing gap detection
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
                    if interval == '1min':
                        cutoff_date = datetime.now(self.ny_tz) - timedelta(days=self.INTRADAY_1MIN_DAYS)
                        combined_df = combined_df[combined_df['timestamp'] >= cutoff_date]
                    else:
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
            logger.error("❌ No tickers to process - master tickerlist not loaded")
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
            logger.error("❌ No tickers to process - master tickerlist not loaded")
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
        logger.info(f"🏁 {interval.upper()} updates completed in {elapsed_time:.1f} seconds")
        logger.info(f"📊 Successfully processed {successful_tickers}/{len(self.master_tickers)} tickers")
        
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
    parser = argparse.ArgumentParser(description="Unified Data Fetch Manager for the Trading System.")
    parser.add_argument(
        '--interval',
        type=str,
        choices=['1min', '30min', 'daily'],
        required=False,
        help="Specify a single interval to update. If not provided, a default action is taken."
    )
    args = parser.parse_args()

    # Instantiate the main class that contains the fetching logic
    manager = DataFetchManager() 
    
    # Get deployment info for tracking
    deployment_info = get_deployment_info()

    # This logic block explicitly handles every possible case
    if args.interval == '1min':
        print(f"--- Triggering 1-Minute Intraday Update Only --- {deployment_info}")
        manager.run_intraday_updates(interval='1min')
    elif args.interval == '30min':
        print(f"--- Triggering 30-Minute Intraday Update Only --- {deployment_info}")
        manager.run_intraday_updates(interval='30min')
    elif args.interval == 'daily':
        print(f"--- Triggering Daily Update Only --- {deployment_info}")
        manager.run_daily_updates()
    else:
        # Default behavior: If the script is run without any arguments, it updates EVERYTHING.
        print(f"--- No specific interval provided. Running full update for ALL intervals. --- {deployment_info}")
        manager.run_all_data_updates()