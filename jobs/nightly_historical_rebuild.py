#!/usr/bin/env python3
"""
Nightly Historical Rebuild - Single Purpose Job
===============================================

This is a clean, single-purpose job that runs ONCE per day after market close.
Its ONLY mission is to do a FULL fetch for ALL tickers in the masterlist.
It rebuilds the ENTIRE historical data file for each ticker.

This is the correct, intended purpose of the FULL API.

USAGE:
    python3 jobs/nightly_historical_rebuild.py
    
SCHEDULE:
    Run once daily after market close (6:00 PM ET recommended)
    
PURPOSE:
    - Full historical data rebuild using Alpha Vantage FULL API
    - Processes ALL tickers from master_tickerlist.csv
    - Replaces entire historical dataset for each ticker
    - Uses exponential backoff retry for API failures
    - Professional logging with clear start/end messages
"""

import logging
import os
import sys
import time
from datetime import datetime, timedelta
from typing import List, Tuple

import pandas as pd
import pytz

# Add project root to Python path for imports
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# Import core utilities
from utils.alpha_vantage_api import get_intraday_data
from utils.config import ALPHA_VANTAGE_API_KEY, SPACES_BUCKET_NAME, INTRADAY_BATCH_SIZE
from utils.helpers import read_master_tickerlist, save_df_to_s3
from utils.market_time import is_market_open_on_date

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("data/logs/nightly_historical_rebuild.log", mode="a")
    ]
)
logger = logging.getLogger(__name__)

# Configuration constants
MAX_RETRIES = 3
RETRY_BASE_DELAY = 2.0
INTERVALS = ["1min", "30min"]  # Process both intervals


class NightlyHistoricalRebuilder:
    """
    Single-purpose nightly historical data rebuilder.
    
    Uses FULL API to completely rebuild historical datasets.
    """
    
    def __init__(self):
        """Initialize the nightly rebuilder."""
        self.ny_tz = pytz.timezone("America/New_York")
        self.start_time = datetime.now(self.ny_tz)
        self.session_stats = {
            "total_tickers": 0,
            "successful_rebuilds": 0,
            "failed_rebuilds": 0,
            "intervals_processed": 0,
            "api_calls_made": 0
        }
        
    def log_job_start(self):
        """Log clear job start message."""
        logger.info("=" * 80)
        logger.info("🌙 NIGHTLY HISTORICAL REBUILD - JOB START")
        logger.info("=" * 80)
        logger.info(f"📅 Start Time: {self.start_time.strftime('%Y-%m-%d %H:%M:%S %Z')}")
        logger.info(f"🎯 Mission: FULL historical data rebuild for ALL tickers")
        logger.info(f"🔧 API Strategy: FULL fetch only (complete historical rebuild)")
        logger.info(f"📊 Intervals: {', '.join(INTERVALS)}")
        
        # API key validation
        if not ALPHA_VANTAGE_API_KEY:
            logger.error("❌ ALPHA_VANTAGE_API_KEY not found - running in TEST mode")
        else:
            logger.info("✅ API key configured")
            
    def log_job_end(self, success: bool = True):
        """Log clear job end message with statistics."""
        end_time = datetime.now(self.ny_tz)
        duration = end_time - self.start_time
        
        logger.info("=" * 80)
        logger.info(f"🌙 NIGHTLY HISTORICAL REBUILD - JOB {'COMPLETED' if success else 'FAILED'}")
        logger.info("=" * 80)
        logger.info(f"📅 End Time: {end_time.strftime('%Y-%m-%d %H:%M:%S %Z')}")
        logger.info(f"⏱️ Duration: {duration}")
        logger.info(f"📊 Statistics:")
        logger.info(f"   Total Tickers: {self.session_stats['total_tickers']}")
        logger.info(f"   Successful Rebuilds: {self.session_stats['successful_rebuilds']}")
        logger.info(f"   Failed Rebuilds: {self.session_stats['failed_rebuilds']}")
        logger.info(f"   Intervals Processed: {self.session_stats['intervals_processed']}")
        logger.info(f"   API Calls Made: {self.session_stats['api_calls_made']}")
        
        success_rate = (self.session_stats['successful_rebuilds'] / 
                       max(1, self.session_stats['total_tickers'])) * 100
        logger.info(f"   Success Rate: {success_rate:.1f}%")
        
    def fetch_with_retry(self, ticker: str, interval: str) -> pd.DataFrame:
        """
        Fetch FULL historical data with exponential backoff retry.
        
        Args:
            ticker: Stock ticker symbol
            interval: Time interval (1min, 30min)
            
        Returns:
            DataFrame with historical data or empty DataFrame on failure
        """
        for attempt in range(MAX_RETRIES):
            try:
                logger.info(f"🔄 {ticker} ({interval}): FULL fetch attempt {attempt + 1}/{MAX_RETRIES}")
                
                self.session_stats['api_calls_made'] += 1
                df = get_intraday_data(ticker, interval=interval, outputsize="full")
                
                if df is not None and not df.empty:
                    logger.info(f"✅ {ticker} ({interval}): FULL fetch successful - {len(df)} historical rows")
                    return df
                else:
                    logger.warning(f"⚠️ {ticker} ({interval}): API returned empty data on attempt {attempt + 1}")
                    
            except Exception as e:
                logger.error(f"❌ {ticker} ({interval}): FULL fetch error on attempt {attempt + 1}: {e}")
                
            # Exponential backoff if not the last attempt
            if attempt < MAX_RETRIES - 1:
                wait_time = RETRY_BASE_DELAY * (2 ** attempt)
                logger.info(f"⏳ {ticker} ({interval}): Waiting {wait_time:.1f}s before retry...")
                time.sleep(wait_time)
        
        logger.error(f"❌ {ticker} ({interval}): All FULL fetch attempts failed")
        return pd.DataFrame()
        
    def rebuild_ticker_data(self, ticker: str) -> bool:
        """
        Rebuild complete historical data for a single ticker.
        
        Args:
            ticker: Stock ticker symbol
            
        Returns:
            True if rebuild was successful for at least one interval
        """
        ticker_success = False
        
        for interval in INTERVALS:
            logger.info(f"🔧 {ticker}: Starting FULL historical rebuild for {interval}")
            
            # Fetch complete historical data using FULL API
            df = self.fetch_with_retry(ticker, interval)
            
            if df.empty:
                logger.error(f"❌ {ticker} ({interval}): FULL historical rebuild FAILED")
                self.session_stats['failed_rebuilds'] += 1
                continue
                
            # Save complete historical dataset
            try:
                # Generate dynamic file path
                if interval == "30min":
                    object_name = f"data/intraday_30min/{ticker}_30min.csv"
                else:
                    object_name = f"data/intraday/{ticker}_1min.csv"
                    
                save_success = save_df_to_s3(df, object_name)
                
                if save_success:
                    logger.info(f"✅ {ticker} ({interval}): FULL historical rebuild COMPLETED")
                    logger.info(f"   📁 Saved to: {object_name}")
                    logger.info(f"   📊 Total historical rows: {len(df)}")
                    self.session_stats['successful_rebuilds'] += 1
                    self.session_stats['intervals_processed'] += 1
                    ticker_success = True
                else:
                    logger.error(f"❌ {ticker} ({interval}): Failed to save rebuilt data")
                    self.session_stats['failed_rebuilds'] += 1
                    
            except Exception as e:
                logger.error(f"❌ {ticker} ({interval}): Error saving rebuilt data: {e}")
                self.session_stats['failed_rebuilds'] += 1
                
        return ticker_success
        
    def run_nightly_rebuild(self) -> bool:
        """
        Execute the complete nightly historical rebuild process.
        
        Returns:
            True if the job completed successfully
        """
        try:
            self.log_job_start()
            
            # Load all tickers from master list
            tickers = read_master_tickerlist()
            
            if not tickers:
                logger.error("❌ No tickers found in master list - aborting rebuild")
                self.log_job_end(success=False)
                return False
                
            self.session_stats['total_tickers'] = len(tickers)
            logger.info(f"📋 Processing {len(tickers)} tickers for FULL historical rebuild")
            
            # Process all tickers with FULL API rebuild
            for i, ticker in enumerate(tickers, 1):
                logger.info(f"🎯 Processing ticker {i}/{len(tickers)}: {ticker}")
                
                success = self.rebuild_ticker_data(ticker)
                
                if success:
                    logger.info(f"✅ {ticker}: Historical rebuild completed successfully")
                else:
                    logger.error(f"❌ {ticker}: Historical rebuild failed")
                    
                # Rate limiting between tickers
                if i < len(tickers):
                    time.sleep(1.0)  # 1 second between tickers to respect API limits
                    
            self.log_job_end(success=True)
            return True
            
        except Exception as e:
            logger.error(f"❌ CRITICAL ERROR in nightly rebuild: {e}")
            import traceback
            logger.error(f"Full traceback: {traceback.format_exc()}")
            self.log_job_end(success=False)
            return False


def main():
    """Main entry point for the nightly historical rebuilder."""
    # Ensure log directory exists
    os.makedirs("data/logs", exist_ok=True)
    
    rebuilder = NightlyHistoricalRebuilder()
    success = rebuilder.run_nightly_rebuild()
    
    if success:
        sys.exit(0)
    else:
        sys.exit(1)


if __name__ == "__main__":
    main()