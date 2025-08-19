#!/usr/bin/env python3
"""
Real-time Intraday Updater - Single Purpose Job
===============================================

This is a clean, single-purpose job that runs EVERY MINUTE, 24/5.
Its ONLY mission is to do a COMPACT fetch for ALL tickers.
It gets the last 100 candles and merges them with existing historical files.

This is how we get TODAY'S data, and it will be done correctly.

USAGE:
    python3 jobs/realtime_intraday_updater.py
    
SCHEDULE:
    Run every minute, 24 hours a day, 5 days a week
    
PURPOSE:
    - Real-time data updates using Alpha Vantage COMPACT API
    - Processes ALL tickers from master_tickerlist.csv  
    - Gets last 100 candles and merges with existing historical data
    - Uses exponential backoff retry for API failures
    - Professional logging with clear start/end messages
"""

import logging
import os
import sys
import time
from datetime import datetime, timedelta
from typing import List, Optional, Tuple

import pandas as pd
import pytz

# Add project root to Python path for imports
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# Import core utilities  
from utils.alpha_vantage_api import get_intraday_data
from utils.config import ALPHA_VANTAGE_API_KEY, SPACES_BUCKET_NAME, INTRADAY_BATCH_SIZE
from utils.helpers import read_master_tickerlist, save_df_to_s3
from utils.data_storage import read_df_from_s3
from utils.market_time import is_market_open_on_date

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("data/logs/realtime_intraday_updater.log", mode="a")
    ]
)
logger = logging.getLogger(__name__)

# Configuration constants
MAX_RETRIES = 3
RETRY_BASE_DELAY = 1.5  # Faster retry for real-time updates
INTERVALS = ["1min", "30min"]  # Process both intervals


class RealtimeIntradayUpdater:
    """
    Single-purpose real-time intraday data updater.
    
    Uses COMPACT API to get latest 100 candles and merge with existing data.
    """
    
    def __init__(self):
        """Initialize the real-time updater."""
        self.ny_tz = pytz.timezone("America/New_York")
        self.start_time = datetime.now(self.ny_tz)
        self.session_stats = {
            "total_tickers": 0,
            "successful_updates": 0,
            "failed_updates": 0,
            "intervals_processed": 0,
            "api_calls_made": 0,
            "new_candles_added": 0
        }
        
    def log_job_start(self):
        """Log clear job start message."""
        logger.info("=" * 80)
        logger.info("⚡ REAL-TIME INTRADAY UPDATER - JOB START")
        logger.info("=" * 80)
        logger.info(f"📅 Start Time: {self.start_time.strftime('%Y-%m-%d %H:%M:%S %Z')}")
        logger.info(f"🎯 Mission: COMPACT data updates for ALL tickers")
        logger.info(f"🔧 API Strategy: COMPACT fetch only (last 100 candles)")
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
        logger.info(f"⚡ REAL-TIME INTRADAY UPDATER - JOB {'COMPLETED' if success else 'FAILED'}")
        logger.info("=" * 80)
        logger.info(f"📅 End Time: {end_time.strftime('%Y-%m-%d %H:%M:%S %Z')}")
        logger.info(f"⏱️ Duration: {duration}")
        logger.info(f"📊 Statistics:")
        logger.info(f"   Total Tickers: {self.session_stats['total_tickers']}")
        logger.info(f"   Successful Updates: {self.session_stats['successful_updates']}")
        logger.info(f"   Failed Updates: {self.session_stats['failed_updates']}")
        logger.info(f"   Intervals Processed: {self.session_stats['intervals_processed']}")
        logger.info(f"   API Calls Made: {self.session_stats['api_calls_made']}")
        logger.info(f"   New Candles Added: {self.session_stats['new_candles_added']}")
        
        success_rate = (self.session_stats['successful_updates'] / 
                       max(1, self.session_stats['total_tickers'])) * 100
        logger.info(f"   Success Rate: {success_rate:.1f}%")
        
    def fetch_with_retry(self, ticker: str, interval: str) -> pd.DataFrame:
        """
        Fetch COMPACT real-time data with exponential backoff retry.
        
        Args:
            ticker: Stock ticker symbol
            interval: Time interval (1min, 30min)
            
        Returns:
            DataFrame with latest 100 candles or empty DataFrame on failure
        """
        for attempt in range(MAX_RETRIES):
            try:
                logger.info(f"🔄 {ticker} ({interval}): COMPACT fetch attempt {attempt + 1}/{MAX_RETRIES}")
                
                self.session_stats['api_calls_made'] += 1
                df = get_intraday_data(ticker, interval=interval, outputsize="compact")
                
                if df is not None and not df.empty:
                    logger.info(f"✅ {ticker} ({interval}): COMPACT fetch successful - {len(df)} latest candles")
                    return df
                else:
                    logger.warning(f"⚠️ {ticker} ({interval}): API returned empty data on attempt {attempt + 1}")
                    
            except Exception as e:
                logger.error(f"❌ {ticker} ({interval}): COMPACT fetch error on attempt {attempt + 1}: {e}")
                
            # Exponential backoff if not the last attempt
            if attempt < MAX_RETRIES - 1:
                wait_time = RETRY_BASE_DELAY * (2 ** attempt)
                logger.info(f"⏳ {ticker} ({interval}): Waiting {wait_time:.1f}s before retry...")
                time.sleep(wait_time)
        
        logger.error(f"❌ {ticker} ({interval}): All COMPACT fetch attempts failed")
        return pd.DataFrame()
        
    def intelligent_data_merge(self, existing_df: pd.DataFrame, new_df: pd.DataFrame, ticker: str, interval: str) -> pd.DataFrame:
        """
        Intelligently merge new compact data with existing historical data.
        
        Args:
            existing_df: Existing historical data
            new_df: New compact data (last 100 candles)
            ticker: Stock ticker symbol
            interval: Time interval
            
        Returns:
            Merged DataFrame with deduplicated data
        """
        if new_df.empty:
            logger.warning(f"⚠️ {ticker} ({interval}): No new data to merge")
            return existing_df
            
        if existing_df.empty:
            logger.info(f"🆕 {ticker} ({interval}): No existing data - using new data as baseline")
            self.session_stats['new_candles_added'] += len(new_df)
            return new_df
            
        try:
            # Ensure timestamp columns exist and are properly formatted
            if 'timestamp' not in existing_df.columns or 'timestamp' not in new_df.columns:
                logger.error(f"❌ {ticker} ({interval}): Missing timestamp column")
                return existing_df
                
            # Convert timestamps to datetime for comparison
            existing_df['timestamp'] = pd.to_datetime(existing_df['timestamp'])
            new_df['timestamp'] = pd.to_datetime(new_df['timestamp'])
            
            # Find overlapping timestamps  
            overlap_mask = new_df['timestamp'].isin(existing_df['timestamp'])
            overlapping_timestamps = new_df[overlap_mask]['timestamp']
            new_timestamps = new_df[~overlap_mask]['timestamp']
            
            logger.info(f"📊 {ticker} ({interval}): Data merge analysis:")
            logger.info(f"   Existing candles: {len(existing_df)}")
            logger.info(f"   New candles fetched: {len(new_df)}")
            logger.info(f"   Overlapping timestamps: {len(overlapping_timestamps)}")
            logger.info(f"   Truly new timestamps: {len(new_timestamps)}")
            
            if len(new_timestamps) > 0:
                # Update overlapping data with latest values
                if len(overlapping_timestamps) > 0:
                    # Remove overlapping timestamps from existing data
                    existing_df = existing_df[~existing_df['timestamp'].isin(overlapping_timestamps)]
                    logger.info(f"🔄 {ticker} ({interval}): Updated {len(overlapping_timestamps)} overlapping candles")
                
                # Combine existing data with all new data
                merged_df = pd.concat([existing_df, new_df], ignore_index=True)
                
                # Sort by timestamp
                merged_df = merged_df.sort_values('timestamp').reset_index(drop=True)
                
                self.session_stats['new_candles_added'] += len(new_timestamps)
                logger.info(f"✅ {ticker} ({interval}): Successfully merged {len(new_timestamps)} new candles")
                logger.info(f"   📈 Total candles after merge: {len(merged_df)}")
                
                return merged_df
            else:
                logger.info(f"📊 {ticker} ({interval}): No new candles to add - data is up to date")
                return existing_df
                
        except Exception as e:
            logger.error(f"❌ {ticker} ({interval}): Error during data merge: {e}")
            import traceback
            logger.error(f"Full traceback: {traceback.format_exc()}")
            return existing_df
            
    def update_ticker_data(self, ticker: str) -> bool:
        """
        Update real-time data for a single ticker using compact API.
        
        Args:
            ticker: Stock ticker symbol
            
        Returns:
            True if update was successful for at least one interval
        """
        ticker_success = False
        
        for interval in INTERVALS:
            logger.info(f"⚡ {ticker}: Starting COMPACT real-time update for {interval}")
            
            # Generate dynamic file path
            if interval == "30min":
                object_name = f"data/intraday_30min/{ticker}_30min.csv"
            else:
                object_name = f"data/intraday/{ticker}_1min.csv"
                
            # Load existing historical data
            try:
                existing_df = read_df_from_s3(object_name)
                if existing_df.empty:
                    logger.warning(f"⚠️ {ticker} ({interval}): No existing historical data found")
                else:
                    logger.info(f"📂 {ticker} ({interval}): Loaded {len(existing_df)} existing candles")
            except Exception as e:
                logger.warning(f"⚠️ {ticker} ({interval}): Could not load existing data: {e}")
                existing_df = pd.DataFrame()
            
            # Fetch latest compact data (last 100 candles)
            new_df = self.fetch_with_retry(ticker, interval)
            
            if new_df.empty:
                logger.error(f"❌ {ticker} ({interval}): COMPACT real-time update FAILED")
                self.session_stats['failed_updates'] += 1
                continue
                
            # Intelligently merge new data with existing historical data
            merged_df = self.intelligent_data_merge(existing_df, new_df, ticker, interval)
            
            # Save updated dataset
            try:
                save_success = save_df_to_s3(merged_df, object_name)
                
                if save_success:
                    logger.info(f"✅ {ticker} ({interval}): COMPACT real-time update COMPLETED")
                    logger.info(f"   📁 Saved to: {object_name}")
                    logger.info(f"   📊 Total candles: {len(merged_df)}")
                    self.session_stats['successful_updates'] += 1
                    self.session_stats['intervals_processed'] += 1
                    ticker_success = True
                else:
                    logger.error(f"❌ {ticker} ({interval}): Failed to save updated data")
                    self.session_stats['failed_updates'] += 1
                    
            except Exception as e:
                logger.error(f"❌ {ticker} ({interval}): Error saving updated data: {e}")
                self.session_stats['failed_updates'] += 1
                
        return ticker_success
        
    def run_realtime_update(self) -> bool:
        """
        Execute the complete real-time intraday update process.
        
        Returns:
            True if the job completed successfully
        """
        try:
            self.log_job_start()
            
            # Load all tickers from master list
            tickers = read_master_tickerlist()
            
            if not tickers:
                logger.error("❌ No tickers found in master list - aborting update")
                self.log_job_end(success=False)
                return False
                
            self.session_stats['total_tickers'] = len(tickers)
            logger.info(f"📋 Processing {len(tickers)} tickers for COMPACT real-time update")
            
            # Process all tickers with COMPACT API updates
            for i, ticker in enumerate(tickers, 1):
                logger.info(f"🎯 Processing ticker {i}/{len(tickers)}: {ticker}")
                
                success = self.update_ticker_data(ticker)
                
                if success:
                    logger.info(f"✅ {ticker}: Real-time update completed successfully")
                else:
                    logger.error(f"❌ {ticker}: Real-time update failed")
                    
                # Rate limiting between tickers (faster for real-time)
                if i < len(tickers):
                    time.sleep(0.5)  # 0.5 seconds between tickers for real-time updates
                    
            self.log_job_end(success=True)
            return True
            
        except Exception as e:
            logger.error(f"❌ CRITICAL ERROR in real-time update: {e}")
            import traceback
            logger.error(f"Full traceback: {traceback.format_exc()}")
            self.log_job_end(success=False)
            return False


def main():
    """Main entry point for the real-time intraday updater."""
    # Ensure log directory exists
    os.makedirs("data/logs", exist_ok=True)
    
    updater = RealtimeIntradayUpdater()
    success = updater.run_realtime_update()
    
    if success:
        sys.exit(0)
    else:
        sys.exit(1)


if __name__ == "__main__":
    main()