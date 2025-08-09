"""
Update daily data job for Trading Station.
Fetches daily OHLCV data for all tickers in master list.
"""

import pandas as pd
from datetime import datetime, timedelta
from typing import List, Optional, Dict
import time

from utils.config import DAILY_DIR
from utils.logging_setup import get_logger, log_job_start, log_job_complete, log_ticker_result
from utils.storage import get_storage
from utils.alpha_vantage_api import get_api
from utils.validators import validate_complete_dataset
from utils.ticker_management import load_master_tickerlist
from utils.time_utils import now_et, prev_trading_day

logger = get_logger(__name__)

class DailyDataUpdater:
    """Updates daily OHLCV data for all tickers."""
    
    def __init__(self):
        self.storage = get_storage()
        self.api = get_api()
        self.max_retries = 3
        self.retry_delay = 2  # seconds
        
    def load_existing_data(self, ticker: str) -> Optional[pd.DataFrame]:
        """Load existing daily data for a ticker."""
        file_path = f"{DAILY_DIR}/{ticker}_daily.csv"
        return self.storage.read_df(file_path)
    
    def save_data(self, ticker: str, df: pd.DataFrame) -> bool:
        """Save daily data for a ticker."""
        try:
            file_path = f"{DAILY_DIR}/{ticker}_daily.csv"
            success = self.storage.save_df(df, file_path)
            
            if success:
                logger.debug(f"Saved {len(df)} daily rows for {ticker}")
            
            return success
            
        except Exception as e:
            logger.error(f"Failed to save daily data for {ticker}: {e}")
            return False
    
    def fetch_fresh_data(self, ticker: str, outputsize: str = "compact") -> Optional[pd.DataFrame]:
        """Fetch fresh daily data from API."""
        for attempt in range(self.max_retries):
            try:
                df = self.api.get_daily_data(
                    symbol=ticker,
                    outputsize=outputsize
                )
                
                if df is not None and not df.empty:
                    return df
                
                logger.warning(f"No daily data returned for {ticker} (attempt {attempt + 1})")
                
            except Exception as e:
                if attempt < self.max_retries - 1:
                    wait_time = self.retry_delay * (2 ** attempt)
                    logger.warning(f"API error for {ticker} (attempt {attempt + 1}): {e}. Retrying in {wait_time}s")
                    time.sleep(wait_time)
                else:
                    logger.error(f"Failed to fetch daily data for {ticker} after {self.max_retries} attempts: {e}")
        
        return None
    
    def merge_and_dedupe(self, existing_df: Optional[pd.DataFrame], new_df: pd.DataFrame) -> pd.DataFrame:
        """Merge existing and new daily data, removing duplicates."""
        if existing_df is None or existing_df.empty:
            return new_df.copy()
        
        # Combine DataFrames
        combined_df = pd.concat([existing_df, new_df], ignore_index=True)
        
        # Remove duplicates based on date
        combined_df['date'] = pd.to_datetime(combined_df['date']).dt.date
        combined_df = combined_df.drop_duplicates(subset=['date'], keep='last')
        
        # Sort by date
        combined_df = combined_df.sort_values('date').reset_index(drop=True)
        
        return combined_df
    
    def validate_daily_data_freshness(self, df: pd.DataFrame) -> bool:
        """Validate that daily data includes recent trading day."""
        if df.empty:
            return False
        
        # Get the latest date in our data
        latest_date = pd.to_datetime(df['date']).max().date()
        
        # Get the previous trading day (since we might be running before market close)
        prev_trading = prev_trading_day(now_et()).date()
        
        # Data should be at most 3 trading days old
        max_age_days = 5  # Allow for long weekends
        cutoff_date = prev_trading - timedelta(days=max_age_days)
        
        is_fresh = latest_date >= cutoff_date
        
        if not is_fresh:
            logger.warning(f"Daily data may be stale: latest date {latest_date}, expected >= {cutoff_date}")
        
        return is_fresh
    
    def update_ticker_data(self, ticker: str, force_full: bool = False) -> bool:
        """Update daily data for a single ticker."""
        start_time = time.time()
        
        try:
            # Load existing data
            existing_df = self.load_existing_data(ticker)
            
            # Determine what to fetch
            if existing_df is None or existing_df.empty or force_full:
                # No existing data or forced full update - fetch full dataset
                outputsize = "full"
                logger.debug(f"Fetching full daily dataset for {ticker}")
            else:
                # Have existing data - fetch compact (last 100 data points)
                outputsize = "compact"
                logger.debug(f"Updating existing daily data for {ticker}")
            
            # Fetch fresh data
            new_df = self.fetch_fresh_data(ticker, outputsize)
            
            if new_df is None or new_df.empty:
                log_ticker_result(logger, ticker, "FETCH", False, "No daily data returned from API")
                return False
            
            # Merge with existing data
            merged_df = self.merge_and_dedupe(existing_df, new_df)
            
            # Validate data freshness
            if not self.validate_daily_data_freshness(merged_df):
                log_ticker_result(logger, ticker, "VALIDATE", False, "Daily data appears stale")
                # Continue anyway - stale data is better than no data
            
            # Validate data integrity
            validation_result = validate_complete_dataset(
                merged_df, 
                ticker, 
                data_type="daily",
                max_retention_days=365  # Keep 1 year of daily data
            )
            
            if not validation_result.valid:
                log_ticker_result(logger, ticker, "VALIDATE", False, validation_result.message)
                return False
            
            # Save data
            if not self.save_data(ticker, merged_df):
                log_ticker_result(logger, ticker, "SAVE", False, "Storage operation failed")
                return False
            
            # Log success
            elapsed_ms = (time.time() - start_time) * 1000
            new_rows = len(merged_df) - (len(existing_df) if existing_df is not None else 0)
            latest_date = pd.to_datetime(merged_df['date']).max().date()
            
            log_ticker_result(
                logger, ticker, "UPDATE", True, 
                f"{len(merged_df)} total rows, {new_rows} new, latest: {latest_date}",
                elapsed_ms
            )
            
            return True
            
        except Exception as e:
            elapsed_ms = (time.time() - start_time) * 1000
            log_ticker_result(logger, ticker, "UPDATE", False, str(e), elapsed_ms)
            return False
    
    def run_update(self, ticker_list: Optional[List[str]] = None, force_full: bool = False) -> Dict[str, bool]:
        """Run daily data update for all tickers."""
        start_time = datetime.now()
        
        # Load ticker list
        if ticker_list is None:
            ticker_list = load_master_tickerlist()
        
        if not ticker_list:
            logger.warning("No tickers found in master list")
            return {}
        
        log_job_start(logger, "update_daily", len(ticker_list))
        
        results = {}
        success_count = 0
        
        # Process each ticker
        for i, ticker in enumerate(ticker_list, 1):
            logger.info(f"Processing {ticker} ({i}/{len(ticker_list)})")
            
            success = self.update_ticker_data(ticker, force_full)
            results[ticker] = success
            
            if success:
                success_count += 1
            
            # Add delay to respect rate limits
            if i < len(ticker_list):  # Don't sleep after last ticker
                time.sleep(1.0)  # Longer delay for daily data
        
        # Log completion
        elapsed_ms = (datetime.now() - start_time).total_seconds() * 1000
        log_job_complete(logger, "update_daily", elapsed_ms, success_count)
        
        logger.info(f"Daily update complete: {success_count}/{len(ticker_list)} successful")
        
        return results
    
    def run_backfill(self, ticker_list: Optional[List[str]] = None) -> Dict[str, bool]:
        """Run full backfill of daily data."""
        logger.info("Starting daily data backfill (full history)")
        return self.run_update(ticker_list, force_full=True)

def main(ticker_list: Optional[List[str]] = None, backfill: bool = False):
    """Main entry point for daily data update."""
    try:
        updater = DailyDataUpdater()
        
        if backfill:
            results = updater.run_backfill(ticker_list)
        else:
            results = updater.run_update(ticker_list)
        
        # Print summary
        success_count = sum(1 for success in results.values() if success)
        total_count = len(results)
        
        update_type = "backfill" if backfill else "update"
        print(f"Daily data {update_type} complete: {success_count}/{total_count} successful")
        
        # Print any failures
        failures = [ticker for ticker, success in results.items() if not success]
        if failures:
            print(f"Failed tickers: {', '.join(failures[:10])}")
            if len(failures) > 10:
                print(f"... and {len(failures) - 10} more")
        
        return results
        
    except Exception as e:
        logger.error(f"Daily update job failed: {e}")
        return {}

if __name__ == "__main__":
    import sys
    
    # Check for command line arguments
    backfill = "--backfill" in sys.argv
    ticker_args = [arg for arg in sys.argv[1:] if not arg.startswith("--")]
    ticker_list = ticker_args if ticker_args else None
    
    main(ticker_list, backfill)