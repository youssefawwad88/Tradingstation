"""
Update intraday data job for Trading Station.
Fetches 1-minute intraday data for all tickers in master list.
"""

import pandas as pd
from datetime import datetime, timedelta
from typing import List, Optional
import time

from utils.config import INTRADAY_TRIM_DAYS, INTRADAY_EXCLUDE_TODAY, INTRADAY_1M_DIR
from utils.logging_setup import get_logger, log_job_start, log_job_complete, log_ticker_result
from utils.storage import get_storage
from utils.alpha_vantage_api import get_api
from utils.validators import validate_complete_dataset, raise_validation_error
from utils.ticker_management import load_master_tickerlist
from utils.time_utils import now_et, is_market_regular_session, current_day_id
from utils.helpers import add_session_labels, trim_to_retention_period

logger = get_logger(__name__)

class IntradayDataUpdater:
    """Updates 1-minute intraday data for all tickers."""
    
    def __init__(self):
        self.storage = get_storage()
        self.api = get_api()
        self.max_retries = 3
        self.retry_delay = 2  # seconds
        
    def load_existing_data(self, ticker: str) -> Optional[pd.DataFrame]:
        """Load existing intraday data for a ticker."""
        file_path = f"{INTRADAY_1M_DIR}/{ticker}_1min.csv"
        return self.storage.read_df(file_path)
    
    def save_data(self, ticker: str, df: pd.DataFrame) -> bool:
        """Save intraday data for a ticker."""
        try:
            file_path = f"{INTRADAY_1M_DIR}/{ticker}_1min.csv"
            success = self.storage.save_df(df, file_path)
            
            if success:
                logger.debug(f"Saved {len(df)} rows for {ticker}")
            
            return success
            
        except Exception as e:
            logger.error(f"Failed to save data for {ticker}: {e}")
            return False
    
    def fetch_fresh_data(self, ticker: str, outputsize: str = "compact") -> Optional[pd.DataFrame]:
        """Fetch fresh intraday data from API."""
        for attempt in range(self.max_retries):
            try:
                df = self.api.get_intraday_data(
                    symbol=ticker,
                    interval="1min",
                    outputsize=outputsize,
                    extended_hours=True
                )
                
                if df is not None and not df.empty:
                    # Add session labels and day_id
                    df = add_session_labels(df)
                    return df
                
                logger.warning(f"No data returned for {ticker} (attempt {attempt + 1})")
                
            except Exception as e:
                if attempt < self.max_retries - 1:
                    wait_time = self.retry_delay * (2 ** attempt)
                    logger.warning(f"API error for {ticker} (attempt {attempt + 1}): {e}. Retrying in {wait_time}s")
                    time.sleep(wait_time)
                else:
                    logger.error(f"Failed to fetch data for {ticker} after {self.max_retries} attempts: {e}")
        
        return None
    
    def merge_and_dedupe(self, existing_df: Optional[pd.DataFrame], new_df: pd.DataFrame) -> pd.DataFrame:
        """Merge existing and new data, removing duplicates."""
        if existing_df is None or existing_df.empty:
            return new_df.copy()
        
        # Combine DataFrames
        combined_df = pd.concat([existing_df, new_df], ignore_index=True)
        
        # Remove duplicates based on timestamp
        combined_df['timestamp'] = pd.to_datetime(combined_df['timestamp'])
        combined_df = combined_df.drop_duplicates(subset=['timestamp'], keep='last')
        
        # Sort by timestamp
        combined_df = combined_df.sort_values('timestamp').reset_index(drop=True)
        
        return combined_df
    
    def apply_retention_policy(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply data retention policy."""
        if df.empty:
            return df
        
        include_today = not INTRADAY_EXCLUDE_TODAY
        
        trimmed_df = trim_to_retention_period(
            df, 
            retention_days=INTRADAY_TRIM_DAYS,
            timestamp_col='timestamp',
            include_today=include_today
        )
        
        return trimmed_df
    
    def update_ticker_data(self, ticker: str) -> bool:
        """Update intraday data for a single ticker."""
        start_time = time.time()
        
        try:
            # Load existing data
            existing_df = self.load_existing_data(ticker)
            
            # Determine what to fetch
            if existing_df is None or existing_df.empty:
                # No existing data - fetch full dataset
                outputsize = "full"
                logger.debug(f"No existing data for {ticker}, fetching full dataset")
            else:
                # Have existing data - fetch compact (last 100 data points)
                outputsize = "compact"
                logger.debug(f"Updating existing data for {ticker}")
            
            # Fetch fresh data
            new_df = self.fetch_fresh_data(ticker, outputsize)
            
            if new_df is None or new_df.empty:
                log_ticker_result(logger, ticker, "FETCH", False, "No data returned from API")
                return False
            
            # Merge with existing data
            merged_df = self.merge_and_dedupe(existing_df, new_df)
            
            # Apply retention policy
            final_df = self.apply_retention_policy(merged_df)
            
            # Validate data
            validation_result = validate_complete_dataset(
                final_df, 
                ticker, 
                data_type="intraday",
                max_retention_days=INTRADAY_TRIM_DAYS + 1  # Allow 1 extra day
            )
            
            if not validation_result.valid:
                log_ticker_result(logger, ticker, "VALIDATE", False, validation_result.message)
                return False
            
            # Save data
            if not self.save_data(ticker, final_df):
                log_ticker_result(logger, ticker, "SAVE", False, "Storage operation failed")
                return False
            
            # Log success
            elapsed_ms = (time.time() - start_time) * 1000
            new_rows = len(final_df) - (len(existing_df) if existing_df is not None else 0)
            
            log_ticker_result(
                logger, ticker, "UPDATE", True, 
                f"{len(final_df)} total rows, {new_rows} new",
                elapsed_ms
            )
            
            return True
            
        except Exception as e:
            elapsed_ms = (time.time() - start_time) * 1000
            log_ticker_result(logger, ticker, "UPDATE", False, str(e), elapsed_ms)
            return False
    
    def should_run_update(self) -> bool:
        """Check if update should run based on market hours."""
        current_time = now_et()
        
        # Always allow manual runs
        if not is_market_regular_session(current_time):
            logger.info("Market not in regular session - allowing manual update")
            return True
        
        logger.info("Market in regular session - proceeding with update")
        return True
    
    def run_update(self, ticker_list: Optional[List[str]] = None) -> Dict[str, bool]:
        """Run intraday data update for all tickers."""
        if not self.should_run_update():
            logger.info("Skipping intraday update - market conditions not met")
            return {}
        
        start_time = datetime.now()
        
        # Load ticker list
        if ticker_list is None:
            ticker_list = load_master_tickerlist()
        
        if not ticker_list:
            logger.warning("No tickers found in master list")
            return {}
        
        log_job_start(logger, "update_intraday", len(ticker_list))
        
        results = {}
        success_count = 0
        
        # Process each ticker
        for i, ticker in enumerate(ticker_list, 1):
            logger.info(f"Processing {ticker} ({i}/{len(ticker_list)})")
            
            success = self.update_ticker_data(ticker)
            results[ticker] = success
            
            if success:
                success_count += 1
            
            # Add small delay to respect rate limits
            if i < len(ticker_list):  # Don't sleep after last ticker
                time.sleep(0.5)
        
        # Log completion
        elapsed_ms = (datetime.now() - start_time).total_seconds() * 1000
        log_job_complete(logger, "update_intraday", elapsed_ms, success_count)
        
        logger.info(f"Intraday update complete: {success_count}/{len(ticker_list)} successful")
        
        return results

def main(ticker_list: Optional[List[str]] = None):
    """Main entry point for intraday data update."""
    try:
        updater = IntradayDataUpdater()
        results = updater.run_update(ticker_list)
        
        # Print summary
        success_count = sum(1 for success in results.values() if success)
        total_count = len(results)
        
        print(f"Intraday data update complete: {success_count}/{total_count} successful")
        
        # Print any failures
        failures = [ticker for ticker, success in results.items() if not success]
        if failures:
            print(f"Failed tickers: {', '.join(failures[:10])}")
            if len(failures) > 10:
                print(f"... and {len(failures) - 10} more")
        
        return results
        
    except Exception as e:
        logger.error(f"Intraday update job failed: {e}")
        return {}

if __name__ == "__main__":
    import sys
    
    # Allow passing specific tickers as command line arguments
    ticker_list = sys.argv[1:] if len(sys.argv) > 1 else None
    main(ticker_list)