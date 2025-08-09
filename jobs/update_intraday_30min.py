"""
Update 30-minute intraday data job for Trading Station.
Derives 30-min bars from 1-min data when possible, otherwise fetches from API.
"""

import pandas as pd
from datetime import datetime, timedelta
from typing import List, Optional, Dict
import time

from utils.config import INTRADAY_30M_DIR, INTRADAY_1M_DIR
from utils.logging_setup import get_logger, log_job_start, log_job_complete, log_ticker_result
from utils.storage import get_storage
from utils.alpha_vantage_api import get_api
from utils.validators import validate_complete_dataset
from utils.ticker_management import load_master_tickerlist
from utils.time_utils import now_et
from utils.helpers import resample_1m_to_30m, add_session_labels, trim_to_retention_period

logger = get_logger(__name__)

class Intraday30minUpdater:
    """Updates 30-minute intraday data for all tickers."""
    
    def __init__(self):
        self.storage = get_storage()
        self.api = get_api()
        self.max_retries = 3
        self.retry_delay = 2  # seconds
        
    def load_existing_data(self, ticker: str) -> Optional[pd.DataFrame]:
        """Load existing 30-min data for a ticker."""
        file_path = f"{INTRADAY_30M_DIR}/{ticker}_30min.csv"
        return self.storage.read_df(file_path)
    
    def load_1min_data(self, ticker: str) -> Optional[pd.DataFrame]:
        """Load 1-minute data for resampling."""
        file_path = f"{INTRADAY_1M_DIR}/{ticker}_1min.csv"
        return self.storage.read_df(file_path)
    
    def save_data(self, ticker: str, df: pd.DataFrame) -> bool:
        """Save 30-min data for a ticker."""
        try:
            file_path = f"{INTRADAY_30M_DIR}/{ticker}_30min.csv"
            success = self.storage.save_df(df, file_path)
            
            if success:
                logger.debug(f"Saved {len(df)} 30min rows for {ticker}")
            
            return success
            
        except Exception as e:
            logger.error(f"Failed to save 30min data for {ticker}: {e}")
            return False
    
    def resample_from_1min(self, ticker: str) -> Optional[pd.DataFrame]:
        """Generate 30-min data by resampling 1-min data."""
        try:
            # Load 1-minute data
            df_1min = self.load_1min_data(ticker)
            
            if df_1min is None or df_1min.empty:
                logger.debug(f"No 1-min data available for {ticker} to resample")
                return None
            
            # Resample to 30-minute bars
            df_30min = resample_1m_to_30m(df_1min)
            
            if df_30min.empty:
                logger.warning(f"Resampling produced no 30-min bars for {ticker}")
                return None
            
            logger.debug(f"Resampled {len(df_1min)} 1-min bars to {len(df_30min)} 30-min bars for {ticker}")
            return df_30min
            
        except Exception as e:
            logger.warning(f"Failed to resample 1-min data for {ticker}: {e}")
            return None
    
    def fetch_30min_from_api(self, ticker: str, outputsize: str = "compact") -> Optional[pd.DataFrame]:
        """Fetch 30-min data directly from API."""
        for attempt in range(self.max_retries):
            try:
                # Note: Alpha Vantage supports 30min interval
                df = self.api.get_intraday_data(
                    symbol=ticker,
                    interval="30min",
                    outputsize=outputsize,
                    extended_hours=True
                )
                
                if df is not None and not df.empty:
                    # Add session labels
                    df = add_session_labels(df)
                    return df
                
                logger.warning(f"No 30-min data returned for {ticker} (attempt {attempt + 1})")
                
            except Exception as e:
                if attempt < self.max_retries - 1:
                    wait_time = self.retry_delay * (2 ** attempt)
                    logger.warning(f"API error for {ticker} (attempt {attempt + 1}): {e}. Retrying in {wait_time}s")
                    time.sleep(wait_time)
                else:
                    logger.error(f"Failed to fetch 30-min data for {ticker} after {self.max_retries} attempts: {e}")
        
        return None
    
    def merge_and_dedupe(self, existing_df: Optional[pd.DataFrame], new_df: pd.DataFrame) -> pd.DataFrame:
        """Merge existing and new 30-min data, removing duplicates."""
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
    
    def should_prefer_resampling(self, ticker: str) -> bool:
        """Determine if we should prefer resampling over API fetch."""
        # Check if 1-min data exists and is recent
        df_1min = self.load_1min_data(ticker)
        
        if df_1min is None or df_1min.empty:
            return False
        
        # Check if 1-min data is recent (within last 2 hours)
        latest_timestamp = pd.to_datetime(df_1min['timestamp']).max()
        cutoff_time = now_et() - timedelta(hours=2)
        
        is_recent = latest_timestamp.tz_localize(None) >= cutoff_time.tz_localize(None)
        
        if is_recent:
            logger.debug(f"Using resampling for {ticker} - recent 1-min data available")
            return True
        else:
            logger.debug(f"Using API for {ticker} - 1-min data is stale")
            return False
    
    def update_ticker_data(self, ticker: str) -> bool:
        """Update 30-min data for a single ticker."""
        start_time = time.time()
        
        try:
            # Load existing 30-min data
            existing_df = self.load_existing_data(ticker)
            
            # Decide on data source strategy
            if self.should_prefer_resampling(ticker):
                # Try resampling from 1-min data first
                new_df = self.resample_from_1min(ticker)
                
                if new_df is None:
                    # Fallback to API
                    logger.debug(f"Resampling failed for {ticker}, falling back to API")
                    new_df = self.fetch_30min_from_api(ticker)
            else:
                # Fetch directly from API
                outputsize = "full" if existing_df is None else "compact"
                new_df = self.fetch_30min_from_api(ticker, outputsize)
            
            if new_df is None or new_df.empty:
                log_ticker_result(logger, ticker, "FETCH", False, "No 30-min data available")
                return False
            
            # Merge with existing data
            merged_df = self.merge_and_dedupe(existing_df, new_df)
            
            # Apply retention policy (same as 1-min data)
            final_df = trim_to_retention_period(
                merged_df, 
                retention_days=7,  # Same as 1-min data
                timestamp_col='timestamp',
                include_today=True
            )
            
            # Validate data
            validation_result = validate_complete_dataset(
                final_df, 
                ticker, 
                data_type="intraday_30min",
                max_retention_days=8  # Allow 1 extra day
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
            data_source = "resampled" if self.should_prefer_resampling(ticker) else "API"
            
            log_ticker_result(
                logger, ticker, "UPDATE", True, 
                f"{len(final_df)} total rows, {new_rows} new ({data_source})",
                elapsed_ms
            )
            
            return True
            
        except Exception as e:
            elapsed_ms = (time.time() - start_time) * 1000
            log_ticker_result(logger, ticker, "UPDATE", False, str(e), elapsed_ms)
            return False
    
    def run_update(self, ticker_list: Optional[List[str]] = None) -> Dict[str, bool]:
        """Run 30-min data update for all tickers."""
        start_time = datetime.now()
        
        # Load ticker list
        if ticker_list is None:
            ticker_list = load_master_tickerlist()
        
        if not ticker_list:
            logger.warning("No tickers found in master list")
            return {}
        
        log_job_start(logger, "update_intraday_30min", len(ticker_list))
        
        results = {}
        success_count = 0
        resample_count = 0
        api_count = 0
        
        # Process each ticker
        for i, ticker in enumerate(ticker_list, 1):
            logger.info(f"Processing {ticker} ({i}/{len(ticker_list)})")
            
            # Track data source for statistics
            used_resampling = self.should_prefer_resampling(ticker)
            
            success = self.update_ticker_data(ticker)
            results[ticker] = success
            
            if success:
                success_count += 1
                if used_resampling:
                    resample_count += 1
                else:
                    api_count += 1
            
            # Add delay to respect rate limits (shorter delay since many will be resampled)
            if i < len(ticker_list):  # Don't sleep after last ticker
                time.sleep(0.3)
        
        # Log completion
        elapsed_ms = (datetime.now() - start_time).total_seconds() * 1000
        log_job_complete(logger, "update_intraday_30min", elapsed_ms, success_count)
        
        logger.info(
            f"30-min update complete: {success_count}/{len(ticker_list)} successful "
            f"({resample_count} resampled, {api_count} from API)"
        )
        
        return results

def main(ticker_list: Optional[List[str]] = None):
    """Main entry point for 30-min intraday data update."""
    try:
        updater = Intraday30minUpdater()
        results = updater.run_update(ticker_list)
        
        # Print summary
        success_count = sum(1 for success in results.values() if success)
        total_count = len(results)
        
        print(f"30-min intraday data update complete: {success_count}/{total_count} successful")
        
        # Print any failures
        failures = [ticker for ticker, success in results.items() if not success]
        if failures:
            print(f"Failed tickers: {', '.join(failures[:10])}")
            if len(failures) > 10:
                print(f"... and {len(failures) - 10} more")
        
        return results
        
    except Exception as e:
        logger.error(f"30-min update job failed: {e}")
        return {}

if __name__ == "__main__":
    import sys
    
    # Allow passing specific tickers as command line arguments
    ticker_list = sys.argv[1:] if len(sys.argv) > 1 else None
    main(ticker_list)