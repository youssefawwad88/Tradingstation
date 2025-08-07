import os
import logging
import pandas as pd
from datetime import datetime
import sys
import traceback

# Add the project root to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.config import (
    DEFAULT_TICKERS, 
    INTRADAY_INTERVALS,
    INTRADAY_DATA_DIR,
    DEBUG_MODE
)
from utils.helpers import (
    fetch_intraday_data, 
    save_df_to_local, 
    save_df_to_s3,
    save_to_local_filesystem
)

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def update_intraday_data(tickers=None, intervals=None):
    """
    Update intraday data for specified tickers and intervals.
    
    Args:
        tickers (list): List of ticker symbols to update
        intervals (list): List of time intervals to update
        
    Returns:
        dict: Summary of update results
    """
    tickers = tickers or DEFAULT_TICKERS
    intervals = intervals or INTRADAY_INTERVALS
    
    results = {
        'success': [],
        'failed': [],
        'timestamp': datetime.now().isoformat()
    }
    
    # Log environment configuration
    print("📦 DigitalOcean Config:")
    print(f"   SPACES_ACCESS_KEY_ID = {os.getenv('SPACES_ACCESS_KEY_ID')}")
    print(f"   SPACES_SECRET_ACCESS_KEY = {'*' * 8 if os.getenv('SPACES_SECRET_ACCESS_KEY') else '❌ Not Set'}")
    print(f"   SPACES_BUCKET_NAME = {os.getenv('SPACES_BUCKET_NAME')}")
    print(f"   Saving locally to: {INTRADAY_DATA_DIR}/")
    
    if DEBUG_MODE:
        print(f"🛠 DEBUG ON: Verbose logging enabled.")
    
    # Ensure data directory exists
    os.makedirs(INTRADAY_DATA_DIR, exist_ok=True)
    
    for ticker in tickers:
        for interval in intervals:
            logger.info(f"Processing {ticker} ({interval})")
            
            try:
                # Fetch data
                df, fetch_success = fetch_intraday_data(ticker, interval=interval)
                
                if not fetch_success or df is None or df.empty:
                    logger.error(f"Failed to fetch data for {ticker} ({interval})")
                    results['failed'].append(f"{ticker}_{interval}")
                    
                    # Traceable logging per ticker
                    print(f"🎯 Ticker: {ticker}")
                    print(f"📊 API Fetch: ❌ Failed")
                    print(f"💾 Local Save Path: ❌ Failed")
                    print(f"☁️ Spaces Upload: ❌ Failed")
                    continue
                
                # Save to local filesystem
                local_file_path, local_success = save_df_to_local(df, ticker, interval)
                
                # Save to DigitalOcean Spaces
                spaces_success = save_df_to_s3(df, ticker, interval)
                
                # If Spaces upload fails, ensure we have a local backup
                if not spaces_success:
                    logger.warning(f"Failed to upload {ticker} ({interval}) to Spaces, using local fallback")
                    if not local_success:
                        fallback_path, fallback_success = save_to_local_filesystem(df, ticker, interval)
                        local_file_path = fallback_path if fallback_success else None
                        local_success = fallback_success
                
                # Record result
                if fetch_success and (local_success or spaces_success):
                    results['success'].append(f"{ticker}_{interval}")
                else:
                    results['failed'].append(f"{ticker}_{interval}")
                
                # Traceable logging per ticker
                print(f"🎯 Ticker: {ticker}")
                print(f"📊 API Fetch: {'✅ Success' if fetch_success else '❌ Failed'}")
                print(f"💾 Local Save Path: {local_file_path if local_success else '❌ Failed'}")
                print(f"☁️ Spaces Upload: {'✅ Success' if spaces_success else '❌ Failed'}")
                
                # List workspace files if in debug mode or for failure diagnostics
                if DEBUG_MODE or not (local_success or spaces_success):
                    if os.path.exists(INTRADAY_DATA_DIR):
                        print(f"📁 Workspace files: {os.listdir(INTRADAY_DATA_DIR)}")
                    else:
                        print(f"📁 Directory does not exist: {INTRADAY_DATA_DIR}")
                
            except Exception as e:
                logger.error(f"Unexpected error processing {ticker} ({interval}): {e}")
                traceback.print_exc()
                results['failed'].append(f"{ticker}_{interval}")
                
                # Traceable logging for unexpected errors
                print(f"🎯 Ticker: {ticker}")
                print(f"❌ UNEXPECTED ERROR: {str(e)}")
    
    # Log summary
    success_count = len(results['success'])
    failed_count = len(results['failed'])
    logger.info(f"Update completed. Success: {success_count}, Failed: {failed_count}")
    print(f"📊 SUMMARY: ✅ {success_count} successful, ❌ {failed_count} failed")
    
    if failed_count > 0:
        print(f"❌ Failed tickers: {', '.join(results['failed'])}")
    
    return results

if __name__ == "__main__":
    logger.info("Starting intraday data update job")
    
    # Get custom tickers from command line args if provided
    custom_tickers = sys.argv[1:] if len(sys.argv) > 1 else None
    
    try:
        results = update_intraday_data(tickers=custom_tickers)
        
        if len(results['failed']) > 0:
            logger.warning(f"Some updates failed: {results['failed']}")
        
        logger.info("Intraday data update job completed")
    except Exception as e:
        logger.critical(f"Critical error in intraday update job: {e}")
        traceback.print_exc()
        sys.exit(1)
