#!/usr/bin/env python3
"""
Force Live Update All Data Script

Critical Instructions: Override TEST MODE for Alpha Vantage Data
IMMEDIATE ACTION REQUIRED:

This script OVERRIDES TEST MODE and makes LIVE API CALLS even on weekends
It runs the UPDATE_ALL_DATA JOB with detailed logging and verification.

Usage:
    python force_live_update_all_data.py --verbose
"""

import os
import sys
import logging
import argparse
from datetime import datetime, timedelta

# Force live API mode regardless of day/time - OVERRIDE TEST MODE
os.environ['FORCE_LIVE_API'] = 'true'
os.environ['TEST_MODE'] = 'false'
os.environ['MODE'] = 'production'

# Add project root to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '.')))

# Import update_all_data function and other utilities
from jobs.update_all_data import run_full_rebuild
from utils.helpers import read_df_from_s3
from utils.config import ALPHA_VANTAGE_API_KEY

def setup_detailed_logging(verbose=False):
    """
    Set up detailed logging for the force live update process.
    """
    log_level = logging.DEBUG if verbose else logging.INFO
    
    # Configure logging with detailed format
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(levelname)s - %(name)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(f'/tmp/force_live_update_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')
        ]
    )
    
    logger = logging.getLogger(__name__)
    return logger

def search_tickerlist_files():
    """
    Search ALL possible locations for tickerlist.txt as specified.
    
    Returns:
        tuple: (path_found, tickers_list)
    """
    logger = logging.getLogger(__name__)
    
    # Search ALL possible locations for tickerlist.txt
    ticker_paths = [
        '/workspace/tickerlist.txt',
        '/workspace/data/tickerlist.txt', 
        './tickerlist.txt',
        '../tickerlist.txt',
        os.path.join(os.path.dirname(__file__), 'tickerlist.txt'),
        os.path.join(os.path.dirname(__file__), 'data', 'tickerlist.txt')
    ]
    
    logger.info("🔍 SEARCHING FOR TICKERLIST.TXT IN MULTIPLE LOCATIONS")
    
    for path in ticker_paths:
        abs_path = os.path.abspath(path)
        logger.info(f"   Checking: {abs_path}")
        
        if os.path.exists(abs_path):
            try:
                with open(abs_path, 'r') as f:
                    tickers = [line.strip().upper() for line in f if line.strip()]
                    
                logger.info(f"✅ FOUND TICKERLIST at {abs_path}")
                logger.info(f"📋 LOADED {len(tickers)} TICKERS: {', '.join(tickers[:5])}{'...' if len(tickers) > 5 else ''}")
                return abs_path, tickers
                
            except Exception as e:
                logger.error(f"❌ Error reading {abs_path}: {e}")
                continue
    
    # Fallback to default tickers if no file found
    logger.warning("⚠️ No tickerlist.txt found in any location - using default tickers")
    from utils.config import DEFAULT_TICKERS
    return None, DEFAULT_TICKERS

def validate_saved_data(ticker, timeframe, file_path):
    """
    Validate that data was saved successfully with detailed verification.
    
    Args:
        ticker (str): Ticker symbol
        timeframe (str): Data timeframe (1min, 30min, daily)
        file_path (str): Path where data should be saved
        
    Returns:
        bool: True if validation successful
    """
    logger = logging.getLogger(__name__)
    
    try:
        # Try to read the saved data
        result_df = read_df_from_s3(file_path)
        
        if not result_df.empty:
            rows = len(result_df)
            
            # Get date range for verification
            date_col = 'Date' if 'Date' in result_df.columns else 'datetime' if 'datetime' in result_df.columns else 'timestamp'
            if date_col in result_df.columns:
                date_range = f"{result_df[date_col].min()} to {result_df[date_col].max()}"
            else:
                date_range = "Date column not found"
            
            logger.info(f"✅ VERIFIED {ticker} {timeframe} data: {rows} rows, {date_range}")
            return True
        else:
            logger.error(f"❌ VERIFICATION FAILED: {file_path} exists but is empty")
            return False
            
    except Exception as e:
        logger.error(f"❌ VERIFICATION FAILED: {file_path} - {e}")
        return False

def update_all_data_with_override(force_live=True, verbose=False):
    """
    Run the update_all_data job with LIVE API override and detailed logging.
    
    Args:
        force_live (bool): Force live API calls even in test mode
        verbose (bool): Enable verbose logging
    """
    logger = logging.getLogger(__name__)
    
    # Log the override status
    logger.info("=" * 80)
    logger.info("🚀 FORCE LIVE UPDATE ALL DATA - OVERRIDE MODE ACTIVE")
    logger.info("=" * 80)
    logger.info("🔥 TEST MODE OVERRIDDEN - MAKING LIVE API CALLS")
    logger.info("🌐 Alpha Vantage API calls will be made regardless of weekend/time")
    logger.info(f"🔑 API Key available: {'✅ YES' if ALPHA_VANTAGE_API_KEY else '❌ NO'}")
    logger.info("=" * 80)
    
    if not ALPHA_VANTAGE_API_KEY:
        logger.error("❌ CRITICAL: No Alpha Vantage API key found!")
        logger.error("   Set ALPHA_VANTAGE_API_KEY environment variable")
        return False
    
    # Search for ticker list
    ticker_file_path, tickers = search_tickerlist_files()
    
    if not tickers:
        logger.error("❌ No tickers found to process!")
        return False
    
    logger.info(f"📊 PROCESSING {len(tickers)} TICKERS FOR LIVE DATA UPDATE")
    logger.info("=" * 80)
    
    # Import and patch the update_all_data to ensure it runs in live mode
    import jobs.update_all_data as update_module
    
    # Override any test mode detection within the module
    if hasattr(update_module, 'should_use_test_mode'):
        original_test_mode = update_module.should_use_test_mode
        update_module.should_use_test_mode = lambda: False
        logger.info("🔧 Patched should_use_test_mode() to return False")
    
    try:
        # Run the full rebuild process with live data
        logger.info("🚀 STARTING LIVE DATA REBUILD PROCESS")
        logger.info("   This will make REAL API calls to Alpha Vantage")
        logger.info("   Processing ALL timeframes: daily, 30min, 1min")
        
        # Call the run_full_rebuild function
        run_full_rebuild()
        
        logger.info("✅ LIVE DATA REBUILD COMPLETED SUCCESSFULLY")
        
        # Post-processing verification
        logger.info("🔍 STARTING POST-PROCESSING VERIFICATION")
        verification_results = []
        
        for ticker in tickers[:5]:  # Verify first 5 tickers as sample
            logger.info(f"🔍 Verifying saved data for {ticker}...")
            
            # Standard paths as specified
            paths_to_verify = [
                (f"data/daily/{ticker}_daily.csv", "daily"),
                (f"data/intraday_30min/{ticker}_30min.csv", "30min"),
                (f"data/intraday/{ticker}_1min.csv", "1min")
            ]
            
            ticker_verified = True
            for path, timeframe in paths_to_verify:
                if not validate_saved_data(ticker, timeframe, path):
                    ticker_verified = False
            
            verification_results.append((ticker, ticker_verified))
        
        # Summary
        successful_verifications = sum(1 for _, verified in verification_results if verified)
        logger.info("=" * 80)
        logger.info("📊 VERIFICATION SUMMARY:")
        logger.info(f"   Tickers verified: {successful_verifications}/{len(verification_results)}")
        
        for ticker, verified in verification_results:
            status = "✅ PASS" if verified else "❌ FAIL"
            logger.info(f"   {ticker}: {status}")
        
        logger.info("=" * 80)
        logger.info("🎉 FORCE LIVE UPDATE PROCESS COMPLETED")
        return True
        
    except Exception as e:
        logger.error(f"❌ CRITICAL ERROR during live update: {e}")
        logger.exception("Full error details:")
        return False
    
    finally:
        # Restore original test mode function if we patched it
        if hasattr(update_module, 'should_use_test_mode') and 'original_test_mode' in locals():
            update_module.should_use_test_mode = original_test_mode

def main():
    """
    Main entry point for the force live update script.
    """
    parser = argparse.ArgumentParser(
        description="Force live update of all data - Override test mode and make live API calls"
    )
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Enable verbose logging'
    )
    
    args = parser.parse_args()
    
    # Set up logging
    logger = setup_detailed_logging(args.verbose)
    
    # Log startup
    logger.info("🚀 FORCE LIVE UPDATE ALL DATA SCRIPT STARTED")
    logger.info(f"   Verbose mode: {'ON' if args.verbose else 'OFF'}")
    logger.info(f"   Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Run the update process
    success = update_all_data_with_override(force_live=True, verbose=args.verbose)
    
    if success:
        logger.info("🎉 Script completed successfully!")
        sys.exit(0)
    else:
        logger.error("❌ Script failed!")
        sys.exit(1)

if __name__ == "__main__":
    main()