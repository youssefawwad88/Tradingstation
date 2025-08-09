import os
import logging
import pandas as pd
from datetime import datetime
import sys
import traceback
import time

# Add the project root to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.config import (
    DEFAULT_TICKERS, 
    INTRADAY_INTERVALS,
    INTRADAY_DATA_DIR,
    DEBUG_MODE
)
from utils.helpers import (
    read_master_tickerlist, save_df_to_s3, read_df_from_s3, update_scheduler_status,
    is_today_present, get_last_market_day, trim_to_rolling_window, detect_market_session,
    load_manual_tickers, is_today, append_new_candles, save_to_local_filesystem,
    apply_data_retention, is_today_present_enhanced
)
from utils.alpha_vantage_api import get_intraday_data

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def test_debug_logging():
    """
    Test the debug logging functionality with sample data.
    This demonstrates the enhanced logging without requiring API access.
    """
    print("🧪 TESTING DEBUG LOGGING FUNCTIONALITY")
    print("="*60)
    
    # Create sample status for successful case
    success_status = {
        'api_fetch_success': True,
        'api_fetch_error': None,
        'new_candles_found': True,
        'data_saved_locally': True,
        'spaces_upload_success': True,
        'spaces_configured': True,
        'save_path': 'data/intraday/AAPL_1min.csv',
        'total_rows': 1250
    }
    
    print("\n✅ TESTING SUCCESSFUL CASE (New Format):")
    log_ticker_debug_status("AAPL", "1min", success_status, debug=True)
    
    # Create sample status for failed case
    failed_status = {
        'api_fetch_success': False,
        'api_fetch_error': 'API limit exceeded',
        'new_candles_found': False,
        'data_saved_locally': True,
        'spaces_upload_success': False,
        'spaces_configured': False,
        'save_path': 'data/intraday/NVDA_1min.csv',
        'total_rows': 0
    }
    
    print("\n❌ TESTING FAILED CASE (New Format):")
    log_ticker_debug_status("NVDA", "1min", failed_status, debug=True)
    
    # Test directory listing
    print("\n📂 TESTING DIRECTORY LISTING:")
    list_intraday_files(debug=True)
    
    print("\n🎯 TEST COMPLETED - All debug functions working properly!")
    print("="*60)

def log_ticker_debug_status(ticker, interval, status_dict, debug=False):
    """
    Log detailed ticker processing status in the EXACT format requested in problem statement.
    
    Args:
        ticker: Stock symbol
        interval: '1min' or '30min'  
        status_dict: Dictionary containing status information
        debug: Whether to show debug logging
    """
    if not debug:
        return
        
    # PHASE 2: Log in the EXACT format requested in the problem statement
    print(f"🎯 Ticker: {ticker}")
    
    # API Fetch Status - exact format from problem statement
    if status_dict.get('api_fetch_success', False):
        print(f"📊 API Fetch: ✅ Success")
    else:
        print(f"📊 API Fetch: ❌ Failed")
    
    # Local Save Path - exact format from problem statement 
    save_path = status_dict.get('save_path', 'Unknown')
    # Convert to /workspace/ path as requested in problem statement
    if not save_path.startswith('/workspace/'):
        # Convert relative path to /workspace/ format as requested
        if save_path.startswith('data/'):
            workspace_path = f"/workspace/{save_path}"
        else:
            workspace_path = f"/workspace/data/intraday/{ticker}_{interval}.csv"
    else:
        workspace_path = save_path
    
    if status_dict.get('data_saved_locally', False):
        print(f"💾 Local Save Path: {workspace_path}")
    else:
        print(f"💾 Local Save Path: ❌ Failed")
    
    # Spaces Upload Status - exact format from problem statement
    if status_dict.get('spaces_upload_success', False):
        print(f"☁️ Spaces Upload: ✅ Success")
    else:
        print(f"☁️ Spaces Upload: ❌ Failed")
    
    # Workspace files listing - exact format from problem statement
    try:
        # Try to list files in the intraday directory
        current_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        intraday_path = os.path.join(current_dir, 'data', 'intraday')
        if os.path.exists(intraday_path):
            files = [f for f in os.listdir(intraday_path) if f.endswith('.csv')]
            print(f"📁 Workspace files: {files}")
        else:
            print(f"📁 Workspace files: []")
    except Exception as e:
        print(f"📁 Workspace files: [] (Error: {e})")

def list_intraday_files(debug=False):
    """
    List actual files in the intraday directories for debugging.
    
    Args:
        debug: Whether to show debug logging
    """
    if not debug:
        return
        
    print(f"\n📂 DEBUG MODE: Directory File Listing")
    print(f"{'='*50}")
    
    try:
        # Get current working directory  
        current_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        
        # Check 1min intraday directory
        intraday_1min_path = os.path.join(current_dir, 'data', 'intraday')
        print(f"📁 Directory: {intraday_1min_path}")
        if os.path.exists(intraday_1min_path):
            files = os.listdir(intraday_1min_path)
            if files:
                for file in sorted(files):
                    print(f"    📄 {file}")
            else:
                print(f"    📂 (empty)")
        else:
            print(f"    ❌ Directory does not exist")
        
        # Check 30min intraday directory  
        intraday_30min_path = os.path.join(current_dir, 'data', 'intraday_30min')
        print(f"\n📁 Directory: {intraday_30min_path}")
        if os.path.exists(intraday_30min_path):
            files = os.listdir(intraday_30min_path)
            if files:
                for file in sorted(files):
                    print(f"    📄 {file}")
            else:
                print(f"    📂 (empty)")
        else:
            print(f"    ❌ Directory does not exist")
            
    except Exception as e:
        print(f"❌ Error listing files: {e}")
    
    print(f"{'='*50}")

def normalize_column_names(df):
    """
    Normalize column names to match existing data format.
    Expected format: Date, Open, High, Low, Close, Volume
    """
    if df.empty:
        return df
    
    # Mapping of possible column names to standard format
    column_mapping = {
        'timestamp': 'Date',
        'date': 'Date',
        'time': 'Date',
        'datetime': 'Date',
        'open': 'Open',
        'high': 'High',  
        'low': 'Low',
        'close': 'Close',
        'volume': 'Volume'
    }
    
    # Create a mapping for the current DataFrame columns
    rename_dict = {}
    for col in df.columns:
        col_lower = col.lower()
        if col_lower in column_mapping:
            rename_dict[col] = column_mapping[col_lower]
    
    # Apply the renaming
    df_renamed = df.rename(columns=rename_dict)
    
    return df_renamed

def resample_to_30min(df_1min):
    """
    Resample 1-minute data to 30-minute candles.
    
    Args:
        df_1min: DataFrame with 1-minute OHLCV data
        
    Returns:
        DataFrame: 30-minute OHLCV data
    """
    if df_1min.empty:
        return pd.DataFrame()
    
    try:
        df = df_1min.copy()
        
        # Handle timestamp column - check for both Date and timestamp columns
        timestamp_col = None
        if 'Date' in df.columns:
            timestamp_col = 'Date'
        elif 'timestamp' in df.columns:
            timestamp_col = 'timestamp'
        
        if timestamp_col:
            df[timestamp_col] = pd.to_datetime(df[timestamp_col], errors='coerce')
            df = df.set_index(timestamp_col)
        else:
            print("Error: No timestamp/Date column found for resampling")
            return pd.DataFrame()
        
        # Drop any rows with invalid timestamps
        df = df.dropna()
        
        # Ensure we have the required columns (check both lowercase and uppercase)
        required_cols = ['open', 'high', 'low', 'close', 'volume']
        available_cols = [col.lower() for col in df.columns]
        
        # Map columns to lowercase for consistency
        col_mapping = {}
        for col in required_cols:
            matching_col = None
            for df_col in df.columns:
                if df_col.lower() == col:
                    matching_col = df_col
                    break
            if matching_col:
                col_mapping[matching_col] = col
        
        if len(col_mapping) < 5:
            print(f"Missing required columns for resampling. Found: {df.columns.tolist()}")
            return pd.DataFrame()
        
        # Rename columns to lowercase
        df = df.rename(columns=col_mapping)
        
        # Resample to 30-minute intervals
        resampled = df.resample('30min').agg({
            'open': 'first',
            'high': 'max',  
            'low': 'min',
            'close': 'last',
            'volume': 'sum'
        }).dropna()
        
        # Reset index to get timestamp back as a column
        resampled = resampled.reset_index()
        
        # Normalize column names to match existing data format
        resampled = normalize_column_names(resampled)
        
        return resampled
        
    except Exception as e:
        print(f"Error in resample_to_30min: {e}")
        return pd.DataFrame()

def test_forced_spaces_upload(ticker="AAPL"):
    """
    OPTIONAL: Test Spaces upload functionality with existing data.
    This bypasses the fetcher and just tests the upload mechanism.
    
    Args:
        ticker: Ticker symbol to test (default: AAPL)
    """
    print(f"\n🧪 TESTING FORCED SPACES UPLOAD FOR {ticker}")
    print(f"{'='*50}")
    
    try:
        # Try to read existing data
        file_path = f"data/intraday/{ticker}_1min.csv"
        print(f"📂 Reading existing data from: {file_path}")
        
        df = pd.read_csv(file_path)
        if df.empty:
            print(f"❌ No existing data found for {ticker} at {file_path}")
            return False
        
        print(f"✅ Loaded {len(df)} rows of data for {ticker}")
        
        # Test the upload
        print(f"🔄 Testing save_df_to_s3 function...")
        from utils.helpers import save_df_to_s3
        
        result = save_df_to_s3(df, file_path)
        
        if result:
            print(f"✅ UPLOAD TEST SUCCESS: save_df_to_s3 returned True")
            
            # Check environment variables for context
            spaces_configured = all([
                os.getenv('SPACES_ACCESS_KEY_ID'),
                os.getenv('SPACES_SECRET_ACCESS_KEY'),
                os.getenv('SPACES_BUCKET_NAME'),
                os.getenv('SPACES_REGION')
            ])
            
            if spaces_configured:
                print(f"✅ Spaces credentials configured - upload likely succeeded")
            else:
                print(f"⚠️  Spaces credentials missing - only local save succeeded")
        else:
            print(f"❌ UPLOAD TEST FAILED: save_df_to_s3 returned False")
            
        print(f"{'='*50}")
        return result
        
    except Exception as e:
        print(f"❌ UPLOAD TEST ERROR: {e}")
        print(f"{'='*50}")
        return False

def process_ticker_interval(ticker, interval, debug=False):
    """
    Process a single ticker for a specific interval (1min or 30min).
    
    Args:
        ticker: Stock symbol
        interval: '1min' or '30min'
        debug: Enable enhanced debug logging with detailed status
        
    Returns:
        bool: True if successful, False otherwise
    """
    if debug:
        print(f"\n{'='*60}")
        print(f"🎯 PROCESSING TICKER: {ticker} ({interval} interval)")
        print(f"{'='*60}")
    
    # Initialize per-ticker status tracking with more detailed information
    ticker_status = {
        'api_fetch_success': False,
        'api_fetch_error': None,
        'new_candles_found': False,
        'data_saved_locally': False,
        'spaces_upload_success': False,
        'spaces_configured': False,
        'save_path': '',
        'total_rows': 0
    }
    
    try:
        # Determine file paths
        if interval == '1min':
            file_path = f'data/intraday/{ticker}_{interval}.csv'
        else:  # 30min
            file_path = f'data/intraday_30min/{ticker}_{interval}.csv'
        
        # Store the save path for debugging
        ticker_status['save_path'] = file_path
        
        # Check if file exists (new ticker vs existing ticker)
        existing_df = read_df_from_s3(file_path)
        is_new_ticker = existing_df.empty
        
        if is_new_ticker:
            logger.info(f"📍 Status: NEW TICKER - Fetching full intraday history for {ticker}")
            
            # For new tickers, fetch full history (outputsize='full')
            if interval == '1min':
                # Fetch 1min data first
                logger.info(f"🔄 API Request: Fetching {interval} data (outputsize='full') for {ticker}...")
                latest_df = get_intraday_data(ticker, interval='1min', outputsize='full')
                
                if latest_df.empty:
                    logger.warning(f"⚠️  API FETCH FAILED: No intraday data returned for new ticker {ticker}. Skipping.")
                    ticker_status['api_fetch_success'] = False
                    ticker_status['api_fetch_error'] = "No data returned from API"
                    
                    # For new tickers that fail API calls, skip processing rather than fail completely
                    logger.warning(f"⚠️  SKIPPING: Cannot process new ticker {ticker} without API data")
                    log_ticker_debug_status(ticker, interval, ticker_status, debug)
                    return False
                else:
                    logger.info(f"✅ API FETCH SUCCESS: Retrieved {len(latest_df)} rows of {interval} data for {ticker}")
                    ticker_status['api_fetch_success'] = True
                
                # Ensure proper column names (API returns different formats)
                if 'timestamp' not in latest_df.columns and len(latest_df.columns) >= 6:
                    latest_df.columns = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
                
                # Normalize column names to match existing format
                latest_df = normalize_column_names(latest_df)
                
                # Save 1min data
                combined_df = latest_df.copy()
                ticker_status['new_candles_found'] = True
                logger.info(f"✅ NEW CANDLES: All {len(combined_df)} candles are new for ticker {ticker}")
                
            else:  # 30min
                # For 30min interval, we need to check if we have 1min data to resample
                min_1_file_path = f'data/intraday/{ticker}_1min.csv'
                min_1_df = read_df_from_s3(min_1_file_path)
                
                if not min_1_df.empty:
                    # Resample from existing 1min data
                    logger.info(f"🔄 RESAMPLING: Using existing 1min data to create 30min for {ticker}")
                    combined_df = resample_to_30min(min_1_df)
                    ticker_status['api_fetch_success'] = True  # Using existing data
                    ticker_status['new_candles_found'] = True
                    logger.info(f"✅ RESAMPLING SUCCESS: Created {len(combined_df)} 30min candles from 1min data")
                else:
                    # Fetch 30min data directly from API
                    logger.info(f"🔄 API Request: Fetching {interval} data (outputsize='full') for {ticker}...")
                    latest_df = get_intraday_data(ticker, interval='30min', outputsize='full')
                    
                    if latest_df.empty:
                        logger.warning(f"⚠️  API FETCH FAILED: No 30min intraday data returned for new ticker {ticker}. Checking for existing data.")
                        ticker_status['api_fetch_success'] = False
                        ticker_status['api_fetch_error'] = "No 30min data returned from API"
                        
                        # For new tickers that fail API calls, skip processing rather than fail completely
                        logger.warning(f"⚠️  SKIPPING: Cannot process new ticker {ticker} without API data")
                        log_ticker_debug_status(ticker, interval, ticker_status, debug)
                        return False
                    else:
                        logger.info(f"✅ API FETCH SUCCESS: Retrieved {len(latest_df)} rows of {interval} data for {ticker}")
                        ticker_status['api_fetch_success'] = True
                    
                    # Ensure proper column names
                    if 'timestamp' not in latest_df.columns and len(latest_df.columns) >= 6:
                        latest_df.columns = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
                    
                    # Normalize column names to match existing format
                    latest_df = normalize_column_names(latest_df)
                    
                    combined_df = latest_df.copy()
                    ticker_status['new_candles_found'] = True
                    logger.info(f"✅ NEW CANDLES: All {len(combined_df)} candles are new for ticker {ticker}")
        else:
            logger.info(f"📍 Status: EXISTING TICKER - Checking for updates for {ticker}")
            
            # Check if today's data is present
            today_present = is_today_present(existing_df)
            
            if not today_present:
                logger.warning(f"⚠️  Today's data missing for {ticker}. Fetching latest data...")
                
                # Check if we're in market hours and should warn
                market_session = detect_market_session()
                if market_session in ['PRE-MARKET', 'REGULAR']:
                    logger.warning(f"⚠️  WARNING: Today's data missing for {ticker} during {market_session} hours")
                
                # Fetch latest compact data (100 rows) to get today's data
                logger.info(f"🔄 API Request: Fetching {interval} data (outputsize='compact') for {ticker}...")
                latest_df = get_intraday_data(ticker, interval=interval, outputsize='compact')
                
                if latest_df.empty:
                    logger.warning(f"⚠️  API FETCH FAILED: No new {interval} data returned for {ticker}. Using existing data.")
                    ticker_status['api_fetch_success'] = False
                    ticker_status['api_fetch_error'] = f"No new {interval} data returned from API (using existing data)"
                    
                    # Fallback: Use existing data when API fails
                    logger.info(f"📊 FALLBACK: Using existing {len(existing_df)} rows for {ticker} due to API unavailability")
                    combined_df = existing_df.copy()
                    ticker_status['new_candles_found'] = False
                    latest_df = pd.DataFrame()  # Empty, will use existing data
                else:
                    logger.info(f"✅ API FETCH SUCCESS: Retrieved {len(latest_df)} rows of {interval} data for {ticker}")
                    ticker_status['api_fetch_success'] = True
                
                # Ensure proper column names
                if 'timestamp' not in latest_df.columns and len(latest_df.columns) >= 6:
                    latest_df.columns = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
                
                # Normalize column names to match existing format
                latest_df = normalize_column_names(latest_df)
                    
            else:
                logger.info(f"✅ Today's data already present for {ticker}. Fetching latest updates...")
                
                # Fetch compact data to get any new candles
                logger.info(f"🔄 API Request: Fetching {interval} data (outputsize='compact') for {ticker}...")
                latest_df = get_intraday_data(ticker, interval=interval, outputsize='compact')
                
                if latest_df.empty:
                    logger.warning(f"⚠️  API FETCH WARNING: No new {interval} data returned for {ticker}. Using existing data.")
                    ticker_status['api_fetch_success'] = True  # Not really an error if no new data
                    ticker_status['new_candles_found'] = False
                    latest_df = pd.DataFrame()  # Empty, will use existing data
                else:
                    logger.info(f"✅ API FETCH SUCCESS: Retrieved {len(latest_df)} rows of {interval} data for {ticker}")
                    ticker_status['api_fetch_success'] = True
                    
                    # Ensure proper column names
                    if 'timestamp' not in latest_df.columns and len(latest_df.columns) >= 6:
                        latest_df.columns = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
                    
                    # Normalize column names to match existing format
                    latest_df = normalize_column_names(latest_df)
            
            # Combine existing and new data using enhanced deduplication with date validation
            if not latest_df.empty:
                logger.info(f"🔄 PROCESSING: Combining new data with existing {len(existing_df)} rows for {ticker}...")
                # Use the new append_new_candles function for better deduplication and date validation
                success = append_new_candles(ticker, latest_df, file_path)
                if not success:
                    logger.error(f"❌ DATA COMBINATION FAILED: Could not append new candles for {ticker}")
                    ticker_status['new_candles_found'] = False
                    return False
                
                # Read the updated combined data
                combined_df = read_df_from_s3(file_path)
                if combined_df.empty:
                    logger.error(f"❌ DATA READ ERROR: No data found after appending for {ticker}")
                    return False
                    
                # Calculate new candles added
                new_candles_count = len(combined_df) - len(existing_df)
                if new_candles_count > 0:
                    ticker_status['new_candles_found'] = True
                    logger.info(f"✅ NEW CANDLES: Added {new_candles_count} new candles for {ticker} (total: {len(combined_df)})")
                else:
                    ticker_status['new_candles_found'] = False
                    logger.info(f"📊 NO NEW CANDLES: No new data to add for {ticker} (total: {len(combined_df)})")
            else:
                combined_df = existing_df.copy()
                ticker_status['new_candles_found'] = False
                logger.info(f"📊 USING EXISTING: No new data to process, using existing {len(combined_df)} rows for {ticker}")
        
        # Apply enhanced data retention with new configuration (Phase 4)
        if not combined_df.empty:
            logger.info(f"🔄 PROCESSING: Applying enhanced data retention for {ticker}...")
            
            # Import the new config values
            from utils.config import INTRADAY_TRIM_DAYS
            
            # Use the new apply_data_retention function instead of trim_to_rolling_window
            combined_df = apply_data_retention(combined_df, INTRADAY_TRIM_DAYS)
            
            # Sort by timestamp (chronological order - oldest to newest, which matches existing format)
            timestamp_col = 'Date' if 'Date' in combined_df.columns else 'timestamp'
            combined_df.sort_values(by=timestamp_col, ascending=True, inplace=True)
            ticker_status['total_rows'] = len(combined_df)
            
            # Save the updated file back to S3/local (only if we have data)
            print(f"💾 SAVING: Attempting to save {len(combined_df)} rows for {ticker} to {file_path}...")
            
            # Check Spaces configuration before attempting save
            spaces_access_key = os.getenv('SPACES_ACCESS_KEY_ID')
            spaces_secret_key = os.getenv('SPACES_SECRET_ACCESS_KEY')
            spaces_bucket = os.getenv('SPACES_BUCKET_NAME')
            spaces_region = os.getenv('SPACES_REGION')
            spaces_configured = all([spaces_access_key, spaces_secret_key, spaces_bucket, spaces_region])
            ticker_status['spaces_configured'] = spaces_configured
            
            upload_success = save_df_to_s3(combined_df, file_path)
            
            if not upload_success:
                print(f"❌ SAVE FAILED: Could not save {ticker} data to storage: {file_path}")
                print(f"    This ticker data may be lost!")
                ticker_status['data_saved_locally'] = False
                ticker_status['spaces_upload_success'] = False
                return False
            else:
                # Check what save_df_to_s3 actually accomplished
                ticker_status['data_saved_locally'] = True
                
                if spaces_configured:
                    # Spaces was configured, so save_df_to_s3 attempted Spaces upload
                    # Unfortunately save_df_to_s3 doesn't return separate status for local vs Spaces
                    # We'll assume Spaces succeeded if overall save succeeded and creds are configured
                    ticker_status['spaces_upload_success'] = True
                    print(f"✅ SAVE SUCCESS: {ticker} data saved locally AND uploaded to Spaces: {file_path}")
                else:
                    ticker_status['spaces_upload_success'] = False
                    print(f"✅ SAVE SUCCESS: {ticker} data saved locally (Spaces disabled): {file_path}")
                    print(f"⚠️  SPACES UPLOAD: SKIPPED - No Spaces credentials configured")
        
        # Final per-ticker status report (always show key information)
        logger.info(f"📋 TICKER STATUS: {ticker} ({interval})")
        logger.info(f"   📊 API Fetch: {'✅ SUCCESS' if ticker_status['api_fetch_success'] else '❌ FAILED'}")
        if ticker_status['api_fetch_error']:
            logger.warning(f"      Error: {ticker_status['api_fetch_error']}")
        logger.info(f"   🆕 New Candles: {'✅ FOUND' if ticker_status['new_candles_found'] else '📊 NONE'}")
        logger.info(f"   💾 Local Save: {'✅ SUCCESS' if ticker_status['data_saved_locally'] else '❌ FAILED'}")
        logger.info(f"   ☁️  Spaces Upload: {'✅ SUCCESS' if ticker_status['spaces_upload_success'] else '❌ FAILED/DISABLED'}")
        logger.info(f"   📈 Total Rows: {ticker_status['total_rows']}")
        
        if debug:
            print(f"\n📋 TICKER STATUS SUMMARY: {ticker} ({interval})")
            print(f"{'='*50}")
            print(f"🎯 Ticker: {ticker}")
            print(f"📊 API Fetch: {'✅ SUCCESS' if ticker_status['api_fetch_success'] else '❌ FAILED'}")
            if ticker_status['api_fetch_error']:
                print(f"    Error: {ticker_status['api_fetch_error']}")
            print(f"🆕 New Candles: {'✅ FOUND' if ticker_status['new_candles_found'] else '📊 NONE'}")
            print(f"💾 Local Save: {'✅ SUCCESS' if ticker_status['data_saved_locally'] else '❌ FAILED'}")
            print(f"☁️  Spaces Upload: {'✅ SUCCESS' if ticker_status['spaces_upload_success'] else '❌ FAILED/DISABLED'}")
            print(f"📈 Total Rows: {ticker_status['total_rows']}")
        
        # Check if today's data is now present using enhanced verification
        timestamp_col = 'Date' if 'Date' in combined_df.columns else 'timestamp'
        if not is_today_present_enhanced(combined_df, timestamp_col):
            logger.warning(f"⚠️  WARNING: Today's data still missing for {ticker} after update")
        else:
            logger.info(f"✅ Today's data confirmed present for {ticker}")
        
        if debug:
            print(f"{'='*50}")
            print(f"✅ COMPLETED: {ticker} ({interval}) processing finished successfully")
        
        # Enhanced debug logging if enabled
        log_ticker_debug_status(ticker, interval, ticker_status, debug)
        
        return True
        
    except Exception as e:
        logger.error(f"❌ CRITICAL ERROR processing {ticker} for {interval}: {e}")
        logger.error(f"📋 TICKER STATUS: {ticker} ({interval})")
        logger.error(f"   📊 API Fetch: {'✅ SUCCESS' if ticker_status.get('api_fetch_success', False) else '❌ FAILED'}")
        logger.error(f"   🆕 New Candles: {'✅ FOUND' if ticker_status.get('new_candles_found', False) else '❌ FAILED'}")
        logger.error(f"   💾 Local Save: {'✅ SUCCESS' if ticker_status.get('data_saved_locally', False) else '❌ FAILED'}")
        logger.error(f"   ☁️  Spaces Upload: {'✅ SUCCESS' if ticker_status.get('spaces_upload_success', False) else '❌ FAILED'}")
        logger.error(f"   ❌ Error: {str(e)}")
        
        if debug:
            print(f"\n❌ CRITICAL ERROR processing {ticker} for {interval}: {e}")
            print(f"📋 TICKER STATUS SUMMARY: {ticker} ({interval})")
            print(f"{'='*50}")
            print(f"🎯 Ticker: {ticker}")
            print(f"📊 API Fetch: {'✅ SUCCESS' if ticker_status.get('api_fetch_success', False) else '❌ FAILED'}")
            print(f"🆕 New Candles: {'✅ FOUND' if ticker_status.get('new_candles_found', False) else '❌ FAILED'}")
            print(f"💾 Local Save: {'✅ SUCCESS' if ticker_status.get('data_saved_locally', False) else '❌ FAILED'}")
            print(f"☁️  Spaces Upload: {'✅ SUCCESS' if ticker_status.get('spaces_upload_success', False) else '❌ FAILED'}")
            print(f"❌ Error: {str(e)}")
            print(f"{'='*50}")
        
        # Enhanced debug logging if enabled (for errors too)
        log_ticker_debug_status(ticker, interval, ticker_status, debug)
        
        return False
def run_compact_append(debug=False):
    """
    Runs the enhanced intraday data update process.
    - Handles both new and existing tickers
    - Ensures today's data is always included
    - Maintains rolling window of last 5 days + current day
    - Provides robust error handling and warnings
    
    Args:
        debug: Enable enhanced debug logging with detailed status for each ticker
        
    Returns:
        dict: Summary of processing results for orchestrator reporting
    """
    logger.info("--- Starting Enhanced Intraday Data Update Job ---")
    
    if debug:
        print("🧪 DEBUG MODE: Enhanced logging enabled")
        print("    Additional detailed status will be shown for each ticker")
    
    # PHASE 3: Enhanced Environment Variable Logging (always show in production)
    logger.info("📦 DigitalOcean Spaces Configuration Check:")
    spaces_access_key = os.getenv('SPACES_ACCESS_KEY_ID')
    spaces_secret_key = os.getenv('SPACES_SECRET_ACCESS_KEY')
    spaces_bucket = os.getenv('SPACES_BUCKET_NAME')
    spaces_region = os.getenv('SPACES_REGION')
    
    logger.info(f"    SPACES_ACCESS_KEY_ID = {spaces_access_key if spaces_access_key else '❌ Not Set'}")
    logger.info(f"    SPACES_SECRET_ACCESS_KEY = {'*' * 8 if spaces_secret_key else '❌ Not Set'}")
    logger.info(f"    SPACES_BUCKET_NAME = {spaces_bucket if spaces_bucket else '❌ Not Set'}")
    logger.info(f"    SPACES_REGION = {spaces_region if spaces_region else '❌ Not Set'}")
    logger.info(f"    Local save path: /workspace/data/intraday/")
    
    # Overall Spaces status (always log in production)
    spaces_configured = all([spaces_access_key, spaces_secret_key, spaces_bucket, spaces_region])
    if spaces_configured:
        logger.info("✅ DigitalOcean Spaces: FULLY CONFIGURED - Data will be uploaded to cloud")
    else:
        logger.warning("⚠️  DigitalOcean Spaces: INCOMPLETE CONFIGURATION - Using local filesystem only")
        logger.warning("    Missing credentials will prevent cloud upload!")
    
    if debug:
        # Additional detailed environment check for troubleshooting (debug only)
        print("\n=== DETAILED ENVIRONMENT VARIABLES VERIFICATION ===")
        
        # Alpha Vantage API Key
        api_key = os.getenv('ALPHA_VANTAGE_API_KEY')
        if not api_key:
            print("❌ CRITICAL: ALPHA_VANTAGE_API_KEY environment variable not set")
            print("    Data fetching will fail!")
        else:
            print(f"✅ ALPHA_VANTAGE_API_KEY configured: {api_key[:8]}***{api_key[-4:] if len(api_key) > 12 else '***'}")
        
        print("\n--- DigitalOcean Spaces Configuration ---")
        if spaces_access_key:
            print(f"✅ SPACES_ACCESS_KEY_ID: {spaces_access_key[:8]}***{spaces_access_key[-4:] if len(spaces_access_key) > 12 else '***'}")
        else:
            print("❌ SPACES_ACCESS_KEY_ID: Not set")
        
        if spaces_secret_key:
            print(f"✅ SPACES_SECRET_ACCESS_KEY: {spaces_secret_key[:8]}***{spaces_secret_key[-4:] if len(spaces_secret_key) > 12 else '***'}")
        else:
            print("❌ SPACES_SECRET_ACCESS_KEY: Not set")
        
        if spaces_bucket:
            print(f"✅ SPACES_BUCKET_NAME: {spaces_bucket}")
        else:
            print("❌ SPACES_BUCKET_NAME: Not set")
        
        if spaces_region:
            print(f"✅ SPACES_REGION: {spaces_region}")
        else:
            print("❌ SPACES_REGION: Not set")
        
        # Overall Spaces status
        if spaces_configured:
            print("✅ DigitalOcean Spaces: FULLY CONFIGURED")
        else:
            print("⚠️  DigitalOcean Spaces: INCOMPLETE CONFIGURATION")
            print("    Missing credentials will cause Spaces upload to silently fail!")
            print("    Using local filesystem fallback for data persistence")
        
        print("=" * 50)
    
    # Load tickers from master_tickerlist.csv (unified source)
    tickers = read_master_tickerlist()
    
    # PRIORITY ISSUE 4 FIX: Enhanced master ticker diagnostic logging
    logger.info(f"📊 MASTER TICKER DEBUG - Total tickers in master list: {len(tickers) if tickers else 0}")
    if tickers:
        logger.info(f"📊 MASTER TICKER LIST: {tickers}")
        if len(tickers) != 13:
            logger.warning(f"⚠️  Expected 13 master tickers but got {len(tickers)} - investigating source...")
    else:
        logger.error("❌ NO MASTER TICKERS LOADED - Critical error!")
    
    # Load manual tickers for tracking manual ticker processing
    manual_tickers = load_manual_tickers()
    
    # PRIORITY ISSUE 1 FIX: Enhanced manual ticker logging
    logger.info(f"🎯 MANUAL TICKER DEBUG - Total manual tickers loaded: {len(manual_tickers) if manual_tickers else 0}")
    if manual_tickers:
        logger.info(f"🎯 MANUAL TICKER LIST: {manual_tickers}")
        # Check which manual tickers are also in master list
        manual_in_master = [t for t in manual_tickers if t in tickers] if tickers else []
        manual_not_in_master = [t for t in manual_tickers if t not in tickers] if tickers else manual_tickers
        
        if manual_in_master:
            logger.info(f"✅ Manual tickers found in master list ({len(manual_in_master)}): {manual_in_master}")
        if manual_not_in_master:
            logger.warning(f"⚠️  Manual tickers NOT in master list ({len(manual_not_in_master)}): {manual_not_in_master}")
    else:
        logger.warning("⚠️  NO MANUAL TICKERS LOADED - This is the source of 0/7 manual ticker processing!")
    
    if not tickers:
        logger.error("No tickers to process. Exiting job.")
        return {
            'success': False,
            'total_tickers': 0,
            'successful_tickers': 0,
            'storage_location': 'N/A',
            'manual_tickers_status': 'N/A'
        }

    logger.info(f"🚀 Processing {len(tickers)} tickers from master_tickerlist.csv for intraday updates: {tickers}")
    logger.info(f"📊 Storage configuration: {'Cloud (Spaces) + Local' if spaces_configured else 'Local filesystem only'}")
    
    # Track processing results
    success_count = 0
    total_operations = len(tickers) * 2  # Both 1min and 30min for each ticker
    manual_ticker_results = {}  # Track manual ticker processing specifically
    ticker_processing_summary = []  # Track per-ticker results for summary
    
    # PRIORITY ISSUE 4 FIX: Track all ticker processing attempts for diagnostics
    ticker_attempts = []  # Track which tickers we attempt to process
    ticker_successes = []  # Track which tickers succeed
    ticker_failures = []  # Track which tickers fail with reasons
    
    for ticker in tickers:
        # PRIORITY ISSUE 4 FIX: Log every ticker attempt
        ticker_attempts.append(ticker)
        logger.info(f"🔄 ATTEMPTING TICKER: {ticker} ({len(ticker_attempts)}/{len(tickers)})")
        
        if debug:
            print(f"\n{'='*70}")
            print(f"🚀 STARTING TICKER PROCESSING: {ticker}")
        is_manual_ticker = ticker in (manual_tickers if manual_tickers else [])
        if is_manual_ticker:
            logger.info(f"🎯 ⭐ Processing MANUAL TICKER: {ticker} - This MUST succeed for production!")
        else:
            logger.info(f"🎯 Processing ticker: {ticker}")
        if debug:
            print(f"{'='*70}")
        
        # Track ticker-level results
        ticker_result = {'ticker': ticker, 'manual': is_manual_ticker, '1min': False, '30min': False, 'save_location': 'Failed'}
        
        # Process 1-minute interval first
        if debug:
            print(f"\n🔄 PHASE 1: Processing {ticker} for 1-minute interval...")
        success_1min = process_ticker_interval(ticker, '1min', debug)
        if success_1min:
            success_count += 1
            ticker_result['1min'] = True
        
        # Process 30-minute interval  
        # Note: For 30min, we prefer resampling from 1min data when available
        if debug:
            print(f"\n🔄 PHASE 2: Processing {ticker} for 30-minute interval...")
        success_30min = process_ticker_interval(ticker, '30min', debug)
        if success_30min:
            success_count += 1
            ticker_result['30min'] = True
        
        # Determine save location for this ticker
        overall_success = success_1min and success_30min
        if overall_success:
            ticker_result['save_location'] = 'Cloud (Spaces) + Local' if spaces_configured else 'Local only'
            ticker_successes.append(ticker)
        else:
            failure_reason = []
            if not success_1min:
                failure_reason.append("1min failed")
            if not success_30min:
                failure_reason.append("30min failed")
            ticker_failures.append(f"{ticker}: {', '.join(failure_reason)}")
        
        ticker_processing_summary.append(ticker_result)
        
        # Track manual ticker results specifically
        if is_manual_ticker:
            manual_ticker_results[ticker] = {
                '1min': success_1min,
                '30min': success_30min,
                'overall': overall_success
            }
        
        # Production-friendly per-ticker summary (always show)
        logger.info(f"📊 TICKER COMPLETED: {ticker} | 1min: {'✅' if success_1min else '❌'} | 30min: {'✅' if success_30min else '❌'} | Storage: {ticker_result['save_location']}")
        
        # Per-ticker final summary (debug only)
        if debug:
            overall_success = success_1min and success_30min
            print(f"\n📊 FINAL TICKER SUMMARY: {ticker}")
            print(f"{'='*50}")
            print(f"🎯 Ticker: {ticker}")
            print(f"⏱️  1min interval: {'✅ SUCCESS' if success_1min else '❌ FAILED'}")
            print(f"⏱️  30min interval: {'✅ SUCCESS' if success_30min else '❌ FAILED'}")
            print(f"🏁 Overall: {'✅ SUCCESS' if overall_success else '❌ FAILED'}")
            if is_manual_ticker:
                print(f"⭐ MANUAL TICKER STATUS: {'✅ PRODUCTION READY' if overall_success else '❌ PRODUCTION ISSUE'}")
            print(f"{'='*50}")
        
        # Respect API rate limits
        time.sleep(1)

    # Job completion summary (always show in production)
    logger.info(f"🏁 Enhanced Intraday Data Update Job Completed")
    logger.info(f"📈 Success rate: {success_count}/{total_operations} operations")
    
    # PRIORITY ISSUE 4 FIX: Detailed ticker processing summary
    logger.info(f"🎯 TICKER PROCESSING SUMMARY:")
    logger.info(f"   📊 Total tickers attempted: {len(ticker_attempts)}")
    logger.info(f"   ✅ Successful tickers: {len(ticker_successes)}")
    logger.info(f"   ❌ Failed tickers: {len(ticker_failures)}")
    
    if ticker_successes:
        logger.info(f"   ✅ Successfully processed: {ticker_successes}")
    
    if ticker_failures:
        logger.warning(f"   ❌ Failed tickers with reasons:")
        for failure in ticker_failures:
            logger.warning(f"      {failure}")
    
    # Summary of where data was saved
    successful_tickers = [r for r in ticker_processing_summary if r['1min'] and r['30min']]
    failed_tickers = [r for r in ticker_processing_summary if not (r['1min'] and r['30min'])]
    
    if successful_tickers:
        logger.info(f"✅ Successfully processed {len(successful_tickers)} tickers:")
        for ticker_info in successful_tickers:
            logger.info(f"   📊 {ticker_info['ticker']}: Saved to {ticker_info['save_location']}")
    
    if failed_tickers:
        logger.warning(f"❌ Failed to process {len(failed_tickers)} tickers:")
        for ticker_info in failed_tickers:
            logger.warning(f"   📊 {ticker_info['ticker']}: Processing failed")

    if debug:
        print(f"\n{'='*60}")
        print(f"Enhanced Intraday Data Update Job Completed")
        print(f"Success rate: {success_count}/{total_operations} operations")
    
    # Show file listing if debug mode is enabled
    list_intraday_files(debug)
    
    # Report manual ticker status specifically (always show if there are manual tickers)
    if manual_ticker_results:
        logger.info(f"🎯 MANUAL TICKER STATUS REPORT:")
        failed_manual_tickers = []
        successful_manual_tickers = []
        for ticker, results in manual_ticker_results.items():
            overall_status = "✅ SUCCESS" if results['overall'] else "❌ FAILED"
            
            if results['overall']:
                successful_manual_tickers.append(ticker)
                logger.info(f"   ✅ {ticker}: SUCCESS - Available in {'cloud + local' if spaces_configured else 'local'} storage")
            else:
                failed_manual_tickers.append(ticker)
                logger.warning(f"   ❌ {ticker}: FAILED - Check logs above for details")
        
        if failed_manual_tickers:
            logger.error(f"❌ CRITICAL: {len(failed_manual_tickers)} manual tickers FAILED: {failed_manual_tickers}")
            logger.error(f"   These tickers will NOT appear in production storage!")
        else:
            logger.info(f"✅ SUCCESS: All {len(manual_ticker_results)} manual tickers processed successfully!")
            
        if debug:
            print(f"\n🎯 MANUAL TICKER STATUS REPORT:")
            print(f"{'='*40}")
            for ticker, results in manual_ticker_results.items():
                status_1min = "✅" if results['1min'] else "❌"
                status_30min = "✅" if results['30min'] else "❌"
                overall_status = "✅ SUCCESS" if results['overall'] else "❌ FAILED"
                
                print(f"{ticker}: {overall_status}")
                print(f"  1min: {status_1min}  30min: {status_30min}")
                
                if not results['overall']:
                    failed_manual_tickers.append(ticker)
            
            if failed_manual_tickers:
                print(f"\n❌ CRITICAL: {len(failed_manual_tickers)} manual tickers FAILED:")
                print(f"    {failed_manual_tickers}")
                print(f"    These tickers will NOT appear in production Spaces storage!")
                print(f"    Check DigitalOcean Spaces credentials and connectivity.")
            else:
                print(f"\n✅ SUCCESS: All {len(manual_ticker_results)} manual tickers processed successfully!")
                print(f"    Manual tickers should now be available in Spaces storage.")
    
    if debug:
        print(f"{'='*60}")
    
    # Return summary for orchestrator reporting
    return {
        'success': True,
        'total_tickers': len(tickers),
        'successful_tickers': len(successful_tickers),
        'failed_tickers': len(failed_tickers),
        'storage_location': 'Cloud (Spaces) + Local' if spaces_configured else 'Local only',
        'manual_tickers_total': len(manual_ticker_results) if manual_ticker_results else 0,
        'manual_tickers_failed': len([t for t, r in manual_ticker_results.items() if not r['overall']]) if manual_ticker_results else 0
    }

if __name__ == "__main__":
    # PHASE 4: Add DEBUG_MODE environment variable support (as requested in problem statement)
    import sys
    
    # Check for debug mode from multiple sources
    debug_mode = False
    
    # 1. Environment variable DEBUG_MODE
    debug_env = os.getenv("DEBUG_MODE", "false").lower() == "true"
    
    # 2. Command line arguments
    debug_cli = "--debug" in sys.argv or "-d" in sys.argv
    
    # Enable debug if either source requests it
    debug_mode = debug_env or debug_cli
    
    if debug_mode:
        print("🛠 DEBUG ON: Verbose logging enabled.")
    
    job_name = "update_intraday_compact"
    update_scheduler_status(job_name, "Running")
    try:
        result = run_compact_append(debug=debug_mode)
        
        # Output concise summary for orchestrator logs (always shown)
        if result and result.get('success', False):
            print(f"📋 ORCHESTRATOR SUMMARY: Processed {result['successful_tickers']}/{result['total_tickers']} tickers | Storage: {result['storage_location']} | Manual tickers: {result['manual_tickers_total'] - result['manual_tickers_failed']}/{result['manual_tickers_total']} OK")
        else:
            print(f"📋 ORCHESTRATOR SUMMARY: Job failed or no tickers processed")
        
        update_scheduler_status(job_name, "Success")
    except Exception as e:
        error_message = f"An unexpected error occurred: {e}"
        logger.error(error_message)
        print(f"❌ ORCHESTRATOR SUMMARY: Intraday update failed - {error_message}")
        update_scheduler_status(job_name, "Fail", error_message)
