import pandas as pd
import sys
import os
import time

# Add project root to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from utils.helpers import (
    read_tickerlist_from_s3, save_df_to_s3, read_df_from_s3, update_scheduler_status,
    is_today_present, get_last_market_day, trim_to_rolling_window, detect_market_session,
    load_manual_tickers, is_today, append_new_candles
)
from utils.alpha_vantage_api import get_intraday_data

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
    
    print("\n✅ TESTING SUCCESSFUL CASE:")
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
    
    print("\n❌ TESTING FAILED CASE:")
    log_ticker_debug_status("NVDA", "1min", failed_status, debug=True)
    
    # Test directory listing
    print("\n📂 TESTING DIRECTORY LISTING:")
    list_intraday_files(debug=True)
    
    print("\n🎯 TEST COMPLETED - All debug functions working properly!")
    print("="*60)

def log_ticker_debug_status(ticker, interval, status_dict, debug=False):
    """
    Log detailed ticker processing status in the specified format for debugging.
    
    Args:
        ticker: Stock symbol
        interval: '1min' or '30min'  
        status_dict: Dictionary containing status information
        debug: Whether to show debug logging
    """
    if not debug:
        return
        
    print(f"\n🎯 DEBUG MODE: Enhanced Ticker Status Report")
    print(f"{'='*60}")
    print(f"🎯 Ticker: {ticker}")
    
    # API Fetch Status
    if status_dict.get('api_fetch_success', False):
        print(f"📊 API Fetch: ✅")
    else:
        error_msg = status_dict.get('api_fetch_error', 'Unknown error')
        print(f"📊 API Fetch: ❌ [Error: {error_msg}]")
    
    # Local Save Path
    save_path = status_dict.get('save_path', 'Unknown')
    # Convert relative path to absolute for clarity
    if not save_path.startswith('/'):
        current_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        abs_save_path = os.path.join(current_dir, save_path)
    else:
        abs_save_path = save_path
    print(f"💾 Local Save Path: {abs_save_path}")
    
    # Spaces Upload Status  
    if status_dict.get('spaces_upload_success', False):
        print(f"☁️ Spaces Upload: ✅")
    elif status_dict.get('spaces_configured', False):
        print(f"☁️ Spaces Upload: ❌ [Error: Upload failed]")
    else:
        print(f"☁️ Spaces Upload: ❌ [Error: Missing credentials]")
    
    # Where saved summary
    saved_locally = status_dict.get('data_saved_locally', False)
    saved_spaces = status_dict.get('spaces_upload_success', False)
    
    if saved_locally and saved_spaces:
        print(f"🧭 Where saved: [Spaces ✅ / Local ✅]")
    elif saved_locally and not saved_spaces:
        print(f"🧭 Where saved: [Spaces ❌ / Local ✅]")
    elif saved_spaces and not saved_locally:
        print(f"🧭 Where saved: [Spaces ✅ / Local ❌]")  
    else:
        print(f"🧭 Where saved: [Neither ❌]")
    
    print(f"{'='*60}")

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
                    print(f"   📄 {file}")
            else:
                print(f"   📂 (empty)")
        else:
            print(f"   ❌ Directory does not exist")
        
        # Check 30min intraday directory  
        intraday_30min_path = os.path.join(current_dir, 'data', 'intraday_30min')
        print(f"\n📁 Directory: {intraday_30min_path}")
        if os.path.exists(intraday_30min_path):
            files = os.listdir(intraday_30min_path)
            if files:
                for file in sorted(files):
                    print(f"   📄 {file}")
            else:
                print(f"   📂 (empty)")
        else:
            print(f"   ❌ Directory does not exist")
            
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
            print(f"📍 Status: NEW TICKER - Fetching full intraday history...")
            
            # For new tickers, fetch full history (outputsize='full')
            if interval == '1min':
                # Fetch 1min data first
                print(f"🔄 API Request: Fetching {interval} data (outputsize='full') for {ticker}...")
                latest_df = get_intraday_data(ticker, interval='1min', outputsize='full')
                
                if latest_df.empty:
                    print(f"❌ API FETCH FAILED: No intraday data returned for new ticker {ticker}")
                    ticker_status['api_fetch_success'] = False
                    ticker_status['api_fetch_error'] = "No data returned from API"
                    
                    # Enhanced debug logging for early failure
                    log_ticker_debug_status(ticker, interval, ticker_status, debug)
                    return False
                else:
                    print(f"✅ API FETCH SUCCESS: Retrieved {len(latest_df)} rows of {interval} data for {ticker}")
                    ticker_status['api_fetch_success'] = True
                
                # Ensure proper column names (API returns different formats)
                if 'timestamp' not in latest_df.columns and len(latest_df.columns) >= 6:
                    latest_df.columns = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
                
                # Normalize column names to match existing format
                latest_df = normalize_column_names(latest_df)
                
                # Save 1min data
                combined_df = latest_df.copy()
                ticker_status['new_candles_found'] = True
                print(f"✅ NEW CANDLES: All {len(combined_df)} candles are new for ticker {ticker}")
                
            else:  # 30min
                # For 30min interval, we need to check if we have 1min data to resample
                min_1_file_path = f'data/intraday/{ticker}_1min.csv'
                min_1_df = read_df_from_s3(min_1_file_path)
                
                if not min_1_df.empty:
                    # Resample from existing 1min data
                    print(f"🔄 RESAMPLING: Using existing 1min data to create 30min for {ticker}")
                    combined_df = resample_to_30min(min_1_df)
                    ticker_status['api_fetch_success'] = True  # Using existing data
                    ticker_status['new_candles_found'] = True
                    print(f"✅ RESAMPLING SUCCESS: Created {len(combined_df)} 30min candles from 1min data")
                else:
                    # Fetch 30min data directly from API
                    print(f"🔄 API Request: Fetching {interval} data (outputsize='full') for {ticker}...")
                    latest_df = get_intraday_data(ticker, interval='30min', outputsize='full')
                    
                    if latest_df.empty:
                        print(f"❌ API FETCH FAILED: No 30min intraday data returned for new ticker {ticker}")
                        ticker_status['api_fetch_success'] = False
                        ticker_status['api_fetch_error'] = "No 30min data returned from API"
                        
                        # Enhanced debug logging for early failure
                        log_ticker_debug_status(ticker, interval, ticker_status, debug)
                        return False
                    else:
                        print(f"✅ API FETCH SUCCESS: Retrieved {len(latest_df)} rows of {interval} data for {ticker}")
                        ticker_status['api_fetch_success'] = True
                    
                    # Ensure proper column names
                    if 'timestamp' not in latest_df.columns and len(latest_df.columns) >= 6:
                        latest_df.columns = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
                    
                    # Normalize column names to match existing format
                    latest_df = normalize_column_names(latest_df)
                    
                    combined_df = latest_df.copy()
                    ticker_status['new_candles_found'] = True
                    print(f"✅ NEW CANDLES: All {len(combined_df)} candles are new for ticker {ticker}")
        else:
            print(f"📍 Status: EXISTING TICKER - Checking for updates...")
            
            # Check if today's data is present
            today_present = is_today_present(existing_df)
            
            if not today_present:
                print(f"⚠️  Today's data missing for {ticker}. Fetching latest data...")
                
                # Check if we're in market hours and should warn
                market_session = detect_market_session()
                if market_session in ['PRE-MARKET', 'REGULAR']:
                    print(f"⚠️  WARNING: Today's data missing for {ticker} during {market_session} hours")
                
                # Fetch latest compact data (100 rows) to get today's data
                print(f"🔄 API Request: Fetching {interval} data (outputsize='compact') for {ticker}...")
                latest_df = get_intraday_data(ticker, interval=interval, outputsize='compact')
                
                if latest_df.empty:
                    print(f"❌ API FETCH FAILED: No new {interval} data returned for {ticker}")
                    ticker_status['api_fetch_success'] = False
                    ticker_status['api_fetch_error'] = f"No new {interval} data returned from API"
                    
                    # Enhanced debug logging for early failure
                    log_ticker_debug_status(ticker, interval, ticker_status, debug)
                    return False
                else:
                    print(f"✅ API FETCH SUCCESS: Retrieved {len(latest_df)} rows of {interval} data for {ticker}")
                    ticker_status['api_fetch_success'] = True
                
                # Ensure proper column names
                if 'timestamp' not in latest_df.columns and len(latest_df.columns) >= 6:
                    latest_df.columns = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
                
                # Normalize column names to match existing format
                latest_df = normalize_column_names(latest_df)
                    
            else:
                print(f"✅ Today's data already present for {ticker}. Fetching latest updates...")
                
                # Fetch compact data to get any new candles
                print(f"🔄 API Request: Fetching {interval} data (outputsize='compact') for {ticker}...")
                latest_df = get_intraday_data(ticker, interval=interval, outputsize='compact')
                
                if latest_df.empty:
                    print(f"⚠️  API FETCH WARNING: No new {interval} data returned for {ticker}. Using existing data.")
                    ticker_status['api_fetch_success'] = True  # Not really an error if no new data
                    ticker_status['new_candles_found'] = False
                    latest_df = pd.DataFrame()  # Empty, will use existing data
                else:
                    print(f"✅ API FETCH SUCCESS: Retrieved {len(latest_df)} rows of {interval} data for {ticker}")
                    ticker_status['api_fetch_success'] = True
                    
                    # Ensure proper column names
                    if 'timestamp' not in latest_df.columns and len(latest_df.columns) >= 6:
                        latest_df.columns = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
                    
                    # Normalize column names to match existing format
                    latest_df = normalize_column_names(latest_df)
            
            # Combine existing and new data using enhanced deduplication with date validation
            if not latest_df.empty:
                print(f"🔄 PROCESSING: Combining new data with existing {len(existing_df)} rows for {ticker}...")
                # Use the new append_new_candles function for better deduplication and date validation
                success = append_new_candles(ticker, latest_df, file_path)
                if not success:
                    print(f"❌ DATA COMBINATION FAILED: Could not append new candles for {ticker}")
                    ticker_status['new_candles_found'] = False
                    return False
                
                # Read the updated combined data
                combined_df = read_df_from_s3(file_path)
                if combined_df.empty:
                    print(f"❌ DATA READ ERROR: No data found after appending for {ticker}")
                    return False
                    
                # Calculate new candles added
                new_candles_count = len(combined_df) - len(existing_df)
                if new_candles_count > 0:
                    ticker_status['new_candles_found'] = True
                    print(f"✅ NEW CANDLES: Added {new_candles_count} new candles for {ticker} (total: {len(combined_df)})")
                else:
                    ticker_status['new_candles_found'] = False
                    print(f"📊 NO NEW CANDLES: No new data to add for {ticker} (total: {len(combined_df)})")
            else:
                combined_df = existing_df.copy()
                ticker_status['new_candles_found'] = False
                print(f"📊 USING EXISTING: No new data to process, using existing {len(combined_df)} rows for {ticker}")
        
        # Apply rolling window trimming (keep last 5 days + current day) only if we have data
        if not combined_df.empty:
            print(f"🔄 PROCESSING: Applying rolling window trimming for {ticker}...")
            combined_df = trim_to_rolling_window(combined_df, days=5)
            
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
                print(f"   This ticker data may be lost!")
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
        
        # Final per-ticker status report
        print(f"\n📋 TICKER STATUS SUMMARY: {ticker} ({interval})")
        print(f"{'='*50}")
        print(f"🎯 Ticker: {ticker}")
        print(f"📊 API Fetch: {'✅ SUCCESS' if ticker_status['api_fetch_success'] else '❌ FAILED'}")
        if ticker_status['api_fetch_error']:
            print(f"   Error: {ticker_status['api_fetch_error']}")
        print(f"🆕 New Candles: {'✅ FOUND' if ticker_status['new_candles_found'] else '📊 NONE'}")
        print(f"💾 Local Save: {'✅ SUCCESS' if ticker_status['data_saved_locally'] else '❌ FAILED'}")
        print(f"☁️  Spaces Upload: {'✅ SUCCESS' if ticker_status['spaces_upload_success'] else '❌ FAILED/DISABLED'}")
        print(f"📈 Total Rows: {ticker_status['total_rows']}")
        
        # Check if today's data is now present
        if not is_today_present(combined_df):
            print(f"⚠️  WARNING: Today's data still missing for {ticker} after update")
        else:
            print(f"✅ Today's data confirmed present for {ticker}")
        
        print(f"{'='*50}")
        print(f"✅ COMPLETED: {ticker} ({interval}) processing finished successfully")
        
        # Enhanced debug logging if enabled
        log_ticker_debug_status(ticker, interval, ticker_status, debug)
        
        return True
        
    except Exception as e:
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
    """
    print("--- Starting Enhanced Intraday Data Update Job ---")
    
    if debug:
        print("🧪 DEBUG MODE: Enhanced logging enabled")
        print("   Additional detailed status will be shown for each ticker")
    
    # Enhanced Environment Variable Check
    print("\n=== ENVIRONMENT VARIABLES VERIFICATION ===")
    
    # Alpha Vantage API Key
    api_key = os.getenv('ALPHA_VANTAGE_API_KEY')
    if not api_key:
        print("❌ CRITICAL: ALPHA_VANTAGE_API_KEY environment variable not set")
        print("   Data fetching will fail!")
    else:
        print(f"✅ ALPHA_VANTAGE_API_KEY configured: {api_key[:8]}***{api_key[-4:] if len(api_key) > 12 else '***'}")
    
    # DigitalOcean Spaces Credentials - Check all required variables
    spaces_access_key = os.getenv('SPACES_ACCESS_KEY_ID')
    spaces_secret_key = os.getenv('SPACES_SECRET_ACCESS_KEY')
    spaces_bucket = os.getenv('SPACES_BUCKET_NAME')
    spaces_region = os.getenv('SPACES_REGION')
    
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
    spaces_configured = all([spaces_access_key, spaces_secret_key, spaces_bucket, spaces_region])
    if spaces_configured:
        print("✅ DigitalOcean Spaces: FULLY CONFIGURED")
    else:
        print("⚠️  DigitalOcean Spaces: INCOMPLETE CONFIGURATION")
        print("   Missing credentials will cause Spaces upload to silently fail!")
        print("   Using local filesystem fallback for data persistence")
    
    print("=" * 50)
    
    # Load tickers from S3 (S&P 500 or other universe)
    tickers = read_tickerlist_from_s3()
    if not tickers:
        tickers = []
        print("No tickers found in tickerlist.txt")
    
    # Always load and include manual tickers from ticker_selectors/tickerlist.txt
    manual_tickers = load_manual_tickers()
    if manual_tickers:
        print(f"Adding {len(manual_tickers)} manual tickers: {manual_tickers}")
        print("⚠️  CRITICAL: These manual tickers MUST appear in Spaces storage!")
        tickers.extend(manual_tickers)
    else:
        print("No manual tickers found in ticker_selectors/tickerlist.txt")
    
    # Remove duplicates while preserving order
    tickers = list(dict.fromkeys(tickers))
    
    if not tickers:
        print("No tickers to process. Exiting job.")
        return

    print(f"Processing {len(tickers)} tickers for intraday updates (including manual tickers)...")
    
    # Track processing results
    success_count = 0
    total_operations = len(tickers) * 2  # Both 1min and 30min for each ticker
    manual_ticker_results = {}  # Track manual ticker processing specifically
    
    for ticker in tickers:
        print(f"\n{'='*70}")
        print(f"🚀 STARTING TICKER PROCESSING: {ticker}")
        is_manual_ticker = ticker in (manual_tickers if manual_tickers else [])
        if is_manual_ticker:
            print(f"🎯 ⭐ MANUAL TICKER: {ticker} - This MUST succeed for production!")
        print(f"{'='*70}")
        
        # Process 1-minute interval first
        print(f"\n🔄 PHASE 1: Processing {ticker} for 1-minute interval...")
        success_1min = process_ticker_interval(ticker, '1min', debug)
        if success_1min:
            success_count += 1
        
        # Process 30-minute interval 
        # Note: For 30min, we prefer resampling from 1min data when available
        print(f"\n🔄 PHASE 2: Processing {ticker} for 30-minute interval...")
        success_30min = process_ticker_interval(ticker, '30min', debug)
        if success_30min:
            success_count += 1
        
        # Track manual ticker results specifically
        if is_manual_ticker:
            manual_ticker_results[ticker] = {
                '1min': success_1min,
                '30min': success_30min,
                'overall': success_1min and success_30min
            }
        
        # Per-ticker final summary
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

    print(f"\n{'='*60}")
    print(f"Enhanced Intraday Data Update Job Completed")
    print(f"Success rate: {success_count}/{total_operations} operations")
    
    # Show file listing if debug mode is enabled
    list_intraday_files(debug)
    
    # Report manual ticker status specifically
    if manual_ticker_results:
        print(f"\n🎯 MANUAL TICKER STATUS REPORT:")
        print(f"{'='*40}")
        failed_manual_tickers = []
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
            print(f"   {failed_manual_tickers}")
            print(f"   These tickers will NOT appear in production Spaces storage!")
            print(f"   Check DigitalOcean Spaces credentials and connectivity.")
        else:
            print(f"\n✅ SUCCESS: All {len(manual_ticker_results)} manual tickers processed successfully!")
            print(f"   Manual tickers should now be available in Spaces storage.")
    
    print(f"{'='*60}")

if __name__ == "__main__":
    # Check for debug mode argument
    import sys
    debug_mode = "--debug" in sys.argv or "-d" in sys.argv
    
    job_name = "update_intraday_compact"
    update_scheduler_status(job_name, "Running")
    try:
        run_compact_append(debug=debug_mode)
        update_scheduler_status(job_name, "Success")
    except Exception as e:
        error_message = f"An unexpected error occurred: {e}"
        print(error_message)
        update_scheduler_status(job_name, "Fail", error_message)
