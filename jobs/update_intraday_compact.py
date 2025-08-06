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

def process_ticker_interval(ticker, interval):
    """
    Process a single ticker for a specific interval (1min or 30min).
    
    Args:
        ticker: Stock symbol
        interval: '1min' or '30min'
        
    Returns:
        bool: True if successful, False otherwise
    """
    print(f"\n--- Processing {ticker} for {interval} interval ---")
    
    try:
        # Determine file paths
        if interval == '1min':
            file_path = f'data/intraday/{ticker}_{interval}.csv'
        else:  # 30min
            file_path = f'data/intraday_30min/{ticker}_{interval}.csv'
        
        # Check if file exists (new ticker vs existing ticker)
        existing_df = read_df_from_s3(file_path)
        is_new_ticker = existing_df.empty
        
        if is_new_ticker:
            print(f"New ticker detected: {ticker}. Fetching full intraday history...")
            
            # For new tickers, fetch full history (outputsize='full')
            if interval == '1min':
                # Fetch 1min data first
                latest_df = get_intraday_data(ticker, interval='1min', outputsize='full')
                if latest_df.empty:
                    print(f"No intraday data returned for new ticker {ticker}. Skipping.")
                    return False
                
                # Ensure proper column names (API returns different formats)
                if 'timestamp' not in latest_df.columns and len(latest_df.columns) >= 6:
                    latest_df.columns = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
                
                # Normalize column names to match existing format
                latest_df = normalize_column_names(latest_df)
                
                # Save 1min data
                combined_df = latest_df.copy()
                
            else:  # 30min
                # For 30min interval, we need to check if we have 1min data to resample
                min_1_file_path = f'data/intraday/{ticker}_1min.csv'
                min_1_df = read_df_from_s3(min_1_file_path)
                
                if not min_1_df.empty:
                    # Resample from existing 1min data
                    print(f"Resampling existing 1min data to 30min for {ticker}")
                    combined_df = resample_to_30min(min_1_df)
                else:
                    # Fetch 30min data directly from API
                    latest_df = get_intraday_data(ticker, interval='30min', outputsize='full')
                    if latest_df.empty:
                        print(f"No 30min intraday data returned for new ticker {ticker}. Skipping.")
                        return False
                    
                    # Ensure proper column names
                    if 'timestamp' not in latest_df.columns and len(latest_df.columns) >= 6:
                        latest_df.columns = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
                    
                    # Normalize column names to match existing format
                    latest_df = normalize_column_names(latest_df)
                    
                    combined_df = latest_df.copy()
        else:
            print(f"Existing ticker: {ticker}. Checking for today's data...")
            
            # Check if today's data is present
            today_present = is_today_present(existing_df)
            
            if not today_present:
                print(f"Today's data missing for {ticker}. Fetching latest data...")
                
                # Check if we're in market hours and should warn
                market_session = detect_market_session()
                if market_session in ['PRE-MARKET', 'REGULAR']:
                    print(f"⚠️  WARNING: Today's data missing for {ticker} during {market_session} hours")
                
                # Fetch latest compact data (100 rows) to get today's data
                latest_df = get_intraday_data(ticker, interval=interval, outputsize='compact')
                if latest_df.empty:
                    print(f"No new {interval} data returned for {ticker}. Skipping.")
                    return False
                
                # Ensure proper column names
                if 'timestamp' not in latest_df.columns and len(latest_df.columns) >= 6:
                    latest_df.columns = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
                
                # Normalize column names to match existing format
                latest_df = normalize_column_names(latest_df)
                    
            else:
                print(f"Today's data already present for {ticker}. Fetching latest updates...")
                
                # Fetch compact data to get any new candles
                latest_df = get_intraday_data(ticker, interval=interval, outputsize='compact')
                if latest_df.empty:
                    print(f"No new {interval} data returned for {ticker}. Using existing data.")
                    latest_df = pd.DataFrame()  # Empty, will use existing data
                else:
                    # Ensure proper column names
                    if 'timestamp' not in latest_df.columns and len(latest_df.columns) >= 6:
                        latest_df.columns = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
                    
                    # Normalize column names to match existing format
                    latest_df = normalize_column_names(latest_df)
            
            # Combine existing and new data using enhanced deduplication with date validation
            if not latest_df.empty:
                # Use the new append_new_candles function for better deduplication and date validation
                success = append_new_candles(ticker, latest_df, file_path)
                if not success:
                    print(f"Failed to append new candles for {ticker}")
                    return False
                
                # Read the updated combined data
                combined_df = read_df_from_s3(file_path)
                if combined_df.empty:
                    print(f"Error: No data found after appending for {ticker}")
                    return False
            else:
                combined_df = existing_df.copy()
        
        # Apply rolling window trimming (keep last 5 days + current day) only if we have data
        if not combined_df.empty:
            combined_df = trim_to_rolling_window(combined_df, days=5)
            
            # Sort by timestamp (chronological order - oldest to newest, which matches existing format)
            timestamp_col = 'Date' if 'Date' in combined_df.columns else 'timestamp'
            combined_df.sort_values(by=timestamp_col, ascending=True, inplace=True)
            
            # Save the updated file back to S3/local (only if we have data)
            upload_success = save_df_to_s3(combined_df, file_path)
            if not upload_success:
                print(f"❌ CRITICAL ERROR: Failed to save {ticker} data to both Spaces and local: {file_path}")
                print(f"   This ticker data may be lost!")
                return False
            else:
                print(f"✅ Successfully saved {ticker} data: {file_path}")
        
        print(f"✅ Finished processing {ticker} for {interval}. Total rows: {len(combined_df)}")
        
        # Check if today's data is now present
        if not is_today_present(combined_df):
            print(f"⚠️  WARNING: Today's data still missing for {ticker} after update")
        
        return True
        
    except Exception as e:
        print(f"❌ ERROR processing {ticker} for {interval}: {e}")
        return False
def run_compact_append():
    """
    Runs the enhanced intraday data update process.
    - Handles both new and existing tickers
    - Ensures today's data is always included
    - Maintains rolling window of last 5 days + current day
    - Provides robust error handling and warnings
    """
    print("--- Starting Enhanced Intraday Data Update Job ---")
    
    # System health check
    print("\n=== System Health Check ===")
    api_key = os.getenv('ALPHA_VANTAGE_API_KEY')
    if not api_key:
        print("❌ CRITICAL: ALPHA_VANTAGE_API_KEY environment variable not set")
        print("   Data fetching will fail!")
    else:
        print("✅ Alpha Vantage API key configured")
    
    spaces_access_key = os.getenv('SPACES_ACCESS_KEY_ID')
    spaces_secret_key = os.getenv('SPACES_SECRET_ACCESS_KEY')
    if not spaces_access_key or not spaces_secret_key:
        print("⚠️  WARNING: DigitalOcean Spaces credentials not configured")
        print("   Using local filesystem fallback for data persistence")
    else:
        print("✅ DigitalOcean Spaces credentials configured")
    print("===============================\n")
    
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
        print(f"\n{'='*50}")
        print(f"Processing ticker: {ticker}")
        is_manual_ticker = ticker in (manual_tickers if manual_tickers else [])
        if is_manual_ticker:
            print(f"🎯 MANUAL TICKER: {ticker} - This MUST succeed for production!")
        print(f"{'='*50}")
        
        # Process 1-minute interval first
        success_1min = process_ticker_interval(ticker, '1min')
        if success_1min:
            success_count += 1
        
        # Process 30-minute interval 
        # Note: For 30min, we prefer resampling from 1min data when available
        success_30min = process_ticker_interval(ticker, '30min')
        if success_30min:
            success_count += 1
        
        # Track manual ticker results specifically
        if is_manual_ticker:
            manual_ticker_results[ticker] = {
                '1min': success_1min,
                '30min': success_30min,
                'overall': success_1min and success_30min
            }
        
        # Respect API rate limits
        time.sleep(1)

    print(f"\n{'='*60}")
    print(f"Enhanced Intraday Data Update Job Completed")
    print(f"Success rate: {success_count}/{total_operations} operations")
    
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
    job_name = "update_intraday_compact"
    update_scheduler_status(job_name, "Running")
    try:
        run_compact_append()
        update_scheduler_status(job_name, "Success")
    except Exception as e:
        error_message = f"An unexpected error occurred: {e}"
        print(error_message)
        update_scheduler_status(job_name, "Fail", error_message)
