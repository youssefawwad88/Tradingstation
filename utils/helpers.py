import os
import pandas as pd
import requests
import logging
from datetime import datetime
import time
import json
import pytz
from utils.config import (
    ALPHA_VANTAGE_API_KEY, 
    INTRADAY_DATA_DIR,
    DEBUG_MODE,
    INTRADAY_TRIM_DAYS,
    INTRADAY_EXCLUDE_TODAY,
    INTRADAY_INCLUDE_PREMARKET,
    INTRADAY_INCLUDE_AFTERHOURS,
    TIMEZONE
)
from utils.spaces_manager import upload_dataframe

logger = logging.getLogger(__name__)

def fetch_intraday_data(ticker, interval='1min', outputsize='compact'):
    """
    Fetch intraday data from Alpha Vantage API.
    
    Args:
        ticker (str): Stock ticker symbol
        interval (str): Time interval between data points (1min, 5min, 15min, 30min, 60min)
        outputsize (str): 'compact' returns latest 100 data points, 'full' returns up to 20+ years of data
        
    Returns:
        tuple: (DataFrame or None, bool indicating success)
    """
    if not ALPHA_VANTAGE_API_KEY:
        logger.error("Alpha Vantage API key not found in environment variables")
        return None, False
    
    endpoint = 'https://www.alphavantage.co/query'
    params = {
        'function': 'TIME_SERIES_INTRADAY',
        'symbol': ticker,
        'interval': interval,
        'outputsize': outputsize,
        'apikey': ALPHA_VANTAGE_API_KEY
    }
    
    try:
        response = requests.get(endpoint, params=params)
        data = response.json()
        
        if 'Error Message' in data:
            logger.error(f"Alpha Vantage API error for {ticker}: {data['Error Message']}")
            return None, False
            
        time_series_key = f'Time Series ({interval})'
        if time_series_key not in data:
            logger.error(f"Unexpected response format for {ticker}: {json.dumps(data)[:200]}...")
            return None, False
            
        time_series = data[time_series_key]
        
        # Convert to DataFrame
        df = pd.DataFrame.from_dict(time_series, orient='index')
        
        # Rename columns
        df.columns = [col.split('. ')[1] for col in df.columns]
        
        # Convert values to float
        for col in df.columns:
            df[col] = df[col].astype(float)
            
        # Add date and ticker columns
        df.index = pd.to_datetime(df.index)
        df = df.reset_index()
        df.rename(columns={'index': 'datetime'}, inplace=True)
        df['ticker'] = ticker
        
        logger.info(f"Successfully fetched intraday data for {ticker} ({interval}): {len(df)} records")
        return df, True
    except Exception as e:
        logger.error(f"Error fetching intraday data for {ticker}: {e}")
        return None, False

def save_df_to_local(df, ticker, interval, directory=INTRADAY_DATA_DIR):
    """
    Save DataFrame to local filesystem.
    
    Args:
        df (pandas.DataFrame): DataFrame to save
        ticker (str): Stock ticker symbol
        interval (str): Time interval of the data
        directory (str): Directory to save the file
        
    Returns:
        tuple: (file path or None, bool indicating success)
    """
    os.makedirs(directory, exist_ok=True)
    file_path = os.path.join(directory, f"{ticker}_{interval}.csv")
    
    try:
        df.to_csv(file_path, index=False)
        logger.info(f"Saved {ticker} data to {file_path}")
        return file_path, True
    except Exception as e:
        logger.error(f"Error saving {ticker} data to {file_path}: {e}")
        return None, False

def save_df_to_s3(df, object_name_or_ticker, interval=None, s3_prefix='intraday'):
    """
    Save DataFrame to DigitalOcean Spaces (with flexible parameters) with local fallback.
    
    Args:
        df (pandas.DataFrame): DataFrame to save
        object_name_or_ticker (str): Object name/path in S3 OR ticker symbol  
        interval (str, optional): Time interval of the data (if ticker provided)
        s3_prefix (str): Prefix/folder in the S3 bucket (if ticker provided)
        
    Returns:
        bool: True if successful (either Spaces or local), False otherwise
    """
    # Determine if we got an object name or ticker
    if interval is not None:
        # Old-style call with ticker and interval
        object_name = f"{s3_prefix}/{object_name_or_ticker}_{interval}.csv"
        ticker = object_name_or_ticker
        logger.info(f"Uploading {object_name_or_ticker} data to Spaces at {object_name}")
    else:
        # New-style call with direct object name
        object_name = object_name_or_ticker
        # Extract ticker from object name for local fallback
        if '/' in object_name:
            parts = object_name.split('/')
            filename = parts[-1]  # Get filename from path
            if '_' in filename:
                ticker = filename.split('_')[0]
                interval_part = filename.replace(f'{ticker}_', '').replace('.csv', '')
            else:
                ticker = filename.replace('.csv', '')
                interval_part = '1min'  # default
        else:
            ticker = object_name.replace('.csv', '')
            interval_part = '1min'  # default
        logger.info(f"Uploading DataFrame to Spaces at {object_name}")
    
    # Try Spaces upload first
    success = upload_dataframe(df, object_name)
    if success:
        logger.info(f"Successfully uploaded to Spaces at {object_name}")
        return True
    else:
        logger.warning(f"Failed to upload to Spaces at {object_name}. Trying local filesystem fallback...")
        
        # Fallback to local filesystem
        try:
            # Extract interval from object name if not provided
            if interval is None:
                interval = interval_part if 'interval_part' in locals() else '1min'
            
            # Determine directory based on interval
            if '30min' in str(interval):
                directory = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "intraday_30min")
            else:
                directory = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "intraday")
            
            # Save to local filesystem
            os.makedirs(directory, exist_ok=True)
            local_path = os.path.join(directory, f"{ticker}_{interval}.csv")
            df.to_csv(local_path, index=False)
            logger.info(f"Successfully saved to local filesystem: {local_path}")
            return True
            
        except Exception as e:
            logger.error(f"Local filesystem fallback also failed: {e}")
            return False

def save_to_local_filesystem(df, ticker, interval):
    """
    Fallback function to save data locally if S3 upload fails.
    
    Args:
        df (pandas.DataFrame): DataFrame to save
        ticker (str): Stock ticker symbol
        interval (str): Time interval of the data
        
    Returns:
        tuple: (file path or None, bool indicating success)
    """
    file_path, success = save_df_to_local(df, ticker, interval)
    if success:
        logger.info(f"Fallback save successful for {ticker} to {file_path}")
    else:
        logger.error(f"Fallback save failed for {ticker}")
    return file_path, success

def update_scheduler_status(job_name, status, error_details=None):
    """
    Update the scheduler status for a job.
    
    Args:
        job_name (str): Name of the job
        status (str): Status of the job ('Running', 'Success', 'Fail')
        error_details (str, optional): Error details if status is 'Fail'
    """
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    if status == "Running":
        logger.info(f"[{timestamp}] Job '{job_name}' started")
    elif status == "Success":
        logger.info(f"[{timestamp}] Job '{job_name}' completed successfully")
    elif status == "Fail":
        error_msg = f"[{timestamp}] Job '{job_name}' failed"
        if error_details:
            error_msg += f" - {error_details}"
        logger.error(error_msg)
    else:
        logger.warning(f"[{timestamp}] Job '{job_name}' status unknown: {status}")

def detect_market_session():
    """
    Detect the current market session based on Eastern Time.
    
    Returns:
        str: Market session ('PRE-MARKET', 'REGULAR', 'AFTER-HOURS', 'CLOSED')
    """
    ny_tz = pytz.timezone('America/New_York')
    current_time = datetime.now(ny_tz)
    current_weekday = current_time.weekday()  # 0=Monday, 6=Sunday
    
    # Check if it's weekend
    if current_weekday >= 5:  # Saturday=5, Sunday=6
        return 'CLOSED'
    
    # Get current time in minutes since midnight
    current_minutes = current_time.hour * 60 + current_time.minute
    
    # Market session times (in minutes since midnight ET)
    premarket_start = 4 * 60  # 4:00 AM
    regular_start = 9 * 60 + 30  # 9:30 AM
    regular_end = 16 * 60  # 4:00 PM
    afterhours_end = 20 * 60  # 8:00 PM
    
    if current_minutes < premarket_start:
        return 'CLOSED'
    elif current_minutes < regular_start:
        return 'PRE-MARKET'
    elif current_minutes < regular_end:
        return 'REGULAR'
    elif current_minutes < afterhours_end:
        return 'AFTER-HOURS'
    else:
        return 'CLOSED'

def read_tickerlist_from_s3(filename):
    """
    Read ticker list from S3/Spaces or local fallback.
    
    Args:
        filename (str): Name of the ticker file
        
    Returns:
        list: List of ticker symbols
    """
    logger.info(f"Reading ticker list from {filename}")
    
    # Try to read from local file first as fallback
    local_file = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), filename)
    if os.path.exists(local_file):
        try:
            with open(local_file, 'r') as f:
                tickers = [line.strip() for line in f.readlines() if line.strip()]
            logger.info(f"Successfully read {len(tickers)} tickers from local file")
            return tickers
        except Exception as e:
            logger.error(f"Error reading local ticker file: {e}")
    
    # Fallback to default tickers from config
    from utils.config import DEFAULT_TICKERS
    logger.warning(f"Using default tickers: {DEFAULT_TICKERS}")
    return DEFAULT_TICKERS

def read_master_tickerlist():
    """
    Read master ticker list from master_tickerlist.csv (local or Spaces).
    This is the unified function used by all fetchers.
    
    Returns:
        list: List of ticker symbols from master list
    """
    logger.info("Reading master ticker list from master_tickerlist.csv")
    
    try:
        # Try local file first
        local_file = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "master_tickerlist.csv")
        if os.path.exists(local_file):
            df = pd.read_csv(local_file)
            if 'ticker' in df.columns:
                tickers = df['ticker'].tolist()
                logger.info(f"✅ Successfully read {len(tickers)} tickers from master_tickerlist.csv")
                return tickers
        
        # Try to read from Spaces
        df = read_df_from_s3("master_tickerlist.csv")
        if not df.empty and 'ticker' in df.columns:
            tickers = df['ticker'].tolist()
            logger.info(f"✅ Successfully read {len(tickers)} tickers from master_tickerlist.csv (Spaces)")
            return tickers
        
        # Fallback: generate master list if it doesn't exist
        logger.warning("⚠️ master_tickerlist.csv not found, falling back to manual tickers")
        manual_tickers = load_manual_tickers()
        if manual_tickers:
            logger.info(f"Using {len(manual_tickers)} manual tickers as fallback")
            return manual_tickers
        
        # Final fallback to default tickers
        from utils.config import DEFAULT_TICKERS
        logger.warning(f"Using default tickers as last resort: {DEFAULT_TICKERS}")
        return DEFAULT_TICKERS
        
    except Exception as e:
        logger.error(f"Error reading master ticker list: {e}")
        # Fallback to manual tickers
        manual_tickers = load_manual_tickers()
        if manual_tickers:
            return manual_tickers
        
        from utils.config import DEFAULT_TICKERS
        return DEFAULT_TICKERS

def read_df_from_s3(object_name):
    """
    Read DataFrame from S3/Spaces or local fallback.
    
    Args:
        object_name (str): Object name/path in S3
        
    Returns:
        pandas.DataFrame: DataFrame if successful, empty DataFrame otherwise
    """
    logger.info(f"Attempting to read DataFrame from {object_name}")
    
    # Try to read from local file first
    local_file = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), object_name)
    if os.path.exists(local_file):
        try:
            df = pd.read_csv(local_file)
            logger.info(f"Successfully read {len(df)} rows from local file")
            return df
        except Exception as e:
            logger.error(f"Error reading local file {local_file}: {e}")
    
    # Return empty DataFrame if file doesn't exist or can't be read
    logger.warning(f"File not found or unreadable: {object_name} - returning empty DataFrame")
    return pd.DataFrame()

def load_manual_tickers():
    """
    Load manual ticker list from tickerlist.txt (as per TICKER_MANAGEMENT.md documentation).
    
    Returns:
        list: List of manual tickers
    """
    # Try to read from tickerlist.txt first (documented manual ticker source)
    manual_file = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "tickerlist.txt")
    if os.path.exists(manual_file):
        try:
            with open(manual_file, 'r') as f:
                tickers = [line.strip() for line in f.readlines() if line.strip()]
            logger.info(f"Loaded {len(tickers)} manual tickers from tickerlist.txt")
            return tickers
        except Exception as e:
            logger.error(f"Error reading manual tickers from tickerlist.txt: {e}")
    
    # Fallback: try manual_tickers.txt for backwards compatibility
    manual_file_alt = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "manual_tickers.txt")
    if os.path.exists(manual_file_alt):
        try:
            with open(manual_file_alt, 'r') as f:
                tickers = [line.strip() for line in f.readlines() if line.strip()]
            logger.info(f"Loaded {len(tickers)} manual tickers from manual_tickers.txt (fallback)")
            return tickers
        except Exception as e:
            logger.error(f"Error reading manual tickers from manual_tickers.txt: {e}")
    
    # Final fallback to default tickers
    from utils.config import DEFAULT_TICKERS
    logger.warning("No manual ticker file found, using DEFAULT_TICKERS")
    return DEFAULT_TICKERS

def is_today_present(df):
    """
    Check if today's data is present in the DataFrame.
    
    Args:
        df (pandas.DataFrame): DataFrame with datetime column
        
    Returns:
        bool: True if today's data is present
    """
    if df is None or df.empty:
        return False
    
    try:
        ny_tz = pytz.timezone('America/New_York')
        today = datetime.now(ny_tz).date()
        
        # Assume datetime column exists
        if 'datetime' in df.columns:
            df_dates = pd.to_datetime(df['datetime']).dt.date
            return today in df_dates.values
        
        return False
    except Exception as e:
        logger.error(f"Error checking if today is present: {e}")
        return False

def get_last_market_day():
    """
    Get the last market day.
    
    Returns:
        datetime.date: Last market day
    """
    ny_tz = pytz.timezone('America/New_York')
    current = datetime.now(ny_tz)
    
    # Simple implementation - go back until we find a weekday
    while current.weekday() >= 5:  # Weekend
        current = current - pd.Timedelta(days=1)
    
    return current.date()

def is_today():
    """
    Check if current date is today.
    
    Returns:
        bool: Always True (helper function)
    """
    return True

def trim_to_rolling_window(df, window_days=30):
    """
    Trim DataFrame to rolling window with enhanced data retention logic.
    KEEPS TODAY'S DATA by default.
    
    Args:
        df (pandas.DataFrame): DataFrame to trim
        window_days (int): Number of days to keep
        
    Returns:
        pandas.DataFrame: Trimmed DataFrame
    """
    if df is None or df.empty:
        return df
    
    try:
        # Use the new enhanced retention function
        return apply_data_retention(df, window_days)
    except Exception as e:
        logger.error(f"Error trimming to rolling window: {e}")
        return df

def apply_data_retention(df, trim_days=None):
    """
    Apply enhanced data retention rules based on environment configuration.
    KEEPS TODAY'S DATA by default as per Phase 4 requirements.
    
    Args:
        df (pandas.DataFrame): DataFrame with date/datetime column
        trim_days (int, optional): Override trim days from config
        
    Returns:
        pandas.DataFrame: Filtered DataFrame
    """
    if df is None or df.empty:
        return df
    
    try:
        from utils.config import (
            INTRADAY_TRIM_DAYS, INTRADAY_EXCLUDE_TODAY, 
            INTRADAY_INCLUDE_PREMARKET, INTRADAY_INCLUDE_AFTERHOURS,
            TIMEZONE, DEBUG_MODE
        )
        
        # Log initial state
        initial_count = len(df)
        logger.info(f"🔄 RETENTION: Starting with {initial_count} rows")
        
        # Convert to Eastern Time and handle date column
        combined_df = df.copy()
        
        # Find the date/datetime column
        date_col = None
        if 'Date' in combined_df.columns:
            date_col = 'Date'
        elif 'datetime' in combined_df.columns:
            date_col = 'datetime'
        elif 'timestamp' in combined_df.columns:
            date_col = 'timestamp'
        
        if not date_col:
            logger.warning("No date column found for retention filtering")
            return df
        
        # Convert to datetime and timezone-aware
        combined_df[date_col] = pd.to_datetime(combined_df[date_col])
        combined_df = combined_df.set_index(date_col)
        
        # Handle timezone conversion
        if combined_df.index.tz is None:
            # Assume UTC if no timezone info
            combined_df = combined_df.tz_localize('UTC')
        
        # Convert to Eastern Time
        combined_df = combined_df.tz_convert(TIMEZONE)
        combined_df = combined_df.reset_index()
        
        # Get current date in ET
        ny_tz = pytz.timezone(TIMEZONE)
        now_et = datetime.now(ny_tz)
        today_et = now_et.replace(hour=0, minute=0, second=0, microsecond=0)
        
        # Apply retention rules
        trim_days_to_use = trim_days if trim_days is not None else INTRADAY_TRIM_DAYS
        exclude_today = INTRADAY_EXCLUDE_TODAY
        
        # Calculate start date for retention (N days back)
        start_date = today_et - pd.Timedelta(days=trim_days_to_use)
        
        logger.info(f"📅 RETENTION CONFIG:")
        logger.info(f"   Current time (ET): {now_et}")
        logger.info(f"   Today (ET): {today_et}")
        logger.info(f"   Start date: {start_date}")
        logger.info(f"   Trim days: {trim_days_to_use}")
        logger.info(f"   Exclude today: {exclude_today}")
        
        # Log data range before filtering
        min_date = combined_df[date_col].min()
        max_date = combined_df[date_col].max()
        logger.info(f"📊 DATA RANGE BEFORE FILTERING:")
        logger.info(f"   Minimum timestamp: {min_date}")
        logger.info(f"   Maximum timestamp: {max_date}")
        
        # Filter data - KEEP TODAY'S DATA by default!
        if exclude_today:
            # Only if explicitly set to exclude today
            combined_df = combined_df[(combined_df[date_col] >= start_date) & 
                                    (combined_df[date_col] < today_et)]
            logger.warning(f"⚠️ EXCLUDING TODAY'S DATA as requested by config")
        else:
            # DEFAULT: Keep everything from start_date forward INCLUDING TODAY
            combined_df = combined_df[combined_df[date_col] >= start_date]
            logger.info(f"✅ KEEPING TODAY'S DATA (default behavior)")
        
        after_date_filter_count = len(combined_df)
        logger.info(f"📊 After date filtering: {after_date_filter_count} rows")
        
        # Apply market session filtering if needed
        include_premarket = INTRADAY_INCLUDE_PREMARKET
        include_afterhours = INTRADAY_INCLUDE_AFTERHOURS
        
        # Only apply session filtering if either is False
        if not (include_premarket and include_afterhours):
            logger.info(f"🕐 APPLYING SESSION FILTERING:")
            logger.info(f"   Include pre-market: {include_premarket}")
            logger.info(f"   Include after-hours: {include_afterhours}")
            
            # Extract time component
            combined_df['time'] = combined_df[date_col].dt.time
            
            # Define market session times (Eastern Time)
            pre_market_start = pd.Timestamp('04:00:00').time()
            market_open = pd.Timestamp('09:30:00').time()
            market_close = pd.Timestamp('16:00:00').time()
            after_hours_end = pd.Timestamp('20:00:00').time()
            
            # Create mask for each session
            if include_premarket and not include_afterhours:
                # Keep pre-market and regular hours
                combined_df = combined_df[
                    (combined_df['time'] >= pre_market_start) & 
                    (combined_df['time'] <= market_close)
                ]
                logger.info(f"   Keeping: Pre-market + Regular hours")
            elif not include_premarket and include_afterhours:
                # Keep regular hours and after hours
                combined_df = combined_df[
                    (combined_df['time'] >= market_open) & 
                    (combined_df['time'] <= after_hours_end)
                ]
                logger.info(f"   Keeping: Regular hours + After-hours")
            elif not include_premarket and not include_afterhours:
                # Keep only regular hours
                combined_df = combined_df[
                    (combined_df['time'] >= market_open) & 
                    (combined_df['time'] <= market_close)
                ]
                logger.info(f"   Keeping: Regular hours only")
            
            # Remove temporary time column
            combined_df = combined_df.drop('time', axis=1)
        else:
            logger.info(f"✅ KEEPING ALL MARKET SESSIONS (pre-market, regular, after-hours)")
        
        final_count = len(combined_df)
        logger.info(f"📊 RETENTION SUMMARY:")
        logger.info(f"   Initial rows: {initial_count}")
        logger.info(f"   After date filter: {after_date_filter_count}")
        logger.info(f"   Final rows: {final_count}")
        logger.info(f"   Rows removed: {initial_count - final_count}")
        
        # Log data range after filtering
        if not combined_df.empty:
            min_date_after = combined_df[date_col].min()
            max_date_after = combined_df[date_col].max()
            logger.info(f"📊 DATA RANGE AFTER FILTERING:")
            logger.info(f"   Minimum timestamp: {min_date_after}")
            logger.info(f"   Maximum timestamp: {max_date_after}")
            
            # ALWAYS confirm today's data is present after filtering
            today_present = is_today_present_enhanced(combined_df, date_col)
            if today_present:
                logger.info(f"✅ TODAY'S DATA CONFIRMED PRESENT after filtering")
            else:
                logger.warning(f"⚠️ TODAY'S DATA MISSING after filtering - this may be an issue!")
        else:
            logger.warning(f"⚠️ NO DATA REMAINING after filtering")
        
        return combined_df
        
    except Exception as e:
        logger.error(f"Error applying data retention: {e}")
        return df

def is_today_present_enhanced(df, date_col='Date'):
    """
    Enhanced version of is_today_present with better timezone handling.
    
    Args:
        df (pandas.DataFrame): DataFrame with datetime column
        date_col (str): Name of the date column
        
    Returns:
        bool: True if today's data is present
    """
    if df is None or df.empty:
        return False
    
    try:
        from utils.config import TIMEZONE
        
        ny_tz = pytz.timezone(TIMEZONE)
        today = datetime.now(ny_tz).date()
        
        if date_col in df.columns:
            # Convert to datetime and extract date
            df_dates = pd.to_datetime(df[date_col]).dt.date
            return today in df_dates.values
        
        return False
    except Exception as e:
        logger.error(f"Error checking if today is present: {e}")
        return False

def append_new_candles(existing_df, new_df):
    """
    Append new candles to existing DataFrame.
    
    Args:
        existing_df (pandas.DataFrame): Existing data
        new_df (pandas.DataFrame): New data to append
        
    Returns:
        pandas.DataFrame: Combined DataFrame
    """
    if existing_df is None or existing_df.empty:
        return new_df
    
    if new_df is None or new_df.empty:
        return existing_df
    
    try:
        combined = pd.concat([existing_df, new_df], ignore_index=True)
        # Remove duplicates if datetime column exists
        if 'datetime' in combined.columns:
            combined = combined.drop_duplicates(subset=['datetime'], keep='last')
            combined = combined.sort_values('datetime')
        return combined
    except Exception as e:
        logger.error(f"Error appending new candles: {e}")
        return existing_df

# Add simple stubs for other functions that might be needed
def format_to_two_decimal(value):
    """Format value to two decimal places."""
    try:
        return round(float(value), 2)
    except:
        return value

def get_previous_day_close(ticker):
    """Get previous day close price - stub implementation."""
    logger.warning(f"get_previous_day_close not implemented for {ticker}")
    return None

def get_premarket_data(ticker):
    """Get premarket data - stub implementation."""
    logger.warning(f"get_premarket_data not implemented for {ticker}")
    return None

def calculate_avg_early_volume(ticker):
    """Calculate average early volume - stub implementation."""
    logger.warning(f"calculate_avg_early_volume not implemented for {ticker}")
    return None

def calculate_vwap(df):
    """Calculate VWAP - stub implementation."""
    logger.warning(f"calculate_vwap not implemented")
    return None

def calculate_avg_daily_volume(ticker):
    """Calculate average daily volume - stub implementation."""
    logger.warning(f"calculate_avg_daily_volume not implemented for {ticker}")
    return None

def is_weekend():
    """
    Check if current day is weekend (Saturday or Sunday).
    
    Returns:
        bool: True if it's weekend (Saturday=5, Sunday=6)
    """
    import datetime
    import pytz
    
    ny_tz = pytz.timezone('America/New_York')
    current_time = datetime.datetime.now(ny_tz)
    current_weekday = current_time.weekday()  # 0=Monday, 6=Sunday
    
    return current_weekday >= 5  # Saturday=5, Sunday=6

def should_use_test_mode():
    """
    Determine if test mode should be used based on configuration and weekend status.
    
    Returns:
        bool: True if test mode should be active
    """
    from utils.config import TEST_MODE, WEEKEND_TEST_MODE_ENABLED
    
    if TEST_MODE == "enabled":
        return True
    elif TEST_MODE == "disabled":
        return False
    else:  # TEST_MODE == "auto"
        return WEEKEND_TEST_MODE_ENABLED and is_weekend()

def log_detailed_operation(ticker, operation, start_time=None, row_count_before=None, row_count_after=None, details=None):
    """
    Log detailed operation information for test mode visibility.
    
    Args:
        ticker (str): Ticker symbol
        operation (str): Operation being performed
        start_time (datetime, optional): When operation started
        row_count_before (int, optional): Row count before operation
        row_count_after (int, optional): Row count after operation
        details (str, optional): Additional details
    """
    import datetime
    
    timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    if start_time:
        duration = (datetime.datetime.now() - start_time).total_seconds()
        duration_str = f" ({duration:.2f}s)"
    else:
        duration_str = ""
    
    log_parts = [f"[{timestamp}] {ticker}: {operation}{duration_str}"]
    
    if row_count_before is not None and row_count_after is not None:
        log_parts.append(f" | Rows: {row_count_before} → {row_count_after}")
    elif row_count_after is not None:
        log_parts.append(f" | Rows: {row_count_after}")
    
    if details:
        log_parts.append(f" | {details}")
    
    logger.info("".join(log_parts))

def cleanup_data_retention(ticker, daily_df, intraday_30min_df, intraday_1min_df):
    """
    Apply data retention limits after full fetch:
    - Daily: Keep only last 200 rows
    - 30-min: Keep only last 500 rows  
    - 1-min: Keep only last 7 days, preserving early volume data and today's feed
    
    Args:
        ticker (str): Ticker symbol
        daily_df (DataFrame): Daily data
        intraday_30min_df (DataFrame): 30-min intraday data
        intraday_1min_df (DataFrame): 1-min intraday data
        
    Returns:
        tuple: (cleaned_daily_df, cleaned_30min_df, cleaned_1min_df)
    """
    import datetime
    import pandas as pd
    
    start_time = datetime.datetime.now()
    
    # Daily data cleanup - keep last 200 rows
    daily_before = len(daily_df) if not daily_df.empty else 0
    cleaned_daily = daily_df.head(200) if not daily_df.empty else daily_df
    daily_after = len(cleaned_daily)
    
    # 30-min data cleanup - keep last 500 rows
    intraday_30min_before = len(intraday_30min_df) if not intraday_30min_df.empty else 0
    cleaned_30min = intraday_30min_df.head(500) if not intraday_30min_df.empty else intraday_30min_df
    intraday_30min_after = len(cleaned_30min)
    
    # 1-min data cleanup - keep last 7 days but preserve early volume and today's data
    intraday_1min_before = len(intraday_1min_df) if not intraday_1min_df.empty else 0
    cleaned_1min = intraday_1min_df
    
    if not intraday_1min_df.empty:
        # Ensure timestamp column exists and is datetime
        if 'timestamp' in intraday_1min_df.columns:
            cleaned_1min = intraday_1min_df.copy()
            try:
                cleaned_1min['timestamp'] = pd.to_datetime(cleaned_1min['timestamp'])
                
                # Keep last 7 calendar days
                seven_days_ago = datetime.datetime.now() - datetime.timedelta(days=7)
                mask = cleaned_1min['timestamp'] >= seven_days_ago
                cleaned_1min = cleaned_1min[mask]
                
                # If filtering resulted in empty data, fallback to row-based limit
                if cleaned_1min.empty:
                    cleaned_1min = intraday_1min_df.head(10080)  # ~7 days worth
            except Exception as e:
                # If timestamp parsing fails, use row-based limit
                cleaned_1min = intraday_1min_df.head(10080)  # ~7 days worth
        else:
            # If no timestamp column, just keep first 7*24*60 = 10080 rows (roughly 7 days of 1-min data)
            cleaned_1min = intraday_1min_df.head(10080)
    
    intraday_1min_after = len(cleaned_1min)
    
    # Log detailed cleanup results
    log_detailed_operation(
        ticker, 
        "Data Cleanup", 
        start_time,
        details=f"Daily: {daily_before}→{daily_after}, 30min: {intraday_30min_before}→{intraday_30min_after}, 1min: {intraday_1min_before}→{intraday_1min_after}"
    )
    
    return cleaned_daily, cleaned_30min, cleaned_1min
