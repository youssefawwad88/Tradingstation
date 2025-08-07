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
    DEBUG_MODE
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
    Save DataFrame to DigitalOcean Spaces (with flexible parameters).
    
    Args:
        df (pandas.DataFrame): DataFrame to save
        object_name_or_ticker (str): Object name/path in S3 OR ticker symbol  
        interval (str, optional): Time interval of the data (if ticker provided)
        s3_prefix (str): Prefix/folder in the S3 bucket (if ticker provided)
        
    Returns:
        bool: True if successful, False otherwise
    """
    # Determine if we got an object name or ticker
    if interval is not None:
        # Old-style call with ticker and interval
        object_name = f"{s3_prefix}/{object_name_or_ticker}_{interval}.csv"
        logger.info(f"Uploading {object_name_or_ticker} data to Spaces at {object_name}")
    else:
        # New-style call with direct object name
        object_name = object_name_or_ticker
        logger.info(f"Uploading DataFrame to Spaces at {object_name}")
    
    success = upload_dataframe(df, object_name)
    if success:
        logger.info(f"Successfully uploaded to Spaces at {object_name}")
    else:
        logger.error(f"Failed to upload to Spaces at {object_name}")
    
    return success

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
    Load manual ticker list.
    
    Returns:
        list: List of manual tickers
    """
    # Try to read from a manual tickers file, fallback to default
    manual_file = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "manual_tickers.txt")
    if os.path.exists(manual_file):
        try:
            with open(manual_file, 'r') as f:
                tickers = [line.strip() for line in f.readlines() if line.strip()]
            logger.info(f"Loaded {len(tickers)} manual tickers")
            return tickers
        except Exception as e:
            logger.error(f"Error reading manual tickers: {e}")
    
    from utils.config import DEFAULT_TICKERS
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
    Trim DataFrame to rolling window.
    
    Args:
        df (pandas.DataFrame): DataFrame to trim
        window_days (int): Number of days to keep
        
    Returns:
        pandas.DataFrame: Trimmed DataFrame
    """
    if df is None or df.empty:
        return df
    
    try:
        cutoff_date = datetime.now() - pd.Timedelta(days=window_days)
        if 'datetime' in df.columns:
            df_filtered = df[pd.to_datetime(df['datetime']) >= cutoff_date]
            return df_filtered
        return df
    except Exception as e:
        logger.error(f"Error trimming to rolling window: {e}")
        return df

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
