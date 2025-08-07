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

def save_df_to_s3(df, ticker, interval, s3_prefix='intraday'):
    """
    Save DataFrame to DigitalOcean Spaces.
    
    Args:
        df (pandas.DataFrame): DataFrame to save
        ticker (str): Stock ticker symbol
        interval (str): Time interval of the data
        s3_prefix (str): Prefix/folder in the S3 bucket
        
    Returns:
        bool: True if successful, False otherwise
    """
    object_name = f"{s3_prefix}/{ticker}_{interval}.csv"
    
    success = upload_dataframe(df, object_name)
    if success:
        logger.info(f"Uploaded {ticker} data to Spaces at {object_name}")
    else:
        logger.error(f"Failed to upload {ticker} data to Spaces at {object_name}")
    
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
