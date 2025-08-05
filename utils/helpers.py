import os
import boto3
from botocore.exceptions import NoCredentialsError, ClientError
import pandas as pd
from io import StringIO
import json
from datetime import datetime, time
import pytz
import numpy as np

from .spaces_manager import spaces_manager
from .config import DO_SPACES_CONFIG

def get_boto_client():
    """Initializes and returns a Boto3 S3 client. Maintained for backward compatibility."""
    return spaces_manager.client

# --- Core S3 Data I/O Functions ---
def read_df_from_s3(file_path):
    """Reads a CSV file from S3 into a pandas DataFrame."""
    return spaces_manager.download_dataframe(file_path)

def save_df_to_s3(df, file_path):
    """Saves a pandas DataFrame to a CSV file in S3."""
    return spaces_manager.upload_dataframe(df, file_path)

def read_tickerlist_from_s3(file_path='tickerlist.txt'):
    """
    Reads a list of tickers from S3. Handles both simple .txt files
    and the first column of a .csv file.
    """
    try:
        content = spaces_manager.download_string(file_path)
        if content is None:
            return []
        
        if file_path.lower().endswith('.csv'):
            # If it's a CSV, read it with pandas and take the first column
            df = pd.read_csv(StringIO(content))
            return df.iloc[:, 0].dropna().unique().tolist()
        else:
            # Otherwise, treat it as a simple text file
            return [line.strip().upper() for line in content.split('\n') if line.strip()]

    except Exception as e:
        print(f"Error reading tickerlist from {file_path}: {e}")
        return []

def save_list_to_s3(data_list, file_path):
    """Saves a Python list of strings to a text file in S3."""
    return spaces_manager.upload_list(data_list, file_path)

# --- System Status & Session Helpers ---
def update_scheduler_status(job_name, status, details=""):
    """Updates or creates a scheduler status log in S3."""
    log_file_key = 'data/logs/scheduler_status.csv'
    timestamp = datetime.now(pytz.utc).strftime('%Y-%m-%d %H:%M:%S')
    status_df = read_df_from_s3(log_file_key)
    
    if status_df.empty:
        status_df = pd.DataFrame(columns=['job_name', 'last_run_timestamp', 'status', 'details'])
    
    if job_name in status_df['job_name'].values:
        job_index = status_df[status_df['job_name'] == job_name].index[0]
        status_df.loc[job_index, ['last_run_timestamp', 'status', 'details']] = [timestamp, status, details]
    else:
        new_row = pd.DataFrame([{'job_name': job_name, 'last_run_timestamp': timestamp, 'status': status, 'details': details}])
        status_df = pd.concat([status_df, new_row], ignore_index=True)
    
    return save_df_to_s3(status_df, log_file_key)

def detect_market_session():
    """Detects the current market session based on New York time."""
    ny_timezone = pytz.timezone('America/New_York')
    ny_time = datetime.now(ny_timezone).time()
    if time(4, 0) <= ny_time < time(9, 30):
        return 'PRE-MARKET'
    elif time(9, 30) <= ny_time < time(16, 0):
        return 'REGULAR'
    else:
        return 'CLOSED'

# --- SHARED CALCULATION HELPERS ---
def format_to_two_decimal(value):
    try:
        if value is None:
            return None
        float_val = float(value)
        if np.isnan(float_val) or np.isinf(float_val):
            return None
        return round(float_val, 2)
    except (ValueError, TypeError):
        return None

def calculate_vwap(df):
    if df.empty or 'high' not in df.columns or 'volume' not in df.columns:
        return None
    try:
        typical_price = (df['high'] + df['low'] + df['close']) / 3
        return round((typical_price * df['volume']).sum() / df['volume'].sum(), 2)
    except (TypeError, ZeroDivisionError):
        return None

def calculate_ema(series, span):
    return series.ewm(span=span, adjust=False).mean()

def calculate_sma(series, window):
    return series.rolling(window=window).mean()

def calculate_atr(df, window=14):
    if df.empty: return None
    high_low = df['high'] - df['low']
    high_close = abs(df['high'] - df['close'].shift())
    low_close = abs(df['low'] - df['close'].shift())
    tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
    return tr.rolling(window=window).mean()

def is_volume_spike(current_volume, avg_volume, threshold=1.15):
    try:
        return current_volume >= avg_volume * threshold
    except (ValueError, TypeError):
        return False

def get_premarket_data(intraday_df):
    """
    Extract premarket data (4:00 AM - 9:29:59 AM ET) from intraday DataFrame.
    Returns DataFrame with premarket data only.
    """
    if intraday_df.empty:
        return pd.DataFrame()
    
    try:
        df = intraday_df.copy()
        
        # Handle different timestamp formats
        if 'timestamp' in df.columns:
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            df = df.set_index('timestamp')
        
        # Ensure we have a datetime index
        if not isinstance(df.index, pd.DatetimeIndex):
            return pd.DataFrame()
        
        # Handle timezone conversion carefully
        ny_tz = pytz.timezone('America/New_York')
        if df.index.tz is None:
            # Assume data is already in ET if no timezone info
            df.index = df.index.tz_localize(ny_tz)
        elif df.index.tz != ny_tz:
            df.index = df.index.tz_convert(ny_tz)
        
        # Filter for premarket hours (4:00 AM - 9:29:59 AM ET)
        pre_market_df = df.between_time('04:00', '09:29:59')
        return pre_market_df
        
    except Exception as e:
        print(f"Error in get_premarket_data: {e}")
        return pd.DataFrame()

def get_previous_day_close(daily_df):
    if daily_df is not None and len(daily_df) > 1:
        daily_df['timestamp'] = pd.to_datetime(daily_df['timestamp'])
        daily_df = daily_df.sort_values(by='timestamp', ascending=False)
        return daily_df['close'].iloc[1]
    return None

def calculate_avg_daily_volume(daily_df, window=20):
    if daily_df is not None and not daily_df.empty and 'volume' in daily_df.columns:
        return daily_df['volume'].rolling(window=window).mean().iloc[-1]
    return 0

def calculate_avg_early_volume(intraday_df, days=5):
    """
    Calculate average early volume for the first 15 minutes of trading (9:30-9:44 AM)
    for a given number of days from intraday data.
    """
    if intraday_df is None or intraday_df.empty:
        return 0
    
    try:
        # Ensure timestamp is in datetime format and set as index
        if 'timestamp' in intraday_df.columns:
            df = intraday_df.copy()
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            df = df.set_index('timestamp')
        else:
            df = intraday_df.copy()
        
        # Convert to NY timezone if not already
        if df.index.tz is None:
            df.index = df.index.tz_localize('UTC').tz_convert('America/New_York')
        elif df.index.tz != pytz.timezone('America/New_York'):
            df.index = df.index.tz_convert('America/New_York')
        
        # Get early volume for each day (9:30-9:44 AM)
        early_volumes = []
        unique_dates = df.index.date
        unique_dates = sorted(set(unique_dates))[-days:]  # Get last N days
        
        for date in unique_dates:
            day_data = df[df.index.date == date]
            early_data = day_data.between_time('09:30', '09:44')
            if not early_data.empty:
                early_volumes.append(early_data['volume'].sum())
        
        return sum(early_volumes) / len(early_volumes) if early_volumes else 0
        
    except Exception as e:
        print(f"Error in calculate_avg_early_volume: {e}")
        return 0
