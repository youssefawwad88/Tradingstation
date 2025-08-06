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

def is_today_present(df):
    """
    Check if today's date is present in the intraday DataFrame.
    
    Args:
        df: DataFrame with Date column (as per existing format) or timestamp column or datetime index
        
    Returns:
        bool: True if today's data is present, False otherwise
    """
    if df is None or df.empty:
        return False
    
    try:
        # Get today's date in NY timezone
        ny_tz = pytz.timezone('America/New_York')
        today = datetime.now(ny_tz).date()
        
        # Handle DataFrame with Date column (existing format)
        if 'Date' in df.columns:
            df_copy = df.copy()
            df_copy['Date'] = pd.to_datetime(df_copy['Date'])
            
            # Convert to NY timezone if needed
            if df_copy['Date'].dt.tz is None:
                # Assume UTC if no timezone info
                df_copy['Date'] = df_copy['Date'].dt.tz_localize('UTC').dt.tz_convert(ny_tz)
            else:
                df_copy['Date'] = df_copy['Date'].dt.tz_convert(ny_tz)
            
            dates = df_copy['Date'].dt.date
        # Handle DataFrame with timestamp column
        elif 'timestamp' in df.columns:
            df_copy = df.copy()
            df_copy['timestamp'] = pd.to_datetime(df_copy['timestamp'])
            
            # Convert to NY timezone if needed
            if df_copy['timestamp'].dt.tz is None:
                # Assume UTC if no timezone info
                df_copy['timestamp'] = df_copy['timestamp'].dt.tz_localize('UTC').dt.tz_convert(ny_tz)
            else:
                df_copy['timestamp'] = df_copy['timestamp'].dt.tz_convert(ny_tz)
            
            dates = df_copy['timestamp'].dt.date
        else:
            # Handle DataFrame with datetime index
            df_copy = df.copy()
            if df_copy.index.tz is None:
                df_copy.index = df_copy.index.tz_localize('UTC').tz_convert(ny_tz)
            else:
                df_copy.index = df_copy.index.tz_convert(ny_tz)
            
            dates = df_copy.index.date
        
        return today in dates.unique()
        
    except Exception as e:
        print(f"Error in is_today_present: {e}")
        return False

def get_last_market_day():
    """
    Get the last market day (excluding weekends).
    
    Returns:
        datetime.date: The last market day
    """
    try:
        ny_tz = pytz.timezone('America/New_York')
        now = datetime.now(ny_tz)
        current_date = now.date()
        
        # If it's Monday, last market day was Friday
        if current_date.weekday() == 0:  # Monday
            last_market_day = current_date - pd.Timedelta(days=3)
        # If it's Sunday, last market day was Friday  
        elif current_date.weekday() == 6:  # Sunday
            last_market_day = current_date - pd.Timedelta(days=2)
        # Otherwise, yesterday was a market day (assuming no holidays)
        else:
            last_market_day = current_date - pd.Timedelta(days=1)
            
        return last_market_day.date() if hasattr(last_market_day, 'date') else last_market_day
        
    except Exception as e:
        print(f"Error in get_last_market_day: {e}")
        return (datetime.now() - pd.Timedelta(days=1)).date()

def trim_to_rolling_window(df, days=5):
    """
    Trim DataFrame to keep only the last N days plus current day.
    
    Args:
        df: DataFrame with Date column (as per existing format) or timestamp column or datetime index
        days: Number of days to keep (default 5)
        
    Returns:
        DataFrame: Trimmed DataFrame
    """
    if df is None or df.empty:
        return df
    
    try:
        ny_tz = pytz.timezone('America/New_York')
        today = datetime.now(ny_tz).date()
        cutoff_date = today - pd.Timedelta(days=days)
        
        # Handle DataFrame with Date column (existing format)
        if 'Date' in df.columns:
            df_copy = df.copy()
            df_copy['Date'] = pd.to_datetime(df_copy['Date'])
            
            # Convert to NY timezone if needed
            if df_copy['Date'].dt.tz is None:
                df_copy['Date'] = df_copy['Date'].dt.tz_localize('UTC').dt.tz_convert(ny_tz)
            else:
                df_copy['Date'] = df_copy['Date'].dt.tz_convert(ny_tz)
            
            # Filter to keep only last N days + today
            mask = df_copy['Date'].dt.date >= cutoff_date
            return df_copy[mask].copy()
        # Handle DataFrame with timestamp column
        elif 'timestamp' in df.columns:
            df_copy = df.copy()
            df_copy['timestamp'] = pd.to_datetime(df_copy['timestamp'])
            
            # Convert to NY timezone if needed
            if df_copy['timestamp'].dt.tz is None:
                df_copy['timestamp'] = df_copy['timestamp'].dt.tz_localize('UTC').dt.tz_convert(ny_tz)
            else:
                df_copy['timestamp'] = df_copy['timestamp'].dt.tz_convert(ny_tz)
            
            # Filter to keep only last N days + today
            mask = df_copy['timestamp'].dt.date >= cutoff_date
            return df_copy[mask].copy()
        else:
            # Handle DataFrame with datetime index
            df_copy = df.copy()
            if df_copy.index.tz is None:
                df_copy.index = df_copy.index.tz_localize('UTC').tz_convert(ny_tz)
            else:
                df_copy.index = df_copy.index.tz_convert(ny_tz)
            
            # Filter to keep only last N days + today
            mask = df_copy.index.date >= cutoff_date
            return df_copy[mask].copy()
            
    except Exception as e:
        print(f"Error in trim_to_rolling_window: {e}")
        return df

# --- MANUAL TICKER MANAGEMENT ---
def load_manual_tickers():
    """
    Load manually selected tickers from ticker_selectors/tickerlist.txt
    Always include these tickers regardless of whether it's Sunday or scheduled runs.
    Returns deduplicated list of uppercase tickers.
    """
    try:
        file_path = 'ticker_selectors/tickerlist.txt'
        content = spaces_manager.download_string(file_path)
        if content is None:
            print(f"Warning: Could not load manual tickers from {file_path}")
            return []
        
        # Process each line, remove numbering (like "1.NVDA"), strip whitespace, and uppercase
        manual_tickers = []
        for line in content.split('\n'):
            line = line.strip()
            if line:
                # Remove numbering pattern like "1.", "2.", etc.
                if '.' in line and line.split('.')[0].isdigit():
                    ticker = line.split('.', 1)[1].strip().upper()
                else:
                    ticker = line.strip().upper()
                if ticker:
                    manual_tickers.append(ticker)
        
        # Return deduplicated list
        return list(set(manual_tickers))
        
    except Exception as e:
        print(f"Error loading manual tickers: {e}")
        return []

def is_today(timestamp_str, format_str="%Y-%m-%d %H:%M:%S"):
    """
    Check if a timestamp string represents today's date (U.S. Eastern Time preferred).
    
    Args:
        timestamp_str: Timestamp string to check
        format_str: Format of the timestamp string
        
    Returns:
        bool: True if timestamp is for today's date, False otherwise
    """
    try:
        # Parse the timestamp
        dt = datetime.strptime(timestamp_str, format_str)
        
        # Get today's date in Eastern Time
        ny_tz = pytz.timezone('America/New_York')
        today_ny = datetime.now(ny_tz).date()
        
        # If timestamp is naive, assume it's in NY timezone
        if dt.tzinfo is None:
            dt = ny_tz.localize(dt)
        else:
            dt = dt.astimezone(ny_tz)
            
        return dt.date() == today_ny
        
    except Exception as e:
        print(f"Error checking if timestamp is today: {e}")
        return False

def append_new_candles(ticker, new_rows, file_path):
    """
    Append new candles to existing ticker data with proper deduplication.
    Only appends candles that are newer than the last saved timestamp and are for today.
    
    Args:
        ticker: Stock symbol
        new_rows: List of dictionaries or DataFrame with new candle data
        file_path: Path to save the data (e.g., "data/intraday/AAPL_1min.csv")
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Convert new_rows to DataFrame if it's a list
        if isinstance(new_rows, list):
            if not new_rows:
                return True  # Nothing to append
            df_new = pd.DataFrame(new_rows)
        else:
            df_new = new_rows.copy()
            
        if df_new.empty:
            return True
            
        # Determine timestamp column name
        timestamp_col = None
        for col in ['timestamp', 'Date', 'date', 'datetime']:
            if col in df_new.columns:
                timestamp_col = col
                break
                
        if timestamp_col is None:
            print(f"Error: No timestamp column found in new data for {ticker}")
            return False
            
        # Read existing data
        df_existing = read_df_from_s3(file_path)
        
        if not df_existing.empty:
            # Find timestamp column in existing data
            existing_timestamp_col = None
            for col in ['timestamp', 'Date', 'date', 'datetime']:
                if col in df_existing.columns:
                    existing_timestamp_col = col
                    break
                    
            if existing_timestamp_col is None:
                print(f"Error: No timestamp column found in existing data for {ticker}")
                return False
                
            # Ensure both DataFrames use the same timestamp column name
            if timestamp_col != existing_timestamp_col:
                df_new = df_new.rename(columns={timestamp_col: existing_timestamp_col})
                timestamp_col = existing_timestamp_col
                
            # Convert timestamps to datetime
            df_existing[timestamp_col] = pd.to_datetime(df_existing[timestamp_col])
            df_new[timestamp_col] = pd.to_datetime(df_new[timestamp_col])
            
            # Get the last timestamp from existing data
            last_time = df_existing[timestamp_col].max()
            
            # Filter new rows: only keep those that are newer than last saved timestamp and are for today
            today_mask = df_new[timestamp_col].apply(lambda x: is_today(x.strftime("%Y-%m-%d %H:%M:%S")))
            newer_mask = df_new[timestamp_col] > last_time
            
            # Apply both filters
            df_new_filtered = df_new[today_mask & newer_mask]
            
            if df_new_filtered.empty:
                print(f"No new candles to append for {ticker} (already up to date)")
                return True
                
            # Combine existing and new data
            df_combined = pd.concat([df_existing, df_new_filtered], ignore_index=True)
        else:
            # No existing data, filter new data to only today's candles
            df_new[timestamp_col] = pd.to_datetime(df_new[timestamp_col])
            today_mask = df_new[timestamp_col].apply(lambda x: is_today(x.strftime("%Y-%m-%d %H:%M:%S")))
            df_combined = df_new[today_mask].copy()
            
        # Remove any duplicates based on timestamp (keep last occurrence)
        df_combined = df_combined.drop_duplicates(subset=[timestamp_col], keep='last')
        
        # Sort by timestamp
        df_combined = df_combined.sort_values(by=timestamp_col, ascending=True)
        
        # Save back to S3
        save_df_to_s3(df_combined, file_path)
        print(f"Successfully appended {len(df_combined) - (len(df_existing) if not df_existing.empty else 0)} new candles for {ticker}")
        
        return True
        
    except Exception as e:
        print(f"Error appending new candles for {ticker}: {e}")
        return False
