import os
import boto3
from botocore.exceptions import NoCredentialsError, ClientError
import pandas as pd
from io import StringIO
import json
from datetime import datetime, time
import pytz
import numpy as np

# --- Environment Variable Loading & S3 Client Initialization ---
SPACES_ACCESS_KEY_ID = os.getenv('SPACES_ACCESS_KEY_ID')
SPACES_SECRET_ACCESS_KEY = os.getenv('SPACES_SECRET_ACCESS_KEY')
SPACES_BUCKET_NAME = os.getenv('SPACES_BUCKET_NAME')
SPACES_REGION = os.getenv('SPACES_REGION')

def get_boto_client():
    """Initializes and returns a Boto3 S3 client."""
    if not all([SPACES_ACCESS_KEY_ID, SPACES_SECRET_ACCESS_KEY, SPACES_BUCKET_NAME, SPACES_REGION]):
        print("CRITICAL ERROR: S3 environment variables are not fully configured.")
        return None
    try:
        session = boto3.session.Session()
        return session.client('s3',
                              region_name=SPACES_REGION,
                              endpoint_url=f'https://{SPACES_REGION}.digitaloceanspaces.com',
                              aws_access_key_id=SPACES_ACCESS_KEY_ID,
                              aws_secret_access_key=SPACES_SECRET_ACCESS_KEY)
    except Exception as e:
        print(f"Error creating Boto3 client: {e}")
        return None

# --- Core S3 Data I/O Functions ---
def read_df_from_s3(file_path):
    """Reads a CSV file from S3 into a pandas DataFrame."""
    s3_client = get_boto_client()
    if not s3_client: return pd.DataFrame()
    try:
        response = s3_client.get_object(Bucket=SPACES_BUCKET_NAME, Key=file_path)
        return pd.read_csv(response['Body'])
    except ClientError as e:
        if e.response['Error']['Code'] != 'NoSuchKey':
            print(f"ClientError reading DataFrame from {file_path}: {e}")
        return pd.DataFrame()
    except Exception as e:
        print(f"Unexpected error reading DataFrame from {file_path}: {e}")
        return pd.DataFrame()

def save_df_to_s3(df, file_path):
    """Saves a pandas DataFrame to a CSV file in S3."""
    s3_client = get_boto_client()
    if not s3_client: return False
    try:
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)
        s3_client.put_object(Bucket=SPACES_BUCKET_NAME, Key=file_path, Body=csv_buffer.getvalue())
        return True
    except Exception as e:
        print(f"Error saving DataFrame to {file_path}: {e}")
        return False

def read_tickerlist_from_s3(file_path='tickerlist.txt'):
    """Reads a simple text file (like the tickerlist) from S3."""
    s3_client = get_boto_client()
    if not s3_client: return []
    try:
        response = s3_client.get_object(Bucket=SPACES_BUCKET_NAME, Key=file_path)
        content = response['Body'].read().decode('utf-8')
        return [line.strip().upper() for line in content.split('\n') if line.strip()]
    except Exception as e:
        print(f"Error reading tickerlist from {file_path}: {e}")
        return []

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
        status_df.loc[job_index, 'last_run_timestamp'] = timestamp
        status_df.loc[job_index, 'status'] = status
        status_df.loc[job_index, 'details'] = details
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

# --- SHARED CALCULATION HELPERS (As per your logic) ---

def format_to_two_decimal(value):
    """Round numeric values to 2 decimal places, handling errors safely."""
    try:
        return round(float(value), 2)
    except (ValueError, TypeError):
        return None

def calculate_vwap(df):
    """Calculate Volume Weighted Average Price (VWAP)."""
    if df.empty or 'high' not in df.columns or 'volume' not in df.columns:
        return None
    try:
        typical_price = (df['high'] + df['low'] + df['close']) / 3
        return round((typical_price * df['volume']).sum() / df['volume'].sum(), 2)
    except (TypeError, ZeroDivisionError):
        return None

def calculate_ema(series, span):
    """Calculate Exponential Moving Average."""
    return series.ewm(span=span, adjust=False).mean()

def calculate_sma(series, window):
    """Calculate Simple Moving Average."""
    return series.rolling(window=window).mean()

def calculate_atr(df, window=14):
    """Calculate Average True Range."""
    if df.empty: return None
    high_low = df['high'] - df['low']
    high_close = abs(df['high'] - df['close'].shift())
    low_close = abs(df['low'] - df['close'].shift())
    tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
    return tr.rolling(window=window).mean()

def is_volume_spike(current_volume, avg_volume, threshold=1.15):
    """Check if current volume is a spike compared to average volume."""
    try:
        return current_volume >= avg_volume * threshold
    except (ValueError, TypeError):
        return False

def get_premarket_data(intraday_df):
    """Extracts pre-market candles and calculates key stats."""
    if intraday_df.empty or 'timestamp' not in intraday_df.columns:
        return {"pre_high": None, "pre_low": None, "pre_vwap": None, "pre_volume": 0, "pre_range": None}

    df = intraday_df.copy()
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df = df.set_index('timestamp').tz_localize('UTC').tz_convert('America/New_York')
    pre_market_df = df.between_time('04:00', '09:29:59')

    if pre_market_df.empty:
        return {"pre_high": None, "pre_low": None, "pre_vwap": None, "pre_volume": 0, "pre_range": None}

    pre_high = pre_market_df['high'].max()
    pre_low = pre_market_df['low'].min()
    pre_volume = int(pre_market_df['volume'].sum())
    pre_vwap = calculate_vwap(pre_market_df)
    pre_range = pre_high - pre_low if pre_high is not None and pre_low is not None else None

    return {
        "pre_high": pre_high, "pre_low": pre_low, "pre_vwap": pre_vwap,
        "pre_volume": pre_volume, "pre_range": pre_range
    }
