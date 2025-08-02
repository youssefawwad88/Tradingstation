import os
import boto3
from botocore.exceptions import NoCredentialsError, ClientError
import pandas as pd
from io import StringIO
from datetime import datetime, time
import pytz
import numpy as np

# --- S3 Client Initialization ---
SPACES_ACCESS_KEY_ID = os.getenv('SPACES_ACCESS_KEY_ID')
SPACES_SECRET_ACCESS_KEY = os.getenv('SPACES_SECRET_ACCESS_KEY')
SPACES_BUCKET_NAME = os.getenv('SPACES_BUCKET_NAME')
SPACES_REGION = os.getenv('SPACES_REGION')

def get_boto_client():
    """Initializes and returns a Boto3 S3 client, or None if config is missing."""
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

# --- Core S3 I/O Functions ---
def read_df_from_s3(file_path):
    """Reads a CSV file from S3 into a pandas DataFrame."""
    s3_client = get_boto_client()
    if not s3_client: return pd.DataFrame()
    try:
        response = s3_client.get_object(Bucket=SPACES_BUCKET_NAME, Key=file_path)
        return pd.read_csv(response['Body'])
    except ClientError:
        # This is an expected failure (e.g., file doesn't exist yet), so no error is printed.
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

def save_list_to_s3(data_list, file_path):
    """Saves a Python list to a text file in S3."""
    s3_client = get_boto_client()
    if not s3_client: return False
    try:
        content = "\n".join(data_list)
        s3_client.put_object(Bucket=SPACES_BUCKET_NAME, Key=file_path, Body=content)
        return True
    except Exception as e:
        print(f"Error saving list to {file_path}: {e}")
        return False

def list_files_in_s3_dir(prefix):
    """Lists all file names in a given 'directory' in the S3 bucket."""
    s3_client = get_boto_client()
    if not s3_client: return []
    try:
        paginator = s3_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=SPACES_BUCKET_NAME, Prefix=prefix)
        return [os.path.basename(obj['Key']) for page in pages if "Contents" in page for obj in page['Contents'] if not obj['Key'].endswith('/')]
    except Exception as e:
        print(f"Error listing files in {prefix}: {e}")
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
        idx = status_df[status_df['job_name'] == job_name].index[0]
        status_df.loc[idx, ['last_run_timestamp', 'status', 'details']] = [timestamp, status, details]
    else:
        new_row = pd.DataFrame([{'job_name': job_name, 'last_run_timestamp': timestamp, 'status': status, 'details': details}])
        status_df = pd.concat([status_df, new_row], ignore_index=True)
    
    return save_df_to_s3(status_df, log_file_key)

def detect_market_session():
    """Detects the current market session based on New York time."""
    ny_time = datetime.now(pytz.timezone('America/New_York')).time()
    if time(4, 0) <= ny_time < time(9, 30): return 'PRE-MARKET'
    if time(9, 30) <= ny_time < time(16, 0): return 'REGULAR'
    return 'CLOSED'

# --- ALL REQUIRED CALCULATION HELPERS ---

def format_to_two_decimal(value):
    """Rounds numeric values to 2 decimal places, handling errors safely."""
    try: return round(float(value), 2)
    except (ValueError, TypeError): return None

def calculate_vwap(df):
    """Calculates Volume Weighted Average Price (VWAP)."""
    if df.empty or not all(k in df for k in ['high', 'low', 'close', 'volume']): return None
    try:
        tp = (df['high'] + df['low'] + df['close']) / 3
        vol = df['volume']
        return round((tp * vol).sum() / vol.sum(), 2)
    except ZeroDivisionError: return None

def get_premarket_data(intraday_df):
    """Extracts pre-market candles from a full day's intraday data."""
    if intraday_df.empty or 'timestamp' not in intraday_df.columns: return pd.DataFrame()
    df = intraday_df.copy()
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df = df.set_index('timestamp').tz_localize('UTC').tz_convert('America/New_York')
    return df.between_time('04:00', '09:29:59')

def get_previous_day_close(daily_df):
    """Safely gets the previous day's close from a daily DataFrame."""
    if daily_df is not None and len(daily_df) > 1:
        df = daily_df.copy()
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        return df.sort_values(by='timestamp', ascending=False)['close'].iloc[1]
    return None

def calculate_avg_early_volume(historical_intraday_df, days=5):
    """Calculates the average volume for the 9:30-9:44 AM ET window over the last N days."""
    if historical_intraday_df.empty: return None
    df = historical_intraday_df.copy()
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df = df.set_index('timestamp').tz_localize('UTC').tz_convert('America/New_York')
    
    early_session_df = df.between_time('09:30', '09:44')
    if early_session_df.empty: return None
    
    daily_early_volume = early_session_df.groupby(early_session_df.index.date)['volume'].sum()
    return daily_early_volume.tail(days).mean()

def calculate_avg_daily_volume(daily_df, days=10):
    """Calculates the average daily volume over the last N days."""
    if daily_df is not None and len(daily_df) >= days:
        return daily_df.sort_values(by='timestamp', ascending=False).head(days)['volume'].mean()
    return None
