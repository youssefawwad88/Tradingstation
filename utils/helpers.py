import os
import boto3
from botocore.exceptions import NoCredentialsError, ClientError
import pandas as pd
from io import StringIO, BytesIO
from datetime import datetime, time
import pytz
import numpy as np
import json

# --- Environment Variable Loading ---
# Centralized place to get credentials. Ensures consistency.
SPACES_ACCESS_KEY_ID = os.getenv('SPACES_ACCESS_KEY_ID')
SPACES_SECRET_ACCESS_KEY = os.getenv('SPACES_SECRET_ACCESS_KEY')
SPACES_BUCKET_NAME = os.getenv('SPACES_BUCKET_NAME')
SPACES_REGION = os.getenv('SPACES_REGION')

def get_boto_client():
    """
    Initializes and returns a Boto3 S3 client.
    Returns None if credentials are not available.
    """
    if not all([SPACES_ACCESS_KEY_ID, SPACES_SECRET_ACCESS_KEY, SPACES_BUCKET_NAME, SPACES_REGION]):
        print("CRITICAL ERROR: S3 environment variables are not fully configured.")
        return None
    
    try:
        session = boto3.session.Session()
        client = session.client('s3',
                                region_name=SPACES_REGION,
                                endpoint_url=f'https://{SPACES_REGION}.digitaloceanspaces.com',
                                aws_access_key_id=SPACES_ACCESS_KEY_ID,
                                aws_secret_access_key=SPACES_SECRET_ACCESS_KEY)
        return client
    except Exception as e:
        print(f"Error creating Boto3 client: {e}")
        return None

# --- S3 Data I/O Functions ---

def save_df_to_s3(df, file_path):
    """Saves a pandas DataFrame to a CSV file in the DigitalOcean Space."""
    s3_client = get_boto_client()
    if not s3_client: return False
    
    try:
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)
        s3_client.put_object(Bucket=SPACES_BUCKET_NAME, Key=file_path, Body=csv_buffer.getvalue())
        print(f"Successfully saved DataFrame to s3://{SPACES_BUCKET_NAME}/{file_path}")
        return True
    except (NoCredentialsError, ClientError, Exception) as e:
        print(f"Error saving DataFrame to {file_path}: {e}")
        return False

def read_df_from_s3(file_path):
    """Reads a CSV file from the DigitalOcean Space into a pandas DataFrame."""
    s3_client = get_boto_client()
    if not s3_client: return pd.DataFrame()

    try:
        response = s3_client.get_object(Bucket=SPACES_BUCKET_NAME, Key=file_path)
        df = pd.read_csv(response['Body'])
        print(f"Successfully read DataFrame from s3://{SPACES_BUCKET_NAME}/{file_path}")
        return df
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchKey':
            print(f"File not found at {file_path}. Returning empty DataFrame.")
        else:
            print(f"ClientError reading DataFrame from {file_path}: {e}")
        return pd.DataFrame()
    except Exception as e:
        print(f"An unexpected error occurred reading DataFrame from {file_path}: {e}")
        return pd.DataFrame()

def save_list_to_s3(data_list, file_path):
    """Saves a Python list of strings to a text file in the DigitalOcean Space."""
    s3_client = get_boto_client()
    if not s3_client: return False

    try:
        content = "\n".join(data_list)
        s3_client.put_object(Bucket=SPACES_BUCKET_NAME, Key=file_path, Body=content)
        print(f"Successfully saved list to s3://{SPACES_BUCKET_NAME}/{file_path}")
        return True
    except (NoCredentialsError, ClientError, Exception) as e:
        print(f"Error saving list to {file_path}: {e}")
        return False

def read_tickerlist_from_s3(file_path='tickerlist.txt'):
    """Reads a simple text file (like the tickerlist) from the Space."""
    s3_client = get_boto_client()
    if not s3_client: return []

    try:
        response = s3_client.get_object(Bucket=SPACES_BUCKET_NAME, Key=file_path)
        content = response['Body'].read().decode('utf-8')
        tickers = [line.strip().upper() for line in content.split('\n') if line.strip()]
        print(f"Successfully read {len(tickers)} tickers from s3://{SPACES_BUCKET_NAME}/{file_path}")
        return tickers
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchKey':
            print(f"Tickerlist not found at {file_path}. Returning empty list.")
        else:
            print(f"ClientError reading tickerlist from {file_path}: {e}")
        return []
    except Exception as e:
        print(f"An unexpected error occurred reading tickerlist from {file_path}: {e}")
        return []

def update_scheduler_status(job_name, status, details=""):
    """Updates or creates a scheduler status log in DigitalOcean Spaces."""
    s3_client = get_boto_client()
    if not s3_client: return False

    log_file_key = 'data/logs/scheduler_status.csv'
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
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

# --- NEW: Configuration File Helpers ---

def save_config_to_s3(config_dict, file_path):
    """Saves a Python dictionary to a JSON file in the DigitalOcean Space."""
    s3_client = get_boto_client()
    if not s3_client: return False

    try:
        content = json.dumps(config_dict, indent=4)
        s3_client.put_object(Bucket=SPACES_BUCKET_NAME, Key=file_path, Body=content)
        print(f"Successfully saved config to s3://{SPACES_BUCKET_NAME}/{file_path}")
        return True
    except (NoCredentialsError, ClientError, Exception) as e:
        print(f"Error saving config to {file_path}: {e}")
        return False

def read_config_from_s3(file_path):
    """Reads a JSON config file from the Space into a Python dictionary."""
    s3_client = get_boto_client()
    if not s3_client: return {}

    try:
        response = s3_client.get_object(Bucket=SPACES_BUCKET_NAME, Key=file_path)
        content = response['Body'].read().decode('utf-8')
        config = json.loads(content)
        print(f"Successfully read config from s3://{SPACES_BUCKET_NAME}/{file_path}")
        return config
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchKey':
            print(f"Config file not found at {file_path}. Returning empty dict.")
        else:
            print(f"ClientError reading config from {file_path}: {e}")
        return {}
    except Exception as e:
        print(f"An unexpected error occurred reading config from {file_path}: {e}")
        return {}

# --- General Calculation Helpers ---

def format_to_two_decimal(value):
    if isinstance(value, (int, float)) and not np.isnan(value):
        return f"{value:.2f}"
    return value

def calculate_vwap(df):
    """Calculates VWAP for a given DataFrame with 'close', 'volume', 'high', 'low'."""
    q = (df['close'] + df['high'] + df['low']) / 3 * df['volume']
    return q.cumsum() / df['volume'].cumsum()

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

def get_previous_day_close(daily_df):
    """Safely gets the previous day's close from a daily DataFrame."""
    if daily_df is not None and len(daily_df) > 1:
        return daily_df['close'].iloc[-2]
    return None
