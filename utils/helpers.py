import os
import pandas as pd
import boto3
from io import StringIO
from datetime import time, datetime
import pytz
import numpy as np

# --- Cloud Storage (S3) Configuration ---
SPACES_ACCESS_KEY_ID = os.getenv('SPACES_ACCESS_KEY_ID')
SPACES_SECRET_ACCESS_KEY = os.getenv('SPACES_SECRET_ACCESS_KEY')
SPACES_BUCKET_NAME = os.getenv('SPACES_BUCKET_NAME')
SPACES_REGION = os.getenv('SPACES_REGION')

s3_client = None
if all([SPACES_ACCESS_KEY_ID, SPACES_SECRET_ACCESS_KEY, SPACES_BUCKET_NAME, SPACES_REGION]):
    s3_client = boto3.client(
        's3',
        region_name=SPACES_REGION,
        endpoint_url=f'https://{SPACES_REGION}.digitaloceanspaces.com',
        aws_access_key_id=SPACES_ACCESS_KEY_ID,
        aws_secret_access_key=SPACES_SECRET_ACCESS_KEY
    )
else:
    print("CRITICAL WARNING: S3 client not initialized. Missing one or more environment variables.")
    # This will cause the script to fail if any S3 functions are called, which is intended.

# --- S3 Helper Functions ---

def list_files_in_s3_dir(prefix):
    """Lists all files in a given 'directory' in the S3 bucket."""
    if not s3_client: return []
    try:
        paginator = s3_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=SPACES_BUCKET_NAME, Prefix=prefix)
        file_list = [os.path.basename(obj['Key']) for page in pages if "Contents" in page for obj in page['Contents'] if not obj['Key'].endswith('/')]
        print(f"Found {len(file_list)} files in s3://{SPACES_BUCKET_NAME}/{prefix}")
        return file_list
    except Exception as e:
        print(f"Error listing files in {prefix}: {e}")
        return []

def save_df_to_s3(df, file_path):
    """Saves a pandas DataFrame to a CSV file in the DigitalOcean Space."""
    if not s3_client: return
    try:
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)
        s3_client.put_object(Bucket=SPACES_BUCKET_NAME, Key=file_path, Body=csv_buffer.getvalue())
        print(f"Successfully saved DataFrame to s3://{SPACES_BUCKET_NAME}/{file_path}")
    except Exception as e:
        print(f"Error saving DataFrame to {file_path}: {e}")

def read_df_from_s3(file_path):
    """Reads a CSV file from the DigitalOcean Space into a pandas DataFrame."""
    if not s3_client: return pd.DataFrame()
    try:
        response = s3_client.get_object(Bucket=SPACES_BUCKET_NAME, Key=file_path)
        csv_content = response['Body'].read().decode('utf-8')
        df = pd.read_csv(StringIO(csv_content))
        print(f"Successfully read DataFrame from s3://{SPACES_BUCKET_NAME}/{file_path}")
        return df
    except s3_client.exceptions.NoSuchKey:
        print(f"File not found at {file_path}. Returning empty DataFrame.")
        return pd.DataFrame()
    except Exception as e:
        print(f"Error reading DataFrame from {file_path}: {e}")
        return pd.DataFrame()

def read_tickerlist_from_s3(file_path='tickerlist.txt'):
    """Reads a simple text file (like the tickerlist) from the Space."""
    if not s3_client: return []
    try:
        response = s3_client.get_object(Bucket=SPACES_BUCKET_NAME, Key=file_path)
        content = response['Body'].read().decode('utf-8')
        tickers = [line.strip() for line in content.split('\n') if line.strip()]
        print(f"Successfully read tickerlist from s3://{SPACES_BUCKET_NAME}/{file_path}")
        return tickers
    except s3_client.exceptions.NoSuchKey:
        print(f"Tickerlist not found at {file_path}. Returning empty list.")
        return []
    except Exception as e:
        print(f"Error reading tickerlist from {file_path}: {e}")
        return []

def upload_initial_data_to_s3():
    """One-time function to upload essential data files from the repo to the Space."""
    if not s3_client:
        print("Cannot upload initial data: S3 client is not configured.")
        return
    print("Attempting to upload initial data to Space...")
    initial_files = {
        'data/universe/sp500.csv': 'data/universe/sp500.csv',
    }
    for local_path, s3_key in initial_files.items():
        try:
            s3_client.head_object(Bucket=SPACES_BUCKET_NAME, Key=s3_key)
            print(f"File {s3_key} already exists in Space. Skipping upload.")
        except s3_client.exceptions.ClientError as e:
            if e.response['Error']['Code'] == '404':
                print(f"File {s3_key} not found in Space. Uploading from repo...")
                try:
                    full_local_path = os.path.join('/workspace', local_path)
                    with open(full_local_path, "rb") as f:
                        s3_client.put_object(Bucket=SPACES_BUCKET_NAME, Key=s3_key, Body=f)
                        print(f"Successfully uploaded {local_path} to {s3_key}")
                except FileNotFoundError:
                    print(f"CRITICAL: Initial file not found in repo: {full_local_path}. Cannot seed.")
                except Exception as upload_error:
                    print(f"Error uploading {local_path}: {upload_error}")

# --- General Calculation Helpers ---
def format_to_two_decimal(value):
    if isinstance(value, (int, float)) and not np.isnan(value):
        return f"{value:.2f}"
    return "N/A"

def calculate_vwap(df):
    q = df['volume'] * (df['high'] + df['low'] + df['close']) / 3
    return q.cumsum() / df['volume'].cumsum()

def detect_market_session():
    ny_timezone = pytz.timezone('America/New_York')
    ny_time = datetime.now(ny_timezone).time()
    if time(4, 0) <= ny_time < time(9, 30): return 'PRE-MARKET'
    elif time(9, 30) <= ny_time < time(16, 0): return 'REGULAR'
    else: return 'CLOSED'

def get_previous_day_close(daily_df):
    if len(daily_df) > 1: return daily_df['close'].iloc[-2]
    return None

def get_premarket_data(today_intraday_df):
    return today_intraday_df.between_time(time(4, 0), time(9, 29))

def calculate_avg_early_volume(intraday_df, days=5):
    early_volume_df = intraday_df.between_time(time(9, 30), time(9, 44))
    if early_volume_df.empty: return 0
    daily_early_volume = early_volume_df.groupby(early_volume_df.index.date)['volume'].sum()
    if len(daily_early_volume) < days: return daily_early_volume.mean()
    return daily_early_volume.tail(days).mean()

def calculate_avg_daily_volume(daily_df, days=20):
    if len(daily_df) < days + 1: return None
    return daily_df['volume'].iloc[-days-1:-1].mean()
