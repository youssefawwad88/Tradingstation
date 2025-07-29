import os
import pandas as pd
import boto3
from io import StringIO
from datetime import time
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
    print("WARNING: S3 client not initialized. Missing one or more environment variables.")

# --- S3 Helper Functions ---

def list_files_in_s3_dir(prefix):
    """Lists all files in a given 'directory' in the S3 bucket."""
    if not s3_client: return []
    try:
        paginator = s3_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=SPACES_BUCKET_NAME, Prefix=prefix)
        file_list = []
        for page in pages:
            if "Contents" in page:
                for obj in page['Contents']:
                    if not obj['Key'].endswith('/'): # Exclude directories
                        file_list.append(os.path.basename(obj['Key']))
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

# --- General Calculation Helpers ---

def format_to_two_decimal(value):
    """Formats a number to two decimal places, handling non-numeric inputs."""
    if isinstance(value, (int, float)) and not np.isnan(value):
        return f"{value:.2f}"
    return "N/A"

def calculate_vwap(df):
    """Calculates the Volume Weighted Average Price (VWAP)."""
    q = df['volume'] * (df['high'] + df['low'] + df['close']) / 3
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
    """Gets the previous day's close from a daily dataframe."""
    if len(daily_df) > 1:
        return daily_df['close'].iloc[-2]
    return None

def get_premarket_data(today_intraday_df):
    """Extracts pre-market data from today's intraday dataframe."""
    return today_intraday_df.between_time(time(4, 0), time(9, 29))

def calculate_avg_early_volume(intraday_df, days=5):
    """Calculates the average volume for the first 15 minutes of the regular session."""
    early_volume_df = intraday_df.between_time(time(9, 30), time(9, 44))
    if early_volume_df.empty:
        return 0
    daily_early_volume = early_volume_df.groupby(early_volume_df.index.date)['volume'].sum()
    if len(daily_early_volume) < days:
        return daily_early_volume.mean()
    return daily_early_volume.tail(days).mean()

def calculate_avg_daily_volume(daily_df, days=20):
    """Calculates the average daily volume over a specified number of days."""
    if len(daily_df) < days:
        return None
    return daily_df['volume'].iloc[-days-1:-1].mean()
