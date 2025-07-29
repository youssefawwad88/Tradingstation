import os
import pandas as pd
import boto3
from io import StringIO, BytesIO

# --- Cloud Storage (S3) Configuration ---
# These variables are read from the DigitalOcean App Platform's environment variables
SPACES_ACCESS_KEY_ID = os.getenv('SPACES_ACCESS_KEY_ID')
SPACES_SECRET_ACCESS_KEY = os.getenv('SPACES_SECRET_ACCESS_KEY')
SPACES_BUCKET_NAME = os.getenv('SPACES_BUCKET_NAME')
SPACES_REGION = os.getenv('SPACES_REGION')

# Initialize the S3 client
s3_client = boto3.client(
    's3',
    region_name=SPACES_REGION,
    endpoint_url=f'https://{SPACES_REGION}.digitaloceanspaces.com',
    aws_access_key_id=SPACES_ACCESS_KEY_ID,
    aws_secret_access_key=SPACES_SECRET_ACCESS_KEY
)

def get_s3_path(file_path):
    """
    Constructs the full S3 path from a relative file path.
    Removes the initial 'data/' if present, as it will be part of the object key.
    Example: 'data/signals/gapgo.csv' becomes 'signals/gapgo.csv'
    """
    if file_path.startswith('data/'):
        return file_path[5:]
    return file_path

def save_df_to_s3(df, file_path):
    """
    Saves a pandas DataFrame to a CSV file in the DigitalOcean Space.
    """
    s3_path = get_s3_path(file_path)
    try:
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)
        s3_client.put_object(
            Bucket=SPACES_BUCKET_NAME,
            Key=s3_path,
            Body=csv_buffer.getvalue(),
            ACL='private' # Access Control List: 'private' or 'public-read'
        )
        print(f"Successfully saved DataFrame to s3://{SPACES_BUCKET_NAME}/{s3_path}")
    except Exception as e:
        print(f"Error saving DataFrame to {s3_path}: {e}")

def read_df_from_s3(file_path):
    """
    Reads a CSV file from the DigitalOcean Space into a pandas DataFrame.
    Returns an empty DataFrame if the file does not exist.
    """
    s3_path = get_s3_path(file_path)
    try:
        response = s3_client.get_object(Bucket=SPACES_BUCKET_NAME, Key=s3_path)
        csv_content = response['Body'].read().decode('utf-8')
        df = pd.read_csv(StringIO(csv_content))
        print(f"Successfully read DataFrame from s3://{SPACES_BUCKET_NAME}/{s3_path}")
        return df
    except s3_client.exceptions.NoSuchKey:
        print(f"File not found at {s3_path}. Returning empty DataFrame.")
        return pd.DataFrame()
    except Exception as e:
        print(f"Error reading DataFrame from {s3_path}: {e}")
        return pd.DataFrame()

def read_tickerlist_from_s3(file_path='tickerlist.txt'):
    """
    Reads a simple text file (like the tickerlist) from the Space.
    """
    s3_path = get_s3_path(file_path)
    try:
        response = s3_client.get_object(Bucket=SPACES_BUCKET_NAME, Key=s3_path)
        content = response['Body'].read().decode('utf-8')
        tickers = [line.strip() for line in content.split('\n') if line.strip()]
        print(f"Successfully read tickerlist from s3://{SPACES_BUCKET_NAME}/{s3_path}")
        return tickers
    except s3_client.exceptions.NoSuchKey:
        print(f"Tickerlist not found at {s3_path}. Returning empty list.")
        return []
    except Exception as e:
        print(f"Error reading tickerlist from {s3_path}: {e}")
        return []

def upload_initial_data_to_s3():
    """
    One-time function to upload all local data files from the 'data/' directory
    in the GitHub repo to the DigitalOcean Space. This seeds the system.
    """
    print("Starting initial data upload to Space...")
    # We need to find all files in the 'data' directory from our repo structure
    # This is a simplified example. In a real scenario, you might list files from a local clone.
    # For now, we assume the files are accessible relative to this script's execution context
    # during the build process on DigitalOcean.
    
    # This function is more conceptual for now. The primary way to get data into the space
    # will be for the `update_all_data.py` job to run and save its output directly to S3.
    # However, we need to upload the initial universe, like sp500.csv.

    # Let's manually specify the essential files from the repo to upload.
    initial_files = [
        'data/universe/sp500.csv',
        'tickerlist.txt'
        # Add other essential bootstrap files here if necessary
    ]

    for local_path in initial_files:
        s3_path = get_s3_path(local_path)
        try:
            with open(local_path, "rb") as f:
                s3_client.put_object(
                    Bucket=SPACES_BUCKET_NAME,
                    Key=s3_path,
                    Body=f,
                    ACL='private'
                )
                print(f"Successfully uploaded {local_path} to {s3_path}")
        except FileNotFoundError:
            print(f"Initial file not found: {local_path}. Skipping.")
        except Exception as e:
            print(f"Error uploading {local_path}: {e}")

# --- You can keep other helper functions below this line if you have them ---
