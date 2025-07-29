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
            ACL='private'
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

def save_list_to_s3(data_list, file_path):
    """
    Saves a Python list of strings to a text file in the DigitalOcean Space,
    with each item on a new line.
    """
    s3_path = get_s3_path(file_path)
    try:
        # Join the list into a single string with newline characters
        content = "\n".join(data_list)
        s3_client.put_object(
            Bucket=SPACES_BUCKET_NAME,
            Key=s3_path,
            Body=content,
            ACL='private'
        )
        print(f"Successfully saved list to s3://{SPACES_BUCKET_NAME}/{s3_path}")
    except Exception as e:
        print(f"Error saving list to {s3_path}: {e}")

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
    One-time function to upload essential data files from the repo to the Space.
    This should only be called if the data does not already exist in the Space.
    """
    print("Attempting to upload initial data to Space...")
    
    initial_files = {
        'data/universe/sp500.csv': 'universe/sp500.csv',
        'tickerlist.txt': 'tickerlist.txt' # A default empty tickerlist
    }

    for local_path, s3_key in initial_files.items():
        try:
            # Check if the file already exists in S3
            s3_client.head_object(Bucket=SPACES_BUCKET_NAME, Key=s3_key)
            print(f"File {s3_key} already exists in Space. Skipping upload.")
        except s3_client.exceptions.ClientError as e:
            if e.response['Error']['Code'] == '404': # Not found, so we upload
                print(f"File {s3_key} not found in Space. Uploading from repo...")
                try:
                    with open(local_path, "rb") as f:
                        s3_client.put_object(
                            Bucket=SPACES_BUCKET_NAME,
                            Key=s3_key,
                            Body=f,
                            ACL='private'
                        )
                        print(f"Successfully uploaded {local_path} to {s3_key}")
                except FileNotFoundError:
                    print(f"Initial file not found in repo: {local_path}. Cannot seed.")
                except Exception as upload_error:
                    print(f"Error uploading {local_path}: {upload_error}")
            else:
                print(f"Error checking for {s3_key}: {e}")

