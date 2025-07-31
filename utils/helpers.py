import pandas as pd
from datetime import datetime
import os
import boto3
from botocore.exceptions import NoCredentialsError, ClientError
import io

# Assume your existing get_boto_client() and other helpers are here...

def update_scheduler_status(job_name, status, details=""):
    """
    Updates or creates a scheduler status log in DigitalOcean Spaces.

    This function is designed to be robust:
    1. It tries to read an existing log file.
    2. If the file exists, it updates the row for the specific job_name.
    3. If the row doesn't exist, it adds a new one.
    4. If the file doesn't exist, it creates a new DataFrame and the file.
    5. It saves the updated DataFrame back to Spaces.

    Args:
        job_name (str): The name of the job being logged (e.g., 'update_daily_data').
        status (str): The status of the job ('Success', 'Fail', 'Running').
        details (str, optional): Any error messages or extra info. Defaults to "".

    Returns:
        bool: True if the log was saved successfully, False otherwise.
    """
    bucket_name = os.getenv('SPACES_BUCKET_NAME')
    log_file_key = 'data/logs/scheduler_status.csv'
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    try:
        s3_client = get_boto_client() # Assuming you have this helper from before
        
        try:
            # Try to get the existing log file
            response = s3_client.get_object(Bucket=bucket_name, Key=log_file_key)
            status_df = pd.read_csv(io.BytesIO(response['Body'].read()))
        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchKey':
                # File doesn't exist, create an empty DataFrame
                status_df = pd.DataFrame(columns=['job_name', 'last_run_timestamp', 'status', 'details'])
            else:
                # Another error occurred
                print(f"Error reading status log from S3: {e}")
                return False

        # Check if the job is already in the log
        if job_name in status_df['job_name'].values:
            # Update existing job entry
            job_index = status_df[status_df['job_name'] == job_name].index[0]
            status_df.loc[job_index, 'last_run_timestamp'] = timestamp
            status_df.loc[job_index, 'status'] = status
            status_df.loc[job_index, 'details'] = details
        else:
            # Add new job entry
            new_row = pd.DataFrame([{
                'job_name': job_name,
                'last_run_timestamp': timestamp,
                'status': status,
                'details': details
            }])
            status_df = pd.concat([status_df, new_row], ignore_index=True)

        # Save the updated DataFrame back to S3
        csv_buffer = io.StringIO()
        status_df.to_csv(csv_buffer, index=False)
        s3_client.put_object(Bucket=bucket_name, Key=log_file_key, Body=csv_buffer.getvalue())
        
        print(f"Successfully logged status for '{job_name}': {status}")
        return True

    except (NoCredentialsError, ClientError, Exception) as e:
        print(f"CRITICAL ERROR: Failed to update scheduler status log in S3. Reason: {e}")
        return False

