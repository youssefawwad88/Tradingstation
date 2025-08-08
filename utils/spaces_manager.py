import boto3
import pandas as pd
import io
import logging
import os
from botocore.exceptions import ClientError
from utils.config import (
    SPACES_ACCESS_KEY_ID, 
    SPACES_SECRET_ACCESS_KEY, 
    SPACES_BUCKET_NAME,
    SPACES_ENDPOINT_URL,
    DEBUG_MODE
)

logger = logging.getLogger(__name__)

def get_spaces_client():
    """
    Create and return a boto3 client for DigitalOcean Spaces.
    """
    if not all([SPACES_ACCESS_KEY_ID, SPACES_SECRET_ACCESS_KEY, SPACES_BUCKET_NAME]):
        logger.error("Missing required Spaces credentials or bucket name")
        if DEBUG_MODE:
            print(f"🔑 Missing Spaces credentials: "
                  f"Key ID: {'✅ Set' if SPACES_ACCESS_KEY_ID else '❌ Missing'}, "
                  f"Secret: {'✅ Set' if SPACES_SECRET_ACCESS_KEY else '❌ Missing'}, "
                  f"Bucket: {SPACES_BUCKET_NAME or '❌ Missing'}")
        return None
        
    try:
        session = boto3.session.Session()
        client = session.client('s3',
            region_name='nyc3',
            endpoint_url=SPACES_ENDPOINT_URL,
            aws_access_key_id=SPACES_ACCESS_KEY_ID,
            aws_secret_access_key=SPACES_SECRET_ACCESS_KEY
        )
        return client
    except Exception as e:
        logger.error(f"Failed to create Spaces client: {e}")
        if DEBUG_MODE:
            print(f"❌ Failed to create Spaces client: {e}")
        return None

def upload_dataframe(df, object_name, file_format='csv'):
    """
    Upload a pandas DataFrame directly to DigitalOcean Spaces.
    
    Args:
        df (pandas.DataFrame): DataFrame to upload
        object_name (str): Object name in the Spaces bucket
        file_format (str): Format to save the DataFrame ('csv' or 'parquet')
        
    Returns:
        bool: True if successful, False otherwise
    """
    client = get_spaces_client()
    if not client:
        logger.warning(f"Cannot upload to Spaces - no client available")
        return False
    
    try:
        buffer = io.BytesIO()
        
        if file_format.lower() == 'csv':
            df.to_csv(buffer, index=False)
        elif file_format.lower() == 'parquet':
            df.to_parquet(buffer, index=False)
        else:
            logger.error(f"Unsupported file format: {file_format}")
            return False
            
        buffer.seek(0)
        client.upload_fileobj(buffer, SPACES_BUCKET_NAME, object_name)
        logger.info(f"Uploaded DataFrame to {SPACES_BUCKET_NAME}/{object_name}")
        if DEBUG_MODE:
            print(f"☁️ Successfully uploaded to Spaces: {SPACES_BUCKET_NAME}/{object_name}")
        return True
    except Exception as e:
        logger.error(f"Error uploading DataFrame to Spaces: {e}")
        if DEBUG_MODE:
            print(f"❌ Failed to upload to Spaces: {e}")
        return False

def spaces_manager():
    """
    Initialize and return a spaces manager client for backwards compatibility.
    This function is called by the dashboard and other components.
    
    Returns:
        boto3 S3 client configured for DigitalOcean Spaces, or None if configuration fails
    """
    return get_spaces_client()

class SpacesManager:
    """
    Spaces manager class to provide file listing and management functionality.
    """
    
    def __init__(self):
        self.client = get_spaces_client()
    
    def list_objects(self, prefix=''):
        """
        List objects in the Spaces bucket with the given prefix.
        
        Args:
            prefix (str): Prefix to filter objects
            
        Returns:
            list: List of object names, or empty list if error
        """
        if not self.client:
            logger.error("No Spaces client available for listing objects")
            return []
            
        try:
            from utils.config import SPACES_BUCKET_NAME
            if not SPACES_BUCKET_NAME:
                logger.error("No bucket name configured")
                return []
                
            response = self.client.list_objects_v2(
                Bucket=SPACES_BUCKET_NAME,
                Prefix=prefix
            )
            
            if 'Contents' in response:
                # Return just the filenames without the prefix path
                return [obj['Key'].replace(prefix, '').lstrip('/') 
                       for obj in response['Contents'] 
                       if not obj['Key'].endswith('/')]
            else:
                return []
                
        except Exception as e:
            logger.error(f"Error listing objects with prefix '{prefix}': {e}")
            return []

# Create a global instance for compatibility
spaces_manager = SpacesManager()
