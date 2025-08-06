"""
DigitalOcean Spaces Manager
Provides upload, download, and list functions for DigitalOcean Spaces using boto3.
"""
import os
import boto3
from botocore.exceptions import NoCredentialsError, ClientError
import pandas as pd
from io import StringIO
import json
from typing import List, Optional, Union

from .config import DO_SPACES_CONFIG


class SpacesManager:
    """Manager class for DigitalOcean Spaces operations."""
    
    def __init__(self):
        """Initialize the Spaces client."""
        self.client = self._get_client()
        self.bucket_name = DO_SPACES_CONFIG['bucket_name']
    
    def _get_client(self):
        """Create and return a boto3 client for DigitalOcean Spaces."""
        try:
            if not all([
                DO_SPACES_CONFIG['access_key_id'],
                DO_SPACES_CONFIG['secret_access_key'],
                DO_SPACES_CONFIG['bucket_name'],
                DO_SPACES_CONFIG['region']
            ]):
                print("WARNING: DigitalOcean Spaces credentials are not fully configured.")
                return None
            
            return boto3.client(
                's3',
                region_name=DO_SPACES_CONFIG['region'],
                endpoint_url=DO_SPACES_CONFIG['endpoint_url'],
                aws_access_key_id=DO_SPACES_CONFIG['access_key_id'],
                aws_secret_access_key=DO_SPACES_CONFIG['secret_access_key']
            )
        except Exception as e:
            print(f"Error creating DigitalOcean Spaces client: {e}")
            return None
    
    def upload_file(self, local_file_path: str, remote_key: str) -> bool:
        """
        Upload a file to DigitalOcean Spaces.
        
        Args:
            local_file_path: Path to the local file
            remote_key: The key (path) in the bucket where the file will be stored
            
        Returns:
            bool: True if successful, False otherwise
        """
        if not self.client:
            return False
        
        try:
            self.client.upload_file(local_file_path, self.bucket_name, remote_key)
            print(f"Successfully uploaded {local_file_path} to {remote_key}")
            return True
        except FileNotFoundError:
            print(f"Local file not found: {local_file_path}")
            return False
        except ClientError as e:
            print(f"Error uploading file {local_file_path}: {e}")
            return False
    
    def upload_dataframe(self, df: pd.DataFrame, remote_key: str) -> bool:
        """
        Upload a pandas DataFrame as CSV to DigitalOcean Spaces.
        
        Args:
            df: The pandas DataFrame to upload
            remote_key: The key (path) in the bucket where the CSV will be stored
            
        Returns:
            bool: True if successful, False otherwise
        """
        if not self.client:
            print(f"❌ ERROR: Cannot upload {remote_key} - DigitalOcean Spaces client not initialized!")
            print("   Check that SPACES_ACCESS_KEY_ID, SPACES_SECRET_ACCESS_KEY, and other credentials are set.")
            return False
        
        try:
            csv_buffer = StringIO()
            df.to_csv(csv_buffer, index=False)
            self.client.put_object(
                Bucket=self.bucket_name,
                Key=remote_key,
                Body=csv_buffer.getvalue()
            )
            print(f"Successfully uploaded DataFrame to {remote_key}")
            return True
        except Exception as e:
            print(f"❌ ERROR uploading DataFrame to {remote_key}: {e}")
            return False
    
    def upload_string(self, content: str, remote_key: str) -> bool:
        """
        Upload string content to DigitalOcean Spaces.
        
        Args:
            content: The string content to upload
            remote_key: The key (path) in the bucket where the content will be stored
            
        Returns:
            bool: True if successful, False otherwise
        """
        if not self.client:
            return False
        
        try:
            self.client.put_object(
                Bucket=self.bucket_name,
                Key=remote_key,
                Body=content
            )
            print(f"Successfully uploaded string content to {remote_key}")
            return True
        except Exception as e:
            print(f"Error uploading string content to {remote_key}: {e}")
            return False
    
    def upload_list(self, data_list: List[str], remote_key: str) -> bool:
        """
        Upload a list of strings as a newline-separated file to DigitalOcean Spaces.
        
        Args:
            data_list: List of strings to upload
            remote_key: The key (path) in the bucket where the file will be stored
            
        Returns:
            bool: True if successful, False otherwise
        """
        content = "\n".join(data_list)
        return self.upload_string(content, remote_key)
    
    def download_file(self, remote_key: str, local_file_path: str) -> bool:
        """
        Download a file from DigitalOcean Spaces.
        
        Args:
            remote_key: The key (path) of the file in the bucket
            local_file_path: Path where the file will be saved locally
            
        Returns:
            bool: True if successful, False otherwise
        """
        if not self.client:
            return False
        
        try:
            self.client.download_file(self.bucket_name, remote_key, local_file_path)
            print(f"Successfully downloaded {remote_key} to {local_file_path}")
            return True
        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchKey':
                print(f"File not found in Spaces: {remote_key}")
            else:
                print(f"Error downloading file {remote_key}: {e}")
            return False
    
    def download_dataframe(self, remote_key: str) -> pd.DataFrame:
        """
        Download a CSV file from DigitalOcean Spaces as a pandas DataFrame.
        
        Args:
            remote_key: The key (path) of the CSV file in the bucket
            
        Returns:
            pd.DataFrame: The downloaded data, or empty DataFrame if failed
        """
        if not self.client:
            return pd.DataFrame()
        
        try:
            response = self.client.get_object(Bucket=self.bucket_name, Key=remote_key)
            return pd.read_csv(response['Body'])
        except ClientError as e:
            if e.response['Error']['Code'] != 'NoSuchKey':
                print(f"Error downloading DataFrame from {remote_key}: {e}")
            return pd.DataFrame()
        except Exception as e:
            print(f"Unexpected error downloading DataFrame from {remote_key}: {e}")
            return pd.DataFrame()
    
    def download_string(self, remote_key: str) -> Optional[str]:
        """
        Download string content from DigitalOcean Spaces.
        
        Args:
            remote_key: The key (path) of the file in the bucket
            
        Returns:
            str: The file content, or None if failed
        """
        if not self.client:
            return None
        
        try:
            response = self.client.get_object(Bucket=self.bucket_name, Key=remote_key)
            return response['Body'].read().decode('utf-8')
        except ClientError as e:
            if e.response['Error']['Code'] != 'NoSuchKey':
                print(f"Error downloading string content from {remote_key}: {e}")
            return None
        except Exception as e:
            print(f"Unexpected error downloading string content from {remote_key}: {e}")
            return None
    
    def download_list(self, remote_key: str) -> List[str]:
        """
        Download a newline-separated file from DigitalOcean Spaces as a list of strings.
        
        Args:
            remote_key: The key (path) of the file in the bucket
            
        Returns:
            List[str]: List of strings, or empty list if failed
        """
        content = self.download_string(remote_key)
        if content is None:
            return []
        
        return [line.strip() for line in content.split('\n') if line.strip()]
    
    def list_objects(self, prefix: str = "") -> List[str]:
        """
        List objects in the DigitalOcean Spaces bucket.
        
        Args:
            prefix: Optional prefix to filter objects
            
        Returns:
            List[str]: List of object keys, or empty list if failed
        """
        if not self.client:
            return []
        
        try:
            response = self.client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=prefix
            )
            
            if 'Contents' in response:
                return [obj['Key'] for obj in response['Contents']]
            else:
                return []
        except ClientError as e:
            print(f"Error listing objects with prefix '{prefix}': {e}")
            return []
    
    def delete_object(self, remote_key: str) -> bool:
        """
        Delete an object from DigitalOcean Spaces.
        
        Args:
            remote_key: The key (path) of the object to delete
            
        Returns:
            bool: True if successful, False otherwise
        """
        if not self.client:
            return False
        
        try:
            self.client.delete_object(Bucket=self.bucket_name, Key=remote_key)
            print(f"Successfully deleted {remote_key}")
            return True
        except ClientError as e:
            print(f"Error deleting object {remote_key}: {e}")
            return False
    
    def object_exists(self, remote_key: str) -> bool:
        """
        Check if an object exists in DigitalOcean Spaces.
        
        Args:
            remote_key: The key (path) of the object to check
            
        Returns:
            bool: True if exists, False otherwise
        """
        if not self.client:
            return False
        
        try:
            self.client.head_object(Bucket=self.bucket_name, Key=remote_key)
            return True
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                return False
            else:
                print(f"Error checking if object exists {remote_key}: {e}")
                return False


# Global instance for easy importing
spaces_manager = SpacesManager()