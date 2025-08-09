"""
Storage abstraction layer for Trading Station.
Provides unified interface for local filesystem and cloud storage (S3/Spaces).
"""

import os
import boto3
import pandas as pd
from pathlib import Path
from typing import Optional, List, Dict, Any
from io import StringIO
import logging
from botocore.exceptions import ClientError, NoCredentialsError

from .config import (
    SPACES_ACCESS_KEY_ID, SPACES_SECRET_ACCESS_KEY, 
    SPACES_BUCKET_NAME, SPACES_REGION, SPACES_BASE_PREFIX,
    TEST_MODE
)
from .logging_setup import get_logger

logger = get_logger(__name__)

class StorageError(Exception):
    """Base exception for storage operations."""
    pass

class StorageInterface:
    """Abstract interface for storage operations."""
    
    def save_df(self, df: pd.DataFrame, path: str) -> bool:
        """Save DataFrame to storage."""
        raise NotImplementedError
    
    def read_df(self, path: str) -> Optional[pd.DataFrame]:
        """Read DataFrame from storage."""
        raise NotImplementedError
    
    def exists(self, path: str) -> bool:
        """Check if path exists in storage."""
        raise NotImplementedError
    
    def list_files(self, prefix: str) -> List[str]:
        """List files with given prefix."""
        raise NotImplementedError
    
    def delete(self, path: str) -> bool:
        """Delete file from storage."""
        raise NotImplementedError
    
    def copy(self, src_path: str, dst_path: str) -> bool:
        """Copy file within storage."""
        raise NotImplementedError

class LocalStorage(StorageInterface):
    """Local filesystem storage implementation."""
    
    def __init__(self, base_path: str = "."):
        self.base_path = Path(base_path)
        self.base_path.mkdir(parents=True, exist_ok=True)
    
    def _resolve_path(self, path: str) -> Path:
        """Resolve relative path to absolute path."""
        return self.base_path / path
    
    def save_df(self, df: pd.DataFrame, path: str) -> bool:
        """Save DataFrame to local CSV file."""
        try:
            full_path = self._resolve_path(path)
            full_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Save to CSV
            df.to_csv(full_path, index=False)
            
            # Verify by reading back
            verification_df = pd.read_csv(full_path)
            if len(verification_df) == 0 and len(df) > 0:
                raise StorageError(f"Verification failed: saved {len(df)} rows but read back 0")
            
            logger.debug(f"Saved and verified {path}: {len(df)} rows")
            return True
            
        except Exception as e:
            logger.error(f"Failed to save DataFrame to {path}: {e}")
            raise StorageError(f"Save failed: {e}")
    
    def read_df(self, path: str) -> Optional[pd.DataFrame]:
        """Read DataFrame from local CSV file."""
        try:
            full_path = self._resolve_path(path)
            if not full_path.exists():
                return None
            
            df = pd.read_csv(full_path)
            logger.debug(f"Read {path}: {len(df)} rows")
            return df
            
        except Exception as e:
            logger.error(f"Failed to read DataFrame from {path}: {e}")
            return None
    
    def exists(self, path: str) -> bool:
        """Check if file exists locally."""
        return self._resolve_path(path).exists()
    
    def list_files(self, prefix: str) -> List[str]:
        """List files with given prefix."""
        try:
            prefix_path = self._resolve_path(prefix)
            if prefix_path.is_file():
                return [str(prefix_path.relative_to(self.base_path))]
            
            files = []
            if prefix_path.exists():
                for file_path in prefix_path.rglob("*"):
                    if file_path.is_file():
                        files.append(str(file_path.relative_to(self.base_path)))
            
            return sorted(files)
            
        except Exception as e:
            logger.error(f"Failed to list files with prefix {prefix}: {e}")
            return []
    
    def delete(self, path: str) -> bool:
        """Delete file from local storage."""
        try:
            full_path = self._resolve_path(path)
            if full_path.exists():
                full_path.unlink()
                return True
            return False
            
        except Exception as e:
            logger.error(f"Failed to delete {path}: {e}")
            return False
    
    def copy(self, src_path: str, dst_path: str) -> bool:
        """Copy file within local storage."""
        try:
            src_full = self._resolve_path(src_path)
            dst_full = self._resolve_path(dst_path)
            
            dst_full.parent.mkdir(parents=True, exist_ok=True)
            
            import shutil
            shutil.copy2(src_full, dst_full)
            return True
            
        except Exception as e:
            logger.error(f"Failed to copy {src_path} to {dst_path}: {e}")
            return False

class CloudStorage(StorageInterface):
    """Cloud storage implementation for S3/DigitalOcean Spaces."""
    
    def __init__(self):
        if not all([SPACES_ACCESS_KEY_ID, SPACES_SECRET_ACCESS_KEY, SPACES_BUCKET_NAME]):
            raise StorageError("Cloud storage credentials not configured")
        
        try:
            self.s3_client = boto3.client(
                's3',
                region_name=SPACES_REGION,
                endpoint_url=f'https://{SPACES_REGION}.digitaloceanspaces.com',
                aws_access_key_id=SPACES_ACCESS_KEY_ID,
                aws_secret_access_key=SPACES_SECRET_ACCESS_KEY
            )
            self.bucket = SPACES_BUCKET_NAME
            self.prefix = SPACES_BASE_PREFIX
            
            # Test connection
            self.s3_client.head_bucket(Bucket=self.bucket)
            logger.info(f"Connected to cloud storage: {self.bucket}")
            
        except (ClientError, NoCredentialsError) as e:
            logger.error(f"Failed to initialize cloud storage: {e}")
            raise StorageError(f"Cloud storage initialization failed: {e}")
    
    def _get_key(self, path: str) -> str:
        """Get full S3 key for a path."""
        # Remove leading slash and combine with prefix
        path = path.lstrip('/')
        if self.prefix:
            return f"{self.prefix}/{path}"
        return path
    
    def save_df(self, df: pd.DataFrame, path: str) -> bool:
        """Save DataFrame to cloud storage as CSV."""
        try:
            key = self._get_key(path)
            
            # Convert DataFrame to CSV string
            csv_buffer = StringIO()
            df.to_csv(csv_buffer, index=False)
            csv_content = csv_buffer.getvalue()
            
            # Upload to S3
            self.s3_client.put_object(
                Bucket=self.bucket,
                Key=key,
                Body=csv_content,
                ContentType='text/csv'
            )
            
            # Verify by reading back
            verification_df = self.read_df(path)
            if verification_df is None or (len(verification_df) == 0 and len(df) > 0):
                raise StorageError(f"Verification failed for {path}")
            
            logger.debug(f"Saved and verified {path}: {len(df)} rows")
            return True
            
        except Exception as e:
            logger.error(f"Failed to save DataFrame to cloud {path}: {e}")
            raise StorageError(f"Cloud save failed: {e}")
    
    def read_df(self, path: str) -> Optional[pd.DataFrame]:
        """Read DataFrame from cloud storage."""
        try:
            key = self._get_key(path)
            
            response = self.s3_client.get_object(Bucket=self.bucket, Key=key)
            csv_content = response['Body'].read().decode('utf-8')
            
            df = pd.read_csv(StringIO(csv_content))
            logger.debug(f"Read {path}: {len(df)} rows")
            return df
            
        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchKey':
                return None
            logger.error(f"Failed to read DataFrame from cloud {path}: {e}")
            return None
        except Exception as e:
            logger.error(f"Failed to read DataFrame from cloud {path}: {e}")
            return None
    
    def exists(self, path: str) -> bool:
        """Check if file exists in cloud storage."""
        try:
            key = self._get_key(path)
            self.s3_client.head_object(Bucket=self.bucket, Key=key)
            return True
        except ClientError:
            return False
    
    def list_files(self, prefix: str) -> List[str]:
        """List files with given prefix in cloud storage."""
        try:
            key_prefix = self._get_key(prefix)
            
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket,
                Prefix=key_prefix
            )
            
            files = []
            if 'Contents' in response:
                for obj in response['Contents']:
                    # Remove the base prefix to get relative path
                    relative_key = obj['Key']
                    if self.prefix and relative_key.startswith(f"{self.prefix}/"):
                        relative_key = relative_key[len(f"{self.prefix}/"):]
                    files.append(relative_key)
            
            return sorted(files)
            
        except Exception as e:
            logger.error(f"Failed to list files with prefix {prefix}: {e}")
            return []
    
    def delete(self, path: str) -> bool:
        """Delete file from cloud storage."""
        try:
            key = self._get_key(path)
            self.s3_client.delete_object(Bucket=self.bucket, Key=key)
            return True
            
        except Exception as e:
            logger.error(f"Failed to delete {path} from cloud: {e}")
            return False
    
    def copy(self, src_path: str, dst_path: str) -> bool:
        """Copy file within cloud storage."""
        try:
            src_key = self._get_key(src_path)
            dst_key = self._get_key(dst_path)
            
            copy_source = {'Bucket': self.bucket, 'Key': src_key}
            self.s3_client.copy_object(
                CopySource=copy_source,
                Bucket=self.bucket,
                Key=dst_key
            )
            return True
            
        except Exception as e:
            logger.error(f"Failed to copy {src_path} to {dst_path} in cloud: {e}")
            return False

class Storage:
    """Unified storage interface that handles both local and cloud storage."""
    
    def __init__(self, use_cloud: bool = None):
        """
        Initialize storage interface.
        
        Args:
            use_cloud: Force cloud storage usage. If None, auto-detect based on config.
        """
        self.local_storage = LocalStorage()
        self.cloud_storage = None
        
        # Determine if we should use cloud storage
        if use_cloud is None:
            use_cloud = bool(SPACES_ACCESS_KEY_ID and SPACES_SECRET_ACCESS_KEY and 
                           SPACES_BUCKET_NAME and not TEST_MODE)
        
        if use_cloud:
            try:
                self.cloud_storage = CloudStorage()
                logger.info("Using cloud storage as primary")
            except StorageError as e:
                logger.warning(f"Cloud storage unavailable, falling back to local: {e}")
                self.cloud_storage = None
        
        self.primary_storage = self.cloud_storage if self.cloud_storage else self.local_storage
        logger.info(f"Primary storage: {'cloud' if self.cloud_storage else 'local'}")
    
    def save_df(self, df: pd.DataFrame, path: str) -> bool:
        """Save DataFrame using primary storage."""
        return self.primary_storage.save_df(df, path)
    
    def read_df(self, path: str) -> Optional[pd.DataFrame]:
        """Read DataFrame using primary storage."""
        return self.primary_storage.read_df(path)
    
    def exists(self, path: str) -> bool:
        """Check if path exists in primary storage."""
        return self.primary_storage.exists(path)
    
    def list_files(self, prefix: str) -> List[str]:
        """List files with given prefix in primary storage."""
        return self.primary_storage.list_files(prefix)
    
    def delete(self, path: str) -> bool:
        """Delete file from primary storage."""
        return self.primary_storage.delete(path)
    
    def copy(self, src_path: str, dst_path: str) -> bool:
        """Copy file within primary storage."""
        return self.primary_storage.copy(src_path, dst_path)
    
    def sync_to_cloud(self, path: str) -> bool:
        """Sync local file to cloud storage."""
        if not self.cloud_storage:
            return False
        
        try:
            df = self.local_storage.read_df(path)
            if df is not None:
                return self.cloud_storage.save_df(df, path)
            return False
        except Exception as e:
            logger.error(f"Failed to sync {path} to cloud: {e}")
            return False
    
    def sync_from_cloud(self, path: str) -> bool:
        """Sync cloud file to local storage."""
        if not self.cloud_storage:
            return False
        
        try:
            df = self.cloud_storage.read_df(path)
            if df is not None:
                return self.local_storage.save_df(df, path)
            return False
        except Exception as e:
            logger.error(f"Failed to sync {path} from cloud: {e}")
            return False

# Global storage instance
_storage_instance: Optional[Storage] = None

def get_storage() -> Storage:
    """Get the global storage instance."""
    global _storage_instance
    if _storage_instance is None:
        _storage_instance = Storage()
    return _storage_instance

# Convenience functions
def save_df(df: pd.DataFrame, path: str) -> bool:
    """Save DataFrame to storage."""
    return get_storage().save_df(df, path)

def read_df(path: str) -> Optional[pd.DataFrame]:
    """Read DataFrame from storage."""
    return get_storage().read_df(path)

def exists(path: str) -> bool:
    """Check if path exists in storage."""
    return get_storage().exists(path)

def list_files(prefix: str) -> List[str]:
    """List files with given prefix."""
    return get_storage().list_files(prefix)

# Export classes and functions
__all__ = [
    'Storage', 'LocalStorage', 'CloudStorage', 'StorageError',
    'get_storage', 'save_df', 'read_df', 'exists', 'list_files'
]