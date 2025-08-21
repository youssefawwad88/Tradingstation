"""
DigitalOcean Spaces interface for cloud storage operations.

This module provides atomic file operations, metadata management, and
reliable upload/download functionality for the trading data lake.
"""

import io
import json
import logging
from typing import Any, Dict, List, Optional, Union

import boto3
import pandas as pd
from botocore.exceptions import ClientError

from utils.config import config

logger = logging.getLogger(__name__)


def _parse_timestamps_on_read(df: pd.DataFrame) -> pd.DataFrame:
    """
    Parse timestamps when reading CSV from Spaces to ensure UTC timezone.
    
    Args:
        df: DataFrame read from CSV
        
    Returns:
        DataFrame with properly parsed UTC timestamps
    """
    # Import here to avoid circular imports
    from utils.time_utils import as_utc
    
    df = df.copy()
    
    # Handle intraday timestamp columns
    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
        
    # Handle daily date columns  
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], utc=True, errors="coerce")
        
    return df


def _prepare_timestamps_for_write(df: pd.DataFrame) -> pd.DataFrame:
    """
    Prepare timestamps when writing CSV to Spaces to ensure UTC format.
    
    Args:
        df: DataFrame to write
        
    Returns:
        DataFrame with UTC timestamps
    """
    # Import here to avoid circular imports
    from utils.time_utils import as_utc
    
    df = df.copy()
    
    # Ensure timestamp column is UTC if it exists
    if "timestamp" in df.columns and not df["timestamp"].empty:
        try:
            df["timestamp"] = as_utc(df["timestamp"])
            # Optional: write as ISO8601 string with "Z" for guaranteed round-trip
            df["timestamp"] = df["timestamp"].dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        except Exception as e:
            logger.warning(f"Could not convert timestamp column to UTC: {e}")
            
    # Ensure date column is UTC if it exists
    if "date" in df.columns and not df["date"].empty:
        try:
            df["date"] = as_utc(df["date"])
            df["date"] = df["date"].dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        except Exception as e:
            logger.warning(f"Could not convert date column to UTC: {e}")
            
    return df


class SpacesIO:
    """DigitalOcean Spaces client with atomic operations and metadata support."""

    def __init__(self) -> None:
        """Initialize the Spaces client."""
        self._client: Optional[boto3.client] = None
        self._initialize_client()

    def _initialize_client(self) -> None:
        """Initialize the boto3 client for DigitalOcean Spaces."""
        try:
            if not all([
                config.SPACES_ACCESS_KEY_ID,
                config.SPACES_SECRET_ACCESS_KEY,
                config.SPACES_BUCKET_NAME,
            ]):
                logger.warning("Spaces credentials not configured - running in local mode")
                return

            self._client = boto3.client(
                "s3",
                endpoint_url=config.SPACES_ENDPOINT,
                region_name=config.SPACES_REGION,
                aws_access_key_id=config.SPACES_ACCESS_KEY_ID,
                aws_secret_access_key=config.SPACES_SECRET_ACCESS_KEY,
            )
            
            # Test connection
            self._client.head_bucket(Bucket=config.SPACES_BUCKET_NAME)
            logger.info("Successfully connected to DigitalOcean Spaces")
            
        except Exception as e:
            logger.error(f"Failed to initialize Spaces client: {e}")
            self._client = None

    @property
    def is_available(self) -> bool:
        """Check if Spaces client is available."""
        return self._client is not None

    def get_object(self, key: str) -> Optional[bytes]:
        """
        Download an object from Spaces.
        
        Args:
            key: Object key/path in Spaces
            
        Returns:
            Object content as bytes, or None if not found
        """
        if not self.is_available:
            logger.warning(f"Spaces not available - cannot get object {key}")
            return None

        try:
            response = self._client.get_object(
                Bucket=config.SPACES_BUCKET_NAME,
                Key=key
            )
            return response["Body"].read()
        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchKey":
                logger.debug(f"Object not found: {key}")
                return None
            logger.error(f"Error getting object {key}: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error getting object {key}: {e}")
            return None

    def put_object_atomic(
        self,
        key: str,
        data: Union[bytes, str],
        metadata: Optional[Dict[str, str]] = None,
    ) -> bool:
        """
        Upload an object to Spaces atomically using tmp->final pattern.
        
        Args:
            key: Final object key/path
            data: Data to upload
            metadata: Optional metadata to attach
            
        Returns:
            True if successful, False otherwise
        """
        if not self.is_available:
            logger.warning(f"Spaces not available - cannot put object {key}")
            return False

        # Convert string data to bytes
        if isinstance(data, str):
            data = data.encode("utf-8")

        tmp_key = f"{key}.tmp"
        
        # Prepare metadata
        final_metadata = {
            "managed-by": "data_fetch_manager",
            "commit": config.DEPLOYMENT_TAG or "unknown",
        }
        if metadata:
            final_metadata.update(metadata)

        try:
            # Step 1: Upload to temporary key
            self._client.put_object(
                Bucket=config.SPACES_BUCKET_NAME,
                Key=tmp_key,
                Body=data,
                Metadata=final_metadata,
            )

            # Step 2: Server-side copy to final key
            copy_source = {
                "Bucket": config.SPACES_BUCKET_NAME,
                "Key": tmp_key,
            }
            self._client.copy_object(
                Bucket=config.SPACES_BUCKET_NAME,
                Key=key,
                CopySource=copy_source,
                MetadataDirective="COPY",
            )

            # Step 3: Delete temporary file
            self._client.delete_object(
                Bucket=config.SPACES_BUCKET_NAME,
                Key=tmp_key,
            )

            logger.debug(f"Successfully uploaded object atomically: {key}")
            return True

        except Exception as e:
            logger.error(f"Error uploading object {key}: {e}")
            
            # Cleanup: try to delete temp file
            try:
                self._client.delete_object(
                    Bucket=config.SPACES_BUCKET_NAME,
                    Key=tmp_key,
                )
            except Exception:
                pass  # Ignore cleanup errors
                
            return False

    def list_objects(self, prefix: str = "") -> List[Dict[str, Any]]:
        """
        List objects with a given prefix.
        
        Args:
            prefix: Object key prefix to filter by
            
        Returns:
            List of object metadata dictionaries
        """
        if not self.is_available:
            logger.warning(f"Spaces not available - cannot list objects with prefix {prefix}")
            return []

        try:
            objects = []
            paginator = self._client.get_paginator("list_objects_v2")
            
            for page in paginator.paginate(
                Bucket=config.SPACES_BUCKET_NAME,
                Prefix=prefix,
            ):
                if "Contents" in page:
                    for obj in page["Contents"]:
                        objects.append({
                            "key": obj["Key"],
                            "size": obj["Size"],
                            "last_modified": obj["LastModified"],
                            "etag": obj["ETag"].strip('"'),
                        })
                        
            return objects
            
        except Exception as e:
            logger.error(f"Error listing objects with prefix {prefix}: {e}")
            return []

    def object_metadata(self, key: str) -> Optional[Dict[str, Any]]:
        """
        Get object metadata without downloading the content.
        
        Args:
            key: Object key/path
            
        Returns:
            Object metadata dictionary, or None if not found
        """
        if not self.is_available:
            return None

        try:
            response = self._client.head_object(
                Bucket=config.SPACES_BUCKET_NAME,
                Key=key,
            )
            
            return {
                "size": response["ContentLength"],
                "last_modified": response["LastModified"],
                "etag": response["ETag"].strip('"'),
                "metadata": response.get("Metadata", {}),
            }
            
        except ClientError as e:
            if e.response["Error"]["Code"] == "404":
                return None
            logger.error(f"Error getting metadata for {key}: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error getting metadata for {key}: {e}")
            return None

    def object_exists(self, key: str) -> bool:
        """Check if an object exists in Spaces."""
        return self.object_metadata(key) is not None

    def download_dataframe(
        self,
        key: str,
        file_format: str = "csv",
    ) -> Optional[pd.DataFrame]:
        """
        Download a DataFrame from Spaces with UTC timestamp parsing.
        
        Args:
            key: Object key/path
            file_format: File format ("csv" or "parquet")
            
        Returns:
            DataFrame with UTC timestamps or None if not found/error
        """
        data = self.get_object(key)
        if data is None:
            return None

        try:
            buffer = io.BytesIO(data)
            
            if file_format.lower() == "csv":
                df = pd.read_csv(buffer)
                # Parse timestamps to ensure they are UTC timezone-aware
                df = _parse_timestamps_on_read(df)
                return df
            elif file_format.lower() == "parquet":
                return pd.read_parquet(buffer)
            else:
                logger.error(f"Unsupported file format: {file_format}")
                return None
                
        except Exception as e:
            logger.error(f"Error reading DataFrame from {key}: {e}")
            return None

    def upload_dataframe(
        self,
        df: pd.DataFrame,
        key: str,
        file_format: str = "csv",
        metadata: Optional[Dict[str, str]] = None,
    ) -> bool:
        """
        Upload a DataFrame to Spaces with UTC timestamp enforcement.
        
        Args:
            df: DataFrame to upload
            key: Object key/path
            file_format: File format ("csv" or "parquet")
            metadata: Optional metadata to attach
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Prepare timestamps for writing if CSV format
            if file_format.lower() == "csv":
                df = _prepare_timestamps_for_write(df)
                
            buffer = io.BytesIO()
            
            if file_format.lower() == "csv":
                df.to_csv(buffer, index=False)
            elif file_format.lower() == "parquet":
                df.to_parquet(buffer, index=False)
            else:
                logger.error(f"Unsupported file format: {file_format}")
                return False

            buffer.seek(0)
            data = buffer.getvalue()
            
            # Add DataFrame metadata
            df_metadata = {
                "rows": str(len(df)),
                "columns": str(len(df.columns)),
                "format": file_format,
                "timestamps": "UTC",  # Mark that timestamps are in UTC
            }
            if metadata:
                df_metadata.update(metadata)
                
            return self.put_object_atomic(key, data, df_metadata)
            
        except Exception as e:
            logger.error(f"Error uploading DataFrame to {key}: {e}")
            return False

    def upload_json(
        self,
        data: Dict[str, Any],
        key: str,
        metadata: Optional[Dict[str, str]] = None,
    ) -> bool:
        """
        Upload JSON data to Spaces.
        
        Args:
            data: Dictionary to upload as JSON
            key: Object key/path
            metadata: Optional metadata to attach
            
        Returns:
            True if successful, False otherwise
        """
        try:
            json_str = json.dumps(data, indent=2, default=str)
            json_metadata = {"format": "json"}
            if metadata:
                json_metadata.update(metadata)
                
            return self.put_object_atomic(key, json_str, json_metadata)
            
        except Exception as e:
            logger.error(f"Error uploading JSON to {key}: {e}")
            return False

    def download_json(self, key: str) -> Optional[Dict[str, Any]]:
        """
        Download JSON data from Spaces.
        
        Args:
            key: Object key/path
            
        Returns:
            Dictionary or None if not found/error
        """
        data = self.get_object(key)
        if data is None:
            return None

        try:
            return json.loads(data.decode("utf-8"))
        except Exception as e:
            logger.error(f"Error parsing JSON from {key}: {e}")
            return None


# Global Spaces client instance
spaces_io = SpacesIO()


# Convenience functions for backward compatibility
def get_object(key: str) -> Optional[bytes]:
    """Get an object from Spaces."""
    return spaces_io.get_object(key)


def put_object_atomic(
    key: str,
    data: Union[bytes, str],
    metadata: Optional[Dict[str, str]] = None,
) -> bool:
    """Put an object to Spaces atomically."""
    return spaces_io.put_object_atomic(key, data, metadata)


def list_objects(prefix: str = "") -> List[Dict[str, Any]]:
    """List objects with prefix."""
    return spaces_io.list_objects(prefix)


def object_metadata(key: str) -> Optional[Dict[str, Any]]:
    """Get object metadata."""
    return spaces_io.object_metadata(key)


def download_dataframe(key: str, file_format: str = "csv") -> Optional[pd.DataFrame]:
    """Download DataFrame from Spaces."""
    return spaces_io.download_dataframe(key, file_format)


def upload_dataframe(
    df: pd.DataFrame,
    key: str,
    file_format: str = "csv",
    metadata: Optional[Dict[str, str]] = None,
) -> bool:
    """Upload DataFrame to Spaces."""
    return spaces_io.upload_dataframe(df, key, file_format, metadata)