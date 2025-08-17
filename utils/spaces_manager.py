import io
import logging
import os

import boto3
import pandas as pd
from botocore.exceptions import ClientError

from utils.config import (
    DEBUG_MODE,
    SPACES_ACCESS_KEY_ID,
    SPACES_BUCKET_NAME,
    SPACES_ENDPOINT_URL,
    SPACES_REGION,
    SPACES_SECRET_ACCESS_KEY,
)

logger = logging.getLogger(__name__)


def get_spaces_credentials_status():
    """
    Check the status of all required Spaces credentials.

    Returns:
        dict: Contains 'all_present' boolean and 'missing' list of missing credential names
    """
    missing_vars = []

    if not SPACES_ACCESS_KEY_ID:
        missing_vars.append("SPACES_ACCESS_KEY_ID")
    if not SPACES_SECRET_ACCESS_KEY:
        missing_vars.append("SPACES_SECRET_ACCESS_KEY")
    if not SPACES_BUCKET_NAME:
        missing_vars.append("SPACES_BUCKET_NAME")
    if not SPACES_REGION:
        missing_vars.append("SPACES_REGION")

    return {
        "all_present": len(missing_vars) == 0,
        "missing": missing_vars,
        "status_details": {
            "SPACES_ACCESS_KEY_ID": "✅ Set" if SPACES_ACCESS_KEY_ID else "❌ Missing",
            "SPACES_SECRET_ACCESS_KEY": (
                "✅ Set" if SPACES_SECRET_ACCESS_KEY else "❌ Missing"
            ),
            "SPACES_BUCKET_NAME": "✅ Set" if SPACES_BUCKET_NAME else "❌ Missing",
            "SPACES_REGION": "✅ Set" if SPACES_REGION else "❌ Missing",
        },
    }


def get_spaces_client():
    """
    Create and return a boto3 client for DigitalOcean Spaces.
    """
    # Check credential status using helper function
    creds_status = get_spaces_credentials_status()

    # Check if core credentials are present (region gets fallback)
    if not all([SPACES_ACCESS_KEY_ID, SPACES_SECRET_ACCESS_KEY, SPACES_BUCKET_NAME]):
        # Get missing core credentials (excluding SPACES_REGION which has fallback)
        core_missing = [
            var for var in creds_status["missing"] if var != "SPACES_REGION"
        ]

        if core_missing:
            status_list = [
                f"{var}: {creds_status['status_details'][var]}"
                for var in creds_status["status_details"]
            ]
            logger.warning(
                f"⚠️ Cannot create Spaces client - Missing required environment variables: {', '.join(core_missing)}. "
                f"Please set the following environment variables: "
                f"{', '.join(status_list)}"
            )

            if DEBUG_MODE:
                print(
                    f"🔑 Missing Spaces credentials: "
                    f"Key ID: {creds_status['status_details']['SPACES_ACCESS_KEY_ID']}, "
                    f"Secret: {creds_status['status_details']['SPACES_SECRET_ACCESS_KEY']}, "
                    f"Bucket: {creds_status['status_details']['SPACES_BUCKET_NAME']}, "
                    f"Region: {creds_status['status_details']['SPACES_REGION']}"
                )
            return None

    try:
        # Validate SPACES_REGION and provide fallback
        validated_region = SPACES_REGION if SPACES_REGION else "nyc3"

        if not SPACES_REGION:
            logger.info(
                f"SPACES_REGION not set, using default fallback: {validated_region}"
            )

        session = boto3.session.Session()
        client = session.client(
            "s3",
            region_name=validated_region,
            endpoint_url=SPACES_ENDPOINT_URL,
            aws_access_key_id=SPACES_ACCESS_KEY_ID,
            aws_secret_access_key=SPACES_SECRET_ACCESS_KEY,
        )
        return client
    except Exception as e:
        logger.error(f"Failed to create Spaces client: {e}")
        if DEBUG_MODE:
            print(f"❌ Failed to create Spaces client: {e}")
        return None


def upload_dataframe(df, object_name, file_format="csv"):
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

        if file_format.lower() == "csv":
            df.to_csv(buffer, index=False)
        elif file_format.lower() == "parquet":
            df.to_parquet(buffer, index=False)
        else:
            logger.error(f"Unsupported file format: {file_format}")
            return False

        buffer.seek(0)
        client.upload_fileobj(buffer, SPACES_BUCKET_NAME, object_name)
        logger.info(f"Uploaded DataFrame to {SPACES_BUCKET_NAME}/{object_name}")
        if DEBUG_MODE:
            print(
                f"☁️ Successfully uploaded to Spaces: {SPACES_BUCKET_NAME}/{object_name}"
            )
        return True
    except Exception as e:
        logger.error(f"Error uploading DataFrame to Spaces: {e}")
        if DEBUG_MODE:
            print(f"❌ Failed to upload to Spaces: {e}")
        return False


def download_dataframe(object_name, file_format="csv"):
    """
    Download a pandas DataFrame directly from DigitalOcean Spaces.

    Args:
        object_name (str): Object name in the Spaces bucket
        file_format (str): Format of the file ('csv' or 'parquet')

    Returns:
        pandas.DataFrame: Downloaded DataFrame, or empty DataFrame if failed
    """
    client = get_spaces_client()
    if not client:
        logger.warning(f"Cannot download from Spaces - no client available")
        return pd.DataFrame()

    try:
        # Download the object
        response = client.get_object(Bucket=SPACES_BUCKET_NAME, Key=object_name)
        content = response["Body"].read()

        if file_format.lower() == "csv":
            df = pd.read_csv(io.BytesIO(content))
        elif file_format.lower() == "parquet":
            df = pd.read_parquet(io.BytesIO(content))
        else:
            logger.error(f"Unsupported file format: {file_format}")
            return pd.DataFrame()

        logger.info(
            f"Downloaded DataFrame from {SPACES_BUCKET_NAME}/{object_name}: {len(df)} rows"
        )
        if DEBUG_MODE:
            print(
                f"☁️ Successfully downloaded from Spaces: {SPACES_BUCKET_NAME}/{object_name} - {len(df)} rows"
            )
        return df
    except Exception as e:
        logger.warning(f"Error downloading DataFrame from Spaces {object_name}: {e}")
        if DEBUG_MODE:
            print(f"⚠️ Failed to download from Spaces {object_name}: {e}")
        return pd.DataFrame()


def get_cloud_file_size_bytes(object_name):
    """
    Get the size of a file in cloud storage (DigitalOcean Spaces) without downloading it.

    Args:
        object_name (str): Object name/path in the cloud storage bucket

    Returns:
        int: File size in bytes, or 0 if file doesn't exist or error occurs
    """
    client = get_spaces_client()
    if not client:
        logger.warning("Cannot check cloud file size - no client available")
        return 0

    try:
        # Use HEAD request to get object metadata without downloading
        response = client.head_object(Bucket=SPACES_BUCKET_NAME, Key=object_name)
        file_size = response.get("ContentLength", 0)
        logger.debug(f"☁️ Cloud file size for {object_name}: {file_size} bytes")
        return file_size
    except ClientError as e:
        error_code = e.response["Error"]["Code"]
        if error_code == "404":
            logger.debug(f"☁️ Cloud file not found: {object_name}")
        else:
            logger.warning(f"☁️ Error checking cloud file size for {object_name}: {e}")
        return 0
    except Exception as e:
        logger.warning(
            f"☁️ Unexpected error checking cloud file size for {object_name}: {e}"
        )
        return 0


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

    def list_objects(self, prefix=""):
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
                Bucket=SPACES_BUCKET_NAME, Prefix=prefix
            )

            if "Contents" in response:
                # Return just the filenames without the prefix path
                return [
                    obj["Key"].replace(prefix, "").lstrip("/")
                    for obj in response["Contents"]
                    if not obj["Key"].endswith("/")
                ]
            else:
                return []

        except Exception as e:
            logger.error(f"Error listing objects with prefix '{prefix}': {e}")
            return []


# Create a global instance for compatibility
spaces_manager = SpacesManager()
