"""
Data storage and retrieval operations.

This module handles all data storage operations including:
- Local filesystem operations
- Cloud storage (DigitalOcean Spaces) operations
- Data retention and cleanup
- Path management
"""

import logging
import os
from typing import Optional, Tuple

import pandas as pd

from .config import DAILY_DATA_DIR, INTRADAY_30MIN_DATA_DIR, INTRADAY_DATA_DIR
from .spaces_manager import upload_dataframe

logger = logging.getLogger(__name__)


def save_df_to_local(
    df: pd.DataFrame, ticker: str, interval: str, directory: str = INTRADAY_DATA_DIR
) -> Tuple[Optional[str], bool]:
    """
    Save DataFrame to local filesystem.

    Args:
        df: DataFrame to save
        ticker: Stock ticker symbol
        interval: Time interval of the data
        directory: Directory to save the file

    Returns:
        Tuple of (file path or None, success boolean)
    """
    os.makedirs(directory, exist_ok=True)
    file_path = os.path.join(directory, f"{ticker}_{interval}.csv")

    try:
        df.to_csv(file_path, index=False)
        logger.info(f"Saved {ticker} data to {file_path}")
        return file_path, True
    except Exception as e:
        logger.error(f"Error saving {ticker} data to {file_path}: {e}")
        return None, False


def save_df_to_s3(
    df: pd.DataFrame,
    object_name_or_ticker: str,
    interval: Optional[str] = None,
    s3_prefix: str = "intraday",
) -> bool:
    """
    Save DataFrame to DigitalOcean Spaces with enhanced logging and path verification.
    STANDARDIZES on data/ prefix for all storage and logs exact paths used.
    
    ENHANCED for Phase 1.3 Cloud Storage Audit with verification of actual upload paths.

    Args:
        df: DataFrame to save
        object_name_or_ticker: Object name/path in S3 OR ticker symbol
        interval: Time interval of the data (if ticker provided)
        s3_prefix: Prefix/folder in the S3 bucket (if ticker provided)

    Returns:
        True if successful (either Spaces or local), False otherwise
    """
    # Determine if we got an object name or ticker
    if interval is not None:
        # Old-style call with ticker and interval
        # STANDARDIZE on data/ prefix as specified in Phase 4
        object_name = f"data/{s3_prefix}/{object_name_or_ticker}_{interval}.csv"
        ticker = object_name_or_ticker
        logger.info(
            f"💾 SAVE_DF_TO_S3: Processing {object_name_or_ticker} {interval} data"
        )
    else:
        # New-style call with direct object name - ensure data/ prefix
        object_name = object_name_or_ticker
        if not object_name.startswith("data/"):
            object_name = f"data/{object_name}"
        # Extract ticker from object name for local fallback
        if "/" in object_name:
            parts = object_name.split("/")
            filename = parts[-1]  # Get filename from path
            if "_" in filename:
                ticker = filename.split("_")[0]
                interval_part = filename.replace(f"{ticker}_", "").replace(".csv", "")
            else:
                ticker = filename.replace(".csv", "")
                interval_part = "1min"  # default
        else:
            ticker = object_name.replace(".csv", "")
            interval_part = "1min"  # default
        logger.info(f"💾 SAVE_DF_TO_S3: Processing direct object name")

    # PHASE 1.3: LOG the exact path used for each save operation as required
    logger.info(f"📂 PHASE 1.3 AUDIT: Saving to Spaces path: {object_name}")
    logger.info(f"   Data rows: {len(df)}")
    if not df.empty:
        date_col = (
            "Date"
            if "Date" in df.columns
            else "datetime" if "datetime" in df.columns else "timestamp"
        )
        if date_col in df.columns:
            min_date = pd.to_datetime(df[date_col]).min()
            max_date = pd.to_datetime(df[date_col]).max()
            logger.info(f"   Date range: {min_date} to {max_date}")

    # Try Spaces upload first
    success = upload_dataframe(df, object_name)
    if success:
        # PHASE 1.3: CONFIRM the file exists after saving as required
        logger.info(f"✅ File saved successfully to CLOUD STORAGE: {object_name}")
        logger.info(f"☁️ Spaces upload confirmed for {ticker}")
        
        # PHASE 1.3: Verify upload by checking if file exists and getting its size
        from .spaces_manager import file_exists_in_spaces, get_cloud_file_size_bytes
        
        if file_exists_in_spaces(object_name):
            cloud_file_size = get_cloud_file_size_bytes(object_name)
            logger.info(f"🔍 PHASE 1.3 VERIFICATION: Cloud file exists at {object_name}")
            logger.info(f"🔍 PHASE 1.3 VERIFICATION: Cloud file size: {cloud_file_size} bytes")
            
            # Additional verification: ensure the cloud file size matches expectations
            # Calculate expected size based on DataFrame
            import io
            temp_buffer = io.StringIO()
            df.to_csv(temp_buffer, index=False)
            expected_size = len(temp_buffer.getvalue().encode('utf-8'))
            temp_buffer.close()
            
            logger.info(f"🔍 PHASE 1.3 VERIFICATION: Expected size: {expected_size} bytes")
            
            # Allow for small differences due to encoding/formatting
            size_difference = abs(cloud_file_size - expected_size)
            if size_difference <= 100:  # Allow 100 bytes difference
                logger.info(f"✅ PHASE 1.3 VERIFICATION: File size matches (diff: {size_difference} bytes)")
            else:
                logger.warning(f"⚠️ PHASE 1.3 VERIFICATION: File size mismatch (diff: {size_difference} bytes)")
                logger.warning("   This could indicate a partial upload or file corruption!")
        else:
            logger.error(f"❌ PHASE 1.3 VERIFICATION: Cloud file NOT FOUND at {object_name}")
            logger.error("   This indicates a critical upload failure!")
            
        return True
    else:
        logger.warning(
            f"⚠️ Failed to upload to Spaces at {object_name}. Trying local filesystem fallback..."
        )
        logger.warning("💡 CSV files will NOT be updated in cloud storage")

        # Fallback to local filesystem with enhanced logging
        try:
            # Extract interval from object name if not provided
            if interval is None:
                interval = interval_part if "interval_part" in locals() else "1min"

            # Determine directory based on data/ standardized paths
            base_dir = os.path.join(
                os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data"
            )

            if "daily" in object_name or interval == "daily":
                directory = os.path.join(base_dir, "daily")
                local_filename = f"{ticker}_daily.csv"
            elif "30min" in str(interval) or "30min" in object_name:
                directory = os.path.join(base_dir, "intraday_30min")
                local_filename = f"{ticker}_30min.csv"
            else:
                directory = os.path.join(base_dir, "intraday")
                local_filename = f"{ticker}_1min.csv"

            # Save to local filesystem
            os.makedirs(directory, exist_ok=True)
            local_path = os.path.join(directory, local_filename)

            logger.warning(f"💽 LOCAL FALLBACK: Saving to {local_path}")
            logger.warning(
                "⚠️ NOTE: Data saved locally but NOT uploaded to cloud storage"
            )
            df.to_csv(local_path, index=False)

            # Verify file exists locally
            if os.path.exists(local_path):
                file_size = os.path.getsize(local_path)
                logger.info(f"✅ File saved successfully to {local_path}")
                logger.info(f"📁 Local file confirmed: {file_size} bytes")
                
                # PHASE 1.3: Log local file path for audit trail
                logger.info(f"🔍 PHASE 1.3 LOCAL AUDIT: Local file path: {local_path}")
                logger.info(f"🔍 PHASE 1.3 LOCAL AUDIT: Local file size: {file_size} bytes")
                return True
            else:
                logger.error(f"❌ Local file verification failed: {local_path}")
                return False

        except Exception as e:
            logger.error(f"❌ Local filesystem fallback also failed: {e}")
            return False


def read_df_from_s3(object_name: str) -> pd.DataFrame:
    """
    Read DataFrame from S3/Spaces with cloud-first approach and local fallback.

    Args:
        object_name: Object name/path in S3

    Returns:
        DataFrame if successful, empty DataFrame only when file is not found

    Raises:
        pd.errors.ParserError: When CSV file is corrupted
        pd.errors.EmptyDataError: When CSV file is empty
        Other exceptions: Re-raised after logging for proper error handling
    """
    logger.info(f"Attempting to read DataFrame from {object_name}")

    # Try to read from Spaces first (if credentials available)
    from .spaces_manager import download_dataframe

    try:
        cloud_df = download_dataframe(object_name)
        if not cloud_df.empty:
            logger.info(
                f"✅ Successfully read {len(cloud_df)} rows from CLOUD STORAGE: {object_name}"
            )
            return cloud_df
        else:
            logger.info(
                f"⚠️ Cloud file exists but is empty or unreadable: {object_name}"
            )
    except FileNotFoundError:
        # File not found in cloud - continue to local fallback
        logger.debug(f"File not found in cloud storage: {object_name}")
    except (pd.errors.ParserError, pd.errors.EmptyDataError) as e:
        # Re-raise critical parsing errors after logging
        logger.error(f"Critical error reading from cloud storage {object_name}: {e}")
        raise
    except Exception as e:
        # Log other cloud errors but continue to local fallback
        logger.warning(f"⚠️ Could not read from cloud storage: {e}")

    # Fallback to local file
    local_file = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))), object_name
    )
    if os.path.exists(local_file):
        try:
            df = pd.read_csv(local_file)
            logger.info(
                f"📁 Successfully read {len(df)} rows from LOCAL FILE: {local_file}"
            )
            return df
        except (pd.errors.ParserError, pd.errors.EmptyDataError) as e:
            # Re-raise critical parsing errors after logging
            logger.error(f"Critical error reading local file {local_file}: {e}")
            raise
        except Exception as e:
            # Re-raise other local file errors after logging
            logger.error(f"Error reading local file {local_file}: {e}")
            # Re-raise the exception after logging
            raise

    # Return empty DataFrame only if file doesn't exist (FileNotFoundError case)
    logger.warning(
        f"File not found in cloud or locally: {object_name} - returning empty DataFrame"
    )
    return pd.DataFrame()


def get_data_directory(data_type: str) -> str:
    """
    Get the appropriate data directory for a given data type.

    Args:
        data_type: Type of data ('daily', '30min', '1min', 'intraday')

    Returns:
        Directory path
    """
    base_dir = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data"
    )

    if data_type == "daily":
        return os.path.join(base_dir, "daily")
    elif data_type == "30min":
        return os.path.join(base_dir, "intraday_30min")
    else:  # '1min' or 'intraday'
        return os.path.join(base_dir, "intraday")


def ensure_directories_exist() -> None:
    """Ensure all required data directories exist."""
    directories = [INTRADAY_DATA_DIR, INTRADAY_30MIN_DATA_DIR, DAILY_DATA_DIR]

    for directory in directories:
        os.makedirs(directory, exist_ok=True)
        logger.debug(f"Ensured directory exists: {directory}")


def cleanup_old_files(directory: str, max_files: int = 100) -> None:
    """
    Clean up old files in a directory, keeping only the most recent ones.

    Args:
        directory: Directory to clean up
        max_files: Maximum number of files to keep
    """
    if not os.path.exists(directory):
        return

    try:
        files = []
        for filename in os.listdir(directory):
            if filename.endswith(".csv"):
                filepath = os.path.join(directory, filename)
                files.append((filepath, os.path.getmtime(filepath)))

        # Sort by modification time (newest first)
        files.sort(key=lambda x: x[1], reverse=True)

        # Remove excess files
        for filepath, _ in files[max_files:]:
            try:
                os.remove(filepath)
                logger.info(f"Removed old file: {filepath}")
            except Exception as e:
                logger.error(f"Error removing file {filepath}: {e}")

    except Exception as e:
        logger.error(f"Error cleaning up directory {directory}: {e}")


def get_file_size(filepath: str) -> int:
    """
    Get the size of a file in bytes.

    Args:
        filepath: Path to the file

    Returns:
        File size in bytes, 0 if file doesn't exist
    """
    try:
        return os.path.getsize(filepath) if os.path.exists(filepath) else 0
    except Exception as e:
        logger.error(f"Error getting file size for {filepath}: {e}")
        return 0
