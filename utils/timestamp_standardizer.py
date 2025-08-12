"""
Timestamp Standardization Module

This module implements the rigorous timestamp standardization process required for all data:
1. Parse Timestamps: Read the raw timestamp string from the API
2. Localize to New York Time: Convert to timezone-aware object using 'America/New_York' timezone  
3. Standardize to UTC for Storage: Save the final timestamp in UTC format to CSV files

This ensures all data has consistent, standardized timestamps regardless of source.
"""

import pandas as pd
import pytz
import logging
from datetime import datetime
from typing import Union, Optional

logger = logging.getLogger(__name__)

# Define timezone constants
NY_TIMEZONE = pytz.timezone('America/New_York')
UTC_TIMEZONE = pytz.UTC


def standardize_timestamp_column(df: pd.DataFrame, timestamp_col: str = 'timestamp') -> pd.DataFrame:
    """
    Standardize timestamps in a DataFrame according to the 3-step process:
    1. Parse Timestamps: Read the raw timestamp string from the API
    2. Localize to New York Time: Convert to timezone-aware object using 'America/New_York' timezone
    3. Standardize to UTC for Storage: Save the final timestamp in UTC format
    
    Args:
        df: DataFrame containing timestamp data
        timestamp_col: Name of the timestamp column to standardize
        
    Returns:
        DataFrame with standardized UTC timestamps
    """
    if df.empty or timestamp_col not in df.columns:
        logger.warning(f"DataFrame is empty or missing {timestamp_col} column")
        return df
    
    df_copy = df.copy()
    
    try:
        logger.info(f"🔄 STANDARDIZING TIMESTAMPS: Processing {len(df)} rows")
        
        # Step 1: Parse Timestamps - Read the raw timestamp string from the API
        logger.info("📅 Step 1: Parsing timestamps from API response")
        df_copy[timestamp_col] = pd.to_datetime(df_copy[timestamp_col], errors='coerce')
        
        # Remove any rows with invalid timestamps
        invalid_timestamps = df_copy[timestamp_col].isna().sum()
        if invalid_timestamps > 0:
            logger.warning(f"⚠️  Removing {invalid_timestamps} rows with invalid timestamps")
            df_copy = df_copy.dropna(subset=[timestamp_col])
        
        # Step 2: Localize to New York Time - Convert to timezone-aware using 'America/New_York'
        logger.info("🏢 Step 2: Localizing to America/New_York timezone")
        
        # Check if timestamps already have timezone info
        if df_copy[timestamp_col].dt.tz is None:
            # Assume timestamps from API are in Eastern Time if no timezone specified
            logger.info("   Timestamps have no timezone info - assuming Eastern Time")
            df_copy[timestamp_col] = df_copy[timestamp_col].dt.tz_localize(NY_TIMEZONE)
        else:
            # Convert existing timezone to Eastern Time
            logger.info(f"   Converting from {df_copy[timestamp_col].dt.tz} to America/New_York")
            df_copy[timestamp_col] = df_copy[timestamp_col].dt.tz_convert(NY_TIMEZONE)
        
        # Step 3: Standardize to UTC for Storage - Save in UTC format for consistency
        logger.info("🌍 Step 3: Converting to UTC for standardized storage")
        df_copy[timestamp_col] = df_copy[timestamp_col].dt.tz_convert(UTC_TIMEZONE)
        
        # Format as clean UTC string for CSV storage (remove microseconds for clean timestamps)
        df_copy[timestamp_col] = df_copy[timestamp_col].dt.strftime('%Y-%m-%d %H:%M:%S+00:00')
        
        logger.info(f"✅ TIMESTAMP STANDARDIZATION COMPLETE: {len(df_copy)} rows processed")
        logger.info(f"   Sample standardized timestamp: {df_copy[timestamp_col].iloc[0]}")
        
        return df_copy
        
    except Exception as e:
        logger.error(f"❌ TIMESTAMP STANDARDIZATION FAILED: {e}")
        logger.error("   Returning original DataFrame without standardization")
        return df


def standardize_daily_timestamps(df: pd.DataFrame, date_col: str = 'timestamp') -> pd.DataFrame:
    """
    Standardize daily timestamps (dates) to consistent UTC format.
    
    Args:
        df: DataFrame containing daily data
        date_col: Name of the date column to standardize
        
    Returns:
        DataFrame with standardized daily timestamps
    """
    if df.empty or date_col not in df.columns:
        logger.warning(f"DataFrame is empty or missing {date_col} column")
        return df
    
    df_copy = df.copy()
    
    try:
        logger.info(f"🔄 STANDARDIZING DAILY TIMESTAMPS: Processing {len(df)} rows")
        
        # Parse dates
        df_copy[date_col] = pd.to_datetime(df_copy[date_col], errors='coerce')
        
        # For daily data, assume market close time (4:00 PM ET) and convert to UTC
        # This provides consistent timestamp handling for daily data
        logger.info("📅 Setting daily timestamps to market close time (4:00 PM ET)")
        
        # Set time to 4:00 PM Eastern (market close)
        df_copy[date_col] = df_copy[date_col].dt.normalize()  # Set to midnight
        df_copy[date_col] = df_copy[date_col] + pd.Timedelta(hours=16)  # Add 16 hours for 4:00 PM
        
        # Localize to Eastern Time
        df_copy[date_col] = df_copy[date_col].dt.tz_localize(NY_TIMEZONE)
        
        # Convert to UTC
        df_copy[date_col] = df_copy[date_col].dt.tz_convert(UTC_TIMEZONE)
        
        # Format as ISO 8601 UTC string
        df_copy[date_col] = df_copy[date_col].dt.strftime('%Y-%m-%d %H:%M:%S+00:00')
        
        logger.info(f"✅ DAILY TIMESTAMP STANDARDIZATION COMPLETE: {len(df_copy)} rows processed")
        logger.info(f"   Sample standardized timestamp: {df_copy[date_col].iloc[0]}")
        
        return df_copy
        
    except Exception as e:
        logger.error(f"❌ DAILY TIMESTAMP STANDARDIZATION FAILED: {e}")
        return df


def validate_timestamp_standardization(df: pd.DataFrame, timestamp_col: str = 'timestamp') -> bool:
    """
    Validate that timestamps in DataFrame are properly standardized to UTC.
    
    Args:
        df: DataFrame to validate
        timestamp_col: Name of timestamp column to check
        
    Returns:
        True if timestamps are properly standardized, False otherwise
    """
    if df.empty or timestamp_col not in df.columns:
        return False
    
    try:
        # Check if timestamps can be parsed and are in UTC format
        sample_timestamps = df[timestamp_col].head(5)
        
        for ts in sample_timestamps:
            if not ts.endswith('+00:00'):
                logger.error(f"❌ Invalid timestamp format (not UTC): {ts}")
                return False
            
            # Try to parse the timestamp
            parsed = pd.to_datetime(ts)
            if parsed.tz is None:
                logger.error(f"❌ Timestamp missing timezone info: {ts}")
                return False
        
        logger.info("✅ Timestamp standardization validation passed")
        return True
        
    except Exception as e:
        logger.error(f"❌ Timestamp validation failed: {e}")
        return False


def apply_timestamp_standardization_to_api_data(df: pd.DataFrame, data_type: str = 'intraday') -> pd.DataFrame:
    """
    Apply complete timestamp standardization to data fetched from Alpha Vantage API.
    
    This is the main function to be called by data fetching jobs to ensure
    all data follows the rigorous timestamp standardization process.
    
    Args:
        df: DataFrame from Alpha Vantage API
        data_type: Type of data ('intraday', 'daily', '30min', etc.)
        
    Returns:
        DataFrame with properly standardized timestamps
    """
    if df.empty:
        return df
    
    logger.info(f"🎯 APPLYING TIMESTAMP STANDARDIZATION: {data_type} data")
    
    # Determine timestamp column name based on data type and existing columns
    timestamp_col = None
    for col in ['timestamp', 'datetime', 'Date', 'date']:
        if col in df.columns:
            timestamp_col = col
            break
    
    if not timestamp_col:
        logger.error("❌ No timestamp column found in DataFrame")
        return df
    
    logger.info(f"   Using timestamp column: {timestamp_col}")
    
    # Apply appropriate standardization based on data type
    if data_type in ['daily']:
        standardized_df = standardize_daily_timestamps(df, timestamp_col)
    else:
        standardized_df = standardize_timestamp_column(df, timestamp_col)
    
    # Ensure the column is named 'timestamp' for consistency
    if timestamp_col != 'timestamp':
        standardized_df = standardized_df.rename(columns={timestamp_col: 'timestamp'})
        logger.debug(f"Renamed column '{timestamp_col}' to 'timestamp' for consistency")
    
    # Validate the standardization
    if validate_timestamp_standardization(standardized_df, 'timestamp'):
        logger.info("✅ TIMESTAMP STANDARDIZATION VALIDATED SUCCESSFULLY")
    else:
        logger.error("❌ TIMESTAMP STANDARDIZATION VALIDATION FAILED")
    
    return standardized_df


def convert_stored_data_to_utc(df: pd.DataFrame, timestamp_col: str = 'timestamp') -> pd.DataFrame:
    """
    Convert existing stored data with Eastern Time timestamps to UTC format.
    
    This function can be used to migrate existing data files to the new UTC standard.
    
    Args:
        df: DataFrame with existing Eastern Time timestamps
        timestamp_col: Name of timestamp column
        
    Returns:
        DataFrame with UTC timestamps
    """
    if df.empty or timestamp_col not in df.columns:
        return df
    
    try:
        logger.info(f"🔄 MIGRATING EXISTING DATA TO UTC: Processing {len(df)} rows")
        
        df_copy = df.copy()
        
        # Parse existing timestamps (may have timezone info)
        df_copy[timestamp_col] = pd.to_datetime(df_copy[timestamp_col], errors='coerce')
        
        # If no timezone info, assume Eastern Time (as that's what was stored before)
        if df_copy[timestamp_col].dt.tz is None:
            logger.info("   Assuming existing timestamps are in Eastern Time")
            df_copy[timestamp_col] = df_copy[timestamp_col].dt.tz_localize(NY_TIMEZONE)
        else:
            # Convert to Eastern Time first, then to UTC
            df_copy[timestamp_col] = df_copy[timestamp_col].dt.tz_convert(NY_TIMEZONE)
        
        # Convert to UTC
        df_copy[timestamp_col] = df_copy[timestamp_col].dt.tz_convert(UTC_TIMEZONE)
        
        # Format as ISO 8601 UTC string
        df_copy[timestamp_col] = df_copy[timestamp_col].dt.strftime('%Y-%m-%d %H:%M:%S+00:00')
        
        logger.info(f"✅ DATA MIGRATION TO UTC COMPLETE: {len(df_copy)} rows")
        return df_copy
        
    except Exception as e:
        logger.error(f"❌ DATA MIGRATION FAILED: {e}")
        return df