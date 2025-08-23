"""Log formatting utilities for consistent logging across the trading system.

This module provides common formatting functions for log messages,
extracting duplicated formatting logic from intraday and daily paths.
"""

import logging
from typing import Optional

import pandas as pd

logger = logging.getLogger(__name__)


def log_write_meta(
    symbol: str,
    interval: str,
    s3_key: str,
    rows_before: int,
    rows_after: int,
    appended: int,
    pruned_days: int = 0,
    etag: str = "unknown",
    size: int = 0,
    last_modified: str = "unknown"
) -> None:
    """Log write metadata in consistent format.
    
    Args:
        symbol: Stock symbol
        interval: Time interval
        s3_key: S3 object key
        rows_before: Number of rows before write
        rows_after: Number of rows after write  
        appended: Number of rows appended
        pruned_days: Days of data pruned (for retention)
        etag: S3 object ETag
        size: Object size in bytes
        last_modified: Last modified timestamp
    """
    logger.info(
        f"write_ok interval={interval} symbol={symbol} s3_key={s3_key} "
        f"rows_before={rows_before} rows_after={rows_after} appended={appended} "
        f"pruned_days={pruned_days} etag={etag} size={size} last_modified={last_modified}"
    )


def log_write_skip(
    symbol: str,
    interval: str,
    s3_key: str,
    reason: str = "no_new_rows",
    latest_ts: str = "none"
) -> None:
    """Log write skip in consistent format.
    
    Args:
        symbol: Stock symbol
        interval: Time interval
        s3_key: S3 object key
        reason: Reason for skipping write
        latest_ts: Latest timestamp in existing data
    """
    logger.info(
        f"write_skip interval={interval} symbol={symbol} reason={reason} "
        f"s3_key={s3_key} latest_ts={latest_ts}"
    )


def format_timestamp_iso(timestamp: Optional[pd.Timestamp]) -> str:
    """Format timestamp as ISO string for logging.
    
    Args:
        timestamp: Pandas timestamp or None
        
    Returns:
        ISO formatted timestamp string or "none"
    """
    if timestamp is None or pd.isna(timestamp):
        return "none"
    return timestamp.strftime("%Y-%m-%dT%H:%M:%SZ")
