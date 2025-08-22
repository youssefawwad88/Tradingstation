"""Path utilities for the trading system.

This module provides centralized path building functions for S3/Spaces objects,
eliminating string concatenation in calling code.
"""

import os


def s3_key(*parts: str) -> str:
    """Build a complete S3/Spaces key from path parts.
    
    Args:
        *parts: Path components to join
        
    Returns:
        Complete S3 key with SPACES_BASE_PREFIX
    """
    spaces_base_prefix = os.getenv("SPACES_BASE_PREFIX", "trading-system")
    if not parts:
        return spaces_base_prefix
    return f"{spaces_base_prefix}/" + "/".join(parts)


def key_intraday_1min(sym: str) -> str:
    """Get the S3 key for 1-minute intraday data.
    
    Args:
        sym: Stock symbol
        
    Returns:
        S3 key for 1-minute intraday CSV file
    """
    data_root = os.getenv("DATA_ROOT", "data")
    return f"{data_root}/intraday/1min/{sym}.csv"


def key_intraday_30min(sym: str) -> str:
    """Get the S3 key for 30-minute intraday data.
    
    Args:
        sym: Stock symbol
        
    Returns:
        S3 key for 30-minute intraday CSV file
    """
    data_root = os.getenv("DATA_ROOT", "data")
    return f"{data_root}/intraday/30min/{sym}.csv"


def key_daily(sym: str) -> str:
    """Get the S3 key for daily data.
    
    Args:
        sym: Stock symbol
        
    Returns:
        S3 key for daily CSV file
    """
    data_root = os.getenv("DATA_ROOT", "data")
    return f"{data_root}/daily/{sym}.csv"


def universe_key() -> str:
    """Get the S3 key for the universe/ticker list.
    
    Returns:
        S3 key for universe CSV file
    """
    return os.getenv("UNIVERSE_KEY", "data/universe/master_tickerlist.csv")