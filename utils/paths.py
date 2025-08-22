"""Path utilities for the trading system.

This module provides centralized path building functions for S3/Spaces objects,
eliminating string concatenation in calling code and ensuring all paths are
constructed consistently under the base prefix.
"""

import logging
import os

logger = logging.getLogger(__name__)

# Path configuration from environment
BASE = os.getenv("SPACES_BASE_PREFIX", "").strip("/")  # e.g. "trading-system"
DATA_ROOT = os.getenv("DATA_ROOT", "data").strip("/")  # e.g. "data" (RELATIVE)
UNIVERSE_KEY = os.getenv("UNIVERSE_KEY", "data/Universe/master_tickerlist.csv").strip("/")  # RELATIVE


def k(*parts: str) -> str:
    """Join path parts, stripping slashes and ignoring empty parts.
    
    Args:
        *parts: Path components to join
        
    Returns:
        Joined path with slashes, empty parts filtered out
    """
    return "/".join(p.strip("/") for p in parts if p is not None and p != "")


def universe_key() -> str:
    """Get the S3 key for the universe/ticker list.
    
    Returns:
        Complete S3 key under base prefix for universe CSV
    """
    return k(BASE, UNIVERSE_KEY)


def intraday_key(symbol: str, interval: str) -> str:
    """Get the S3 key for intraday data.
    
    Args:
        symbol: Stock symbol
        interval: Time interval (e.g., "1min", "30min")
        
    Returns:
        Complete S3 key under base prefix for intraday CSV
    """
    return k(BASE, DATA_ROOT, "intraday", interval, f"{symbol.upper()}.csv")


def daily_key(symbol: str) -> str:
    """Get the S3 key for daily data.
    
    Args:
        symbol: Stock symbol
        
    Returns:
        Complete S3 key under base prefix for daily CSV
    """
    return k(BASE, DATA_ROOT, "daily", f"{symbol.upper()}.csv")


# Backward compatibility aliases
def s3_key(*parts: str) -> str:
    """Build a complete S3/Spaces key from path parts (legacy function).
    
    Args:
        *parts: Path components to join
        
    Returns:
        Complete S3 key with SPACES_BASE_PREFIX
    """
    if not parts:
        return BASE
    return k(BASE, *parts)


def key_intraday_1min(sym: str) -> str:
    """Get the S3 key for 1-minute intraday data (legacy function).
    
    Args:
        sym: Stock symbol
        
    Returns:
        Complete S3 key for 1-minute intraday CSV file
    """
    return intraday_key(sym, "1min")


def key_intraday_30min(sym: str) -> str:
    """Get the S3 key for 30-minute intraday data (legacy function).
    
    Args:
        sym: Stock symbol
        
    Returns:
        Complete S3 key for 30-minute intraday CSV file
    """
    return intraday_key(sym, "30min")


def key_daily(sym: str) -> str:
    """Get the S3 key for daily data (legacy function).
    
    Args:
        sym: Stock symbol
        
    Returns:
        Complete S3 key for daily CSV file
    """
    return daily_key(sym)


def log_startup_paths() -> None:
    """Log path resolution on startup as required by instrumentation."""
    sample_prefix = k(BASE, DATA_ROOT, "intraday", "1min")
    logger.info(f"paths_resolved base={BASE} data_root={DATA_ROOT} universe_key={UNIVERSE_KEY} write_prefix={sample_prefix}/")
