"""
General helper utilities for the trading system.

This module provides miscellaneous utility functions that don't fit
into other specific utility modules.
"""

import hashlib
import os
import socket
from typing import Any, Dict, Optional

from utils.config import config
from utils.logging_setup import get_logger
from utils.time_utils import get_market_time, is_weekend

logger = get_logger(__name__)


def get_file_hash(file_path: str) -> Optional[str]:
    """
    Calculate MD5 hash of a file.
    
    Args:
        file_path: Path to file
        
    Returns:
        MD5 hash as hex string, or None if error
    """
    try:
        hash_md5 = hashlib.md5()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()
    except Exception as e:
        logger.error(f"Error calculating file hash for {file_path}: {e}")
        return None


def format_currency(amount: float, decimals: int = 2) -> str:
    """
    Format amount as currency string.
    
    Args:
        amount: Amount to format
        decimals: Number of decimal places
        
    Returns:
        Formatted currency string
    """
    return f"${amount:,.{decimals}f}"


def format_percentage(value: float, decimals: int = 2) -> str:
    """
    Format value as percentage string.
    
    Args:
        value: Value to format (0.1 = 10%)
        decimals: Number of decimal places
        
    Returns:
        Formatted percentage string
    """
    return f"{value * 100:.{decimals}f}%"


def safe_divide(numerator: float, denominator: float, default: float = 0.0) -> float:
    """
    Safely divide two numbers, returning default if denominator is zero.
    
    Args:
        numerator: Numerator
        denominator: Denominator
        default: Value to return if division by zero
        
    Returns:
        Division result or default
    """
    return numerator / denominator if denominator != 0 else default


def calculate_r_multiple(entry: float, exit: float, stop: float, direction: str) -> float:
    """
    Calculate R-multiple for a trade.
    
    Args:
        entry: Entry price
        exit: Exit price
        stop: Stop loss price
        direction: "long" or "short"
        
    Returns:
        R-multiple (positive for profit, negative for loss)
    """
    if direction.lower() == "long":
        risk = entry - stop
        profit = exit - entry
    else:  # short
        risk = stop - entry
        profit = entry - exit
    
    return safe_divide(profit, risk, 0.0)


def calculate_position_size(
    account_size: float,
    risk_pct: float,
    entry: float,
    stop: float,
) -> int:
    """
    Calculate position size in shares based on risk management.
    
    Args:
        account_size: Total account size
        risk_pct: Risk percentage (e.g., 0.02 for 2%)
        entry: Entry price
        stop: Stop loss price
        
    Returns:
        Position size in shares
    """
    risk_amount = account_size * risk_pct
    risk_per_share = abs(entry - stop)
    
    if risk_per_share <= 0:
        return 0
    
    shares = int(risk_amount / risk_per_share)
    return max(0, shares)


def should_use_test_mode() -> bool:
    """
    Determine if system should run in test mode.
    
    Returns:
        True if test mode should be active
    """
    # Use config method
    return config.is_test_mode()


def get_test_mode_reason() -> tuple[bool, str]:
    """
    Get reason for test mode status.
    
    Returns:
        Tuple of (is_test_mode, reason_string)
    """
    is_test = config.is_test_mode()
    
    if is_test:
        if not config.MARKETDATA_TOKEN:
            return True, "No API key configured - running in TEST MODE"
        elif is_weekend():
            return True, f"Weekend detected - running in TEST MODE"
        else:
            return True, "TEST_MODE explicitly enabled"
    else:
        return False, "Live API credentials available - running in LIVE MODE"


def get_system_info() -> Dict[str, Any]:
    """
    Get system information for debugging and monitoring.
    
    Returns:
        Dictionary with system information
    """
    current_time = get_market_time()
    
    return {
        "hostname": socket.gethostname(),
        "python_version": os.sys.version,
        "current_time_et": current_time.isoformat(),
        "is_weekend": is_weekend(),
        "is_test_mode": should_use_test_mode(),
        "app_env": config.APP_ENV,
        "deployment_tag": config.DEPLOYMENT_TAG,
        "debug_mode": config.DEBUG_MODE,
        "credentials_status": config.get_credentials_status(),
    }


def truncate_string(text: str, max_length: int = 100, suffix: str = "...") -> str:
    """
    Truncate string to maximum length with suffix.
    
    Args:
        text: Text to truncate
        max_length: Maximum length
        suffix: Suffix to add if truncated
        
    Returns:
        Truncated string
    """
    if len(text) <= max_length:
        return text
    
    return text[:max_length - len(suffix)] + suffix


def chunk_list(lst: list, chunk_size: int) -> list[list]:
    """
    Split list into chunks of specified size.
    
    Args:
        lst: List to chunk
        chunk_size: Size of each chunk
        
    Returns:
        List of chunks
    """
    return [lst[i:i + chunk_size] for i in range(0, len(lst), chunk_size)]


def flatten_dict(d: dict, parent_key: str = "", sep: str = "_") -> dict:
    """
    Flatten nested dictionary.
    
    Args:
        d: Dictionary to flatten
        parent_key: Parent key prefix
        sep: Separator for nested keys
        
    Returns:
        Flattened dictionary
    """
    items = []
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten_dict(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)


def merge_dicts(*dicts: dict) -> dict:
    """
    Merge multiple dictionaries, with later dicts taking precedence.
    
    Args:
        *dicts: Dictionaries to merge
        
    Returns:
        Merged dictionary
    """
    result = {}
    for d in dicts:
        result.update(d)
    return result


def ensure_directory(path: str) -> None:
    """
    Ensure directory exists, creating if necessary.
    
    Args:
        path: Directory path
    """
    os.makedirs(path, exist_ok=True)


def get_file_size_mb(file_path: str) -> float:
    """
    Get file size in MB.
    
    Args:
        file_path: Path to file
        
    Returns:
        File size in MB
    """
    try:
        size_bytes = os.path.getsize(file_path)
        return size_bytes / (1024 * 1024)
    except OSError:
        return 0.0


def format_duration(seconds: float) -> str:
    """
    Format duration in seconds to human-readable string.
    
    Args:
        seconds: Duration in seconds
        
    Returns:
        Formatted duration string
    """
    if seconds < 60:
        return f"{seconds:.1f}s"
    elif seconds < 3600:
        minutes = seconds / 60
        return f"{minutes:.1f}m"
    else:
        hours = seconds / 3600
        return f"{hours:.1f}h"


def create_summary_stats(values: list[float]) -> Dict[str, float]:
    """
    Create summary statistics for a list of values.
    
    Args:
        values: List of numeric values
        
    Returns:
        Dictionary with summary statistics
    """
    if not values:
        return {
            "count": 0,
            "mean": 0.0,
            "min": 0.0,
            "max": 0.0,
            "std": 0.0,
        }
    
    import statistics
    
    return {
        "count": len(values),
        "mean": statistics.mean(values),
        "min": min(values),
        "max": max(values),
        "std": statistics.stdev(values) if len(values) > 1 else 0.0,
    }