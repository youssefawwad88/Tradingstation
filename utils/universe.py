"""Universe loader for the trading system.

This module handles loading the universe (list of active symbols) from Spaces storage
with fallback to default tickers if the universe file is not found.
"""

import logging
from typing import List, Optional

import pandas as pd

from utils.config import config
from utils.paths import universe_key
from utils.spaces_io import spaces_io

logger = logging.getLogger(__name__)

# Global cache for universe data
_universe_cache: Optional[pd.DataFrame] = None
_universe_loaded = False


def load_universe() -> List[str]:
    """Load universe of active symbols from Spaces.
    
    Reads CSV from Spaces at universe_key() with columns: symbol,active,fetch_1min,fetch_30min,fetch_daily.
    Returns active symbols only. Falls back to FALLBACK_TICKERS if 404 or error.
    
    Returns:
        List of active symbol strings
    """
    global _universe_cache, _universe_loaded
    
    if _universe_loaded and _universe_cache is not None:
        # Return cached active symbols
        active_symbols = _universe_cache[_universe_cache["active"] == 1]["symbol"].tolist()
        return active_symbols
    
    try:
        # Download universe CSV from Spaces
        key = universe_key()
        df = spaces_io.download_dataframe(key)
        
        # If not found, probe case sensitivity
        if df is None:
            probe_key = None
            if "/Universe/" in key:
                probe_key = key.replace("/Universe/", "/universe/")
            elif "/universe/" in key:
                probe_key = key.replace("/universe/", "/Universe/")
            
            if probe_key and probe_key != key:
                logger.info(f"universe_probe tried={probe_key}")
                df = spaces_io.download_dataframe(probe_key)
        
        if df is None or df.empty:
            logger.warning("universe_not_found - using fallback tickers")
            return config.FALLBACK_TICKERS
        
        # Validate required columns exist
        required_columns = ["symbol", "active", "fetch_1min", "fetch_30min", "fetch_daily"]
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            logger.error(f"Universe CSV missing required columns: {missing_columns}")
            logger.warning("universe_not_found - using fallback tickers")
            return config.FALLBACK_TICKERS
        
        # Cache the DataFrame
        _universe_cache = df
        _universe_loaded = True
        
        # Filter for active symbols
        active_symbols = df[df["active"] == 1]["symbol"].tolist()
        
        # Log success with sample
        sample_symbols = active_symbols[:3] if len(active_symbols) >= 3 else active_symbols
        logger.info(f"universe_loaded s3_key={key} count={len(active_symbols)} sample={sample_symbols}")
        
        return active_symbols
        
    except Exception as e:
        logger.error(f"Error loading universe from {universe_key()}: {e}")
        logger.warning("universe_not_found - using fallback tickers")
        return config.FALLBACK_TICKERS


def get_universe_dataframe() -> Optional[pd.DataFrame]:
    """Get the full universe DataFrame with all columns.
    
    Returns:
        DataFrame with universe data or None if not loaded
    """
    global _universe_cache
    if not _universe_loaded:
        # Trigger loading
        load_universe()
    return _universe_cache


def clear_universe_cache() -> None:
    """Clear the universe cache to force reload on next access."""
    global _universe_cache, _universe_loaded
    _universe_cache = None
    _universe_loaded = False