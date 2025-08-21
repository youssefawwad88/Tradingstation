"""
Provider routing layer for market data fetching.

This module provides a unified interface for fetching market data from different providers
while maintaining consistency in data format and timestamps.
"""

import os
from typing import Optional

import pandas as pd

from utils.logging_setup import get_logger

logger = get_logger(__name__)


def get_candles(
    symbol: str,
    resolution: str,                 # "1" | "30" | "D"
    *,
    from_iso: Optional[str] = None,     # e.g., "2025-08-21T12:00:00Z"
    to_iso: Optional[str] = None,       # e.g., "2025-08-21T20:00:00Z"
    countback: Optional[int] = None,    # e.g., 520
    extended: bool = False,          # intraday only
    adjustsplits: Optional[bool] = None # daily only
) -> pd.DataFrame:
    """
    Fetch market data candles from the configured provider.
    
    Returns DataFrame with columns:
      timestamp (tz-aware UTC), open, high, low, close, volume
      
    Args:
        symbol: Stock ticker symbol
        resolution: Time resolution ("1" for 1min, "30" for 30min, "D" for daily)
        from_iso: Start time in ISO format (UTC), optional
        to_iso: End time in ISO format (UTC), optional
        countback: Number of bars to fetch backwards from to_iso, optional
        extended: Include extended hours for intraday data
        adjustsplits: Apply split adjustments for daily data
        
    Returns:
        DataFrame with standardized OHLCV data and UTC timestamps
    """
    # Route to MarketData provider (only provider after migration)
    from utils.providers.marketdata import MarketDataProvider
    
    provider = MarketDataProvider()
    return provider.get_candles(
        symbol=symbol,
        resolution=resolution,
        from_iso=from_iso,
        to_iso=to_iso,
        countback=countback,
        extended=extended,
        adjustsplits=adjustsplits,
    )


def health_check() -> tuple[bool, str]:
    """
    Check provider health with a simple probe.
    
    Returns:
        Tuple of (is_healthy, status_message)
    """
    from utils.providers.marketdata import MarketDataProvider
    
    provider = MarketDataProvider()
    return provider.health_check()