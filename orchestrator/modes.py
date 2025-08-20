"""
Market mode detection and definitions.
"""

from enum import Enum
from datetime import datetime, time
import pytz


class MarketMode(Enum):
    """Market session modes."""
    PREMARKET = "premarket"
    MARKET = "market"
    POSTMARKET = "postmarket"
    DAILY = "daily"


def determine_market_mode() -> MarketMode:
    """
    Determine current market mode based on Eastern Time.
    
    Market hours (ET):
    - Premarket: 4:00 AM - 9:30 AM
    - Market: 9:30 AM - 4:00 PM
    - Postmarket: 4:00 PM - 8:00 PM
    - Daily: 8:00 PM - 4:00 AM (next day)
    """
    et_tz = pytz.timezone('US/Eastern')
    now_et = datetime.now(et_tz).time()
    
    # Define time boundaries
    premarket_start = time(4, 0)   # 4:00 AM
    market_open = time(9, 30)      # 9:30 AM
    market_close = time(16, 0)     # 4:00 PM
    postmarket_end = time(20, 0)   # 8:00 PM
    
    if premarket_start <= now_et < market_open:
        return MarketMode.PREMARKET
    elif market_open <= now_et < market_close:
        return MarketMode.MARKET
    elif market_close <= now_et < postmarket_end:
        return MarketMode.POSTMARKET
    else:
        return MarketMode.DAILY


def is_market_day() -> bool:
    """Check if today is a market day (Monday-Friday, excluding holidays)."""
    et_tz = pytz.timezone('US/Eastern')
    now_et = datetime.now(et_tz)
    
    # Check if weekday (0=Monday, 6=Sunday)
    if now_et.weekday() >= 5:  # Saturday or Sunday
        return False
    
    # TODO: Add holiday checking if needed
    # For now, assume all weekdays are market days
    return True