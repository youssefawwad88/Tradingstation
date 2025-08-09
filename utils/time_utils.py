"""
Time utilities for Trading Station.
Provides market-hours awareness, session handling, and timezone management.
"""

import pytz
from datetime import datetime, time, timedelta
from typing import Optional, Tuple
import holidays
from .config import TIMEZONE

# Set up timezone
ET = pytz.timezone(TIMEZONE)
UTC = pytz.UTC

# Market hours (ET)
MARKET_OPEN_TIME = time(9, 30)  # 9:30 AM ET
MARKET_CLOSE_TIME = time(16, 0)  # 4:00 PM ET
PREMARKET_START = time(4, 0)    # 4:00 AM ET
POSTMARKET_END = time(20, 0)    # 8:00 PM ET

# Special times for strategies
FIRST_VALID_TIME_FOR_GAPGO = time(9, 36)  # 9:36 AM ET
ORB_RANGE_START = time(9, 30)    # 9:30 AM ET
ORB_RANGE_END = time(9, 39)      # 9:39 AM ET

# US market holidays
US_HOLIDAYS = holidays.UnitedStates()

def now_utc() -> datetime:
    """Get current time in UTC."""
    return datetime.now(UTC)

def now_et() -> datetime:
    """Get current time in Eastern Time."""
    return datetime.now(ET)

def to_et(dt: datetime) -> datetime:
    """Convert datetime to Eastern Time."""
    if dt.tzinfo is None:
        # Assume UTC if no timezone
        dt = UTC.localize(dt)
    return dt.astimezone(ET)

def to_utc(dt: datetime) -> datetime:
    """Convert datetime to UTC."""
    if dt.tzinfo is None:
        # Assume ET if no timezone
        dt = ET.localize(dt)
    return dt.astimezone(UTC)

def is_trading_day(date_et: datetime) -> bool:
    """
    Check if a given date is a trading day (weekday, not holiday).
    
    Args:
        date_et: Date in Eastern Time
        
    Returns:
        True if it's a trading day
    """
    # Check if it's a weekend
    if date_et.weekday() >= 5:  # Saturday = 5, Sunday = 6
        return False
    
    # Check if it's a US market holiday
    if date_et.date() in US_HOLIDAYS:
        return False
    
    return True

def is_market_open(dt: Optional[datetime] = None) -> bool:
    """
    Check if the market is currently open.
    
    Args:
        dt: DateTime to check (defaults to now in ET)
        
    Returns:
        True if market is open
    """
    if dt is None:
        dt = now_et()
    else:
        dt = to_et(dt)
    
    # Check if it's a trading day
    if not is_trading_day(dt):
        return False
    
    # Check if within market hours
    return MARKET_OPEN_TIME <= dt.time() <= MARKET_CLOSE_TIME

def is_market_regular_session(dt: Optional[datetime] = None) -> bool:
    """
    Check if it's during regular market session (9:30-16:00 ET).
    
    Args:
        dt: DateTime to check (defaults to now in ET)
        
    Returns:
        True if during regular session
    """
    return is_market_open(dt)

def is_premarket(dt: Optional[datetime] = None) -> bool:
    """
    Check if it's during premarket hours (4:00-9:30 ET).
    
    Args:
        dt: DateTime to check (defaults to now in ET)
        
    Returns:
        True if during premarket
    """
    if dt is None:
        dt = now_et()
    else:
        dt = to_et(dt)
    
    # Check if it's a trading day
    if not is_trading_day(dt):
        return False
    
    # Check if within premarket hours
    return PREMARKET_START <= dt.time() < MARKET_OPEN_TIME

def is_postmarket(dt: Optional[datetime] = None) -> bool:
    """
    Check if it's during postmarket hours (16:00-20:00 ET).
    
    Args:
        dt: DateTime to check (defaults to now in ET)
        
    Returns:
        True if during postmarket
    """
    if dt is None:
        dt = now_et()
    else:
        dt = to_et(dt)
    
    # Check if it's a trading day
    if not is_trading_day(dt):
        return False
    
    # Check if within postmarket hours
    return MARKET_CLOSE_TIME < dt.time() <= POSTMARKET_END

def get_session_type(dt: Optional[datetime] = None) -> str:
    """
    Get the current market session type.
    
    Args:
        dt: DateTime to check (defaults to now in ET)
        
    Returns:
        'pre', 'regular', 'post', or 'closed'
    """
    if dt is None:
        dt = now_et()
    else:
        dt = to_et(dt)
    
    if not is_trading_day(dt):
        return 'closed'
    
    if is_premarket(dt):
        return 'pre'
    elif is_market_regular_session(dt):
        return 'regular'
    elif is_postmarket(dt):
        return 'post'
    else:
        return 'closed'

def current_day_id(dt: Optional[datetime] = None) -> str:
    """
    Get the current trading day ID in YYYY-MM-DD format (ET).
    
    Args:
        dt: DateTime to check (defaults to now in ET)
        
    Returns:
        Day ID string in YYYY-MM-DD format
    """
    if dt is None:
        dt = now_et()
    else:
        dt = to_et(dt)
    
    return dt.strftime('%Y-%m-%d')

def next_trading_day(date_et: datetime) -> datetime:
    """
    Get the next trading day after the given date.
    
    Args:
        date_et: Starting date in Eastern Time
        
    Returns:
        Next trading day
    """
    next_day = date_et + timedelta(days=1)
    
    while not is_trading_day(next_day):
        next_day += timedelta(days=1)
    
    return next_day

def prev_trading_day(date_et: datetime) -> datetime:
    """
    Get the previous trading day before the given date.
    
    Args:
        date_et: Starting date in Eastern Time
        
    Returns:
        Previous trading day
    """
    prev_day = date_et - timedelta(days=1)
    
    while not is_trading_day(prev_day):
        prev_day -= timedelta(days=1)
    
    return prev_day

def get_trading_days_range(start_date: datetime, end_date: datetime) -> list:
    """
    Get all trading days in a date range.
    
    Args:
        start_date: Start date (inclusive)
        end_date: End date (inclusive)
        
    Returns:
        List of trading days in the range
    """
    trading_days = []
    current_date = start_date
    
    while current_date <= end_date:
        if is_trading_day(current_date):
            trading_days.append(current_date)
        current_date += timedelta(days=1)
    
    return trading_days

def market_open_today() -> datetime:
    """Get market open time for today in ET."""
    today = now_et().date()
    return ET.localize(datetime.combine(today, MARKET_OPEN_TIME))

def market_close_today() -> datetime:
    """Get market close time for today in ET."""
    today = now_et().date()
    return ET.localize(datetime.combine(today, MARKET_CLOSE_TIME))

def time_until_market_open() -> Optional[timedelta]:
    """
    Get time until next market open.
    
    Returns:
        Timedelta until market open, or None if market is currently open
    """
    now = now_et()
    
    if is_market_open(now):
        return None
    
    # If after market close today, next open is tomorrow
    if now.time() > MARKET_CLOSE_TIME:
        next_open = next_trading_day(now)
        next_open = ET.localize(datetime.combine(next_open.date(), MARKET_OPEN_TIME))
    else:
        # Before market open today
        if is_trading_day(now):
            next_open = ET.localize(datetime.combine(now.date(), MARKET_OPEN_TIME))
        else:
            next_trading = next_trading_day(now)
            next_open = ET.localize(datetime.combine(next_trading.date(), MARKET_OPEN_TIME))
    
    return next_open - now

def time_until_market_close() -> Optional[timedelta]:
    """
    Get time until market close.
    
    Returns:
        Timedelta until market close, or None if market is closed
    """
    now = now_et()
    
    if not is_market_open(now):
        return None
    
    market_close = ET.localize(datetime.combine(now.date(), MARKET_CLOSE_TIME))
    return market_close - now

def is_valid_gapgo_time(dt: Optional[datetime] = None) -> bool:
    """
    Check if it's a valid time for Gap & Go strategy (>= 9:36 AM ET).
    
    Args:
        dt: DateTime to check (defaults to now in ET)
        
    Returns:
        True if it's a valid Gap & Go time
    """
    if dt is None:
        dt = now_et()
    else:
        dt = to_et(dt)
    
    return (is_market_regular_session(dt) and 
            dt.time() >= FIRST_VALID_TIME_FOR_GAPGO)

def is_orb_range_time(dt: Optional[datetime] = None) -> bool:
    """
    Check if it's within ORB range time (9:30-9:39 AM ET).
    
    Args:
        dt: DateTime to check (defaults to now in ET)
        
    Returns:
        True if within ORB range time
    """
    if dt is None:
        dt = now_et()
    else:
        dt = to_et(dt)
    
    return (is_market_regular_session(dt) and 
            ORB_RANGE_START <= dt.time() <= ORB_RANGE_END)

def format_market_time(dt: datetime) -> str:
    """
    Format datetime for market display (ET timezone).
    
    Args:
        dt: DateTime to format
        
    Returns:
        Formatted string in ET timezone
    """
    dt_et = to_et(dt)
    return dt_et.strftime('%Y-%m-%d %H:%M:%S ET')

def parse_market_time(time_str: str) -> datetime:
    """
    Parse market time string to datetime in ET.
    
    Args:
        time_str: Time string in format 'YYYY-MM-DD HH:MM:SS' or 'HH:MM:SS'
        
    Returns:
        Datetime in ET timezone
    """
    if ' ' in time_str and len(time_str.split(' ')[0]) == 10:
        # Full datetime
        dt = datetime.strptime(time_str, '%Y-%m-%d %H:%M:%S')
        return ET.localize(dt)
    else:
        # Time only - use today's date
        time_obj = datetime.strptime(time_str, '%H:%M:%S').time()
        today = now_et().date()
        return ET.localize(datetime.combine(today, time_obj))

# Export functions
__all__ = [
    'now_utc', 'now_et', 'to_et', 'to_utc',
    'is_trading_day', 'is_market_open', 'is_market_regular_session',
    'is_premarket', 'is_postmarket', 'get_session_type',
    'current_day_id', 'next_trading_day', 'prev_trading_day',
    'get_trading_days_range', 'market_open_today', 'market_close_today',
    'time_until_market_open', 'time_until_market_close',
    'is_valid_gapgo_time', 'is_orb_range_time',
    'format_market_time', 'parse_market_time',
    'FIRST_VALID_TIME_FOR_GAPGO', 'ORB_RANGE_START', 'ORB_RANGE_END'
]