"""
Time and market calendar utilities.

This module provides timezone-aware time functions, market hours calculations,
and trading session logic for the trading system.
"""

import datetime
import os
from typing import Optional, Tuple, Union

import pandas as pd
import pytz

from utils.config import config

# Market timezone
MARKET_TZ = pytz.timezone(config.TIMEZONE)
UTC_TZ = pytz.UTC

# Environment variable for market timezone (for problem statement compatibility)
MARKET_TZ_NAME = os.getenv("MARKET_TZ", "America/New_York")


def get_market_time() -> datetime.datetime:
    """Get current time in market timezone."""
    return datetime.datetime.now(MARKET_TZ)


def get_utc_time() -> datetime.datetime:
    """Get current time in UTC."""
    return datetime.datetime.now(UTC_TZ)


def is_weekend() -> bool:
    """Check if current day is weekend."""
    current_time = get_market_time()
    return current_time.weekday() >= 5  # Saturday=5, Sunday=6


def is_market_hours() -> bool:
    """
    Check if current time is during regular market hours.
    
    Returns:
        True if market is open (9:30 AM - 4:00 PM ET on weekdays)
    """
    if is_weekend():
        return False
    
    current_time = get_market_time()
    market_open = current_time.replace(
        hour=config.MARKET_OPEN_HOUR,
        minute=config.MARKET_OPEN_MINUTE,
        second=0,
        microsecond=0,
    )
    market_close = current_time.replace(
        hour=config.MARKET_CLOSE_HOUR,
        minute=config.MARKET_CLOSE_MINUTE,
        second=0,
        microsecond=0,
    )
    
    return market_open <= current_time <= market_close


def get_market_open_time(date: Optional[datetime.date] = None) -> datetime.datetime:
    """
    Get market open time for a given date.
    
    Args:
        date: Date to get open time for (defaults to today)
        
    Returns:
        Market open datetime in market timezone
    """
    if date is None:
        date = get_market_time().date()
    
    return MARKET_TZ.localize(
        datetime.datetime.combine(
            date,
            datetime.time(config.MARKET_OPEN_HOUR, config.MARKET_OPEN_MINUTE),
        )
    )


def get_market_close_time(date: Optional[datetime.date] = None) -> datetime.datetime:
    """
    Get market close time for a given date.
    
    Args:
        date: Date to get close time for (defaults to today)
        
    Returns:
        Market close datetime in market timezone
    """
    if date is None:
        date = get_market_time().date()
    
    return MARKET_TZ.localize(
        datetime.datetime.combine(
            date,
            datetime.time(config.MARKET_CLOSE_HOUR, config.MARKET_CLOSE_MINUTE),
        )
    )


def get_premarket_window(date: Optional[datetime.date] = None) -> Tuple[datetime.datetime, datetime.datetime]:
    """
    Get premarket trading window (4:00 AM - 9:30 AM ET).
    
    Args:
        date: Date to get window for (defaults to today)
        
    Returns:
        Tuple of (premarket_start, premarket_end) in market timezone
    """
    if date is None:
        date = get_market_time().date()
    
    premarket_start = MARKET_TZ.localize(
        datetime.datetime.combine(date, datetime.time(4, 0))
    )
    premarket_end = get_market_open_time(date)
    
    return premarket_start, premarket_end


def get_afterhours_window(date: Optional[datetime.date] = None) -> Tuple[datetime.datetime, datetime.datetime]:
    """
    Get after-hours trading window (4:00 PM - 8:00 PM ET).
    
    Args:
        date: Date to get window for (defaults to today)
        
    Returns:
        Tuple of (afterhours_start, afterhours_end) in market timezone
    """
    if date is None:
        date = get_market_time().date()
    
    afterhours_start = get_market_close_time(date)
    afterhours_end = MARKET_TZ.localize(
        datetime.datetime.combine(date, datetime.time(20, 0))
    )
    
    return afterhours_start, afterhours_end


def get_session_window(date: Optional[datetime.date] = None) -> Tuple[datetime.datetime, datetime.datetime]:
    """
    Get regular trading session window (9:30 AM - 4:00 PM ET).
    
    Args:
        date: Date to get window for (defaults to today)
        
    Returns:
        Tuple of (session_start, session_end) in market timezone
    """
    if date is None:
        date = get_market_time().date()
    
    return get_market_open_time(date), get_market_close_time(date)


def convert_to_utc(dt: datetime.datetime, from_tz: Optional[pytz.BaseTzInfo] = None) -> datetime.datetime:
    """
    Convert datetime to UTC.
    
    Args:
        dt: Datetime to convert
        from_tz: Source timezone (defaults to market timezone)
        
    Returns:
        UTC datetime
    """
    if from_tz is None:
        from_tz = MARKET_TZ
    
    if dt.tzinfo is None:
        dt = from_tz.localize(dt)
    
    return dt.astimezone(UTC_TZ)


def convert_from_utc(dt: datetime.datetime, to_tz: Optional[pytz.BaseTzInfo] = None) -> datetime.datetime:
    """
    Convert UTC datetime to target timezone.
    
    Args:
        dt: UTC datetime to convert
        to_tz: Target timezone (defaults to market timezone)
        
    Returns:
        Datetime in target timezone
    """
    if to_tz is None:
        to_tz = MARKET_TZ
    
    if dt.tzinfo is None:
        dt = UTC_TZ.localize(dt)
    
    return dt.astimezone(to_tz)


def get_trading_days_back(days: int, from_date: Optional[datetime.date] = None) -> list[datetime.date]:
    """
    Get list of trading days going back from a date.
    
    Args:
        days: Number of trading days to go back
        from_date: Starting date (defaults to today)
        
    Returns:
        List of trading dates in chronological order
    """
    if from_date is None:
        from_date = get_market_time().date()
    
    trading_days = []
    current_date = from_date
    
    while len(trading_days) < days:
        # Skip weekends (simple implementation - doesn't handle holidays)
        if current_date.weekday() < 5:  # Monday=0, Friday=4
            trading_days.append(current_date)
        current_date -= datetime.timedelta(days=1)
    
    return list(reversed(trading_days))


def is_in_session_window(
    timestamp: datetime.datetime,
    date: Optional[datetime.date] = None,
) -> bool:
    """
    Check if timestamp is within regular trading session.
    
    Args:
        timestamp: Timestamp to check (should be timezone-aware)
        date: Date of the session (defaults to timestamp date)
        
    Returns:
        True if timestamp is within session window
    """
    if date is None:
        # Convert to market time to get the date
        market_time = convert_from_utc(timestamp) if timestamp.tzinfo == UTC_TZ else timestamp
        date = market_time.date()
    
    session_start, session_end = get_session_window(date)
    
    # Convert timestamp to market timezone for comparison
    if timestamp.tzinfo != MARKET_TZ:
        if timestamp.tzinfo == UTC_TZ:
            timestamp = convert_from_utc(timestamp)
        else:
            # Assume naive timestamps are in market timezone
            timestamp = MARKET_TZ.localize(timestamp) if timestamp.tzinfo is None else timestamp
    
    return session_start <= timestamp <= session_end


def is_in_premarket_window(
    timestamp: datetime.datetime,
    date: Optional[datetime.date] = None,
) -> bool:
    """
    Check if timestamp is within premarket window.
    
    Args:
        timestamp: Timestamp to check (should be timezone-aware)
        date: Date of the session (defaults to timestamp date)
        
    Returns:
        True if timestamp is within premarket window
    """
    if date is None:
        # Convert to market time to get the date
        market_time = convert_from_utc(timestamp) if timestamp.tzinfo == UTC_TZ else timestamp
        date = market_time.date()
    
    premarket_start, premarket_end = get_premarket_window(date)
    
    # Convert timestamp to market timezone for comparison
    if timestamp.tzinfo != MARKET_TZ:
        if timestamp.tzinfo == UTC_TZ:
            timestamp = convert_from_utc(timestamp)
        else:
            # Assume naive timestamps are in market timezone
            timestamp = MARKET_TZ.localize(timestamp) if timestamp.tzinfo is None else timestamp
    
    return premarket_start <= timestamp < premarket_end


def get_breakout_guard_time(date: Optional[datetime.date] = None) -> datetime.datetime:
    """
    Get the breakout guard time (09:36 AM ET) for Gap & Go strategy.
    
    Args:
        date: Date to get guard time for (defaults to today)
        
    Returns:
        Breakout guard time in market timezone
    """
    if date is None:
        date = get_market_time().date()
    
    return MARKET_TZ.localize(
        datetime.datetime.combine(
            date,
            datetime.time(9, 30 + config.BREAKOUT_TIME_GUARD_MINUTES),
        )
    )


def get_early_volume_window(date: Optional[datetime.date] = None) -> Tuple[datetime.datetime, datetime.datetime]:
    """
    Get early volume calculation window (09:30-09:44 AM ET) for Gap & Go.
    
    Args:
        date: Date to get window for (defaults to today)
        
    Returns:
        Tuple of (window_start, window_end) in market timezone
    """
    if date is None:
        date = get_market_time().date()
    
    window_start = get_market_open_time(date)
    window_end = MARKET_TZ.localize(
        datetime.datetime.combine(date, datetime.time(9, 44))
    )
    
    return window_start, window_end


def filter_market_hours(
    df: pd.DataFrame,
    timestamp_col: str = "timestamp",
    include_premarket: bool = True,
    include_afterhours: bool = True,
) -> pd.DataFrame:
    """
    Filter DataFrame to only include market hours data.
    
    Args:
        df: DataFrame with timestamp column
        timestamp_col: Name of timestamp column
        include_premarket: Whether to include premarket hours
        include_afterhours: Whether to include after-hours
        
    Returns:
        Filtered DataFrame
    """
    if df.empty or timestamp_col not in df.columns:
        return df
    
    # Ensure timestamp column is datetime
    df = df.copy()
    df[timestamp_col] = pd.to_datetime(df[timestamp_col], utc=True)
    
    # Create filter mask
    mask = pd.Series(False, index=df.index)
    
    for _, row in df.iterrows():
        timestamp = row[timestamp_col]
        date = convert_from_utc(timestamp).date()
        
        # Skip weekends
        if date.weekday() >= 5:
            continue
        
        # Check if in regular session
        if is_in_session_window(timestamp, date):
            mask.loc[row.name] = True
            continue
        
        # Check premarket if enabled
        if include_premarket and is_in_premarket_window(timestamp, date):
            mask.loc[row.name] = True
            continue
        
        # Check after-hours if enabled
        if include_afterhours:
            afterhours_start, afterhours_end = get_afterhours_window(date)
            if afterhours_start <= convert_from_utc(timestamp) <= afterhours_end:
                mask.loc[row.name] = True
    
    return df[mask].reset_index(drop=True)


def get_current_session_state() -> dict:
    """
    Get comprehensive information about current market session state.
    
    Returns:
        Dictionary with session state information
    """
    current_time = get_market_time()
    current_date = current_time.date()
    
    return {
        "current_time_et": current_time,
        "current_time_utc": get_utc_time(),
        "date": current_date,
        "is_weekend": is_weekend(),
        "is_market_hours": is_market_hours(),
        "is_premarket": is_in_premarket_window(current_time),
        "market_open": get_market_open_time(current_date),
        "market_close": get_market_close_time(current_date),
        "premarket_window": get_premarket_window(current_date),
        "afterhours_window": get_afterhours_window(current_date),
        "breakout_guard_time": get_breakout_guard_time(current_date),
        "early_volume_window": get_early_volume_window(current_date),
    }


# === New timezone utility functions for problem statement requirements ===

def utc_now() -> pd.Timestamp:
    """Return current timestamp in UTC timezone."""
    return pd.Timestamp.now(tz="UTC")


def as_utc(ts: Union[pd.Series, pd.DatetimeIndex]) -> Union[pd.Series, pd.DatetimeIndex]:
    """
    Convert timestamps to UTC timezone.
    
    If tz-naive: assume already UTC and localize to UTC
    If ET or other tz: convert to UTC
    
    Args:
        ts: Pandas Series or DatetimeIndex with timestamps
        
    Returns:
        Timestamps converted to UTC
    """
    if isinstance(ts, pd.Series):
        if getattr(ts.dt, "tz", None) is None:
            return ts.dt.tz_localize("UTC")
        return ts.dt.tz_convert("UTC")
    else:  # DatetimeIndex
        if getattr(ts, "tz", None) is None:
            return ts.tz_localize("UTC")
        return ts.tz_convert("UTC")


def to_market_tz(ts_utc: Union[pd.Series, pd.DatetimeIndex]) -> Union[pd.Series, pd.DatetimeIndex]:
    """
    Convert UTC timestamps to market timezone.
    
    Args:
        ts_utc: UTC timestamps
        
    Returns:
        Timestamps converted to market timezone
    """
    if isinstance(ts_utc, pd.Series):
        return ts_utc.dt.tz_convert(MARKET_TZ_NAME)
    else:  # DatetimeIndex
        return ts_utc.tz_convert(MARKET_TZ_NAME)


def parse_intraday_from_alpha_vantage(df: pd.DataFrame, col: str = "timestamp") -> pd.DataFrame:
    """
    Parse intraday timestamps from Alpha Vantage API.
    
    AV intraday timestamps are in US/Eastern (local market time) with no tz info.
    
    Args:
        df: DataFrame with timestamp column
        col: Name of timestamp column
        
    Returns:
        DataFrame with UTC timestamps
    """
    df = df.copy()
    ts = pd.to_datetime(df[col], errors="coerce")
    if getattr(ts.dt, "tz", None) is None:
        ts = ts.dt.tz_localize(MARKET_TZ_NAME, ambiguous="infer", nonexistent="shift_forward")
    ts = ts.dt.tz_convert("UTC")
    df[col] = ts
    return df


def parse_daily_from_alpha_vantage(df: pd.DataFrame, col: str = "date") -> pd.DataFrame:
    """
    Parse daily timestamps from Alpha Vantage API.
    
    Daily dates are calendar days; store as UTC midnight.
    
    Args:
        df: DataFrame with date column  
        col: Name of date column
        
    Returns:
        DataFrame with UTC timestamps
    """
    df = df.copy()
    ts = pd.to_datetime(df[col], errors="coerce")
    if getattr(ts.dt, "tz", None) is None:
        ts = ts.dt.tz_localize("UTC")
    else:
        ts = ts.dt.tz_convert("UTC")
    df[col] = ts.dt.normalize()  # 00:00 UTC
    return df