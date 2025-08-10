"""
Market session detection and time utilities.

This module handles all time-related operations including:
- Market session detection
- Timezone handling
- Trading hours validation
"""

import logging
from datetime import datetime, time, timedelta
from typing import Tuple
import pytz

logger = logging.getLogger(__name__)


def detect_market_session() -> str:
    """
    Detect the current market session based on Eastern Time.

    Returns:
        Market session ('PRE-MARKET', 'REGULAR', 'AFTER-HOURS', 'CLOSED')
    """
    ny_tz = pytz.timezone("America/New_York")
    current_time = datetime.now(ny_tz)
    current_weekday = current_time.weekday()  # 0=Monday, 6=Sunday

    # Check if it's weekend
    if current_weekday >= 5:  # Saturday=5, Sunday=6
        return "CLOSED"

    # Get current time in minutes since midnight
    current_minutes = current_time.hour * 60 + current_time.minute

    # Market session times (in minutes since midnight ET)
    premarket_start = 4 * 60  # 4:00 AM
    regular_start = 9 * 60 + 30  # 9:30 AM
    regular_end = 16 * 60  # 4:00 PM
    afterhours_end = 20 * 60  # 8:00 PM

    if current_minutes < premarket_start:
        return "CLOSED"
    elif current_minutes < regular_start:
        return "PRE-MARKET"
    elif current_minutes < regular_end:
        return "REGULAR"
    elif current_minutes < afterhours_end:
        return "AFTER-HOURS"
    else:
        return "CLOSED"


def is_market_open() -> bool:
    """
    Check if the market is currently open (regular hours only).

    Returns:
        True if market is open during regular hours
    """
    return detect_market_session() == "REGULAR"


def is_extended_hours() -> bool:
    """
    Check if we're in extended hours (pre-market or after-hours).

    Returns:
        True if in extended trading hours
    """
    session = detect_market_session()
    return session in ["PRE-MARKET", "AFTER-HOURS"]


def is_weekend() -> bool:
    """
    Check if current day is weekend (Saturday or Sunday).

    Returns:
        True if it's weekend (Saturday=5, Sunday=6)
    """
    ny_tz = pytz.timezone("America/New_York")
    current_time = datetime.now(ny_tz)
    current_weekday = current_time.weekday()  # 0=Monday, 6=Sunday

    return current_weekday >= 5  # Saturday=5, Sunday=6


def get_last_market_day() -> datetime:
    """
    Get the last market day (trading day).

    Returns:
        Last market day as datetime
    """
    ny_tz = pytz.timezone("America/New_York")
    current = datetime.now(ny_tz)

    # Simple implementation - go back until we find a weekday
    while current.weekday() >= 5:  # Weekend
        current = current - timedelta(days=1)

    return current


def get_market_open_time(date: datetime = None) -> datetime:
    """
    Get market open time for a given date.

    Args:
        date: Date to get market open time for (defaults to today)

    Returns:
        Market open time (9:30 AM ET)
    """
    if date is None:
        date = datetime.now(pytz.timezone("America/New_York"))

    ny_tz = pytz.timezone("America/New_York")
    if date.tzinfo is None:
        date = ny_tz.localize(date)
    else:
        date = date.astimezone(ny_tz)

    return date.replace(hour=9, minute=30, second=0, microsecond=0)


def get_market_close_time(date: datetime = None) -> datetime:
    """
    Get market close time for a given date.

    Args:
        date: Date to get market close time for (defaults to today)

    Returns:
        Market close time (4:00 PM ET)
    """
    if date is None:
        date = datetime.now(pytz.timezone("America/New_York"))

    ny_tz = pytz.timezone("America/New_York")
    if date.tzinfo is None:
        date = ny_tz.localize(date)
    else:
        date = date.astimezone(ny_tz)

    return date.replace(hour=16, minute=0, second=0, microsecond=0)


def get_premarket_start_time(date: datetime = None) -> datetime:
    """
    Get pre-market start time for a given date.

    Args:
        date: Date to get pre-market start time for (defaults to today)

    Returns:
        Pre-market start time (4:00 AM ET)
    """
    if date is None:
        date = datetime.now(pytz.timezone("America/New_York"))

    ny_tz = pytz.timezone("America/New_York")
    if date.tzinfo is None:
        date = ny_tz.localize(date)
    else:
        date = date.astimezone(ny_tz)

    return date.replace(hour=4, minute=0, second=0, microsecond=0)


def get_afterhours_end_time(date: datetime = None) -> datetime:
    """
    Get after-hours end time for a given date.

    Args:
        date: Date to get after-hours end time for (defaults to today)

    Returns:
        After-hours end time (8:00 PM ET)
    """
    if date is None:
        date = datetime.now(pytz.timezone("America/New_York"))

    ny_tz = pytz.timezone("America/New_York")
    if date.tzinfo is None:
        date = ny_tz.localize(date)
    else:
        date = date.astimezone(ny_tz)

    return date.replace(hour=20, minute=0, second=0, microsecond=0)


def is_trading_day(date: datetime) -> bool:
    """
    Check if a given date is a trading day (weekday, no holidays).

    Note: This is a simple implementation that only checks weekdays.
    A production system should also check for market holidays.

    Args:
        date: Date to check

    Returns:
        True if it's a trading day
    """
    return date.weekday() < 5  # Monday=0, Friday=4


def time_until_market_open() -> timedelta:
    """
    Get time remaining until market opens.

    Returns:
        Timedelta until market opens, or zero if market is open
    """
    ny_tz = pytz.timezone("America/New_York")
    now = datetime.now(ny_tz)

    if is_market_open():
        return timedelta(0)

    # If it's weekend or after market close, calculate time to next Monday 9:30 AM
    if is_weekend() or now.hour >= 16:
        # Find next Monday
        days_until_monday = (7 - now.weekday()) % 7
        if days_until_monday == 0 and now.hour < 9:  # If it's Monday before market open
            days_until_monday = 0
        elif days_until_monday == 0:  # If it's Monday after market close
            days_until_monday = 7

        next_market_open = (now + timedelta(days=days_until_monday)).replace(
            hour=9, minute=30, second=0, microsecond=0
        )
    else:
        # Market opens today
        next_market_open = now.replace(hour=9, minute=30, second=0, microsecond=0)

    return next_market_open - now


def time_until_market_close() -> timedelta:
    """
    Get time remaining until market closes.

    Returns:
        Timedelta until market closes, or zero if market is closed
    """
    ny_tz = pytz.timezone("America/New_York")
    now = datetime.now(ny_tz)

    if not is_market_open():
        return timedelta(0)

    market_close = now.replace(hour=16, minute=0, second=0, microsecond=0)
    return market_close - now


def get_trading_minutes_elapsed_today() -> int:
    """
    Get the number of trading minutes elapsed since market open today.

    Returns:
        Minutes since market open, or 0 if market hasn't opened yet
    """
    ny_tz = pytz.timezone("America/New_York")
    now = datetime.now(ny_tz)

    if not is_trading_day(now):
        return 0

    market_open = get_market_open_time(now)

    if now < market_open:
        return 0

    elapsed = now - market_open
    return int(elapsed.total_seconds() / 60)


def format_market_time(dt: datetime) -> str:
    """
    Format a datetime for market display (Eastern Time).

    Args:
        dt: Datetime to format

    Returns:
        Formatted time string
    """
    ny_tz = pytz.timezone("America/New_York")

    if dt.tzinfo is None:
        dt = ny_tz.localize(dt)
    else:
        dt = dt.astimezone(ny_tz)

    return dt.strftime("%Y-%m-%d %H:%M:%S %Z")
