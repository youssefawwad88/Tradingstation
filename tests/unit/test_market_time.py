"""
Unit tests for market_time module.
"""

from datetime import datetime, timedelta
from unittest.mock import patch

import pytest
import pytz

from utils.market_time import (
    detect_market_session,
    get_last_market_day,
    get_market_close_time,
    get_market_open_time,
    get_trading_minutes_elapsed_today,
    is_extended_hours,
    is_market_open,
    is_trading_day,
    is_weekend,
    time_until_market_open,
)


class TestMarketTime:
    """Test cases for market time functions."""

    @patch("utils.market_time.datetime")
    def test_detect_market_session_weekend(self, mock_datetime):
        """Test market session detection on weekend."""
        # Mock Saturday
        ny_tz = pytz.timezone("America/New_York")
        mock_datetime.now.return_value = ny_tz.localize(
            datetime(2025, 1, 4, 10, 0)
        )  # Saturday

        session = detect_market_session()
        assert session == "CLOSED"

    @patch("utils.market_time.datetime")
    def test_detect_market_session_premarket(self, mock_datetime):
        """Test market session detection during pre-market hours."""
        # Mock Monday 8:00 AM ET
        ny_tz = pytz.timezone("America/New_York")
        mock_datetime.now.return_value = ny_tz.localize(
            datetime(2025, 1, 6, 8, 0)
        )  # Monday

        session = detect_market_session()
        assert session == "PRE-MARKET"

    @patch("utils.market_time.datetime")
    def test_detect_market_session_regular(self, mock_datetime):
        """Test market session detection during regular hours."""
        # Mock Monday 10:00 AM ET
        ny_tz = pytz.timezone("America/New_York")
        mock_datetime.now.return_value = ny_tz.localize(
            datetime(2025, 1, 6, 10, 0)
        )  # Monday

        session = detect_market_session()
        assert session == "REGULAR"

    @patch("utils.market_time.datetime")
    def test_detect_market_session_afterhours(self, mock_datetime):
        """Test market session detection during after-hours."""
        # Mock Monday 5:00 PM ET
        ny_tz = pytz.timezone("America/New_York")
        mock_datetime.now.return_value = ny_tz.localize(
            datetime(2025, 1, 6, 17, 0)
        )  # Monday

        session = detect_market_session()
        assert session == "AFTER-HOURS"

    @patch("utils.market_time.detect_market_session")
    def test_is_market_open_true(self, mock_detect):
        """Test is_market_open returns True during regular hours."""
        mock_detect.return_value = "REGULAR"
        assert is_market_open() is True

    @patch("utils.market_time.detect_market_session")
    def test_is_market_open_false(self, mock_detect):
        """Test is_market_open returns False outside regular hours."""
        mock_detect.return_value = "PRE-MARKET"
        assert is_market_open() is False

    @patch("utils.market_time.detect_market_session")
    def test_is_extended_hours_true(self, mock_detect):
        """Test is_extended_hours returns True during extended hours."""
        mock_detect.return_value = "PRE-MARKET"
        assert is_extended_hours() is True

        mock_detect.return_value = "AFTER-HOURS"
        assert is_extended_hours() is True

    @patch("utils.market_time.detect_market_session")
    def test_is_extended_hours_false(self, mock_detect):
        """Test is_extended_hours returns False during regular/closed hours."""
        mock_detect.return_value = "REGULAR"
        assert is_extended_hours() is False

        mock_detect.return_value = "CLOSED"
        assert is_extended_hours() is False

    @patch("utils.market_time.datetime")
    def test_is_weekend_true(self, mock_datetime):
        """Test is_weekend returns True on weekends."""
        ny_tz = pytz.timezone("America/New_York")
        mock_datetime.now.return_value = ny_tz.localize(
            datetime(2025, 1, 4, 10, 0)
        )  # Saturday
        assert is_weekend() is True

        mock_datetime.now.return_value = ny_tz.localize(
            datetime(2025, 1, 5, 10, 0)
        )  # Sunday
        assert is_weekend() is True

    @patch("utils.market_time.datetime")
    def test_is_weekend_false(self, mock_datetime):
        """Test is_weekend returns False on weekdays."""
        ny_tz = pytz.timezone("America/New_York")
        mock_datetime.now.return_value = ny_tz.localize(
            datetime(2025, 1, 6, 10, 0)
        )  # Monday
        assert is_weekend() is False

    def test_is_trading_day(self):
        """Test is_trading_day function."""
        # Monday should be a trading day
        monday = datetime(2025, 1, 6)  # Monday
        assert is_trading_day(monday) is True

        # Friday should be a trading day
        friday = datetime(2025, 1, 10)  # Friday
        assert is_trading_day(friday) is True

        # Saturday should not be a trading day
        saturday = datetime(2025, 1, 4)  # Saturday
        assert is_trading_day(saturday) is False

        # Sunday should not be a trading day
        sunday = datetime(2025, 1, 5)  # Sunday
        assert is_trading_day(sunday) is False

    def test_get_market_open_time(self):
        """Test get_market_open_time function."""
        ny_tz = pytz.timezone("America/New_York")
        test_date = ny_tz.localize(datetime(2025, 1, 6, 15, 30))  # Monday 3:30 PM

        market_open = get_market_open_time(test_date)

        assert market_open.hour == 9
        assert market_open.minute == 30
        assert market_open.second == 0
        assert market_open.tzinfo.zone == "America/New_York"

    def test_get_market_close_time(self):
        """Test get_market_close_time function."""
        ny_tz = pytz.timezone("America/New_York")
        test_date = ny_tz.localize(datetime(2025, 1, 6, 10, 30))  # Monday 10:30 AM

        market_close = get_market_close_time(test_date)

        assert market_close.hour == 16
        assert market_close.minute == 0
        assert market_close.second == 0
        assert market_close.tzinfo.zone == "America/New_York"

    @patch("utils.market_time.is_market_open")
    def test_time_until_market_open_already_open(self, mock_is_open):
        """Test time_until_market_open when market is already open."""
        mock_is_open.return_value = True

        time_remaining = time_until_market_open()
        assert time_remaining == timedelta(0)

    @patch("utils.market_time.is_market_open")
    @patch("utils.market_time.is_trading_day")
    def test_get_trading_minutes_elapsed_today(self, mock_is_trading_day, mock_is_open):
        """Test get_trading_minutes_elapsed_today function."""
        mock_is_trading_day.return_value = False

        minutes = get_trading_minutes_elapsed_today()
        assert minutes == 0
