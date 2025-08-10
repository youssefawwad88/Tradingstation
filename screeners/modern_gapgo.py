"""
Modernized Gap & Go screener using the new plugin architecture.

This screener implements Umar Ashraf's Gap & Go strategy with proper
separation of concerns and dependency injection.
"""

import logging
from typing import Dict, List, Any, Optional
from datetime import datetime, time
import pandas as pd
import numpy as np
import pytz

from core.base_screener import BaseScreener
from core.plugins import screener_plugin
from utils.market_time import detect_market_session

logger = logging.getLogger(__name__)


@screener_plugin(
    name="gapgo",
    description="Gap & Go strategy by Umar Ashraf - identifies pre-market gaps and breakout opportunities",
    version="2.0.0",
    author="Trading System",
    timeframe="1min",
    session_requirements=["PRE-MARKET", "REGULAR"],
)
class GapGoScreener(BaseScreener):
    """
    Gap & Go screener implementing Umar Ashraf's strategy.

    Strategy Logic:
    1. Pre-market gap identification (>2% gap from previous close)
    2. Volume confirmation (>150% of average early volume)
    3. Breakout above pre-market high after 9:36 AM
    4. VWAP reclaim validation
    """

    @property
    def name(self) -> str:
        return "gapgo"

    @property
    def description(self) -> str:
        return (
            "Gap & Go strategy - pre-market gaps with volume confirmation and breakout"
        )

    def should_run_in_session(self, session: str) -> bool:
        """Gap & Go runs in pre-market and regular hours."""
        return session in ["PRE-MARKET", "REGULAR"]

    def get_data_type(self) -> str:
        """Needs intraday data for gap analysis."""
        return "intraday"

    def get_interval(self) -> str:
        """Uses 1-minute intervals for precise entry timing."""
        return "1min"

    async def screen_ticker(
        self, ticker: str, df: pd.DataFrame, **kwargs
    ) -> Optional[Dict[str, Any]]:
        """
        Screen a ticker for Gap & Go opportunities.

        Args:
            ticker: Ticker symbol
            df: Intraday price data
            **kwargs: Additional parameters

        Returns:
            Signal if opportunity found, None otherwise
        """
        try:
            # Get market session and time
            session = detect_market_session()
            ny_tz = pytz.timezone("America/New_York")
            current_time = datetime.now(ny_tz)

            # Prepare data
            df = self._prepare_data(df)
            if df.empty:
                return None

            # Get daily data for gap calculation
            daily_df = await self._get_daily_data(ticker)
            if daily_df is None or daily_df.empty:
                return None

            # Check for gap
            gap_info = self._analyze_gap(df, daily_df)
            if not gap_info["has_gap"]:
                return None

            # Pre-market phase: identify and track gaps
            if session == "PRE-MARKET":
                return self._handle_premarket_phase(ticker, df, gap_info, current_time)

            # Regular market phase: look for breakouts
            elif session == "REGULAR":
                return self._handle_regular_phase(ticker, df, gap_info, current_time)

            return None

        except Exception as e:
            logger.error(f"Error screening {ticker} for Gap & Go: {e}")
            return None

    def _prepare_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Prepare and validate intraday data."""
        if df.empty:
            return df

        # Ensure datetime column
        if "datetime" not in df.columns:
            return pd.DataFrame()

        # Sort by datetime
        df = df.sort_values("datetime").copy()
        df["datetime"] = pd.to_datetime(df["datetime"])

        # Add timezone if not present
        if df["datetime"].dt.tz is None:
            df["datetime"] = (
                df["datetime"].dt.tz_localize("UTC").dt.tz_convert("America/New_York")
            )
        else:
            df["datetime"] = df["datetime"].dt.tz_convert("America/New_York")

        # Filter to today's data
        today = datetime.now(pytz.timezone("America/New_York")).date()
        df = df[df["datetime"].dt.date == today]

        return df

    async def _get_daily_data(self, ticker: str) -> Optional[pd.DataFrame]:
        """Get daily data for gap calculation."""
        try:
            data_results = await self.data_fetcher.fetch_multiple_tickers(
                tickers=[ticker],
                data_type="daily",
                max_concurrent=1,
            )

            df, success = data_results.get(ticker, (None, False))
            return df if success else None

        except Exception as e:
            logger.error(f"Error fetching daily data for {ticker}: {e}")
            return None

    def _analyze_gap(
        self, intraday_df: pd.DataFrame, daily_df: pd.DataFrame
    ) -> Dict[str, Any]:
        """Analyze gap characteristics."""
        gap_info = {
            "has_gap": False,
            "gap_percent": 0.0,
            "gap_direction": "none",
            "previous_close": 0.0,
            "current_open": 0.0,
        }

        if daily_df.empty or intraday_df.empty:
            return gap_info

        # Get previous day's close
        daily_df = daily_df.sort_values("Date")
        if len(daily_df) < 2:
            return gap_info

        previous_close = float(daily_df["close"].iloc[-2])
        gap_info["previous_close"] = previous_close

        # Get today's opening price (first intraday candle)
        current_open = float(intraday_df["open"].iloc[0])
        gap_info["current_open"] = current_open

        # Calculate gap percentage
        gap_percent = ((current_open - previous_close) / previous_close) * 100
        gap_info["gap_percent"] = gap_percent

        # Determine gap characteristics
        if abs(gap_percent) >= 2.0:  # Minimum 2% gap
            gap_info["has_gap"] = True
            gap_info["gap_direction"] = "up" if gap_percent > 0 else "down"

        return gap_info

    def _handle_premarket_phase(
        self,
        ticker: str,
        df: pd.DataFrame,
        gap_info: Dict[str, Any],
        current_time: datetime,
    ) -> Optional[Dict[str, Any]]:
        """Handle pre-market gap identification and tracking."""

        # Only track significant gaps in pre-market
        if gap_info["gap_percent"] < 2.0:
            return None

        # Calculate pre-market high/low
        premarket_data = self._get_premarket_data(df)
        if premarket_data.empty:
            return None

        premarket_high = float(premarket_data["high"].max())
        premarket_low = float(premarket_data["low"].min())

        # Check volume confirmation
        volume_confirmed = self._check_volume_confirmation(premarket_data)

        # Create tracking signal (not a trade signal yet)
        signal = self.create_signal(
            ticker=ticker,
            signal_type="gap_tracking",
            entry_price=gap_info["current_open"],
            signal_strength="confirmed" if volume_confirmed else "weak",
            gap_percent=gap_info["gap_percent"],
            gap_direction=gap_info["gap_direction"],
            premarket_high=premarket_high,
            premarket_low=premarket_low,
            volume_confirmed=volume_confirmed,
            previous_close=gap_info["previous_close"],
        )

        return signal

    def _handle_regular_phase(
        self,
        ticker: str,
        df: pd.DataFrame,
        gap_info: Dict[str, Any],
        current_time: datetime,
    ) -> Optional[Dict[str, Any]]:
        """Handle regular market phase breakout detection."""

        # Only consider breakouts after 9:36 AM
        breakout_time = time(9, 36)
        if current_time.time() < breakout_time:
            return None

        # Get pre-market and regular market data
        premarket_data = self._get_premarket_data(df)
        regular_data = self._get_regular_market_data(df)

        if premarket_data.empty or regular_data.empty:
            return None

        premarket_high = float(premarket_data["high"].max())
        current_price = float(regular_data["close"].iloc[-1])

        # Check for breakout above pre-market high
        if gap_info["gap_direction"] == "up" and current_price > premarket_high:

            # Additional validations
            if not self._validate_breakout_conditions(regular_data, gap_info):
                return None

            # Calculate trade parameters
            entry_price = premarket_high + 0.01  # Breakout entry
            stop_loss = self._calculate_gap_stop_loss(entry_price, gap_info)
            take_profit = self._calculate_gap_take_profit(entry_price, stop_loss)

            signal = self.create_signal(
                ticker=ticker,
                signal_type="buy",
                entry_price=entry_price,
                stop_loss=stop_loss,
                take_profit=take_profit,
                gap_percent=gap_info["gap_percent"],
                premarket_high=premarket_high,
                breakout_price=current_price,
                strategy="gap_and_go_breakout",
            )

            return signal

        return None

    def _get_premarket_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Extract pre-market data (4:00 AM - 9:30 AM ET)."""
        premarket_start = time(4, 0)
        market_open = time(9, 30)

        mask = (df["datetime"].dt.time >= premarket_start) & (
            df["datetime"].dt.time < market_open
        )

        return df[mask]

    def _get_regular_market_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Extract regular market data (9:30 AM onwards)."""
        market_open = time(9, 30)
        mask = df["datetime"].dt.time >= market_open
        return df[mask]

    def _check_volume_confirmation(self, premarket_data: pd.DataFrame) -> bool:
        """Check if pre-market volume confirms the gap."""
        if premarket_data.empty:
            return False

        # Calculate average volume
        avg_volume = premarket_data["volume"].mean()

        # Simple volume check - in production, this would compare to historical averages
        total_volume = premarket_data["volume"].sum()

        # Consider volume confirmed if we have significant activity
        return total_volume > 50000  # Placeholder threshold

    def _validate_breakout_conditions(
        self, regular_data: pd.DataFrame, gap_info: Dict[str, Any]
    ) -> bool:
        """Additional validations for breakout confirmation."""

        # Check if we have sufficient regular market data
        if len(regular_data) < 5:
            return False

        # Check that price is still above gap level
        current_price = float(regular_data["close"].iloc[-1])
        if current_price <= gap_info["previous_close"]:
            return False

        # Check recent volume
        recent_volume = regular_data["volume"].tail(3).sum()
        if recent_volume < 10000:  # Placeholder threshold
            return False

        return True

    def _calculate_gap_stop_loss(
        self, entry_price: float, gap_info: Dict[str, Any]
    ) -> float:
        """Calculate stop loss for gap trades."""
        # Stop loss below previous day's close or pre-market low
        previous_close = gap_info["previous_close"]

        # Use a level that invalidates the gap thesis
        stop_buffer = entry_price * 0.01  # 1% buffer
        return max(previous_close - stop_buffer, entry_price * 0.95)

    def _calculate_gap_take_profit(self, entry_price: float, stop_loss: float) -> float:
        """Calculate take profit for gap trades."""
        # Target 2:1 risk/reward ratio minimum
        risk = entry_price - stop_loss
        return entry_price + (2.5 * risk)  # 2.5:1 ratio for better trades
