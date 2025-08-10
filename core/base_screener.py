"""
Base screener implementation with common functionality.

This module provides a base class for all trading screeners with common
utilities and a standardized interface.
"""

import logging
from typing import Dict, List, Any, Optional
from datetime import datetime
import pandas as pd

from core.interfaces import Screener, DataFetcher
from core.di_container import injectable, inject
from utils.market_time import detect_market_session, is_market_open

logger = logging.getLogger(__name__)


@injectable
class BaseScreener(Screener):
    """Base implementation for trading screeners."""

    def __init__(self, data_fetcher: Optional[DataFetcher] = None):
        self.data_fetcher = data_fetcher or inject(DataFetcher)
        self._last_scan_time: Optional[datetime] = None
        self._cached_data: Dict[str, pd.DataFrame] = {}

    @property
    def name(self) -> str:
        """Default name based on class name."""
        return self.__class__.__name__.lower().replace("screener", "")

    @property
    def description(self) -> str:
        """Default description."""
        return f"{self.name.title()} trading strategy screener"

    async def scan(self, tickers: List[str], **kwargs) -> List[Dict[str, Any]]:
        """
        Default scan implementation that fetches data and calls screen_ticker.

        Args:
            tickers: List of ticker symbols to scan
            **kwargs: Additional parameters

        Returns:
            List of signals found
        """
        logger.info(f"Starting {self.name} scan for {len(tickers)} tickers")

        self._last_scan_time = datetime.now()
        signals = []

        # Check market session
        session = detect_market_session()
        if not self.should_run_in_session(session):
            logger.info(
                f"Skipping {self.name} scan - inappropriate market session: {session}"
            )
            return signals

        # Fetch data for all tickers
        try:
            data_results = await self.data_fetcher.fetch_multiple_tickers(
                tickers=tickers,
                data_type=self.get_data_type(),
                interval=self.get_interval(),
                max_concurrent=kwargs.get("max_concurrent", 5),
            )
        except Exception as e:
            logger.error(f"Error fetching data for {self.name} scan: {e}")
            return signals

        # Screen each ticker
        for ticker, (df, success) in data_results.items():
            if not success or df is None or df.empty:
                logger.debug(f"No data available for {ticker}")
                continue

            try:
                signal = await self.screen_ticker(ticker, df, **kwargs)
                if signal and self.validate_signal(signal):
                    signals.append(signal)
            except Exception as e:
                logger.error(f"Error screening {ticker}: {e}")

        logger.info(f"{self.name} scan complete: {len(signals)} signals found")
        return signals

    async def screen_ticker(
        self, ticker: str, df: pd.DataFrame, **kwargs
    ) -> Optional[Dict[str, Any]]:
        """
        Screen a single ticker for opportunities.

        This method should be implemented by concrete screeners.

        Args:
            ticker: Ticker symbol
            df: Price data DataFrame
            **kwargs: Additional parameters

        Returns:
            Signal dictionary if opportunity found, None otherwise
        """
        raise NotImplementedError("Subclasses must implement screen_ticker")

    def validate_signal(self, signal: Dict[str, Any]) -> bool:
        """
        Validate a trading signal.

        Args:
            signal: Signal to validate

        Returns:
            True if signal is valid
        """
        required_fields = ["ticker", "signal_type", "timestamp", "entry_price"]

        for field in required_fields:
            if field not in signal:
                logger.warning(f"Signal missing required field: {field}")
                return False

        # Validate numeric fields
        try:
            float(signal["entry_price"])
        except (ValueError, TypeError):
            logger.warning(
                f"Invalid entry_price in signal: {signal.get('entry_price')}"
            )
            return False

        return True

    def should_run_in_session(self, session: str) -> bool:
        """
        Check if screener should run in the given market session.

        Args:
            session: Market session ('PRE-MARKET', 'REGULAR', 'AFTER-HOURS', 'CLOSED')

        Returns:
            True if screener should run
        """
        # By default, only run during regular market hours
        return session == "REGULAR"

    def get_data_type(self) -> str:
        """
        Get the type of data needed by this screener.

        Returns:
            Data type ('intraday' or 'daily')
        """
        return "intraday"

    def get_interval(self) -> str:
        """
        Get the data interval needed by this screener.

        Returns:
            Data interval ('1min', '5min', '15min', '30min', '60min')
        """
        return "1min"

    def calculate_entry_price(self, df: pd.DataFrame) -> float:
        """
        Calculate entry price based on current data.

        Args:
            df: Price data DataFrame

        Returns:
            Entry price
        """
        if df.empty:
            return 0.0

        # Use latest close price as default
        return float(df["close"].iloc[-1])

    def calculate_stop_loss(
        self, entry_price: float, df: pd.DataFrame, **kwargs
    ) -> float:
        """
        Calculate stop loss level.

        Args:
            entry_price: Entry price for the trade
            df: Price data DataFrame
            **kwargs: Additional parameters

        Returns:
            Stop loss price
        """
        # Default: 2% below entry for long positions
        return entry_price * 0.98

    def calculate_take_profit(
        self, entry_price: float, stop_loss: float, **kwargs
    ) -> float:
        """
        Calculate take profit level.

        Args:
            entry_price: Entry price for the trade
            stop_loss: Stop loss price
            **kwargs: Additional parameters

        Returns:
            Take profit price
        """
        # Default: 2:1 risk/reward ratio
        risk = abs(entry_price - stop_loss)
        return entry_price + (2 * risk)

    def create_signal(
        self,
        ticker: str,
        signal_type: str,
        entry_price: float,
        stop_loss: Optional[float] = None,
        take_profit: Optional[float] = None,
        **additional_data,
    ) -> Dict[str, Any]:
        """
        Create a standardized signal dictionary.

        Args:
            ticker: Ticker symbol
            signal_type: Type of signal (e.g., 'buy', 'sell')
            entry_price: Entry price
            stop_loss: Stop loss price
            take_profit: Take profit price
            **additional_data: Additional signal data

        Returns:
            Signal dictionary
        """
        signal = {
            "ticker": ticker,
            "signal_type": signal_type,
            "timestamp": datetime.now().isoformat(),
            "entry_price": entry_price,
            "screener": self.name,
            **additional_data,
        }

        if stop_loss is not None:
            signal["stop_loss"] = stop_loss

        if take_profit is not None:
            signal["take_profit"] = take_profit

            # Calculate risk/reward ratio
            if stop_loss is not None:
                risk = abs(entry_price - stop_loss)
                reward = abs(take_profit - entry_price)
                if risk > 0:
                    signal["risk_reward_ratio"] = reward / risk

        return signal

    def get_last_scan_time(self) -> Optional[datetime]:
        """Get the timestamp of the last scan."""
        return self._last_scan_time

    def clear_cache(self) -> None:
        """Clear any cached data."""
        self._cached_data.clear()
        logger.debug(f"Cleared cache for {self.name} screener")
