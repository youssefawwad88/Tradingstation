"""
Abstract base classes and interfaces for the trading system.

This module defines the core interfaces that provide contracts for different
components of the trading system, enabling loose coupling and easier testing.
"""

from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Tuple, Any
import pandas as pd


class DataFetcher(ABC):
    """Abstract interface for data fetching operations."""

    @abstractmethod
    async def fetch_intraday_data(
        self, ticker: str, interval: str = "1min", outputsize: str = "compact"
    ) -> Tuple[Optional[pd.DataFrame], bool]:
        """
        Fetch intraday data for a ticker.

        Args:
            ticker: Stock ticker symbol
            interval: Time interval for data
            outputsize: Size of data to fetch

        Returns:
            Tuple of (DataFrame or None, success boolean)
        """
        pass

    @abstractmethod
    async def fetch_daily_data(
        self, ticker: str, outputsize: str = "compact"
    ) -> Tuple[Optional[pd.DataFrame], bool]:
        """
        Fetch daily data for a ticker.

        Args:
            ticker: Stock ticker symbol
            outputsize: Size of data to fetch

        Returns:
            Tuple of (DataFrame or None, success boolean)
        """
        pass

    @abstractmethod
    async def fetch_multiple_tickers(
        self,
        tickers: List[str],
        data_type: str = "intraday",
        interval: str = "1min",
        max_concurrent: int = 5,
    ) -> Dict[str, Tuple[Optional[pd.DataFrame], bool]]:
        """
        Fetch data for multiple tickers concurrently.

        Args:
            tickers: List of ticker symbols
            data_type: Type of data to fetch
            interval: Time interval for intraday data
            max_concurrent: Maximum concurrent requests

        Returns:
            Dictionary mapping ticker to (DataFrame, success) tuples
        """
        pass


class Screener(ABC):
    """Abstract interface for trading strategy screeners."""

    @property
    @abstractmethod
    def name(self) -> str:
        """Get the name of this screener."""
        pass

    @property
    @abstractmethod
    def description(self) -> str:
        """Get a description of this screener's strategy."""
        pass

    @abstractmethod
    async def scan(self, tickers: List[str], **kwargs) -> List[Dict[str, Any]]:
        """
        Scan tickers for trading opportunities.

        Args:
            tickers: List of ticker symbols to scan
            **kwargs: Additional parameters for the screener

        Returns:
            List of trade signals/opportunities
        """
        pass

    @abstractmethod
    def validate_signal(self, signal: Dict[str, Any]) -> bool:
        """
        Validate a trading signal.

        Args:
            signal: Trading signal to validate

        Returns:
            True if signal is valid
        """
        pass
