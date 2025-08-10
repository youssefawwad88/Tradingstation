"""
Unit tests for the core architecture components.
"""

from unittest.mock import Mock, AsyncMock
import pytest
import pandas as pd

from core.interfaces import Screener, DataFetcher
from core.di_container import DIContainer, injectable
from core.plugins import PluginRegistry, screener_plugin
from core.base_screener import BaseScreener


class MockDataFetcher:
    """Mock data fetcher for testing."""

    async def fetch_multiple_tickers(self, tickers, **kwargs):
        # Return mock data for all tickers
        results = {}
        for ticker in tickers:
            df = pd.DataFrame(
                {
                    "datetime": pd.date_range(
                        "2025-01-01 09:30", periods=10, freq="1min"
                    ),
                    "open": [100, 101, 102, 103, 104, 105, 106, 107, 108, 109],
                    "high": [101, 102, 103, 104, 105, 106, 107, 108, 109, 110],
                    "low": [99, 100, 101, 102, 103, 104, 105, 106, 107, 108],
                    "close": [
                        100.5,
                        101.5,
                        102.5,
                        103.5,
                        104.5,
                        105.5,
                        106.5,
                        107.5,
                        108.5,
                        109.5,
                    ],
                    "volume": [
                        1000,
                        1100,
                        1200,
                        1300,
                        1400,
                        1500,
                        1600,
                        1700,
                        1800,
                        1900,
                    ],
                }
            )
            results[ticker] = (df, True)
        return results

    async def fetch_intraday_data(self, ticker, **kwargs):
        df = pd.DataFrame(
            {
                "datetime": pd.date_range("2025-01-01 09:30", periods=10, freq="1min"),
                "open": [100, 101, 102, 103, 104, 105, 106, 107, 108, 109],
                "close": [
                    100.5,
                    101.5,
                    102.5,
                    103.5,
                    104.5,
                    105.5,
                    106.5,
                    107.5,
                    108.5,
                    109.5,
                ],
                "volume": [1000, 1100, 1200, 1300, 1400, 1500, 1600, 1700, 1800, 1900],
            }
        )
        return df, True

    async def fetch_daily_data(self, ticker, **kwargs):
        df = pd.DataFrame(
            {
                "Date": pd.date_range("2025-01-01", periods=5),
                "close": [98, 99, 100, 101, 102],
            }
        )
        return df, True


class TestDIContainer:
    """Test cases for dependency injection container."""

    def test_register_and_get_singleton(self):
        """Test singleton registration and retrieval."""
        container = DIContainer()
        mock_service = Mock()

        container.register_singleton(Mock, mock_service)
        retrieved = container.get(Mock)

        assert retrieved is mock_service

    def test_register_and_get_factory(self):
        """Test factory registration and retrieval."""
        container = DIContainer()

        def factory():
            return Mock(test_attr="factory_created")

        container.register_factory(Mock, factory)
        retrieved = container.get(Mock)

        assert retrieved.test_attr == "factory_created"

    def test_register_class(self):
        """Test class registration with auto-instantiation."""
        container = DIContainer()

        class TestService:
            def __init__(self):
                self.created = True

        container.register_class(TestService, TestService)
        retrieved = container.get(TestService)

        assert isinstance(retrieved, TestService)
        assert retrieved.created is True

    def test_injectable_decorator(self):
        """Test the injectable decorator."""
        # Use the global container for this test
        from core.di_container import get_container
        container = get_container()
        container.clear()  # Clear any existing registrations
        
        mock_dependency = Mock()
        container.register_singleton(Mock, mock_dependency)

        @injectable
        class TestClass:
            def __init__(self, dependency: Mock):
                self.dependency = dependency

        instance = TestClass()
        assert instance.dependency is mock_dependency

    def test_service_not_found(self):
        """Test error when service not found."""
        container = DIContainer()

        with pytest.raises(ValueError, match="Service not registered"):
            container.get(Mock)


class TestPluginRegistry:
    """Test cases for plugin registry."""

    def test_register_screener(self):
        """Test screener registration."""
        registry = PluginRegistry()

        class TestScreener(Screener):
            @property
            def name(self):
                return "test"

            @property
            def description(self):
                return "Test screener"

            async def scan(self, tickers, **kwargs):
                return []

            def validate_signal(self, signal):
                return True

        registry.register_screener(TestScreener)
        assert "testscreener" in registry.list_screeners()

    def test_get_screener(self):
        """Test screener retrieval."""
        registry = PluginRegistry()

        class TestScreener(Screener):
            @property
            def name(self):
                return "test"

            @property
            def description(self):
                return "Test screener"

            async def scan(self, tickers, **kwargs):
                return []

            def validate_signal(self, signal):
                return True

        registry.register_screener(TestScreener, name="test")
        screener = registry.get_screener("test")

        assert isinstance(screener, TestScreener)
        assert screener.name == "test"

    def test_screener_plugin_decorator(self):
        """Test screener plugin decorator."""

        @screener_plugin(name="decorated", version="1.0")
        class DecoratedScreener(Screener):
            @property
            def name(self):
                return "decorated"

            @property
            def description(self):
                return "Decorated screener"

            async def scan(self, tickers, **kwargs):
                return []

            def validate_signal(self, signal):
                return True

        assert hasattr(DecoratedScreener, "__plugin_metadata__")
        assert DecoratedScreener.__plugin_metadata__["name"] == "decorated"
        assert DecoratedScreener.__plugin_metadata__["version"] == "1.0"


class TestBaseScreener:
    """Test cases for base screener."""

    @pytest.mark.asyncio
    async def test_base_screener_scan(self):
        """Test base screener scan functionality."""
        mock_fetcher = MockDataFetcher()

        class TestScreener(BaseScreener):
            def __init__(self):
                super().__init__(data_fetcher=mock_fetcher)

            async def screen_ticker(self, ticker, df, **kwargs):
                if ticker == "AAPL":
                    return self.create_signal(
                        ticker=ticker,
                        signal_type="buy",
                        entry_price=100.0,
                        stop_loss=95.0,
                        take_profit=110.0,
                    )
                return None

        screener = TestScreener()

        # Mock market session
        from unittest.mock import patch
        with patch(
            "core.base_screener.detect_market_session", return_value="REGULAR"
        ):
            signals = await screener.scan(["AAPL", "MSFT"])

        assert len(signals) == 1
        assert signals[0]["ticker"] == "AAPL"
        assert signals[0]["signal_type"] == "buy"

    def test_validate_signal(self):
        """Test signal validation."""
        screener = BaseScreener(data_fetcher=MockDataFetcher())

        valid_signal = {
            "ticker": "AAPL",
            "signal_type": "buy",
            "timestamp": "2025-01-01T10:00:00",
            "entry_price": 100.0,
        }

        invalid_signal = {
            "ticker": "AAPL",
            "signal_type": "buy",
            # Missing required fields
        }

        assert screener.validate_signal(valid_signal) is True
        assert screener.validate_signal(invalid_signal) is False

    def test_create_signal(self):
        """Test signal creation."""
        screener = BaseScreener(data_fetcher=MockDataFetcher())

        signal = screener.create_signal(
            ticker="AAPL",
            signal_type="buy",
            entry_price=100.0,
            stop_loss=95.0,
            take_profit=110.0,
        )

        assert signal["ticker"] == "AAPL"
        assert signal["signal_type"] == "buy"
        assert signal["entry_price"] == 100.0
        assert signal["stop_loss"] == 95.0
        assert signal["take_profit"] == 110.0
        assert "risk_reward_ratio" in signal
        assert signal["risk_reward_ratio"] == 2.0  # (110-100)/(100-95)


if __name__ == "__main__":
    pytest.main([__file__])
