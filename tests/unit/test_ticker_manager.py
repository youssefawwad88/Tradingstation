"""
Unit tests for ticker_manager module.
"""

import pytest
from unittest.mock import patch, mock_open
import tempfile
import os

from utils.ticker_manager import (
    validate_ticker_symbol,
    clean_ticker_list,
    load_manual_tickers,
    merge_ticker_sources,
    get_sp500_tickers,
    save_ticker_list,
)


class TestTickerManager:
    """Test cases for ticker manager functions."""

    def test_validate_ticker_symbol_valid(self):
        """Test validation of valid ticker symbols."""
        valid_tickers = ["AAPL", "MSFT", "GOOGL", "A", "TSLA", "BRK.B", "CUSTOM"]

        for ticker in valid_tickers:
            assert validate_ticker_symbol(ticker) is True

    def test_validate_ticker_symbol_invalid(self):
        """Test validation of invalid ticker symbols."""
        invalid_tickers = [
            "",  # Empty string
            None,  # None value
            "TOOLONG",  # Too long
            "123",  # Numbers only
            "AA1",  # Mixed letters and numbers
            "A-B",  # Special characters
            "   ",  # Whitespace only
        ]

        for ticker in invalid_tickers:
            assert validate_ticker_symbol(ticker) is False

    def test_validate_ticker_symbol_case_handling(self):
        """Test that ticker validation handles case properly."""
        assert validate_ticker_symbol("aapl") is True
        assert validate_ticker_symbol("AAPL") is True
        assert validate_ticker_symbol("ApPl") is True

    def test_clean_ticker_list(self):
        """Test cleaning and validation of ticker lists."""
        dirty_list = [
            "AAPL",  # Valid, uppercase
            "msft",  # Valid, lowercase
            "googl  ",  # Valid with whitespace
            "  TSLA",  # Valid with leading whitespace
            "",  # Empty string
            "TOOLONG",  # Too long
            "AA1",  # Invalid characters
            "NVDA",  # Valid
            "AAPL",  # Duplicate
        ]

        cleaned = clean_ticker_list(dirty_list)

        expected = ["AAPL", "MSFT", "GOOGL", "TSLA", "NVDA"]
        assert cleaned == expected

    def test_merge_ticker_sources(self):
        """Test merging ticker lists from multiple sources."""
        manual_tickers = ["AAPL", "MSFT", "CUSTOM"]
        sp500_tickers = ["AAPL", "GOOGL", "TSLA", "MSFT"]  # Some overlap

        merged = merge_ticker_sources(manual_tickers, sp500_tickers)

        # Should have all unique tickers, manual first
        expected = ["AAPL", "MSFT", "CUSTOM", "GOOGL", "TSLA"]
        assert merged == expected

    def test_get_sp500_tickers(self):
        """Test S&P 500 ticker list retrieval."""
        tickers = get_sp500_tickers()

        assert isinstance(tickers, list)
        assert len(tickers) > 0
        assert "AAPL" in tickers
        assert "MSFT" in tickers

        # All should be valid ticker symbols
        for ticker in tickers:
            assert validate_ticker_symbol(ticker)

    @patch("builtins.open", new_callable=mock_open, read_data="AAPL\nMSFT\nGOOGL\n")
    @patch("os.path.exists")
    def test_load_manual_tickers_success(self, mock_exists, mock_file):
        """Test successful loading of manual tickers."""
        mock_exists.return_value = True

        tickers = load_manual_tickers()

        assert tickers == ["AAPL", "MSFT", "GOOGL"]

    @patch("os.path.exists")
    def test_load_manual_tickers_file_not_found(self, mock_exists):
        """Test loading manual tickers when file not found."""
        mock_exists.return_value = False

        tickers = load_manual_tickers()

        # Should return default tickers
        assert isinstance(tickers, list)
        assert len(tickers) > 0

    def test_save_ticker_list(self, tmp_path):
        """Test saving ticker list to file."""
        tickers = ["AAPL", "MSFT", "GOOGL"]
        test_file = tmp_path / "test_tickers.txt"

        # Mock the file path calculation to use our temp file
        with patch("utils.ticker_manager.os.path.join", return_value=str(test_file)):
            success = save_ticker_list(tickers, "test_tickers.txt")

        assert success is True
        assert test_file.exists()

        # Verify file contents
        with open(test_file, "r") as f:
            saved_tickers = [line.strip() for line in f.readlines()]

        assert saved_tickers == tickers

    @patch("builtins.open", side_effect=IOError("Permission denied"))
    @patch("utils.ticker_manager.os.path.join")
    def test_save_ticker_list_error(self, mock_join, mock_open):
        """Test error handling when saving ticker list fails."""
        mock_join.return_value = "fake_path.txt"
        tickers = ["AAPL", "MSFT"]

        success = save_ticker_list(tickers, "test.txt")

        assert success is False
