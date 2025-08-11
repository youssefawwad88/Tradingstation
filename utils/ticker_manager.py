"""
Ticker list management and operations.

This module handles all ticker-related operations including:
- Loading ticker lists from various sources
- Validating ticker symbols
- Managing manual and automated ticker lists
"""

import os
import logging
from typing import List, Optional
import pandas as pd

from .config import DEFAULT_TICKERS
from .data_storage import read_df_from_s3

logger = logging.getLogger(__name__)


def load_manual_tickers() -> List[str]:
    """
    Load manual ticker list from tickerlist.txt with enhanced diagnostics.
    Checks MULTIPLE POSSIBLE LOCATIONS and provides detailed error logging.

    Returns:
        List of manual tickers
    """
    # Find all potential tickerlist.txt files
    paths_to_check = [
        "/workspace/tickerlist.txt",
        "/workspace/data/tickerlist.txt",
        os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            "tickerlist.txt",
        ),
        "tickerlist.txt",
        "../tickerlist.txt",
    ]

    logger.info(
        "🔍 LOAD_MANUAL_TICKERS: Checking multiple possible locations for tickerlist.txt..."
    )

    for path in paths_to_check:
        logger.info(f"   Checking path: {path}")
        if os.path.exists(path):
            try:
                logger.info(f"✅ FOUND tickerlist.txt at: {path}")
                with open(path, "r") as f:
                    content = f.read().strip()
                    tickers = [
                        line.strip() for line in content.split("\n") if line.strip()
                    ]

                # Print the first 5 tickers found
                logger.info(f"📋 File contents summary:")
                logger.info(f"   Total tickers found: {len(tickers)}")
                logger.info(f"   First 5 tickers: {tickers[:5]}")
                if len(tickers) > 5:
                    logger.info(f"   Additional tickers: {tickers[5:]}")

                logger.info(
                    f"✅ Successfully loaded {len(tickers)} manual tickers from {path}"
                )
                return tickers

            except Exception as e:
                logger.error(f"❌ Error reading tickerlist.txt from {path}: {e}")
                continue
        else:
            logger.info(f"   ❌ Not found: {path}")

    # Fallback: try manual_tickers.txt for backwards compatibility
    manual_file_alt = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "manual_tickers.txt",
    )
    logger.info(f"🔄 Trying fallback: {manual_file_alt}")
    if os.path.exists(manual_file_alt):
        try:
            with open(manual_file_alt, "r") as f:
                tickers = [line.strip() for line in f.readlines() if line.strip()]
            logger.info(
                f"✅ Loaded {len(tickers)} manual tickers from manual_tickers.txt (fallback)"
            )
            logger.info(f"   First 5 tickers: {tickers[:5]}")
            return tickers
        except Exception as e:
            logger.error(
                f"❌ Error reading manual tickers from manual_tickers.txt: {e}"
            )

    # Final fallback to default tickers
    logger.warning("⚠️ No manual ticker file found in any location!")
    logger.warning(
        "   This indicates a configuration issue - tickerlist.txt should exist"
    )
    logger.warning("   Falling back to DEFAULT_TICKERS from config")

    logger.info(f"📋 Using DEFAULT_TICKERS: {DEFAULT_TICKERS[:5]} (first 5)")
    return DEFAULT_TICKERS


def read_tickerlist_from_s3(filename: str) -> List[str]:
    """
    Read ticker list from S3/Spaces or local fallback.

    Args:
        filename: Name of the ticker file

    Returns:
        List of ticker symbols
    """
    logger.info(f"Reading ticker list from {filename}")

    # Try to read from local file first as fallback
    local_file = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))), filename
    )
    if os.path.exists(local_file):
        try:
            with open(local_file, "r") as f:
                tickers = [line.strip() for line in f.readlines() if line.strip()]
            logger.info(f"Successfully read {len(tickers)} tickers from local file")
            return tickers
        except Exception as e:
            logger.error(f"Error reading local ticker file: {e}")

    # Fallback to default tickers from config
    logger.warning(f"Using default tickers: {DEFAULT_TICKERS}")
    return DEFAULT_TICKERS


def read_master_tickerlist() -> List[str]:
    """
    Read master ticker list from master_tickerlist.csv (local or Spaces).
    This is the SINGLE SOURCE OF TRUTH for all fetchers as per unified instructions.

    Returns:
        List of ticker symbols from master list
    """
    logger.info("Reading master ticker list from master_tickerlist.csv (SINGLE SOURCE OF TRUTH)")

    try:
        # Try local file first
        local_file = os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            "master_tickerlist.csv",
        )
        if os.path.exists(local_file):
            df = pd.read_csv(local_file)
            if "ticker" in df.columns:
                tickers = df["ticker"].tolist()
                logger.info(
                    f"✅ Successfully read {len(tickers)} tickers from master_tickerlist.csv (LOCAL)"
                )
                return tickers

        # Try to read from Spaces
        df = read_df_from_s3("master_tickerlist.csv")
        if not df.empty and "ticker" in df.columns:
            tickers = df["ticker"].tolist()
            logger.info(
                f"✅ Successfully read {len(tickers)} tickers from master_tickerlist.csv (SPACES)"
            )
            return tickers

        # If master_tickerlist.csv doesn't exist, create it from defaults
        logger.warning(
            "⚠️ master_tickerlist.csv not found - creating from DEFAULT_TICKERS"
        )
        
        # Create master tickerlist from defaults
        default_df = pd.DataFrame({
            'ticker': DEFAULT_TICKERS,
            'source': ['default'] * len(DEFAULT_TICKERS),
            'generated_at': [pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')] * len(DEFAULT_TICKERS)
        })
        
        # Save to local file
        default_df.to_csv(local_file, index=False)
        logger.info(f"✅ Created master_tickerlist.csv with {len(DEFAULT_TICKERS)} default tickers")
        
        return DEFAULT_TICKERS

    except Exception as e:
        logger.error(f"❌ Error reading master ticker list: {e}")
        logger.warning(f"🔄 Using DEFAULT_TICKERS as emergency fallback: {DEFAULT_TICKERS}")
        return DEFAULT_TICKERS


def validate_ticker_symbol(ticker: str) -> bool:
    """
    Validate a ticker symbol format.

    Args:
        ticker: Ticker symbol to validate

    Returns:
        True if valid ticker format
    """
    if not ticker or not isinstance(ticker, str):
        return False

    # Basic validation: 1-6 characters (allowing for special cases like BRK.B)
    ticker = ticker.strip().upper()
    if len(ticker) < 1 or len(ticker) > 6:
        return False

    # Allow letters and dots (for cases like BRK.B)
    # But ensure it doesn't start or end with a dot
    if ticker.startswith(".") or ticker.endswith("."):
        return False

    # Check if all characters are letters or dots, and no consecutive dots
    import re

    return bool(re.match(r"^[A-Z]+(\.[A-Z]+)?$", ticker))


def clean_ticker_list(tickers: List[str]) -> List[str]:
    """
    Clean and validate a list of ticker symbols.

    Args:
        tickers: List of ticker symbols to clean

    Returns:
        List of valid, cleaned ticker symbols
    """
    cleaned = []
    for ticker in tickers:
        if not ticker:
            continue

        # Clean the ticker
        clean_ticker = ticker.strip().upper()

        # Validate
        if validate_ticker_symbol(clean_ticker):
            if clean_ticker not in cleaned:  # Avoid duplicates
                cleaned.append(clean_ticker)
        else:
            logger.warning(f"Invalid ticker symbol skipped: {ticker}")

    return cleaned


def save_ticker_list(tickers: List[str], filename: str) -> bool:
    """
    Save a ticker list to a local file.

    Args:
        tickers: List of ticker symbols to save
        filename: Name of the file to save to

    Returns:
        True if successful
    """
    try:
        filepath = os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))), filename
        )

        with open(filepath, "w") as f:
            for ticker in tickers:
                f.write(f"{ticker}\n")

        logger.info(f"Saved {len(tickers)} tickers to {filepath}")
        return True

    except Exception as e:
        logger.error(f"Error saving ticker list to {filename}: {e}")
        return False


def get_sp500_tickers() -> List[str]:
    """
    Get S&P 500 ticker list (placeholder implementation).

    In a production system, this would fetch from a reliable source
    like Wikipedia or a financial data provider.

    Returns:
        List of S&P 500 ticker symbols
    """
    # This is a placeholder implementation
    # In production, you would fetch this from a reliable source
    logger.warning(
        "Using placeholder S&P 500 list - implement proper fetching in production"
    )

    # Sample of major S&P 500 tickers
    return [
        "AAPL",
        "MSFT",
        "AMZN",
        "GOOGL",
        "META",
        "TSLA",
        "NVDA",
        "BRK.B",
        "UNH",
        "JNJ",
        "JPM",
        "V",
        "PG",
        "XOM",
        "HD",
        "CVX",
        "MA",
        "PFE",
        "BAC",
        "ABBV",
        "KO",
        "AVGO",
        "LLY",
        "PEP",
        "TMO",
        "COST",
        "WMT",
        "MRK",
        "ADBE",
        "DIS",
        "ABT",
        "ACN",
        "VZ",
        "NFLX",
        "CRM",
        "CMCSA",
        "INTC",
        "NKE",
        "TXN",
        "QCOM",
        "AMD",
        "DHR",
        "MDT",
        "PM",
        "UPS",
        "T",
        "LOW",
        "AMGN",
        "BMY",
        "ORCL",
        "HON",
        "COP",
        "SPGI",
        "RTX",
    ]


def merge_ticker_sources(
    manual_tickers: List[str], sp500_tickers: List[str]
) -> List[str]:
    """
    Merge ticker lists from multiple sources, prioritizing manual tickers.

    Args:
        manual_tickers: Manually specified tickers (always included)
        sp500_tickers: S&P 500 tickers (filtered based on criteria)

    Returns:
        Combined list of tickers
    """
    # Start with manual tickers (always included)
    combined = clean_ticker_list(manual_tickers)

    # Add S&P 500 tickers that aren't already in the list
    for ticker in clean_ticker_list(sp500_tickers):
        if ticker not in combined:
            combined.append(ticker)

    logger.info(
        f"Merged ticker lists: {len(manual_tickers)} manual + {len(sp500_tickers)} S&P 500 = {len(combined)} total"
    )

    return combined


def filter_tickers_by_criteria(
    tickers: List[str], min_price: float = 5.0, max_price: float = 1000.0
) -> List[str]:
    """
    Filter tickers based on various criteria (placeholder implementation).

    Args:
        tickers: List of tickers to filter
        min_price: Minimum stock price
        max_price: Maximum stock price

    Returns:
        Filtered list of tickers
    """
    # This is a placeholder implementation
    # In production, you would fetch current prices and apply real filters
    logger.warning(
        "Using placeholder ticker filtering - implement real criteria in production"
    )

    # For now, just return the input list
    return tickers
