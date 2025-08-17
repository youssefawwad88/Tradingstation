"""
Data fetching and API operations.

This module handles all data fetching operations including:
- Alpha Vantage API interactions
- Data validation and processing
- API rate limiting and error handling
- Timestamp standardization per requirements
"""

import json
import logging
import time
from typing import Any, Dict, Optional, Tuple

import pandas as pd
import requests

from .config import ALPHA_VANTAGE_API_KEY
from .timestamp_standardizer import apply_timestamp_standardization_to_api_data

logger = logging.getLogger(__name__)


def fetch_intraday_data(
    ticker: str, interval: str = "1min", outputsize: str = "compact"
) -> Tuple[Optional[pd.DataFrame], bool]:
    """
    Fetch intraday data from Alpha Vantage API.

    Args:
        ticker: Stock ticker symbol
        interval: Time interval between data points (1min, 5min, 15min, 30min, 60min)
        outputsize: 'compact' returns latest 100 data points, 'full' returns up to 20+ years

    Returns:
        Tuple of (DataFrame or None, success boolean)
    """
    if not ALPHA_VANTAGE_API_KEY:
        logger.error("Alpha Vantage API key not found in environment variables")
        return None, False

    endpoint = "https://www.alphavantage.co/query"
    params = {
        "function": "TIME_SERIES_INTRADAY",
        "symbol": ticker,
        "interval": interval,
        "outputsize": outputsize,
        "apikey": ALPHA_VANTAGE_API_KEY,
    }

    try:
        response = requests.get(endpoint, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()

        if "Error Message" in data:
            logger.error(
                f"Alpha Vantage API error for {ticker}: {data['Error Message']}"
            )
            return None, False

        if "Note" in data:
            logger.warning(f"Alpha Vantage rate limit hit for {ticker}: {data['Note']}")
            return None, False

        time_series_key = f"Time Series ({interval})"
        if time_series_key not in data:
            logger.error(
                f"Unexpected response format for {ticker}: {json.dumps(data)[:200]}..."
            )
            return None, False

        time_series = data[time_series_key]

        # Convert to DataFrame
        df = pd.DataFrame.from_dict(time_series, orient="index")

        # Rename columns
        df.columns = [col.split(". ")[1] for col in df.columns]

        # Convert values to float
        for col in df.columns:
            df[col] = df[col].astype(float)

        # Add date and ticker columns
        df.index = pd.to_datetime(df.index)
        df = df.reset_index()
        df.rename(columns={"index": "timestamp"}, inplace=True)
        df["ticker"] = ticker

        logger.info(
            f"📊 Raw intraday data fetched for {ticker} ({interval}): {len(df)} rows"
        )

        # Apply rigorous timestamp standardization
        df = apply_timestamp_standardization_to_api_data(df, data_type="intraday")

        logger.info(
            f"✅ Intraday data standardized for {ticker} ({interval}): {len(df)} records with UTC timestamps"
        )
        return df, True

    except requests.RequestException as e:
        logger.error(f"Network error fetching data for {ticker}: {e}")
        return None, False
    except (KeyError, ValueError) as e:
        logger.error(f"Data parsing error for {ticker}: {e}")
        return None, False
    except Exception as e:
        logger.error(f"Unexpected error fetching intraday data for {ticker}: {e}")
        return None, False


def fetch_daily_data(
    ticker: str, outputsize: str = "compact"
) -> Tuple[Optional[pd.DataFrame], bool]:
    """
    Fetch daily data from Alpha Vantage API.

    Args:
        ticker: Stock ticker symbol
        outputsize: 'compact' returns latest 100 data points, 'full' returns up to 20+ years

    Returns:
        Tuple of (DataFrame or None, success boolean)
    """
    if not ALPHA_VANTAGE_API_KEY:
        logger.error("Alpha Vantage API key not found in environment variables")
        return None, False

    endpoint = "https://www.alphavantage.co/query"
    params = {
        "function": "TIME_SERIES_DAILY",
        "symbol": ticker,
        "outputsize": outputsize,
        "apikey": ALPHA_VANTAGE_API_KEY,
    }

    try:
        response = requests.get(endpoint, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()

        if "Error Message" in data:
            logger.error(
                f"Alpha Vantage API error for {ticker}: {data['Error Message']}"
            )
            return None, False

        if "Note" in data:
            logger.warning(f"Alpha Vantage rate limit hit for {ticker}: {data['Note']}")
            return None, False

        time_series_key = "Time Series (Daily)"
        if time_series_key not in data:
            logger.error(
                f"Unexpected response format for {ticker}: {json.dumps(data)[:200]}..."
            )
            return None, False

        time_series = data[time_series_key]

        # Convert to DataFrame
        df = pd.DataFrame.from_dict(time_series, orient="index")

        # Rename columns
        df.columns = [col.split(". ")[1] for col in df.columns]

        # Convert values to float
        for col in df.columns:
            df[col] = df[col].astype(float)

        # Add date and ticker columns
        df.index = pd.to_datetime(df.index)
        df = df.reset_index()
        df.rename(columns={"index": "timestamp"}, inplace=True)
        df["ticker"] = ticker

        logger.info(f"📊 Raw daily data fetched for {ticker}: {len(df)} rows")

        # Apply rigorous timestamp standardization
        df = apply_timestamp_standardization_to_api_data(df, data_type="daily")

        logger.info(
            f"✅ Daily data standardized for {ticker}: {len(df)} records with UTC timestamps"
        )
        return df, True

    except requests.RequestException as e:
        logger.error(f"Network error fetching daily data for {ticker}: {e}")
        return None, False
    except (KeyError, ValueError) as e:
        logger.error(f"Data parsing error for {ticker}: {e}")
        return None, False
    except Exception as e:
        logger.error(f"Unexpected error fetching daily data for {ticker}: {e}")
        return None, False


def validate_api_response(data: Dict[str, Any], ticker: str) -> bool:
    """
    Validate Alpha Vantage API response for common error conditions.

    Args:
        data: JSON response from API
        ticker: Ticker symbol for logging

    Returns:
        True if response is valid, False otherwise
    """
    if "Error Message" in data:
        logger.error(f"Alpha Vantage API error for {ticker}: {data['Error Message']}")
        return False

    if "Note" in data:
        logger.warning(f"Alpha Vantage rate limit hit for {ticker}: {data['Note']}")
        return False

    if "Information" in data:
        logger.warning(
            f"Alpha Vantage info message for {ticker}: {data['Information']}"
        )
        return False

    return True


def get_api_rate_limit_delay() -> float:
    """
    Get the recommended delay between API calls to avoid rate limiting.

    Returns:
        Delay in seconds
    """
    # Alpha Vantage allows 5 calls per minute for free tier
    # So we should wait at least 12 seconds between calls
    return 12.0
