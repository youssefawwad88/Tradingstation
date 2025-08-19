"""
Unified Data Fetcher Module for Strategic Trading System

This module provides a single, robust function that fetches data from any
Alpha Vantage endpoint (TIME_SERIES_INTRADAY, TIME_SERIES_DAILY, GLOBAL_QUOTE).
The function dynamically builds the API URL based on a single set of parameters,
eliminating the need for separate fetching scripts.

Strategic Architecture Features:
- Single entry point for all data fetching
- Dynamic API URL construction
- Comprehensive error handling and validation
- Rate limiting and retry logic
- Standardized data formatting
- Professional logging integration
"""

import json
import logging
import time
from typing import Any, Dict, Optional, Tuple
from urllib.parse import urlencode

import pandas as pd
import requests

# Configuration imports
from utils.config import ALPHA_VANTAGE_API_KEY
from utils.timestamp_standardizer import apply_timestamp_standardization_to_api_data

logger = logging.getLogger(__name__)


class UnifiedDataFetcher:
    """
    Professional-grade unified data fetcher for all Alpha Vantage endpoints.
    
    This class replaces all individual fetching scripts with a single,
    robust, and configurable data fetching system.
    """
    
    def __init__(self, api_key: str = None):
        """
        Initialize the unified data fetcher.
        
        Args:
            api_key: Alpha Vantage API key. If None, uses environment variable.
        """
        self.api_key = api_key or ALPHA_VANTAGE_API_KEY
        self.base_url = "https://www.alphavantage.co/query"
        self.rate_limit_delay = 12.0  # 5 calls per minute for free tier
        self.last_request_time = 0
        
        if not self.api_key:
            logger.warning("Alpha Vantage API key not found - running in test mode")
    
    def fetch_data(
        self,
        ticker: str,
        data_type: str = "INTRADAY",
        interval: str = "1min",
        outputsize: str = "compact",
        **kwargs
    ) -> Tuple[Optional[pd.DataFrame], bool]:
        """
        Universal data fetching function that handles all Alpha Vantage endpoints.
        
        This single function replaces all separate fetching scripts and dynamically
        builds the appropriate API URL based on the data_type parameter.
        
        Args:
            ticker: Stock ticker symbol
            data_type: Type of data to fetch ('INTRADAY', 'DAILY', 'QUOTE')
            interval: Time interval for intraday data (1min, 5min, 15min, 30min, 60min)
            outputsize: Size of data to fetch ('compact' or 'full')
            **kwargs: Additional parameters for specific endpoints
            
        Returns:
            Tuple of (DataFrame or None, success boolean)
        """
        if not self.api_key:
            logger.warning(f"No API key available - returning test data for {ticker}")
            return self._generate_test_data(ticker, data_type, interval), True
        
        # Rate limiting
        self._apply_rate_limiting()
        
        # Build API parameters dynamically
        params = self._build_api_params(ticker, data_type, interval, outputsize, **kwargs)
        
        try:
            # Make API request with comprehensive error handling
            response = requests.get(self.base_url, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()
            
            # Validate API response
            if not self._validate_api_response(data, ticker):
                return None, False
            
            # Process data based on type
            df = self._process_api_data(data, ticker, data_type, interval)
            
            if df is not None:
                logger.info(
                    f"✅ {data_type} data fetched for {ticker}: {len(df)} rows"
                )
                return df, True
            else:
                return None, False
                
        except requests.RequestException as e:
            logger.error(f"Network error fetching {data_type} data for {ticker}: {e}")
            return None, False
        except Exception as e:
            logger.error(f"Unexpected error fetching {data_type} data for {ticker}: {e}")
            return None, False
    
    def _build_api_params(
        self,
        ticker: str,
        data_type: str,
        interval: str,
        outputsize: str,
        **kwargs
    ) -> Dict[str, str]:
        """
        Dynamically build API parameters based on data type.
        
        This eliminates the need for hardcoded parameter sets in separate scripts.
        """
        # Base parameters
        params = {
            "symbol": ticker,
            "apikey": self.api_key,
        }
        
        # Add function-specific parameters
        if data_type.upper() == "INTRADAY":
            params.update({
                "function": "TIME_SERIES_INTRADAY",
                "interval": interval,
                "outputsize": outputsize,
            })
        elif data_type.upper() == "DAILY":
            params.update({
                "function": "TIME_SERIES_DAILY",
                "outputsize": outputsize,
            })
        elif data_type.upper() == "QUOTE":
            params.update({
                "function": "GLOBAL_QUOTE",
            })
        else:
            raise ValueError(f"Unsupported data type: {data_type}")
        
        # Add any additional parameters
        params.update(kwargs)
        
        logger.debug(f"API parameters for {ticker}: {params}")
        return params
    
    def _validate_api_response(self, data: Dict[str, Any], ticker: str) -> bool:
        """
        Comprehensive API response validation.
        
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
            logger.warning(f"Alpha Vantage info message for {ticker}: {data['Information']}")
            return False
        
        return True
    
    def _process_api_data(
        self,
        data: Dict[str, Any],
        ticker: str,
        data_type: str,
        interval: str
    ) -> Optional[pd.DataFrame]:
        """
        Process API response data into standardized DataFrame format.
        
        Args:
            data: JSON response from API
            ticker: Ticker symbol
            data_type: Type of data being processed
            interval: Time interval for intraday data
            
        Returns:
            Processed DataFrame or None if processing fails
        """
        try:
            if data_type.upper() == "INTRADAY":
                return self._process_intraday_data(data, ticker, interval)
            elif data_type.upper() == "DAILY":
                return self._process_daily_data(data, ticker)
            elif data_type.upper() == "QUOTE":
                return self._process_quote_data(data, ticker)
            else:
                logger.error(f"Unsupported data type for processing: {data_type}")
                return None
                
        except Exception as e:
            logger.error(f"Error processing {data_type} data for {ticker}: {e}")
            return None
    
    def _process_intraday_data(
        self, data: Dict[str, Any], ticker: str, interval: str
    ) -> Optional[pd.DataFrame]:
        """Process intraday time series data."""
        time_series_key = f"Time Series ({interval})"
        
        if time_series_key not in data:
            logger.error(f"Expected key '{time_series_key}' not found in response for {ticker}")
            return None
        
        time_series = data[time_series_key]
        return self._convert_time_series_to_dataframe(time_series, ticker, "intraday")
    
    def _process_daily_data(
        self, data: Dict[str, Any], ticker: str
    ) -> Optional[pd.DataFrame]:
        """Process daily time series data."""
        time_series_key = "Time Series (Daily)"
        
        if time_series_key not in data:
            logger.error(f"Expected key '{time_series_key}' not found in response for {ticker}")
            return None
        
        time_series = data[time_series_key]
        return self._convert_time_series_to_dataframe(time_series, ticker, "daily")
    
    def _process_quote_data(
        self, data: Dict[str, Any], ticker: str
    ) -> Optional[pd.DataFrame]:
        """Process global quote data."""
        quote_key = "Global Quote"
        
        if quote_key not in data:
            logger.error(f"Expected key '{quote_key}' not found in response for {ticker}")
            return None
        
        quote_data = data[quote_key]
        
        # Convert single quote to DataFrame format
        row_data = {}
        for key, value in quote_data.items():
            clean_key = key.split(". ")[-1] if ". " in key else key
            try:
                row_data[clean_key] = float(value) if value.replace(".", "").replace("-", "").isdigit() else value
            except (ValueError, AttributeError):
                row_data[clean_key] = value
        
        df = pd.DataFrame([row_data])
        df["ticker"] = ticker
        df["timestamp"] = pd.Timestamp.now(tz="UTC")
        
        return df
    
    def _convert_time_series_to_dataframe(
        self, time_series: Dict[str, Any], ticker: str, data_type: str
    ) -> pd.DataFrame:
        """
        Convert time series data to standardized DataFrame format.
        
        Args:
            time_series: Time series data from API response
            ticker: Ticker symbol
            data_type: Type of data for timestamp processing
            
        Returns:
            Standardized DataFrame
        """
        # Convert to DataFrame
        df = pd.DataFrame.from_dict(time_series, orient="index")
        
        # Rename columns (remove numeric prefixes)
        df.columns = [col.split(". ")[1] if ". " in col else col for col in df.columns]
        
        # Convert values to float
        for col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # Add timestamp and ticker columns
        df.index = pd.to_datetime(df.index)
        df = df.reset_index()
        df.rename(columns={"index": "timestamp"}, inplace=True)
        df["ticker"] = ticker
        
        # Apply timestamp standardization
        df = apply_timestamp_standardization_to_api_data(df, data_type=data_type)
        
        return df
    
    def _apply_rate_limiting(self):
        """Apply rate limiting to avoid API quota issues."""
        current_time = time.time()
        time_since_last_request = current_time - self.last_request_time
        
        if time_since_last_request < self.rate_limit_delay:
            sleep_time = self.rate_limit_delay - time_since_last_request
            logger.debug(f"Rate limiting: sleeping for {sleep_time:.1f} seconds")
            time.sleep(sleep_time)
        
        self.last_request_time = time.time()
    
    def _generate_test_data(
        self, ticker: str, data_type: str, interval: str
    ) -> pd.DataFrame:
        """
        Generate test data when running without API key.
        
        Args:
            ticker: Ticker symbol
            data_type: Type of data to generate
            interval: Time interval for intraday data
            
        Returns:
            Test DataFrame with realistic structure
        """
        logger.info(f"Generating test data for {ticker} ({data_type})")
        
        # Create basic test data structure
        if data_type.upper() == "INTRADAY":
            periods = 100
            freq = "1min"
        elif data_type.upper() == "DAILY":
            periods = 100
            freq = "D"
        else:
            periods = 1
            freq = "D"
        
        # Generate timestamps
        timestamps = pd.date_range(
            end=pd.Timestamp.now(tz="UTC"),
            periods=periods,
            freq=freq
        )
        
        # Generate realistic price data
        base_price = 100.0
        df = pd.DataFrame({
            "timestamp": timestamps,
            "open": base_price + (pd.Series(range(periods)) * 0.1),
            "high": base_price + (pd.Series(range(periods)) * 0.1) + 1.0,
            "low": base_price + (pd.Series(range(periods)) * 0.1) - 1.0,
            "close": base_price + (pd.Series(range(periods)) * 0.1) + 0.5,
            "volume": 10000 + (pd.Series(range(periods)) * 100),
            "ticker": ticker
        })
        
        return df


# Global instance for backward compatibility
unified_fetcher = UnifiedDataFetcher()


def fetch_data(
    ticker: str,
    data_type: str = "INTRADAY",
    interval: str = "1min",
    outputsize: str = "compact",
    **kwargs
) -> Tuple[Optional[pd.DataFrame], bool]:
    """
    Convenience function that uses the global unified fetcher instance.
    
    This provides a simple interface while maintaining the unified architecture.
    """
    return unified_fetcher.fetch_data(ticker, data_type, interval, outputsize, **kwargs)