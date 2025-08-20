"""
Alpha Vantage API wrapper for market data fetching.

This module provides a clean interface to Alpha Vantage endpoints with
proper error handling, rate limiting, and data parsing.
"""

import io
import logging
import time
from typing import Optional, Tuple

import pandas as pd
import requests

from utils.config import config

logger = logging.getLogger(__name__)


class AlphaVantageAPI:
    """Alpha Vantage API client with rate limiting and error handling."""

    def __init__(self) -> None:
        """Initialize the API client."""
        self.base_url = "https://www.alphavantage.co/query"
        self.api_key = config.ALPHA_VANTAGE_API_KEY
        self.session = requests.Session()
        self.last_request_time = 0.0
        self.min_request_interval = 60.0 / config.API_RATE_LIMIT_CALLS_PER_MINUTE

    def _rate_limit(self) -> None:
        """Apply rate limiting between requests."""
        current_time = time.time()
        time_since_last = current_time - self.last_request_time
        
        if time_since_last < self.min_request_interval:
            sleep_time = self.min_request_interval - time_since_last
            logger.debug(f"Rate limiting: sleeping for {sleep_time:.2f} seconds")
            time.sleep(sleep_time)
        
        self.last_request_time = time.time()

    def _make_request(
        self,
        params: dict,
        retries: int = 3,
        backoff_factor: float = 1.0,
    ) -> Optional[requests.Response]:
        """
        Make a request to Alpha Vantage with retries and backoff.
        
        Args:
            params: Request parameters
            retries: Number of retry attempts
            backoff_factor: Exponential backoff factor
            
        Returns:
            Response object or None if failed
        """
        if not self.api_key:
            logger.error("Alpha Vantage API key not configured")
            return None

        params["apikey"] = self.api_key
        
        for attempt in range(retries):
            try:
                # Apply rate limiting
                self._rate_limit()
                
                response = self.session.get(
                    self.base_url,
                    params=params,
                    timeout=30,
                )
                
                if response.status_code == 200:
                    # Check for API error messages
                    if "Error Message" in response.text:
                        logger.error(f"API Error: {response.text}")
                        return None
                    elif "Note:" in response.text and "API call frequency" in response.text:
                        logger.warning("API rate limit hit, backing off")
                        time.sleep(60)  # Wait a minute before retry
                        continue
                    
                    return response
                
                elif response.status_code == 429:
                    # Rate limited
                    wait_time = backoff_factor * (2 ** attempt)
                    logger.warning(f"Rate limited, waiting {wait_time} seconds (attempt {attempt + 1})")
                    time.sleep(wait_time)
                    continue
                
                else:
                    logger.error(f"HTTP {response.status_code}: {response.text}")
                    
            except requests.exceptions.RequestException as e:
                logger.error(f"Request failed (attempt {attempt + 1}): {e}")
                if attempt < retries - 1:
                    wait_time = backoff_factor * (2 ** attempt)
                    time.sleep(wait_time)
                    
        return None

    def get_intraday_data(
        self,
        symbol: str,
        interval: str = "1min",
        outputsize: str = "compact",
    ) -> Optional[pd.DataFrame]:
        """
        Fetch intraday data for a symbol.
        
        Args:
            symbol: Stock symbol
            interval: Time interval (1min, 5min, 15min, 30min, 60min)
            outputsize: Data size (compact=last 100 points, full=all available)
            
        Returns:
            DataFrame with OHLCV data or None if failed
        """
        params = {
            "function": "TIME_SERIES_INTRADAY",
            "symbol": symbol,
            "interval": interval,
            "outputsize": outputsize,
            "datatype": "csv",
        }
        
        logger.debug(f"Fetching {interval} intraday data for {symbol} (outputsize={outputsize})")
        
        response = self._make_request(params)
        if response is None:
            return None
            
        try:
            # Parse CSV response
            csv_data = io.StringIO(response.text)
            df = pd.read_csv(csv_data)
            
            # Validate expected columns
            expected_columns = ["timestamp", "open", "high", "low", "close", "volume"]
            if not all(col in df.columns for col in expected_columns):
                logger.error(f"Unexpected CSV format for {symbol}: {df.columns.tolist()}")
                return None
            
            # Convert timestamp to datetime and sort
            df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
            df = df.sort_values("timestamp").reset_index(drop=True)
            
            # Convert price columns to float
            price_cols = ["open", "high", "low", "close"]
            for col in price_cols:
                df[col] = pd.to_numeric(df[col], errors="coerce")
            
            # Convert volume to int
            df["volume"] = pd.to_numeric(df["volume"], errors="coerce").fillna(0).astype(int)
            
            logger.info(f"Successfully fetched {len(df)} rows of {interval} data for {symbol}")
            return df
            
        except Exception as e:
            logger.error(f"Error parsing intraday data for {symbol}: {e}")
            return None

    def get_daily_data(
        self,
        symbol: str,
        outputsize: str = "compact",
        adjusted: bool = False,
    ) -> Optional[pd.DataFrame]:
        """
        Fetch daily data for a symbol.
        
        Args:
            symbol: Stock symbol
            outputsize: Data size (compact=last 100 points, full=all available)
            adjusted: Whether to use adjusted prices
            
        Returns:
            DataFrame with OHLCV data or None if failed
        """
        function = "TIME_SERIES_DAILY_ADJUSTED" if adjusted else "TIME_SERIES_DAILY"
        
        params = {
            "function": function,
            "symbol": symbol,
            "outputsize": outputsize,
            "datatype": "csv",
        }
        
        logger.debug(f"Fetching daily data for {symbol} (outputsize={outputsize}, adjusted={adjusted})")
        
        response = self._make_request(params)
        if response is None:
            return None
            
        try:
            # Parse CSV response
            csv_data = io.StringIO(response.text)
            df = pd.read_csv(csv_data)
            
            # Validate expected columns
            expected_base = ["timestamp", "open", "high", "low", "close", "volume"]
            if adjusted:
                expected_base.extend(["adjusted_close", "dividend_amount", "split_coefficient"])
            
            if not all(col in df.columns for col in expected_base):
                logger.error(f"Unexpected CSV format for {symbol}: {df.columns.tolist()}")
                return None
            
            # Rename timestamp to date for daily data
            df = df.rename(columns={"timestamp": "date"})
            
            # Convert date to datetime and sort
            df["date"] = pd.to_datetime(df["date"]).dt.date
            df = df.sort_values("date").reset_index(drop=True)
            
            # Convert price columns to float
            price_cols = ["open", "high", "low", "close"]
            if adjusted:
                price_cols.append("adjusted_close")
            
            for col in price_cols:
                df[col] = pd.to_numeric(df[col], errors="coerce")
            
            # Convert volume to int
            df["volume"] = pd.to_numeric(df["volume"], errors="coerce").fillna(0).astype(int)
            
            logger.info(f"Successfully fetched {len(df)} rows of daily data for {symbol}")
            return df
            
        except Exception as e:
            logger.error(f"Error parsing daily data for {symbol}: {e}")
            return None

    def get_global_quote(self, symbol: str) -> Optional[dict]:
        """
        Fetch latest quote for a symbol.
        
        Args:
            symbol: Stock symbol
            
        Returns:
            Dictionary with latest quote data or None if failed
        """
        params = {
            "function": "GLOBAL_QUOTE",
            "symbol": symbol,
        }
        
        logger.debug(f"Fetching global quote for {symbol}")
        
        response = self._make_request(params)
        if response is None:
            return None
            
        try:
            data = response.json()
            
            if "Global Quote" not in data:
                logger.error(f"Unexpected quote response for {symbol}: {data}")
                return None
            
            quote = data["Global Quote"]
            
            # Parse and clean the quote data
            parsed_quote = {
                "symbol": quote.get("01. symbol", symbol),
                "price": float(quote.get("05. price", 0)),
                "change": float(quote.get("09. change", 0)),
                "change_percent": quote.get("10. change percent", "0%").replace("%", ""),
                "volume": int(quote.get("06. volume", 0)),
                "latest_trading_day": quote.get("07. latest trading day"),
                "previous_close": float(quote.get("08. previous close", 0)),
                "open": float(quote.get("02. open", 0)),
                "high": float(quote.get("03. high", 0)),
                "low": float(quote.get("04. low", 0)),
            }
            
            logger.info(f"Successfully fetched quote for {symbol}: ${parsed_quote['price']}")
            return parsed_quote
            
        except Exception as e:
            logger.error(f"Error parsing quote for {symbol}: {e}")
            return None

    def test_connection(self) -> Tuple[bool, str]:
        """
        Test the API connection and key validity.
        
        Returns:
            Tuple of (success, message)
        """
        if not self.api_key:
            return False, "API key not configured"
        
        try:
            # Use a simple quote request to test
            quote = self.get_global_quote("AAPL")
            if quote is not None:
                return True, "API connection successful"
            else:
                return False, "API request failed"
                
        except Exception as e:
            return False, f"Connection test failed: {e}"


# Global API client instance
alpha_vantage = AlphaVantageAPI()


# Convenience functions for backward compatibility
def get_intraday_data(
    symbol: str,
    interval: str = "1min",
    outputsize: str = "compact",
) -> Optional[pd.DataFrame]:
    """Get intraday data for a symbol."""
    return alpha_vantage.get_intraday_data(symbol, interval, outputsize)


def get_daily_data(
    symbol: str,
    outputsize: str = "compact",
    adjusted: bool = False,
) -> Optional[pd.DataFrame]:
    """Get daily data for a symbol."""
    return alpha_vantage.get_daily_data(symbol, outputsize, adjusted)


def get_global_quote(symbol: str) -> Optional[dict]:
    """Get latest quote for a symbol."""
    return alpha_vantage.get_global_quote(symbol)


def test_api_connection() -> Tuple[bool, str]:
    """Test API connection."""
    return alpha_vantage.test_connection()