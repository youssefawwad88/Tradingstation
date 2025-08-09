"""
Alpha Vantage API wrapper for Trading Station.
Provides clean interface with throttling, retry logic, and test mode support.
"""

import requests
import pandas as pd
import time
import json
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List
from pathlib import Path
import logging
from dataclasses import dataclass
from io import StringIO

from .config import (
    ALPHA_VANTAGE_API_KEY, API_RATE_LIMIT_PER_MIN, 
    TEST_MODE, FORCE_LIVE_API
)
from .logging_setup import get_logger, log_api_usage
from .time_utils import now_utc, to_utc

logger = get_logger(__name__)

class APIError(Exception):
    """Base exception for API operations."""
    pass

class RateLimitError(APIError):
    """Exception for rate limit violations."""
    pass

@dataclass
class APICall:
    """Track API call metadata."""
    timestamp: datetime
    endpoint: str
    ticker: str
    success: bool
    response_time_ms: float

class RateLimiter:
    """Rate limiter for API calls."""
    
    def __init__(self, max_calls_per_minute: int = API_RATE_LIMIT_PER_MIN):
        self.max_calls = max_calls_per_minute
        self.calls: List[APICall] = []
        self.last_call_time = None
    
    def can_make_call(self) -> bool:
        """Check if we can make an API call without exceeding rate limit."""
        now = now_utc()
        
        # Remove calls older than 1 minute
        cutoff = now - timedelta(minutes=1)
        self.calls = [call for call in self.calls if call.timestamp > cutoff]
        
        return len(self.calls) < self.max_calls
    
    def wait_time(self) -> float:
        """Get seconds to wait before next call."""
        if self.can_make_call():
            return 0.0
        
        # Find the oldest call within the minute
        now = now_utc()
        cutoff = now - timedelta(minutes=1)
        recent_calls = [call for call in self.calls if call.timestamp > cutoff]
        
        if not recent_calls:
            return 0.0
        
        oldest_call = min(recent_calls, key=lambda x: x.timestamp)
        wait_until = oldest_call.timestamp + timedelta(minutes=1)
        wait_seconds = (wait_until - now).total_seconds()
        
        return max(0.0, wait_seconds)
    
    def record_call(self, call: APICall):
        """Record an API call."""
        self.calls.append(call)
        self.last_call_time = call.timestamp
    
    def get_stats(self) -> Dict[str, Any]:
        """Get rate limiter statistics."""
        now = now_utc()
        cutoff = now - timedelta(minutes=1)
        recent_calls = [call for call in self.calls if call.timestamp > cutoff]
        
        return {
            'calls_last_minute': len(recent_calls),
            'max_calls_per_minute': self.max_calls,
            'can_make_call': self.can_make_call(),
            'wait_time_seconds': self.wait_time()
        }

class AlphaVantageAPI:
    """Alpha Vantage API client with throttling and retry."""
    
    BASE_URL = "https://www.alphavantage.co/query"
    
    def __init__(self, api_key: str = None):
        self.api_key = api_key or ALPHA_VANTAGE_API_KEY
        self.rate_limiter = RateLimiter()
        self.session = requests.Session()
        
        # Configure session
        self.session.headers.update({
            'User-Agent': 'TradingStation/1.0',
            'Accept': 'application/json,text/csv'
        })
        
        if not self.api_key and not TEST_MODE:
            logger.warning("No Alpha Vantage API key configured")
    
    def _should_use_live_api(self, force_live: bool = False) -> bool:
        """Determine if we should use live API or fixtures."""
        if TEST_MODE and not force_live and not FORCE_LIVE_API:
            return False
        return True
    
    def _get_fixture_path(self, function: str, symbol: str, **params) -> Path:
        """Get path to test fixture file."""
        fixture_dir = Path(__file__).parent.parent / "tests" / "resources" / "api_fixtures"
        
        # Create a filename based on parameters
        param_str = "_".join(f"{k}_{v}" for k, v in sorted(params.items()) if v)
        filename = f"{function}_{symbol}_{param_str}.csv" if param_str else f"{function}_{symbol}.csv"
        
        return fixture_dir / filename
    
    def _load_fixture(self, function: str, symbol: str, **params) -> Optional[pd.DataFrame]:
        """Load test fixture data."""
        fixture_path = self._get_fixture_path(function, symbol, **params)
        
        if not fixture_path.exists():
            logger.warning(f"Fixture not found: {fixture_path}")
            return None
        
        try:
            df = pd.read_csv(fixture_path)
            logger.debug(f"Loaded fixture: {fixture_path} ({len(df)} rows)")
            return df
        except Exception as e:
            logger.error(f"Failed to load fixture {fixture_path}: {e}")
            return None
    
    def _wait_for_rate_limit(self):
        """Wait if necessary to respect rate limits."""
        wait_time = self.rate_limiter.wait_time()
        if wait_time > 0:
            logger.info(f"Rate limit reached, waiting {wait_time:.1f} seconds")
            time.sleep(wait_time)
    
    def _make_request(
        self, 
        params: Dict[str, Any], 
        retries: int = 3,
        force_live: bool = False
    ) -> Dict[str, Any]:
        """Make API request with retry logic."""
        
        function = params.get('function', 'unknown')
        symbol = params.get('symbol', 'unknown')
        
        # Check if we should use fixtures
        if not self._should_use_live_api(force_live):
            fixture_df = self._load_fixture(function, symbol, **params)
            if fixture_df is not None:
                return {'data': fixture_df, 'from_fixture': True}
            else:
                logger.warning(f"No fixture available for {function} {symbol}, returning empty data")
                return {'data': pd.DataFrame(), 'from_fixture': True}
        
        # Live API call
        if not self.api_key:
            raise APIError("API key required for live calls")
        
        params['apikey'] = self.api_key
        
        for attempt in range(retries + 1):
            try:
                # Wait for rate limit
                self._wait_for_rate_limit()
                
                start_time = time.time()
                
                # Make request
                response = self.session.get(self.BASE_URL, params=params, timeout=30)
                
                elapsed_ms = (time.time() - start_time) * 1000
                
                # Record API call
                api_call = APICall(
                    timestamp=now_utc(),
                    endpoint=function,
                    ticker=symbol,
                    success=response.status_code == 200,
                    response_time_ms=elapsed_ms
                )
                self.rate_limiter.record_call(api_call)
                
                # Log API usage
                log_api_usage(logger, function, symbol, self.rate_limiter.max_calls - len(self.rate_limiter.calls))
                
                if response.status_code == 429:
                    raise RateLimitError("Rate limit exceeded")
                
                response.raise_for_status()
                
                # Parse response
                if 'text/csv' in response.headers.get('content-type', ''):
                    # CSV response
                    df = pd.read_csv(StringIO(response.text))
                    return {'data': df, 'from_fixture': False}
                else:
                    # JSON response
                    data = response.json()
                    
                    # Check for API errors
                    if 'Error Message' in data:
                        raise APIError(f"API Error: {data['Error Message']}")
                    
                    if 'Note' in data:
                        # Rate limit message
                        if 'call frequency' in data['Note'].lower():
                            raise RateLimitError(f"API Note: {data['Note']}")
                    
                    return {'data': data, 'from_fixture': False}
                
            except (requests.exceptions.RequestException, RateLimitError) as e:
                if attempt < retries:
                    wait_time = 2 ** attempt  # Exponential backoff
                    logger.warning(f"API call failed (attempt {attempt + 1}): {e}. Retrying in {wait_time}s")
                    time.sleep(wait_time)
                else:
                    logger.error(f"API call failed after {retries + 1} attempts: {e}")
                    raise APIError(f"Request failed: {e}")
            
            except Exception as e:
                logger.error(f"Unexpected error in API call: {e}")
                raise APIError(f"Unexpected error: {e}")
    
    def get_intraday_data(
        self,
        symbol: str,
        interval: str = "1min",
        outputsize: str = "compact",
        extended_hours: bool = True,
        force_live: bool = False
    ) -> Optional[pd.DataFrame]:
        """
        Get intraday data for a symbol.
        
        Args:
            symbol: Stock ticker symbol
            interval: 1min, 5min, 15min, 30min, 60min
            outputsize: compact (100 data points) or full (full data)
            extended_hours: Include extended hours data
            force_live: Force live API call even in test mode
            
        Returns:
            DataFrame with columns: timestamp, open, high, low, close, volume
        """
        params = {
            'function': 'TIME_SERIES_INTRADAY',
            'symbol': symbol.upper(),
            'interval': interval,
            'outputsize': outputsize,
            'extended_hours': 'true' if extended_hours else 'false'
        }
        
        try:
            result = self._make_request(params, force_live=force_live)
            
            if result.get('from_fixture'):
                df = result['data']
                if 'timestamp' in df.columns:
                    df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True)
                return df
            
            # Parse Alpha Vantage JSON response
            data = result['data']
            time_series_key = f"Time Series ({interval})"
            
            if time_series_key not in data:
                logger.warning(f"No intraday data found for {symbol}")
                return None
            
            time_series = data[time_series_key]
            
            # Convert to DataFrame
            rows = []
            for timestamp_str, values in time_series.items():
                row = {
                    'timestamp': pd.to_datetime(timestamp_str, utc=True),
                    'open': float(values['1. open']),
                    'high': float(values['2. high']),
                    'low': float(values['3. low']),
                    'close': float(values['4. close']),
                    'volume': int(values['5. volume'])
                }
                rows.append(row)
            
            df = pd.DataFrame(rows)
            df = df.sort_values('timestamp').reset_index(drop=True)
            
            logger.info(f"Retrieved {len(df)} intraday data points for {symbol}")
            return df
            
        except Exception as e:
            logger.error(f"Failed to get intraday data for {symbol}: {e}")
            return None
    
    def get_daily_data(
        self,
        symbol: str,
        outputsize: str = "compact",
        force_live: bool = False
    ) -> Optional[pd.DataFrame]:
        """
        Get daily data for a symbol.
        
        Args:
            symbol: Stock ticker symbol
            outputsize: compact (100 data points) or full (20+ years)
            force_live: Force live API call even in test mode
            
        Returns:
            DataFrame with columns: date, open, high, low, close, volume, adj_close
        """
        params = {
            'function': 'TIME_SERIES_DAILY_ADJUSTED',
            'symbol': symbol.upper(),
            'outputsize': outputsize
        }
        
        try:
            result = self._make_request(params, force_live=force_live)
            
            if result.get('from_fixture'):
                df = result['data']
                if 'date' in df.columns:
                    df['date'] = pd.to_datetime(df['date']).dt.date
                return df
            
            # Parse Alpha Vantage JSON response
            data = result['data']
            time_series_key = "Time Series (Daily)"
            
            if time_series_key not in data:
                logger.warning(f"No daily data found for {symbol}")
                return None
            
            time_series = data[time_series_key]
            
            # Convert to DataFrame
            rows = []
            for date_str, values in time_series.items():
                row = {
                    'date': pd.to_datetime(date_str).date(),
                    'open': float(values['1. open']),
                    'high': float(values['2. high']),
                    'low': float(values['3. low']),
                    'close': float(values['4. close']),
                    'volume': int(values['6. volume']),
                    'adj_close': float(values['5. adjusted close'])
                }
                rows.append(row)
            
            df = pd.DataFrame(rows)
            df = df.sort_values('date').reset_index(drop=True)
            
            logger.info(f"Retrieved {len(df)} daily data points for {symbol}")
            return df
            
        except Exception as e:
            logger.error(f"Failed to get daily data for {symbol}: {e}")
            return None
    
    def get_quote(self, symbol: str, force_live: bool = False) -> Optional[Dict[str, Any]]:
        """
        Get real-time quote for a symbol.
        
        Args:
            symbol: Stock ticker symbol
            force_live: Force live API call even in test mode
            
        Returns:
            Dictionary with quote data
        """
        params = {
            'function': 'GLOBAL_QUOTE',
            'symbol': symbol.upper()
        }
        
        try:
            result = self._make_request(params, force_live=force_live)
            
            if result.get('from_fixture'):
                # For fixtures, return a simple mock quote
                return {
                    'symbol': symbol.upper(),
                    'price': 100.0,
                    'change': 1.0,
                    'change_percent': '1.00%',
                    'volume': 1000000,
                    'from_fixture': True
                }
            
            data = result['data']
            quote_key = "Global Quote"
            
            if quote_key not in data:
                logger.warning(f"No quote data found for {symbol}")
                return None
            
            quote = data[quote_key]
            
            return {
                'symbol': quote['01. symbol'],
                'price': float(quote['05. price']),
                'change': float(quote['09. change']),
                'change_percent': quote['10. change percent'],
                'volume': int(quote['06. volume']) if quote['06. volume'] != 'N/A' else 0,
                'from_fixture': False
            }
            
        except Exception as e:
            logger.error(f"Failed to get quote for {symbol}: {e}")
            return None
    
    def get_rate_limit_stats(self) -> Dict[str, Any]:
        """Get rate limiter statistics."""
        return self.rate_limiter.get_stats()

# Global API instance
_api_instance: Optional[AlphaVantageAPI] = None

def get_api() -> AlphaVantageAPI:
    """Get the global API instance."""
    global _api_instance
    if _api_instance is None:
        _api_instance = AlphaVantageAPI()
    return _api_instance

# Export classes and functions
__all__ = [
    'AlphaVantageAPI', 'APIError', 'RateLimitError',
    'get_api'
]