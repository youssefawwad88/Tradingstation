"""MarketData.io API client for fetching market data.

This client provides access to MarketData's REST API with proper authentication,
rate limiting, and data format standardization to UTC timestamps.
"""

import os
import time
from typing import Optional

import pandas as pd
import requests

from utils.logging_setup import get_logger
from utils.time_utils import utc_now

logger = get_logger(__name__)


class MarketDataProvider:
    """MarketData.io API client with rate limiting and standardization."""

    def __init__(self) -> None:
        """Initialize the MarketData provider."""
        self.base_url = os.getenv("MARKETDATA_ENDPOINT", "https://api.marketdata.app")
        self.endpoint_base = "/v1/stocks"
        self.token = os.getenv("MARKETDATA_TOKEN")
        self.session = requests.Session()
        self.last_request_time = 0.0
        # Conservative rate limit (MarketData allows higher rates)
        self.min_request_interval = 0.1  # 10 requests per second max

        # Set up authentication
        if self.token:
            self.session.headers.update({
                "Authorization": f"Bearer {self.token}"
            })

    def _rate_limit(self) -> None:
        """Apply rate limiting between requests."""
        current_time = time.time()
        time_since_last = current_time - self.last_request_time

        if time_since_last < self.min_request_interval:
            sleep_time = self.min_request_interval - time_since_last
            logger.debug(f"Rate limiting: sleeping for {sleep_time:.3f} seconds")
            time.sleep(sleep_time)

        self.last_request_time = time.time()

    def _make_request(
        self,
        endpoint: str,
        params: Optional[dict] = None,
        retries: int = 3,
        backoff_factor: float = 1.0,
    ) -> Optional[requests.Response]:
        """Make a request to MarketData API with retries and backoff.
        
        Args:
            endpoint: API endpoint path
            params: Query parameters
            retries: Number of retry attempts
            backoff_factor: Exponential backoff factor
            
        Returns:
            Response object or None if failed
        """
        if not self.token:
            logger.error("MarketData API token not configured")
            return None

        params = params or {}

        # Add token as query param if not using Bearer auth
        if "Authorization" not in self.session.headers:
            params["token"] = self.token

        url = f"{self.base_url}{self.endpoint_base}{endpoint}"

        for attempt in range(retries):
            try:
                # Apply rate limiting
                self._rate_limit()

                response = self.session.get(
                    url,
                    params=params,
                    timeout=30,
                )

                if response.status_code == 200:
                    return response

                elif response.status_code == 429:
                    # Rate limited
                    wait_time = backoff_factor * (2 ** attempt)
                    logger.warning(f"Rate limited, waiting {wait_time} seconds (attempt {attempt + 1})")
                    time.sleep(wait_time)
                    continue

                elif response.status_code == 401:
                    logger.error("MarketData API authentication failed")
                    return None

                else:
                    logger.error(f"MarketData API error {response.status_code}: {response.text}")

            except requests.exceptions.RequestException as e:
                logger.error(f"Request failed (attempt {attempt + 1}): {e}")
                if attempt < retries - 1:
                    wait_time = backoff_factor * (2 ** attempt)
                    time.sleep(wait_time)

        return None

    def get_candles(
        self,
        symbol: str,
        resolution: str,
        *,
        from_iso: Optional[str] = None,
        to_iso: Optional[str] = None,
        countback: Optional[int] = None,
        extended: bool = False,
        adjustsplits: Optional[bool] = None,
    ) -> pd.DataFrame:
        """Fetch candle data from MarketData API.
        
        Args:
            symbol: Stock ticker symbol
            resolution: Time resolution ("1", "30", "D")
            from_iso: Start time in ISO format (UTC)
            to_iso: End time in ISO format (UTC)
            countback: Number of bars to fetch backwards
            extended: Include extended hours for intraday
            adjustsplits: Apply split adjustments for daily
            
        Returns:
            DataFrame with columns: timestamp (UTC), open, high, low, close, volume
        """
        endpoint = f"/candles/{resolution}/{symbol}/"

        params = {}

        # Add time range parameters
        if from_iso:
            params["from"] = from_iso
        if to_iso:
            params["to"] = to_iso
        if countback:
            params["countback"] = str(countback)

        # Add resolution-specific parameters
        if resolution in ["1", "30"]:  # Intraday
            if extended:
                params["extended"] = "true"
        elif resolution == "D":  # Daily
            if adjustsplits is not None:
                params["adjustsplits"] = "true" if adjustsplits else "false"

        logger.debug(f"Fetching {resolution} candles for {symbol} with params: {params}")

        response = self._make_request(endpoint, params)
        if response is None:
            logger.error(f"Failed to fetch {resolution} data for {symbol}")
            return pd.DataFrame()

        try:
            data = response.json()

            # MarketData returns arrays: t, o, h, l, c, v
            if not all(key in data for key in ["t", "o", "h", "l", "c", "v"]):
                logger.error(f"Unexpected response format for {symbol}: {data.keys()}")
                return pd.DataFrame()

            # Convert to DataFrame
            df = pd.DataFrame({
                "timestamp": data["t"],
                "open": data["o"],
                "high": data["h"],
                "low": data["l"],
                "close": data["c"],
                "volume": data["v"],
            })

            if df.empty:
                logger.warning(f"No data returned for {symbol} ({resolution})")
                return df

            # Convert timestamps from unix seconds to UTC datetime
            df["timestamp"] = pd.to_datetime(df["timestamp"], unit="s", utc=True)

            # Ensure proper data types
            price_cols = ["open", "high", "low", "close"]
            for col in price_cols:
                df[col] = pd.to_numeric(df[col], errors="coerce")

            df["volume"] = pd.to_numeric(df["volume"], errors="coerce").fillna(0).astype(int)

            # Sort by timestamp
            df = df.sort_values("timestamp").reset_index(drop=True)

            logger.info(f"Successfully fetched {len(df)} rows of {resolution} data for {symbol}")
            if not df.empty:
                logger.debug(f"Data range: {df['timestamp'].min()} to {df['timestamp'].max()}")

            return df

        except Exception as e:
            logger.error(f"Error parsing MarketData response for {symbol}: {e}")
            return pd.DataFrame()

    def health_check(self) -> tuple[bool, str]:
        """Perform a health check by fetching recent data for AAPL.
        
        Returns:
            Tuple of (is_healthy, status_message)
        """
        try:
            # Fetch last 3 bars of 1min data for AAPL as health probe
            df = self.get_candles("AAPL", "1", countback=3)

            if df.empty:
                return False, "No data returned from MarketData API"

            # Check if latest data is reasonably recent
            now_utc = utc_now()
            latest_timestamp = df["timestamp"].max()

            # Convert to Eastern Time for market hours check
            from utils.time_utils import convert_utc_to_et
            latest_et = convert_utc_to_et(latest_timestamp)
            now_et = convert_utc_to_et(now_utc)

            age_minutes = (now_et - latest_et).total_seconds() / 60

            # Check if data is fresh (considering market hours)
            stale_threshold = int(os.getenv("DEGRADE_INTRADAY_ON_STALE_MINUTES", "5"))

            if age_minutes > stale_threshold:
                return False, f"Latest data is {age_minutes:.1f} minutes old (threshold: {stale_threshold}m)"

            return True, f"Healthy - latest data is {age_minutes:.1f} minutes old"

        except Exception as e:
            return False, f"Health check failed: {e}"
