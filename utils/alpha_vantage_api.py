import logging
import os
import time
from datetime import datetime
from io import StringIO

import pandas as pd
import pytz
import requests

# Load the API key from environment variables
API_KEY = os.getenv("ALPHA_VANTAGE_API_KEY")
BASE_URL = "https://www.alphavantage.co/query"
# A short, aggressive timeout for every single API call to prevent hangs.
REQUEST_TIMEOUT = 15

# Import timestamp standardization module
from utils.timestamp_standardizer import apply_timestamp_standardization_to_api_data

logger = logging.getLogger(__name__)


def _make_api_request_with_retry(params, max_retries=3, base_delay=1.0):
    """
    Enhanced API request function with exponential backoff retry mechanism.
    
    This addresses the compact fetch failure issue by implementing a robust
    retry strategy when the API returns stale data or fails.
    
    Args:
        params (dict): API request parameters
        max_retries (int): Maximum number of retry attempts
        base_delay (float): Base delay in seconds for exponential backoff
        
    Returns:
        requests.Response: Successful response or None if all retries failed
    """
    if not API_KEY:
        logger.warning(
            "ALPHA_VANTAGE_API_KEY environment variable not set. API calls will be skipped."
        )
        return None

    ny_tz = pytz.timezone("America/New_York")
    today_et = datetime.now(ny_tz).date()
    
    for attempt in range(max_retries + 1):
        try:
            logger.info(f"🔄 API request attempt {attempt + 1}/{max_retries + 1} for {params.get('symbol', 'unknown')}")
            
            response = requests.get(BASE_URL, params=params, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            
            # For compact fetches, validate that today's data is present
            if params.get('outputsize') == 'compact':
                is_valid = _validate_current_day_data(response, today_et, params.get('symbol'))
                if not is_valid and attempt < max_retries:
                    logger.warning(f"⚠️ API response lacks current day data, retrying in {base_delay * (2 ** attempt):.1f}s...")
                    time.sleep(base_delay * (2 ** attempt))  # Exponential backoff
                    continue
                elif not is_valid:
                    logger.error(f"❌ Final attempt: API still not returning current day data for {params.get('symbol')}")
                    # Return the response anyway - let the processing logic handle it
            
            logger.info(f"✅ API request successful for {params.get('symbol')}")
            return response
            
        except requests.exceptions.Timeout:
            logger.error(
                f"API request timed out after {REQUEST_TIMEOUT} seconds for symbol: {params.get('symbol')} (attempt {attempt + 1})"
            )
        except requests.exceptions.RequestException as e:
            logger.error(f"HTTP request failed for symbol {params.get('symbol')}: {e} (attempt {attempt + 1})")
        except Exception as e:
            logger.error(
                f"Unexpected error during API request for {params.get('symbol')}: {e} (attempt {attempt + 1})"
            )
        
        # Wait before retry (except on the last attempt)
        if attempt < max_retries:
            delay = base_delay * (2 ** attempt)
            logger.info(f"🔄 Retrying in {delay:.1f} seconds...")
            time.sleep(delay)
    
    logger.error(f"❌ All retry attempts failed for {params.get('symbol')}")
    return None


def _validate_current_day_data(response, today_et, symbol):
    """
    Validate that the API response contains current day data.
    
    This is critical for compact fetches which should include today's data
    for real-time updates.
    
    Args:
        response (requests.Response): API response to validate
        today_et (datetime.date): Today's date in Eastern Time
        symbol (str): Stock symbol for logging
        
    Returns:
        bool: True if current day data is present, False otherwise
    """
    try:
        # Parse the CSV response quickly
        df = pd.read_csv(StringIO(response.text))
        
        if df.empty or "Error Message" in df.columns:
            logger.warning(f"⚠️ API returned error or empty data for {symbol}")
            return False
            
        # Find timestamp column
        timestamp_col = None
        for col in ["timestamp", "datetime", "Date", "time"]:
            if col in df.columns:
                timestamp_col = col
                break
                
        if not timestamp_col:
            logger.warning(f"⚠️ No timestamp column found in API response for {symbol}")
            return False
            
        # Check for today's data
        df[timestamp_col] = pd.to_datetime(df[timestamp_col], errors='coerce')
        
        # Convert to ET timezone for comparison
        ny_tz = pytz.timezone("America/New_York")
        if df[timestamp_col].dt.tz is None:
            df_et = df[timestamp_col].dt.tz_localize(ny_tz)
        else:
            df_et = df[timestamp_col].dt.tz_convert(ny_tz)
            
        today_data_count = (df_et.dt.date == today_et).sum()
        
        logger.info(f"📊 Current day validation for {symbol}: {today_data_count} rows with today's data")
        
        if today_data_count > 0:
            # Log the time range of today's data for debugging
            today_rows = df_et[df_et.dt.date == today_et]
            logger.info(f"✅ Today's data range: {today_rows.min()} to {today_rows.max()}")
            return True
        else:
            # Log available date range for debugging
            if not df_et.empty:
                logger.warning(f"⚠️ Available data range: {df_et.min()} to {df_et.max()}")
            return False
            
    except Exception as e:
        logger.error(f"❌ Error validating current day data for {symbol}: {e}")
        return False


def _make_api_request(params):
    """A centralized and robust function for making API requests."""
    # Use the enhanced retry mechanism
    return _make_api_request_with_retry(params)


def get_daily_data(symbol, outputsize="compact"):
    """
    Fetches daily adjusted time series data for a given symbol with proper timestamp standardization.

    Implements the rigorous timestamp standardization process:
    1. Parse Timestamps: Read the raw timestamp string from the API
    2. Localize to New York Time: Convert to timezone-aware object using 'America/New_York' timezone
    3. Standardize to UTC for Storage: Save the final timestamp in UTC format
    """
    params = {
        "function": "TIME_SERIES_DAILY_ADJUSTED",
        "symbol": symbol,
        "outputsize": outputsize,
        "apikey": API_KEY,
        "datatype": "csv",
    }
    response = _make_api_request(params)
    if response:
        try:
            df = pd.read_csv(StringIO(response.text))
            if "Error Message" in df.columns or df.empty:
                return pd.DataFrame()

            # Rename the date column to timestamp for consistent processing
            if "timestamp" in df.columns:
                pass  # Already correctly named
            elif "Date" in df.columns:
                df = df.rename(columns={"Date": "timestamp"})
            elif "date" in df.columns:
                df = df.rename(columns={"date": "timestamp"})

            logger.info(f"📊 Raw daily data fetched for {symbol}: {len(df)} rows")

            # Apply rigorous timestamp standardization
            df = apply_timestamp_standardization_to_api_data(df, data_type="daily")

            logger.info(
                f"✅ Daily data standardized for {symbol}: {len(df)} rows with UTC timestamps"
            )
            return df
        except Exception as e:
            print(f"ERROR: Failed to process daily CSV data for {symbol}: {e}")
    return pd.DataFrame()


def get_intraday_data(symbol, interval="1min", outputsize="compact"):
    """
    Fetches intraday time series data for a given symbol with robust current day data handling.

    Enhanced with comprehensive retry mechanism and current day data validation
    to address the compact fetch failure issue.

    Implements the rigorous timestamp standardization process:
    1. Parse Timestamps: Read the raw timestamp string from the API
    2. Localize to New York Time: Convert to timezone-aware object using 'America/New_York' timezone
    3. Standardize to UTC for Storage: Save the final timestamp in UTC format
    
    Args:
        symbol (str): Stock ticker symbol
        interval (str): Time interval ('1min', '30min', etc.)
        outputsize (str): 'compact' for latest 100 data points, 'full' for all available
        
    Returns:
        pandas.DataFrame: Processed data with UTC timestamps or empty DataFrame on failure
    """
    logger.info(f"🔄 Fetching {outputsize} intraday data for {symbol} ({interval})")
    
    params = {
        "function": "TIME_SERIES_INTRADAY",
        "symbol": symbol,
        "interval": interval,
        "outputsize": outputsize,
        "apikey": API_KEY,
        "datatype": "csv",
    }
    
    # Use enhanced retry mechanism
    response = _make_api_request(params)
    if not response:
        logger.error(f"❌ Failed to get API response for {symbol}")
        return pd.DataFrame()
        
    try:
        df = pd.read_csv(StringIO(response.text))
        if "Error Message" in df.columns or df.empty:
            logger.error(f"❌ API returned error or empty data for {symbol}")
            return pd.DataFrame()

        # Rename the timestamp column for consistent processing
        timestamp_col = None
        for col in ["timestamp", "datetime", "Date", "time"]:
            if col in df.columns:
                timestamp_col = col
                break
                
        if timestamp_col and timestamp_col != "timestamp":
            df = df.rename(columns={timestamp_col: "timestamp"})
            logger.debug(f"Renamed timestamp column from '{timestamp_col}' to 'timestamp'")

        logger.info(f"📊 Raw intraday data fetched for {symbol} ({interval}): {len(df)} rows")
        
        # Log current day data availability BEFORE processing
        _log_current_day_availability(df, symbol, "before processing")

        # Apply rigorous timestamp standardization
        df = apply_timestamp_standardization_to_api_data(df, data_type="intraday")

        # Log current day data availability AFTER processing
        _log_current_day_availability(df, symbol, "after processing")

        logger.info(f"✅ Intraday data standardized for {symbol} ({interval}): {len(df)} rows with UTC timestamps")
        
        # Final validation for compact fetches
        if outputsize == "compact":
            _final_compact_validation(df, symbol)
            
        return df
        
    except Exception as e:
        logger.error(f"❌ Failed to process intraday CSV data for {symbol}: {e}")
        return pd.DataFrame()


def _log_current_day_availability(df, symbol, stage):
    """
    Log the availability of current day data for debugging purposes.
    
    Args:
        df (pandas.DataFrame): Data to analyze
        symbol (str): Stock symbol
        stage (str): Processing stage for logging context
    """
    if df.empty:
        logger.warning(f"⚠️ {symbol} ({stage}): DataFrame is empty")
        return
        
    try:
        ny_tz = pytz.timezone("America/New_York")
        today_et = datetime.now(ny_tz).date()
        
        # Handle different timestamp formats
        if 'timestamp' in df.columns:
            timestamps = pd.to_datetime(df['timestamp'], errors='coerce')
            
            # Convert to ET for analysis
            if timestamps.dt.tz is None:
                # For raw data, assume ET
                timestamps_et = timestamps.dt.tz_localize(ny_tz)
            else:
                # For processed data, convert to ET
                timestamps_et = timestamps.dt.tz_convert(ny_tz)
                
            today_count = (timestamps_et.dt.date == today_et).sum()
            
            if today_count > 0:
                today_data = timestamps_et[timestamps_et.dt.date == today_et]
                logger.info(f"✅ {symbol} ({stage}): {today_count} rows with today's data")
                logger.info(f"   📅 Today's data range: {today_data.min()} to {today_data.max()}")
            else:
                if not timestamps_et.empty:
                    logger.warning(f"⚠️ {symbol} ({stage}): No today's data found")
                    logger.warning(f"   📅 Available range: {timestamps_et.min()} to {timestamps_et.max()}")
                else:
                    logger.warning(f"⚠️ {symbol} ({stage}): No valid timestamps found")
        else:
            logger.warning(f"⚠️ {symbol} ({stage}): No timestamp column found")
            
    except Exception as e:
        logger.error(f"❌ Error analyzing current day data for {symbol} ({stage}): {e}")


def _final_compact_validation(df, symbol):
    """
    Perform final validation for compact fetches to ensure current day data is present.
    
    Args:
        df (pandas.DataFrame): Processed data
        symbol (str): Stock symbol
    """
    try:
        ny_tz = pytz.timezone("America/New_York")
        today_et = datetime.now(ny_tz).date()
        
        if df.empty or 'timestamp' not in df.columns:
            logger.error(f"❌ {symbol}: Final validation failed - no data or timestamp column")
            return
            
        timestamps = pd.to_datetime(df['timestamp'], errors='coerce')
        timestamps_et = timestamps.dt.tz_convert(ny_tz)
        today_count = (timestamps_et.dt.date == today_et).sum()
        
        if today_count > 0:
            logger.info(f"✅ {symbol}: Final validation PASSED - {today_count} current day data points")
        else:
            logger.error(f"❌ {symbol}: Final validation FAILED - NO current day data found")
            logger.error(f"   This indicates the compact fetch failure issue!")
            
    except Exception as e:
        logger.error(f"❌ Final validation error for {symbol}: {e}")


def get_company_overview(symbol):
    """Fetches company overview data (Market Cap, Float, etc.) for a given symbol."""
    params = {"function": "OVERVIEW", "symbol": symbol, "apikey": API_KEY}
    response = _make_api_request(params)
    if response:
        try:
            overview_data = response.json()
            if not overview_data or "MarketCapitalization" not in overview_data:
                return None
            return overview_data
        except Exception as e:
            print(f"ERROR: Failed to process overview JSON data for {symbol}: {e}")
    return None


def get_real_time_price(symbol):
    """
    Fetches real-time price using Alpha Vantage Global Quote.
    Used for live price checks (TP/SL validation in dashboards).

    Args:
        symbol (str): Stock symbol

    Returns:
        dict: Dictionary with price data or None if failed
    """
    params = {"function": "GLOBAL_QUOTE", "symbol": symbol, "apikey": API_KEY}
    response = _make_api_request(params)
    if response:
        try:
            quote_data = response.json()
            global_quote = quote_data.get("Global Quote", {})

            if not global_quote:
                print(f"ERROR: No Global Quote data returned for {symbol}")
                return None

            # Extract key price information
            price_data = {
                "symbol": global_quote.get("01. symbol", symbol),
                "price": float(global_quote.get("05. price", 0)),
                "open": float(global_quote.get("02. open", 0)),
                "high": float(global_quote.get("03. high", 0)),
                "low": float(global_quote.get("04. low", 0)),
                "previous_close": float(global_quote.get("08. previous close", 0)),
                "change": float(global_quote.get("09. change", 0)),
                "change_percent": global_quote.get("10. change percent", "0%").rstrip(
                    "%"
                ),
                "volume": int(global_quote.get("06. volume", 0)),
                "latest_trading_day": global_quote.get("07. latest trading day", ""),
                "timestamp": pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S"),
            }

            return price_data

        except Exception as e:
            print(f"ERROR: Failed to process real-time price data for {symbol}: {e}")
    return None
