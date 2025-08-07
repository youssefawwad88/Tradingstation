import os
import requests
import pandas as pd
from io import StringIO
import time

# Load the API key from environment variables
API_KEY = os.getenv('ALPHA_VANTAGE_API_KEY')
BASE_URL = 'https://www.alphavantage.co/query'
# A short, aggressive timeout for every single API call to prevent hangs.
REQUEST_TIMEOUT = 15 

def _make_api_request(params):
    """A centralized and robust function for making API requests."""
    if not API_KEY:
        print("WARNING: ALPHA_VANTAGE_API_KEY environment variable not set. API calls will be skipped.")
        return None
    
    try:
        response = requests.get(BASE_URL, params=params, timeout=REQUEST_TIMEOUT)
        response.raise_for_status() # Raises an HTTPError for bad responses (4xx or 5xx)
        return response
    except requests.exceptions.Timeout:
        print(f"ERROR: API request timed out after {REQUEST_TIMEOUT} seconds for symbol: {params.get('symbol')}")
        return None
    except requests.exceptions.RequestException as e:
        print(f"ERROR: HTTP request failed for symbol {params.get('symbol')}: {e}")
        return None
    except Exception as e:
        print(f"An unexpected error occurred during API request for {params.get('symbol')}: {e}")
        return None

def get_daily_data(symbol, outputsize='compact'):
    """Fetches daily adjusted time series data for a given symbol."""
    params = {
        'function': 'TIME_SERIES_DAILY_ADJUSTED',
        'symbol': symbol,
        'outputsize': outputsize,
        'apikey': API_KEY,
        'datatype': 'csv'
    }
    response = _make_api_request(params)
    if response:
        try:
            df = pd.read_csv(StringIO(response.text))
            if 'Error Message' in df.columns or df.empty:
                return pd.DataFrame()
            return df
        except Exception as e:
            print(f"ERROR: Failed to process daily CSV data for {symbol}: {e}")
    return pd.DataFrame()

def get_intraday_data(symbol, interval='1min', outputsize='compact'):
    """Fetches intraday time series data for a given symbol."""
    params = {
        'function': 'TIME_SERIES_INTRADAY',
        'symbol': symbol,
        'interval': interval,
        'outputsize': outputsize,
        'apikey': API_KEY,
        'datatype': 'csv'
    }
    response = _make_api_request(params)
    if response:
        try:
            df = pd.read_csv(StringIO(response.text))
            if 'Error Message' in df.columns or df.empty:
                return pd.DataFrame()
            return df
        except Exception as e:
            print(f"ERROR: Failed to process intraday CSV data for {symbol}: {e}")
    return pd.DataFrame()

def get_company_overview(symbol):
    """Fetches company overview data (Market Cap, Float, etc.) for a given symbol."""
    params = {
        'function': 'OVERVIEW',
        'symbol': symbol,
        'apikey': API_KEY
    }
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
    params = {
        'function': 'GLOBAL_QUOTE',
        'symbol': symbol,
        'apikey': API_KEY
    }
    response = _make_api_request(params)
    if response:
        try:
            quote_data = response.json()
            global_quote = quote_data.get('Global Quote', {})
            
            if not global_quote:
                print(f"ERROR: No Global Quote data returned for {symbol}")
                return None
            
            # Extract key price information
            price_data = {
                'symbol': global_quote.get('01. symbol', symbol),
                'price': float(global_quote.get('05. price', 0)),
                'open': float(global_quote.get('02. open', 0)),
                'high': float(global_quote.get('03. high', 0)),
                'low': float(global_quote.get('04. low', 0)),
                'previous_close': float(global_quote.get('08. previous close', 0)),
                'change': float(global_quote.get('09. change', 0)),
                'change_percent': global_quote.get('10. change percent', '0%').rstrip('%'),
                'volume': int(global_quote.get('06. volume', 0)),
                'latest_trading_day': global_quote.get('07. latest trading day', ''),
                'timestamp': pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            
            return price_data
            
        except Exception as e:
            print(f"ERROR: Failed to process real-time price data for {symbol}: {e}")
    return None
