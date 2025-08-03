import os
import requests
import pandas as pd
from io import StringIO
import time

# Load the API key from environment variables
API_KEY = os.getenv('ALPHA_VANTAGE_API_KEY')
BASE_URL = 'https://www.alphavantage.co/query'
REQUEST_TIMEOUT = 15 # Set a 15-second timeout for all API calls

def _make_api_request(params):
    """A centralized and robust function for making API requests."""
    if not API_KEY:
        print("CRITICAL ERROR: ALPHA_VANTAGE_API_KEY environment variable not set.")
        return None
    
    try:
        response = requests.get(BASE_URL, params=params, timeout=REQUEST_TIMEOUT)
        response.raise_for_status() # Raises an HTTPError for bad responses (4xx or 5xx)
        return response
    except requests.exceptions.Timeout:
        print(f"ERROR: API request timed out after {REQUEST_TIMEOUT} seconds for params: {params.get('symbol')}")
        return None
    except requests.exceptions.RequestException as e:
        print(f"ERROR: HTTP request failed for {params.get('symbol')}: {e}")
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
