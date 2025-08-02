import os
import requests
import pandas as pd
from io import StringIO
import time

# Load the API key from environment variables
API_KEY = os.getenv('ALPHA_VANTAGE_API_KEY')
BASE_URL = 'https://www.alphavantage.co/query'

def get_daily_data(symbol, outputsize='compact'):
    """Fetches daily adjusted time series data for a given symbol."""
    if not API_KEY:
        print("ERROR: ALPHA_VANTAGE_API_KEY not set.")
        return pd.DataFrame()
    
    params = {
        'function': 'TIME_SERIES_DAILY_ADJUSTED',
        'symbol': symbol,
        'outputsize': outputsize,
        'apikey': API_KEY,
        'datatype': 'csv'
    }
    try:
        response = requests.get(BASE_URL, params=params)
        response.raise_for_status()
        df = pd.read_csv(StringIO(response.text))
        # Handle API error messages which come as JSON in a CSV request
        if 'Error Message' in df.columns or df.empty:
            print(f"Warning: API returned an error or empty data for {symbol} daily.")
            return pd.DataFrame()
        return df
    except requests.exceptions.RequestException as e:
        print(f"ERROR: HTTP request failed for {symbol} daily data: {e}")
        return pd.DataFrame()
    except Exception as e:
        print(f"ERROR: Failed to process daily data for {symbol}: {e}")
        return pd.DataFrame()

def get_intraday_data(symbol, interval='1min', outputsize='compact'):
    """Fetches intraday time series data for a given symbol."""
    if not API_KEY:
        print("ERROR: ALPHA_VANTAGE_API_KEY not set.")
        return pd.DataFrame()
        
    params = {
        'function': 'TIME_SERIES_INTRADAY',
        'symbol': symbol,
        'interval': interval,
        'outputsize': outputsize,
        'apikey': API_KEY,
        'datatype': 'csv'
    }
    try:
        response = requests.get(BASE_URL, params=params)
        response.raise_for_status()
        df = pd.read_csv(StringIO(response.text))
        if 'Error Message' in df.columns or df.empty:
            print(f"Warning: API returned an error or empty data for {symbol} intraday {interval}.")
            return pd.DataFrame()
        return df
    except requests.exceptions.RequestException as e:
        print(f"ERROR: HTTP request failed for {symbol} intraday {interval} data: {e}")
        return pd.DataFrame()
    except Exception as e:
        print(f"ERROR: Failed to process intraday data for {symbol}: {e}")
        return pd.DataFrame()

def get_company_overview(symbol):
    """Fetches company overview data (Market Cap, Float, etc.) for a given symbol."""
    if not API_KEY:
        print("ERROR: ALPHA_VANTAGE_API_KEY not set.")
        return None
        
    params = {
        'function': 'OVERVIEW',
        'symbol': symbol,
        'apikey': API_KEY
    }
    try:
        response = requests.get(BASE_URL, params=params)
        response.raise_for_status()
        overview_data = response.json()
        if not overview_data or "MarketCapitalization" not in overview_data:
            print(f"Warning: No valid overview data returned for {symbol}.")
            return None
        return overview_data
    except requests.exceptions.RequestException as e:
        print(f"ERROR: HTTP request failed for {symbol} overview data: {e}")
        return None
    except Exception as e:
        print(f"ERROR: Failed to process overview data for {symbol}: {e}")
        return None
