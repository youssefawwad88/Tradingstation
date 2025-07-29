import os
import requests
import pandas as pd
from io import StringIO

# Get the Alpha Vantage API key from the environment variables
API_KEY = os.getenv('ALPHA_VANTAGE_API_KEY', 'YOUR_DEFAULT_KEY') 
# The default key is a fallback, but the app will use the encrypted one from DO.

def get_daily_data(ticker, outputsize='full'):
    """
    Fetches daily adjusted stock data from Alpha Vantage for a given ticker.
    """
    if API_KEY == 'YOUR_DEFAULT_KEY':
        print("!!! Alpha Vantage API Key not found in environment variables.")
        return pd.DataFrame()

    print(f"Fetching daily data for {ticker} (size: {outputsize})...")
    url = f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY_ADJUSTED&symbol={ticker}&outputsize={outputsize}&apikey={API_KEY}&datatype=csv'
    
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        
        csv_data = StringIO(response.text)
        df = pd.read_csv(csv_data)
        
        if df.empty or 'timestamp' not in df.columns:
            print(f"Warning: No data returned for {ticker}. The response might be an error message from the API.")
            return pd.DataFrame()
            
        print(f"Successfully fetched {len(df)} daily data points for {ticker}.")
        return df

    except requests.exceptions.RequestException as e:
        print(f"!!! Error fetching daily data for {ticker}: {e}")
        return pd.DataFrame()
    except Exception as e:
        print(f"!!! An unexpected error occurred while processing daily data for {ticker}: {e}")
        return pd.DataFrame()

def get_intraday_data(ticker, interval='1min', outputsize='full'):
    """
    Fetches intraday stock data from Alpha Vantage for a given ticker.
    'interval' can be '1min', '5min', '15min', '30min', '60min'.
    'outputsize' can be 'compact' or 'full'.
    """
    if API_KEY == 'YOUR_DEFAULT_KEY':
        print("!!! Alpha Vantage API Key not found in environment variables.")
        return pd.DataFrame()

    print(f"Fetching {interval} intraday data for {ticker} (size: {outputsize})...")
    url = f'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol={ticker}&interval={interval}&outputsize={outputsize}&apikey={API_KEY}&datatype=csv'
    
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        
        csv_data = StringIO(response.text)
        df = pd.read_csv(csv_data)
        
        if df.empty or 'timestamp' not in df.columns:
            print(f"Warning: No intraday data returned for {ticker}. The response might be an error message from the API.")
            return pd.DataFrame()
            
        print(f"Successfully fetched {len(df)} intraday data points for {ticker}.")
        return df

    except requests.exceptions.RequestException as e:
        print(f"!!! Error fetching intraday data for {ticker}: {e}")
        return pd.DataFrame()
    except Exception as e:
        print(f"!!! An unexpected error occurred while processing intraday data for {ticker}: {e}")
        return pd.DataFrame()
