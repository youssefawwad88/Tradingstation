import os
import requests
import pandas as pd
from io import StringIO
import time

# Get the Alpha Vantage API key from the environment variables
API_KEY = os.getenv('ALPHA_VANTAGE_API_KEY')

def get_daily_data(ticker, outputsize='full'):
    """
    Fetches daily adjusted stock data from Alpha Vantage for a given ticker.
    """
    if not API_KEY:
        print(f"!!! FATAL for {ticker}: Alpha Vantage API Key not found in environment variables.")
        return pd.DataFrame()

    url = f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY_ADJUSTED&symbol={ticker}&outputsize={outputsize}&apikey={API_KEY}&datatype=csv'
    
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        
        csv_data = StringIO(response.text)
        df = pd.read_csv(csv_data)
        
        if df.empty or 'timestamp' not in df.columns:
            # This can happen if the API key is invalid or rate limit is hit
            print(f"Warning: No valid daily data returned for {ticker}. API Response: {response.text[:120]}")
            return pd.DataFrame()
            
        return df

    except requests.exceptions.RequestException as e:
        print(f"!!! Error fetching daily data for {ticker}: {e}")
        return pd.DataFrame()

def get_intraday_data(ticker, interval='1min', outputsize='full'):
    """
    Fetches intraday stock data from Alpha Vantage for a given ticker.
    """
    if not API_KEY:
        print(f"!!! FATAL for {ticker}: Alpha Vantage API Key not found in environment variables.")
        return pd.DataFrame()

    url = f'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol={ticker}&interval={interval}&outputsize={outputsize}&apikey={API_KEY}&datatype=csv'
    
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        
        csv_data = StringIO(response.text)
        df = pd.read_csv(csv_data)
        
        if df.empty or 'timestamp' not in df.columns:
            print(f"Warning: No valid intraday data for {ticker}. API Response: {response.text[:120]}")
            return pd.DataFrame()
            
        return df

    except requests.exceptions.RequestException as e:
        print(f"!!! Error fetching intraday data for {ticker}: {e}")
        return pd.DataFrame()
