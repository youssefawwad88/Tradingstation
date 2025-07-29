import os
import requests
import pandas as pd
from io import StringIO

# Get the Alpha Vantage API key from the environment variables
API_KEY = os.getenv('ALPHA_VANTAGE_API_KEY', 'YOUR_DEFAULT_KEY') 
# The default key is a fallback, but the app will use the encrypted one from DO.

def get_daily_data(ticker):
    """
    Fetches daily adjusted stock data from Alpha Vantage for a given ticker.
    """
    if API_KEY == 'YOUR_DEFAULT_KEY':
        print("!!! Alpha Vantage API Key not found in environment variables.")
        return pd.DataFrame()

    print(f"Fetching daily data for {ticker}...")
    url = f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY_ADJUSTED&symbol={ticker}&outputsize=full&apikey={API_KEY}&datatype=csv'
    
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()  # Raise an exception for bad status codes (4xx or 5xx)
        
        # Use StringIO to read the CSV content directly into pandas
        csv_data = StringIO(response.text)
        df = pd.read_csv(csv_data)
        
        if df.empty or 'timestamp' not in df.columns:
            print(f"Warning: No data returned for {ticker}. The response might be an error message from the API.")
            return pd.DataFrame()
            
        print(f"Successfully fetched {len(df)} data points for {ticker}.")
        return df

    except requests.exceptions.RequestException as e:
        print(f"!!! Error fetching data for {ticker}: {e}")
        return pd.DataFrame()
    except Exception as e:
        print(f"!!! An unexpected error occurred while processing data for {ticker}: {e}")
        return pd.DataFrame()

