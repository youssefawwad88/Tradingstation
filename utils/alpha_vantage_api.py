# /drive/MyDrive/trading-system/utils/alpha_vantage_api.py
import requests
import pandas as pd
import time
from io import StringIO
from pathlib import Path
from . import config

SECONDS_BETWEEN_CALLS = 1.5

def _make_api_request(params: dict) -> dict | None:
    """A centralized function to make any API request and handle common errors."""
    try:
        response = requests.get(config.ALPHA_VANTAGE_BASE_URL, params=params)
        response.raise_for_status()
        data = response.json()
        if not data or "Error Message" in data or "Information" in data:
            print(f"    [!] API Error/Info: {data.get('Error Message') or data.get('Information')}")
            return None
        return data
    except requests.exceptions.RequestException as e:
        print(f"    [!] Network error during API request: {e}")
        return None
    finally:
        time.sleep(SECONDS_BETWEEN_CALLS)

def fetch_daily_data(ticker: str, outputsize: str = 'compact') -> pd.DataFrame | None:
    """Fetches daily data from Alpha Vantage."""
    print(f"--> Fetching DAILY data for {ticker}...")
    params = {"function": "TIME_SERIES_DAILY", "symbol": ticker, "outputsize": outputsize, "apikey": config.ALPHA_VANTAGE_API_KEY}
    data = _make_api_request(params)
    if data and "Time Series (Daily)" in data:
        df = pd.DataFrame.from_dict(data["Time Series (Daily)"], orient='index')
        df.rename(columns={'1. open': config.OPEN_COL, '2. high': config.HIGH_COL, '3. low': config.LOW_COL, '4. close': config.CLOSE_COL, '5. volume': config.VOLUME_COL}, inplace=True)
        for col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        df.index = pd.to_datetime(df.index)
        df.sort_index(ascending=True, inplace=True)
        df.index.name = config.DATE_COL
        print(f"    [✔] Successfully fetched {len(df)} rows of daily data for {ticker}.")
        return df
    return None

def fetch_intraday_data(ticker: str, interval: str = '1min', outputsize: str = 'compact') -> pd.DataFrame | None:
    """Fetches intraday data and returns a standardized DataFrame."""
    print(f"--> Fetching REAL-TIME {interval} data for {ticker}...")
    params = {"function": "TIME_SERIES_INTRADAY", "symbol": ticker, "interval": interval, "outputsize": outputsize, "apikey": config.ALPHA_VANTAGE_API_KEY, "datatype": "csv", "entitlement": "realtime"}
    try:
        response = requests.get(config.ALPHA_VANTAGE_BASE_URL, params=params)
        response.raise_for_status()
        if "Error Message" in response.text:
            print(f"    [!] API Error for {ticker}: {response.text}")
            return None
        df = pd.read_csv(StringIO(response.text))
        df.rename(columns={'timestamp': config.DATE_COL, 'open': config.OPEN_COL, 'high': config.HIGH_COL, 'low': config.LOW_COL, 'close': config.CLOSE_COL, 'volume': config.VOLUME_COL}, inplace=True)
        df[config.DATE_COL] = pd.to_datetime(df[config.DATE_COL])
        df.set_index(config.DATE_COL, inplace=True)
        df.sort_index(ascending=True, inplace=True)
        print(f"    [✔] Successfully fetched {len(df)} rows for {ticker}.")
        return df
    except requests.exceptions.RequestException as e:
        print(f"    [!] Network error fetching data for {ticker}: {e}")
        return None
    finally:
        time.sleep(SECONDS_BETWEEN_CALLS)

def fetch_and_save_data(ticker: str, data_dir: Path, fetch_function, max_rows: int | None = None, **kwargs):
    """
    Generic function to fetch, merge with existing data, and save.
    Includes logic to trim the initial fetch to a max number of rows.
    """
    interval = kwargs.get('interval', 'daily')
    output_path = data_dir / f"{ticker}_{interval}.csv"
    
    new_data = fetch_function(ticker, **kwargs)
    
    if new_data is None or new_data.empty:
        print(f"    No new data fetched for {ticker}. Nothing to save.")
        return

    if output_path.exists():
        existing_data = pd.read_csv(output_path, index_col=config.DATE_COL, parse_dates=True)
        rows_before = len(existing_data)
        print(f"    Merging data: Found {rows_before} existing rows in {output_path.name}.")
        
        combined_data = pd.concat([existing_data, new_data])
        combined_data = combined_data[~combined_data.index.duplicated(keep='last')]
        
        rows_after = len(combined_data)
        new_rows_added = rows_after - rows_before
        print(f"    Merge complete: Added {new_rows_added} new unique row(s). Total rows: {rows_after}.")
    else:
        # If the file doesn't exist, this is the initial fetch.
        # Trim the data down to max_rows before saving.
        if max_rows is not None and len(new_data) > max_rows:
            print(f"    Initial fetch: Trimming from {len(new_data)} rows to the latest {max_rows} rows.")
            combined_data = new_data.tail(max_rows)
        else:
            combined_data = new_data
        print(f"    File not found. Creating new file at {output_path.name} with {len(combined_data)} rows.")
        
    combined_data.sort_index(ascending=True, inplace=True)
    combined_data.to_csv(output_path)
    print(f"    [✔] Successfully saved data for {ticker} to {output_path.name}")
