"""
S&P 500 Ticker Loader Utility

This utility loads the list of S&P 500 stocks from a local CSV file,
which serves as a stable universe for analysis. This version is robust
and can handle CSVs with or without a 'Symbol' header.
"""

import pandas as pd
import os

# --- System Path Setup ---
# This allows the script to be run from anywhere and still find the project root.
try:
    # This will work when the script is run directly
    PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
except NameError:
    # This will work in an interactive environment like a notebook
    PROJECT_ROOT = '/content/drive/MyDrive/trading-system'

def load_sp500_tickers() -> list[str]:
    """
    Loads the list of S&P 500 ticker symbols from the predefined CSV file.
    It intelligently finds the symbol column, even without a specific header.

    Returns:
        list[str]: A list of unique S&P 500 ticker symbols.
    """
    # Construct the absolute path to the CSV file
    csv_path = os.path.join(PROJECT_ROOT, 'data', 'universe', 'sp500.csv')
    
    print(f"--> Loading S&P 500 tickers from {csv_path}")
    
    try:
        if not os.path.exists(csv_path):
            print(f"    - WARNING: S&P 500 ticker file not found at {csv_path}. Returning empty list.")
            return []
            
        df = pd.read_csv(csv_path)
        
        # FIX: Make the loader more robust.
        # First, try to find a column named 'Symbol'.
        if 'Symbol' in df.columns:
            tickers = df['Symbol'].dropna().unique().tolist()
            print(f"    - Successfully loaded {len(tickers)} unique S&P 500 tickers from 'Symbol' column.")
            return tickers
        # If 'Symbol' column doesn't exist, assume the first column contains the tickers.
        elif not df.empty and len(df.columns) > 0:
            tickers = df.iloc[:, 0].dropna().unique().tolist()
            print(f"    - WARNING: 'Symbol' column not found. Using first column as ticker source.")
            print(f"    - Successfully loaded {len(tickers)} unique S&P 500 tickers.")
            return tickers
        else:
            print("    - ERROR: sp500.csv is empty or has no columns.")
            return []
            
    except Exception as e:
        print(f"    - ERROR loading or parsing sp500.csv: {e}")
        return []

if __name__ == '__main__':
    # For testing purposes
    sp500_list = load_sp500_tickers()
    print(f"Loaded {len(sp500_list)} tickers.")
    if sp500_list:
        print("Sample:", sp500_list[:5])
