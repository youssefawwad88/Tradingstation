# /drive/MyDrive/trading-system/utils/helpers.py
import pandas as pd
import numpy as np
from datetime import datetime, time
import pytz
from . import config

# --- General Utilities ---
def is_valid_number(n) -> bool:
    """Checks if a value is a valid, finite number."""
    return isinstance(n, (int, float)) and np.isfinite(n)

def format_to_two_decimal(value) -> str:
    """Formats a number to 2 decimal places, returning 'N/A' if invalid."""
    return f"{value:.2f}" if is_valid_number(value) else "N/A"

# --- Time and Session Utilities ---
def detect_market_session() -> str:
    """Detects the current market session based on New York time."""
    ny_timezone = pytz.timezone('America/New_York')
    ny_time = datetime.now(ny_timezone).time()

    if config.PREMARKET_START <= ny_time < config.REGULAR_SESSION_START:
        return 'PREMARKET'
    if config.REGULAR_SESSION_START <= ny_time < config.REGULAR_SESSION_END:
        return 'REGULAR'
    
    return 'CLOSED'

# --- Technical Indicator Functions ---

def calculate_vwap(df: pd.DataFrame) -> pd.Series:
    """Calculates the Volume Weighted Average Price (VWAP) for a given DataFrame."""
    required_cols = [config.HIGH_COL, config.LOW_COL, config.CLOSE_COL, config.VOLUME_COL]
    if not all(col in df.columns for col in required_cols):
        raise ValueError("DataFrame must contain High, Low, Close, and Volume columns for VWAP.")
    
    typical_price = (df[config.HIGH_COL] + df[config.LOW_COL] + df[config.CLOSE_COL]) / 3
    cumulative_volume = df[config.VOLUME_COL].cumsum()
    cumulative_tp_volume = (typical_price * df[config.VOLUME_COL]).cumsum()
    
    vwap = cumulative_tp_volume / cumulative_volume
    vwap.name = 'VWAP'
    return vwap

def calculate_avg_daily_volume(daily_df: pd.DataFrame, period: int = 10) -> float | None:
    """Calculates the average daily volume over a given period."""
    if daily_df.empty or len(daily_df) < period:
        return None
    
    avg_vol = daily_df.tail(period)[config.VOLUME_COL].mean()
    return avg_vol if is_valid_number(avg_vol) else None

def calculate_avg_early_volume(intraday_df: pd.DataFrame, days: int = 5) -> float | None:
    """
    Calculates the average volume for the early session (9:30-9:44 AM) over the last N days.
    This is a critical component for the 'Live Volume Spike' calculation.
    """
    if intraday_df.empty:
        return None
    
    # Define the time window for early volume
    early_session_start = time(9, 30)
    early_session_end = time(9, 44)

    # Filter for the specific time window
    early_df = intraday_df.between_time(early_session_start, early_session_end)
    
    if early_df.empty:
        return 0

    # Group by date, sum the volume for each day, and get the last N days
    daily_early_volume = early_df.groupby(early_df.index.date)[config.VOLUME_COL].sum().tail(days)
    
    if daily_early_volume.empty:
        return 0

    # Return the average of those daily sums
    return daily_early_volume.mean()


# --- DataFrame Specific Helpers ---

def get_premarket_data(df: pd.DataFrame) -> pd.DataFrame:
    """Filters an intraday DataFrame to return only pre-market data."""
    return df.between_time(config.PREMARKET_START, config.REGULAR_SESSION_START)

def get_previous_day_close(daily_df: pd.DataFrame) -> float | None:
    """Gets the previous trading day's closing price from a daily DataFrame."""
    if daily_df.empty:
        return None
    
    previous_close = daily_df[config.CLOSE_COL].iloc[-1]
    return previous_close if is_valid_number(previous_close) else None

def save_signal_to_csv(signal_df: pd.DataFrame, screener_name: str):
    """Saves a DataFrame of signals to a dedicated CSV file for that screener."""
    if signal_df.empty:
        print(f"No new signals to save for {screener_name}.")
        return
    
    output_path = config.SIGNALS_DIR / f"{screener_name}_signals.csv"
    print(f"Saving {len(signal_df)} signal(s) from {screener_name} to {output_path}...")
    
    signal_df.to_csv(output_path, index=False)
