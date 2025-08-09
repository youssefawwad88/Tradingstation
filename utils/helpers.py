"""
Helper utilities for Trading Station.
Common data transformations, technical indicators, and analysis functions.
"""

import pandas as pd
import numpy as np
from typing import Optional, List, Dict, Any, Tuple
from datetime import datetime, timedelta
import logging

from .time_utils import to_et, get_session_type, ORB_RANGE_START, ORB_RANGE_END
from .logging_setup import get_logger

logger = get_logger(__name__)

def resample_1m_to_30m(df: pd.DataFrame, timestamp_col: str = 'timestamp') -> pd.DataFrame:
    """
    Resample 1-minute data to 30-minute bars.
    
    Args:
        df: DataFrame with 1-minute OHLCV data
        timestamp_col: Name of timestamp column
        
    Returns:
        DataFrame with 30-minute bars
    """
    if df.empty:
        return df.copy()
    
    # Ensure timestamp is datetime and timezone-aware
    df = df.copy()
    df[timestamp_col] = pd.to_datetime(df[timestamp_col], utc=True)
    
    # Set timestamp as index for resampling
    df_indexed = df.set_index(timestamp_col)
    
    # Resample to 30-minute bars
    resampled = df_indexed.resample('30T', label='left', closed='left').agg({
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last',
        'volume': 'sum'
    }).dropna()
    
    # Reset index to get timestamp column back
    result = resampled.reset_index()
    
    logger.debug(f"Resampled {len(df)} 1m bars to {len(result)} 30m bars")
    return result

def add_session_labels(df: pd.DataFrame, timestamp_col: str = 'timestamp') -> pd.DataFrame:
    """
    Add session labels (pre, regular, post) to time series data.
    
    Args:
        df: DataFrame with timestamp column
        timestamp_col: Name of timestamp column
        
    Returns:
        DataFrame with additional 'session' and 'day_id' columns
    """
    if df.empty:
        return df.copy()
    
    df = df.copy()
    df[timestamp_col] = pd.to_datetime(df[timestamp_col], utc=True)
    
    # Convert to ET for session determination
    df['timestamp_et'] = df[timestamp_col].dt.tz_convert('US/Eastern')
    
    # Add session labels
    df['session'] = df['timestamp_et'].apply(lambda x: get_session_type(x))
    
    # Add day_id (YYYY-MM-DD in ET)
    df['day_id'] = df['timestamp_et'].dt.strftime('%Y-%m-%d')
    
    # Clean up temporary column
    df = df.drop('timestamp_et', axis=1)
    
    return df

def calculate_vwap(df: pd.DataFrame, session_scoped: bool = True) -> pd.DataFrame:
    """
    Calculate Volume Weighted Average Price (VWAP).
    
    Args:
        df: DataFrame with OHLCV data
        session_scoped: If True, reset VWAP calculation for each session
        
    Returns:
        DataFrame with additional 'vwap' column
    """
    if df.empty or not all(col in df.columns for col in ['high', 'low', 'close', 'volume']):
        return df.copy()
    
    df = df.copy()
    
    # Calculate typical price
    df['typical_price'] = (df['high'] + df['low'] + df['close']) / 3
    df['price_volume'] = df['typical_price'] * df['volume']
    
    if session_scoped and 'session' in df.columns:
        # Calculate VWAP per session
        df['cumulative_pv'] = df.groupby(['day_id', 'session'])['price_volume'].cumsum()
        df['cumulative_volume'] = df.groupby(['day_id', 'session'])['volume'].cumsum()
    else:
        # Calculate overall VWAP
        df['cumulative_pv'] = df['price_volume'].cumsum()
        df['cumulative_volume'] = df['volume'].cumsum()
    
    # Calculate VWAP (avoid division by zero)
    df['vwap'] = np.where(
        df['cumulative_volume'] > 0,
        df['cumulative_pv'] / df['cumulative_volume'],
        df['typical_price']
    )
    
    # Clean up temporary columns
    df = df.drop(['typical_price', 'price_volume', 'cumulative_pv', 'cumulative_volume'], axis=1)
    
    return df

def calculate_ema(series: pd.Series, period: int) -> pd.Series:
    """Calculate Exponential Moving Average."""
    return series.ewm(span=period, adjust=False).mean()

def calculate_sma(series: pd.Series, period: int) -> pd.Series:
    """Calculate Simple Moving Average."""
    return series.rolling(window=period).mean()

def calculate_bollinger_bands(
    df: pd.DataFrame, 
    period: int = 20, 
    std_dev: float = 2.0,
    price_col: str = 'close'
) -> pd.DataFrame:
    """
    Calculate Bollinger Bands.
    
    Args:
        df: DataFrame with price data
        period: Moving average period
        std_dev: Standard deviation multiplier
        price_col: Column to use for calculation
        
    Returns:
        DataFrame with bb_upper, bb_middle, bb_lower columns
    """
    if df.empty or price_col not in df.columns:
        return df.copy()
    
    df = df.copy()
    
    # Calculate middle band (SMA)
    df['bb_middle'] = calculate_sma(df[price_col], period)
    
    # Calculate standard deviation
    rolling_std = df[price_col].rolling(window=period).std()
    
    # Calculate upper and lower bands
    df['bb_upper'] = df['bb_middle'] + (rolling_std * std_dev)
    df['bb_lower'] = df['bb_middle'] - (rolling_std * std_dev)
    
    return df

def calculate_wick_body_ratios(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate wick and body ratios for candlestick analysis.
    
    Returns:
        DataFrame with additional columns: body_size, upper_wick, lower_wick,
        body_to_range_ratio, upper_wick_ratio, lower_wick_ratio
    """
    if df.empty or not all(col in df.columns for col in ['open', 'high', 'low', 'close']):
        return df.copy()
    
    df = df.copy()
    
    # Calculate body and wick sizes
    df['body_size'] = abs(df['close'] - df['open'])
    df['upper_wick'] = df['high'] - np.maximum(df['open'], df['close'])
    df['lower_wick'] = np.minimum(df['open'], df['close']) - df['low']
    
    # Calculate total range
    df['total_range'] = df['high'] - df['low']
    
    # Calculate ratios (avoid division by zero)
    df['body_to_range_ratio'] = np.where(
        df['total_range'] > 0,
        df['body_size'] / df['total_range'],
        0
    )
    
    df['upper_wick_ratio'] = np.where(
        df['total_range'] > 0,
        df['upper_wick'] / df['total_range'],
        0
    )
    
    df['lower_wick_ratio'] = np.where(
        df['total_range'] > 0,
        df['lower_wick'] / df['total_range'],
        0
    )
    
    return df

def calculate_volume_metrics(df: pd.DataFrame, lookback_days: int = 5) -> pd.DataFrame:
    """
    Calculate volume-based metrics.
    
    Args:
        df: DataFrame with volume data
        lookback_days: Number of days for rolling averages
        
    Returns:
        DataFrame with volume_sma, volume_ratio, volume_zscore columns
    """
    if df.empty or 'volume' not in df.columns:
        return df.copy()
    
    df = df.copy()
    
    # Calculate rolling volume average
    window = lookback_days if 'day_id' not in df.columns else lookback_days * 390  # 390 minutes per trading day
    df['volume_sma'] = df['volume'].rolling(window=window, min_periods=1).mean()
    
    # Calculate volume ratio vs average
    df['volume_ratio'] = np.where(
        df['volume_sma'] > 0,
        df['volume'] / df['volume_sma'],
        1.0
    )
    
    # Calculate volume z-score
    volume_std = df['volume'].rolling(window=window, min_periods=1).std()
    df['volume_zscore'] = np.where(
        volume_std > 0,
        (df['volume'] - df['volume_sma']) / volume_std,
        0.0
    )
    
    return df

def extract_opening_range(df: pd.DataFrame, start_time: str = None, end_time: str = None) -> Dict[str, float]:
    """
    Extract opening range (9:30-9:39 ET by default) high and low.
    
    Args:
        df: DataFrame with intraday data
        start_time: Start time in HH:MM:SS format (ET)
        end_time: End time in HH:MM:SS format (ET)
        
    Returns:
        Dictionary with or_high, or_low, or_range
    """
    if df.empty or 'timestamp' not in df.columns:
        return {'or_high': None, 'or_low': None, 'or_range': None}
    
    # Use default ORB times if not specified
    if start_time is None:
        start_time = ORB_RANGE_START.strftime('%H:%M:%S')
    if end_time is None:
        end_time = ORB_RANGE_END.strftime('%H:%M:%S')
    
    # Convert timestamps to ET
    df_et = df.copy()
    df_et['timestamp_et'] = pd.to_datetime(df_et['timestamp']).dt.tz_convert('US/Eastern')
    df_et['time_only'] = df_et['timestamp_et'].dt.time
    
    # Filter for opening range
    start_time_obj = pd.to_datetime(start_time).time()
    end_time_obj = pd.to_datetime(end_time).time()
    
    or_data = df_et[
        (df_et['time_only'] >= start_time_obj) & 
        (df_et['time_only'] <= end_time_obj)
    ]
    
    if or_data.empty:
        return {'or_high': None, 'or_low': None, 'or_range': None}
    
    or_high = or_data['high'].max()
    or_low = or_data['low'].min()
    or_range = or_high - or_low
    
    return {'or_high': or_high, 'or_low': or_low, 'or_range': or_range}

def extract_premarket_levels(df: pd.DataFrame) -> Dict[str, float]:
    """
    Extract premarket high and low levels.
    
    Args:
        df: DataFrame with extended hours data
        
    Returns:
        Dictionary with premkt_high, premkt_low
    """
    if df.empty or 'session' not in df.columns:
        return {'premkt_high': None, 'premkt_low': None}
    
    premkt_data = df[df['session'] == 'pre']
    
    if premkt_data.empty:
        return {'premkt_high': None, 'premkt_low': None}
    
    return {
        'premkt_high': premkt_data['high'].max(),
        'premkt_low': premkt_data['low'].min()
    }

def calculate_gap_percentage(current_open: float, prev_close: float) -> float:
    """Calculate gap percentage."""
    if prev_close <= 0:
        return 0.0
    return ((current_open - prev_close) / prev_close) * 100

def calculate_atr(df: pd.DataFrame, period: int = 14) -> pd.DataFrame:
    """
    Calculate Average True Range (ATR).
    
    Args:
        df: DataFrame with OHLC data
        period: ATR period
        
    Returns:
        DataFrame with atr column
    """
    if df.empty or not all(col in df.columns for col in ['high', 'low', 'close']):
        return df.copy()
    
    df = df.copy()
    
    # Calculate True Range components
    df['prev_close'] = df['close'].shift(1)
    
    tr1 = df['high'] - df['low']
    tr2 = abs(df['high'] - df['prev_close'])
    tr3 = abs(df['low'] - df['prev_close'])
    
    # True Range is the maximum of the three
    df['true_range'] = np.maximum(tr1, np.maximum(tr2, tr3))
    
    # Calculate ATR as exponential moving average of True Range
    df['atr'] = df['true_range'].ewm(span=period, adjust=False).mean()
    
    # Clean up temporary columns
    df = df.drop(['prev_close', 'true_range'], axis=1)
    
    return df

def calculate_r_multiple_targets(entry: float, stop: float, direction: str) -> Dict[str, float]:
    """
    Calculate R-multiple targets (2R, 3R) based on entry and stop.
    
    Args:
        entry: Entry price
        stop: Stop loss price
        direction: 'long' or 'short'
        
    Returns:
        Dictionary with t1_2R, t2_3R, r_multiple
    """
    if entry <= 0 or stop <= 0:
        return {'t1_2R': None, 't2_3R': None, 'r_multiple': None}
    
    # Calculate 1R (risk per share)
    if direction.lower() == 'long':
        risk_per_share = entry - stop
        if risk_per_share <= 0:
            return {'t1_2R': None, 't2_3R': None, 'r_multiple': None}
        
        t1_2r = entry + (2 * risk_per_share)
        t2_3r = entry + (3 * risk_per_share)
        
    elif direction.lower() == 'short':
        risk_per_share = stop - entry
        if risk_per_share <= 0:
            return {'t1_2R': None, 't2_3R': None, 'r_multiple': None}
        
        t1_2r = entry - (2 * risk_per_share)
        t2_3r = entry - (3 * risk_per_share)
        
    else:
        return {'t1_2R': None, 't2_3R': None, 'r_multiple': None}
    
    return {
        't1_2R': round(t1_2r, 2),
        't2_3R': round(t2_3r, 2),
        'r_multiple': round(risk_per_share, 2)
    }

def calculate_avwap_anchor_score(
    df: pd.DataFrame,
    body_threshold: float = 0.6,
    volume_percentile: int = 80
) -> pd.DataFrame:
    """
    Calculate AVWAP anchor scores for daily bars.
    
    Args:
        df: DataFrame with daily OHLCV data
        body_threshold: Minimum body-to-range ratio
        volume_percentile: Minimum volume percentile
        
    Returns:
        DataFrame with anchor_score column
    """
    if df.empty:
        return df.copy()
    
    df = df.copy()
    
    # Calculate body-to-range ratio
    df = calculate_wick_body_ratios(df)
    
    # Calculate volume percentile
    df['volume_percentile'] = df['volume'].rolling(window=50, min_periods=10).rank(pct=True) * 100
    
    # Calculate anchor score
    df['anchor_score'] = 0.0
    
    # Add points for strong body
    df.loc[df['body_to_range_ratio'] >= body_threshold, 'anchor_score'] += 30
    
    # Add points for high volume
    df.loc[df['volume_percentile'] >= volume_percentile, 'anchor_score'] += 40
    
    # Add points for gap
    df['gap_pct'] = df.apply(
        lambda row: calculate_gap_percentage(row['open'], row['close']) 
        if 'prev_close' in df.columns else 0, axis=1
    )
    df.loc[abs(df['gap_pct']) >= 2.0, 'anchor_score'] += 20
    
    # Add points for range
    df['range_pct'] = ((df['high'] - df['low']) / df['close']) * 100
    df.loc[df['range_pct'] >= 3.0, 'anchor_score'] += 10
    
    return df

def get_latest_bar(df: pd.DataFrame, timestamp_col: str = 'timestamp') -> Optional[Dict[str, Any]]:
    """Get the most recent bar from a DataFrame."""
    if df.empty:
        return None
    
    latest_idx = df[timestamp_col].idxmax()
    return df.loc[latest_idx].to_dict()

def trim_to_retention_period(
    df: pd.DataFrame, 
    retention_days: int,
    timestamp_col: str = 'timestamp',
    include_today: bool = True
) -> pd.DataFrame:
    """
    Trim DataFrame to specified retention period.
    
    Args:
        df: DataFrame with timestamp column
        retention_days: Number of days to keep
        timestamp_col: Name of timestamp column
        include_today: Whether to always include today's data
        
    Returns:
        Trimmed DataFrame
    """
    if df.empty:
        return df.copy()
    
    df = df.copy()
    df[timestamp_col] = pd.to_datetime(df[timestamp_col])
    
    # Calculate cutoff date
    latest_date = df[timestamp_col].max().date()
    cutoff_date = latest_date - timedelta(days=retention_days)
    
    if include_today:
        # Always include data from latest trading day
        mask = (df[timestamp_col].dt.date >= cutoff_date) | (df[timestamp_col].dt.date == latest_date)
    else:
        mask = df[timestamp_col].dt.date >= cutoff_date
    
    trimmed_df = df[mask].copy()
    
    logger.debug(f"Trimmed data from {len(df)} to {len(trimmed_df)} rows (retention: {retention_days} days)")
    return trimmed_df

# Export functions
__all__ = [
    'resample_1m_to_30m', 'add_session_labels', 'calculate_vwap',
    'calculate_ema', 'calculate_sma', 'calculate_bollinger_bands',
    'calculate_wick_body_ratios', 'calculate_volume_metrics',
    'extract_opening_range', 'extract_premarket_levels',
    'calculate_gap_percentage', 'calculate_atr', 'calculate_r_multiple_targets',
    'calculate_avwap_anchor_score', 'get_latest_bar', 'trim_to_retention_period'
]