"""
Market utilities for Trading Station.
AVWAP anchor helpers and market-specific calculations.
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Optional
from datetime import datetime, timedelta

from utils.logging_setup import get_logger
from utils.helpers import calculate_wick_body_ratios, calculate_gap_percentage, calculate_volume_metrics

logger = get_logger(__name__)

def find_power_candles(
    df: pd.DataFrame,
    body_threshold: float = 0.6,
    volume_percentile: int = 80,
    min_gap_percent: float = 2.0
) -> pd.DataFrame:
    """
    Find power candles suitable as AVWAP anchors.
    
    Args:
        df: DataFrame with daily OHLCV data
        body_threshold: Minimum body-to-range ratio
        volume_percentile: Minimum volume percentile
        min_gap_percent: Minimum gap percentage for consideration
        
    Returns:
        DataFrame with power candle indicators
    """
    if df.empty:
        return df.copy()
    
    df_analysis = df.copy()
    
    # Sort by date
    df_analysis = df_analysis.sort_values('date').reset_index(drop=True)
    
    # Calculate wick and body ratios
    df_analysis = calculate_wick_body_ratios(df_analysis)
    
    # Calculate volume metrics
    df_analysis = calculate_volume_metrics(df_analysis, lookback_days=20)
    
    # Calculate volume percentile
    df_analysis['volume_percentile'] = df_analysis['volume'].rolling(
        window=50, min_periods=10
    ).rank(pct=True) * 100
    
    # Calculate gap from previous close
    df_analysis['prev_close'] = df_analysis['close'].shift(1)
    df_analysis['gap_pct'] = df_analysis.apply(
        lambda row: calculate_gap_percentage(row['open'], row['prev_close']) 
        if pd.notna(row['prev_close']) else 0.0, 
        axis=1
    )
    
    # Calculate daily range percentage
    df_analysis['range_pct'] = ((df_analysis['high'] - df_analysis['low']) / df_analysis['close']) * 100
    
    # Identify power candles
    df_analysis['is_power_candle'] = (
        (df_analysis['body_to_range_ratio'] >= body_threshold) &
        (df_analysis['volume_percentile'] >= volume_percentile)
    )
    
    # Enhanced power candles (with gap)
    df_analysis['is_enhanced_power_candle'] = (
        df_analysis['is_power_candle'] &
        (abs(df_analysis['gap_pct']) >= min_gap_percent)
    )
    
    # Calculate power candle score
    df_analysis['power_score'] = 0.0
    
    # Body strength (0-30 points)
    df_analysis.loc[df_analysis['body_to_range_ratio'] >= 0.8, 'power_score'] += 30
    df_analysis.loc[
        (df_analysis['body_to_range_ratio'] >= 0.6) & 
        (df_analysis['body_to_range_ratio'] < 0.8), 'power_score'
    ] += 20
    
    # Volume strength (0-30 points)
    df_analysis.loc[df_analysis['volume_percentile'] >= 90, 'power_score'] += 30
    df_analysis.loc[
        (df_analysis['volume_percentile'] >= 80) & 
        (df_analysis['volume_percentile'] < 90), 'power_score'
    ] += 20
    
    # Gap strength (0-25 points)
    df_analysis.loc[abs(df_analysis['gap_pct']) >= 5.0, 'power_score'] += 25
    df_analysis.loc[
        (abs(df_analysis['gap_pct']) >= 2.0) & 
        (abs(df_analysis['gap_pct']) < 5.0), 'power_score'
    ] += 15
    
    # Range strength (0-15 points)
    df_analysis.loc[df_analysis['range_pct'] >= 5.0, 'power_score'] += 15
    df_analysis.loc[
        (df_analysis['range_pct'] >= 3.0) & 
        (df_analysis['range_pct'] < 5.0), 'power_score'
    ] += 10
    
    return df_analysis

def get_avwap_anchors_for_ticker(
    df: pd.DataFrame,
    ticker: str,
    lookback_days: int = 100,
    min_score: float = 50.0
) -> List[Dict]:
    """
    Get AVWAP anchor points for a specific ticker.
    
    Args:
        df: Daily OHLCV DataFrame
        ticker: Ticker symbol
        lookback_days: Number of days to look back
        min_score: Minimum power score for anchor
        
    Returns:
        List of anchor point dictionaries
    """
    if df.empty:
        return []
    
    # Filter to recent data
    if 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'])
        cutoff_date = df['date'].max() - timedelta(days=lookback_days)
        df_recent = df[df['date'] >= cutoff_date].copy()
    else:
        df_recent = df.tail(lookback_days).copy()
    
    # Find power candles
    df_power = find_power_candles(df_recent)
    
    # Filter for significant anchors
    anchors_df = df_power[
        (df_power['is_power_candle']) & 
        (df_power['power_score'] >= min_score)
    ].copy()
    
    anchors = []
    for _, row in anchors_df.iterrows():
        anchor = {
            'ticker': ticker,
            'anchor_date': row['date'],
            'anchor_price': row['close'],
            'anchor_score': row['power_score'],
            'high': row['high'],
            'low': row['low'],
            'volume': row['volume'],
            'body_ratio': row['body_to_range_ratio'],
            'volume_percentile': row['volume_percentile'],
            'gap_pct': row['gap_pct'],
            'range_pct': row['range_pct'],
            'is_enhanced': row['is_enhanced_power_candle']
        }
        anchors.append(anchor)
    
    # Sort by score descending
    anchors.sort(key=lambda x: x['anchor_score'], reverse=True)
    
    return anchors

def calculate_avwap(
    df: pd.DataFrame,
    anchor_price: float,
    anchor_date: datetime,
    timestamp_col: str = 'timestamp'
) -> pd.DataFrame:
    """
    Calculate Anchored VWAP from a specific anchor point.
    
    Args:
        df: Intraday OHLCV DataFrame
        anchor_price: Anchor price point
        anchor_date: Anchor date
        timestamp_col: Name of timestamp column
        
    Returns:
        DataFrame with AVWAP column added
    """
    if df.empty:
        return df.copy()
    
    df_calc = df.copy()
    df_calc[timestamp_col] = pd.to_datetime(df_calc[timestamp_col])
    
    # Filter data from anchor date forward
    anchor_dt = pd.to_datetime(anchor_date)
    df_calc = df_calc[df_calc[timestamp_col] >= anchor_dt].copy()
    
    if df_calc.empty:
        logger.warning(f"No data available from anchor date {anchor_date}")
        return df.copy()
    
    # Calculate typical price
    df_calc['typical_price'] = (df_calc['high'] + df_calc['low'] + df_calc['close']) / 3
    
    # Calculate price * volume
    df_calc['pv'] = df_calc['typical_price'] * df_calc['volume']
    
    # Calculate cumulative sums from anchor point
    df_calc['cum_pv'] = df_calc['pv'].cumsum()
    df_calc['cum_volume'] = df_calc['volume'].cumsum()
    
    # Calculate AVWAP
    df_calc['avwap'] = np.where(
        df_calc['cum_volume'] > 0,
        df_calc['cum_pv'] / df_calc['cum_volume'],
        anchor_price
    )
    
    # Add back to original DataFrame
    df_result = df.copy()
    if timestamp_col in df_result.columns:
        df_result = df_result.merge(
            df_calc[[timestamp_col, 'avwap']], 
            on=timestamp_col, 
            how='left'
        )
    
    return df_result

def identify_avwap_reclaim(
    df: pd.DataFrame,
    avwap_col: str = 'avwap',
    price_col: str = 'close',
    volume_threshold: float = 1.5
) -> pd.DataFrame:
    """
    Identify AVWAP reclaim signals.
    
    Args:
        df: DataFrame with price and AVWAP data
        avwap_col: Name of AVWAP column
        price_col: Name of price column to compare
        volume_threshold: Minimum volume ratio for valid reclaim
        
    Returns:
        DataFrame with reclaim signals
    """
    if df.empty or avwap_col not in df.columns:
        return df.copy()
    
    df_signals = df.copy()
    
    # Calculate if price is above/below AVWAP
    df_signals['above_avwap'] = df_signals[price_col] > df_signals[avwap_col]
    df_signals['below_avwap'] = df_signals[price_col] < df_signals[avwap_col]
    
    # Find transitions
    df_signals['was_below'] = df_signals['below_avwap'].shift(1)
    df_signals['was_above'] = df_signals['above_avwap'].shift(1)
    
    # Reclaim signals
    df_signals['avwap_reclaim'] = (
        df_signals['was_below'] & 
        df_signals['above_avwap']
    )
    
    df_signals['avwap_rejection'] = (
        df_signals['was_above'] & 
        df_signals['below_avwap']
    )
    
    # Add volume confirmation if volume data available
    if 'volume_ratio' in df_signals.columns:
        df_signals['avwap_reclaim_confirmed'] = (
            df_signals['avwap_reclaim'] & 
            (df_signals['volume_ratio'] >= volume_threshold)
        )
        
        df_signals['avwap_rejection_confirmed'] = (
            df_signals['avwap_rejection'] & 
            (df_signals['volume_ratio'] >= volume_threshold)
        )
    
    return df_signals

def calculate_volume_profile(
    df: pd.DataFrame,
    price_col: str = 'close',
    volume_col: str = 'volume',
    num_levels: int = 20
) -> Dict[str, any]:
    """
    Calculate volume profile for price levels.
    
    Args:
        df: DataFrame with price and volume data
        price_col: Name of price column
        volume_col: Name of volume column
        num_levels: Number of price levels to create
        
    Returns:
        Dictionary with volume profile data
    """
    if df.empty:
        return {}
    
    min_price = df[price_col].min()
    max_price = df[price_col].max()
    
    # Create price bins
    price_bins = np.linspace(min_price, max_price, num_levels + 1)
    df_profile = df.copy()
    
    # Assign price levels
    df_profile['price_level'] = pd.cut(
        df_profile[price_col], 
        bins=price_bins, 
        include_lowest=True,
        labels=range(num_levels)
    )
    
    # Calculate volume at each level
    volume_by_level = df_profile.groupby('price_level')[volume_col].sum()
    
    # Calculate level midpoints
    level_prices = []
    for i in range(num_levels):
        level_price = (price_bins[i] + price_bins[i + 1]) / 2
        level_prices.append(level_price)
    
    # Find point of control (highest volume level)
    poc_level = volume_by_level.idxmax()
    poc_price = level_prices[poc_level] if pd.notna(poc_level) else min_price
    poc_volume = volume_by_level.max()
    
    # Calculate value area (70% of volume)
    total_volume = volume_by_level.sum()
    value_area_volume = total_volume * 0.7
    
    # Find value area high and low
    sorted_levels = volume_by_level.sort_values(ascending=False)
    cumulative_volume = 0
    value_area_levels = []
    
    for level, volume in sorted_levels.items():
        cumulative_volume += volume
        value_area_levels.append(level)
        if cumulative_volume >= value_area_volume:
            break
    
    value_area_high = max([level_prices[lvl] for lvl in value_area_levels])
    value_area_low = min([level_prices[lvl] for lvl in value_area_levels])
    
    return {
        'price_levels': level_prices,
        'volume_by_level': volume_by_level.tolist(),
        'poc_price': poc_price,
        'poc_volume': poc_volume,
        'value_area_high': value_area_high,
        'value_area_low': value_area_low,
        'total_volume': total_volume
    }

def detect_breakout_pattern(
    df: pd.DataFrame,
    lookback_periods: int = 20,
    volume_threshold: float = 2.0,
    price_threshold: float = 0.02  # 2% above resistance
) -> pd.DataFrame:
    """
    Detect breakout patterns with volume confirmation.
    
    Args:
        df: DataFrame with OHLCV data
        lookback_periods: Periods to look back for resistance
        volume_threshold: Minimum volume ratio for confirmation
        price_threshold: Minimum price move above resistance
        
    Returns:
        DataFrame with breakout signals
    """
    if df.empty or len(df) < lookback_periods:
        return df.copy()
    
    df_signals = df.copy()
    
    # Calculate rolling resistance (highest high)
    df_signals['resistance'] = df_signals['high'].rolling(
        window=lookback_periods
    ).max().shift(1)
    
    # Calculate rolling support (lowest low)
    df_signals['support'] = df_signals['low'].rolling(
        window=lookback_periods
    ).min().shift(1)
    
    # Detect breakouts
    df_signals['breakout_above'] = (
        df_signals['close'] > df_signals['resistance'] * (1 + price_threshold)
    )
    
    df_signals['breakdown_below'] = (
        df_signals['close'] < df_signals['support'] * (1 - price_threshold)
    )
    
    # Add volume confirmation if available
    if 'volume_ratio' in df_signals.columns:
        df_signals['breakout_confirmed'] = (
            df_signals['breakout_above'] & 
            (df_signals['volume_ratio'] >= volume_threshold)
        )
        
        df_signals['breakdown_confirmed'] = (
            df_signals['breakdown_below'] & 
            (df_signals['volume_ratio'] >= volume_threshold)
        )
    
    return df_signals

# Export functions
__all__ = [
    'find_power_candles',
    'get_avwap_anchors_for_ticker',
    'calculate_avwap',
    'identify_avwap_reclaim',
    'calculate_volume_profile',
    'detect_breakout_pattern'
]