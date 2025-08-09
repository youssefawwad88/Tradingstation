"""
Data validation utilities for Trading Station.
Validates data integrity, schemas, and business rules.
"""

import pandas as pd
import numpy as np
from typing import List, Dict, Any, Optional, Set
from datetime import datetime
import logging

from .logging_setup import get_logger

logger = get_logger(__name__)

class DataValidationError(Exception):
    """Exception raised for data validation failures."""
    
    def __init__(self, message: str, context: Dict[str, Any] = None):
        super().__init__(message)
        self.context = context or {}

class ValidationResult:
    """Result of a validation check."""
    
    def __init__(self, valid: bool, message: str = "", context: Dict[str, Any] = None):
        self.valid = valid
        self.message = message
        self.context = context or {}
    
    def __bool__(self):
        return self.valid

def validate_ticker_symbol(ticker: str) -> ValidationResult:
    """Validate ticker symbol format."""
    if not ticker or not isinstance(ticker, str):
        return ValidationResult(False, "Ticker must be a non-empty string")
    
    ticker = ticker.strip().upper()
    
    if not (1 <= len(ticker) <= 5):
        return ValidationResult(False, f"Ticker length must be 1-5 characters, got {len(ticker)}")
    
    if not ticker.isalpha():
        return ValidationResult(False, f"Ticker must contain only letters, got '{ticker}'")
    
    return ValidationResult(True, "Valid ticker symbol")

def validate_price_data(df: pd.DataFrame, ticker: str = None) -> ValidationResult:
    """
    Validate OHLCV price data integrity.
    
    Args:
        df: DataFrame with price data
        ticker: Optional ticker symbol for context
        
    Returns:
        ValidationResult indicating if data is valid
    """
    context = {'ticker': ticker, 'row_count': len(df)}
    
    if df.empty:
        return ValidationResult(False, "DataFrame is empty", context)
    
    # Check required columns
    required_cols = ['open', 'high', 'low', 'close', 'volume']
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        return ValidationResult(
            False, 
            f"Missing required columns: {missing_cols}",
            {**context, 'missing_columns': missing_cols}
        )
    
    # Check for NaN values in critical columns
    critical_cols = ['open', 'high', 'low', 'close']
    for col in critical_cols:
        nan_count = df[col].isna().sum()
        if nan_count > 0:
            return ValidationResult(
                False,
                f"Found {nan_count} NaN values in {col}",
                {**context, 'nan_column': col, 'nan_count': nan_count}
            )
    
    # Check data types
    numeric_cols = ['open', 'high', 'low', 'close', 'volume']
    for col in numeric_cols:
        if not pd.api.types.is_numeric_dtype(df[col]):
            return ValidationResult(
                False,
                f"Column {col} must be numeric",
                {**context, 'invalid_column': col, 'dtype': str(df[col].dtype)}
            )
    
    # Check price relationships (high >= low, etc.)
    invalid_prices = df[df['low'] > df['high']]
    if not invalid_prices.empty:
        return ValidationResult(
            False,
            f"Found {len(invalid_prices)} rows where low > high",
            {**context, 'invalid_price_rows': len(invalid_prices)}
        )
    
    # Check that open/close are within high/low range
    invalid_open = df[(df['open'] < df['low']) | (df['open'] > df['high'])]
    if not invalid_open.empty:
        return ValidationResult(
            False,
            f"Found {len(invalid_open)} rows where open is outside high/low range",
            {**context, 'invalid_open_rows': len(invalid_open)}
        )
    
    invalid_close = df[(df['close'] < df['low']) | (df['close'] > df['high'])]
    if not invalid_close.empty:
        return ValidationResult(
            False,
            f"Found {len(invalid_close)} rows where close is outside high/low range",
            {**context, 'invalid_close_rows': len(invalid_close)}
        )
    
    # Check for negative values
    negative_prices = df[(df[['open', 'high', 'low', 'close']] <= 0).any(axis=1)]
    if not negative_prices.empty:
        return ValidationResult(
            False,
            f"Found {len(negative_prices)} rows with non-positive prices",
            {**context, 'negative_price_rows': len(negative_prices)}
        )
    
    negative_volume = df[df['volume'] < 0]
    if not negative_volume.empty:
        return ValidationResult(
            False,
            f"Found {len(negative_volume)} rows with negative volume",
            {**context, 'negative_volume_rows': len(negative_volume)}
        )
    
    return ValidationResult(True, "Price data validation passed", context)

def validate_timestamp_monotonicity(df: pd.DataFrame, timestamp_col: str = 'timestamp') -> ValidationResult:
    """Validate that timestamps are monotonically increasing."""
    if df.empty or timestamp_col not in df.columns:
        return ValidationResult(False, f"Missing timestamp column: {timestamp_col}")
    
    # Convert to datetime if not already
    timestamps = pd.to_datetime(df[timestamp_col])
    
    # Check for monotonic increasing
    if not timestamps.is_monotonic_increasing:
        # Find first non-monotonic position
        diffs = timestamps.diff()
        negative_diffs = diffs[diffs < pd.Timedelta(0)]
        if not negative_diffs.empty:
            first_violation = negative_diffs.index[0]
            return ValidationResult(
                False,
                f"Timestamps not monotonic at index {first_violation}",
                {'violation_index': first_violation, 'timestamp_column': timestamp_col}
            )
    
    # Check for duplicates
    duplicates = timestamps.duplicated()
    if duplicates.any():
        duplicate_count = duplicates.sum()
        return ValidationResult(
            False,
            f"Found {duplicate_count} duplicate timestamps",
            {'duplicate_count': duplicate_count, 'timestamp_column': timestamp_col}
        )
    
    return ValidationResult(True, "Timestamp monotonicity validation passed")

def validate_signal_schema(df: pd.DataFrame) -> ValidationResult:
    """
    Validate signal data schema.
    
    Expected columns: as_of, ticker, direction, setup_valid, entry, stop, 
                     r_multiple, t1_2R, t2_3R, confidence
    """
    if df.empty:
        return ValidationResult(False, "Signal DataFrame is empty")
    
    required_cols = [
        'as_of', 'ticker', 'direction', 'setup_valid', 
        'entry', 'stop', 'r_multiple', 't1_2R', 't2_3R', 'confidence'
    ]
    
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        return ValidationResult(
            False,
            f"Missing required signal columns: {missing_cols}",
            {'missing_columns': missing_cols}
        )
    
    # Validate direction values
    valid_directions = {'long', 'short'}
    invalid_directions = df[~df['direction'].isin(valid_directions)]
    if not invalid_directions.empty:
        return ValidationResult(
            False,
            f"Invalid direction values found (must be 'long' or 'short')",
            {'invalid_direction_count': len(invalid_directions)}
        )
    
    # Validate boolean columns
    bool_cols = ['setup_valid']
    for col in bool_cols:
        if not df[col].dtype == bool:
            return ValidationResult(
                False,
                f"Column {col} must be boolean",
                {'column': col, 'dtype': str(df[col].dtype)}
            )
    
    # Validate numeric columns
    numeric_cols = ['entry', 'stop', 'r_multiple', 't1_2R', 't2_3R', 'confidence']
    for col in numeric_cols:
        if not pd.api.types.is_numeric_dtype(df[col]):
            return ValidationResult(
                False,
                f"Column {col} must be numeric",
                {'column': col, 'dtype': str(df[col].dtype)}
            )
        
        # Check for NaN in critical columns
        if col in ['entry', 'stop'] and df[col].isna().any():
            nan_count = df[col].isna().sum()
            return ValidationResult(
                False,
                f"Column {col} cannot contain NaN values",
                {'column': col, 'nan_count': nan_count}
            )
    
    # Validate confidence range (0-1)
    invalid_confidence = df[(df['confidence'] < 0) | (df['confidence'] > 1)]
    if not invalid_confidence.empty:
        return ValidationResult(
            False,
            f"Confidence values must be between 0 and 1",
            {'invalid_confidence_count': len(invalid_confidence)}
        )
    
    return ValidationResult(True, "Signal schema validation passed")

def validate_data_retention(df: pd.DataFrame, max_days: int, timestamp_col: str = 'timestamp') -> ValidationResult:
    """Validate that data doesn't exceed retention period."""
    if df.empty:
        return ValidationResult(True, "Empty DataFrame - retention OK")
    
    if timestamp_col not in df.columns:
        return ValidationResult(False, f"Missing timestamp column: {timestamp_col}")
    
    timestamps = pd.to_datetime(df[timestamp_col])
    oldest_timestamp = timestamps.min()
    newest_timestamp = timestamps.max()
    
    retention_period = (newest_timestamp - oldest_timestamp).days
    
    if retention_period > max_days:
        return ValidationResult(
            False,
            f"Data spans {retention_period} days, exceeds {max_days} day retention",
            {
                'retention_days': retention_period,
                'max_days': max_days,
                'oldest_timestamp': oldest_timestamp,
                'newest_timestamp': newest_timestamp
            }
        )
    
    return ValidationResult(True, f"Data retention OK ({retention_period} days)")

def validate_volume_data(df: pd.DataFrame) -> ValidationResult:
    """Validate volume data for anomalies."""
    if df.empty or 'volume' not in df.columns:
        return ValidationResult(False, "Missing volume data")
    
    volume = df['volume']
    
    # Check for zero volume (suspicious but not necessarily invalid)
    zero_volume_count = (volume == 0).sum()
    zero_volume_pct = zero_volume_count / len(df) * 100
    
    context = {
        'zero_volume_count': zero_volume_count,
        'zero_volume_percentage': zero_volume_pct
    }
    
    # Warn if more than 10% of bars have zero volume
    if zero_volume_pct > 10:
        return ValidationResult(
            False,
            f"High percentage of zero volume bars: {zero_volume_pct:.1f}%",
            context
        )
    
    # Check for extremely high volume (potential data error)
    if len(volume) > 1:
        median_volume = volume.median()
        high_volume_threshold = median_volume * 100  # 100x median
        
        extreme_volume = volume[volume > high_volume_threshold]
        if not extreme_volume.empty:
            context['extreme_volume_count'] = len(extreme_volume)
            context['median_volume'] = median_volume
            context['threshold'] = high_volume_threshold
            
            # This is a warning, not a hard failure
            logger.warning(f"Found {len(extreme_volume)} bars with extremely high volume")
    
    return ValidationResult(True, "Volume validation passed", context)

def validate_complete_dataset(
    df: pd.DataFrame, 
    ticker: str, 
    data_type: str = "intraday",
    max_retention_days: int = 7
) -> ValidationResult:
    """
    Comprehensive validation of a complete dataset.
    
    Args:
        df: DataFrame to validate
        ticker: Ticker symbol for context
        data_type: Type of data (intraday, daily, signal)
        max_retention_days: Maximum retention period
        
    Returns:
        ValidationResult with overall validation status
    """
    validations = []
    context = {'ticker': ticker, 'data_type': data_type}
    
    # Basic price data validation
    price_result = validate_price_data(df, ticker)
    validations.append(('price_data', price_result))
    
    # Timestamp validation for time series data
    if 'timestamp' in df.columns:
        timestamp_result = validate_timestamp_monotonicity(df, 'timestamp')
        validations.append(('timestamps', timestamp_result))
        
        retention_result = validate_data_retention(df, max_retention_days, 'timestamp')
        validations.append(('retention', retention_result))
    
    # Signal-specific validation
    if data_type == 'signal':
        signal_result = validate_signal_schema(df)
        validations.append(('signal_schema', signal_result))
    
    # Volume validation
    volume_result = validate_volume_data(df)
    validations.append(('volume', volume_result))
    
    # Collect all failures
    failures = [(name, result) for name, result in validations if not result.valid]
    
    if failures:
        failure_messages = [f"{name}: {result.message}" for name, result in failures]
        return ValidationResult(
            False,
            f"Validation failed: {'; '.join(failure_messages)}",
            {**context, 'failed_validations': [name for name, _ in failures]}
        )
    
    # Collect warnings
    warnings = []
    for name, result in validations:
        if result.valid and result.context:
            if 'zero_volume_percentage' in result.context:
                if result.context['zero_volume_percentage'] > 5:  # 5% threshold for warning
                    warnings.append(f"High zero volume percentage: {result.context['zero_volume_percentage']:.1f}%")
    
    success_message = f"All validations passed for {ticker} {data_type} data"
    if warnings:
        success_message += f" (warnings: {'; '.join(warnings)})"
    
    return ValidationResult(True, success_message, context)

def raise_validation_error(result: ValidationResult, operation: str = "Data validation"):
    """Raise DataValidationError from ValidationResult."""
    if not result.valid:
        raise DataValidationError(f"{operation}: {result.message}", result.context)

# Export functions and classes
__all__ = [
    'DataValidationError', 'ValidationResult',
    'validate_ticker_symbol', 'validate_price_data', 'validate_timestamp_monotonicity',
    'validate_signal_schema', 'validate_data_retention', 'validate_volume_data',
    'validate_complete_dataset', 'raise_validation_error'
]