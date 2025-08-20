"""
Data validation utilities for the trading system.

This module provides schema validation, data integrity checks,
and format validation for CSV files and trading data.
"""

import logging
from typing import Any, Dict, List, Optional, Set, Tuple

import pandas as pd

from utils.logging_setup import get_logger

logger = get_logger(__name__)


class ValidationError(Exception):
    """Custom exception for validation errors."""
    pass


class DataValidator:
    """Validator for trading data with schema enforcement."""

    # Schema definitions for different data types
    SCHEMAS = {
        "INTRADAY_1MIN": {
            "required_columns": ["timestamp", "open", "high", "low", "close", "volume"],
            "numeric_columns": ["open", "high", "low", "close", "volume"],
            "datetime_columns": ["timestamp"],
            "non_null_columns": ["timestamp", "open", "high", "low", "close"],
            "min_rows": 1,
            "max_rows": 50000,  # ~7 days of 1-min data
        },
        "INTRADAY_30MIN": {
            "required_columns": ["timestamp", "open", "high", "low", "close", "volume"],
            "numeric_columns": ["open", "high", "low", "close", "volume"],
            "datetime_columns": ["timestamp"],
            "non_null_columns": ["timestamp", "open", "high", "low", "close"],
            "min_rows": 1,
            "max_rows": 1000,  # ~500 bars + buffer
        },
        "DAILY": {
            "required_columns": ["date", "open", "high", "low", "close", "volume"],
            "numeric_columns": ["open", "high", "low", "close", "volume"],
            "datetime_columns": ["date"],
            "non_null_columns": ["date", "open", "high", "low", "close"],
            "min_rows": 1,
            "max_rows": 500,  # ~200 days + buffer
        },
        "SIGNALS": {
            "required_columns": [
                "timestamp_utc", "symbol", "direction", "setup_name", "score",
                "entry", "stop", "tp1", "tp2", "tp3", "r_multiple_at_tp1",
                "r_multiple_at_tp2", "r_multiple_at_tp3", "notes"
            ],
            "numeric_columns": [
                "score", "entry", "stop", "tp1", "tp2", "tp3",
                "r_multiple_at_tp1", "r_multiple_at_tp2", "r_multiple_at_tp3"
            ],
            "datetime_columns": ["timestamp_utc"],
            "non_null_columns": [
                "timestamp_utc", "symbol", "direction", "setup_name",
                "entry", "stop"
            ],
            "min_rows": 0,
            "max_rows": 10000,
        },
        "UNIVERSE": {
            "required_columns": [
                "symbol", "active", "fetch_1min", "fetch_30min", "fetch_daily"
            ],
            "numeric_columns": ["active", "fetch_1min", "fetch_30min", "fetch_daily"],
            "non_null_columns": ["symbol", "active"],
            "min_rows": 1,
            "max_rows": 10000,
        },
    }

    @classmethod
    def validate_dataframe(
        self,
        df: pd.DataFrame,
        schema_name: str,
        symbol: Optional[str] = None,
    ) -> Tuple[bool, List[str]]:
        """
        Validate DataFrame against schema.
        
        Args:
            df: DataFrame to validate
            schema_name: Schema name to validate against
            symbol: Symbol name for logging context
            
        Returns:
            Tuple of (is_valid, list_of_errors)
        """
        if schema_name not in self.SCHEMAS:
            return False, [f"Unknown schema: {schema_name}"]

        schema = self.SCHEMAS[schema_name]
        errors = []
        context = f" for {symbol}" if symbol else ""

        # Check if DataFrame is empty when data is required
        if df.empty and schema["min_rows"] > 0:
            errors.append(f"DataFrame is empty{context}")
            return False, errors

        # Check column presence
        missing_columns = set(schema["required_columns"]) - set(df.columns)
        if missing_columns:
            errors.append(f"Missing required columns{context}: {missing_columns}")

        # Check row count
        row_count = len(df)
        if row_count < schema["min_rows"]:
            errors.append(f"Too few rows{context}: {row_count} < {schema['min_rows']}")
        if row_count > schema["max_rows"]:
            errors.append(f"Too many rows{context}: {row_count} > {schema['max_rows']}")

        # Skip further validation if basic structure is wrong
        if missing_columns:
            return False, errors

        # Check non-null constraints
        for col in schema.get("non_null_columns", []):
            if col in df.columns:
                null_count = df[col].isnull().sum()
                if null_count > 0:
                    errors.append(f"Null values in {col}{context}: {null_count} nulls")

        # Check numeric columns
        for col in schema.get("numeric_columns", []):
            if col in df.columns:
                try:
                    pd.to_numeric(df[col], errors="coerce")
                except Exception as e:
                    errors.append(f"Non-numeric data in {col}{context}: {e}")

        # Check datetime columns
        for col in schema.get("datetime_columns", []):
            if col in df.columns:
                try:
                    pd.to_datetime(df[col], errors="coerce")
                except Exception as e:
                    errors.append(f"Invalid datetime in {col}{context}: {e}")

        # Schema-specific validations
        if schema_name.startswith("INTRADAY") or schema_name == "DAILY":
            errors.extend(self._validate_ohlcv_data(df, context))
        elif schema_name == "SIGNALS":
            errors.extend(self._validate_signals_data(df, context))
        elif schema_name == "UNIVERSE":
            errors.extend(self._validate_universe_data(df, context))

        return len(errors) == 0, errors

    @classmethod
    def _validate_ohlcv_data(self, df: pd.DataFrame, context: str) -> List[str]:
        """Validate OHLCV-specific business rules."""
        errors = []

        if df.empty:
            return errors

        # Check OHLC relationships
        if all(col in df.columns for col in ["open", "high", "low", "close"]):
            # High should be >= max(open, close)
            invalid_high = df["high"] < df[["open", "close"]].max(axis=1)
            if invalid_high.any():
                errors.append(f"Invalid high prices{context}: {invalid_high.sum()} rows")

            # Low should be <= min(open, close)
            invalid_low = df["low"] > df[["open", "close"]].min(axis=1)
            if invalid_low.any():
                errors.append(f"Invalid low prices{context}: {invalid_low.sum()} rows")

        # Check for negative prices
        price_cols = ["open", "high", "low", "close"]
        for col in price_cols:
            if col in df.columns:
                negative_prices = df[col] <= 0
                if negative_prices.any():
                    errors.append(f"Non-positive prices in {col}{context}: {negative_prices.sum()} rows")

        # Check volume
        if "volume" in df.columns:
            negative_volume = df["volume"] < 0
            if negative_volume.any():
                errors.append(f"Negative volume{context}: {negative_volume.sum()} rows")

        return errors

    @classmethod
    def _validate_signals_data(self, df: pd.DataFrame, context: str) -> List[str]:
        """Validate trading signals data."""
        errors = []

        if df.empty:
            return errors

        # Check direction values
        if "direction" in df.columns:
            valid_directions = {"long", "short"}
            invalid_directions = ~df["direction"].isin(valid_directions)
            if invalid_directions.any():
                errors.append(f"Invalid direction values{context}: {invalid_directions.sum()} rows")

        # Check entry/stop relationship
        if all(col in df.columns for col in ["entry", "stop", "direction"]):
            for direction in ["long", "short"]:
                mask = df["direction"] == direction
                if mask.any():
                    subset = df[mask]
                    if direction == "long":
                        invalid_stops = subset["stop"] >= subset["entry"]
                    else:
                        invalid_stops = subset["stop"] <= subset["entry"]
                    
                    if invalid_stops.any():
                        errors.append(f"Invalid {direction} stop levels{context}: {invalid_stops.sum()} rows")

        # Check R-multiples are positive
        r_cols = ["r_multiple_at_tp1", "r_multiple_at_tp2", "r_multiple_at_tp3"]
        for col in r_cols:
            if col in df.columns:
                negative_r = df[col] <= 0
                if negative_r.any():
                    errors.append(f"Non-positive R-multiple in {col}{context}: {negative_r.sum()} rows")

        return errors

    @classmethod
    def _validate_universe_data(self, df: pd.DataFrame, context: str) -> List[str]:
        """Validate universe/ticker list data."""
        errors = []

        if df.empty:
            return errors

        # Check for valid symbols (basic format)
        if "symbol" in df.columns:
            invalid_symbols = df["symbol"].str.len() == 0
            if invalid_symbols.any():
                errors.append(f"Empty symbols{context}: {invalid_symbols.sum()} rows")

        # Check binary flags
        binary_cols = ["active", "fetch_1min", "fetch_30min", "fetch_daily"]
        for col in binary_cols:
            if col in df.columns:
                invalid_flags = ~df[col].isin([0, 1])
                if invalid_flags.any():
                    errors.append(f"Invalid {col} flags{context}: {invalid_flags.sum()} rows")

        return errors

    @classmethod
    def check_data_continuity(
        self,
        df: pd.DataFrame,
        timestamp_col: str = "timestamp",
        interval_minutes: Optional[int] = None,
    ) -> Tuple[bool, List[str]]:
        """
        Check data continuity and identify gaps.
        
        Args:
            df: DataFrame to check
            timestamp_col: Timestamp column name
            interval_minutes: Expected interval in minutes (for gap detection)
            
        Returns:
            Tuple of (is_continuous, list_of_gap_descriptions)
        """
        if df.empty or timestamp_col not in df.columns:
            return True, []

        # Sort by timestamp
        df_sorted = df.sort_values(timestamp_col)
        timestamps = pd.to_datetime(df_sorted[timestamp_col])

        gaps = []

        # Check for duplicates
        duplicates = timestamps.duplicated()
        if duplicates.any():
            gaps.append(f"Duplicate timestamps: {duplicates.sum()}")

        # Check for gaps if interval is specified
        if interval_minutes and len(timestamps) > 1:
            expected_delta = pd.Timedelta(minutes=interval_minutes)
            time_diffs = timestamps.diff().dropna()
            
            # Allow for some tolerance (up to 2x the expected interval)
            max_allowed_gap = expected_delta * 2
            large_gaps = time_diffs > max_allowed_gap
            
            if large_gaps.any():
                gap_count = large_gaps.sum()
                gaps.append(f"Large time gaps: {gap_count} gaps > {max_allowed_gap}")

        return len(gaps) == 0, gaps

    @classmethod
    def validate_file_format(self, file_path: str) -> Tuple[bool, str]:
        """
        Validate file format and basic readability.
        
        Args:
            file_path: Path to file to validate
            
        Returns:
            Tuple of (is_valid, error_message)
        """
        try:
            # Try to read the file
            df = pd.read_csv(file_path, nrows=5)  # Just read first few rows
            
            if df.empty:
                return False, "File is empty"
            
            return True, "File format is valid"
            
        except Exception as e:
            return False, f"File format error: {e}"

    @classmethod
    def clean_dataframe(
        self,
        df: pd.DataFrame,
        schema_name: str,
        remove_duplicates: bool = True,
        fill_missing: bool = False,
    ) -> pd.DataFrame:
        """
        Clean DataFrame according to schema rules.
        
        Args:
            df: DataFrame to clean
            schema_name: Schema to apply
            remove_duplicates: Whether to remove duplicate rows
            fill_missing: Whether to fill missing values
            
        Returns:
            Cleaned DataFrame
        """
        if df.empty or schema_name not in self.SCHEMAS:
            return df

        df_clean = df.copy()
        schema = self.SCHEMAS[schema_name]

        # Remove duplicates if requested
        if remove_duplicates and "timestamp" in df_clean.columns:
            df_clean = df_clean.drop_duplicates(subset=["timestamp"], keep="last")

        # Convert numeric columns
        for col in schema.get("numeric_columns", []):
            if col in df_clean.columns:
                df_clean[col] = pd.to_numeric(df_clean[col], errors="coerce")

        # Convert datetime columns
        for col in schema.get("datetime_columns", []):
            if col in df_clean.columns:
                df_clean[col] = pd.to_datetime(df_clean[col], errors="coerce")

        # Fill missing values if requested
        if fill_missing:
            for col in schema.get("numeric_columns", []):
                if col in df_clean.columns:
                    df_clean[col] = df_clean[col].fillna(0)

        # Sort by timestamp if present
        if "timestamp" in df_clean.columns:
            df_clean = df_clean.sort_values("timestamp").reset_index(drop=True)
        elif "date" in df_clean.columns:
            df_clean = df_clean.sort_values("date").reset_index(drop=True)

        return df_clean


# Global validator instance
validator = DataValidator()


# Convenience functions
def validate_dataframe(
    df: pd.DataFrame,
    schema_name: str,
    symbol: Optional[str] = None,
) -> Tuple[bool, List[str]]:
    """Validate DataFrame against schema."""
    return validator.validate_dataframe(df, schema_name, symbol)


def check_data_continuity(
    df: pd.DataFrame,
    timestamp_col: str = "timestamp",
    interval_minutes: Optional[int] = None,
) -> Tuple[bool, List[str]]:
    """Check data continuity."""
    return validator.check_data_continuity(df, timestamp_col, interval_minutes)


def validate_file_format(file_path: str) -> Tuple[bool, str]:
    """Validate file format."""
    return validator.validate_file_format(file_path)


def clean_dataframe(
    df: pd.DataFrame,
    schema_name: str,
    remove_duplicates: bool = True,
    fill_missing: bool = False,
) -> pd.DataFrame:
    """Clean DataFrame according to schema."""
    return validator.clean_dataframe(df, schema_name, remove_duplicates, fill_missing)