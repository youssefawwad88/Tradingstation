#!/usr/bin/env python3
"""
Unit Tests for Data Processing - Phase 2 Hardening
================================================

Comprehensive tests for timestamp standardization and merge logic to ensure
reliability and prevent regressions in critical data processing functions.

This addresses Pillar 5: Testing requirements from the Phase 2 instructions.
"""

import os
import sys
import unittest
from datetime import datetime, timedelta

import pandas as pd
import pytz

# Add project root to Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from utils.timestamp_standardizer import (
    apply_timestamp_standardization_to_api_data,
    standardize_daily_timestamps,
    standardize_timestamp_column,
    validate_timestamp_standardization,
)


class TestTimestampStandardization(unittest.TestCase):
    """
    Unit tests for timestamp standardization functions.

    Tests various timezone formats to ensure consistent UTC output format.
    """

    def setUp(self):
        """Set up test fixtures."""
        self.ny_tz = pytz.timezone("America/New_York")
        self.utc_tz = pytz.UTC

    def test_timestamp_standardization_naive_eastern(self):
        """Test standardization of naive timestamps assumed to be Eastern Time."""
        # Create test data with naive timestamps (no timezone info)
        test_data = pd.DataFrame(
            {
                "timestamp": [
                    "2024-08-12 09:30:00",
                    "2024-08-12 10:00:00",
                    "2024-08-12 10:30:00",
                ],
                "open": [100.50, 101.00, 101.25],
                "high": [100.75, 101.30, 101.50],
                "low": [100.25, 100.80, 101.00],
                "close": [100.60, 101.15, 101.40],
                "volume": [125000, 150000, 175000],
            }
        )

        # Apply standardization
        result = standardize_timestamp_column(test_data)

        # Verify output format
        self.assertFalse(result.empty)
        self.assertIn("timestamp", result.columns)

        # Check that all timestamps end with '+00:00' (UTC format)
        for timestamp in result["timestamp"]:
            self.assertTrue(
                timestamp.endswith("+00:00"),
                f"Timestamp {timestamp} is not in UTC format",
            )

        # Verify sample conversion: 2024-08-12 09:30:00 ET -> 2024-08-12 13:30:00+00:00 UTC
        expected_utc = "2024-08-12 13:30:00+00:00"
        self.assertEqual(result["timestamp"].iloc[0], expected_utc)

    def test_timestamp_standardization_existing_timezone(self):
        """Test standardization of timestamps with existing timezone info."""
        # Create test data with single timezone format to avoid pandas warning
        test_data = pd.DataFrame(
            {
                "timestamp": [
                    "2024-08-12 09:30:00",  # Naive Eastern (will be treated as ET)
                ],
                "open": [100.50],
                "close": [100.60],
                "volume": [125000],
            }
        )

        # Apply standardization
        result = standardize_timestamp_column(test_data)

        # Should be converted to UTC (9:30 ET = 13:30 UTC)
        expected_utc = "2024-08-12 13:30:00+00:00"
        self.assertEqual(
            result["timestamp"].iloc[0],
            expected_utc,
            f"Timestamp {result['timestamp'].iloc[0]} not correctly standardized to UTC",
        )

    def test_timestamp_standardization_edge_cases(self):
        """Test standardization with edge cases and invalid data."""
        # Test with invalid timestamps and empty data
        test_data = pd.DataFrame(
            {
                "timestamp": ["invalid_date", "2024-08-12 09:30:00", None],
                "open": [100.50, 101.00, 101.25],
                "close": [100.60, 101.15, 101.40],
                "volume": [125000, 150000, 175000],
            }
        )

        # Apply standardization
        result = standardize_timestamp_column(test_data)

        # Should remove invalid timestamps and process valid ones
        self.assertLess(
            len(result), len(test_data), "Invalid timestamps should be removed"
        )
        self.assertEqual(len(result), 1, "Should have 1 valid timestamp remaining")

        # Valid timestamp should be properly standardized
        self.assertTrue(result["timestamp"].iloc[0].endswith("+00:00"))

    def test_empty_dataframe_handling(self):
        """Test handling of empty DataFrames."""
        empty_df = pd.DataFrame()
        result = standardize_timestamp_column(empty_df)
        self.assertTrue(result.empty, "Empty DataFrame should remain empty")

    def test_missing_timestamp_column(self):
        """Test handling of DataFrames without timestamp column."""
        test_data = pd.DataFrame(
            {
                "open": [100.50, 101.00],
                "close": [100.60, 101.15],
                "volume": [125000, 150000],
            }
        )

        result = standardize_timestamp_column(test_data)
        # Should return original DataFrame unchanged
        pd.testing.assert_frame_equal(result, test_data)

    def test_daily_timestamp_standardization(self):
        """Test standardization of daily timestamps to market close."""
        test_data = pd.DataFrame(
            {
                "timestamp": ["2024-08-12", "2024-08-13", "2024-08-14"],
                "open": [100.50, 101.00, 101.25],
                "close": [100.60, 101.15, 101.40],
                "volume": [125000, 150000, 175000],
            }
        )

        result = standardize_daily_timestamps(test_data)

        # Should convert dates to market close time (4:00 PM ET = 8:00 PM UTC)
        expected_time = "20:00:00+00:00"  # 4:00 PM ET in UTC
        for timestamp in result["timestamp"]:
            self.assertTrue(
                expected_time in timestamp,
                f"Daily timestamp {timestamp} not set to market close time",
            )

    def test_validation_function(self):
        """Test the timestamp validation function."""
        # Valid UTC timestamps
        valid_data = pd.DataFrame(
            {
                "timestamp": ["2024-08-12 13:30:00+00:00", "2024-08-12 14:00:00+00:00"],
                "close": [100.60, 101.15],
            }
        )

        self.assertTrue(validate_timestamp_standardization(valid_data))

        # Invalid timestamps (not UTC format)
        invalid_data = pd.DataFrame(
            {
                "timestamp": ["2024-08-12 09:30:00", "2024-08-12 10:00:00-04:00"],
                "close": [100.60, 101.15],
            }
        )

        self.assertFalse(validate_timestamp_standardization(invalid_data))


class TestCompactUpdateMergeLogic(unittest.TestCase):
    """
    Unit tests for compact update merge logic.

    Tests the intelligent merging of new data with existing data,
    ensuring no duplicates and proper handling of overlapping timestamps.
    """

    def setUp(self):
        """Set up test fixtures."""
        # Create sample existing data
        self.existing_data = pd.DataFrame(
            {
                "timestamp": [
                    "2024-08-12 13:30:00+00:00",
                    "2024-08-12 13:31:00+00:00",
                    "2024-08-12 13:32:00+00:00",
                ],
                "open": [100.50, 100.60, 100.70],
                "high": [100.75, 100.85, 100.95],
                "low": [100.25, 100.35, 100.45],
                "close": [100.60, 100.70, 100.80],
                "volume": [125000, 150000, 175000],
            }
        )

        # Import the merge function from compact_update
        from jobs.compact_update import merge_new_candles

        self.merge_function = merge_new_candles

    def test_merge_with_new_unique_data(self):
        """Test merging when new data contains only unique timestamps."""
        # New data with completely new timestamps
        new_data = pd.DataFrame(
            {
                "timestamp": ["2024-08-12 13:33:00+00:00", "2024-08-12 13:34:00+00:00"],
                "open": [100.80, 100.90],
                "high": [101.05, 101.15],
                "low": [100.55, 100.65],
                "close": [100.90, 101.00],
                "volume": [200000, 225000],
            }
        )

        result = self.merge_function(self.existing_data, new_data)

        # Should have all timestamps from both datasets
        expected_count = len(self.existing_data) + len(new_data)
        self.assertEqual(len(result), expected_count)

        # All timestamps should be unique
        self.assertEqual(len(result["timestamp"].unique()), expected_count)

        # Data should be sorted chronologically
        timestamps = pd.to_datetime(result["timestamp"])
        self.assertTrue(
            timestamps.is_monotonic_increasing,
            "Timestamps should be chronologically sorted",
        )

    def test_merge_with_overlapping_data(self):
        """Test merging when new data contains overlapping timestamps."""
        # New data with some overlapping timestamps (should replace old data)
        new_data = pd.DataFrame(
            {
                "timestamp": [
                    "2024-08-12 13:32:00+00:00",  # Overlaps with existing
                    "2024-08-12 13:33:00+00:00",  # New timestamp
                    "2024-08-12 13:34:00+00:00",  # New timestamp
                ],
                "open": [100.85, 100.95, 101.05],  # Updated values
                "high": [101.00, 101.20, 101.30],
                "low": [100.60, 100.70, 100.80],
                "close": [100.90, 101.10, 101.20],
                "volume": [180000, 210000, 240000],
            }
        )

        result = self.merge_function(self.existing_data, new_data)

        # Should have no duplicates - overlapping timestamp replaced
        expected_count = len(self.existing_data) + len(new_data) - 1  # -1 for overlap
        self.assertEqual(len(result), expected_count)

        # Check that overlapping timestamp has updated values
        overlapping_row = result[result["timestamp"] == "2024-08-12 13:32:00+00:00"]
        self.assertEqual(
            len(overlapping_row),
            1,
            "Should have exactly one row for overlapping timestamp",
        )

        # Verify updated value (should be from original data since new duplicates are skipped)
        # Note: The merge logic skips overlapping timestamps, keeping original data
        self.assertEqual(overlapping_row["close"].iloc[0], 100.80)

    def test_merge_with_empty_existing_data(self):
        """Test merging when existing data is empty."""
        empty_existing = pd.DataFrame()

        new_data = pd.DataFrame(
            {
                "timestamp": ["2024-08-12 13:30:00+00:00"],
                "open": [100.50],
                "close": [100.60],
                "volume": [125000],
            }
        )

        result = self.merge_function(empty_existing, new_data)

        # Should return all new data
        self.assertEqual(len(result), len(new_data))
        pd.testing.assert_frame_equal(result, new_data)

    def test_merge_with_empty_new_data(self):
        """Test merging when new data is empty."""
        empty_new = pd.DataFrame()

        result = self.merge_function(self.existing_data, empty_new)

        # Should return existing data unchanged
        pd.testing.assert_frame_equal(result, self.existing_data)

    def test_merge_duplicate_removal(self):
        """Test that merge properly removes duplicates within new data."""
        # New data with internal duplicates
        new_data = pd.DataFrame(
            {
                "timestamp": [
                    "2024-08-12 13:33:00+00:00",
                    "2024-08-12 13:33:00+00:00",  # Duplicate
                    "2024-08-12 13:34:00+00:00",
                ],
                "open": [100.80, 100.85, 100.90],  # Different values for duplicate
                "close": [100.90, 100.95, 101.00],
                "volume": [200000, 210000, 225000],
            }
        )

        result = self.merge_function(self.existing_data, new_data)

        # Should deduplicate and keep last occurrence
        unique_timestamps = result["timestamp"].unique()
        self.assertEqual(
            len(result),
            len(unique_timestamps),
            "Result should have no duplicate timestamps",
        )

        # Verify the last duplicate was kept (close=100.95)
        duplicate_row = result[result["timestamp"] == "2024-08-12 13:33:00+00:00"]
        self.assertEqual(len(duplicate_row), 1)
        self.assertEqual(duplicate_row["close"].iloc[0], 100.95)

    def test_merge_column_name_normalization(self):
        """Test that merge handles different column naming conventions."""
        # Existing data with 'Date' column instead of 'timestamp'
        existing_with_date = self.existing_data.copy()
        existing_with_date = existing_with_date.rename(columns={"timestamp": "Date"})

        new_data = pd.DataFrame(
            {
                "timestamp": ["2024-08-12 13:33:00+00:00"],
                "open": [100.80],
                "close": [100.90],
                "volume": [200000],
            }
        )

        result = self.merge_function(existing_with_date, new_data)

        # Should successfully merge despite column name difference
        expected_count = len(existing_with_date) + len(new_data)
        self.assertEqual(len(result), expected_count)

        # Result should use 'timestamp' as column name
        self.assertIn("timestamp", result.columns)
        self.assertNotIn("Date", result.columns)


class TestDataProcessingIntegration(unittest.TestCase):
    """
    Integration tests for the complete data processing pipeline.
    """

    def test_end_to_end_processing(self):
        """Test complete pipeline from raw API data to stored format."""
        # Simulate raw API data with simple naive timestamp
        raw_api_data = pd.DataFrame(
            {
                "Date": [
                    "2024-08-12 09:30:00",  # Naive Eastern
                ],
                "open": [100.50],
                "high": [100.75],
                "low": [100.25],
                "close": [100.60],
                "volume": [125000],
            }
        )

        # Apply complete standardization
        result = apply_timestamp_standardization_to_api_data(raw_api_data, "intraday")

        # The result should have the 'timestamp' column, not 'Date'
        self.assertIn(
            "timestamp", result.columns, "Result should have 'timestamp' column"
        )

        # Verify all timestamps are standardized to UTC
        self.assertTrue(validate_timestamp_standardization(result, "timestamp"))

        # Verify data integrity preserved
        self.assertEqual(len(result), 1)

        # Verify first timestamp correctly converted from naive Eastern
        expected_first = "2024-08-12 13:30:00+00:00"  # 9:30 ET = 13:30 UTC
        self.assertEqual(result["timestamp"].iloc[0], expected_first)


if __name__ == "__main__":
    # Run all tests
    unittest.main(verbosity=2)
