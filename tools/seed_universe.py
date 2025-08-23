#!/usr/bin/env python3
"""Seed universe tool for uploading ticker lists to Spaces.

This tool allows uploading a universe CSV to Spaces storage either by:
1. Generating from a list of tickers (--tickers)  
2. Uploading an existing CSV file (--csv)
"""

import argparse
import logging
import os
import sys
from pathlib import Path

import pandas as pd

# Add project root to Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from utils.config import config
from utils.paths import universe_key
from utils.spaces_io import spaces_io

logger = logging.getLogger(__name__)


def generate_universe_csv(tickers: list[str]) -> pd.DataFrame:
    """Generate a universe CSV DataFrame with all flags set to 1.
    
    Args:
        tickers: List of ticker symbols
        
    Returns:
        DataFrame with universe structure
    """
    data = []
    for ticker in tickers:
        data.append({
            "symbol": ticker.upper(),
            "active": 1,
            "fetch_1min": 1,
            "fetch_30min": 1,
            "fetch_daily": 1
        })

    return pd.DataFrame(data)


def upload_universe(df: pd.DataFrame) -> bool:
    """Upload universe DataFrame to Spaces.
    
    Args:
        df: Universe DataFrame to upload
        
    Returns:
        True if successful, False otherwise
    """
    try:
        key = universe_key()

        # Upload CSV with metadata
        success = spaces_io.upload_dataframe(df, key, file_format="csv")

        if not success:
            logger.error("Failed to upload universe CSV")
            return False

        # Get object metadata to show confirmation
        if spaces_io.is_available:
            try:
                metadata = spaces_io.object_metadata(key)
                if metadata:
                    size_bytes = metadata.get('size', 0)
                    etag = metadata.get('etag', '')

                    print(f"s3://{config.SPACES_BUCKET_NAME}/{key} size={size_bytes} etag={etag}")
                else:
                    print(f"Universe uploaded to {key} (metadata unavailable)")

            except Exception as e:
                logger.warning(f"Could not get object metadata: {e}")
                print(f"s3://{config.SPACES_BUCKET_NAME}/{key} uploaded successfully")
        else:
            print(f"Universe uploaded to {key}")

        return True

    except Exception as e:
        logger.error(f"Error uploading universe: {e}")
        return False


def main():
    """Main entry point for the seed universe tool."""
    parser = argparse.ArgumentParser(description="Seed universe CSV to Spaces")

    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--tickers", nargs="+", help="List of ticker symbols to generate universe from")
    group.add_argument("--csv", type=str, help="Path to existing CSV file to upload")

    args = parser.parse_args()

    # Setup logging
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    try:
        if args.tickers:
            # Generate universe from ticker list
            print(f"Generating universe CSV from {len(args.tickers)} tickers: {args.tickers}")
            df = generate_universe_csv(args.tickers)

        else:
            # Load existing CSV file
            csv_path = Path(args.csv)
            if not csv_path.exists():
                print(f"Error: CSV file not found: {csv_path}")
                return 1

            print(f"Loading existing CSV: {csv_path}")
            df = pd.read_csv(csv_path)

            # Validate required columns
            required_columns = ["symbol", "active", "fetch_1min", "fetch_30min", "fetch_daily"]
            missing_columns = [col for col in required_columns if col not in df.columns]
            if missing_columns:
                print(f"Error: CSV missing required columns: {missing_columns}")
                return 1

        # Upload to Spaces
        print(f"Uploading universe CSV with {len(df)} symbols...")
        success = upload_universe(df)

        if success:
            print("Universe upload completed successfully")
            return 0
        else:
            print("Universe upload failed")
            return 1

    except Exception as e:
        logger.error(f"Error in seed_universe: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
