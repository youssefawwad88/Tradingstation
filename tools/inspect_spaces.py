#!/usr/bin/env python3
"""Inspect Spaces tool for examining stored data.

This tool allows inspecting data in Spaces storage by:
1. Listing objects by symbol (--symbols)
2. Listing objects by prefix (--prefix)

For each object, shows: path, size, last_modified, and last few timestamps from the data.
"""

import argparse
import logging
import os
import re
import sys
from datetime import datetime
from typing import List, Optional

import pandas as pd

# Add project root to Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from utils.config import config
from utils.paths import key_intraday_1min, key_intraday_30min, key_daily, s3_key
from utils.spaces_io import spaces_io

logger = logging.getLogger(__name__)


def list_objects_by_prefix(prefix: str) -> List[dict]:
    """List all objects with a given prefix.
    
    Args:
        prefix: S3 prefix to search for
        
    Returns:
        List of object metadata dictionaries
    """
    try:
        if not spaces_io.is_available:
            logger.error("Cannot connect to Spaces - client not available")
            return []
        
        objects = spaces_io.list_objects(prefix)
        return [{
            'key': obj['Key'],
            'size': obj['Size'], 
            'last_modified': obj['LastModified']
        } for obj in objects]
        
    except Exception as e:
        logger.error(f"Error listing objects with prefix {prefix}: {e}")
        return []


def get_last_timestamps(key: str, num_lines: int = 10) -> List[str]:
    """Get the last few timestamps from a CSV file.
    
    Args:
        key: S3 key of the CSV file
        num_lines: Number of lines to read from the end
        
    Returns:
        List of timestamp strings found
    """
    try:
        if not spaces_io.is_available:
            return []
        
        # Get the object content as bytes
        content_bytes = spaces_io.get_object(key)
        if not content_bytes:
            return []
            
        # Decode to text
        content = content_bytes.decode('utf-8')
        
        # Get last N lines
        lines = content.strip().split('\n')
        last_lines = lines[-num_lines:] if len(lines) > num_lines else lines[1:]  # Skip header
        
        # Extract timestamps - assume first column is timestamp
        timestamps = []
        for line in last_lines:
            if line.strip() and not line.startswith('timestamp'):  # Skip header
                parts = line.split(',')
                if parts and parts[0]:
                    # Clean up timestamp (remove quotes if present)
                    timestamp = parts[0].strip('"\'')
                    timestamps.append(timestamp)
        
        # Return last 3 timestamps
        return timestamps[-3:] if len(timestamps) >= 3 else timestamps
        
    except Exception as e:
        logger.warning(f"Could not read timestamps from {key}: {e}")
        return []


def inspect_symbols(symbols: List[str]) -> None:
    """Inspect data for specific symbols.
    
    Args:
        symbols: List of symbols to inspect
    """
    print(f"Inspecting data for symbols: {symbols}")
    print("-" * 80)
    
    for symbol in symbols:
        print(f"\nSymbol: {symbol}")
        
        # Check all data types for this symbol
        keys_to_check = [
            ("1min", key_intraday_1min(symbol)),
            ("30min", key_intraday_30min(symbol)), 
            ("daily", key_daily(symbol))
        ]
        
        for data_type, key in keys_to_check:
            full_key = s3_key(key)
            
            try:
                if not spaces_io.is_available:
                    continue
                    
                # Get object metadata
                metadata = spaces_io.object_metadata(full_key)
                if metadata:
                    size = metadata.get('size', 0)
                    last_modified = metadata.get('last_modified', 'Unknown')
                    
                    # Get last timestamps
                    timestamps = get_last_timestamps(full_key)
                    timestamp_str = ", ".join(timestamps) if timestamps else "No timestamps found"
                    
                    print(f"  {data_type:>6}: path={full_key}, size={size} bytes, last_modified={last_modified}")
                    print(f"         Last 3 timestamps: {timestamp_str}")
                else:
                    print(f"  {data_type:>6}: NOT FOUND")
                
            except Exception as e:
                print(f"  {data_type:>6}: ERROR - {e}")


def inspect_prefix(prefix: str) -> None:
    """Inspect all objects with a given prefix.
    
    Args:
        prefix: S3 prefix to inspect
    """
    print(f"Inspecting objects with prefix: {prefix}")
    print("-" * 80)
    
    # Add base prefix if not already included
    full_prefix = s3_key(prefix) if not prefix.startswith(config.SPACES_BASE_PREFIX) else prefix
    
    objects = list_objects_by_prefix(full_prefix)
    
    if not objects:
        print("No objects found with this prefix")
        return
    
    for obj in objects:
        key = obj['key']
        size = obj['size']
        last_modified = obj['last_modified']
        
        # Get last timestamps if it's a CSV file
        timestamps = []
        if key.endswith('.csv'):
            timestamps = get_last_timestamps(key)
        
        timestamp_str = ", ".join(timestamps) if timestamps else "N/A"
        
        print(f"Path: {key}")
        print(f"Size: {size} bytes")
        print(f"Last Modified: {last_modified}")
        print(f"Last 3 timestamps: {timestamp_str}")
        print()


def main():
    """Main entry point for the inspect spaces tool."""
    parser = argparse.ArgumentParser(description="Inspect data in Spaces storage")
    
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--symbols", nargs="+", help="List of symbols to inspect")
    group.add_argument("--prefix", type=str, help="S3 prefix to inspect (e.g., data/intraday/1min/)")
    
    args = parser.parse_args()
    
    # Setup logging
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    
    try:
        if args.symbols:
            inspect_symbols(args.symbols)
        else:
            inspect_prefix(args.prefix)
            
        return 0
        
    except Exception as e:
        logger.error(f"Error in inspect_spaces: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())