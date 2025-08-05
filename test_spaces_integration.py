#!/usr/bin/env python3
"""
Test script to demonstrate DigitalOcean Spaces integration.
This script shows how to use the new spaces_manager functions.
"""

import sys
import os
import pandas as pd
from datetime import datetime

# Add project root to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from utils.spaces_manager import spaces_manager
from utils.config import DO_SPACES_CONFIG

def test_spaces_integration():
    """Test the DigitalOcean Spaces integration."""
    print("=== DigitalOcean Spaces Integration Test ===")
    print(f"Bucket: {DO_SPACES_CONFIG['bucket_name']}")
    print(f"Region: {DO_SPACES_CONFIG['region']}")
    print(f"Endpoint: {DO_SPACES_CONFIG['endpoint_url']}")
    
    # Check if credentials are configured
    if not spaces_manager.client:
        print("\n⚠️  WARNING: Spaces credentials are not configured.")
        print("To set up credentials, set these environment variables:")
        print("- SPACES_ACCESS_KEY_ID")
        print("- SPACES_SECRET_ACCESS_KEY")
        print("Or update utils/config.py with your credentials.")
        return False
    
    print("\n✅ Spaces client is configured and ready!")
    
    # Test upload functions
    print("\n--- Testing Upload Functions ---")
    
    # 1. Test DataFrame upload
    test_df = pd.DataFrame({
        'symbol': ['AAPL', 'MSFT', 'GOOGL'],
        'price': [150.0, 280.0, 2500.0],
        'timestamp': [datetime.now()] * 3
    })
    
    if spaces_manager.upload_dataframe(test_df, 'test/sample_data.csv'):
        print("✅ DataFrame upload successful")
    else:
        print("❌ DataFrame upload failed")
    
    # 2. Test list upload
    test_list = ['AAPL', 'MSFT', 'GOOGL', 'TSLA', 'NVDA']
    if spaces_manager.upload_list(test_list, 'test/sample_tickers.txt'):
        print("✅ List upload successful")
    else:
        print("❌ List upload failed")
    
    # 3. Test string upload
    test_content = f"Test file created at {datetime.now()}\nThis is a test of the Spaces integration."
    if spaces_manager.upload_string(test_content, 'test/sample_text.txt'):
        print("✅ String upload successful")
    else:
        print("❌ String upload failed")
    
    # Test list functions
    print("\n--- Testing List Functions ---")
    objects = spaces_manager.list_objects('test/')
    if objects:
        print(f"✅ Found {len(objects)} objects in test/ folder:")
        for obj in objects:
            print(f"  - {obj}")
    else:
        print("❌ No objects found or list operation failed")
    
    # Test download functions
    print("\n--- Testing Download Functions ---")
    
    # 1. Test DataFrame download
    downloaded_df = spaces_manager.download_dataframe('test/sample_data.csv')
    if not downloaded_df.empty:
        print("✅ DataFrame download successful")
        print(f"   Downloaded {len(downloaded_df)} rows")
    else:
        print("❌ DataFrame download failed")
    
    # 2. Test list download
    downloaded_list = spaces_manager.download_list('test/sample_tickers.txt')
    if downloaded_list:
        print(f"✅ List download successful ({len(downloaded_list)} items)")
    else:
        print("❌ List download failed")
    
    # 3. Test string download
    downloaded_content = spaces_manager.download_string('test/sample_text.txt')
    if downloaded_content:
        print("✅ String download successful")
    else:
        print("❌ String download failed")
    
    print("\n=== Test Complete ===")
    return True

if __name__ == "__main__":
    test_spaces_integration()