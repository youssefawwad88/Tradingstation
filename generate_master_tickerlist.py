#!/usr/bin/env python3
"""
Generate Master Ticker List

This script implements the simplified ticker strategy using only manual sources:
1. Manual tickers from tickerlist.txt (always included, no filters)

The resulting master_tickerlist.csv powers all fetchers (both full and compact).
Note: Automated ticker sources (S&P 500 loader and opportunity finder) have been removed.
"""

import sys
import os
import pandas as pd
import logging
from datetime import datetime

# Add project root to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '.')))

from utils.helpers import save_df_to_s3, update_scheduler_status

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def load_manual_tickers():
    """
    Load manual tickers from tickerlist.txt.
    These are always included with no filters applied.
    
    Returns:
        list: List of manual ticker symbols
    """
    logger.info("Loading manual tickers from tickerlist.txt")
    
    try:
        manual_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), "tickerlist.txt")
        if os.path.exists(manual_file):
            with open(manual_file, 'r') as f:
                # Handle numbered format like "1.NVDA"
                tickers = []
                for line in f.readlines():
                    line = line.strip()
                    if line:
                        # Remove number prefix if present (e.g., "1.NVDA" -> "NVDA")
                        if '.' in line and line.split('.')[0].isdigit():
                            ticker = line.split('.', 1)[1]
                        else:
                            ticker = line
                        tickers.append(ticker)
                
            logger.info(f"✅ Loaded {len(tickers)} manual tickers: {tickers}")
            return tickers
        else:
            logger.warning(f"Manual ticker file not found: {manual_file}")
            return []
    except Exception as e:
        logger.error(f"Error loading manual tickers: {e}")
        return []

def generate_master_tickerlist():
    """
    Generate the master ticker list using only manual tickers from tickerlist.txt.
    S&P 500 automated loading has been removed per new requirements.
    
    Returns:
        bool: True if successful, False otherwise
    """
    logger.info("🚀 Starting Master Ticker List Generation (Manual Only)")
    
    try:
        # Load manual tickers (always included, no filters)
        manual_tickers = load_manual_tickers()
        logger.info(f"📝 Manual tickers: {len(manual_tickers)} loaded")
        
        if not manual_tickers:
            logger.error("No manual tickers found in tickerlist.txt")
            return False
        
        # Use only manual tickers - no S&P 500 automation
        master_tickers = manual_tickers
        
        logger.info(f"🎯 Master ticker list generated: {len(master_tickers)} total tickers (manual only)")
        
        # Create DataFrame and save to CSV
        df = pd.DataFrame({
            'ticker': master_tickers,
            'source': ['manual' for _ in master_tickers],
            'generated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        })
        
        # Save to local file
        output_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), "master_tickerlist.csv")
        df.to_csv(output_file, index=False)
        logger.info(f"💾 Master ticker list saved locally: {output_file}")
        
        # Also save to Spaces if configured
        spaces_path = "master_tickerlist.csv"
        upload_success = save_df_to_s3(df, spaces_path)
        if upload_success:
            logger.info(f"☁️ Master ticker list uploaded to Spaces: {spaces_path}")
        else:
            logger.warning(f"⚠️ Failed to upload to Spaces, but local file created: {output_file}")
        
        # Display final list
        logger.info(f"📋 Final Master Ticker List ({len(master_tickers)} tickers):")
        for i, ticker in enumerate(master_tickers, 1):
            logger.info(f"   {i:2d}. {ticker} (MANUAL)")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Error generating master ticker list: {e}")
        return False

if __name__ == "__main__":
    job_name = "generate_master_tickerlist"
    update_scheduler_status(job_name, "Running")
    
    try:
        success = generate_master_tickerlist()
        if success:
            update_scheduler_status(job_name, "Success")
            logger.info("✅ Master ticker list generation completed successfully")
        else:
            update_scheduler_status(job_name, "Fail", "Failed to generate master ticker list")
            logger.error("❌ Master ticker list generation failed")
    except Exception as e:
        error_message = f"An unexpected error occurred: {e}"
        logger.error(error_message)
        update_scheduler_status(job_name, "Fail", error_message)