"""
Compatibility wrapper for intraday compact updates.

This module provides backward compatibility by wrapping the unified data fetch
manager with the compact mode specifically for intraday data updates.
"""

import sys
import os

# Add the project root to the path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from jobs.data_fetch_manager import DataFetchManager
from utils.logging_setup import get_logger

logger = get_logger(__name__)


def main():
    """Main entry point for intraday compact updates."""
    logger.info("🔄 Starting intraday compact update (compatibility wrapper)")
    
    # Create data fetch manager
    manager = DataFetchManager()
    
    # Run both 1min and 30min updates
    success_1min = manager.run_intraday_updates("1min")
    success_30min = manager.run_intraday_updates("30min")
    
    # Consider successful if at least one interval succeeded
    overall_success = success_1min or success_30min
    
    if overall_success:
        logger.info("✅ Intraday compact update completed successfully")
    else:
        logger.error("❌ Intraday compact update failed")
    
    return overall_success


if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)