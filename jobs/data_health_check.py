#!/usr/bin/env python3
"""
Data Health Check and Auto-Repair Engine
========================================

This script performs automated data health monitoring and recovery:
1. Checks all tickers for data completeness across all timeframes
2. Identifies tickers with missing or insufficient data
3. Automatically triggers targeted full fetch for deficient tickers
4. Ensures data integrity across the entire system

Data Requirements:
- Daily Data: Minimum 200 rows
- 30-Minute Data: Minimum 500 rows  
- 1-Minute Data: Minimum 7 days of coverage

This supervisor job runs every 6 hours to maintain system health.
"""

import os
import sys
import pandas as pd
import logging
from datetime import datetime, timedelta
import pytz

# Add project root to Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Import core utilities
from utils.config import (
    DAILY_MIN_ROWS, 
    THIRTY_MIN_MIN_ROWS, 
    ONE_MIN_REQUIRED_DAYS,
    TIMEZONE
)
from utils.helpers import read_master_tickerlist, read_df_from_s3, update_scheduler_status

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def check_daily_data_health(ticker):
    """
    Check if daily data meets minimum requirements.
    
    Args:
        ticker (str): Stock ticker symbol
        
    Returns:
        bool: True if data is compliant
    """
    try:
        daily_path = f'data/daily/{ticker}_daily.csv'
        daily_df = read_df_from_s3(daily_path)
        
        if daily_df.empty:
            logger.debug(f"❌ {ticker}: Daily data file is empty or missing")
            return False
            
        if len(daily_df) < DAILY_MIN_ROWS:
            logger.debug(f"❌ {ticker}: Daily data insufficient - {len(daily_df)} rows (required: {DAILY_MIN_ROWS})")
            return False
            
        logger.debug(f"✅ {ticker}: Daily data compliant - {len(daily_df)} rows")
        return True
        
    except Exception as e:
        logger.debug(f"❌ {ticker}: Daily data check failed - {e}")
        return False


def check_30min_data_health(ticker):
    """
    Check if 30-minute data meets minimum requirements.
    
    Args:
        ticker (str): Stock ticker symbol
        
    Returns:
        bool: True if data is compliant
    """
    try:
        min_30_path = f'data/intraday_30min/{ticker}_30min.csv'
        min_30_df = read_df_from_s3(min_30_path)
        
        if min_30_df.empty:
            logger.debug(f"❌ {ticker}: 30-minute data file is empty or missing")
            return False
            
        if len(min_30_df) < THIRTY_MIN_MIN_ROWS:
            logger.debug(f"❌ {ticker}: 30-minute data insufficient - {len(min_30_df)} rows (required: {THIRTY_MIN_MIN_ROWS})")
            return False
            
        logger.debug(f"✅ {ticker}: 30-minute data compliant - {len(min_30_df)} rows")
        return True
        
    except Exception as e:
        logger.debug(f"❌ {ticker}: 30-minute data check failed - {e}")
        return False


def check_1min_data_health(ticker):
    """
    Check if 1-minute data meets minimum coverage requirements.
    
    Args:
        ticker (str): Stock ticker symbol
        
    Returns:
        bool: True if data is compliant
    """
    try:
        min_1_path = f'data/intraday/{ticker}_1min.csv'
        min_1_df = read_df_from_s3(min_1_path)
        
        if min_1_df.empty:
            logger.debug(f"❌ {ticker}: 1-minute data file is empty or missing")
            return False
        
        # Check date coverage
        timestamp_col = 'timestamp' if 'timestamp' in min_1_df.columns else 'Date'
        if timestamp_col not in min_1_df.columns:
            logger.debug(f"❌ {ticker}: 1-minute data missing timestamp column")
            return False
            
        min_1_df[timestamp_col] = pd.to_datetime(min_1_df[timestamp_col])
        
        # Calculate required cutoff date
        cutoff_date = datetime.now(pytz.timezone(TIMEZONE)) - timedelta(days=ONE_MIN_REQUIRED_DAYS)
        
        # Find oldest data point
        oldest_data = min_1_df[timestamp_col].min()
        oldest_data_localized = oldest_data.tz_localize(pytz.timezone(TIMEZONE)) if oldest_data.tz is None else oldest_data
        
        if oldest_data_localized > cutoff_date:
            days_coverage = (datetime.now(pytz.timezone(TIMEZONE)) - oldest_data_localized).days
            logger.debug(f"❌ {ticker}: 1-minute data insufficient coverage - {days_coverage} days (required: {ONE_MIN_REQUIRED_DAYS})")
            return False
            
        days_coverage = (datetime.now(pytz.timezone(TIMEZONE)) - oldest_data_localized).days
        logger.debug(f"✅ {ticker}: 1-minute data compliant - {days_coverage} days coverage, {len(min_1_df)} rows")
        return True
        
    except Exception as e:
        logger.debug(f"❌ {ticker}: 1-minute data check failed - {e}")
        return False


def run_health_check():
    """
    Execute the complete data health check and auto-repair process.
    
    This function orchestrates the entire health monitoring system:
    1. Read all tickers from master list
    2. Check each ticker's data health across all timeframes
    3. Identify deficient tickers
    4. Trigger targeted full fetch for repairs
    """
    logger.info("=" * 60)
    logger.info("🏥 STARTING DATA HEALTH AND RECOVERY CHECK")
    logger.info("=" * 60)
    
    # Read master ticker list
    tickers = read_master_tickerlist()
    if not tickers:
        logger.error("❌ No tickers found in master watchlist")
        return False
    
    logger.info(f"📋 Checking health for {len(tickers)} tickers: {tickers}")
    
    # Initialize deficiencies tracking
    deficient_tickers = []
    
    # Loop through every ticker and check health
    for i, ticker in enumerate(tickers, 1):
        logger.debug(f"🔍 Checking health for ticker: {ticker} ({i}/{len(tickers)})")
        
        # Check daily data first (most critical)
        if not check_daily_data_health(ticker):
            deficient_tickers.append(ticker)
            logger.info(f"⚠️ {ticker}: Added to deficient list due to daily data issues")
            continue  # Skip other checks if daily data fails
            
        # Check 30-minute data
        if not check_30min_data_health(ticker):
            deficient_tickers.append(ticker)
            logger.info(f"⚠️ {ticker}: Added to deficient list due to 30-minute data issues")
            continue  # Skip 1-minute check if 30-minute fails
            
        # Check 1-minute data
        if not check_1min_data_health(ticker):
            deficient_tickers.append(ticker)
            logger.info(f"⚠️ {ticker}: Added to deficient list due to 1-minute data issues")
            continue
            
        logger.debug(f"✅ {ticker}: All data health checks passed")
    
    # Analyze results and take action
    if not deficient_tickers:
        logger.info("🎉 Data health check complete. All tickers are compliant.")
        logger.info(f"✅ System health: 100% ({len(tickers)}/{len(tickers)} tickers compliant)")
        return True
    else:
        compliant_count = len(tickers) - len(deficient_tickers)
        compliance_rate = (compliant_count / len(tickers)) * 100 if tickers else 0
        
        logger.info(f"⚠️ Found {len(deficient_tickers)} deficient tickers: {deficient_tickers}")
        logger.info(f"📊 System health: {compliance_rate:.1f}% ({compliant_count}/{len(tickers)} tickers compliant)")
        logger.info("🔧 Triggering targeted full fetch for data recovery...")
        
        # Import and run targeted full fetch
        try:
            from jobs.full_fetch import run_full_fetch
            
            logger.info(f"🚀 Starting targeted full fetch for {len(deficient_tickers)} deficient tickers")
            recovery_success = run_full_fetch(tickers_to_fetch=deficient_tickers)
            
            if recovery_success:
                logger.info("✅ Targeted full fetch completed successfully")
                logger.info("🏥 Data recovery operation finished - system health should be restored")
                return True
            else:
                logger.error("❌ Targeted full fetch failed - manual intervention may be required")
                return False
                
        except Exception as e:
            logger.error(f"❌ Error during targeted full fetch: {e}")
            logger.error("💥 Data recovery failed - manual intervention required")
            return False


if __name__ == "__main__":
    job_name = "data_health_check"
    update_scheduler_status(job_name, "Running")
    
    try:
        success = run_health_check()
        
        if success:
            update_scheduler_status(job_name, "Success")
            logger.info("✅ Data health check job completed successfully")
        else:
            update_scheduler_status(job_name, "Fail", "Data health issues detected or recovery failed")
            logger.error("❌ Data health check job failed")
            
    except Exception as e:
        error_message = f"Critical error in data health check: {e}"
        logger.error(error_message)
        update_scheduler_status(job_name, "Fail", error_message)
        sys.exit(1)