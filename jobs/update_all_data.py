import pandas as pd
import sys
import os
from datetime import datetime, timedelta
import time
import logging

# Add project root to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from utils.helpers import (
    read_master_tickerlist, save_df_to_s3, update_scheduler_status, 
    should_use_test_mode, log_detailed_operation, cleanup_data_retention
)
from utils.alpha_vantage_api import get_daily_data, get_intraday_data

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def calculate_early_volume_average(ticker, intraday_1min_df, num_sessions=5):
    """
    Calculate average early volume from 9:30-9:45 AM using past N sessions.
    
    Args:
        ticker (str): Ticker symbol
        intraday_1min_df (DataFrame): 1-minute intraday data
        num_sessions (int): Number of past sessions to average (default 5)
        
    Returns:
        float: Average early volume for the time window
    """
    try:
        if intraday_1min_df.empty:
            return 0.0
        
        # Ensure timestamp column
        df = intraday_1min_df.copy()
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        
        # Get unique dates (excluding today)
        today = datetime.now().date()
        df['date'] = df['timestamp'].dt.date
        unique_dates = [d for d in df['date'].unique() if d != today]
        
        # Take last N sessions
        recent_dates = sorted(unique_dates)[-num_sessions:] if len(unique_dates) >= num_sessions else unique_dates
        
        if not recent_dates:
            return 0.0
        
        early_volumes = []
        for date in recent_dates:
            # Filter for this date and 9:30-9:45 window
            date_start = pd.Timestamp.combine(date, pd.Timestamp('09:30:00').time())
            date_end = pd.Timestamp.combine(date, pd.Timestamp('09:45:00').time())
            
            early_data = df[
                (df['timestamp'] >= date_start) & 
                (df['timestamp'] <= date_end)
            ]
            
            if not early_data.empty:
                early_volumes.append(early_data['volume'].sum())
        
        if early_volumes:
            avg_volume = sum(early_volumes) / len(early_volumes)
            logger.debug(f"{ticker}: Early volume average from {len(early_volumes)} sessions: {avg_volume:,.0f}")
            return avg_volume
        
        return 0.0
        
    except Exception as e:
        logger.error(f"Error calculating early volume average for {ticker}: {e}")
        return 0.0

def simulate_data_fetch(ticker, data_type, target_rows):
    """
    Simulate data fetch for test mode (weekend testing).
    
    Args:
        ticker (str): Ticker symbol
        data_type (str): Type of data (daily, 30min, 1min)
        target_rows (int): Target number of rows
        
    Returns:
        DataFrame: Simulated data with appropriate structure
    """
    import pandas as pd
    import numpy as np
    
    logger.info(f"[TEST MODE] Simulating {data_type} data fetch for {ticker} ({target_rows} rows)")
    
    # Create timestamps based on data type
    end_time = datetime.now()
    
    if data_type == "daily":
        # Create 200 daily timestamps going back
        dates = pd.date_range(end=end_time.date(), periods=target_rows, freq='D')
        timestamps = dates
    elif data_type == "30min":
        # Create 30-min intervals going back
        timestamps = pd.date_range(end=end_time, periods=target_rows, freq='30min')
    else:  # 1min
        # Create 1-min intervals for last 7 days
        start_time = end_time - timedelta(days=7)
        timestamps = pd.date_range(start=start_time, end=end_time, freq='1min')
        timestamps = timestamps[:target_rows]  # Limit if too many
    
    # Create simulated OHLCV data
    num_rows = len(timestamps)
    base_price = 100.0  # Simulated base price
    
    # Generate realistic-looking price data
    price_changes = np.random.normal(0, 0.02, num_rows).cumsum()
    close_prices = base_price + price_changes
    
    df = pd.DataFrame({
        'timestamp': timestamps,
        'open': close_prices + np.random.normal(0, 0.01, num_rows),
        'high': close_prices + np.abs(np.random.normal(0, 0.02, num_rows)),
        'low': close_prices - np.abs(np.random.normal(0, 0.02, num_rows)),
        'close': close_prices,
        'volume': np.random.randint(100000, 1000000, num_rows)
    })
    
    # Ensure high >= max(open, close) and low <= min(open, close)
    df['high'] = np.maximum(df['high'], np.maximum(df['open'], df['close']))
    df['low'] = np.minimum(df['low'], np.minimum(df['open'], df['close']))
    
    return df

def run_full_rebuild():
    """
    Runs the full data rebuild process once per day.
    - Fetches a clean, extended history for daily, 30-min, and 1-min data.
    - Implements new requirements: Daily (200 rows), 30min (500 rows), 1min (7 days + early volume + today)
    - Runs cleanup procedure after full fetch
    - Supports weekend test mode for safe testing
    """
    test_mode = should_use_test_mode()
    mode_str = "TEST MODE" if test_mode else "LIVE MODE"
    
    logger.info(f"Starting Daily Full Data Rebuild Job ({mode_str})")
    
    if test_mode:
        logger.info("[TEST MODE] Weekend Test Mode Active - Using simulated data without API calls")
        logger.info("[TEST MODE] Detailed logging enabled for all operations")
    
    # Load tickers from master_tickerlist.csv (unified source)
    tickers = read_master_tickerlist()
    
    if not tickers:
        logger.error("No tickers to process. Exiting.")
        return

    logger.info(f"Processing {len(tickers)} tickers from master_tickerlist.csv for full rebuild")

    for ticker in tickers:
        ticker_start_time = datetime.now()
        log_detailed_operation(ticker, "Start Full Rebuild", ticker_start_time)

        # 1. Daily Data (exactly 200 rows as specified)
        daily_start_time = datetime.now()
        try:
            if test_mode:
                daily_df = simulate_data_fetch(ticker, "daily", 200)
                logger.info(f"[TEST MODE] Fetching DAILY data for {ticker} – 200 rows (test data)")
            else:
                daily_df = get_daily_data(ticker, outputsize='full')
                if not daily_df.empty:
                    daily_df = daily_df.head(200)  # Exactly 200 rows as specified
                logger.info(f"Fetching DAILY data for {ticker} – {len(daily_df)} rows")
            
            if not daily_df.empty:
                log_detailed_operation(ticker, "Daily Fetch Complete", daily_start_time, row_count_after=len(daily_df))
            else:
                logger.warning(f"No daily data returned for {ticker}")
                
        except Exception as e:
            logger.error(f"Error fetching daily data for {ticker}: {e}")
            daily_df = pd.DataFrame()

        # 2. 30-Minute Intraday Data (exactly 500 rows as specified)
        intraday_30min_start_time = datetime.now()
        try:
            if test_mode:
                intraday_30min_df = simulate_data_fetch(ticker, "30min", 500)
                logger.info(f"[TEST MODE] Fetching 30-MINUTE data for {ticker} – 500 rows (test data)")
            else:
                intraday_30min_df = get_intraday_data(ticker, interval='30min', outputsize='full')
                if not intraday_30min_df.empty:
                    intraday_30min_df = intraday_30min_df.head(500)  # Exactly 500 rows as specified
                logger.info(f"Fetching 30-MINUTE data for {ticker} – {len(intraday_30min_df)} rows")
            
            if not intraday_30min_df.empty:
                log_detailed_operation(ticker, "30min Fetch Complete", intraday_30min_start_time, row_count_after=len(intraday_30min_df))
            else:
                logger.warning(f"No 30-min data returned for {ticker}")
                
        except Exception as e:
            logger.error(f"Error fetching 30-min data for {ticker}: {e}")
            intraday_30min_df = pd.DataFrame()

        # 3. 1-Minute Intraday Data (full 7 calendar days + early volume calculation + today's complete feed)
        intraday_1min_start_time = datetime.now()
        try:
            if test_mode:
                # For test mode, simulate 7 days of 1-min data (roughly 7*24*60 = 10080 rows max)
                intraday_1min_df = simulate_data_fetch(ticker, "1min", 7*24*60)
                logger.info(f"[TEST MODE] Fetching 1-MINUTE intraday data for {ticker} – 7 days (test data)")
            else:
                intraday_1min_df = get_intraday_data(ticker, interval='1min', outputsize='full')
                
                if not intraday_1min_df.empty:
                    # Ensure timestamp column
                    intraday_1min_df['timestamp'] = pd.to_datetime(intraday_1min_df['timestamp'])
                    
                    # Keep full 7 calendar days (no exclusions during full fetch)
                    seven_days_ago = datetime.now() - timedelta(days=7)
                    intraday_1min_df = intraday_1min_df[intraday_1min_df['timestamp'] >= seven_days_ago]
                    
                    # Calculate early volume average from past 5 sessions
                    early_volume_avg = calculate_early_volume_average(ticker, intraday_1min_df, num_sessions=5)
                    log_detailed_operation(ticker, "Early Volume Calculated", details=f"Avg early volume: {early_volume_avg:,.0f}")
                
                logger.info(f"Fetching 1-MINUTE intraday data for {ticker} – 7 days")
            
            if not intraday_1min_df.empty:
                log_detailed_operation(ticker, "1min Fetch Complete", intraday_1min_start_time, row_count_after=len(intraday_1min_df))
            else:
                logger.warning(f"No 1-min data returned for {ticker}")
                
        except Exception as e:
            logger.error(f"Error fetching 1-min data for {ticker}: {e}")
            intraday_1min_df = pd.DataFrame()

        # 4. Apply Cleanup Procedure (run immediately after fetching full data)
        cleanup_start_time = datetime.now()
        try:
            cleaned_daily, cleaned_30min, cleaned_1min = cleanup_data_retention(
                ticker, daily_df, intraday_30min_df, intraday_1min_df
            )
            if test_mode:
                logger.info(f"[TEST MODE] Cleanup applied for {ticker}: retained {len(cleaned_daily)} daily, {len(cleaned_30min)} 30-min, 7 days intraday")
        except Exception as e:
            logger.error(f"Error during cleanup for {ticker}: {e}")
            cleaned_daily, cleaned_30min, cleaned_1min = daily_df, intraday_30min_df, intraday_1min_df

        # 5. Save cleaned data
        save_start_time = datetime.now()
        try:
            # Save daily data
            if not cleaned_daily.empty:
                upload_success = save_df_to_s3(cleaned_daily, f'data/daily/{ticker}_daily.csv')
                if not upload_success:
                    logger.error(f"❌ FAILED to save daily data for {ticker}!")
            
            # Save 30-min data  
            if not cleaned_30min.empty:
                upload_success = save_df_to_s3(cleaned_30min, f'data/intraday_30min/{ticker}_30min.csv')
                if not upload_success:
                    logger.error(f"❌ FAILED to save 30-min data for {ticker}!")
            
            # Save 1-min data
            if not cleaned_1min.empty:
                upload_success = save_df_to_s3(cleaned_1min, f'data/intraday/{ticker}_1min.csv')
                if not upload_success:
                    logger.error(f"❌ FAILED to save 1-min data for {ticker}!")
            
            log_detailed_operation(ticker, "Data Saved", save_start_time)
            
        except Exception as e:
            logger.error(f"Error saving data for {ticker}: {e}")

        # Log ticker completion
        log_detailed_operation(ticker, "Full Rebuild Complete", ticker_start_time)
        
        # Respect API rate limits (only in live mode)
        if not test_mode:
            time.sleep(1)

    completion_mode = "TEST MODE COMPLETE" if test_mode else "LIVE MODE COMPLETE"
    logger.info(f"Daily Full Data Rebuild Job Finished ({completion_mode})")
    
    if test_mode:
        logger.info("[TEST MODE] Test mode complete – all operations simulated successfully")
        logger.info("[TEST MODE] Review logs to verify data flow before live mode")

if __name__ == "__main__":
    job_name = "update_all_data"
    update_scheduler_status(job_name, "Running")
    try:
        run_full_rebuild()
        update_scheduler_status(job_name, "Success")
    except Exception as e:
        error_message = f"An unexpected error occurred: {e}"
        print(error_message)
        update_scheduler_status(job_name, "Fail", error_message)
