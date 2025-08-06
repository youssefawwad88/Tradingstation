import pandas as pd
import sys
import os
from datetime import datetime, timedelta
import time
import logging

# Add project root to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from utils.helpers import read_tickerlist_from_s3, save_df_to_s3, update_scheduler_status, load_manual_tickers
from utils.alpha_vantage_api import get_daily_data, get_intraday_data

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def run_full_rebuild():
    """
    Runs the full data rebuild process once per day.
    - Fetches a clean, extended history for daily, 30-min, and 1-min data.
    - Overwrites existing files to ensure a clean slate.
    """
    logger.info("Starting Daily Full Data Rebuild Job")
    
    # Load tickers from S3 (S&P 500 or other universe)
    tickers = read_tickerlist_from_s3()
    if not tickers:
        tickers = []
        logger.warning("No tickers found in tickerlist.txt")
    
    # Always load and include manual tickers from ticker_selectors/tickerlist.txt
    manual_tickers = load_manual_tickers()
    if manual_tickers:
        logger.info(f"Adding {len(manual_tickers)} manual tickers: {manual_tickers}")
        tickers.extend(manual_tickers)
    else:
        logger.warning("No manual tickers found in ticker_selectors/tickerlist.txt")
    
    # Remove duplicates while preserving order
    tickers = list(dict.fromkeys(tickers))
    
    if not tickers:
        logger.error("No tickers to process. Exiting.")
        return

    logger.info(f"Processing {len(tickers)} tickers for full rebuild (including manual tickers)")

    for ticker in tickers:
        logger.debug(f"Processing {ticker} for full rebuild")

        # 1. Daily Data (200 days)
        try:
            # CORRECTED: Changed 'output_size' to 'outputsize'
            daily_df = get_daily_data(ticker, outputsize='full')
            if not daily_df.empty:
                daily_df = daily_df.head(200)
                save_df_to_s3(daily_df, f'data/daily/{ticker}_daily.csv')
                logger.debug(f"Saved {len(daily_df)} rows of daily data for {ticker}")
            else:
                logger.warning(f"No daily data returned for {ticker}")
        except Exception as e:
            logger.error(f"Error fetching daily data for {ticker}: {e}")

        # 2. 30-Minute Intraday Data (500 rows)
        try:
            # CORRECTED: Changed 'output_size' to 'outputsize'
            intraday_30min_df = get_intraday_data(ticker, interval='30min', outputsize='full')
            if not intraday_30min_df.empty:
                intraday_30min_df = intraday_30min_df.head(500)
                save_df_to_s3(intraday_30min_df, f'data/intraday_30min/{ticker}_30min.csv')
                logger.debug(f"Saved {len(intraday_30min_df)} rows of 30-min data for {ticker}")
            else:
                logger.warning(f"No 30-min data returned for {ticker}")
        except Exception as e:
            logger.error(f"Error fetching 30-min data for {ticker}: {e}")

        # 3. 1-Minute Intraday Data (Last 7 days)
        try:
            # CORRECTED: Changed 'output_size' to 'outputsize'
            intraday_1min_df = get_intraday_data(ticker, interval='1min', outputsize='full')
            if not intraday_1min_df.empty:
                intraday_1min_df['timestamp'] = pd.to_datetime(intraday_1min_df['timestamp'])
                seven_days_ago = datetime.now() - timedelta(days=7)
                intraday_1min_df = intraday_1min_df[intraday_1min_df['timestamp'] >= seven_days_ago]
                
                save_df_to_s3(intraday_1min_df, f'data/intraday/{ticker}_1min.csv')
                logger.debug(f"Saved {len(intraday_1min_df)} rows of 1-min data for {ticker} (last 7 days)")
            else:
                logger.warning(f"No 1-min data returned for {ticker}")
        except Exception as e:
            logger.error(f"Error fetching 1-min data for {ticker}: {e}")
        
        # Respect API rate limits
        time.sleep(1)

    logger.info("Daily Full Data Rebuild Job Finished")

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
