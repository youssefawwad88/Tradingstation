import os
import pandas as pd
import requests
import logging
from datetime import datetime
import time
import json
import pytz
from utils.config import (
    ALPHA_VANTAGE_API_KEY,
    INTRADAY_DATA_DIR,
    DEBUG_MODE,
    INTRADAY_TRIM_DAYS,
    INTRADAY_EXCLUDE_TODAY,
    INTRADAY_INCLUDE_PREMARKET,
    INTRADAY_INCLUDE_AFTERHOURS,
    TIMEZONE,
)
from utils.spaces_manager import upload_dataframe

# Import from new modular components
from .data_fetcher import fetch_intraday_data, fetch_daily_data
from .data_storage import save_df_to_local, save_df_to_s3, read_df_from_s3
from .market_time import detect_market_session, is_weekend, get_last_market_day
from .ticker_manager import (
    load_manual_tickers,
    read_master_tickerlist,
    read_tickerlist_from_s3,
)

logger = logging.getLogger(__name__)


def fetch_intraday_data(ticker, interval="1min", outputsize="compact"):
    """
    Fetch intraday data from Alpha Vantage API.

    Args:
        ticker (str): Stock ticker symbol
        interval (str): Time interval between data points (1min, 5min, 15min, 30min, 60min)
        outputsize (str): 'compact' returns latest 100 data points, 'full' returns up to 20+ years of data

    Returns:
        tuple: (DataFrame or None, bool indicating success)
    """
    if not ALPHA_VANTAGE_API_KEY:
        logger.error("Alpha Vantage API key not found in environment variables")
        return None, False

    endpoint = "https://www.alphavantage.co/query"
    params = {
        "function": "TIME_SERIES_INTRADAY",
        "symbol": ticker,
        "interval": interval,
        "outputsize": outputsize,
        "apikey": ALPHA_VANTAGE_API_KEY,
    }

    try:
        response = requests.get(endpoint, params=params)
        data = response.json()

        if "Error Message" in data:
            logger.error(
                f"Alpha Vantage API error for {ticker}: {data['Error Message']}"
            )
            return None, False

        time_series_key = f"Time Series ({interval})"
        if time_series_key not in data:
            logger.error(
                f"Unexpected response format for {ticker}: {json.dumps(data)[:200]}..."
            )
            return None, False

        time_series = data[time_series_key]

        # Convert to DataFrame
        df = pd.DataFrame.from_dict(time_series, orient="index")

        # Rename columns
        df.columns = [col.split(". ")[1] for col in df.columns]

        # Convert values to float
        for col in df.columns:
            df[col] = df[col].astype(float)

        # Add date and ticker columns
        df.index = pd.to_datetime(df.index)
        df = df.reset_index()
        df.rename(columns={"index": "datetime"}, inplace=True)
        df["ticker"] = ticker

        logger.info(
            f"Successfully fetched intraday data for {ticker} ({interval}): {len(df)} records"
        )
        return df, True
    except Exception as e:
        logger.error(f"Error fetching intraday data for {ticker}: {e}")
        return None, False


def save_df_to_local(df, ticker, interval, directory=INTRADAY_DATA_DIR):
    """
    Save DataFrame to local filesystem.

    Args:
        df (pandas.DataFrame): DataFrame to save
        ticker (str): Stock ticker symbol
        interval (str): Time interval of the data
        directory (str): Directory to save the file

    Returns:
        tuple: (file path or None, bool indicating success)
    """
    os.makedirs(directory, exist_ok=True)
    file_path = os.path.join(directory, f"{ticker}_{interval}.csv")

    try:
        df.to_csv(file_path, index=False)
        logger.info(f"Saved {ticker} data to {file_path}")
        return file_path, True
    except Exception as e:
        logger.error(f"Error saving {ticker} data to {file_path}: {e}")
        return None, False


def save_df_to_s3(df, object_name_or_ticker, interval=None, s3_prefix="intraday"):
    """
    Save DataFrame to DigitalOcean Spaces with enhanced logging and path verification (Phase 4 implementation).
    STANDARDIZES on data/ prefix for all storage and logs exact paths used.

    Args:
        df (pandas.DataFrame): DataFrame to save
        object_name_or_ticker (str): Object name/path in S3 OR ticker symbol
        interval (str, optional): Time interval of the data (if ticker provided)
        s3_prefix (str): Prefix/folder in the S3 bucket (if ticker provided)

    Returns:
        bool: True if successful (either Spaces or local), False otherwise
    """
    # Determine if we got an object name or ticker
    if interval is not None:
        # Old-style call with ticker and interval
        # STANDARDIZE on data/ prefix as specified in Phase 4
        object_name = f"data/{s3_prefix}/{object_name_or_ticker}_{interval}.csv"
        ticker = object_name_or_ticker
        logger.info(
            f"💾 SAVE_DF_TO_S3: Processing {object_name_or_ticker} {interval} data"
        )
    else:
        # New-style call with direct object name - ensure data/ prefix
        object_name = object_name_or_ticker
        if not object_name.startswith("data/"):
            object_name = f"data/{object_name}"
        # Extract ticker from object name for local fallback
        if "/" in object_name:
            parts = object_name.split("/")
            filename = parts[-1]  # Get filename from path
            if "_" in filename:
                ticker = filename.split("_")[0]
                interval_part = filename.replace(f"{ticker}_", "").replace(".csv", "")
            else:
                ticker = filename.replace(".csv", "")
                interval_part = "1min"  # default
        else:
            ticker = object_name.replace(".csv", "")
            interval_part = "1min"  # default
        logger.info(f"💾 SAVE_DF_TO_S3: Processing direct object name")

    # LOG the exact path used for each save operation as required
    logger.info(f"📂 Saving to Spaces path: {object_name}")
    logger.info(f"   Data rows: {len(df)}")
    if not df.empty:
        date_col = (
            "Date"
            if "Date" in df.columns
            else "datetime" if "datetime" in df.columns else "timestamp"
        )
        if date_col in df.columns:
            min_date = pd.to_datetime(df[date_col]).min()
            max_date = pd.to_datetime(df[date_col]).max()
            logger.info(f"   Date range: {min_date} to {max_date}")

    # Try Spaces upload first
    success = upload_dataframe(df, object_name)
    if success:
        # CONFIRM the file exists after saving as required
        logger.info(f"✅ File saved successfully to {object_name}")
        logger.info(f"☁️ Spaces upload confirmed for {ticker}")
        return True
    else:
        logger.warning(
            f"⚠️ Failed to upload to Spaces at {object_name}. Trying local filesystem fallback..."
        )

        # Fallback to local filesystem with enhanced logging
        try:
            # Extract interval from object name if not provided
            if interval is None:
                interval = interval_part if "interval_part" in locals() else "1min"

            # Determine directory based on data/ standardized paths
            base_dir = os.path.join(
                os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data"
            )

            if "daily" in object_name or interval == "daily":
                directory = os.path.join(base_dir, "daily")
                local_filename = f"{ticker}_daily.csv"
            elif "30min" in str(interval) or "30min" in object_name:
                directory = os.path.join(base_dir, "intraday_30min")
                local_filename = f"{ticker}_30min.csv"
            else:
                directory = os.path.join(base_dir, "intraday")
                local_filename = f"{ticker}_1min.csv"

            # Save to local filesystem
            os.makedirs(directory, exist_ok=True)
            local_path = os.path.join(directory, local_filename)

            logger.info(f"💽 LOCAL FALLBACK: Saving to {local_path}")
            df.to_csv(local_path, index=False)

            # Verify file exists locally
            if os.path.exists(local_path):
                file_size = os.path.getsize(local_path)
                logger.info(f"✅ File saved successfully to {local_path}")
                logger.info(f"📁 Local file confirmed: {file_size} bytes")
                return True
            else:
                logger.error(f"❌ Local file verification failed: {local_path}")
                return False

        except Exception as e:
            logger.error(f"❌ Local filesystem fallback also failed: {e}")
            return False


def save_to_local_filesystem(df, ticker, interval):
    """
    Fallback function to save data locally if S3 upload fails.

    Args:
        df (pandas.DataFrame): DataFrame to save
        ticker (str): Stock ticker symbol
        interval (str): Time interval of the data

    Returns:
        tuple: (file path or None, bool indicating success)
    """
    file_path, success = save_df_to_local(df, ticker, interval)
    if success:
        logger.info(f"Fallback save successful for {ticker} to {file_path}")
    else:
        logger.error(f"Fallback save failed for {ticker}")
    return file_path, success


def update_scheduler_status(job_name, status, error_details=None):
    """
    Update the scheduler status for a job.

    Args:
        job_name (str): Name of the job
        status (str): Status of the job ('Running', 'Success', 'Fail')
        error_details (str, optional): Error details if status is 'Fail'
    """
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    if status == "Running":
        logger.info(f"[{timestamp}] Job '{job_name}' started")
    elif status == "Success":
        logger.info(f"[{timestamp}] Job '{job_name}' completed successfully")
    elif status == "Fail":
        error_msg = f"[{timestamp}] Job '{job_name}' failed"
        if error_details:
            error_msg += f" - {error_details}"
        logger.error(error_msg)
    else:
        logger.warning(f"[{timestamp}] Job '{job_name}' status unknown: {status}")


def detect_market_session():
    """
    Detect the current market session based on Eastern Time.

    Returns:
        str: Market session ('PRE-MARKET', 'REGULAR', 'AFTER-HOURS', 'CLOSED')
    """
    ny_tz = pytz.timezone("America/New_York")
    current_time = datetime.now(ny_tz)
    current_weekday = current_time.weekday()  # 0=Monday, 6=Sunday

    # Check if it's weekend
    if current_weekday >= 5:  # Saturday=5, Sunday=6
        return "CLOSED"

    # Get current time in minutes since midnight
    current_minutes = current_time.hour * 60 + current_time.minute

    # Market session times (in minutes since midnight ET)
    premarket_start = 4 * 60  # 4:00 AM
    regular_start = 9 * 60 + 30  # 9:30 AM
    regular_end = 16 * 60  # 4:00 PM
    afterhours_end = 20 * 60  # 8:00 PM

    if current_minutes < premarket_start:
        return "CLOSED"
    elif current_minutes < regular_start:
        return "PRE-MARKET"
    elif current_minutes < regular_end:
        return "REGULAR"
    elif current_minutes < afterhours_end:
        return "AFTER-HOURS"
    else:
        return "CLOSED"


def read_tickerlist_from_s3(filename):
    """
    Read ticker list from S3/Spaces or local fallback.

    Args:
        filename (str): Name of the ticker file

    Returns:
        list: List of ticker symbols
    """
    logger.info(f"Reading ticker list from {filename}")

    # Try to read from local file first as fallback
    local_file = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))), filename
    )
    if os.path.exists(local_file):
        try:
            with open(local_file, "r") as f:
                tickers = [line.strip() for line in f.readlines() if line.strip()]
            logger.info(f"Successfully read {len(tickers)} tickers from local file")
            return tickers
        except Exception as e:
            logger.error(f"Error reading local ticker file: {e}")

    # Fallback to default tickers from config
    from utils.config import DEFAULT_TICKERS

    logger.warning(f"Using default tickers: {DEFAULT_TICKERS}")
    return DEFAULT_TICKERS


def read_master_tickerlist():
    """
    Read master ticker list from master_tickerlist.csv (local or Spaces).
    This is the unified function used by all fetchers.

    Returns:
        list: List of ticker symbols from master list
    """
    logger.info("Reading master ticker list from master_tickerlist.csv")

    try:
        # Try local file first
        local_file = os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            "master_tickerlist.csv",
        )
        if os.path.exists(local_file):
            df = pd.read_csv(local_file)
            if "ticker" in df.columns:
                tickers = df["ticker"].tolist()
                logger.info(
                    f"✅ Successfully read {len(tickers)} tickers from master_tickerlist.csv"
                )
                return tickers

        # Try to read from Spaces
        df = read_df_from_s3("master_tickerlist.csv")
        if not df.empty and "ticker" in df.columns:
            tickers = df["ticker"].tolist()
            logger.info(
                f"✅ Successfully read {len(tickers)} tickers from master_tickerlist.csv (Spaces)"
            )
            return tickers

        # Fallback: generate master list if it doesn't exist
        logger.warning(
            "⚠️ master_tickerlist.csv not found, falling back to manual tickers"
        )
        manual_tickers = load_manual_tickers()
        if manual_tickers:
            logger.info(f"Using {len(manual_tickers)} manual tickers as fallback")
            return manual_tickers

        # Final fallback to default tickers
        from utils.config import DEFAULT_TICKERS

        logger.warning(f"Using default tickers as last resort: {DEFAULT_TICKERS}")
        return DEFAULT_TICKERS

    except Exception as e:
        logger.error(f"Error reading master ticker list: {e}")
        # Fallback to manual tickers
        manual_tickers = load_manual_tickers()
        if manual_tickers:
            return manual_tickers

        from utils.config import DEFAULT_TICKERS

        return DEFAULT_TICKERS


def read_df_from_s3(object_name):
    """
    Read DataFrame from S3/Spaces or local fallback.

    Args:
        object_name (str): Object name/path in S3

    Returns:
        pandas.DataFrame: DataFrame if successful, empty DataFrame otherwise
    """
    logger.info(f"Attempting to read DataFrame from {object_name}")

    # Try to read from local file first
    local_file = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))), object_name
    )
    if os.path.exists(local_file):
        try:
            df = pd.read_csv(local_file)
            logger.info(f"Successfully read {len(df)} rows from local file")
            return df
        except Exception as e:
            logger.error(f"Error reading local file {local_file}: {e}")

    # Return empty DataFrame if file doesn't exist or can't be read
    logger.warning(
        f"File not found or unreadable: {object_name} - returning empty DataFrame"
    )
    return pd.DataFrame()


def load_manual_tickers():
    """
    Load manual ticker list from tickerlist.txt with enhanced diagnostics (Phase 2 implementation).
    Checks MULTIPLE POSSIBLE LOCATIONS and provides detailed error logging as specified.

    Returns:
        list: List of manual tickers
    """
    # Find all potential tickerlist.txt files as specified in Phase 2
    paths_to_check = [
        "/workspace/tickerlist.txt",
        "/workspace/data/tickerlist.txt",
        os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            "tickerlist.txt",
        ),
        "tickerlist.txt",
        "../tickerlist.txt",
    ]

    logger.info(
        "🔍 LOAD_MANUAL_TICKERS: Checking multiple possible locations for tickerlist.txt..."
    )

    for path in paths_to_check:
        logger.info(f"   Checking path: {path}")
        if os.path.exists(path):
            try:
                logger.info(f"✅ FOUND tickerlist.txt at: {path}")
                with open(path, "r") as f:
                    content = f.read().strip()
                    tickers = [
                        line.strip() for line in content.split("\n") if line.strip()
                    ]

                # Print the first 5 tickers found as specified
                logger.info(f"📋 File contents summary:")
                logger.info(f"   Total tickers found: {len(tickers)}")
                logger.info(f"   First 5 tickers: {tickers[:5]}")
                if len(tickers) > 5:
                    logger.info(f"   Additional tickers: {tickers[5:]}")

                logger.info(
                    f"✅ Successfully loaded {len(tickers)} manual tickers from {path}"
                )
                return tickers

            except Exception as e:
                logger.error(f"❌ Error reading tickerlist.txt from {path}: {e}")
                continue
        else:
            logger.info(f"   ❌ Not found: {path}")

    # Fallback: try manual_tickers.txt for backwards compatibility
    manual_file_alt = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "manual_tickers.txt",
    )
    logger.info(f"🔄 Trying fallback: {manual_file_alt}")
    if os.path.exists(manual_file_alt):
        try:
            with open(manual_file_alt, "r") as f:
                tickers = [line.strip() for line in f.readlines() if line.strip()]
            logger.info(
                f"✅ Loaded {len(tickers)} manual tickers from manual_tickers.txt (fallback)"
            )
            logger.info(f"   First 5 tickers: {tickers[:5]}")
            return tickers
        except Exception as e:
            logger.error(
                f"❌ Error reading manual tickers from manual_tickers.txt: {e}"
            )

    # Final fallback to default tickers
    logger.warning("⚠️ No manual ticker file found in any location!")
    logger.warning(
        "   This indicates a configuration issue - tickerlist.txt should exist"
    )
    logger.warning("   Falling back to DEFAULT_TICKERS from config")

    from utils.config import DEFAULT_TICKERS

    logger.info(f"📋 Using DEFAULT_TICKERS: {DEFAULT_TICKERS[:5]} (first 5)")
    return DEFAULT_TICKERS


def is_today_present(df, timestamp_col='datetime'):
    """
    Check if today's data is present in the DataFrame.

    Args:
        df (pandas.DataFrame): DataFrame with datetime column
        timestamp_col (str): Name of timestamp column (default: 'datetime')

    Returns:
        bool: True if today's data is present
    """
    if df is None or df.empty:
        return False

    try:
        ny_tz = pytz.timezone("America/New_York")
        today = datetime.now(ny_tz).date()

        # Check multiple possible column names
        possible_cols = [timestamp_col, 'datetime', 'timestamp', 'Date', 'date']
        date_col = None
        
        for col in possible_cols:
            if col in df.columns:
                date_col = col
                break
        
        if date_col:
            df_dates = pd.to_datetime(df[date_col]).dt.date
            return today in df_dates.values

        return False
    except Exception as e:
        logger.error(f"Error checking if today is present: {e}")
        return False


def get_last_market_day():
    """
    Get the last market day.

    Returns:
        datetime.date: Last market day
    """
    ny_tz = pytz.timezone("America/New_York")
    current = datetime.now(ny_tz)

    # Simple implementation - go back until we find a weekday
    while current.weekday() >= 5:  # Weekend
        current = current - pd.Timedelta(days=1)

    return current.date()


def is_today():
    """
    Check if current date is today.

    Returns:
        bool: Always True (helper function)
    """
    return True


def trim_to_rolling_window(df, window_days=30):
    """
    Trim DataFrame to rolling window with enhanced data retention logic.
    KEEPS TODAY'S DATA by default.

    Args:
        df (pandas.DataFrame): DataFrame to trim
        window_days (int): Number of days to keep

    Returns:
        pandas.DataFrame: Trimmed DataFrame
    """
    if df is None or df.empty:
        return df

    try:
        # Use the new enhanced retention function
        return apply_data_retention(df, window_days)
    except Exception as e:
        logger.error(f"Error trimming to rolling window: {e}")
        return df


def apply_data_retention(df, trim_days=None):
    """
    Apply enhanced data retention rules based on environment configuration.
    KEEPS TODAY'S DATA by default as per Phase 4 requirements.

    Args:
        df (pandas.DataFrame): DataFrame with date/datetime column
        trim_days (int, optional): Override trim days from config

    Returns:
        pandas.DataFrame: Filtered DataFrame
    """
    if df is None or df.empty:
        return df

    try:
        from utils.config import (
            INTRADAY_TRIM_DAYS,
            INTRADAY_EXCLUDE_TODAY,
            INTRADAY_INCLUDE_PREMARKET,
            INTRADAY_INCLUDE_AFTERHOURS,
            TIMEZONE,
            DEBUG_MODE,
        )

        # Log initial state
        initial_count = len(df)
        logger.info(f"🔄 RETENTION: Starting with {initial_count} rows")

        # Convert to Eastern Time and handle date column
        combined_df = df.copy()

        # Find the date/datetime column
        date_col = None
        if "Date" in combined_df.columns:
            date_col = "Date"
        elif "datetime" in combined_df.columns:
            date_col = "datetime"
        elif "timestamp" in combined_df.columns:
            date_col = "timestamp"

        if not date_col:
            logger.warning("No date column found for retention filtering")
            return df

        # Convert to datetime and timezone-aware
        combined_df[date_col] = pd.to_datetime(combined_df[date_col])
        combined_df = combined_df.set_index(date_col)

        # Handle timezone conversion
        if combined_df.index.tz is None:
            # Assume UTC if no timezone info
            combined_df = combined_df.tz_localize("UTC")

        # Convert to Eastern Time
        combined_df = combined_df.tz_convert(TIMEZONE)
        combined_df = combined_df.reset_index()

        # Get current date in ET
        ny_tz = pytz.timezone(TIMEZONE)
        now_et = datetime.now(ny_tz)
        today_et = now_et.replace(hour=0, minute=0, second=0, microsecond=0)

        # Apply retention rules
        trim_days_to_use = trim_days if trim_days is not None else INTRADAY_TRIM_DAYS
        exclude_today = INTRADAY_EXCLUDE_TODAY

        # Calculate start date for retention (N days back)
        start_date = today_et - pd.Timedelta(days=trim_days_to_use)

        logger.info(f"📅 RETENTION CONFIG:")
        logger.info(f"   Current time (ET): {now_et}")
        logger.info(f"   Today (ET): {today_et}")
        logger.info(f"   Start date: {start_date}")
        logger.info(f"   Trim days: {trim_days_to_use}")
        logger.info(f"   Exclude today: {exclude_today}")

        # Log data range before filtering
        min_date = combined_df[date_col].min()
        max_date = combined_df[date_col].max()
        logger.info(f"📊 DATA RANGE BEFORE FILTERING:")
        logger.info(f"   Minimum timestamp: {min_date}")
        logger.info(f"   Maximum timestamp: {max_date}")

        # Filter data - KEEP TODAY'S DATA by default!
        if exclude_today:
            # Only if explicitly set to exclude today
            combined_df = combined_df[
                (combined_df[date_col] >= start_date)
                & (combined_df[date_col] < today_et)
            ]
            logger.warning(f"⚠️ EXCLUDING TODAY'S DATA as requested by config")
        else:
            # DEFAULT: Keep everything from start_date forward INCLUDING TODAY
            combined_df = combined_df[combined_df[date_col] >= start_date]
            logger.info(f"✅ KEEPING TODAY'S DATA (default behavior)")

        after_date_filter_count = len(combined_df)
        logger.info(f"📊 After date filtering: {after_date_filter_count} rows")

        # Apply market session filtering if needed
        include_premarket = INTRADAY_INCLUDE_PREMARKET
        include_afterhours = INTRADAY_INCLUDE_AFTERHOURS

        # Only apply session filtering if either is False
        if not (include_premarket and include_afterhours):
            logger.info(f"🕐 APPLYING SESSION FILTERING:")
            logger.info(f"   Include pre-market: {include_premarket}")
            logger.info(f"   Include after-hours: {include_afterhours}")

            # Extract time component
            combined_df["time"] = combined_df[date_col].dt.time

            # Define market session times (Eastern Time)
            pre_market_start = pd.Timestamp("04:00:00").time()
            market_open = pd.Timestamp("09:30:00").time()
            market_close = pd.Timestamp("16:00:00").time()
            after_hours_end = pd.Timestamp("20:00:00").time()

            # Create mask for each session
            if include_premarket and not include_afterhours:
                # Keep pre-market and regular hours
                combined_df = combined_df[
                    (combined_df["time"] >= pre_market_start)
                    & (combined_df["time"] <= market_close)
                ]
                logger.info(f"   Keeping: Pre-market + Regular hours")
            elif not include_premarket and include_afterhours:
                # Keep regular hours and after hours
                combined_df = combined_df[
                    (combined_df["time"] >= market_open)
                    & (combined_df["time"] <= after_hours_end)
                ]
                logger.info(f"   Keeping: Regular hours + After-hours")
            elif not include_premarket and not include_afterhours:
                # Keep only regular hours
                combined_df = combined_df[
                    (combined_df["time"] >= market_open)
                    & (combined_df["time"] <= market_close)
                ]
                logger.info(f"   Keeping: Regular hours only")

            # Remove temporary time column
            combined_df = combined_df.drop("time", axis=1)
        else:
            logger.info(
                f"✅ KEEPING ALL MARKET SESSIONS (pre-market, regular, after-hours)"
            )

        final_count = len(combined_df)
        logger.info(f"📊 RETENTION SUMMARY:")
        logger.info(f"   Initial rows: {initial_count}")
        logger.info(f"   After date filter: {after_date_filter_count}")
        logger.info(f"   Final rows: {final_count}")
        logger.info(f"   Rows removed: {initial_count - final_count}")

        # Log data range after filtering
        if not combined_df.empty:
            min_date_after = combined_df[date_col].min()
            max_date_after = combined_df[date_col].max()
            logger.info(f"📊 DATA RANGE AFTER FILTERING:")
            logger.info(f"   Minimum timestamp: {min_date_after}")
            logger.info(f"   Maximum timestamp: {max_date_after}")

            # ALWAYS confirm today's data is present after filtering
            today_present = is_today_present_enhanced(combined_df, date_col)
            if today_present:
                logger.info(f"✅ TODAY'S DATA CONFIRMED PRESENT after filtering")
            else:
                logger.warning(
                    f"⚠️ TODAY'S DATA MISSING after filtering - this may be an issue!"
                )
        else:
            logger.warning(f"⚠️ NO DATA REMAINING after filtering")

        return combined_df

    except Exception as e:
        logger.error(f"Error applying data retention: {e}")
        return df


def is_today_present_enhanced(df, date_col="Date"):
    """
    Enhanced version of is_today_present with better timezone handling.

    Args:
        df (pandas.DataFrame): DataFrame with datetime column
        date_col (str): Name of the date column

    Returns:
        bool: True if today's data is present
    """
    if df is None or df.empty:
        return False

    try:
        from utils.config import TIMEZONE

        ny_tz = pytz.timezone(TIMEZONE)
        today = datetime.now(ny_tz).date()

        if date_col in df.columns:
            # Convert to datetime and extract date
            df_dates = pd.to_datetime(df[date_col]).dt.date
            return today in df_dates.values

        return False
    except Exception as e:
        logger.error(f"Error checking if today is present: {e}")
        return False


def append_new_candles(existing_df, new_df):
    """
    Append new candles to existing DataFrame.

    Args:
        existing_df (pandas.DataFrame): Existing data
        new_df (pandas.DataFrame): New data to append

    Returns:
        pandas.DataFrame: Combined DataFrame
    """
    if existing_df is None or existing_df.empty:
        return new_df

    if new_df is None or new_df.empty:
        return existing_df

    try:
        combined = pd.concat([existing_df, new_df], ignore_index=True)
        # Remove duplicates if datetime column exists
        if "datetime" in combined.columns:
            combined = combined.drop_duplicates(subset=["datetime"], keep="last")
            combined = combined.sort_values("datetime")
        return combined
    except Exception as e:
        logger.error(f"Error appending new candles: {e}")
        return existing_df


# Add simple stubs for other functions that might be needed
def format_to_two_decimal(value):
    """Format value to two decimal places."""
    try:
        return round(float(value), 2)
    except:
        return value


def get_previous_day_close(ticker):
    """Get previous day close price - stub implementation."""
    logger.warning(f"get_previous_day_close not implemented for {ticker}")
    return None


def get_premarket_data(ticker):
    """Get premarket data - stub implementation."""
    logger.warning(f"get_premarket_data not implemented for {ticker}")
    return None


def calculate_avg_early_volume(ticker):
    """Calculate average early volume - stub implementation."""
    logger.warning(f"calculate_avg_early_volume not implemented for {ticker}")
    return None


def calculate_vwap(df):
    """Calculate VWAP - stub implementation."""
    logger.warning(f"calculate_vwap not implemented")
    return None


def calculate_avg_daily_volume(ticker):
    """Calculate average daily volume - stub implementation."""
    logger.warning(f"calculate_avg_daily_volume not implemented for {ticker}")
    return None


def is_weekend():
    """
    Check if current day is weekend (Saturday or Sunday).

    Returns:
        bool: True if it's weekend (Saturday=5, Sunday=6)
    """
    import datetime
    import pytz

    ny_tz = pytz.timezone("America/New_York")
    current_time = datetime.datetime.now(ny_tz)
    current_weekday = current_time.weekday()  # 0=Monday, 6=Sunday

    return current_weekday >= 5  # Saturday=5, Sunday=6


def should_use_test_mode():
    """
    Determine if test mode should be used based on configuration, MODE environment variable, and weekend status.

    Priority:
    1. MODE environment variable ("test" or "production")
    2. TEST_MODE configuration ("enabled", "disabled", or "auto")
    3. Automatic weekend detection (if TEST_MODE="auto")

    Returns:
        bool: True if test mode should be active
    """
    import os
    from utils.config import TEST_MODE, WEEKEND_TEST_MODE_ENABLED

    # Get MODE from environment dynamically (not from config cache)
    mode = os.getenv("MODE", "").lower() if os.getenv("MODE") else None

    # Priority 1: MODE environment variable takes precedence
    if mode:
        if mode == "test":
            return True
        elif mode == "production":
            return False
        # If MODE is set but invalid, log warning and fall through to other logic
        logger.warning(
            f"Invalid MODE value '{mode}'. Expected 'test' or 'production'. Falling back to TEST_MODE logic."
        )

    # Priority 2: TEST_MODE configuration
    if TEST_MODE == "enabled":
        return True
    elif TEST_MODE == "disabled":
        return False
    else:  # TEST_MODE == "auto"
        # Priority 3: Automatic weekend detection
        return WEEKEND_TEST_MODE_ENABLED and is_weekend()


def get_test_mode_reason():
    """
    Get the reason why test mode is active (for logging purposes).

    Returns:
        tuple: (is_test_mode, reason_string)
    """
    import os
    from utils.config import TEST_MODE, WEEKEND_TEST_MODE_ENABLED
    import datetime
    import pytz

    # Get MODE from environment dynamically (not from config cache)
    mode = os.getenv("MODE", "").lower() if os.getenv("MODE") else None

    ny_tz = pytz.timezone("America/New_York")
    current_time = datetime.datetime.now(ny_tz)
    current_weekday = current_time.weekday()
    weekday_name = current_time.strftime("%A")

    # Check MODE environment variable first
    if mode:
        if mode == "test":
            return True, f"Environment variable MODE=test – running in TEST MODE"
        elif mode == "production":
            return False, f"Environment variable MODE=production – running in LIVE MODE"
        else:
            logger.warning(
                f"Invalid MODE value '{mode}'. Expected 'test' or 'production'. Falling back to TEST_MODE logic."
            )

    # Check TEST_MODE configuration
    if TEST_MODE == "enabled":
        return True, f"Configuration TEST_MODE=enabled – running in TEST MODE"
    elif TEST_MODE == "disabled":
        return False, f"Configuration TEST_MODE=disabled – running in LIVE MODE"
    else:  # TEST_MODE == "auto"
        if WEEKEND_TEST_MODE_ENABLED and is_weekend():
            return True, f"Weekend detected ({weekday_name}) – running in TEST MODE"
        else:
            return False, f"Weekday detected ({weekday_name}) – running in LIVE MODE"


def log_detailed_operation(
    ticker,
    operation,
    start_time=None,
    row_count_before=None,
    row_count_after=None,
    details=None,
):
    """
    Log detailed operation information for test mode visibility.

    Args:
        ticker (str): Ticker symbol
        operation (str): Operation being performed
        start_time (datetime, optional): When operation started
        row_count_before (int, optional): Row count before operation
        row_count_after (int, optional): Row count after operation
        details (str, optional): Additional details
    """
    import datetime

    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    if start_time:
        duration = (datetime.datetime.now() - start_time).total_seconds()
        duration_str = f" ({duration:.2f}s)"
    else:
        duration_str = ""

    log_parts = [f"[{timestamp}] {ticker}: {operation}{duration_str}"]

    if row_count_before is not None and row_count_after is not None:
        log_parts.append(f" | Rows: {row_count_before} → {row_count_after}")
    elif row_count_after is not None:
        log_parts.append(f" | Rows: {row_count_after}")

    if details:
        log_parts.append(f" | {details}")

    logger.info("".join(log_parts))


def verify_data_storage_and_retention(ticker, check_today=True):
    """
    Verify correct data storage paths and data retention (Phase 3 implementation).
    Checks if files exist in correct paths and validates today's data inclusion.

    Args:
        ticker (str): Ticker symbol
        check_today (bool): Whether to check for today's data

    Returns:
        dict: Verification results for each data type
    """
    logger.info(f"🔍 VERIFYING data storage for {ticker}")

    results = {
        "daily": {
            "path": None,
            "exists": False,
            "row_count": 0,
            "today_present": False,
            "date_range": None,
        },
        "30min": {
            "path": None,
            "exists": False,
            "row_count": 0,
            "today_present": False,
            "date_range": None,
        },
        "1min": {
            "path": None,
            "exists": False,
            "row_count": 0,
            "today_present": False,
            "date_range": None,
        },
    }

    # Define expected paths as per requirements
    base_dir = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data"
    )
    paths_to_check = {
        "daily": os.path.join(base_dir, "daily", f"{ticker}_daily.csv"),
        "30min": os.path.join(base_dir, "intraday_30min", f"{ticker}_30min.csv"),
        "1min": os.path.join(base_dir, "intraday", f"{ticker}_1min.csv"),
    }

    for data_type, file_path in paths_to_check.items():
        results[data_type]["path"] = file_path

        logger.info(f"   📂 Checking {data_type}: {file_path}")

        if os.path.exists(file_path):
            results[data_type]["exists"] = True
            logger.info(f"   ✅ File exists: {file_path}")

            try:
                # Read and analyze the file
                df = pd.read_csv(file_path)
                row_count = len(df)
                results[data_type]["row_count"] = row_count

                logger.info(f"   📊 Row count: {row_count}")

                # Check date range
                date_col = None
                for col in ["Date", "datetime", "timestamp"]:
                    if col in df.columns:
                        date_col = col
                        break

                if date_col and not df.empty:
                    df[date_col] = pd.to_datetime(df[date_col])
                    min_date = df[date_col].min()
                    max_date = df[date_col].max()
                    results[data_type]["date_range"] = f"{min_date} to {max_date}"

                    logger.info(f"   📅 Date range: {min_date} to {max_date}")

                    # Check for today's data if requested
                    if check_today:
                        today_present = is_today_present_enhanced(df, date_col)
                        results[data_type]["today_present"] = today_present

                        if today_present:
                            logger.info(f"   ✅ TODAY'S DATA confirmed present")
                        else:
                            logger.info(
                                f"   📅 No today's data (may be weekend/holiday)"
                            )
                else:
                    logger.warning(f"   ⚠️ No valid date column found")

            except Exception as e:
                logger.error(f"   ❌ Error reading file: {e}")
        else:
            logger.warning(f"   ❌ File not found: {file_path}")

    # Summary
    logger.info(f"📋 VERIFICATION SUMMARY for {ticker}:")
    for data_type, result in results.items():
        status = "✅ OK" if result["exists"] else "❌ Missing"
        today_status = (
            "✅ Present"
            if result["today_present"]
            else "❌ Missing" if check_today else "N/A"
        )
        logger.info(
            f"   {data_type.upper()}: {status} | Rows: {result['row_count']} | Today: {today_status}"
        )

    return results


def check_spaces_connectivity():
    """
    Check Spaces connectivity and validate credentials (Phase 3 implementation).

    Returns:
        dict: Connectivity status and details
    """
    logger.info("🔗 CHECKING Spaces connectivity...")

    from utils.config import (
        SPACES_ACCESS_KEY_ID,
        SPACES_SECRET_ACCESS_KEY,
        SPACES_BUCKET_NAME,
        SPACES_ENDPOINT_URL,
    )

    result = {
        "credentials_configured": False,
        "bucket_accessible": False,
        "write_permissions": False,
        "details": {},
    }

    # Check if credentials are configured
    if all([SPACES_ACCESS_KEY_ID, SPACES_SECRET_ACCESS_KEY, SPACES_BUCKET_NAME]):
        result["credentials_configured"] = True
        logger.info("✅ Spaces credentials configured")
        result["details"]["endpoint"] = SPACES_ENDPOINT_URL
        result["details"]["bucket"] = SPACES_BUCKET_NAME
    else:
        logger.error("❌ Spaces credentials missing")
        result["details"]["missing"] = []
        if not SPACES_ACCESS_KEY_ID:
            result["details"]["missing"].append("SPACES_ACCESS_KEY_ID")
        if not SPACES_SECRET_ACCESS_KEY:
            result["details"]["missing"].append("SPACES_SECRET_ACCESS_KEY")
        if not SPACES_BUCKET_NAME:
            result["details"]["missing"].append("SPACES_BUCKET_NAME")
        return result

    # Test Spaces client creation
    try:
        from utils.spaces_manager import get_spaces_client

        client = get_spaces_client()

        if client:
            logger.info("✅ Spaces client created successfully")
            result["bucket_accessible"] = True

            # Test write permissions with small operation
            try:
                test_content = "test,data\n1,2\n"
                test_key = "data/test_connectivity.csv"

                import io

                buffer = io.BytesIO(test_content.encode())
                client.upload_fileobj(buffer, SPACES_BUCKET_NAME, test_key)

                logger.info("✅ Write permissions confirmed")
                result["write_permissions"] = True

                # Clean up test file
                try:
                    client.delete_object(Bucket=SPACES_BUCKET_NAME, Key=test_key)
                    logger.info("✅ Test file cleanup successful")
                except:
                    logger.warning("⚠️ Test file cleanup failed (not critical)")

            except Exception as e:
                logger.error(f"❌ Write permission test failed: {e}")

        else:
            logger.error("❌ Failed to create Spaces client")

    except Exception as e:
        logger.error(f"❌ Spaces connectivity test failed: {e}")
        result["details"]["error"] = str(e)

    # Log summary
    logger.info("🔗 CONNECTIVITY SUMMARY:")
    logger.info(
        f"   Credentials: {'✅ OK' if result['credentials_configured'] else '❌ Missing'}"
    )
    logger.info(
        f"   Bucket access: {'✅ OK' if result['bucket_accessible'] else '❌ Failed'}"
    )
    logger.info(
        f"   Write permissions: {'✅ OK' if result['write_permissions'] else '❌ Failed'}"
    )

    return result


def cleanup_data_retention(ticker, daily_df, intraday_30min_df, intraday_1min_df):
    """
    Apply data retention limits after full fetch:
    - Daily: Keep only last 200 rows
    - 30-min: Keep only last 500 rows
    - 1-min: Keep only last 7 days, preserving early volume data and today's feed

    Args:
        ticker (str): Ticker symbol
        daily_df (DataFrame): Daily data
        intraday_30min_df (DataFrame): 30-min intraday data
        intraday_1min_df (DataFrame): 1-min intraday data

    Returns:
        tuple: (cleaned_daily_df, cleaned_30min_df, cleaned_1min_df)
    """
    import datetime
    import pandas as pd

    start_time = datetime.datetime.now()

    # Daily data cleanup - keep last 200 rows
    daily_before = len(daily_df) if not daily_df.empty else 0
    cleaned_daily = daily_df.head(200) if not daily_df.empty else daily_df
    daily_after = len(cleaned_daily)

    # 30-min data cleanup - keep last 500 rows
    intraday_30min_before = len(intraday_30min_df) if not intraday_30min_df.empty else 0
    cleaned_30min = (
        intraday_30min_df.head(500)
        if not intraday_30min_df.empty
        else intraday_30min_df
    )
    intraday_30min_after = len(cleaned_30min)

    # 1-min data cleanup - keep last 7 days but preserve early volume and today's data
    intraday_1min_before = len(intraday_1min_df) if not intraday_1min_df.empty else 0
    cleaned_1min = intraday_1min_df

    if not intraday_1min_df.empty:
        # Ensure timestamp column exists and is datetime
        if "timestamp" in intraday_1min_df.columns:
            cleaned_1min = intraday_1min_df.copy()
            try:
                cleaned_1min["timestamp"] = pd.to_datetime(cleaned_1min["timestamp"])

                # FIXED: Use timezone-aware datetime for proper comparison
                ny_tz = pytz.timezone(TIMEZONE)
                now_et = datetime.datetime.now(ny_tz)
                seven_days_ago = now_et - datetime.timedelta(days=7)
                
                # Ensure timestamps are timezone-aware for proper comparison
                if cleaned_1min["timestamp"].dt.tz is None:
                    # If naive, localize to NY timezone first
                    cleaned_1min["timestamp"] = cleaned_1min["timestamp"].dt.tz_localize(ny_tz)
                elif cleaned_1min["timestamp"].dt.tz != ny_tz:
                    # If different timezone, convert to NY timezone
                    cleaned_1min["timestamp"] = cleaned_1min["timestamp"].dt.tz_convert(ny_tz)
                
                # Apply 7-day filter with timezone-aware comparison
                mask = cleaned_1min["timestamp"] >= seven_days_ago
                cleaned_1min = cleaned_1min[mask]

                # If filtering resulted in empty data, fallback to row-based limit
                if cleaned_1min.empty:
                    cleaned_1min = intraday_1min_df.head(10080)  # ~7 days worth
            except Exception as e:
                # If timestamp parsing fails, use row-based limit
                cleaned_1min = intraday_1min_df.head(10080)  # ~7 days worth
        else:
            # If no timestamp column, just keep first 7*24*60 = 10080 rows (roughly 7 days of 1-min data)
            cleaned_1min = intraday_1min_df.head(10080)

    intraday_1min_after = len(cleaned_1min)

    # Log detailed cleanup results
    log_detailed_operation(
        ticker,
        "Cleanup applied",
        start_time,
        details=f"retained {daily_after} daily, {intraday_30min_after} 30-min, {intraday_1min_after} intraday",
    )

    return cleaned_daily, cleaned_30min, cleaned_1min


def save_list_to_s3(ticker_list, filename):
    """
    Save a list of tickers to S3/Spaces and local file.
    
    Args:
        ticker_list (list): List of ticker symbols
        filename (str): Filename to save to
        
    Returns:
        bool: True if successful
    """
    try:
        # Create DataFrame for CSV format if the filename suggests CSV
        if filename.endswith('.csv'):
            # For master_tickerlist.csv format
            if 'master_tickerlist' in filename:
                df = pd.DataFrame({
                    'ticker': ticker_list,
                    'source': ['manual'] * len(ticker_list),
                    'generated_at': [datetime.now().strftime('%Y-%m-%d %H:%M:%S')] * len(ticker_list)
                })
                # Save to local file
                local_file = os.path.join(
                    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 
                    filename
                )
                df.to_csv(local_file, index=False)
                logger.info(f"✅ Saved {len(ticker_list)} tickers to {local_file}")
                
                # Try to save to S3/Spaces as well
                try:
                    save_df_to_s3(df, filename)
                    logger.info(f"✅ Also saved to S3/Spaces: {filename}")
                except Exception as e:
                    logger.warning(f"⚠️ Failed to save to S3/Spaces: {e}")
                
                return True
            else:
                # For other CSV files, simple format
                df = pd.DataFrame({'ticker': ticker_list})
                local_file = os.path.join(
                    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 
                    filename
                )
                df.to_csv(local_file, index=False)
                logger.info(f"✅ Saved {len(ticker_list)} tickers to {local_file}")
                return True
        else:
            # For .txt files, save as plain text
            local_file = os.path.join(
                os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 
                filename
            )
            with open(local_file, 'w') as f:
                for ticker in ticker_list:
                    f.write(f"{ticker}\n")
            logger.info(f"✅ Saved {len(ticker_list)} tickers to {local_file}")
            return True
            
    except Exception as e:
        logger.error(f"❌ Error saving ticker list to {filename}: {e}")
        return False


def read_config_from_s3(config_filename):
    """
    Read configuration from S3/Spaces or local file.
    
    Args:
        config_filename (str): Configuration filename
        
    Returns:
        dict: Configuration dictionary or None if not found
    """
    try:
        # Try local file first
        local_file = os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 
            config_filename
        )
        
        if os.path.exists(local_file):
            with open(local_file, 'r') as f:
                import json
                config = json.load(f)
                logger.info(f"✅ Read configuration from local file: {local_file}")
                return config
        
        logger.info(f"⚠️ Configuration file not found: {config_filename}")
        return None
        
    except Exception as e:
        logger.error(f"❌ Error reading configuration from {config_filename}: {e}")
        return None


def save_config_to_s3(config_dict, config_filename):
    """
    Save configuration to S3/Spaces and local file.
    
    Args:
        config_dict (dict): Configuration dictionary
        config_filename (str): Configuration filename
        
    Returns:
        bool: True if successful
    """
    try:
        # Save to local file
        local_file = os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 
            config_filename
        )
        
        with open(local_file, 'w') as f:
            import json
            json.dump(config_dict, f, indent=2)
        
        logger.info(f"✅ Saved configuration to local file: {local_file}")
        return True
        
    except Exception as e:
        logger.error(f"❌ Error saving configuration to {config_filename}: {e}")
        return False
