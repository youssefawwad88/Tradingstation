# /drive/MyDrive/trading-system/utils/config.py
import os
from pathlib import Path
from datetime import time

# --- Project Root ---
PROJECT_ROOT = Path('/content/drive/MyDrive/trading-system')

# --- API Keys ---
ALPHA_VANTAGE_API_KEY = os.getenv('ALPHA_VANTAGE_API_KEY', 'YOUR_DEFAULT_KEY_HERE')
NEWS_API_KEY = os.getenv('NEWS_API_KEY', 'YOUR_NEWS_API_KEY_HERE')
FMP_API_KEY = os.getenv('FMP_API_KEY', 'YOUR_FMP_API_KEY_HERE')
ALPHA_VANTAGE_BASE_URL = 'https://www.alphavantage.co/query'


# --- Data Storage Paths ---
DATA_DIR = PROJECT_ROOT / 'data'
INTRADAY_1MIN_DIR = DATA_DIR / 'intraday'
INTRADAY_30MIN_DIR = DATA_DIR / 'intraday_30min'
DAILY_DIR = DATA_DIR / 'daily'
SIGNALS_DIR = DATA_DIR / 'signals'
JOBS_DIR = PROJECT_ROOT / 'jobs'

# --- Ticker List File ---
TICKER_LIST_FILE = PROJECT_ROOT / 'tickerlist.txt'

def get_tickers_from_file(filepath: Path) -> list[str]:
    """Reads tickers from a text file, one per line."""
    if not filepath.exists():
        print(f"⚠️ WARNING: Ticker file not found at {filepath}. Using empty list.")
        return []
    
    with open(filepath, 'r') as f:
        tickers = [line.strip().upper() for line in f if line.strip()]
    
    print(f"✅ Loaded {len(tickers)} tickers from {filepath.name}")
    return tickers

# --- Master Ticker List ---
MASTER_TICKER_LIST = get_tickers_from_file(TICKER_LIST_FILE)

# --- Data Retention Policies (For Initial Fetch) ---
# Defines the maximum number of rows to keep when creating a new data file.
DAILY_DATA_MAX_ROWS = 200
INTRADAY_30MIN_MAX_ROWS = 500
INTRADAY_1MIN_MAX_ROWS = 390 * 7 # 390 minutes in a trading day * 7 days = ~1 week of data

# --- Market Session Times (in NY Timezone - ET) ---
PREMARKET_START = time(4, 0)
REGULAR_SESSION_START = time(9, 30)
REGULAR_SESSION_END = time(16, 0)
AFTER_HOURS_END = time(20, 0)

# --- Selector Configuration ---
TOP_GAINERS_COUNT = 20
EARNINGS_DAYS_AHEAD = 7

# --- Screener-Specific Thresholds ---
GAP_UP_THRESHOLD_PCT = 2.0
PREMARKET_VOLUME_THRESHOLD = 50000
OPENING_MINUTE_VOLUME_SPIKE_FACTOR = 5
ORB_DURATION_MINUTES = 5
AVWAP_ANCHOR_DAYS = [5, 20, 50]
EMA_FAST_PERIOD = 9
EMA_SLOW_PERIOD = 21

# --- Standardized DataFrame Column Names ---
DATE_COL = 'Date'
OPEN_COL = 'Open'
HIGH_COL = 'High'
LOW_COL = 'Low'
CLOSE_COL = 'Close'
VOLUME_COL = 'Volume'

# --- Output File Names ---
TRADE_SIGNALS_FILE = SIGNALS_DIR / 'trade_signals.csv'
EXECUTION_JOURNAL_FILE = PROJECT_ROOT / 'execution_journal.csv'

# --- Automation to create folders ---
def setup_directories():
    """Creates all necessary data directories if they don't already exist."""
    print("Verifying project directories...")
    for path in [DATA_DIR, INTRADAY_1MIN_DIR, INTRADAY_30MIN_DIR, DAILY_DIR, SIGNALS_DIR, JOBS_DIR]:
        path.mkdir(parents=True, exist_ok=True)
    print("All directories are present in Google Drive.")

setup_directories()
