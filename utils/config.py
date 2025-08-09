import os

# API Keys
ALPHA_VANTAGE_API_KEY = os.getenv("ALPHA_VANTAGE_API_KEY")

# DigitalOcean Spaces Configuration
SPACES_ACCESS_KEY_ID = os.getenv("SPACES_ACCESS_KEY_ID")
SPACES_SECRET_ACCESS_KEY = os.getenv("SPACES_SECRET_ACCESS_KEY")
SPACES_BUCKET_NAME = os.getenv("SPACES_BUCKET_NAME")
SPACES_REGION = os.getenv("SPACES_REGION", "nyc3")
SPACES_ENDPOINT_URL = f"https://{SPACES_BUCKET_NAME}.{SPACES_REGION}.digitaloceanspaces.com" if SPACES_BUCKET_NAME else None

# Phase 1: Environment Variable Setup - Path Structure Variables
SPACES_BASE_PREFIX = os.getenv("SPACES_BASE_PREFIX", "data")
SPACES_STRUCTURE_VERSION = os.getenv("SPACES_STRUCTURE_VERSION", "v2")

# Phase 1: Data Retention Configuration - KEEP TODAY'S DATA
INTRADAY_TRIM_DAYS = int(os.getenv("INTRADAY_TRIM_DAYS", "7"))
INTRADAY_EXCLUDE_TODAY = os.getenv("INTRADAY_EXCLUDE_TODAY", "false").lower() == "true"
INTRADAY_INCLUDE_PREMARKET = os.getenv("INTRADAY_INCLUDE_PREMARKET", "true").lower() == "true"
INTRADAY_INCLUDE_AFTERHOURS = os.getenv("INTRADAY_INCLUDE_AFTERHOURS", "true").lower() == "true"
TIMEZONE = os.getenv("TIMEZONE", "America/New_York")

# Phase 1: Processing Controls
PROCESS_MANUAL_TICKERS = os.getenv("PROCESS_MANUAL_TICKERS", "true").lower() == "true"
MAX_TICKERS_PER_RUN = int(os.getenv("MAX_TICKERS_PER_RUN", "25"))
INTRADAY_BATCH_SIZE = int(os.getenv("INTRADAY_BATCH_SIZE", "25"))
MARKET_HOURS_ONLY = os.getenv("MARKET_HOURS_ONLY", "false").lower() == "true"
SKIP_IF_FRESH_MINUTES = int(os.getenv("SKIP_IF_FRESH_MINUTES", "0"))
DEBUG_MODE = os.getenv("DEBUG_MODE", "true").lower() == "true"  # Default to true for debugging

# Weekend Test Mode Configuration
TEST_MODE = os.getenv("TEST_MODE", "auto").lower()  # auto, enabled, disabled
WEEKEND_TEST_MODE_ENABLED = os.getenv("WEEKEND_TEST_MODE_ENABLED", "true").lower() == "true"

# Master Orchestrator Mode Configuration
# MODE environment variable takes priority over automatic weekend detection
# Valid values: "test", "production" (case insensitive)
# If not set, falls back to TEST_MODE and weekend detection logic
MODE = os.getenv("MODE", "").lower() if os.getenv("MODE") else None

# File Paths
BASE_DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data")
INTRADAY_DATA_DIR = f"{BASE_DATA_DIR}/intraday"
INTRADAY_30MIN_DATA_DIR = f"{BASE_DATA_DIR}/intraday_30min"
DAILY_DATA_DIR = f"{BASE_DATA_DIR}/daily"

# Ensure directories exist
os.makedirs(INTRADAY_DATA_DIR, exist_ok=True)
os.makedirs(INTRADAY_30MIN_DATA_DIR, exist_ok=True)
os.makedirs(DAILY_DATA_DIR, exist_ok=True)

# Stock Tickers
DEFAULT_TICKERS = [
    "AAPL", "MSFT", "AMZN", "GOOGL", "META", 
    "TSLA", "NVDA", "AMD", "INTC", "IBM"
]

# Time Intervals
INTRADAY_INTERVALS = ["1min", "30min"]
