import os

# API Keys
ALPHA_VANTAGE_API_KEY = os.getenv("ALPHA_VANTAGE_API_KEY")

# DigitalOcean Spaces Configuration
SPACES_ACCESS_KEY_ID = os.getenv("SPACES_ACCESS_KEY_ID")
SPACES_SECRET_ACCESS_KEY = os.getenv("SPACES_SECRET_ACCESS_KEY")
SPACES_BUCKET_NAME = os.getenv("SPACES_BUCKET_NAME")
SPACES_ENDPOINT_URL = f"https://{SPACES_BUCKET_NAME}.nyc3.digitaloceanspaces.com" if SPACES_BUCKET_NAME else None

# File Paths
BASE_DATA_DIR = "/workspace/data"
INTRADAY_DATA_DIR = f"{BASE_DATA_DIR}/intraday"
DAILY_DATA_DIR = f"{BASE_DATA_DIR}/daily"

# Ensure directories exist
os.makedirs(INTRADAY_DATA_DIR, exist_ok=True)
os.makedirs(DAILY_DATA_DIR, exist_ok=True)

# Debug mode
DEBUG_MODE = os.getenv("DEBUG_MODE", "false").lower() == "true"

# Stock Tickers
DEFAULT_TICKERS = [
    "AAPL", "MSFT", "AMZN", "GOOGL", "META", 
    "TSLA", "NVDA", "AMD", "INTC", "IBM"
]

# Time Intervals
INTRADAY_INTERVALS = ["1min", "30min"]
