# utils/config.py
from __future__ import annotations
import os
from urllib.parse import urlparse

# ---- Constants (no magic strings in code) ----
DEFAULT_SPACES_REGION = os.getenv("SPACES_REGION_DEFAULT", "nyc3")
DEFAULT_SPACES_ENDPOINT = f"https://{DEFAULT_SPACES_REGION}.digitaloceanspaces.com"

def _ensure_scheme(host_or_url: str) -> str:
    if not host_or_url:
        return DEFAULT_SPACES_ENDPOINT
    if host_or_url.startswith("http://") or host_or_url.startswith("https://"):
        return host_or_url
    return "https://" + host_or_url

def normalize_spaces_endpoint(raw_endpoint: str) -> str:
    """
    Accepts any of:
      - nyc3.digitaloceanspaces.com
      - https://nyc3.digitaloceanspaces.com
      - trading-station-data-youssef.nyc3.digitaloceanspaces.com
      - https://trading-station-data-youssef.nyc3.digitaloceanspaces.com
    Returns canonical region endpoint: https://<region>.digitaloceanspaces.com
    """
    ep = _ensure_scheme(raw_endpoint)
    parsed = urlparse(ep)
    host = (parsed.hostname or "").lower()

    if not host.endswith(".digitaloceanspaces.com"):
        # Fallback to default if someone fed us garbage
        return DEFAULT_SPACES_ENDPOINT

    parts = host.split(".")
    # bucket-hosted = my-bucket.nyc3.digitaloceanspaces.com
    # region-only   = nyc3.digitaloceanspaces.com
    if len(parts) >= 4 and parts[-3].isdigit() is False:  # heuristic: bucket-hosted has 4+ segments
        region = parts[-3]  # nyc3
    else:
        region = parts[0]   # nyc3

    return f"https://{region}.digitaloceanspaces.com"

def normalize_prefix(prefix: str | None) -> str:
    p = (prefix or "").strip()
    if not p:
        return "data/"
    return p if p.endswith("/") else p + "/"

def derive_origin_url(bucket: str, endpoint_url: str) -> str:
    host = urlparse(_ensure_scheme(endpoint_url)).hostname or ""
    # host is like nyc3.digitaloceanspaces.com
    return f"https://{bucket}.{host}/"

# Load environment variables from .env file if it exists
try:
    from dotenv import load_dotenv
    from pathlib import Path

    env_path = Path(__file__).resolve().parent.parent / ".env"
    if env_path.exists():
        load_dotenv(env_path)
except ImportError:
    # python-dotenv not installed, continue with system environment variables
    pass


class Config:
    """Central configuration class for all trading system settings."""

    # === API Keys ===
    MARKETDATA_TOKEN: Optional[str] = os.getenv("MARKETDATA_TOKEN")

    # === DigitalOcean Spaces Configuration ===
    SPACES_ACCESS_KEY_ID: Optional[str] = os.getenv("SPACES_ACCESS_KEY_ID")
    SPACES_SECRET_ACCESS_KEY: Optional[str] = os.getenv("SPACES_SECRET_ACCESS_KEY")
    
    SPACES_ENDPOINT_RAW = os.getenv("SPACES_ENDPOINT", "")
    SPACES_ENDPOINT = normalize_spaces_endpoint(SPACES_ENDPOINT_RAW)

    SPACES_BUCKET_NAME = os.getenv("SPACES_BUCKET_NAME", "").strip()
    SPACES_BASE_PREFIX = normalize_prefix(os.getenv("SPACES_BASE_PREFIX", "data/"))

    DATA_ROOT = os.getenv("DATA_ROOT", "data")
    UNIVERSE_KEY = os.getenv("UNIVERSE_KEY", "data/universe/master_tickerlist.csv")

    # Boolean/int envs (single-line style per review)
    INTRADAY_EXTENDED: bool = os.getenv("INTRADAY_EXTENDED", "false").lower() == "true"
    DEGRADE_INTRADAY_ON_STALE_MINUTES: int = int(os.getenv("DEGRADE_INTRADAY_ON_STALE_MINUTES", "5"))
    PROVIDER_DEGRADED_ALLOWED: bool = os.getenv("PROVIDER_DEGRADED_ALLOWED", "true").lower() == "true"

    # Derived, read-only
    SPACES_ORIGIN_URL = derive_origin_url(SPACES_BUCKET_NAME, SPACES_ENDPOINT)
    SPACES_REGION: str = urlparse(SPACES_ENDPOINT).hostname.split('.')[0] if urlparse(SPACES_ENDPOINT).hostname else DEFAULT_SPACES_REGION

    # === DigitalOcean App Configuration ===
    DO_APP_ID: Optional[str] = os.getenv("DO_APP_ID")

    # === Application Environment ===
    APP_ENV: str = os.getenv("APP_ENV", "development")
    DEPLOYMENT_TAG: Optional[str] = os.getenv("DEPLOYMENT_TAG")

    # === Feature Flags ===
    FETCH_EXTENDED_HOURS: bool = os.getenv("FETCH_EXTENDED_HOURS", "true").lower() == "true"
    TEST_MODE_INIT_ALLOWED: bool = os.getenv("TEST_MODE_INIT_ALLOWED", "true").lower() == "true"
    DEBUG_MODE: bool = os.getenv("DEBUG_MODE", "false").lower() == "true"

    # === Fallback Configuration ===
    FALLBACK_TICKERS: list[str] = ["NVDA", "AAPL", "TSLA", "MSFT", "GOOGL"]
    MASTER_TICKERLIST_PATH: tuple[str, ...] = (
        "data", "universe", "master_tickerlist.csv"
    )

    # === Data Retention Windows ===
    INTRADAY_1MIN_RETENTION_DAYS: int = int(os.getenv("INTRADAY_1MIN_RETENTION_DAYS", "7"))
    INTRADAY_30MIN_RETENTION_ROWS: int = int(os.getenv("INTRADAY_30MIN_RETENTION_ROWS", "500"))
    DAILY_RETENTION_ROWS: int = int(os.getenv("DAILY_RETENTION_ROWS", "200"))

    # === Data Fetch Constants ===
    ONE_MIN_REQUIRED_DAYS: int = 7  # 7 days + today for 1min data retention

    # === Processing Controls ===
    MAX_TICKERS_PER_RUN: int = int(os.getenv("MAX_TICKERS_PER_RUN", "25"))
    API_RATE_LIMIT_CALLS_PER_MINUTE: int = int(os.getenv("API_RATE_LIMIT_CALLS_PER_MINUTE", "150"))

    # === Market Schedule ===
    TIMEZONE: str = os.getenv("TIMEZONE", "America/New_York")
    MARKET_OPEN_HOUR: int = 9
    MARKET_OPEN_MINUTE: int = 30
    MARKET_CLOSE_HOUR: int = 16
    MARKET_CLOSE_MINUTE: int = 0

    # === Gap & Go Strategy Configuration ===
    MIN_GAP_LONG_PCT: float = float(os.getenv("MIN_GAP_LONG_PCT", "2.0"))
    MIN_GAP_SHORT_PCT: float = float(os.getenv("MIN_GAP_SHORT_PCT", "-2.0"))
    VOLUME_SPIKE_THRESHOLD: float = float(os.getenv("VOLUME_SPIKE_THRESHOLD", "1.15"))
    BREAKOUT_TIME_GUARD_MINUTES: int = int(os.getenv("BREAKOUT_TIME_GUARD_MINUTES", "6"))  # 09:36

    # === Risk Management ===
    ACCOUNT_SIZE: float = float(os.getenv("ACCOUNT_SIZE", "100000"))
    MAX_RISK_PER_TRADE_PCT: float = float(os.getenv("MAX_RISK_PER_TRADE_PCT", "2.0"))
    MAX_DAILY_RISK_PCT: float = float(os.getenv("MAX_DAILY_RISK_PCT", "6.0"))
    DEFAULT_POSITION_SIZE_SHARES: int = int(os.getenv("DEFAULT_POSITION_SIZE_SHARES", "100"))

    # === File Size Thresholds ===
    MIN_FILE_SIZE_BYTES: int = int(os.getenv("MIN_FILE_SIZE_BYTES", "10240"))  # 10KB

    @classmethod
    def get_spaces_path(cls, *path_parts: str) -> str:
        """Get a complete Spaces object path."""
        return f"{cls.SPACES_BASE_PREFIX}/" + "/".join(path_parts)

    # === Fallback Configuration ===
    FALLBACK_TICKERS: list[str] = ["NVDA", "AAPL", "TSLA", "MSFT", "GOOGL"]
    MASTER_TICKERLIST_PATH: tuple[str, ...] = (
        "data", "universe", "master_tickerlist.csv"
    )

    @classmethod
    def get_spaces_origin_url(cls) -> str:
        """Derive the bucket-hosted origin URL for Spaces.
        
        Returns:
            Bucket origin URL: https://bucket-name.region.digitaloceanspaces.com/
        """
        return derive_origin_url(cls.SPACES_BUCKET_NAME or "", cls.SPACES_ENDPOINT)

    @classmethod
    def get_spaces_path(cls, *path_parts: str) -> str:
        """Get a complete Spaces object path."""
        return f"{cls.SPACES_BASE_PREFIX}/" + "/".join(path_parts)

    # === Data Retention Windows ===
    INTRADAY_1MIN_RETENTION_DAYS: int = int(os.getenv("INTRADAY_1MIN_RETENTION_DAYS", "7"))
    INTRADAY_30MIN_RETENTION_ROWS: int = int(os.getenv("INTRADAY_30MIN_RETENTION_ROWS", "500"))
    DAILY_RETENTION_ROWS: int = int(os.getenv("DAILY_RETENTION_ROWS", "200"))

    # === Data Fetch Constants ===
    ONE_MIN_REQUIRED_DAYS: int = 7  # 7 days + today for 1min data retention

    # === Processing Controls ===
    MAX_TICKERS_PER_RUN: int = int(os.getenv("MAX_TICKERS_PER_RUN", "25"))
    API_RATE_LIMIT_CALLS_PER_MINUTE: int = int(os.getenv("API_RATE_LIMIT_CALLS_PER_MINUTE", "150"))

    # === Market Schedule ===
    TIMEZONE: str = os.getenv("TIMEZONE", "America/New_York")
    MARKET_OPEN_HOUR: int = 9
    MARKET_OPEN_MINUTE: int = 30
    MARKET_CLOSE_HOUR: int = 16
    MARKET_CLOSE_MINUTE: int = 0

    # === Gap & Go Strategy Configuration ===
    MIN_GAP_LONG_PCT: float = float(os.getenv("MIN_GAP_LONG_PCT", "2.0"))
    MIN_GAP_SHORT_PCT: float = float(os.getenv("MIN_GAP_SHORT_PCT", "-2.0"))
    VOLUME_SPIKE_THRESHOLD: float = float(os.getenv("VOLUME_SPIKE_THRESHOLD", "1.15"))
    BREAKOUT_TIME_GUARD_MINUTES: int = int(os.getenv("BREAKOUT_TIME_GUARD_MINUTES", "6"))  # 09:36

    # === Risk Management ===
    ACCOUNT_SIZE: float = float(os.getenv("ACCOUNT_SIZE", "100000"))
    MAX_RISK_PER_TRADE_PCT: float = float(os.getenv("MAX_RISK_PER_TRADE_PCT", "2.0"))
    MAX_DAILY_RISK_PCT: float = float(os.getenv("MAX_DAILY_RISK_PCT", "6.0"))
    DEFAULT_POSITION_SIZE_SHARES: int = int(os.getenv("DEFAULT_POSITION_SIZE_SHARES", "100"))

    # === File Size Thresholds ===
    MIN_FILE_SIZE_BYTES: int = int(os.getenv("MIN_FILE_SIZE_BYTES", "10240"))  # 10KB

    @classmethod
    def validate_configuration(cls) -> tuple[bool, list[str]]:
        """Validate that all required configuration is present.
        
        Returns:
            tuple: (is_valid, list_of_errors)
        """
        errors = []

        # Check required API keys for production
        if cls.APP_ENV == "production":
            if not cls.MARKETDATA_TOKEN:
                errors.append("MARKETDATA_TOKEN is required in production")
            if not cls.SPACES_ACCESS_KEY_ID:
                errors.append("SPACES_ACCESS_KEY_ID is required in production")
            if not cls.SPACES_SECRET_ACCESS_KEY:
                errors.append("SPACES_SECRET_ACCESS_KEY is required in production")
            if not cls.SPACES_BUCKET_NAME:
                errors.append("SPACES_BUCKET_NAME is required in production")

        # Validate numeric ranges
        if cls.MAX_RISK_PER_TRADE_PCT <= 0 or cls.MAX_RISK_PER_TRADE_PCT > 10:
            errors.append("MAX_RISK_PER_TRADE_PCT must be between 0 and 10")

        if cls.ACCOUNT_SIZE <= 0:
            errors.append("ACCOUNT_SIZE must be positive")

        return len(errors) == 0, errors

    @classmethod
    def get_credentials_status(cls) -> dict[str, bool]:
        """Get the status of all credentials."""
        return {
            "marketdata": bool(cls.MARKETDATA_TOKEN),
            "spaces_access_key": bool(cls.SPACES_ACCESS_KEY_ID),
            "spaces_secret_key": bool(cls.SPACES_SECRET_ACCESS_KEY),
            "spaces_bucket": bool(cls.SPACES_BUCKET_NAME),
        }

    @classmethod
    def is_test_mode(cls) -> bool:
        """Determine if the system should run in test mode."""
        # Check explicit test mode override
        test_mode = os.getenv("TEST_MODE", "auto").lower()
        if test_mode == "enabled":
            return True
        elif test_mode == "disabled":
            return False

        # Auto mode: check if we have credentials and it's a weekday during market hours
        import datetime

        import pytz

        ny_tz = pytz.timezone(cls.TIMEZONE)
        current_time = datetime.datetime.now(ny_tz)
        is_weekend = current_time.weekday() >= 5

        # If no API key, always test mode
        if not cls.MARKETDATA_TOKEN:
            return True

        # If weekend and test mode init allowed, use test mode
        if is_weekend and cls.TEST_MODE_INIT_ALLOWED:
            return True

        return False


# Global config instance
config = Config()


def get_deployment_info() -> str:
    """Get deployment information for logging."""
    tag = config.DEPLOYMENT_TAG or "unknown"
    env = config.APP_ENV
    return f"Deployment: {tag} | Environment: {env}"


# Backward compatibility exports
MARKETDATA_TOKEN = config.MARKETDATA_TOKEN
SPACES_ACCESS_KEY_ID = config.SPACES_ACCESS_KEY_ID
SPACES_SECRET_ACCESS_KEY = config.SPACES_SECRET_ACCESS_KEY
SPACES_BUCKET_NAME = config.SPACES_BUCKET_NAME
SPACES_REGION = config.SPACES_REGION
SPACES_ENDPOINT = config.SPACES_ENDPOINT
SPACES_ORIGIN_URL = config.get_spaces_origin_url()
DEBUG_MODE = config.DEBUG_MODE

# Logging line for canonical paths (used anywhere we print canonical paths)
def print_paths_resolved():
    """Print canonical paths resolution summary."""
    print(
        "paths_resolved "
        f"base=trading-system data_root={config.DATA_ROOT} universe_key={config.UNIVERSE_KEY} "
        "orchestrator=orchestrator/run_all.py "
        f"endpoint={config.SPACES_ENDPOINT} bucket={config.SPACES_BUCKET_NAME} prefix={config.SPACES_BASE_PREFIX} "
        f"origin_url={config.SPACES_ORIGIN_URL} python_version=3.11.9"
    )
