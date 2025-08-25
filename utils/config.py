# utils/config.py
from __future__ import annotations
import os
from urllib.parse import urlparse

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

DEFAULT_SPACES_ENDPOINT = "https://nyc3.digitaloceanspaces.com"
DO_HOST_SUFFIX = ".digitaloceanspaces.com"


def _normalize_spaces_endpoint(raw_endpoint: str | None) -> str:
    if not raw_endpoint:
        return DEFAULT_SPACES_ENDPOINT
    ep = raw_endpoint.strip().lower()
    if not ep:
        return DEFAULT_SPACES_ENDPOINT
    if not ep.startswith("http://") and not ep.startswith("https://"):
        ep = "https://" + ep
    from urllib.parse import urlparse
    p = urlparse(ep)
    host = (p.hostname or "").lower()
    
    # Handle bare region names (like 'nyc3')
    if host and not host.endswith(DO_HOST_SUFFIX):
        if "." not in host:
            host = f"{host}.digitaloceanspaces.com"
    
    # If someone passed a bucket-hosted endpoint, strip the bucket to region host.
    if host.endswith(DO_HOST_SUFFIX):
        parts = host.split(".")
        # e.g. ["trading-station-data-youssef","nyc3","digitaloceanspaces","com"]
        if len(parts) >= 4:
            # region host is the last 3 parts joined ("nyc3.digitaloceanspaces.com")
            host = ".".join(parts[-3:])
    return f"https://{host}"


class Config:
    # ---- canonical paths ----
    DATA_ROOT: str = os.getenv("DATA_ROOT", "data")
    SPACES_BASE_PREFIX: str = os.getenv("SPACES_BASE_PREFIX", "data")
    # ensure trailing slash on base prefix
    if SPACES_BASE_PREFIX and not SPACES_BASE_PREFIX.endswith("/"):
        SPACES_BASE_PREFIX += "/"

    UNIVERSE_KEY: str = os.getenv(
        "UNIVERSE_KEY",
        "data/universe/master_tickerlist.csv",
    )

    # ---- de-duplicated constants ----
    FALLBACK_TICKERS: list[str] = ["NVDA", "AAPL", "TSLA", "MSFT", "GOOGL"]
    MASTER_TICKERLIST_PATH: tuple[str, ...] = ("data", "universe", "master_tickerlist.csv")

    # ---- simple booleans in single lines (nit acceptance) ----
    INTRADAY_EXTENDED: bool = os.getenv("INTRADAY_EXTENDED", "false").lower() == "true"
    DEGRADE_INTRADAY_ON_STALE_MINUTES: int = int(os.getenv("DEGRADE_INTRADAY_ON_STALE_MINUTES", "5"))
    PROVIDER_DEGRADED_ALLOWED: bool = os.getenv("PROVIDER_DEGRADED_ALLOWED", "true").lower() == "true"

    # ---- Spaces configuration ----
    SPACES_ENDPOINT_RAW: str = os.getenv("SPACES_ENDPOINT", "")
    SPACES_ENDPOINT: str = _normalize_spaces_endpoint(SPACES_ENDPOINT_RAW)
    SPACES_BUCKET_NAME: str = os.getenv("SPACES_BUCKET_NAME", "")
    
    # derived public origin for dashboards
    _host = urlparse(SPACES_ENDPOINT).hostname or DO_HOST_SUFFIX
    SPACES_ORIGIN_URL: str = f"https://{SPACES_BUCKET_NAME}.{_host}/" if SPACES_BUCKET_NAME else ""

    # ---- API Keys ----
    MARKETDATA_TOKEN: str = os.getenv("MARKETDATA_TOKEN", "")
    SPACES_ACCESS_KEY_ID: str = os.getenv("SPACES_ACCESS_KEY_ID", "")
    SPACES_SECRET_ACCESS_KEY: str = os.getenv("SPACES_SECRET_ACCESS_KEY", "")
    
    # ---- DigitalOcean App Configuration ----
    DO_APP_ID: str = os.getenv("DO_APP_ID", "")

    # ---- Application Environment ----
    APP_ENV: str = os.getenv("APP_ENV", "development")
    DEPLOYMENT_TAG: str = os.getenv("DEPLOYMENT_TAG", "")

    # ---- Feature Flags ----
    FETCH_EXTENDED_HOURS: bool = os.getenv("FETCH_EXTENDED_HOURS", "true").lower() == "true"
    TEST_MODE_INIT_ALLOWED: bool = os.getenv("TEST_MODE_INIT_ALLOWED", "true").lower() == "true"
    DEBUG_MODE: bool = os.getenv("DEBUG_MODE", "false").lower() == "true"

    # ---- Data Retention Windows ----
    INTRADAY_1MIN_RETENTION_DAYS: int = int(os.getenv("INTRADAY_1MIN_RETENTION_DAYS", "7"))
    INTRADAY_30MIN_RETENTION_ROWS: int = int(os.getenv("INTRADAY_30MIN_RETENTION_ROWS", "500"))
    DAILY_RETENTION_ROWS: int = int(os.getenv("DAILY_RETENTION_ROWS", "200"))

    # ---- Data Fetch Constants ----
    ONE_MIN_REQUIRED_DAYS: int = 7  # 7 days + today for 1min data retention

    # ---- Processing Controls ----
    MAX_TICKERS_PER_RUN: int = int(os.getenv("MAX_TICKERS_PER_RUN", "25"))
    API_RATE_LIMIT_CALLS_PER_MINUTE: int = int(os.getenv("API_RATE_LIMIT_CALLS_PER_MINUTE", "150"))

    # ---- Market Schedule ----
    TIMEZONE: str = os.getenv("TIMEZONE", "America/New_York")
    MARKET_OPEN_HOUR: int = 9
    MARKET_OPEN_MINUTE: int = 30
    MARKET_CLOSE_HOUR: int = 16
    MARKET_CLOSE_MINUTE: int = 0

    # ---- Gap & Go Strategy Configuration ----
    MIN_GAP_LONG_PCT: float = float(os.getenv("MIN_GAP_LONG_PCT", "2.0"))
    MIN_GAP_SHORT_PCT: float = float(os.getenv("MIN_GAP_SHORT_PCT", "-2.0"))
    VOLUME_SPIKE_THRESHOLD: float = float(os.getenv("VOLUME_SPIKE_THRESHOLD", "1.15"))
    BREAKOUT_TIME_GUARD_MINUTES: int = int(os.getenv("BREAKOUT_TIME_GUARD_MINUTES", "6"))  # 09:36

    # ---- Risk Management ----
    ACCOUNT_SIZE: float = float(os.getenv("ACCOUNT_SIZE", "100000"))
    MAX_RISK_PER_TRADE_PCT: float = float(os.getenv("MAX_RISK_PER_TRADE_PCT", "2.0"))
    MAX_DAILY_RISK_PCT: float = float(os.getenv("MAX_DAILY_RISK_PCT", "6.0"))
    DEFAULT_POSITION_SIZE_SHARES: int = int(os.getenv("DEFAULT_POSITION_SIZE_SHARES", "100"))

    # ---- File Size Thresholds ----
    MIN_FILE_SIZE_BYTES: int = int(os.getenv("MIN_FILE_SIZE_BYTES", "10240"))  # 10KB

    # ---- path join helper (single definition) ----
    @classmethod
    def get_spaces_path(cls, *parts: str) -> str:
        # avoid '//' if part already has a leading slash
        suffix = "/".join(p.strip("/") for p in parts if p)
        return f"{cls.SPACES_BASE_PREFIX}{suffix}" if suffix else cls.SPACES_BASE_PREFIX

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
SPACES_REGION = urlparse(config.SPACES_ENDPOINT).hostname.split('.')[0] if urlparse(config.SPACES_ENDPOINT).hostname else "nyc3"
SPACES_ENDPOINT = config.SPACES_ENDPOINT
SPACES_ORIGIN_URL = config.SPACES_ORIGIN_URL
DEBUG_MODE = config.DEBUG_MODE

# Logging line for canonical paths (used anywhere we print canonical paths)
def print_paths_resolved():
    """Print canonical paths resolution summary."""
    import sys
    print(
        "paths_resolved "
        f"base=trading-system data_root={config.DATA_ROOT} universe_key={config.UNIVERSE_KEY} "
        "orchestrator=orchestrator/run_all.py "
        f"endpoint={config.SPACES_ENDPOINT} bucket={config.SPACES_BUCKET_NAME} prefix={config.SPACES_BASE_PREFIX} "
        f"origin_url={config.SPACES_ORIGIN_URL} python_version={sys.version.split()[0]}"
    )
