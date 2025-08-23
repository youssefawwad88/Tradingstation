"""Central configuration management for the trading system.

This module handles all configuration, environment variables, and constants.
No secrets are stored in code - everything comes from environment variables.
"""

import os
from pathlib import Path
from typing import Optional

# Load environment variables from .env file if it exists
try:
    from dotenv import load_dotenv

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
    
    # Environment variables for degraded mode
    INTRADAY_EXTENDED: bool = os.getenv("INTRADAY_EXTENDED", "false").lower() == "true"
    DEGRADE_INTRADAY_ON_STALE_MINUTES: int = int(os.getenv("DEGRADE_INTRADAY_ON_STALE_MINUTES", "5"))
    PROVIDER_DEGRADED_ALLOWED: bool = os.getenv("PROVIDER_DEGRADED_ALLOWED", "true").lower() == "true"

    # === DigitalOcean Spaces Configuration ===
    SPACES_ACCESS_KEY_ID: Optional[str] = os.getenv("SPACES_ACCESS_KEY_ID")
    SPACES_SECRET_ACCESS_KEY: Optional[str] = os.getenv("SPACES_SECRET_ACCESS_KEY")
    SPACES_BUCKET_NAME: Optional[str] = os.getenv("SPACES_BUCKET_NAME")
    SPACES_REGION: str = os.getenv("SPACES_REGION", "nyc3")
    SPACES_ENDPOINT: str = f"https://{SPACES_REGION}.digitaloceanspaces.com"

    # === Application Environment ===
    APP_ENV: str = os.getenv("APP_ENV", "development")
    DEPLOYMENT_TAG: Optional[str] = os.getenv("DEPLOYMENT_TAG")

    # === Feature Flags ===
    FETCH_EXTENDED_HOURS: bool = os.getenv("FETCH_EXTENDED_HOURS", "true").lower() == "true"
    TEST_MODE_INIT_ALLOWED: bool = os.getenv("TEST_MODE_INIT_ALLOWED", "true").lower() == "true"
    DEBUG_MODE: bool = os.getenv("DEBUG_MODE", "false").lower() == "true"

    # === Data Layer Structure (Spaces paths) ===
    SPACES_BASE_PREFIX: str = os.getenv("SPACES_BASE_PREFIX", "trading-system")
    DATA_ROOT: str = os.getenv("DATA_ROOT", "data")
    UNIVERSE_KEY: str = os.getenv("UNIVERSE_KEY", "data/Universe/master_tickerlist.csv")

    # === Fallback Configuration ===
    FALLBACK_TICKERS: list[str] = ["NVDA", "AAPL", "TSLA", "MSFT", "GOOGL"]
    MASTER_TICKERLIST_PATH: tuple[str, ...] = ("data", "universe", "master_tickerlist.csv")

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
DEBUG_MODE = config.DEBUG_MODE
