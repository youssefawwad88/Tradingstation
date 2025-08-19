"""
Central Configuration Module for Strategic Trading System

This file serves as the single source of truth for the entire application,
replacing all hardcoded variables with a centralized configuration system.

Strategic Architecture Features:
- Single source of truth for all configuration
- Environment variable integration
- Type-safe configuration access
- Default value management
- Configuration validation
"""

import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Union
from dataclasses import dataclass

# Load environment variables from .env file if available
try:
    from dotenv import load_dotenv
    
    env_path = Path(__file__).parent.parent / ".env"
    if env_path.exists():
        load_dotenv(env_path)
except ImportError:
    # python-dotenv not installed, continue with system environment variables
    pass


@dataclass
class TradingConfig:
    """
    Central configuration class for the strategic trading system.
    
    This replaces all hardcoded variables and provides a single,
    professional configuration interface.
    """
    
    # Strategic Core Parameters (as specified in requirements)
    TICKER_SYMBOL: str = "AAPL"
    DATA_INTERVAL: str = "1min"
    DATA_TYPE: str = "INTRADAY"
    
    # API Configuration
    ALPHA_VANTAGE_API_KEY: Optional[str] = None
    API_RATE_LIMIT_CALLS_PER_MINUTE: int = 5
    API_TIMEOUT_SECONDS: int = 30
    API_RETRY_ATTEMPTS: int = 3
    API_MAX_CONCURRENT_REQUESTS: int = 5
    
    # DigitalOcean Spaces Configuration
    SPACES_ACCESS_KEY_ID: Optional[str] = None
    SPACES_SECRET_ACCESS_KEY: Optional[str] = None
    SPACES_BUCKET_NAME: Optional[str] = None
    SPACES_REGION: str = "nyc3"
    SPACES_BASE_PREFIX: str = "data"
    SPACES_STRUCTURE_VERSION: str = "v2"
    
    # Data Management Configuration
    INTRADAY_TRIM_DAYS: int = 7
    INTRADAY_EXCLUDE_TODAY: bool = False
    INTRADAY_INCLUDE_PREMARKET: bool = True
    INTRADAY_INCLUDE_AFTERHOURS: bool = True
    TIMEZONE: str = "America/New_York"
    
    # Processing Controls
    PROCESS_MANUAL_TICKERS: bool = True
    MAX_TICKERS_PER_RUN: int = 25
    INTRADAY_BATCH_SIZE: int = 25
    MARKET_HOURS_ONLY: bool = False
    SKIP_IF_FRESH_MINUTES: int = 0
    
    # Debug and Test Mode Configuration
    DEBUG_MODE: bool = True
    TEST_MODE: str = "auto"  # auto, enabled, disabled
    WEEKEND_TEST_MODE_ENABLED: bool = True
    MODE: Optional[str] = None  # test, production
    
    # Data Storage Paths
    BASE_DATA_DIR: str = "./data"
    INTRADAY_DATA_DIR: str = "./data/intraday"
    INTRADAY_30MIN_DATA_DIR: str = "./data/intraday_30min"
    DAILY_DATA_DIR: str = "./data/daily"
    
    # Stock Tickers
    DEFAULT_TICKERS: List[str] = None
    
    # Time Intervals
    INTRADAY_INTERVALS: List[str] = None
    
    # Data Health Check Requirements
    DAILY_MIN_ROWS: int = 200
    THIRTY_MIN_MIN_ROWS: int = 500
    ONE_MIN_REQUIRED_DAYS: int = 7
    
    # Logging Configuration
    LOG_LEVEL: str = "INFO"
    LOG_FORMAT: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    LOG_FILE_ENABLED: bool = True
    LOG_FILE_PATH: str = "./logs/trading.log"
    LOG_MAX_FILE_SIZE_MB: int = 10
    LOG_BACKUP_COUNT: int = 5
    
    # Performance Configuration
    BATCH_SIZE: int = 25
    PROCESSING_THREADS: int = 4
    MEMORY_LIMIT_MB: int = 512
    ENABLE_PROFILING: bool = False
    
    # Cache Configuration
    CACHE_MEMORY_SIZE_MB: int = 100
    CACHE_DISK_SIZE_GB: float = 1.0
    CACHE_DEFAULT_TTL_SECONDS: int = 300
    CACHE_DIR: str = "./cache"
    
    # Trading Strategy Configuration
    RISK_PER_TRADE: float = 100.0
    ENABLE_SHORTS: bool = True
    VOLUME_SPIKE_MULTIPLIER: float = 2.5
    AVWAP_ANCHOR_KEYWORDS: str = "earnings,guidance,fda,approval,contract,acquisition"
    
    def __post_init__(self):
        """Initialize default values and validate configuration."""
        
        # Set default lists if None
        if self.DEFAULT_TICKERS is None:
            self.DEFAULT_TICKERS = [
                "AAPL", "MSFT", "AMZN", "GOOGL", "META",
                "TSLA", "NVDA", "AMD", "INTC", "IBM"
            ]
        
        if self.INTRADAY_INTERVALS is None:
            self.INTRADAY_INTERVALS = ["1min", "30min"]
        
        # Create computed properties
        self.SPACES_ENDPOINT_URL = (
            f"https://{self.SPACES_BUCKET_NAME}.{self.SPACES_REGION}.digitaloceanspaces.com"
            if self.SPACES_BUCKET_NAME
            else None
        )
        
        # Ensure directories exist
        self._ensure_directories_exist()
    
    def _ensure_directories_exist(self):
        """Ensure all required directories exist."""
        directories = [
            self.BASE_DATA_DIR,
            self.INTRADAY_DATA_DIR,
            self.INTRADAY_30MIN_DATA_DIR,
            self.DAILY_DATA_DIR,
            self.CACHE_DIR,
            os.path.dirname(self.LOG_FILE_PATH)
        ]
        
        for directory in directories:
            if directory:  # Only create if not empty
                os.makedirs(directory, exist_ok=True)
    
    @classmethod
    def from_environment(cls) -> 'TradingConfig':
        """
        Create configuration from environment variables.
        
        Returns:
            TradingConfig instance with values from environment
        """
        return cls(
            # Strategic Core Parameters
            TICKER_SYMBOL=os.getenv("TICKER_SYMBOL", "AAPL"),
            DATA_INTERVAL=os.getenv("DATA_INTERVAL", "1min"),
            DATA_TYPE=os.getenv("DATA_TYPE", "INTRADAY"),
            
            # API Configuration
            ALPHA_VANTAGE_API_KEY=os.getenv("ALPHA_VANTAGE_API_KEY"),
            API_RATE_LIMIT_CALLS_PER_MINUTE=int(os.getenv("API_RATE_LIMIT_CALLS_PER_MINUTE", "5")),
            API_TIMEOUT_SECONDS=int(os.getenv("API_TIMEOUT_SECONDS", "30")),
            API_RETRY_ATTEMPTS=int(os.getenv("API_RETRY_ATTEMPTS", "3")),
            API_MAX_CONCURRENT_REQUESTS=int(os.getenv("API_MAX_CONCURRENT_REQUESTS", "5")),
            
            # DigitalOcean Spaces Configuration
            SPACES_ACCESS_KEY_ID=os.getenv("SPACES_ACCESS_KEY_ID"),
            SPACES_SECRET_ACCESS_KEY=os.getenv("SPACES_SECRET_ACCESS_KEY"),
            SPACES_BUCKET_NAME=os.getenv("SPACES_BUCKET_NAME"),
            SPACES_REGION=os.getenv("SPACES_REGION", "nyc3"),
            SPACES_BASE_PREFIX=os.getenv("SPACES_BASE_PREFIX", "data"),
            SPACES_STRUCTURE_VERSION=os.getenv("SPACES_STRUCTURE_VERSION", "v2"),
            
            # Data Management Configuration
            INTRADAY_TRIM_DAYS=int(os.getenv("INTRADAY_TRIM_DAYS", "7")),
            INTRADAY_EXCLUDE_TODAY=os.getenv("INTRADAY_EXCLUDE_TODAY", "false").lower() == "true",
            INTRADAY_INCLUDE_PREMARKET=os.getenv("INTRADAY_INCLUDE_PREMARKET", "true").lower() == "true",
            INTRADAY_INCLUDE_AFTERHOURS=os.getenv("INTRADAY_INCLUDE_AFTERHOURS", "true").lower() == "true",
            TIMEZONE=os.getenv("TIMEZONE", "America/New_York"),
            
            # Processing Controls
            PROCESS_MANUAL_TICKERS=os.getenv("PROCESS_MANUAL_TICKERS", "true").lower() == "true",
            MAX_TICKERS_PER_RUN=int(os.getenv("MAX_TICKERS_PER_RUN", "25")),
            INTRADAY_BATCH_SIZE=int(os.getenv("INTRADAY_BATCH_SIZE", "25")),
            MARKET_HOURS_ONLY=os.getenv("MARKET_HOURS_ONLY", "false").lower() == "true",
            SKIP_IF_FRESH_MINUTES=int(os.getenv("SKIP_IF_FRESH_MINUTES", "0")),
            
            # Debug and Test Mode Configuration
            DEBUG_MODE=os.getenv("DEBUG_MODE", "true").lower() == "true",
            TEST_MODE=os.getenv("TEST_MODE", "auto").lower(),
            WEEKEND_TEST_MODE_ENABLED=os.getenv("WEEKEND_TEST_MODE_ENABLED", "true").lower() == "true",
            MODE=os.getenv("MODE", "").lower() if os.getenv("MODE") else None,
            
            # Data Storage Paths
            BASE_DATA_DIR=os.getenv("BASE_DATA_DIR", "./data"),
            INTRADAY_DATA_DIR=os.getenv("INTRADAY_DATA_DIR", "./data/intraday"),
            INTRADAY_30MIN_DATA_DIR=os.getenv("INTRADAY_30MIN_DATA_DIR", "./data/intraday_30min"),
            DAILY_DATA_DIR=os.getenv("DAILY_DATA_DIR", "./data/daily"),
            
            # Data Health Check Requirements
            DAILY_MIN_ROWS=int(os.getenv("DAILY_MIN_ROWS", "200")),
            THIRTY_MIN_MIN_ROWS=int(os.getenv("THIRTY_MIN_MIN_ROWS", "500")),
            ONE_MIN_REQUIRED_DAYS=int(os.getenv("ONE_MIN_REQUIRED_DAYS", "7")),
            
            # Logging Configuration
            LOG_LEVEL=os.getenv("LOG_LEVEL", "INFO"),
            LOG_FORMAT=os.getenv("LOG_FORMAT", "%(asctime)s - %(name)s - %(levelname)s - %(message)s"),
            LOG_FILE_ENABLED=os.getenv("LOG_FILE_ENABLED", "true").lower() == "true",
            LOG_FILE_PATH=os.getenv("LOG_FILE_PATH", "./logs/trading.log"),
            LOG_MAX_FILE_SIZE_MB=int(os.getenv("LOG_MAX_FILE_SIZE_MB", "10")),
            LOG_BACKUP_COUNT=int(os.getenv("LOG_BACKUP_COUNT", "5")),
            
            # Performance Configuration
            BATCH_SIZE=int(os.getenv("BATCH_SIZE", "25")),
            PROCESSING_THREADS=int(os.getenv("PROCESSING_THREADS", "4")),
            MEMORY_LIMIT_MB=int(os.getenv("MEMORY_LIMIT_MB", "512")),
            ENABLE_PROFILING=os.getenv("ENABLE_PROFILING", "false").lower() == "true",
            
            # Cache Configuration
            CACHE_MEMORY_SIZE_MB=int(os.getenv("CACHE_MEMORY_SIZE_MB", "100")),
            CACHE_DISK_SIZE_GB=float(os.getenv("CACHE_DISK_SIZE_GB", "1.0")),
            CACHE_DEFAULT_TTL_SECONDS=int(os.getenv("CACHE_DEFAULT_TTL_SECONDS", "300")),
            CACHE_DIR=os.getenv("CACHE_DIR", "./cache"),
            
            # Trading Strategy Configuration
            RISK_PER_TRADE=float(os.getenv("RISK_PER_TRADE", "100.0")),
            ENABLE_SHORTS=os.getenv("ENABLE_SHORTS", "true").lower() == "true",
            VOLUME_SPIKE_MULTIPLIER=float(os.getenv("VOLUME_SPIKE_MULTIPLIER", "2.5")),
            AVWAP_ANCHOR_KEYWORDS=os.getenv("AVWAP_ANCHOR_KEYWORDS", "earnings,guidance,fda,approval,contract,acquisition"),
        )
    
    def to_dict(self) -> Dict[str, Any]:
        """
        Convert configuration to dictionary.
        
        Returns:
            Dictionary representation of configuration
        """
        return {
            field.name: getattr(self, field.name)
            for field in self.__dataclass_fields__.values()
        }
    
    def validate(self) -> List[str]:
        """
        Validate configuration and return list of errors.
        
        Returns:
            List of validation error messages
        """
        errors = []
        
        # Validate required API keys for production mode
        if self.MODE == "production" or (self.MODE is None and not self.TEST_MODE):
            if not self.ALPHA_VANTAGE_API_KEY:
                errors.append("ALPHA_VANTAGE_API_KEY is required for production mode")
        
        # Validate numeric ranges
        if self.API_RATE_LIMIT_CALLS_PER_MINUTE <= 0:
            errors.append("API_RATE_LIMIT_CALLS_PER_MINUTE must be positive")
        
        if self.INTRADAY_TRIM_DAYS < 0:
            errors.append("INTRADAY_TRIM_DAYS cannot be negative")
        
        if self.MAX_TICKERS_PER_RUN <= 0:
            errors.append("MAX_TICKERS_PER_RUN must be positive")
        
        # Validate intervals
        valid_intervals = ["1min", "5min", "15min", "30min", "60min"]
        if self.DATA_INTERVAL not in valid_intervals:
            errors.append(f"DATA_INTERVAL must be one of {valid_intervals}")
        
        # Validate data type
        valid_data_types = ["INTRADAY", "DAILY", "QUOTE"]
        if self.DATA_TYPE not in valid_data_types:
            errors.append(f"DATA_TYPE must be one of {valid_data_types}")
        
        return errors
    
    def is_production_mode(self) -> bool:
        """
        Check if running in production mode.
        
        Returns:
            True if in production mode
        """
        if self.MODE:
            return self.MODE.lower() == "production"
        
        return self.TEST_MODE.lower() == "disabled"
    
    def is_test_mode(self) -> bool:
        """
        Check if running in test mode.
        
        Returns:
            True if in test mode
        """
        if self.MODE:
            return self.MODE.lower() == "test"
        
        return self.TEST_MODE.lower() == "enabled"


# Global configuration instance
config = TradingConfig.from_environment()

# Backward compatibility - export commonly used variables
TICKER_SYMBOL = config.TICKER_SYMBOL
DATA_INTERVAL = config.DATA_INTERVAL
DATA_TYPE = config.DATA_TYPE

ALPHA_VANTAGE_API_KEY = config.ALPHA_VANTAGE_API_KEY

SPACES_ACCESS_KEY_ID = config.SPACES_ACCESS_KEY_ID
SPACES_SECRET_ACCESS_KEY = config.SPACES_SECRET_ACCESS_KEY
SPACES_BUCKET_NAME = config.SPACES_BUCKET_NAME
SPACES_REGION = config.SPACES_REGION
SPACES_ENDPOINT_URL = config.SPACES_ENDPOINT_URL

BASE_DATA_DIR = config.BASE_DATA_DIR
INTRADAY_DATA_DIR = config.INTRADAY_DATA_DIR
INTRADAY_30MIN_DATA_DIR = config.INTRADAY_30MIN_DATA_DIR
DAILY_DATA_DIR = config.DAILY_DATA_DIR

DEFAULT_TICKERS = config.DEFAULT_TICKERS
INTRADAY_INTERVALS = config.INTRADAY_INTERVALS

DEBUG_MODE = config.DEBUG_MODE
TEST_MODE = config.TEST_MODE


def get_config() -> TradingConfig:
    """
    Get the global configuration instance.
    
    Returns:
        Global TradingConfig instance
    """
    return config


def reload_config() -> TradingConfig:
    """
    Reload configuration from environment variables.
    
    Returns:
        New TradingConfig instance
    """
    global config
    config = TradingConfig.from_environment()
    return config


def validate_config() -> bool:
    """
    Validate the current configuration.
    
    Returns:
        True if configuration is valid
    """
    errors = config.validate()
    
    if errors:
        import logging
        logger = logging.getLogger(__name__)
        logger.error("Configuration validation failed:")
        for error in errors:
            logger.error(f"  - {error}")
        return False
    
    return True