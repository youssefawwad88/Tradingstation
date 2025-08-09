"""
Configuration management for Trading Station.
Reads environment variables with defaults and provides constants for data paths.
"""

import os
from pathlib import Path
from typing import Dict, Any, Optional
import yaml

# Load environment variables from .env file if it exists
def _load_env_file():
    """Load environment variables from .env file if present."""
    env_file = Path(__file__).parent.parent / '.env'
    if env_file.exists():
        with open(env_file, 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, value = line.split('=', 1)
                    # Only set if not already in environment
                    if key not in os.environ:
                        os.environ[key] = value

# Load .env file first
_load_env_file()

# Core Configuration
TIMEZONE = os.getenv('TIMEZONE', 'America/New_York')
DEBUG_MODE = os.getenv('DEBUG_MODE', 'false').lower() == 'true'
TEST_MODE = os.getenv('TEST_MODE', 'false').lower() == 'true'
FORCE_LIVE_API = os.getenv('FORCE_LIVE_API', 'false').lower() == 'true'

# Storage Configuration (DigitalOcean Spaces or S3)
SPACES_BASE_PREFIX = os.getenv('SPACES_BASE_PREFIX', 'data')
SPACES_ACCESS_KEY_ID = os.getenv('SPACES_ACCESS_KEY_ID', '')
SPACES_SECRET_ACCESS_KEY = os.getenv('SPACES_SECRET_ACCESS_KEY', '')
SPACES_BUCKET_NAME = os.getenv('SPACES_BUCKET_NAME', '')
SPACES_REGION = os.getenv('SPACES_REGION', 'nyc3')

# Data Retention Settings
INTRADAY_TRIM_DAYS = int(os.getenv('INTRADAY_TRIM_DAYS', '7'))
INTRADAY_EXCLUDE_TODAY = os.getenv('INTRADAY_EXCLUDE_TODAY', 'false').lower() == 'true'
PROCESS_MANUAL_TICKERS = os.getenv('PROCESS_MANUAL_TICKERS', 'true').lower() == 'true'

# API Configuration (Alpha Vantage)
ALPHA_VANTAGE_API_KEY = os.getenv('ALPHA_VANTAGE_API_KEY', '')
API_RATE_LIMIT_PER_MIN = int(os.getenv('API_RATE_LIMIT_PER_MIN', '5'))

# Orchestration Settings
HEALTHCHECK_DEGRADED_AFTER_MINS = int(os.getenv('HEALTHCHECK_DEGRADED_AFTER_MINS', '3'))
HEALTHCHECK_FAIL_AFTER_MINS = int(os.getenv('HEALTHCHECK_FAIL_AFTER_MINS', '10'))

# Data Path Constants
DATA_PREFIX = "data"
DAILY_DIR = "data/daily"
INTRADAY_1M_DIR = "data/intraday"
INTRADAY_30M_DIR = "data/intraday_30min"
SIGNALS_DIR = "data/signals"
UNIVERSE_DIR = "data/universe"
SYSTEM_DIR = "data/system"

# Strategy Settings - loaded from config if available
def load_strategy_config() -> Dict[str, Any]:
    """Load strategy-specific configuration from YAML files."""
    config_dir = Path(__file__).parent.parent / 'config'
    strategy_config = {}
    
    # Load symbols universe
    symbols_file = config_dir / 'symbols_universe.yaml'
    if symbols_file.exists():
        with open(symbols_file, 'r') as f:
            strategy_config['universe'] = yaml.safe_load(f)
    
    # Load schedules
    schedules_file = config_dir / 'schedules.yml'
    if schedules_file.exists():
        with open(schedules_file, 'r') as f:
            strategy_config['schedules'] = yaml.safe_load(f)
    
    return strategy_config

# Strategy configuration (lazily loaded)
_strategy_config: Optional[Dict[str, Any]] = None

def get_strategy_config() -> Dict[str, Any]:
    """Get strategy configuration, loading it if not already loaded."""
    global _strategy_config
    if _strategy_config is None:
        _strategy_config = load_strategy_config()
    return _strategy_config

# Manual ticker search paths
MANUAL_TICKER_SEARCH_PATHS = [
    "/workspace/tickerlist.txt",
    "/workspace/data/tickerlist.txt", 
    "./tickerlist.txt"
]

# Validation settings
def validate_config() -> bool:
    """Validate that required configuration is present."""
    required_for_live = [
        ('ALPHA_VANTAGE_API_KEY', ALPHA_VANTAGE_API_KEY),
    ]
    
    # Only validate API keys if not in test mode
    if not TEST_MODE:
        for name, value in required_for_live:
            if not value:
                print(f"WARNING: {name} not configured - API calls will fail")
                return False
    
    return True

# Strategy-specific constants (can be overridden in config)
class StrategyDefaults:
    """Default values for trading strategies."""
    
    # Gap & Go
    GAPGO_MIN_GAP_PERCENT = 2.0
    GAPGO_MIN_VOLUME_MULTIPLE = 2.0
    GAPGO_FIRST_VALID_TIME = "09:36:00"  # ET
    
    # ORB (Opening Range Breakout)
    ORB_RANGE_START = "09:30:00"  # ET
    ORB_RANGE_END = "09:39:00"    # ET
    ORB_MIN_VOLUME_MULTIPLE = 1.5
    
    # AVWAP
    AVWAP_MIN_VOLUME_PERCENTILE = 80
    AVWAP_MIN_BODY_TO_RANGE_RATIO = 0.6
    
    # Risk Management
    DEFAULT_RISK_MULTIPLE = 1.0  # 1R
    TARGET_2R = 2.0
    TARGET_3R = 3.0
    
    # Volume analysis
    VOLUME_LOOKBACK_DAYS = 5
    HIGH_VOLUME_THRESHOLD = 2.0  # 2x average

# Export main configuration items
__all__ = [
    'TIMEZONE', 'DEBUG_MODE', 'TEST_MODE', 'FORCE_LIVE_API',
    'SPACES_BASE_PREFIX', 'SPACES_ACCESS_KEY_ID', 'SPACES_SECRET_ACCESS_KEY', 
    'SPACES_BUCKET_NAME', 'SPACES_REGION',
    'INTRADAY_TRIM_DAYS', 'INTRADAY_EXCLUDE_TODAY', 'PROCESS_MANUAL_TICKERS',
    'ALPHA_VANTAGE_API_KEY', 'API_RATE_LIMIT_PER_MIN',
    'HEALTHCHECK_DEGRADED_AFTER_MINS', 'HEALTHCHECK_FAIL_AFTER_MINS',
    'DATA_PREFIX', 'DAILY_DIR', 'INTRADAY_1M_DIR', 'INTRADAY_30M_DIR', 
    'SIGNALS_DIR', 'UNIVERSE_DIR', 'SYSTEM_DIR',
    'MANUAL_TICKER_SEARCH_PATHS',
    'get_strategy_config', 'validate_config',
    'StrategyDefaults'
]