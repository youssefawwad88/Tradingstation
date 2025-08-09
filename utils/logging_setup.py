"""
Logging setup for Trading Station.
Configures structured logging with run IDs, timestamps, and multiple output formats.
"""

import logging
import logging.config
import os
import sys
import uuid
from pathlib import Path
from typing import Optional
import yaml

# Global run ID for this execution
_current_run_id: Optional[str] = None

def get_run_id() -> str:
    """Get or create a run ID for this execution."""
    global _current_run_id
    if _current_run_id is None:
        _current_run_id = str(uuid.uuid4())[:8]  # Short UUID for readability
    return _current_run_id

def set_run_id(run_id: str) -> None:
    """Set a specific run ID for this execution."""
    global _current_run_id
    _current_run_id = run_id

class RunIdFilter(logging.Filter):
    """Add run_id to log records."""
    
    def filter(self, record):
        record.run_id = get_run_id()
        return True

class StructuredFormatter(logging.Formatter):
    """Custom formatter that includes structured fields."""
    
    def format(self, record):
        # Add standard fields
        if not hasattr(record, 'run_id'):
            record.run_id = get_run_id()
        
        if not hasattr(record, 'elapsed_ms'):
            record.elapsed_ms = 0
            
        if not hasattr(record, 'ticker'):
            record.ticker = 'N/A'
            
        if not hasattr(record, 'job_id'):
            record.job_id = 'N/A'
        
        return super().format(record)

def setup_logging(
    config_path: Optional[str] = None,
    level: Optional[str] = None,
    run_id: Optional[str] = None
) -> None:
    """
    Set up logging configuration.
    
    Args:
        config_path: Path to logging configuration YAML file
        level: Override log level (DEBUG, INFO, WARNING, ERROR)
        run_id: Specific run ID to use
    """
    
    # Set run ID if provided
    if run_id:
        set_run_id(run_id)
    
    # Ensure logs directory exists
    logs_dir = Path("logs")
    logs_dir.mkdir(exist_ok=True)
    
    # Default config path
    if config_path is None:
        config_path = Path(__file__).parent.parent / "config" / "logging.yml"
    
    # Load configuration
    config = None
    if Path(config_path).exists():
        try:
            with open(config_path, 'r') as f:
                config = yaml.safe_load(f)
        except Exception as e:
            print(f"Warning: Failed to load logging config from {config_path}: {e}")
    
    # Use default configuration if loading failed
    if config is None:
        config = _get_default_logging_config()
    
    # Override level if specified
    if level:
        level = level.upper()
        if 'root' in config:
            config['root']['level'] = level
        for logger_config in config.get('loggers', {}).values():
            logger_config['level'] = level
    
    # Apply configuration
    try:
        logging.config.dictConfig(config)
        
        # Add run ID filter to all handlers
        run_id_filter = RunIdFilter()
        for handler in logging.root.handlers:
            handler.addFilter(run_id_filter)
            
        # Also add to named loggers
        for logger_name in config.get('loggers', {}):
            logger = logging.getLogger(logger_name)
            for handler in logger.handlers:
                handler.addFilter(run_id_filter)
                
    except Exception as e:
        # Fallback to basic configuration
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.StreamHandler(sys.stdout),
                logging.FileHandler('logs/tradingstation.log')
            ]
        )
        print(f"Warning: Using basic logging config due to error: {e}")

def _get_default_logging_config() -> dict:
    """Get default logging configuration."""
    return {
        'version': 1,
        'disable_existing_loggers': False,
        'formatters': {
            'detailed': {
                'format': '%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s',
                'datefmt': '%Y-%m-%d %H:%M:%S'
            },
            'structured': {
                'format': '%(asctime)s | %(levelname)-8s | %(name)-20s | %(funcName)-15s | run_id:%(run_id)s | %(message)s',
                'datefmt': '%Y-%m-%d %H:%M:%S'
            }
        },
        'handlers': {
            'console': {
                'class': 'logging.StreamHandler',
                'level': 'INFO',
                'formatter': 'structured',
                'stream': 'ext://sys.stdout'
            },
            'file': {
                'class': 'logging.handlers.RotatingFileHandler',
                'level': 'DEBUG',
                'formatter': 'detailed',
                'filename': 'logs/tradingstation.log',
                'maxBytes': 10485760,  # 10MB
                'backupCount': 5
            }
        },
        'root': {
            'level': 'INFO',
            'handlers': ['console', 'file']
        }
    }

def get_logger(name: str) -> logging.Logger:
    """
    Get a logger with the specified name.
    
    Args:
        name: Logger name (typically __name__)
        
    Returns:
        Configured logger instance
    """
    return logging.getLogger(name)

def log_with_context(
    logger: logging.Logger,
    level: str,
    message: str,
    ticker: Optional[str] = None,
    job_id: Optional[str] = None,
    elapsed_ms: Optional[float] = None,
    **kwargs
) -> None:
    """
    Log a message with additional context fields.
    
    Args:
        logger: Logger instance
        level: Log level (debug, info, warning, error)
        message: Log message
        ticker: Ticker symbol being processed
        job_id: Job identifier
        elapsed_ms: Elapsed time in milliseconds
        **kwargs: Additional context fields
    """
    
    # Create extra fields
    extra = {
        'ticker': ticker or 'N/A',
        'job_id': job_id or 'N/A',
        'elapsed_ms': elapsed_ms or 0
    }
    extra.update(kwargs)
    
    # Log with context
    log_method = getattr(logger, level.lower())
    log_method(message, extra=extra)

# Convenience functions for common logging patterns
def log_job_start(logger: logging.Logger, job_name: str, ticker_count: int = 0) -> None:
    """Log job start with context."""
    log_with_context(
        logger, 'INFO', 
        f"Starting job: {job_name}",
        job_id=job_name,
        ticker_count=ticker_count
    )

def log_job_complete(logger: logging.Logger, job_name: str, elapsed_ms: float, success_count: int = 0) -> None:
    """Log job completion with context."""
    log_with_context(
        logger, 'INFO',
        f"Completed job: {job_name}",
        job_id=job_name,
        elapsed_ms=elapsed_ms,
        success_count=success_count
    )

def log_ticker_result(
    logger: logging.Logger, 
    ticker: str, 
    action: str, 
    success: bool, 
    message: str = "",
    elapsed_ms: float = 0
) -> None:
    """Log ticker processing result with context."""
    level = 'INFO' if success else 'WARNING'
    status = 'SUCCESS' if success else 'FAILED'
    full_message = f"{action} {ticker}: {status}"
    if message:
        full_message += f" - {message}"
    
    log_with_context(
        logger, level, full_message,
        ticker=ticker,
        elapsed_ms=elapsed_ms,
        success=success
    )

def log_api_usage(logger: logging.Logger, endpoint: str, ticker: str, rate_limit_remaining: int = 0) -> None:
    """Log API usage for rate limit tracking."""
    log_with_context(
        logger, 'DEBUG',
        f"API call: {endpoint}",
        ticker=ticker,
        endpoint=endpoint,
        rate_limit_remaining=rate_limit_remaining
    )

# Module exports
__all__ = [
    'setup_logging',
    'get_logger', 
    'get_run_id',
    'set_run_id',
    'log_with_context',
    'log_job_start',
    'log_job_complete', 
    'log_ticker_result',
    'log_api_usage'
]