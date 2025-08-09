"""
Structured logging system for the trading platform.

This module provides structured logging with proper formatting, levels,
and integration with monitoring systems.
"""

import json
import logging
import logging.handlers
import os
import sys
import time
from datetime import datetime
from typing import Any, Dict, Optional
from pathlib import Path

from core.config_manager import get_config


class StructuredFormatter(logging.Formatter):
    """Custom formatter for structured JSON logging."""

    def format(self, record: logging.LogRecord) -> str:
        """Format log record as structured JSON."""
        # Base log data
        log_data = {
            "timestamp": datetime.utcfromtimestamp(record.created).isoformat() + "Z",
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
        }

        # Add exception info if present
        if record.exc_info:
            log_data["exception"] = self.formatException(record.exc_info)

        # Add extra fields from record
        for key, value in record.__dict__.items():
            if key not in [
                "name",
                "msg",
                "args",
                "levelname",
                "levelno",
                "pathname",
                "filename",
                "module",
                "exc_info",
                "exc_text",
                "stack_info",
                "lineno",
                "funcName",
                "created",
                "msecs",
                "relativeCreated",
                "thread",
                "threadName",
                "processName",
                "process",
                "getMessage",
            ]:
                log_data[key] = value

        return json.dumps(log_data)


class TradingLogger:
    """Enhanced logger for trading operations with structured logging."""

    def __init__(self, name: str):
        self.logger = logging.getLogger(name)
        self._setup_logger()

    def _setup_logger(self):
        """Set up logger with configuration from config manager."""
        config = get_config()

        # Clear existing handlers
        self.logger.handlers.clear()

        # Set level
        level = config.get("logging.level", "INFO")
        self.logger.setLevel(getattr(logging, level.upper()))

        # Console handler
        console_handler = logging.StreamHandler(sys.stdout)
        console_formatter = StructuredFormatter()
        console_handler.setFormatter(console_formatter)
        self.logger.addHandler(console_handler)

        # File handler if enabled
        if config.get("logging.file_enabled", True):
            self._setup_file_handler(config)

        # Prevent propagation to root logger
        self.logger.propagate = False

    def _setup_file_handler(self, config):
        """Set up rotating file handler."""
        log_file = config.get("logging.file_path", "./logs/trading.log")
        log_dir = os.path.dirname(log_file)

        # Create log directory
        Path(log_dir).mkdir(parents=True, exist_ok=True)

        # Set up rotating file handler
        max_bytes = config.get("logging.max_file_size_mb", 10) * 1024 * 1024
        backup_count = config.get("logging.backup_count", 5)

        file_handler = logging.handlers.RotatingFileHandler(
            log_file, maxBytes=max_bytes, backupCount=backup_count
        )

        file_formatter = StructuredFormatter()
        file_handler.setFormatter(file_formatter)
        self.logger.addHandler(file_handler)

    def info(self, message: str, **kwargs):
        """Log info message with structured data."""
        self.logger.info(message, extra=kwargs)

    def warning(self, message: str, **kwargs):
        """Log warning message with structured data."""
        self.logger.warning(message, extra=kwargs)

    def error(self, message: str, **kwargs):
        """Log error message with structured data."""
        self.logger.error(message, extra=kwargs)

    def debug(self, message: str, **kwargs):
        """Log debug message with structured data."""
        self.logger.debug(message, extra=kwargs)

    def critical(self, message: str, **kwargs):
        """Log critical message with structured data."""
        self.logger.critical(message, extra=kwargs)

    def trade_signal(self, signal: Dict[str, Any]):
        """Log a trading signal with structured data."""
        self.info(
            "Trading signal generated",
            signal_type=signal.get("signal_type"),
            ticker=signal.get("ticker"),
            entry_price=signal.get("entry_price"),
            screener=signal.get("screener"),
            risk_reward_ratio=signal.get("risk_reward_ratio"),
            signal_data=signal,
        )

    def market_event(self, event_type: str, **kwargs):
        """Log a market event."""
        self.info(f"Market event: {event_type}", event_type=event_type, **kwargs)

    def performance_metric(self, metric_name: str, value: float, **kwargs):
        """Log a performance metric."""
        self.info(
            f"Performance metric: {metric_name}",
            metric_name=metric_name,
            metric_value=value,
            **kwargs,
        )

    def api_call(
        self, endpoint: str, ticker: str, duration: float, success: bool, **kwargs
    ):
        """Log an API call."""
        self.info(
            f"API call to {endpoint}",
            endpoint=endpoint,
            ticker=ticker,
            duration=duration,
            success=success,
            **kwargs,
        )


class LoggerManager:
    """Manager for creating and managing trading loggers."""

    _loggers: Dict[str, TradingLogger] = {}

    @classmethod
    def get_logger(cls, name: str) -> TradingLogger:
        """Get or create a logger instance."""
        if name not in cls._loggers:
            cls._loggers[name] = TradingLogger(name)
        return cls._loggers[name]

    @classmethod
    def setup_all_loggers(cls):
        """Set up all existing loggers with current configuration."""
        for logger in cls._loggers.values():
            logger._setup_logger()


# Convenience functions
def get_logger(name: str) -> TradingLogger:
    """Get a trading logger instance."""
    return LoggerManager.get_logger(name)


def setup_logging():
    """Set up logging system with configuration."""
    LoggerManager.setup_all_loggers()


# Log context manager for timing operations
class LogTimer:
    """Context manager for timing and logging operations."""

    def __init__(self, logger: TradingLogger, operation: str, **kwargs):
        self.logger = logger
        self.operation = operation
        self.kwargs = kwargs
        self.start_time = None

    def __enter__(self):
        self.start_time = time.time()
        self.logger.debug(f"Starting {self.operation}", **self.kwargs)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        duration = time.time() - self.start_time

        if exc_type is None:
            self.logger.info(
                f"Completed {self.operation}",
                duration=duration,
                success=True,
                **self.kwargs,
            )
        else:
            self.logger.error(
                f"Failed {self.operation}",
                duration=duration,
                success=False,
                error=str(exc_val),
                **self.kwargs,
            )


# Decorator for automatic logging of function calls
def log_function_call(logger_name: str = None):
    """Decorator to automatically log function calls."""

    def decorator(func):
        def wrapper(*args, **kwargs):
            logger = get_logger(logger_name or func.__module__)

            with LogTimer(logger, f"{func.__name__} call"):
                return func(*args, **kwargs)

        return wrapper

    return decorator
