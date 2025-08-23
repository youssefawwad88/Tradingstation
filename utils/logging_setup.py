"""Structured logging system for the trading platform.

This module provides JSON-structured logging with deployment tracking,
proper formatting, and integration with monitoring systems.
"""

import json
import logging
import logging.handlers
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

from utils.config import config


class StructuredFormatter(logging.Formatter):
    """Custom formatter for structured JSON logging."""

    def __init__(self) -> None:
        """Initialize structured formatter."""
        super().__init__()

    def format(self, record: logging.LogRecord) -> str:
        """Format log record as structured JSON."""
        # Base log entry
        log_entry = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
        }

        # Add deployment information
        if config.DEPLOYMENT_TAG:
            log_entry["deployment"] = config.DEPLOYMENT_TAG

        log_entry["environment"] = config.APP_ENV

        # Add exception information if present
        if record.exc_info:
            log_entry["exception"] = self.formatException(record.exc_info)

        # Add extra fields from the log record
        extra_fields = {}
        for key, value in record.__dict__.items():
            if key not in [
                "name", "msg", "args", "levelname", "levelno", "pathname",
                "filename", "module", "lineno", "funcName", "created",
                "msecs", "relativeCreated", "thread", "threadName",
                "processName", "process", "getMessage", "exc_info",
                "exc_text", "stack_info",
            ]:
                extra_fields[key] = value

        if extra_fields:
            log_entry["extra"] = extra_fields

        return json.dumps(log_entry, default=str, separators=(',', ':'))


class TradingLogger:
    """Enhanced logger for trading operations with structured logging."""

    def __init__(self, name: str) -> None:
        """Initialize trading logger.
        
        Args:
            name: Logger name
        """
        self.name = name
        self.logger = logging.getLogger(name)
        self._setup_logger()

    def _setup_logger(self) -> None:
        """Set up logger with appropriate handlers and formatting."""
        # Clear existing handlers
        self.logger.handlers.clear()

        # Set log level based on debug mode
        level = logging.DEBUG if config.DEBUG_MODE else logging.INFO
        self.logger.setLevel(level)

        # Console handler with structured formatting
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(level)
        console_handler.setFormatter(StructuredFormatter())
        self.logger.addHandler(console_handler)

        # File handler with rotation (if not in cloud environment)
        if config.APP_ENV != "production":
            self._setup_file_handler()

        # Prevent propagation to root logger
        self.logger.propagate = False

    def _setup_file_handler(self) -> None:
        """Set up rotating file handler for local development."""
        log_dir = Path("logs")
        log_dir.mkdir(exist_ok=True)

        log_file = log_dir / f"{self.name}.log"

        # Rotating file handler (10MB max, keep 5 files)
        file_handler = logging.handlers.RotatingFileHandler(
            log_file,
            maxBytes=10 * 1024 * 1024,  # 10MB
            backupCount=5,
        )

        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(StructuredFormatter())
        self.logger.addHandler(file_handler)

    def info(self, message: str, **kwargs) -> None:
        """Log info message with structured data."""
        self.logger.info(message, extra=kwargs)

    def warning(self, message: str, **kwargs) -> None:
        """Log warning message with structured data."""
        self.logger.warning(message, extra=kwargs)

    def error(self, message: str, **kwargs) -> None:
        """Log error message with structured data."""
        self.logger.error(message, extra=kwargs)

    def debug(self, message: str, **kwargs) -> None:
        """Log debug message with structured data."""
        self.logger.debug(message, extra=kwargs)

    def critical(self, message: str, **kwargs) -> None:
        """Log critical message with structured data."""
        self.logger.critical(message, extra=kwargs)

    def trade_signal(self, signal: Dict[str, Any]) -> None:
        """Log a trading signal with structured data."""
        self.info(
            "Trading signal generated",
            signal_type="trade",
            symbol=signal.get("symbol"),
            direction=signal.get("direction"),
            entry=signal.get("entry"),
            stop=signal.get("stop"),
            setup=signal.get("setup_name"),
            score=signal.get("score"),
        )

    def market_event(self, event_type: str, **kwargs) -> None:
        """Log a market event with structured data."""
        self.info(
            f"Market event: {event_type}",
            event_type=event_type,
            **kwargs,
        )

    def api_call(
        self,
        endpoint: str,
        symbol: str,
        success: bool,
        duration_ms: Optional[float] = None,
        **kwargs,
    ) -> None:
        """Log an API call with performance metrics."""
        level = self.info if success else self.error
        level(
            f"API call to {endpoint}",
            api_endpoint=endpoint,
            symbol=symbol,
            success=success,
            duration_ms=duration_ms,
            **kwargs,
        )

    def data_operation(
        self,
        operation: str,
        symbol: str,
        interval: Optional[str] = None,
        rows_processed: Optional[int] = None,
        file_size_bytes: Optional[int] = None,
        **kwargs,
    ) -> None:
        """Log a data operation with metrics."""
        self.info(
            f"Data operation: {operation}",
            operation=operation,
            symbol=symbol,
            interval=interval,
            rows_processed=rows_processed,
            file_size_bytes=file_size_bytes,
            **kwargs,
        )

    def job_start(self, job_name: str, **kwargs) -> None:
        """Log job start with deployment tracking."""
        deployment_info = f"[DEPLOYMENT {config.DEPLOYMENT_TAG}]" if config.DEPLOYMENT_TAG else ""
        self.info(
            f"🚀 {job_name} {deployment_info} - Initialization Starting",
            job=job_name,
            status="starting",
            **kwargs,
        )

    def job_complete(
        self,
        job_name: str,
        duration_seconds: Optional[float] = None,
        success: bool = True,
        **kwargs,
    ) -> None:
        """Log job completion."""
        status_icon = "✅" if success else "❌"
        status = "completed" if success else "failed"

        self.info(
            f"{status_icon} {job_name} - {status.title()}",
            job=job_name,
            status=status,
            duration_seconds=duration_seconds,
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
    def setup_all_loggers(cls) -> None:
        """Set up all existing loggers with current configuration."""
        for logger in cls._loggers.values():
            logger._setup_logger()


# Convenience functions
def get_logger(name: str) -> TradingLogger:
    """Get a trading logger instance."""
    return LoggerManager.get_logger(name)


def setup_logging() -> None:
    """Set up logging system with configuration."""
    # Configure root logger to prevent interference
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.WARNING)

    # Remove default handlers
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)

    # Set up all managed loggers
    LoggerManager.setup_all_loggers()


class LogTimer:
    """Context manager for timing and logging operations."""

    def __init__(self, logger: TradingLogger, operation: str, **kwargs) -> None:
        """Initialize log timer.
        
        Args:
            logger: Logger instance
            operation: Operation name
            **kwargs: Additional fields to log
        """
        self.logger = logger
        self.operation = operation
        self.extra_fields = kwargs
        self.start_time: Optional[float] = None

    def __enter__(self) -> "LogTimer":
        """Start timing."""
        import time
        self.start_time = time.time()
        self.logger.debug(f"Starting {self.operation}", **self.extra_fields)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """End timing and log results."""
        import time
        if self.start_time is not None:
            duration = time.time() - self.start_time
            success = exc_type is None

            self.logger.info(
                f"Completed {self.operation}",
                duration_seconds=duration,
                success=success,
                **self.extra_fields,
            )

            if not success:
                self.logger.error(
                    f"Error in {self.operation}: {exc_val}",
                    error_type=exc_type.__name__ if exc_type else None,
                    **self.extra_fields,
                )


def log_function_call(logger_name: Optional[str] = None):
    """Decorator to automatically log function calls."""
    def decorator(func):
        def wrapper(*args, **kwargs):
            logger = get_logger(logger_name or func.__module__)

            with LogTimer(
                logger,
                f"{func.__name__}",
                function=func.__name__,
                module=func.__module__,
            ):
                return func(*args, **kwargs)

        return wrapper
    return decorator


# Initialize logging on import
setup_logging()
