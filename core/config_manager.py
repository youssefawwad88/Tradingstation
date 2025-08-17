"""
Configuration management system for the trading platform.

This module provides a flexible configuration system that supports
multiple sources (environment variables, files, defaults) with
hot-reloading and validation capabilities.
"""

import json
import logging
import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import yaml

logger = logging.getLogger(__name__)


class ConfigurationManager:
    """
    Flexible configuration manager supporting multiple sources and hot-reloading.
    """

    def __init__(
        self,
        config_file: Optional[str] = None,
        env_prefix: str = "TRADING_",
        auto_reload: bool = False,
    ):
        self.config_file = config_file
        self.env_prefix = env_prefix
        self.auto_reload = auto_reload
        self._config: Dict[str, Any] = {}
        self._defaults: Dict[str, Any] = {}
        self._file_mtime: Optional[float] = None

        self._load_defaults()
        self.reload()

    def _load_defaults(self) -> None:
        """Load default configuration values."""
        self._defaults = {
            # API Configuration
            "api": {
                "alpha_vantage_key": "",
                "rate_limit_calls_per_minute": 5,
                "timeout_seconds": 30,
                "retry_attempts": 3,
                "max_concurrent_requests": 5,
            },
            # Data Storage Configuration
            "storage": {
                "spaces_access_key": "",
                "spaces_secret_key": "",
                "spaces_bucket": "",
                "spaces_region": "nyc3",
                "local_data_dir": "./data",
                "backup_enabled": True,
            },
            # Cache Configuration
            "cache": {
                "memory_size_mb": 100,
                "disk_size_gb": 1.0,
                "default_ttl_seconds": 300,
                "cache_dir": "./cache",
            },
            # Trading Configuration
            "trading": {
                "default_risk_per_trade": 0.02,  # 2%
                "max_position_size": 0.1,  # 10% of account
                "default_stop_loss_percent": 0.02,  # 2%
                "default_take_profit_ratio": 2.0,  # 2:1 R/R
                "market_hours_only": False,
            },
            # Screener Configuration
            "screeners": {
                "enabled": ["gapgo", "avwap", "orb"],
                "max_signals_per_screener": 50,
                "min_gap_percent": 2.0,
                "min_volume_confirmation": 1.5,
                "schedule": {
                    "premarket_start": "04:00",
                    "market_open": "09:30",
                    "market_close": "16:00",
                    "afterhours_end": "20:00",
                },
            },
            # Logging Configuration
            "logging": {
                "level": "INFO",
                "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
                "file_enabled": True,
                "file_path": "./logs/trading.log",
                "max_file_size_mb": 10,
                "backup_count": 5,
            },
            # Performance Configuration
            "performance": {
                "batch_size": 25,
                "processing_threads": 4,
                "memory_limit_mb": 512,
                "enable_profiling": False,
            },
            # Development Configuration
            "development": {
                "test_mode": False,
                "debug_enabled": False,
                "mock_api_calls": False,
                "save_debug_data": False,
            },
        }

    def reload(self) -> None:
        """Reload configuration from all sources."""
        logger.debug("Reloading configuration")

        # Start with defaults
        self._config = self._deep_copy_dict(self._defaults)

        # Load from file if specified
        if self.config_file:
            self._load_from_file()

        # Override with environment variables
        self._load_from_env()

        logger.info("Configuration reloaded successfully")

    def _load_from_file(self) -> None:
        """Load configuration from file."""
        if not self.config_file or not os.path.exists(self.config_file):
            return

        try:
            file_path = Path(self.config_file)
            current_mtime = file_path.stat().st_mtime

            # Check if file has changed
            if self._file_mtime is not None and current_mtime == self._file_mtime:
                return

            self._file_mtime = current_mtime

            with open(self.config_file, "r") as f:
                if self.config_file.endswith((".yml", ".yaml")):
                    file_config = yaml.safe_load(f)
                else:
                    file_config = json.load(f)

            # Merge file config with defaults
            self._merge_config(self._config, file_config)
            logger.debug(f"Loaded configuration from {self.config_file}")

        except Exception as e:
            logger.error(f"Error loading config file {self.config_file}: {e}")

    def _load_from_env(self) -> None:
        """Load configuration from environment variables."""
        for key, value in os.environ.items():
            if key.startswith(self.env_prefix):
                # Convert environment variable to config path
                config_key = key[len(self.env_prefix) :].lower()
                config_path = config_key.split("_")

                # Convert value to appropriate type
                converted_value = self._convert_env_value(value)

                # Set nested configuration value
                self._set_nested_value(self._config, config_path, converted_value)
                logger.debug(f"Set config from env: {config_path} = {converted_value}")

    def _convert_env_value(self, value: str) -> Any:
        """Convert environment variable string to appropriate type."""
        # Try boolean
        if value.lower() in ("true", "false"):
            return value.lower() == "true"

        # Try integer
        try:
            return int(value)
        except ValueError:
            pass

        # Try float
        try:
            return float(value)
        except ValueError:
            pass

        # Try JSON
        try:
            return json.loads(value)
        except (json.JSONDecodeError, ValueError):
            pass

        # Return as string
        return value

    def _set_nested_value(
        self, config: Dict[str, Any], path: List[str], value: Any
    ) -> None:
        """Set a nested configuration value."""
        current = config
        for key in path[:-1]:
            if key not in current:
                current[key] = {}
            current = current[key]
        current[path[-1]] = value

    def _merge_config(self, base: Dict[str, Any], override: Dict[str, Any]) -> None:
        """Merge override configuration into base configuration."""
        for key, value in override.items():
            if key in base and isinstance(base[key], dict) and isinstance(value, dict):
                self._merge_config(base[key], value)
            else:
                base[key] = value

    def _deep_copy_dict(self, d: Dict[str, Any]) -> Dict[str, Any]:
        """Deep copy a dictionary."""
        import copy

        return copy.deepcopy(d)

    def get(self, key: str, default: Any = None) -> Any:
        """
        Get a configuration value using dot notation.

        Args:
            key: Configuration key (e.g., 'api.rate_limit')
            default: Default value if key not found

        Returns:
            Configuration value
        """
        # Check for hot-reload
        if self.auto_reload and self.config_file:
            self._check_file_changes()

        keys = key.split(".")
        current = self._config

        try:
            for k in keys:
                current = current[k]
            return current
        except (KeyError, TypeError):
            return default

    def set(self, key: str, value: Any) -> None:
        """
        Set a configuration value using dot notation.

        Args:
            key: Configuration key (e.g., 'api.rate_limit')
            value: Value to set
        """
        keys = key.split(".")
        self._set_nested_value(self._config, keys, value)

    def get_section(self, section: str) -> Dict[str, Any]:
        """
        Get an entire configuration section.

        Args:
            section: Section name

        Returns:
            Section configuration dictionary
        """
        return self.get(section, {})

    def _check_file_changes(self) -> None:
        """Check if configuration file has changed and reload if needed."""
        if not self.config_file or not os.path.exists(self.config_file):
            return

        try:
            current_mtime = Path(self.config_file).stat().st_mtime
            if self._file_mtime is None or current_mtime > self._file_mtime:
                logger.info("Configuration file changed, reloading")
                self.reload()
        except Exception as e:
            logger.error(f"Error checking file changes: {e}")

    def save_to_file(self, file_path: str, format: str = "json") -> None:
        """
        Save current configuration to file.

        Args:
            file_path: Path to save configuration
            format: File format ('json' or 'yaml')
        """
        try:
            os.makedirs(os.path.dirname(file_path), exist_ok=True)

            with open(file_path, "w") as f:
                if format.lower() == "yaml":
                    yaml.dump(self._config, f, default_flow_style=False, indent=2)
                else:
                    json.dump(self._config, f, indent=2)

            logger.info(f"Configuration saved to {file_path}")

        except Exception as e:
            logger.error(f"Error saving configuration to {file_path}: {e}")

    def validate(self) -> List[str]:
        """
        Validate current configuration.

        Returns:
            List of validation errors
        """
        errors = []

        # Validate API configuration
        if not self.get("api.alpha_vantage_key"):
            errors.append("API key not configured")

        # Validate storage configuration
        storage_config = self.get_section("storage")
        if storage_config.get("spaces_access_key") and not storage_config.get(
            "spaces_secret_key"
        ):
            errors.append("Spaces secret key missing")

        # Validate trading configuration
        risk_per_trade = self.get("trading.default_risk_per_trade", 0)
        if risk_per_trade <= 0 or risk_per_trade > 0.1:  # Max 10%
            errors.append("Risk per trade should be between 0% and 10%")

        return errors

    def get_all(self) -> Dict[str, Any]:
        """Get all configuration values."""
        return self._deep_copy_dict(self._config)

    def reset_to_defaults(self) -> None:
        """Reset configuration to defaults."""
        self._config = self._deep_copy_dict(self._defaults)
        logger.info("Configuration reset to defaults")


# Global configuration instance
_config_manager: Optional[ConfigurationManager] = None


def get_config() -> ConfigurationManager:
    """Get the global configuration manager instance."""
    global _config_manager
    if _config_manager is None:
        # Look for config file in standard locations
        config_file = None
        for path in ["./config.json", "./config.yaml", "./config.yml"]:
            if os.path.exists(path):
                config_file = path
                break

        _config_manager = ConfigurationManager(
            config_file=config_file, auto_reload=True
        )

    return _config_manager


def configure(config_file: Optional[str] = None, **kwargs) -> ConfigurationManager:
    """
    Configure the global configuration manager.

    Args:
        config_file: Path to configuration file
        **kwargs: Additional configuration manager options

    Returns:
        Configuration manager instance
    """
    global _config_manager
    _config_manager = ConfigurationManager(config_file=config_file, **kwargs)
    return _config_manager


# Convenience functions
def get_value(key: str, default: Any = None) -> Any:
    """Get a configuration value."""
    return get_config().get(key, default)


def set_value(key: str, value: Any) -> None:
    """Set a configuration value."""
    get_config().set(key, value)


def get_section(section: str) -> Dict[str, Any]:
    """Get a configuration section."""
    return get_config().get_section(section)
