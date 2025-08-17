"""
Plugin system for extensible screeners and strategies.

This module provides a flexible plugin architecture that allows new trading
strategies and screeners to be added without modifying the core system.
"""

import importlib
import inspect
import logging
import os
import pkgutil
from pathlib import Path
from typing import Any, Dict, List, Optional, Type

from core.di_container import get_container
from core.interfaces import Screener

logger = logging.getLogger(__name__)


class PluginRegistry:
    """Registry for managing trading strategy plugins."""

    def __init__(self):
        self._screeners: Dict[str, Type[Screener]] = {}
        self._instances: Dict[str, Screener] = {}
        self._metadata: Dict[str, Dict[str, Any]] = {}

    def register_screener(
        self,
        screener_class: Type[Screener],
        name: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        Register a screener plugin.

        Args:
            screener_class: Screener class to register
            name: Optional name override
            metadata: Optional metadata about the screener
        """
        screener_name = name or screener_class.__name__.lower()

        if not issubclass(screener_class, Screener):
            raise ValueError(f"{screener_class} must implement Screener interface")

        self._screeners[screener_name] = screener_class
        self._metadata[screener_name] = metadata or {}

        logger.info(f"Registered screener plugin: {screener_name}")

    def get_screener(self, name: str) -> Screener:
        """
        Get a screener instance by name.

        Args:
            name: Name of the screener

        Returns:
            Screener instance

        Raises:
            ValueError: If screener is not registered
        """
        if name not in self._screeners:
            raise ValueError(f"Screener not registered: {name}")

        # Return cached instance if available
        if name in self._instances:
            return self._instances[name]

        # Create new instance
        screener_class = self._screeners[name]

        # Try to use DI container for instantiation
        try:
            container = get_container()
            instance = container._create_instance(screener_class)
        except Exception:
            # Fallback to direct instantiation
            instance = screener_class()

        self._instances[name] = instance
        return instance

    def list_screeners(self) -> List[str]:
        """
        Get list of registered screener names.

        Returns:
            List of screener names
        """
        return list(self._screeners.keys())

    def get_screener_metadata(self, name: str) -> Dict[str, Any]:
        """
        Get metadata for a screener.

        Args:
            name: Name of the screener

        Returns:
            Metadata dictionary
        """
        return self._metadata.get(name, {})

    def unregister_screener(self, name: str) -> None:
        """
        Unregister a screener.

        Args:
            name: Name of the screener to unregister
        """
        if name in self._screeners:
            del self._screeners[name]
        if name in self._instances:
            del self._instances[name]
        if name in self._metadata:
            del self._metadata[name]

        logger.info(f"Unregistered screener: {name}")

    def reload_screener(self, name: str) -> None:
        """
        Reload a screener (useful for development).

        Args:
            name: Name of the screener to reload
        """
        if name in self._instances:
            del self._instances[name]

        # Recreate instance on next access
        logger.info(f"Marked screener for reload: {name}")

    def discover_and_load_plugins(self, plugin_directories: List[str]) -> None:
        """
        Discover and load plugins from directories.

        Args:
            plugin_directories: List of directories to search for plugins
        """
        for directory in plugin_directories:
            self._load_plugins_from_directory(directory)

    def _load_plugins_from_directory(self, directory: str) -> None:
        """Load plugins from a specific directory."""
        if not os.path.exists(directory):
            logger.warning(f"Plugin directory does not exist: {directory}")
            return

        logger.info(f"Loading plugins from: {directory}")

        # Add directory to Python path temporarily
        import sys

        if directory not in sys.path:
            sys.path.insert(0, directory)

        try:
            # Discover Python modules in directory
            for finder, name, ispkg in pkgutil.iter_modules([directory]):
                try:
                    module = importlib.import_module(name)
                    self._extract_screeners_from_module(module)
                except Exception as e:
                    logger.error(f"Error loading plugin module {name}: {e}")

        finally:
            # Remove from path
            if directory in sys.path:
                sys.path.remove(directory)

    def _extract_screeners_from_module(self, module) -> None:
        """Extract screener classes from a module."""
        for name, obj in inspect.getmembers(module, inspect.isclass):
            if (
                issubclass(obj, Screener)
                and obj != Screener
                and not inspect.isabstract(obj)
            ):
                # Check for plugin metadata
                metadata = getattr(obj, "__plugin_metadata__", {})
                self.register_screener(obj, metadata=metadata)


class PluginManager:
    """High-level plugin manager."""

    def __init__(self):
        self.registry = PluginRegistry()
        self._loaded_directories: List[str] = []

    def load_builtin_screeners(self) -> None:
        """Load built-in screeners from the screeners directory."""
        screeners_dir = os.path.join(
            os.path.dirname(os.path.dirname(__file__)), "screeners"
        )

        if os.path.exists(screeners_dir):
            self.registry.discover_and_load_plugins([screeners_dir])
            self._loaded_directories.append(screeners_dir)

    def load_custom_screeners(self, custom_directories: List[str]) -> None:
        """
        Load custom screeners from specified directories.

        Args:
            custom_directories: List of directories containing custom screeners
        """
        for directory in custom_directories:
            if directory not in self._loaded_directories:
                self.registry.discover_and_load_plugins([directory])
                self._loaded_directories.append(directory)

    def get_available_screeners(self) -> Dict[str, Dict[str, Any]]:
        """
        Get information about all available screeners.

        Returns:
            Dictionary mapping screener names to their metadata
        """
        result = {}
        for name in self.registry.list_screeners():
            metadata = self.registry.get_screener_metadata(name)

            # Get screener instance to access properties
            try:
                screener = self.registry.get_screener(name)
                result[name] = {
                    "name": screener.name,
                    "description": screener.description,
                    "metadata": metadata,
                }
            except Exception as e:
                logger.error(f"Error getting screener info for {name}: {e}")
                result[name] = {
                    "name": name,
                    "description": "Error loading screener",
                    "metadata": metadata,
                    "error": str(e),
                }

        return result

    async def run_screener(
        self, screener_name: str, tickers: List[str], **kwargs
    ) -> List[Dict[str, Any]]:
        """
        Run a specific screener.

        Args:
            screener_name: Name of the screener to run
            tickers: List of tickers to scan
            **kwargs: Additional parameters for the screener

        Returns:
            List of signals/opportunities found
        """
        screener = self.registry.get_screener(screener_name)

        logger.info(f"Running screener: {screener_name} on {len(tickers)} tickers")

        try:
            results = await screener.scan(tickers, **kwargs)
            logger.info(f"Screener {screener_name} found {len(results)} opportunities")
            return results
        except Exception as e:
            logger.error(f"Error running screener {screener_name}: {e}")
            raise

    async def run_all_screeners(
        self, tickers: List[str], **kwargs
    ) -> Dict[str, List[Dict[str, Any]]]:
        """
        Run all registered screeners.

        Args:
            tickers: List of tickers to scan
            **kwargs: Additional parameters for screeners

        Returns:
            Dictionary mapping screener names to their results
        """
        results = {}

        for screener_name in self.registry.list_screeners():
            try:
                screener_results = await self.run_screener(
                    screener_name, tickers, **kwargs
                )
                results[screener_name] = screener_results
            except Exception as e:
                logger.error(f"Error running screener {screener_name}: {e}")
                results[screener_name] = []

        return results


# Decorator for marking screener plugins
def screener_plugin(
    name: Optional[str] = None,
    description: Optional[str] = None,
    version: Optional[str] = None,
    author: Optional[str] = None,
    **metadata,
):
    """
    Decorator to mark a class as a screener plugin.

    Args:
        name: Plugin name
        description: Plugin description
        version: Plugin version
        author: Plugin author
        **metadata: Additional metadata
    """

    def decorator(cls):
        plugin_metadata = {
            "name": name or cls.__name__,
            "description": description,
            "version": version,
            "author": author,
            **metadata,
        }
        cls.__plugin_metadata__ = plugin_metadata
        return cls

    return decorator


# Global plugin manager
_plugin_manager: Optional[PluginManager] = None


def get_plugin_manager() -> PluginManager:
    """Get the global plugin manager instance."""
    global _plugin_manager
    if _plugin_manager is None:
        _plugin_manager = PluginManager()
        _plugin_manager.load_builtin_screeners()
    return _plugin_manager


def register_screener(screener_class: Type[Screener], **kwargs) -> None:
    """
    Convenience function to register a screener.

    Args:
        screener_class: Screener class to register
        **kwargs: Additional arguments for registration
    """
    manager = get_plugin_manager()
    manager.registry.register_screener(screener_class, **kwargs)
