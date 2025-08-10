"""
Dependency injection container for the trading system.

This module provides a simple but powerful dependency injection system
that allows for loose coupling between components and easier testing.
"""

import logging
from typing import Any, Callable, Dict, Optional, Type, TypeVar, Union
import inspect
from functools import wraps

logger = logging.getLogger(__name__)

T = TypeVar("T")


class DIContainer:
    """Simple dependency injection container."""

    def __init__(self):
        self._services: Dict[str, Any] = {}
        self._factories: Dict[str, Callable[[], Any]] = {}
        self._singletons: Dict[str, Any] = {}
        self._types: Dict[Type, str] = {}

    def register_singleton(
        self, service_type: Type[T], instance: T, name: Optional[str] = None
    ) -> None:
        """
        Register a singleton instance.

        Args:
            service_type: The type/interface for the service
            instance: The instance to register
            name: Optional name for the service
        """
        service_name = name or service_type.__name__
        self._singletons[service_name] = instance
        self._types[service_type] = service_name
        logger.debug(f"Registered singleton: {service_name}")

    def register_factory(
        self,
        service_type: Type[T],
        factory: Callable[[], T],
        name: Optional[str] = None,
    ) -> None:
        """
        Register a factory function for creating instances.

        Args:
            service_type: The type/interface for the service
            factory: Factory function to create instances
            name: Optional name for the service
        """
        service_name = name or service_type.__name__
        self._factories[service_name] = factory
        self._types[service_type] = service_name
        logger.debug(f"Registered factory: {service_name}")

    def register_class(
        self,
        service_type: Type[T],
        implementation: Type[T],
        name: Optional[str] = None,
        singleton: bool = False,
    ) -> None:
        """
        Register a class for automatic instantiation.

        Args:
            service_type: The type/interface for the service
            implementation: The concrete implementation class
            name: Optional name for the service
            singleton: Whether to create as singleton
        """
        service_name = name or service_type.__name__

        def factory():
            return self._create_instance(implementation)

        if singleton:
            # Create singleton immediately
            instance = factory()
            self.register_singleton(service_type, instance, name)
        else:
            self.register_factory(service_type, factory, name)

    def get(self, service_type: Type[T], name: Optional[str] = None) -> T:
        """
        Get a service instance.

        Args:
            service_type: The type/interface of the service
            name: Optional name for the service

        Returns:
            Service instance

        Raises:
            ValueError: If service is not registered
        """
        service_name = name or self._types.get(service_type, service_type.__name__)

        # Check singletons first
        if service_name in self._singletons:
            return self._singletons[service_name]

        # Check factories
        if service_name in self._factories:
            return self._factories[service_name]()

        # Try direct lookup
        if service_name in self._services:
            return self._services[service_name]

        raise ValueError(f"Service not registered: {service_name}")

    def _create_instance(self, cls: Type[T]) -> T:
        """
        Create an instance with automatic dependency injection.

        Args:
            cls: Class to instantiate

        Returns:
            Instance with dependencies injected
        """
        # Get constructor signature
        sig = inspect.signature(cls.__init__)
        kwargs = {}

        # Resolve dependencies
        for param_name, param in sig.parameters.items():
            if param_name == "self":
                continue

            # Skip parameters with default values for now
            if param.default != inspect.Parameter.empty:
                continue

            # Try to resolve the parameter type
            if param.annotation != inspect.Parameter.empty:
                try:
                    dependency = self.get(param.annotation)
                    kwargs[param_name] = dependency
                except ValueError:
                    logger.warning(
                        f"Could not resolve dependency {param_name} "
                        f"of type {param.annotation} for {cls.__name__}"
                    )

        return cls(**kwargs)

    def configure_from_dict(self, config: Dict[str, Any]) -> None:
        """
        Configure the container from a dictionary.

        Args:
            config: Configuration dictionary
        """
        for service_name, service_config in config.items():
            if isinstance(service_config, dict):
                service_type = service_config.get("type")
                implementation = service_config.get("implementation")
                singleton = service_config.get("singleton", False)

                if service_type and implementation:
                    self.register_class(
                        service_type, implementation, service_name, singleton
                    )
            else:
                # Direct instance registration
                self._services[service_name] = service_config

    def clear(self) -> None:
        """Clear all registered services."""
        self._services.clear()
        self._factories.clear()
        self._singletons.clear()
        self._types.clear()
        logger.debug("Container cleared")

    def get_registered_services(self) -> Dict[str, str]:
        """
        Get list of registered services.

        Returns:
            Dictionary mapping service names to their types
        """
        services = {}

        for name in self._singletons:
            services[name] = "singleton"

        for name in self._factories:
            services[name] = "factory"

        for name in self._services:
            services[name] = "instance"

        return services


# Global container instance
_container: Optional[DIContainer] = None


def get_container() -> DIContainer:
    """Get the global DI container instance."""
    global _container
    if _container is None:
        _container = DIContainer()
    return _container


def inject(service_type: Type[T], name: Optional[str] = None) -> T:
    """
    Inject a dependency.

    Args:
        service_type: Type of service to inject
        name: Optional service name

    Returns:
        Service instance
    """
    container = get_container()
    return container.get(service_type, name)


def injectable(cls: Type[T]) -> Type[T]:
    """
    Decorator to mark a class as injectable.

    This decorator modifies the class constructor to automatically
    resolve dependencies from the DI container.
    """
    original_init = cls.__init__

    @wraps(original_init)
    def new_init(self, *args, **kwargs):
        container = get_container()

        # Get constructor signature
        sig = inspect.signature(original_init)
        bound_args = sig.bind_partial(self, *args, **kwargs)

        # Try to resolve missing dependencies
        for param_name, param in sig.parameters.items():
            if param_name == "self":
                continue

            if param_name not in bound_args.arguments:
                if param.annotation != inspect.Parameter.empty:
                    try:
                        dependency = container.get(param.annotation)
                        bound_args.arguments[param_name] = dependency
                    except ValueError:
                        # If we can't resolve and there's no default, let the original error occur
                        if param.default == inspect.Parameter.empty:
                            logger.warning(
                                f"Could not resolve dependency {param_name} "
                                f"of type {param.annotation} for {cls.__name__}"
                            )

        original_init(*bound_args.args, **bound_args.kwargs)

    cls.__init__ = new_init
    return cls


def configure_default_services():
    """Configure default services in the container."""
    container = get_container()

    # Import and register concrete implementations
    try:
        from utils.async_client import AsyncAlphaVantageClient
        from utils.cache import TieredCache
        from utils.pipeline import DataPipeline
        from core.interfaces import DataFetcher

        # Register services
        container.register_class(DataFetcher, AsyncAlphaVantageClient, singleton=False)

        # Register cache as singleton
        cache = TieredCache()
        container.register_singleton(TieredCache, cache)

        # Register pipeline
        container.register_class(DataPipeline, DataPipeline, singleton=True)

        logger.info("Default services configured")

    except ImportError as e:
        logger.warning(f"Could not configure some default services: {e}")


# Auto-configure on import
configure_default_services()
