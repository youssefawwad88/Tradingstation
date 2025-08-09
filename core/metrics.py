"""
Metrics collection and monitoring system.

This module provides comprehensive metrics collection for performance
monitoring, alerting, and system optimization.
"""

import time
from collections import defaultdict, deque
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Callable
from threading import Lock
import json

from core.logging_system import get_logger

logger = get_logger(__name__)


class MetricType:
    """Metric type constants."""

    COUNTER = "counter"
    GAUGE = "gauge"
    HISTOGRAM = "histogram"
    TIMER = "timer"


class Metric:
    """Base metric class."""

    def __init__(
        self,
        name: str,
        metric_type: str,
        description: str = "",
        tags: Dict[str, str] = None,
    ):
        self.name = name
        self.type = metric_type
        self.description = description
        self.tags = tags or {}
        self.created_at = datetime.now()
        self.last_updated = self.created_at


class Counter(Metric):
    """Counter metric that only increases."""

    def __init__(self, name: str, description: str = "", tags: Dict[str, str] = None):
        super().__init__(name, MetricType.COUNTER, description, tags)
        self.value = 0
        self._lock = Lock()

    def increment(self, amount: float = 1.0) -> None:
        """Increment the counter."""
        with self._lock:
            self.value += amount
            self.last_updated = datetime.now()

    def get_value(self) -> float:
        """Get current counter value."""
        return self.value

    def reset(self) -> None:
        """Reset counter to zero."""
        with self._lock:
            self.value = 0
            self.last_updated = datetime.now()


class Gauge(Metric):
    """Gauge metric that can go up or down."""

    def __init__(self, name: str, description: str = "", tags: Dict[str, str] = None):
        super().__init__(name, MetricType.GAUGE, description, tags)
        self.value = 0.0
        self._lock = Lock()

    def set(self, value: float) -> None:
        """Set the gauge value."""
        with self._lock:
            self.value = value
            self.last_updated = datetime.now()

    def increment(self, amount: float = 1.0) -> None:
        """Increment the gauge."""
        with self._lock:
            self.value += amount
            self.last_updated = datetime.now()

    def decrement(self, amount: float = 1.0) -> None:
        """Decrement the gauge."""
        with self._lock:
            self.value -= amount
            self.last_updated = datetime.now()

    def get_value(self) -> float:
        """Get current gauge value."""
        return self.value


class Histogram(Metric):
    """Histogram metric for tracking distributions."""

    def __init__(
        self,
        name: str,
        description: str = "",
        tags: Dict[str, str] = None,
        max_samples: int = 1000,
    ):
        super().__init__(name, MetricType.HISTOGRAM, description, tags)
        self.samples = deque(maxlen=max_samples)
        self._lock = Lock()

    def observe(self, value: float) -> None:
        """Record a value."""
        with self._lock:
            self.samples.append((time.time(), value))
            self.last_updated = datetime.now()

    def get_stats(self) -> Dict[str, float]:
        """Get histogram statistics."""
        if not self.samples:
            return {"count": 0}

        values = [v for _, v in self.samples]
        values.sort()

        count = len(values)
        total = sum(values)
        mean = total / count

        # Percentiles
        p50_idx = int(count * 0.5)
        p95_idx = int(count * 0.95)
        p99_idx = int(count * 0.99)

        return {
            "count": count,
            "sum": total,
            "mean": mean,
            "min": values[0],
            "max": values[-1],
            "p50": values[p50_idx],
            "p95": values[p95_idx] if count > 20 else values[-1],
            "p99": values[p99_idx] if count > 100 else values[-1],
        }


class Timer(Metric):
    """Timer metric for measuring durations."""

    def __init__(self, name: str, description: str = "", tags: Dict[str, str] = None):
        super().__init__(name, MetricType.TIMER, description, tags)
        self.histogram = Histogram(f"{name}_duration", description, tags)
        self.count = Counter(f"{name}_count", f"{description} - call count", tags)

    def time(self):
        """Context manager for timing operations."""
        return TimerContext(self)

    def record(self, duration: float) -> None:
        """Record a duration."""
        self.histogram.observe(duration)
        self.count.increment()
        self.last_updated = datetime.now()

    def get_stats(self) -> Dict[str, Any]:
        """Get timer statistics."""
        stats = self.histogram.get_stats()
        stats["total_calls"] = self.count.get_value()
        return stats


class TimerContext:
    """Context manager for timing operations."""

    def __init__(self, timer: Timer):
        self.timer = timer
        self.start_time = None

    def __enter__(self):
        self.start_time = time.time()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.start_time is not None:
            duration = time.time() - self.start_time
            self.timer.record(duration)


class MetricsCollector:
    """Central metrics collection system."""

    def __init__(self):
        self.metrics: Dict[str, Metric] = {}
        self._lock = Lock()

        # Initialize common trading metrics
        self._initialize_trading_metrics()

    def _initialize_trading_metrics(self):
        """Initialize common trading system metrics."""
        # API metrics
        self.register_timer(
            "api_call_duration", "Duration of API calls", {"component": "api"}
        )
        self.register_counter(
            "api_calls_total", "Total API calls", {"component": "api"}
        )
        self.register_counter(
            "api_errors_total", "Total API errors", {"component": "api"}
        )
        self.register_gauge(
            "api_rate_limit_remaining", "API rate limit remaining", {"component": "api"}
        )

        # Data processing metrics
        self.register_counter("tickers_processed_total", "Total tickers processed")
        self.register_timer("data_fetch_duration", "Data fetch duration")
        self.register_gauge("cache_hit_rate", "Cache hit rate percentage")
        self.register_gauge("active_tickers", "Number of active tickers")

        # Trading metrics
        self.register_counter("signals_generated_total", "Total signals generated")
        self.register_counter("trades_executed_total", "Total trades executed")
        self.register_gauge("portfolio_value", "Current portfolio value")
        self.register_gauge("daily_pnl", "Daily P&L")

        # System metrics
        self.register_gauge("memory_usage_percent", "Memory usage percentage")
        self.register_gauge("disk_usage_percent", "Disk usage percentage")
        self.register_timer("screener_execution_time", "Screener execution time")

    def register_counter(
        self, name: str, description: str = "", tags: Dict[str, str] = None
    ) -> Counter:
        """Register a counter metric."""
        with self._lock:
            if name in self.metrics:
                if isinstance(self.metrics[name], Counter):
                    return self.metrics[name]
                else:
                    raise ValueError(
                        f"Metric {name} already exists with different type"
                    )

            counter = Counter(name, description, tags)
            self.metrics[name] = counter
            return counter

    def register_gauge(
        self, name: str, description: str = "", tags: Dict[str, str] = None
    ) -> Gauge:
        """Register a gauge metric."""
        with self._lock:
            if name in self.metrics:
                if isinstance(self.metrics[name], Gauge):
                    return self.metrics[name]
                else:
                    raise ValueError(
                        f"Metric {name} already exists with different type"
                    )

            gauge = Gauge(name, description, tags)
            self.metrics[name] = gauge
            return gauge

    def register_histogram(
        self, name: str, description: str = "", tags: Dict[str, str] = None
    ) -> Histogram:
        """Register a histogram metric."""
        with self._lock:
            if name in self.metrics:
                if isinstance(self.metrics[name], Histogram):
                    return self.metrics[name]
                else:
                    raise ValueError(
                        f"Metric {name} already exists with different type"
                    )

            histogram = Histogram(name, description, tags)
            self.metrics[name] = histogram
            return histogram

    def register_timer(
        self, name: str, description: str = "", tags: Dict[str, str] = None
    ) -> Timer:
        """Register a timer metric."""
        with self._lock:
            if name in self.metrics:
                if isinstance(self.metrics[name], Timer):
                    return self.metrics[name]
                else:
                    raise ValueError(
                        f"Metric {name} already exists with different type"
                    )

            timer = Timer(name, description, tags)
            self.metrics[name] = timer
            return timer

    def get_metric(self, name: str) -> Optional[Metric]:
        """Get a metric by name."""
        return self.metrics.get(name)

    def increment_counter(
        self, name: str, amount: float = 1.0, tags: Dict[str, str] = None
    ) -> None:
        """Increment a counter metric."""
        metric = self.get_metric(name)
        if isinstance(metric, Counter):
            metric.increment(amount)
        else:
            # Auto-create counter if it doesn't exist
            counter = self.register_counter(name, tags=tags)
            counter.increment(amount)

    def set_gauge(self, name: str, value: float, tags: Dict[str, str] = None) -> None:
        """Set a gauge metric value."""
        metric = self.get_metric(name)
        if isinstance(metric, Gauge):
            metric.set(value)
        else:
            # Auto-create gauge if it doesn't exist
            gauge = self.register_gauge(name, tags=tags)
            gauge.set(value)

    def observe_histogram(
        self, name: str, value: float, tags: Dict[str, str] = None
    ) -> None:
        """Record a histogram observation."""
        metric = self.get_metric(name)
        if isinstance(metric, Histogram):
            metric.observe(value)
        else:
            # Auto-create histogram if it doesn't exist
            histogram = self.register_histogram(name, tags=tags)
            histogram.observe(value)

    def time_operation(self, name: str, tags: Dict[str, str] = None) -> TimerContext:
        """Get a timer context for an operation."""
        metric = self.get_metric(name)
        if isinstance(metric, Timer):
            return metric.time()
        else:
            # Auto-create timer if it doesn't exist
            timer = self.register_timer(name, tags=tags)
            return timer.time()

    def get_all_metrics(self) -> Dict[str, Any]:
        """Get all metrics with their current values."""
        result = {}

        for name, metric in self.metrics.items():
            if isinstance(metric, Counter):
                result[name] = {
                    "type": metric.type,
                    "value": metric.get_value(),
                    "description": metric.description,
                    "tags": metric.tags,
                    "last_updated": metric.last_updated.isoformat(),
                }
            elif isinstance(metric, Gauge):
                result[name] = {
                    "type": metric.type,
                    "value": metric.get_value(),
                    "description": metric.description,
                    "tags": metric.tags,
                    "last_updated": metric.last_updated.isoformat(),
                }
            elif isinstance(metric, Histogram):
                result[name] = {
                    "type": metric.type,
                    "stats": metric.get_stats(),
                    "description": metric.description,
                    "tags": metric.tags,
                    "last_updated": metric.last_updated.isoformat(),
                }
            elif isinstance(metric, Timer):
                result[name] = {
                    "type": metric.type,
                    "stats": metric.get_stats(),
                    "description": metric.description,
                    "tags": metric.tags,
                    "last_updated": metric.last_updated.isoformat(),
                }

        return result

    def export_prometheus(self) -> str:
        """Export metrics in Prometheus format."""
        lines = []

        for name, metric in self.metrics.items():
            # Add help comment
            lines.append(f"# HELP {name} {metric.description}")
            lines.append(f"# TYPE {name} {metric.type}")

            # Add metric value(s)
            if isinstance(metric, (Counter, Gauge)):
                tags_str = self._format_prometheus_tags(metric.tags)
                lines.append(f"{name}{tags_str} {metric.get_value()}")
            elif isinstance(metric, Histogram):
                stats = metric.get_stats()
                tags_str = self._format_prometheus_tags(metric.tags)
                for stat_name, value in stats.items():
                    lines.append(f"{name}_{stat_name}{tags_str} {value}")
            elif isinstance(metric, Timer):
                stats = metric.get_stats()
                tags_str = self._format_prometheus_tags(metric.tags)
                for stat_name, value in stats.items():
                    lines.append(f"{name}_{stat_name}{tags_str} {value}")

        return "\n".join(lines)

    def _format_prometheus_tags(self, tags: Dict[str, str]) -> str:
        """Format tags for Prometheus export."""
        if not tags:
            return ""

        tag_pairs = [f'{key}="{value}"' for key, value in tags.items()]
        return "{" + ",".join(tag_pairs) + "}"


# Global metrics collector
_metrics_collector: Optional[MetricsCollector] = None


def get_metrics() -> MetricsCollector:
    """Get the global metrics collector."""
    global _metrics_collector
    if _metrics_collector is None:
        _metrics_collector = MetricsCollector()
    return _metrics_collector


# Convenience functions
def increment_counter(name: str, amount: float = 1.0, **tags) -> None:
    """Increment a counter metric."""
    get_metrics().increment_counter(name, amount, tags)


def set_gauge(name: str, value: float, **tags) -> None:
    """Set a gauge metric."""
    get_metrics().set_gauge(name, value, tags)


def observe_histogram(name: str, value: float, **tags) -> None:
    """Record a histogram observation."""
    get_metrics().observe_histogram(name, value, tags)


def time_operation(name: str, **tags) -> TimerContext:
    """Time an operation."""
    return get_metrics().time_operation(name, tags)


# Decorator for timing functions
def timed(metric_name: str = None, **tags):
    """Decorator to time function execution."""

    def decorator(func):
        def wrapper(*args, **kwargs):
            name = metric_name or f"{func.__module__}.{func.__name__}"
            with time_operation(name, **tags):
                return func(*args, **kwargs)

        return wrapper

    return decorator
