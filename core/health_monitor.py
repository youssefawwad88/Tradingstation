"""
Health check system for monitoring system components.

This module provides health monitoring capabilities for all system
components with detailed status reporting and alerting.
"""

import asyncio
import time
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Callable, Dict, List, Optional

import aiohttp

from core.config_manager import get_config
from core.logging_system import get_logger

logger = get_logger(__name__)


class HealthStatus(Enum):
    """Health check status levels."""

    HEALTHY = "healthy"
    WARNING = "warning"
    CRITICAL = "critical"
    UNKNOWN = "unknown"


class HealthCheck:
    """Individual health check definition."""

    def __init__(
        self,
        name: str,
        check_func: Callable[[], Any],
        timeout: float = 10.0,
        interval: float = 60.0,
        critical: bool = False,
        description: str = "",
    ):
        self.name = name
        self.check_func = check_func
        self.timeout = timeout
        self.interval = interval
        self.critical = critical
        self.description = description
        self.last_check: Optional[datetime] = None
        self.last_status = HealthStatus.UNKNOWN
        self.last_error: Optional[str] = None
        self.last_duration: float = 0.0

    async def run(self) -> Dict[str, Any]:
        """Run the health check and return results."""
        start_time = time.time()
        self.last_check = datetime.now()

        try:
            # Run check with timeout
            if asyncio.iscoroutinefunction(self.check_func):
                result = await asyncio.wait_for(self.check_func(), timeout=self.timeout)
            else:
                result = await asyncio.get_event_loop().run_in_executor(
                    None, self.check_func
                )

            self.last_duration = time.time() - start_time

            # Determine status from result
            if isinstance(result, bool):
                self.last_status = (
                    HealthStatus.HEALTHY if result else HealthStatus.CRITICAL
                )
                details = {"success": result}
            elif isinstance(result, dict):
                # Detailed result with status
                self.last_status = HealthStatus(result.get("status", "unknown"))
                details = result
            else:
                self.last_status = HealthStatus.HEALTHY
                details = {"result": result}

            self.last_error = None

        except asyncio.TimeoutError:
            self.last_duration = time.time() - start_time
            self.last_status = HealthStatus.CRITICAL
            self.last_error = f"Check timed out after {self.timeout}s"
            details = {"error": self.last_error}

        except Exception as e:
            self.last_duration = time.time() - start_time
            self.last_status = HealthStatus.CRITICAL
            self.last_error = str(e)
            details = {"error": self.last_error}

        return {
            "name": self.name,
            "status": self.last_status.value,
            "timestamp": self.last_check.isoformat(),
            "duration": self.last_duration,
            "critical": self.critical,
            "description": self.description,
            "details": details,
        }


class HealthMonitor:
    """Health monitoring system for all components."""

    def __init__(self):
        self.checks: Dict[str, HealthCheck] = {}
        self.running = False
        self.monitor_task: Optional[asyncio.Task] = None
        self.last_overall_status = HealthStatus.UNKNOWN

    def register_check(self, health_check: HealthCheck) -> None:
        """Register a health check."""
        self.checks[health_check.name] = health_check
        logger.info(f"Registered health check: {health_check.name}")

    def remove_check(self, name: str) -> None:
        """Remove a health check."""
        if name in self.checks:
            del self.checks[name]
            logger.info(f"Removed health check: {name}")

    async def run_all_checks(self) -> Dict[str, Any]:
        """Run all health checks and return aggregated results."""
        if not self.checks:
            return {
                "status": HealthStatus.HEALTHY.value,
                "timestamp": datetime.now().isoformat(),
                "checks": {},
                "summary": {"total": 0, "healthy": 0, "warning": 0, "critical": 0},
            }

        # Run all checks concurrently
        check_tasks = [check.run() for check in self.checks.values()]
        results = await asyncio.gather(*check_tasks, return_exceptions=True)

        # Process results
        check_results = {}
        summary = {"total": 0, "healthy": 0, "warning": 0, "critical": 0, "unknown": 0}

        for result in results:
            if isinstance(result, Exception):
                logger.error(f"Health check failed with exception: {result}")
                continue

            check_results[result["name"]] = result
            summary["total"] += 1
            summary[result["status"]] += 1

        # Determine overall status
        overall_status = self._determine_overall_status(check_results)
        self.last_overall_status = overall_status

        return {
            "status": overall_status.value,
            "timestamp": datetime.now().isoformat(),
            "checks": check_results,
            "summary": summary,
        }

    def _determine_overall_status(self, check_results: Dict[str, Any]) -> HealthStatus:
        """Determine overall system health status."""
        if not check_results:
            return HealthStatus.UNKNOWN

        # Check for critical failures
        for result in check_results.values():
            if result["critical"] and result["status"] == HealthStatus.CRITICAL.value:
                return HealthStatus.CRITICAL

        # Check for any critical non-critical failures
        critical_count = sum(
            1
            for r in check_results.values()
            if r["status"] == HealthStatus.CRITICAL.value
        )
        warning_count = sum(
            1
            for r in check_results.values()
            if r["status"] == HealthStatus.WARNING.value
        )

        if critical_count > 0:
            return HealthStatus.CRITICAL
        elif warning_count > 0:
            return HealthStatus.WARNING
        else:
            return HealthStatus.HEALTHY

    async def start_monitoring(self, interval: float = 30.0) -> None:
        """Start continuous health monitoring."""
        if self.running:
            return

        self.running = True
        logger.info(f"Starting health monitoring with {interval}s interval")

        while self.running:
            try:
                results = await self.run_all_checks()

                # Log overall status
                status = results["status"]
                summary = results["summary"]

                logger.info(
                    f"Health check completed",
                    overall_status=status,
                    total_checks=summary["total"],
                    healthy=summary["healthy"],
                    warning=summary["warning"],
                    critical=summary["critical"],
                )

                # Log individual failures
                for name, result in results["checks"].items():
                    if result["status"] != HealthStatus.HEALTHY.value:
                        logger.warning(
                            f"Health check failed: {name}",
                            check_name=name,
                            status=result["status"],
                            error=result["details"].get("error"),
                            duration=result["duration"],
                        )

                await asyncio.sleep(interval)

            except Exception as e:
                logger.error(f"Error in health monitoring: {e}")
                await asyncio.sleep(interval)

    def stop_monitoring(self) -> None:
        """Stop health monitoring."""
        self.running = False
        if self.monitor_task:
            self.monitor_task.cancel()
        logger.info("Health monitoring stopped")


# Pre-defined health checks
async def check_api_connectivity() -> Dict[str, Any]:
    """Check Alpha Vantage API connectivity."""
    config = get_config()
    api_key = config.get("api.alpha_vantage_key")

    if not api_key:
        return {"status": "critical", "message": "API key not configured"}

    try:
        async with aiohttp.ClientSession() as session:
            url = "https://www.alphavantage.co/query"
            params = {"function": "GLOBAL_QUOTE", "symbol": "AAPL", "apikey": api_key}

            async with session.get(url, params=params, timeout=10) as response:
                data = await response.json()

                if "Error Message" in data:
                    return {"status": "critical", "message": "API returned error"}
                elif "Note" in data:
                    return {"status": "warning", "message": "API rate limited"}
                else:
                    return {"status": "healthy", "message": "API accessible"}

    except Exception as e:
        return {"status": "critical", "message": f"API unreachable: {e}"}


def check_disk_space() -> Dict[str, Any]:
    """Check available disk space."""
    import shutil

    try:
        config = get_config()
        data_dir = config.get("storage.local_data_dir", "./data")

        total, used, free = shutil.disk_usage(data_dir)
        free_percent = (free / total) * 100

        if free_percent < 5:
            return {"status": "critical", "free_percent": free_percent}
        elif free_percent < 15:
            return {"status": "warning", "free_percent": free_percent}
        else:
            return {"status": "healthy", "free_percent": free_percent}

    except Exception as e:
        return {"status": "critical", "message": f"Disk check failed: {e}"}


def check_memory_usage() -> Dict[str, Any]:
    """Check system memory usage."""
    import psutil

    try:
        memory = psutil.virtual_memory()
        memory_percent = memory.percent

        if memory_percent > 90:
            return {"status": "critical", "memory_percent": memory_percent}
        elif memory_percent > 80:
            return {"status": "warning", "memory_percent": memory_percent}
        else:
            return {"status": "healthy", "memory_percent": memory_percent}

    except Exception as e:
        return {"status": "unknown", "message": f"Memory check failed: {e}"}


def check_cache_health() -> Dict[str, Any]:
    """Check cache system health."""
    try:
        from utils.cache import get_cache

        cache = get_cache()
        stats = cache.stats()

        utilization = stats["memory"]["utilization"]

        if utilization > 0.95:
            return {"status": "warning", "utilization": utilization}
        else:
            return {"status": "healthy", "utilization": utilization, "stats": stats}

    except Exception as e:
        return {"status": "critical", "message": f"Cache check failed: {e}"}


# Global health monitor instance
_health_monitor: Optional[HealthMonitor] = None


def get_health_monitor() -> HealthMonitor:
    """Get the global health monitor instance."""
    global _health_monitor
    if _health_monitor is None:
        _health_monitor = HealthMonitor()

        # Register default checks
        _health_monitor.register_check(
            HealthCheck(
                "api_connectivity",
                check_api_connectivity,
                timeout=10.0,
                critical=True,
                description="Alpha Vantage API connectivity",
            )
        )

        _health_monitor.register_check(
            HealthCheck(
                "disk_space",
                check_disk_space,
                timeout=5.0,
                critical=True,
                description="Available disk space",
            )
        )

        _health_monitor.register_check(
            HealthCheck(
                "memory_usage",
                check_memory_usage,
                timeout=5.0,
                critical=False,
                description="System memory usage",
            )
        )

        _health_monitor.register_check(
            HealthCheck(
                "cache_health",
                check_cache_health,
                timeout=5.0,
                critical=False,
                description="Cache system health",
            )
        )

    return _health_monitor


async def get_system_health() -> Dict[str, Any]:
    """Get current system health status."""
    monitor = get_health_monitor()
    return await monitor.run_all_checks()
