"""
Intelligent caching layer for market data with TTL and size limits.

This module provides high-performance caching for frequently accessed data
with automatic expiration and memory management.
"""

import hashlib
import json
import logging
import os
import pickle
import tempfile
import threading
import time
from datetime import datetime, timedelta
from typing import Any, Dict, Optional, Tuple, Union

import pandas as pd

logger = logging.getLogger(__name__)


class CacheEntry:
    """Represents a cached data entry with metadata."""

    def __init__(self, data: Any, ttl_seconds: int = 3600):
        self.data = data
        self.created_at = time.time()
        self.ttl_seconds = ttl_seconds
        self.access_count = 0
        self.last_accessed = self.created_at

    def is_expired(self) -> bool:
        """Check if the cache entry has expired."""
        return time.time() - self.created_at > self.ttl_seconds

    def access(self) -> Any:
        """Access the cached data and update metadata."""
        self.access_count += 1
        self.last_accessed = time.time()
        return self.data

    def size_bytes(self) -> int:
        """Get approximate size of cached data in bytes."""
        try:
            if isinstance(self.data, pd.DataFrame):
                return self.data.memory_usage(deep=True).sum()
            else:
                return len(pickle.dumps(self.data))
        except Exception:
            return 1024  # Default fallback size


class InMemoryCache:
    """Thread-safe in-memory cache with TTL and LRU eviction."""

    def __init__(self, max_size_bytes: int = 100 * 1024 * 1024):  # 100MB default
        self.max_size_bytes = max_size_bytes
        self.cache: Dict[str, CacheEntry] = {}
        self.current_size_bytes = 0
        self._lock = threading.RLock()

    def _generate_key(self, *args, **kwargs) -> str:
        """Generate a cache key from arguments."""
        key_data = {"args": args, "kwargs": sorted(kwargs.items())}
        key_string = json.dumps(key_data, sort_keys=True, default=str)
        return hashlib.md5(key_string.encode()).hexdigest()

    def _evict_lru(self) -> None:
        """Evict least recently used items to free space."""
        if not self.cache:
            return

        # Sort by last accessed time (oldest first)
        sorted_entries = sorted(self.cache.items(), key=lambda x: x[1].last_accessed)

        # Remove oldest entries until we're under 80% of max size
        target_size = int(self.max_size_bytes * 0.8)

        for key, entry in sorted_entries:
            if self.current_size_bytes <= target_size:
                break

            self.current_size_bytes -= entry.size_bytes()
            del self.cache[key]
            logger.debug(f"Evicted cache entry: {key}")

    def _cleanup_expired(self) -> None:
        """Remove expired entries from cache."""
        expired_keys = [key for key, entry in self.cache.items() if entry.is_expired()]

        for key in expired_keys:
            entry = self.cache[key]
            self.current_size_bytes -= entry.size_bytes()
            del self.cache[key]
            logger.debug(f"Removed expired cache entry: {key}")

    def get(self, key: str) -> Optional[Any]:
        """Get data from cache if it exists and is not expired."""
        with self._lock:
            if key not in self.cache:
                return None

            entry = self.cache[key]
            if entry.is_expired():
                self.current_size_bytes -= entry.size_bytes()
                del self.cache[key]
                return None

            return entry.access()

    def set(self, key: str, data: Any, ttl_seconds: int = 3600) -> None:
        """Store data in cache with TTL."""
        with self._lock:
            entry = CacheEntry(data, ttl_seconds)
            entry_size = entry.size_bytes()

            # Check if single entry is too large
            if entry_size > self.max_size_bytes:
                logger.warning(
                    f"Cache entry too large ({entry_size} bytes), not caching"
                )
                return

            # Remove existing entry if key exists
            if key in self.cache:
                old_entry = self.cache[key]
                self.current_size_bytes -= old_entry.size_bytes()

            # Ensure we have space
            if self.current_size_bytes + entry_size > self.max_size_bytes:
                self._evict_lru()

            # If still not enough space, cleanup expired
            if self.current_size_bytes + entry_size > self.max_size_bytes:
                self._cleanup_expired()

            # Final check and forced eviction if needed
            if self.current_size_bytes + entry_size > self.max_size_bytes:
                self._evict_lru()

            # Store the entry
            self.cache[key] = entry
            self.current_size_bytes += entry_size

            logger.debug(
                f"Cached entry: {key} ({entry_size} bytes, "
                f"total: {self.current_size_bytes}/{self.max_size_bytes})"
            )

    def clear(self) -> None:
        """Clear all cached data."""
        with self._lock:
            self.cache.clear()
            self.current_size_bytes = 0
            logger.info("Cache cleared")

    def stats(self) -> Dict[str, Any]:
        """Get cache statistics."""
        with self._lock:
            total_accesses = sum(entry.access_count for entry in self.cache.values())
            return {
                "entries": len(self.cache),
                "size_bytes": self.current_size_bytes,
                "max_size_bytes": self.max_size_bytes,
                "utilization": self.current_size_bytes / self.max_size_bytes,
                "total_accesses": total_accesses,
            }


class DiskCache:
    """Persistent disk cache for larger datasets."""

    def __init__(self, cache_dir: Optional[str] = None, max_size_gb: float = 1.0):
        self.cache_dir = cache_dir or os.path.join(
            tempfile.gettempdir(), "tradingstation_cache"
        )
        self.max_size_bytes = int(max_size_gb * 1024 * 1024 * 1024)
        os.makedirs(self.cache_dir, exist_ok=True)

    def _get_cache_path(self, key: str) -> str:
        """Get the file path for a cache key."""
        return os.path.join(self.cache_dir, f"{key}.pkl")

    def _get_meta_path(self, key: str) -> str:
        """Get the metadata file path for a cache key."""
        return os.path.join(self.cache_dir, f"{key}.meta")

    def get(self, key: str) -> Optional[Any]:
        """Get data from disk cache."""
        cache_path = self._get_cache_path(key)
        meta_path = self._get_meta_path(key)

        if not os.path.exists(cache_path) or not os.path.exists(meta_path):
            return None

        try:
            # Check if expired
            with open(meta_path, "r") as f:
                meta = json.load(f)

            if time.time() - meta["created_at"] > meta["ttl_seconds"]:
                # Expired, remove files
                os.remove(cache_path)
                os.remove(meta_path)
                return None

            # Load data
            with open(cache_path, "rb") as f:
                data = pickle.load(f)

            # Update access time
            meta["last_accessed"] = time.time()
            meta["access_count"] = meta.get("access_count", 0) + 1
            with open(meta_path, "w") as f:
                json.dump(meta, f)

            return data

        except Exception as e:
            logger.error(f"Error reading from disk cache: {e}")
            # Clean up corrupted files
            for path in [cache_path, meta_path]:
                if os.path.exists(path):
                    os.remove(path)
            return None

    def set(self, key: str, data: Any, ttl_seconds: int = 3600) -> None:
        """Store data in disk cache."""
        cache_path = self._get_cache_path(key)
        meta_path = self._get_meta_path(key)

        try:
            # Save data
            with open(cache_path, "wb") as f:
                pickle.dump(data, f)

            # Save metadata
            meta = {
                "created_at": time.time(),
                "ttl_seconds": ttl_seconds,
                "access_count": 0,
                "last_accessed": time.time(),
            }
            with open(meta_path, "w") as f:
                json.dump(meta, f)

            logger.debug(f"Cached to disk: {key}")

        except Exception as e:
            logger.error(f"Error writing to disk cache: {e}")

    def clear(self) -> None:
        """Clear all disk cache files."""
        try:
            for filename in os.listdir(self.cache_dir):
                file_path = os.path.join(self.cache_dir, filename)
                os.remove(file_path)
            logger.info("Disk cache cleared")
        except Exception as e:
            logger.error(f"Error clearing disk cache: {e}")


class TieredCache:
    """Two-tier cache system with memory and disk layers."""

    def __init__(
        self,
        memory_size_mb: int = 100,
        disk_size_gb: float = 1.0,
        cache_dir: Optional[str] = None,
    ):
        self.memory_cache = InMemoryCache(memory_size_mb * 1024 * 1024)
        self.disk_cache = DiskCache(cache_dir, disk_size_gb)

    def get(self, key: str) -> Optional[Any]:
        """Get data from cache, checking memory first, then disk."""
        # Try memory cache first
        data = self.memory_cache.get(key)
        if data is not None:
            return data

        # Try disk cache
        data = self.disk_cache.get(key)
        if data is not None:
            # Promote to memory cache
            self.memory_cache.set(key, data)
            return data

        return None

    def set(self, key: str, data: Any, ttl_seconds: int = 3600) -> None:
        """Store data in both memory and disk cache."""
        self.memory_cache.set(key, data, ttl_seconds)
        self.disk_cache.set(key, data, ttl_seconds)

    def clear(self) -> None:
        """Clear both memory and disk cache."""
        self.memory_cache.clear()
        self.disk_cache.clear()

    def stats(self) -> Dict[str, Any]:
        """Get combined cache statistics."""
        memory_stats = self.memory_cache.stats()
        return {
            "memory": memory_stats,
            "disk_dir": self.disk_cache.cache_dir,
        }


# Global cache instance
_global_cache: Optional[TieredCache] = None


def get_cache() -> TieredCache:
    """Get or create the global cache instance."""
    global _global_cache
    if _global_cache is None:
        _global_cache = TieredCache()
    return _global_cache


def cache_key_for_ticker_data(
    ticker: str, data_type: str, interval: str = "1min", outputsize: str = "compact"
) -> str:
    """Generate a cache key for ticker data."""
    return f"ticker_{ticker}_{data_type}_{interval}_{outputsize}"


def cached_fetch_wrapper(func):
    """Decorator to add caching to data fetching functions."""

    def wrapper(*args, **kwargs):
        cache = get_cache()

        # Generate cache key using the memory cache's method
        cache_key = cache.memory_cache._generate_key(*args, **kwargs)

        # Try to get from cache
        cached_result = cache.get(cache_key)
        if cached_result is not None:
            logger.debug(f"Cache hit for {func.__name__}")
            return cached_result

        # Call function and cache result
        result = func(*args, **kwargs)
        if result is not None and len(result) == 2 and result[1]:  # Success case
            cache.set(cache_key, result, ttl_seconds=300)  # 5 minute TTL for API data
            logger.debug(f"Cached result for {func.__name__}")

        return result

    return wrapper
