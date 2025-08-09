"""
Unit tests for cache module.
"""

import tempfile
import time
from unittest.mock import patch
import pandas as pd
import pytest

from utils.cache import (
    CacheEntry,
    InMemoryCache,
    DiskCache,
    TieredCache,
    get_cache,
    cache_key_for_ticker_data,
    cached_fetch_wrapper,
)


class TestCacheEntry:
    """Test cases for cache entry."""

    def test_cache_entry_creation(self):
        """Test cache entry creation and basic properties."""
        data = {"test": "data"}
        entry = CacheEntry(data, ttl_seconds=60)

        assert entry.data == data
        assert entry.ttl_seconds == 60
        assert entry.access_count == 0
        assert not entry.is_expired()

    def test_cache_entry_expiration(self):
        """Test cache entry expiration."""
        data = {"test": "data"}
        entry = CacheEntry(data, ttl_seconds=0)

        # Should be expired immediately
        time.sleep(0.1)
        assert entry.is_expired()

    def test_cache_entry_access(self):
        """Test cache entry access tracking."""
        data = {"test": "data"}
        entry = CacheEntry(data)

        # Access the data
        result = entry.access()

        assert result == data
        assert entry.access_count == 1
        assert entry.last_accessed > entry.created_at


class TestInMemoryCache:
    """Test cases for in-memory cache."""

    def test_cache_set_get(self):
        """Test basic cache set and get operations."""
        cache = InMemoryCache()

        cache.set("test_key", "test_value")
        result = cache.get("test_key")

        assert result == "test_value"

    def test_cache_miss(self):
        """Test cache miss returns None."""
        cache = InMemoryCache()
        result = cache.get("nonexistent_key")
        assert result is None

    def test_cache_expiration(self):
        """Test cache entry expiration."""
        cache = InMemoryCache()

        cache.set("test_key", "test_value", ttl_seconds=0)
        time.sleep(0.1)
        result = cache.get("test_key")

        assert result is None

    def test_cache_eviction(self):
        """Test LRU eviction when cache is full."""
        # Create small cache
        cache = InMemoryCache(max_size_bytes=1024)

        # Fill cache with large entries
        large_data = "x" * 500  # 500 bytes each
        cache.set("key1", large_data)
        cache.set("key2", large_data)
        cache.set("key3", large_data)  # Should trigger eviction

        # key1 should be evicted (oldest)
        assert cache.get("key1") is None
        assert cache.get("key2") is not None
        assert cache.get("key3") is not None

    def test_cache_clear(self):
        """Test cache clear operation."""
        cache = InMemoryCache()

        cache.set("key1", "value1")
        cache.set("key2", "value2")
        cache.clear()

        assert cache.get("key1") is None
        assert cache.get("key2") is None
        assert cache.current_size_bytes == 0

    def test_cache_stats(self):
        """Test cache statistics."""
        cache = InMemoryCache()

        cache.set("key1", "value1")
        cache.get("key1")  # Access once

        stats = cache.stats()

        assert stats["entries"] == 1
        assert stats["total_accesses"] == 1
        assert 0 <= stats["utilization"] <= 1


class TestDiskCache:
    """Test cases for disk cache."""

    def test_disk_cache_set_get(self):
        """Test basic disk cache operations."""
        with tempfile.TemporaryDirectory() as temp_dir:
            cache = DiskCache(cache_dir=temp_dir)

            cache.set("test_key", "test_value")
            result = cache.get("test_key")

            assert result == "test_value"

    def test_disk_cache_expiration(self):
        """Test disk cache expiration."""
        with tempfile.TemporaryDirectory() as temp_dir:
            cache = DiskCache(cache_dir=temp_dir)

            cache.set("test_key", "test_value", ttl_seconds=0)
            time.sleep(0.1)
            result = cache.get("test_key")

            assert result is None

    def test_disk_cache_dataframe(self):
        """Test caching pandas DataFrames."""
        with tempfile.TemporaryDirectory() as temp_dir:
            cache = DiskCache(cache_dir=temp_dir)

            df = pd.DataFrame({"A": [1, 2, 3], "B": [4, 5, 6]})
            cache.set("df_key", df)
            result = cache.get("df_key")

            assert isinstance(result, pd.DataFrame)
            pd.testing.assert_frame_equal(result, df)


class TestTieredCache:
    """Test cases for tiered cache."""

    def test_tiered_cache_promotion(self):
        """Test promotion from disk to memory cache."""
        with tempfile.TemporaryDirectory() as temp_dir:
            cache = TieredCache(memory_size_mb=1, cache_dir=temp_dir)

            # Set in both tiers
            cache.set("test_key", "test_value")

            # Clear memory cache to simulate memory eviction
            cache.memory_cache.clear()

            # Get should promote from disk to memory
            result = cache.get("test_key")

            assert result == "test_value"
            # Should now be in memory cache
            assert cache.memory_cache.get("test_key") == "test_value"

    def test_tiered_cache_stats(self):
        """Test tiered cache statistics."""
        cache = TieredCache()

        cache.set("test_key", "test_value")
        stats = cache.stats()

        assert "memory" in stats
        assert "disk_dir" in stats


def test_cache_key_generation():
    """Test cache key generation for ticker data."""
    key = cache_key_for_ticker_data("AAPL", "intraday", "1min", "compact")
    assert isinstance(key, str)
    assert "AAPL" in key
    assert "intraday" in key


def test_cached_fetch_wrapper():
    """Test caching decorator for fetch functions."""
    call_count = 0

    @cached_fetch_wrapper
    def mock_fetch_function(ticker):
        nonlocal call_count
        call_count += 1
        return pd.DataFrame({"price": [100]}), True

    # First call should execute function
    result1 = mock_fetch_function("AAPL")
    assert call_count == 1

    # Second call should return cached result
    result2 = mock_fetch_function("AAPL")
    assert call_count == 1  # Should not increment

    # Results should be the same
    assert result1[1] == result2[1]  # Success flags match


def test_global_cache_singleton():
    """Test global cache singleton pattern."""
    cache1 = get_cache()
    cache2 = get_cache()

    assert cache1 is cache2  # Should be the same instance
