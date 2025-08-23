"""API rate limiting utilities.

This module provides token bucket rate limiting for API calls with
configurable limits and retry logic.
"""

import logging
import time
from typing import Dict, Optional

from utils.config import config

logger = logging.getLogger(__name__)


class TokenBucket:
    """Token bucket rate limiter implementation."""

    def __init__(
        self,
        capacity: int,
        refill_rate: float,
        name: str = "bucket",
    ) -> None:
        """Initialize token bucket.
        
        Args:
            capacity: Maximum number of tokens
            refill_rate: Tokens added per second
            name: Bucket name for logging
        """
        self.capacity = capacity
        self.refill_rate = refill_rate
        self.name = name
        self.tokens = float(capacity)
        self.last_refill = time.time()

    def _refill(self) -> None:
        """Refill tokens based on elapsed time."""
        now = time.time()
        elapsed = now - self.last_refill
        tokens_to_add = elapsed * self.refill_rate

        self.tokens = min(self.capacity, self.tokens + tokens_to_add)
        self.last_refill = now

    def consume(self, tokens: int = 1) -> bool:
        """Try to consume tokens from bucket.
        
        Args:
            tokens: Number of tokens to consume
            
        Returns:
            True if tokens were consumed, False if insufficient
        """
        self._refill()

        if self.tokens >= tokens:
            self.tokens -= tokens
            logger.debug(f"{self.name}: Consumed {tokens} tokens, {self.tokens:.1f} remaining")
            return True

        logger.debug(f"{self.name}: Insufficient tokens ({self.tokens:.1f} < {tokens})")
        return False

    def wait_for_tokens(self, tokens: int = 1, timeout: Optional[float] = None) -> bool:
        """Wait until sufficient tokens are available.
        
        Args:
            tokens: Number of tokens needed
            timeout: Maximum time to wait (None for no timeout)
            
        Returns:
            True if tokens were acquired, False if timeout
        """
        start_time = time.time()

        while True:
            if self.consume(tokens):
                return True

            # Check timeout
            if timeout is not None and (time.time() - start_time) >= timeout:
                logger.warning(f"{self.name}: Timeout waiting for {tokens} tokens")
                return False

            # Calculate sleep time based on refill rate
            sleep_time = min(1.0, tokens / self.refill_rate)
            time.sleep(sleep_time)

    def get_status(self) -> Dict[str, float]:
        """Get current bucket status."""
        self._refill()
        return {
            "tokens": self.tokens,
            "capacity": self.capacity,
            "refill_rate": self.refill_rate,
            "utilization": (self.capacity - self.tokens) / self.capacity,
        }


class RateLimiter:
    """Rate limiter with multiple buckets for different API endpoints."""

    def __init__(self) -> None:
        """Initialize rate limiter with configured limits."""
        # Alpha Vantage rate limits
        calls_per_minute = config.API_RATE_LIMIT_CALLS_PER_MINUTE

        # Create token buckets for different time windows
        self.buckets = {
            "per_minute": TokenBucket(
                capacity=calls_per_minute,
                refill_rate=calls_per_minute / 60.0,  # tokens per second
                name="per_minute",
            ),
            "per_second": TokenBucket(
                capacity=5,  # Conservative burst limit
                refill_rate=calls_per_minute / 60.0,
                name="per_second",
            ),
        }

        # Track API call statistics
        self.call_count = 0
        self.last_call_time = 0.0
        self.call_history: list[float] = []

    def acquire(
        self,
        endpoint: str = "default",
        tokens: int = 1,
        timeout: Optional[float] = 30.0,
    ) -> bool:
        """Acquire rate limit tokens for an API call.
        
        Args:
            endpoint: API endpoint name (for logging)
            tokens: Number of tokens to acquire
            timeout: Maximum time to wait
            
        Returns:
            True if tokens acquired, False if timeout or failed
        """
        start_time = time.time()

        # Check all buckets
        for bucket_name, bucket in self.buckets.items():
            if not bucket.wait_for_tokens(tokens, timeout):
                logger.warning(f"Rate limit timeout for {endpoint} on bucket {bucket_name}")
                return False

        # Update statistics
        self.call_count += 1
        self.last_call_time = time.time()
        self.call_history.append(self.last_call_time)

        # Clean old history (keep last hour)
        cutoff_time = self.last_call_time - 3600
        self.call_history = [t for t in self.call_history if t > cutoff_time]

        elapsed = time.time() - start_time
        if elapsed > 0.1:  # Log if we had to wait
            logger.info(f"Rate limiter acquired for {endpoint} after {elapsed:.2f}s wait")

        return True

    def get_statistics(self) -> Dict[str, any]:
        """Get rate limiter statistics."""
        current_time = time.time()

        # Calculate calls in last minute
        minute_ago = current_time - 60
        calls_last_minute = len([t for t in self.call_history if t > minute_ago])

        # Calculate calls in last hour
        hour_ago = current_time - 3600
        calls_last_hour = len([t for t in self.call_history if t > hour_ago])

        return {
            "total_calls": self.call_count,
            "calls_last_minute": calls_last_minute,
            "calls_last_hour": calls_last_hour,
            "last_call_time": self.last_call_time,
            "buckets": {name: bucket.get_status() for name, bucket in self.buckets.items()},
        }

    def reset(self) -> None:
        """Reset rate limiter state."""
        for bucket in self.buckets.values():
            bucket.tokens = bucket.capacity
            bucket.last_refill = time.time()

        self.call_count = 0
        self.last_call_time = 0.0
        self.call_history.clear()

        logger.info("Rate limiter reset")


# Global rate limiter instance
rate_limiter = RateLimiter()


def acquire_rate_limit(
    endpoint: str = "default",
    tokens: int = 1,
    timeout: Optional[float] = 30.0,
) -> bool:
    """Acquire rate limit for an API call.
    
    Args:
        endpoint: API endpoint name
        tokens: Number of tokens to acquire
        timeout: Maximum time to wait
        
    Returns:
        True if acquired, False if timeout
    """
    return rate_limiter.acquire(endpoint, tokens, timeout)


def get_rate_limit_status() -> Dict[str, any]:
    """Get current rate limit status."""
    return rate_limiter.get_statistics()


def reset_rate_limiter() -> None:
    """Reset rate limiter state."""
    rate_limiter.reset()


class RateLimitedAPI:
    """Base class for rate-limited API clients."""

    def __init__(self, limiter: Optional[RateLimiter] = None) -> None:
        """Initialize with rate limiter."""
        self.limiter = limiter or rate_limiter

    def _call_with_rate_limit(
        self,
        func,
        endpoint: str,
        *args,
        timeout: Optional[float] = 30.0,
        **kwargs,
    ):
        """Call a function with rate limiting.
        
        Args:
            func: Function to call
            endpoint: Endpoint name for rate limiting
            *args: Function arguments
            timeout: Rate limit timeout
            **kwargs: Function keyword arguments
            
        Returns:
            Function result or None if rate limited
        """
        if not self.limiter.acquire(endpoint, timeout=timeout):
            logger.error(f"Rate limit timeout for {endpoint}")
            return None

        try:
            return func(*args, **kwargs)
        except Exception as e:
            logger.error(f"Error calling {endpoint}: {e}")
            return None


def create_adaptive_limiter(
    base_rate: float,
    burst_capacity: int = 5,
    adaptive_factor: float = 0.8,
) -> TokenBucket:
    """Create an adaptive rate limiter that adjusts based on API responses.
    
    Args:
        base_rate: Base tokens per second
        burst_capacity: Maximum burst tokens
        adaptive_factor: Factor to reduce rate on errors
        
    Returns:
        Adaptive token bucket
    """
    return TokenBucket(
        capacity=burst_capacity,
        refill_rate=base_rate * adaptive_factor,
        name="adaptive",
    )


def wait_for_rate_limit_reset(seconds: float = 60.0) -> None:
    """Wait for rate limit reset period.
    
    Args:
        seconds: Time to wait in seconds
    """
    logger.info(f"Waiting {seconds}s for rate limit reset")
    time.sleep(seconds)


def calculate_optimal_delay(
    calls_made: int,
    time_window: float,
    max_calls: int,
) -> float:
    """Calculate optimal delay between calls to respect rate limits.
    
    Args:
        calls_made: Number of calls already made
        time_window: Time window in seconds
        max_calls: Maximum calls allowed in window
        
    Returns:
        Optimal delay in seconds
    """
    if calls_made >= max_calls:
        return time_window  # Wait for full reset

    remaining_calls = max_calls - calls_made
    return time_window / remaining_calls if remaining_calls > 0 else 0.0
