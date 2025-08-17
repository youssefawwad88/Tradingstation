# API Optimization Guidelines

## Current API Usage Analysis

### Alpha Vantage API Calls

The system makes API calls in multiple locations:

1. **Daily Data Fetching** (`fetch_daily.py`)
   - Endpoint: `TIME_SERIES_DAILY`
   - Frequency: Once per day for all tickers
   - Volume: 500+ calls per day

2. **Intraday Data Fetching** (`fetch_intraday_compact.py`)
   - Endpoint: `TIME_SERIES_INTRADAY`
   - Frequency: Every minute during market hours
   - Volume: 3000+ calls per day

3. **30-Minute Data** (`fetch_30min.py`)
   - Endpoint: `TIME_SERIES_INTRADAY` with 30min interval
   - Frequency: Every 30 minutes
   - Volume: 500+ calls per day

## Optimization Opportunities

### 1. Caching Layer Implementation

**Current State**: Limited caching in `utils/cache.py`
**Recommendation**: Implement comprehensive caching

```python
# Enhanced caching strategy
CACHE_STRATEGIES = {
    "daily_data": {"ttl": 86400, "size_limit": "100MB"},  # 24 hours
    "intraday_1min": {"ttl": 300, "size_limit": "500MB"},  # 5 minutes
    "intraday_30min": {"ttl": 1800, "size_limit": "200MB"},  # 30 minutes
    "ticker_metadata": {"ttl": 604800, "size_limit": "10MB"},  # 1 week
}
```

### 2. API Call Batching

**Current**: Individual calls per ticker
**Optimized**: Batch multiple tickers when possible

```python
# Batch processing example
def fetch_multiple_tickers_batch(tickers, data_type, interval):
    """Fetch multiple tickers in optimized batches."""
    batch_size = 5  # API rate limit consideration
    results = {}
    
    for i in range(0, len(tickers), batch_size):
        batch = tickers[i:i + batch_size]
        # Process batch with rate limiting
        time.sleep(12)  # Alpha Vantage rate limit: 5 calls/minute
        batch_results = process_ticker_batch(batch, data_type, interval)
        results.update(batch_results)
    
    return results
```

### 3. Data Deduplication

**Issue**: Multiple fetchers may request same data
**Solution**: Centralized data coordinator

```python
class DataCoordinator:
    """Centralized coordinator to prevent duplicate API calls."""
    
    def __init__(self):
        self.pending_requests = {}
        self.request_lock = threading.Lock()
    
    def get_data(self, ticker, data_type, interval):
        request_key = f"{ticker}_{data_type}_{interval}"
        
        with self.request_lock:
            # Check if request is already pending
            if request_key in self.pending_requests:
                return self.pending_requests[request_key].result()
            
            # Make new request
            future = self.submit_request(ticker, data_type, interval)
            self.pending_requests[request_key] = future
            
        return future.result()
```

### 4. Smart Refresh Strategy

**Current**: Fixed intervals regardless of market conditions
**Optimized**: Dynamic refresh based on market activity

```python
def get_refresh_interval(ticker, market_session, volatility):
    """Dynamic refresh intervals based on market conditions."""
    base_intervals = {
        "REGULAR": 60,      # 1 minute during regular hours
        "PRE-MARKET": 300,  # 5 minutes pre-market
        "AFTER-HOURS": 300, # 5 minutes after-hours
        "CLOSED": 3600,     # 1 hour when closed
    }
    
    # Adjust for volatility
    volatility_multiplier = 1.0
    if volatility > 0.05:  # High volatility
        volatility_multiplier = 0.5  # More frequent updates
    elif volatility < 0.01:  # Low volatility
        volatility_multiplier = 2.0  # Less frequent updates
    
    return int(base_intervals[market_session] * volatility_multiplier)
```

## Performance Monitoring

### API Call Tracking

```python
class APICallTracker:
    """Track and monitor API usage."""
    
    def __init__(self):
        self.call_history = []
        self.rate_limits = {
            "alpha_vantage": {"calls_per_minute": 5, "calls_per_day": 500}
        }
    
    def log_call(self, endpoint, ticker, timestamp, response_time):
        """Log API call for monitoring."""
        self.call_history.append({
            "endpoint": endpoint,
            "ticker": ticker,
            "timestamp": timestamp,
            "response_time": response_time,
            "success": True if response_time else False
        })
    
    def get_usage_stats(self, time_window="1h"):
        """Get API usage statistics."""
        # Implementation for usage analysis
        pass
```

### Cache Hit Ratio Monitoring

```python
def monitor_cache_performance():
    """Monitor cache hit ratios and effectiveness."""
    stats = {
        "total_requests": cache.total_requests,
        "cache_hits": cache.cache_hits,
        "cache_misses": cache.cache_misses,
        "hit_ratio": cache.cache_hits / cache.total_requests if cache.total_requests > 0 else 0,
        "avg_response_time_cached": cache.avg_cached_response_time,
        "avg_response_time_api": cache.avg_api_response_time
    }
    return stats
```

## Implementation Priority

### Phase 1: Quick Wins (1-2 days)
1. Add timeout to all requests calls
2. Implement basic request deduplication
3. Add API call logging

### Phase 2: Caching Enhancement (3-5 days)
1. Enhance existing cache with TTL strategies
2. Implement cache warming for popular tickers
3. Add cache hit ratio monitoring

### Phase 3: Advanced Optimization (1-2 weeks)
1. Implement data coordinator
2. Dynamic refresh intervals
3. Comprehensive performance monitoring
4. API usage optimization dashboard

## Expected Impact

- **API Calls Reduction**: 40-60% fewer redundant calls
- **Response Time**: 80% improvement for cached data
- **Rate Limit Compliance**: 100% adherence to API limits
- **Cost Reduction**: Potential 50% reduction in API costs
- **Reliability**: Better handling of API failures and rate limits

## Monitoring Metrics

1. **API Call Volume**: Daily/hourly call counts by endpoint
2. **Cache Performance**: Hit ratio, miss ratio, eviction rate
3. **Response Times**: Average response times for cached vs API calls
4. **Error Rates**: API failures, timeout rates, rate limit hits
5. **Cost Tracking**: API usage costs and trends