# Silent Failure Resolution Summary

## Overview

This document provides solutions for common "silent failure" scenarios where the TradingStation system appears to be running but is not functioning correctly.

## Common Silent Failure Scenarios

### 1. No Signals Generated

**Symptoms:**
- System runs without errors
- Signal files are empty or contain old data
- Dashboard shows no opportunities
- Logs show successful completion

**Root Causes & Solutions:**

#### Missing or Stale Data
```bash
# Check data freshness
python3 tools/health_check.py --check data

# Force data refresh
python3 jobs/data_fetch_manager.py --job daily --force
python3 jobs/data_fetch_manager.py --job intraday_1min --force
```

#### Overly Restrictive Parameters
```bash
# Check with relaxed parameters
python3 screeners/gapgo.py --min-gap 1.0 --max-gap 10.0 --verbose
python3 screeners/orb.py --volume-threshold 1.0 --verbose
```

#### Universe Issues
```bash
# Verify universe is populated
python3 jobs/data_fetch_manager.py --job universe --verbose

# Check universe file
python3 -c "
from utils.spaces_io import SpacesIO
df = SpacesIO().download_csv('data/universe/master_tickerlist.csv')
print(f'Universe size: {len(df) if df is not None else 0}')
"
```

### 2. Data Pipeline Running But No Data Stored

**Symptoms:**
- Data fetch jobs complete successfully
- No error messages in logs
- Data files missing or empty in Spaces
- API calls being made but results not persisted

**Root Causes & Solutions:**

#### Spaces Upload Failures
```bash
# Test Spaces connectivity
python3 tools/verify_deployment.py --json | grep -A 10 spaces_connectivity

# Check Spaces permissions
python3 -c "
from utils.spaces_io import SpacesIO
spaces = SpacesIO()
test_content = 'test'
success = spaces.upload_text(test_content, 'test/connectivity.txt')
print(f'Upload test: {success}')
if success:
    downloaded = spaces.download_text('test/connectivity.txt')
    print(f'Download test: {downloaded == test_content}')
    spaces.delete_file('test/connectivity.txt')
"
```

#### API Response Issues
```bash
# Test API responses with verbose logging
python3 jobs/data_fetch_manager.py --job daily --tickers AAPL --verbose --test-mode

# Check API key validity
python3 -c "
from utils.alpha_vantage_api import AlphaVantageAPI
api = AlphaVantageAPI()
data = api.get_daily_data('AAPL', compact=True)
print(f'API test: {data is not None and not data.empty}')
"
```

#### Data Validation Failures
```bash
# Check for validation errors
python3 jobs/data_fetch_manager.py --job daily --tickers AAPL --verbose 2>&1 | grep -i "validation\|error\|fail"

# Test with minimal validation
python3 -c "
from jobs.data_fetch_manager import DataFetchManager
manager = DataFetchManager()
manager.config.validation_enabled = False  # Disable validation temporarily
result = manager.fetch_daily_data(['AAPL'])
print(f'Validation bypass test: {result}')
"
```

### 3. API Calls Succeeding But Returning No Data

**Symptoms:**
- API connectivity tests pass
- No rate limit errors
- Data fetch operations complete
- Resulting datasets are empty

**Root Causes & Solutions:**

#### Market Hours/Weekend Issues
```bash
# Check if market is open
python3 -c "
from orchestrator.modes import determine_market_mode, is_market_day
print(f'Market mode: {determine_market_mode()}')
print(f'Market day: {is_market_day()}')
"

# Force historical data fetch
python3 jobs/data_fetch_manager.py --job daily --tickers AAPL --force-full --verbose
```

#### Symbol Validity Issues
```bash
# Test with known good symbols
python3 jobs/data_fetch_manager.py --job daily --tickers AAPL,NVDA,TSLA --verbose

# Check symbol validity
python3 -c "
from utils.alpha_vantage_api import AlphaVantageAPI
api = AlphaVantageAPI()
for symbol in ['AAPL', 'INVALID_SYMBOL']:
    data = api.get_daily_data(symbol, compact=True)
    print(f'{symbol}: {len(data) if data is not None else 0} records')
"
```

#### API Response Format Changes
```bash
# Check raw API response
python3 -c "
import requests
import os
api_key = os.getenv('ALPHA_VANTAGE_API_KEY')
url = f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=AAPL&apikey={api_key}'
response = requests.get(url)
print(f'Status: {response.status_code}')
print(f'Response keys: {list(response.json().keys()) if response.status_code == 200 else \"Error\"}')
"
```

### 4. Dashboard Not Updating

**Symptoms:**
- Dashboard generation completes without errors
- Signal files contain recent data
- Dashboard shows stale or no data
- Dashboard file exists but content is outdated

**Root Causes & Solutions:**

#### Signal File Access Issues
```bash
# Test signal file access
python3 -c "
from utils.spaces_io import SpacesIO
spaces = SpacesIO()
signal_files = ['gapgo.csv', 'orb.csv', 'avwap_reclaim.csv']
for file in signal_files:
    df = spaces.download_csv(f'data/signals/{file}')
    print(f'{file}: {len(df) if df is not None else 0} signals')
"
```

#### Dashboard Generation Logic Issues
```bash
# Test dashboard with minimal data
python3 dashboard/master_dashboard.py --verbose --hours-lookback 72

# Check dashboard logic step by step
python3 -c "
from dashboard.master_dashboard import MasterDashboard
dashboard = MasterDashboard()
signals = dashboard.load_all_signals()
print(f'Total signals loaded: {len(signals)}')
active = dashboard.filter_active_signals(signals, 24)
print(f'Active signals (24h): {len(active)}')
"
```

#### Timestamp Issues
```bash
# Check timestamp formats
python3 -c "
from utils.spaces_io import SpacesIO
import pandas as pd
spaces = SpacesIO()
df = spaces.download_csv('data/signals/gapgo.csv')
if df is not None and not df.empty:
    print(f'Timestamp column: {\"timestamp_utc\" in df.columns}')
    if 'timestamp_utc' in df.columns:
        print(f'Sample timestamp: {df[\"timestamp_utc\"].iloc[0]}')
        df['timestamp_utc'] = pd.to_datetime(df['timestamp_utc'])
        print(f'Parsed timestamp: {df[\"timestamp_utc\"].iloc[0]}')
"
```

### 5. Rate Limiting Not Working

**Symptoms:**
- API rate limit errors occurring
- System not respecting quotas
- Excessive API calls being made
- Performance degradation

**Root Causes & Solutions:**

#### Rate Limiter Configuration
```bash
# Test rate limiter
python3 -c "
from utils.rate_limit import RateLimiter
limiter = RateLimiter(calls_per_minute=5)
import time
for i in range(7):
    start = time.time()
    allowed = limiter.acquire()
    elapsed = time.time() - start
    print(f'Call {i+1}: allowed={allowed}, wait_time={elapsed:.2f}s')
"
```

#### Multiple Instance Issues
```bash
# Check for multiple running instances
ps aux | grep python3 | grep -E "(data_fetch|screener|orchestrator)"

# Kill duplicate processes if found
pkill -f "python3.*data_fetch_manager"
```

#### Persistent Rate Limit State
```bash
# Reset rate limiter state
python3 -c "
from utils.rate_limit import RateLimiter
limiter = RateLimiter(calls_per_minute=5)
limiter.reset()
print('Rate limiter reset')
"
```

### 6. Configuration Not Loading

**Symptoms:**
- Default values being used instead of configured values
- Environment variables appear set but not being read
- Inconsistent behavior across components

**Root Causes & Solutions:**

#### Environment Variable Access
```bash
# Test environment variable access
python3 -c "
import os
from utils.config import Config
config = Config()
print(f'API Key set: {bool(os.getenv(\"ALPHA_VANTAGE_API_KEY\"))}')
print(f'Spaces Key set: {bool(os.getenv(\"SPACES_ACCESS_KEY_ID\"))}')
print(f'Config API Key: {bool(config.alpha_vantage_api_key)}')
print(f'Config test mode: {config.test_mode}')
"
```

#### Configuration Loading Order
```bash
# Check configuration loading
python3 -c "
from utils.config import Config
import os
print('Environment variables:')
for key in ['ALPHA_VANTAGE_API_KEY', 'SPACES_BUCKET_NAME', 'APP_ENV']:
    print(f'  {key}: {bool(os.getenv(key))}')
print('\\nConfig object:')
config = Config()
print(f'  test_mode: {config.test_mode}')
print(f'  bucket_name: {config.spaces_bucket_name}')
"
```

## Diagnostic Commands

### Quick System Health Check
```bash
# One-command health overview
python3 tools/health_check.py --json | jq '.checks | to_entries[] | {name: .key, status: .value.status}'
```

### Component-by-Component Test
```bash
# Test each major component
echo "=== Testing Data Fetch ==="
python3 jobs/data_fetch_manager.py --job daily --tickers AAPL --test-mode --verbose

echo "=== Testing Screener ==="
python3 screeners/gapgo.py --test-mode --tickers AAPL --verbose

echo "=== Testing Dashboard ==="
python3 dashboard/master_dashboard.py --test-mode --verbose

echo "=== Testing Orchestrator ==="
python3 orchestrator/run_all.py --mode premarket --dry-run --verbose
```

### Data Flow Verification
```bash
# Trace data flow end-to-end
echo "1. Check universe..."
python3 -c "from utils.spaces_io import SpacesIO; df = SpacesIO().download_csv('data/universe/master_tickerlist.csv'); print(f'Universe: {len(df) if df else 0} symbols')"

echo "2. Check daily data..."
python3 -c "from utils.spaces_io import SpacesIO; files = SpacesIO().list_files('data/daily/'); print(f'Daily files: {len(files) if files else 0}')"

echo "3. Check signals..."
python3 -c "from utils.spaces_io import SpacesIO; files = SpacesIO().list_files('data/signals/'); print(f'Signal files: {len(files) if files else 0}')"

echo "4. Check dashboard..."
python3 -c "from utils.spaces_io import SpacesIO; df = SpacesIO().download_csv('data/dashboard/master_dashboard.csv'); print(f'Dashboard: {\"exists\" if df is not None else \"missing\"}')"
```

## Prevention Strategies

### 1. Comprehensive Logging
Add verbose logging to all critical paths:
```python
self.logger.info(f"Starting operation with {len(symbols)} symbols")
self.logger.debug(f"API response size: {len(data)} records")
self.logger.warning(f"No data returned for {symbol}")
```

### 2. Data Validation Checkpoints
Implement validation at each stage:
```python
if data is None or data.empty:
    self.logger.error(f"Empty data for {symbol}")
    return False
```

### 3. Health Monitoring
Set up automated health checks:
```bash
# Run health check every 30 minutes
*/30 * * * * /usr/bin/python3 /app/tools/health_check.py --json > /tmp/health.log
```

### 4. Graceful Degradation
Implement fallback mechanisms:
```python
try:
    result = primary_operation()
except Exception as e:
    self.logger.warning(f"Primary failed: {e}, trying fallback")
    result = fallback_operation()
```

## Emergency Recovery

### Complete System Reset
```bash
# 1. Stop all processes
pkill -f "python3.*trading"

# 2. Clear temporary data
python3 jobs/backfill_rebuilder.py --operation cleanup

# 3. Rebuild from scratch
python3 jobs/backfill_rebuilder.py --operation rebuild-all

# 4. Verify system
python3 tools/smoke_test_e2e.py
```

### Data Recovery
```bash
# Recover from specific date
python3 jobs/backfill_rebuilder.py --operation restore --date 2024-01-15

# Rebuild specific component
python3 jobs/data_fetch_manager.py --job daily --force --symbols NVDA,AAPL,TSLA
```

This summary provides systematic approaches to identify and resolve silent failures, ensuring the TradingStation system operates reliably and transparently.