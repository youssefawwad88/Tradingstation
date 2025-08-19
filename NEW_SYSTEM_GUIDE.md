# NEW DATA FETCHING SYSTEM - COMPLETE REBUILD

## Overview

This system has been completely rebuilt from the ground up to address the fundamental issues with the previous approach. The old system tried to mix the purposes of FULL API (historical data) and COMPACT API (real-time data), resulting in wasted API calls and zero real-time data.

## The Two-Job Architecture

### Job 1: `nightly_historical_rebuild.py`
**Purpose**: Single-purpose nightly historical data rebuilder
- **Schedule**: Run ONCE per day after market close (6:00 PM ET recommended)
- **API Strategy**: FULL fetch only - rebuilds entire historical datasets  
- **Mission**: Complete historical data rebuild for ALL tickers
- **Intervals**: Both 1min and 30min data
- **Usage**: `python3 jobs/nightly_historical_rebuild.py`

### Job 2: `realtime_intraday_updater.py`  
**Purpose**: Single-purpose real-time data updater
- **Schedule**: Run EVERY MINUTE, 24/5 during trading days
- **API Strategy**: COMPACT fetch only - gets last 100 candles
- **Mission**: Real-time data updates by merging with existing historical files
- **Intervals**: Both 1min and 30min data
- **Usage**: `python3 jobs/realtime_intraday_updater.py`

## Key Benefits

✅ **Clean Separation**: FULL API for historical rebuilds, COMPACT API for real-time updates
✅ **Single Purpose**: Each job has one clear responsibility  
✅ **Professional Error Handling**: Exponential backoff retry with detailed logging
✅ **Dynamic File Paths**: No hardcoded paths, uses standardized structure
✅ **Centralized Configuration**: All settings in `utils/config.py`
✅ **Production Ready**: Comprehensive logging with clear start/end messages

## File Structure

```
data/
├── intraday/           # 1-minute data files  
│   ├── AAPL_1min.csv
│   ├── NVDA_1min.csv
│   └── ...
└── intraday_30min/     # 30-minute data files
    ├── AAPL_30min.csv
    ├── NVDA_30min.csv
    └── ...
```

## Scheduling Examples

### Cron Setup for Production

```bash
# Nightly historical rebuild (6:00 PM ET after market close)
0 18 * * 1-5 cd /path/to/Tradingstation && python3 jobs/nightly_historical_rebuild.py

# Real-time updates (every minute during trading days)  
* * * * 1-5 cd /path/to/Tradingstation && python3 jobs/realtime_intraday_updater.py
```

### Manual Testing

```bash
# Test nightly rebuild
python3 jobs/nightly_historical_rebuild.py

# Test real-time updates
python3 jobs/realtime_intraday_updater.py

# Run system validation
python3 test_new_system.py
```

## Monitoring and Logging

Both jobs create detailed logs in `data/logs/`:
- `nightly_historical_rebuild.log` - Historical rebuild activity
- `realtime_intraday_updater.log` - Real-time update activity

Each job provides:
- Clear start/end messages with timestamps
- Detailed statistics (success rates, API calls, etc.)
- Error tracking with retry attempts
- Performance metrics

## Configuration

All configuration is centralized in `utils/config.py`:

```python
# API Keys
ALPHA_VANTAGE_API_KEY = os.getenv("ALPHA_VANTAGE_API_KEY")

# Processing Controls  
MAX_TICKERS_PER_RUN = int(os.getenv("MAX_TICKERS_PER_RUN", "25"))
INTRADAY_BATCH_SIZE = int(os.getenv("INTRADAY_BATCH_SIZE", "25"))

# Cloud Storage
SPACES_BUCKET_NAME = os.getenv("SPACES_BUCKET_NAME")
```

## What Was Removed

The following outdated files have been completely removed:
- `jobs/intraday_fetcher.py` - Complex multi-purpose fetcher
- `jobs/master_compact_fetcher.py` - Overly complex "unified" system
- Any other scripts that mixed FULL and COMPACT API purposes

## Production Deployment

1. **Set Environment Variables**: Ensure API keys and cloud storage credentials are configured
2. **Schedule Jobs**: Set up cron jobs as shown above
3. **Monitor Logs**: Watch the log files for any issues
4. **Validate Data**: Check that both historical and real-time data are being updated

This is the FINAL solution that properly separates historical rebuilds from real-time updates.