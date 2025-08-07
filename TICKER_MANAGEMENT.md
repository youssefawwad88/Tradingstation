# Ticker Management & Data Fetching System

This document describes the unified ticker management system that implements Ashraf's breakout logic for S&P 500 filtering.

## Overview

The system merges two ticker sources into a single `master_tickerlist.csv`:

1. **Manual Tickers** (from `tickerlist.txt`) - Always included, no filters applied
2. **S&P 500 Tickers** (from `data/universe/sp500.csv`) - Filtered using Ashraf's breakout logic

## Core Components

### 1. Master Ticker List Generation
```bash
python generate_master_tickerlist.py
```
- Combines manual and filtered S&P 500 tickers
- Applies Ashraf filtering logic to S&P 500 stocks
- Outputs `master_tickerlist.csv` (used by all fetchers)

### 2. Daily Full Fetch (Once per Day - 6:00 AM ET)
```bash
python fetch_daily.py     # Daily data (200 rows)
python fetch_30min.py     # 30-min data (500 rows) 
python jobs/update_all_data.py  # All intervals combined
```

### 3. Intraday Compact Fetcher (Every Minute)
```bash
python fetch_intraday_compact.py
```
- Fetches only today's 1-min data
- Appends new candles only
- Used for live price action monitoring

## Ashraf Filtering Logic

Applied to S&P 500 tickers before adding to master list:

### Price Action Filters
- ✅ **Gap %** > 1.5% from prior close (Gap Up or Gap Down)
- ✅ **Early Volume Spike**: 9:30–9:44 volume must exceed 115% of 5-day average
- ✅ **VWAP Reclaimed** OR Breakout above pre-market high

### Fundamental Filters
- ✅ **Market Cap** > $2B
- ✅ **Float** > 100M shares (avoids low float < 20M)
- ✅ **Stock Price** > $5

## Data Specifications

| Interval | Rows | Purpose |
|----------|------|---------|
| Daily | 200 | AVWAP anchors, swing analysis |
| 30min | 500 | Breakout windows, ORB, EMA pulls |
| 1min | Past 7 days | Pre-market VWAP, early volume (excludes today's 9:30–9:45 for volume averages) |

## Storage Structure

All data saved to DigitalOcean Spaces:
```
/data/intraday/TSLA_1min.csv
/data/intraday_30min/TSLA_30min.csv  
/data/daily/TSLA_daily.csv
```

## Real-Time Price Endpoint

For live price checks (TP/SL validation):
```python
from utils.alpha_vantage_api import get_real_time_price

price_data = get_real_time_price('TSLA')
current_price = price_data['price']
```

Uses Alpha Vantage Global Quote function.

## Usage Examples

### Generate Master Ticker List
```bash
# Daily at 6:00 AM ET
python generate_master_tickerlist.py
```

### Run Full Data Fetch
```bash
# After master list is generated
python fetch_daily.py
python fetch_30min.py 
# OR combined:
python jobs/update_all_data.py
```

### Run Intraday Updates
```bash
# Every minute during market hours
python fetch_intraday_compact.py
```

## Testing

Run the test suite to validate the system:
```bash
python test_ticker_management.py
```

## Environment Variables

Required for full functionality:
- `ALPHA_VANTAGE_API_KEY` - For data fetching
- `SPACES_ACCESS_KEY_ID` - For cloud storage
- `SPACES_SECRET_ACCESS_KEY` - For cloud storage  
- `SPACES_BUCKET_NAME` - For cloud storage
- `SPACES_REGION` - For cloud storage

## Files Structure

```
├── generate_master_tickerlist.py   # Main ticker filtering script
├── master_tickerlist.csv          # Generated unified ticker list
├── fetch_daily.py                 # Daily data fetcher
├── fetch_30min.py                 # 30-min data fetcher  
├── fetch_intraday_compact.py      # Intraday compact fetcher
├── jobs/
│   ├── update_all_data.py         # Combined full fetch
│   └── update_intraday_compact.py # Legacy intraday fetcher
├── utils/
│   ├── alpha_vantage_api.py       # API functions + real-time price
│   └── helpers.py                 # read_master_tickerlist() + utilities
├── data/
│   ├── universe/sp500.csv         # S&P 500 source data
│   ├── daily/                     # Daily OHLCV data
│   ├── intraday/                  # 1-min OHLCV data
│   └── intraday_30min/            # 30-min OHLCV data
└── tickerlist.txt                 # Manual tickers (always included)
```

## API Rate Limits

Respects Alpha Vantage's 150 requests/minute limit with appropriate delays between requests.