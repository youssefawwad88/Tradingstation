# Data Schemas and API Specifications

## Overview

This document defines the standardized data schemas used throughout the TradingStation system.

## Core Data Schemas

### 1. Universe Schema (master_tickerlist.csv)

**Location**: `data/universe/master_tickerlist.csv`

| Column | Type | Description | Example |
|--------|------|-------------|---------|
| symbol | string | Stock ticker symbol | "NVDA" |
| name | string | Company name | "NVIDIA Corporation" |
| exchange | string | Exchange code | "NASDAQ" |
| market_cap | string | Market capitalization tier | "large" |
| sector | string | Industry sector | "Technology" |
| active | boolean | Whether symbol is actively traded | true |
| added_date | string | Date added to universe (YYYY-MM-DD) | "2024-01-15" |

### 2. OHLCV Data Schema

**Locations**: 
- `data/daily/{SYMBOL}.csv`
- `data/intraday/1min/{SYMBOL}.csv`
- `data/intraday/30min/{SYMBOL}.csv`

| Column | Type | Description | Example |
|--------|------|-------------|---------|
| timestamp | string | ISO timestamp (UTC) | "2024-01-15T14:30:00Z" |
| open | float | Opening price | 125.50 |
| high | float | High price | 127.25 |
| low | float | Low price | 124.80 |
| close | float | Closing price | 126.75 |
| volume | integer | Trading volume | 1234567 |

### 3. Signal Schema (Standardized)

**Locations**: `data/signals/{strategy}.csv`

#### Required Fields (All Strategies)
| Column | Type | Description | Example |
|--------|------|-------------|---------|
| timestamp_utc | string | Signal generation time (UTC) | "2024-01-15T14:30:00Z" |
| symbol | string | Stock ticker symbol | "NVDA" |
| direction | string | Trade direction | "long" or "short" |
| setup_name | string | Specific setup identification | "Gap Up Volume Confirmation" |
| score | float | Signal confidence (1-10) | 8.5 |
| entry | float | Recommended entry price | 126.50 |
| stop | float | Stop loss price | 123.25 |
| tp1 | float | First target price | 130.00 |
| tp2 | float | Second target price | 134.50 |
| tp3 | float | Third target price | 139.75 |

#### R-Multiple Fields
| Column | Type | Description | Example |
|--------|------|-------------|---------|
| r_multiple_tp1 | float | Risk multiple for TP1 | 1.08 |
| r_multiple_tp2 | float | Risk multiple for TP2 | 2.54 |
| r_multiple_tp3 | float | Risk multiple for TP3 | 4.08 |

#### Strategy-Specific Fields

##### Gap & Go (gapgo.csv)
| Column | Type | Description | Example |
|--------|------|-------------|---------|
| gap_percent | float | Gap size percentage | 4.2 |
| premarket_volume | integer | Premarket volume | 123456 |
| avg_volume_ratio | float | Volume vs average ratio | 2.8 |
| guard_time_met | boolean | 09:36 guard time satisfied | true |

##### Opening Range Breakout (orb.csv)
| Column | Type | Description | Example |
|--------|------|-------------|---------|
| or_window_minutes | integer | Opening range window | 15 |
| or_high | float | Opening range high | 127.50 |
| or_low | float | Opening range low | 125.25 |
| breakout_volume_ratio | float | Breakout volume vs average | 1.8 |

##### AVWAP Reclaim (avwap_reclaim.csv)
| Column | Type | Description | Example |
|--------|------|-------------|---------|
| avwap_anchor_date | string | Anchor VWAP date | "2024-01-10" |
| avwap_anchor_type | string | Type of anchor | "gap_day" |
| avwap_price | float | Anchor VWAP price | 124.75 |
| reclaim_quality | float | Reclaim strength (1-10) | 7.5 |
| time_above_avwap | integer | Minutes above AVWAP | 45 |

##### Breakout (breakout.csv)
| Column | Type | Description | Example |
|--------|------|-------------|---------|
| consolidation_days | integer | Days in consolidation | 8 |
| resistance_level | float | Breakout resistance level | 128.50 |
| range_compression | float | Range compression ratio | 0.65 |
| volume_confirmation | boolean | Volume confirms breakout | true |

##### EMA Pullback (ema_pullback.csv)
| Column | Type | Description | Example |
|--------|------|-------------|---------|
| ema_period | integer | EMA period used | 21 |
| ema_price | float | EMA price at signal | 125.80 |
| pullback_depth | float | Pullback depth percentage | 2.3 |
| trend_strength | float | Trend strength score (1-10) | 8.2 |

##### Exhaustion Reversal (exhaustion_reversal.csv)
| Column | Type | Description | Example |
|--------|------|-------------|---------|
| volume_spike_ratio | float | Volume spike vs average | 3.5 |
| price_extension | float | Extension from moving average | 8.7 |
| reversal_candle_type | string | Type of reversal candle | "doji" |
| momentum_divergence | boolean | Momentum divergence present | true |

### 4. AVWAP Anchor Schema

**Location**: `data/avwap_anchors/{SYMBOL}.csv`

| Column | Type | Description | Example |
|--------|------|-------------|---------|
| date | string | Anchor date (YYYY-MM-DD) | "2024-01-15" |
| symbol | string | Stock ticker symbol | "NVDA" |
| anchor_type | string | Type of anchor | "gap_day", "power_candle", "volume_spike" |
| vwap_price | float | VWAP price for the anchor | 125.75 |
| trigger_value | float | Value that triggered anchor | 4.2 |
| trigger_metric | string | Metric that triggered | "gap_percent" |
| quality_score | float | Anchor quality (1-10) | 8.5 |

### 5. Fetch Status Schema

**Location**: `data/manifest/fetch_status.json`

```json
{
  "last_updated": "2024-01-15T20:30:00Z",
  "universe": {
    "last_fetch": "2024-01-15T06:00:00Z",
    "symbol_count": 150,
    "status": "success"
  },
  "daily": {
    "last_fetch": "2024-01-15T06:15:00Z", 
    "symbols_updated": ["NVDA", "AAPL", "TSLA"],
    "status": "success"
  },
  "intraday_1min": {
    "last_fetch": "2024-01-15T20:25:00Z",
    "symbols_updated": ["NVDA", "AAPL"],
    "status": "partial",
    "errors": []
  },
  "intraday_30min": {
    "last_fetch": "2024-01-15T20:00:00Z",
    "symbols_updated": ["NVDA", "AAPL", "TSLA"],
    "status": "success"
  }
}
```

### 6. Dashboard Schema

**Location**: `data/dashboard/master_dashboard.csv`

```json
{
  "generated_at": "2024-01-15T20:30:00Z",
  "summary": {
    "total_signals": 15,
    "screeners_active": 6,
    "long_signals": 12,
    "short_signals": 3,
    "avg_score": 7.2,
    "unique_symbols": 8
  },
  "top_opportunities": [
    {
      "symbol": "NVDA",
      "screener": "gapgo",
      "setup_name": "Gap Up Volume Confirmation",
      "direction": "long",
      "score": 8.5,
      "composite_score": 9.2,
      "entry": 126.50,
      "stop": 123.25,
      "r_multiple": 2.1,
      "timestamp": "2024-01-15T14:30:00Z"
    }
  ],
  "risk_summary": {
    "total_risk_percent": 12.5,
    "position_count": 6,
    "risk_level": "MODERATE"
  }
}
```

## API Specifications

### Alpha Vantage API Usage

#### Daily Data
```
Function: TIME_SERIES_DAILY
Parameters: symbol, outputsize=full
Rate Limit: 5 calls/minute, 500 calls/day
```

#### Intraday Data
```
Function: TIME_SERIES_INTRADAY
Parameters: symbol, interval=1min|5min|15min|30min|60min, outputsize=full
Rate Limit: 5 calls/minute, 500 calls/day
```

### DigitalOcean Spaces API

#### File Structure
```
trading-system/
├── data/
│   ├── universe/
│   │   └── master_tickerlist.csv
│   ├── daily/
│   │   ├── NVDA.csv
│   │   ├── AAPL.csv
│   │   └── ...
│   ├── intraday/
│   │   ├── 1min/
│   │   │   ├── NVDA.csv
│   │   │   └── ...
│   │   └── 30min/
│   │       ├── NVDA.csv
│   │       └── ...
│   ├── signals/
│   │   ├── gapgo.csv
│   │   ├── orb.csv
│   │   └── ...
│   ├── avwap_anchors/
│   │   ├── NVDA.csv
│   │   └── ...
│   ├── dashboard/
│   │   └── master_dashboard.csv
│   └── manifest/
│       └── fetch_status.json
```

## Data Validation Rules

### Required Field Validation
- All timestamps must be in UTC ISO format
- Numeric fields must be positive (except for negative gaps)
- Direction must be "long" or "short"
- Scores must be between 1-10

### Business Logic Validation
- Entry price must be between high and low of signal day
- Stop loss must be on correct side of entry (below for long, above for short)
- Target prices must be in correct order (TP1 < TP2 < TP3 for long)
- R-multiples must be positive

### Data Quality Checks
- No duplicate timestamps for same symbol
- Volume must be positive
- Price fields must have reasonable values (no obvious errors)
- Signal timestamps must be within market hours or reasonable extended hours

## Error Handling

### Missing Data
- Return empty DataFrame with proper schema
- Log warning with specific missing data details
- Continue processing other symbols/timeframes

### Invalid Data
- Skip invalid records and log details
- Validate against schema before processing
- Return partial results when possible

### API Errors
- Implement exponential backoff retry logic
- Log all API errors with full context
- Fall back to test mode when API unavailable