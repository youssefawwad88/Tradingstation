# TradingStation Strategy Guide

## Overview

This guide provides detailed information about the six modular strategy screeners implemented in the TradingStation system. Each screener follows a standardized signal output format while maintaining strategy-specific customization.

## Strategy Screeners

### 1. Gap & Go (Umar Ashraf Strategy)

**File**: `screeners/gapgo.py`  
**Signal File**: `data/signals/gapgo.csv`

#### Description
Identifies pre-market gap opportunities with volume confirmation and proper timing constraints.

#### Entry Criteria
- Gap size: 2-8% (configurable)
- Premarket volume > 50% of average daily volume
- Guard time: Must wait until 09:36 ET or later
- Volume confirmation on breakout

#### Signal Fields
- **Standard Fields**: All required fields (timestamp, symbol, direction, etc.)
- **Gap-Specific Fields**:
  - `gap_percent`: Size of the gap (positive for gap up, negative for gap down)
  - `premarket_volume`: Actual premarket volume
  - `avg_volume_ratio`: Premarket volume vs 20-day average ratio
  - `guard_time_met`: Boolean indicating if 09:36 guard time was satisfied

#### Usage
```bash
# Run with default settings
python3 screeners/gapgo.py

# Custom gap range and specific tickers
python3 screeners/gapgo.py --min-gap 3.0 --max-gap 6.0 --tickers NVDA,AAPL,TSLA

# Test mode with verbose output
python3 screeners/gapgo.py --test-mode --verbose
```

### 2. Opening Range Breakout (ORB)

**File**: `screeners/orb.py`  
**Signal File**: `data/signals/orb.csv`

#### Description
Captures breakouts from defined opening range windows with volume validation.

#### Entry Criteria
- Opening range: 5, 15, or 30 minutes (configurable)
- Breakout above/below range with volume > 1.5x average
- Price action: Clean breakout without false starts

#### Signal Fields
- **ORB-Specific Fields**:
  - `or_window_minutes`: Opening range window used (5, 15, or 30)
  - `or_high`: High price of opening range
  - `or_low`: Low price of opening range 
  - `breakout_volume_ratio`: Volume on breakout vs average

#### Usage
```bash
# Default 15-minute opening range
python3 screeners/orb.py

# 5-minute opening range
python3 screeners/orb.py --or-window 5

# 30-minute with volume threshold
python3 screeners/orb.py --or-window 30 --volume-threshold 2.0
```

### 3. AVWAP Reclaim (Brian Shannon Strategy)

**File**: `screeners/avwap_reclaim.py`  
**Signal File**: `data/signals/avwap_reclaim.csv`

#### Description
Identifies reclaims of Anchored VWAP (AVWAP) levels with institutional flow confirmation.

#### Entry Criteria
- Valid AVWAP anchor within lookback period
- Price reclaims and holds above AVWAP for minimum time
- Volume confirmation on reclaim
- Quality score > minimum threshold

#### AVWAP Anchor Types
1. **Gap Days**: Days with gaps > 3%
2. **Power Candles**: Large volume spikes with significant price movement
3. **Volume Spikes**: Volume > 3x average with momentum

#### Signal Fields
- **AVWAP-Specific Fields**:
  - `avwap_anchor_date`: Date of the AVWAP anchor
  - `avwap_anchor_type`: Type of anchor (gap_day, power_candle, volume_spike)
  - `avwap_price`: Price level of the AVWAP
  - `reclaim_quality`: Quality score of the reclaim (1-10)
  - `time_above_avwap`: Minutes price has been above AVWAP

#### Usage
```bash
# Default settings
python3 screeners/avwap_reclaim.py

# Minimum score and lookback
python3 screeners/avwap_reclaim.py --min-score 7 --lookback-days 20

# Find and update anchors first
python3 jobs/find_avwap_anchors.py --lookback-days 30
python3 screeners/avwap_reclaim.py
```

### 4. Breakout Strategy

**File**: `screeners/breakout.py`  
**Signal File**: `data/signals/breakout.csv`

#### Description
Identifies breakouts from consolidation patterns with range compression and volume confirmation.

#### Entry Criteria
- Consolidation period: 5-20 days (configurable)
- Range compression: Current range < 70% of average range
- Breakout with volume > 1.8x average
- Clear resistance/support level break

#### Signal Fields
- **Breakout-Specific Fields**:
  - `consolidation_days`: Number of days in consolidation
  - `resistance_level`: Price level of resistance being broken
  - `range_compression`: Ratio of current range to average range
  - `volume_confirmation`: Boolean for volume confirmation

#### Usage
```bash
# Default consolidation period
python3 screeners/breakout.py

# Custom consolidation and compression
python3 screeners/breakout.py --min-consolidation-days 7 --max-compression 0.8

# Volume threshold
python3 screeners/breakout.py --volume-threshold 2.5
```

### 5. EMA Pullback Strategy

**File**: `screeners/ema_pullback.py`  
**Signal File**: `data/signals/ema_pullback.csv`

#### Description
Captures trend continuation opportunities on pullbacks to key EMA levels.

#### Entry Criteria
- Strong uptrend: Price > EMA for minimum period
- Pullback to EMA level (within tolerance)
- Bounce confirmation with volume
- Trend strength score > threshold

#### Signal Fields
- **EMA Pullback-Specific Fields**:
  - `ema_period`: EMA period used (8, 21, 50)
  - `ema_price`: EMA price at signal time
  - `pullback_depth`: Percentage pullback from recent high
  - `trend_strength`: Trend strength score (1-10)

#### Usage
```bash
# Default 21 EMA
python3 screeners/ema_pullback.py

# 8 EMA with custom parameters
python3 screeners/ema_pullback.py --ema-period 8 --max-pullback 5.0

# 50 EMA for swing trades
python3 screeners/ema_pullback.py --ema-period 50 --min-trend-strength 8
```

### 6. Exhaustion Reversal Strategy

**File**: `screeners/exhaustion_reversal.py`  
**Signal File**: `data/signals/exhaustion_reversal.csv`

#### Description
Identifies potential reversal opportunities after climactic volume and price exhaustion.

#### Entry Criteria
- Volume spike: > 2.5x average volume
- Price extension: > 2 standard deviations from mean
- Reversal candlestick pattern
- Momentum divergence (optional)

#### Signal Fields
- **Exhaustion-Specific Fields**:
  - `volume_spike_ratio`: Volume vs average ratio
  - `price_extension`: Price extension in standard deviations
  - `reversal_candle_type`: Type of reversal candle (doji, hammer, etc.)
  - `momentum_divergence`: Boolean for momentum divergence

#### Usage
```bash
# Default settings
python3 screeners/exhaustion_reversal.py

# Custom volume and extension thresholds
python3 screeners/exhaustion_reversal.py --volume-spike-threshold 3.0 --extension-threshold 2.5

# Focus on specific reversal patterns
python3 screeners/exhaustion_reversal.py --require-divergence
```

## Standardized Signal Schema

All screeners output signals with this standardized format:

### Required Fields
| Field | Type | Description |
|-------|------|-------------|
| `timestamp_utc` | string | Signal generation time (UTC) |
| `symbol` | string | Stock ticker symbol |
| `direction` | string | "long" or "short" |
| `setup_name` | string | Specific setup identification |
| `score` | float | Signal confidence (1-10) |
| `entry` | float | Recommended entry price |
| `stop` | float | Stop loss price |
| `tp1`, `tp2`, `tp3` | float | Target prices |
| `r_multiple_tp1`, `r_multiple_tp2`, `r_multiple_tp3` | float | Risk multiples |

### Strategy-Specific Fields
Each screener adds its own fields as documented above.

## Configuration and Customization

### Global Configuration
Set via environment variables or command-line arguments:
- `--test-mode`: Use test data instead of live API calls
- `--verbose`: Enable detailed logging
- `--dry-run`: Show what would be done without executing
- `--tickers`: Limit to specific symbols

### Strategy-Specific Parameters
Each screener accepts strategy-specific parameters:

```bash
# Gap & Go
--min-gap 2.0 --max-gap 8.0 --min-volume-ratio 1.5

# ORB
--or-window 15 --volume-threshold 1.8

# AVWAP Reclaim  
--min-score 6 --lookback-days 15 --min-hold-time 10

# Breakout
--min-consolidation-days 5 --max-compression 0.7

# EMA Pullback
--ema-period 21 --max-pullback 3.0 --min-trend-strength 7

# Exhaustion Reversal
--volume-spike-threshold 2.5 --extension-threshold 2.0
```

## Risk Management

### Position Sizing
All signals include calculated position sizing based on:
- Account risk: 2% per trade (configurable)
- Stop loss distance
- Entry price

### R-Multiple Targets
Standardized target levels with risk multiples:
- **TP1**: Conservative target (1-2R)
- **TP2**: Moderate target (2-4R)  
- **TP3**: Aggressive target (4-8R)

### Quality Scoring
Signals include quality scores (1-10) based on:
- Setup strength
- Market context
- Volume confirmation
- Risk/reward ratio

## Integration with Dashboard

All signals automatically feed into the master dashboard:
- **Signal Aggregation**: Combined view of all opportunities
- **Risk Analysis**: Portfolio-level risk management
- **Trade Planning**: Automated position sizing and target calculation
- **Ranking**: Composite scoring across all strategies

## Monitoring and Alerts

### Signal Generation
- Signals saved to Spaces automatically
- Timestamped with UTC timezone
- Logged with structured JSON format

### Performance Tracking
- Hit rates by strategy
- Average R-multiples achieved
- Signal quality distribution
- Market condition correlation

## Best Practices

### Strategy Selection
- **High volatility markets**: Gap & Go, ORB
- **Trending markets**: EMA Pullback, Breakout
- **Institutional flow**: AVWAP Reclaim
- **Reversal conditions**: Exhaustion Reversal

### Risk Management
- Never risk more than 2% per trade
- Diversify across strategies and timeframes
- Monitor correlation between signals
- Adjust position sizes based on market volatility

### Execution Timing
- **Premarket**: Gap & Go signals
- **Market open**: ORB signals
- **Market hours**: AVWAP Reclaim, EMA Pullback
- **End of day**: Exhaustion Reversal for next day

## Troubleshooting

### No Signals Generated
1. Check data availability for symbols
2. Verify market conditions meet criteria
3. Review parameter settings
4. Check logs for errors

### Poor Signal Quality
1. Adjust quality thresholds
2. Review market conditions
3. Consider parameter optimization
4. Check for data issues

### Performance Issues
1. Use `--tickers` to limit scope
2. Enable `--test-mode` for development
3. Check API rate limits
4. Monitor system resources