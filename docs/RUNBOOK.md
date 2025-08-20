# TradingStation Operations Runbook

## System Overview

The TradingStation system is a modular algorithmic trading platform with:
- **Data Layer**: DigitalOcean Spaces as single source of truth
- **Processing Layer**: Self-healing data fetch manager and screeners
- **Orchestration Layer**: Automated scheduling for different market sessions
- **Dashboard Layer**: Trade plan generation and signal aggregation

## Daily Operations

### Premarket (4:00 AM - 9:30 AM ET)
```bash
# Manual execution
python3 orchestrator/run_all.py --mode premarket

# What runs:
# 1. Update universe (ticker list)
# 2. Fetch daily data for all symbols
# 3. Find AVWAP anchors
# 4. Gap & Go screener
# 5. Opening Range Breakout screener
```

### Market Hours (9:30 AM - 4:00 PM ET)
```bash
# Manual execution
python3 orchestrator/run_all.py --mode market

# What runs:
# 1. Update 1min intraday data
# 2. AVWAP Reclaim screener
# 3. Breakout screener
# 4. EMA Pullback screener
```

### Postmarket (4:00 PM - 8:00 PM ET)
```bash
# Manual execution
python3 orchestrator/run_all.py --mode postmarket

# What runs:
# 1. Update 30min intraday data
# 2. Exhaustion Reversal screener
# 3. Generate Master Dashboard
```

### Daily Maintenance (8:00 PM - 4:00 AM ET)
```bash
# Manual execution
python3 orchestrator/run_all.py --mode daily

# What runs:
# 1. Health check
# 2. Data integrity verification
```

## Manual Operations

### Individual Data Jobs
```bash
# Update specific data types
python3 jobs/data_fetch_manager.py --job universe
python3 jobs/data_fetch_manager.py --job daily --tickers NVDA,AAPL,TSLA
python3 jobs/data_fetch_manager.py --job intraday_1min --tickers NVDA
python3 jobs/data_fetch_manager.py --job intraday_30min --tickers NVDA

# Find AVWAP anchors
python3 jobs/find_avwap_anchors.py --lookback-days 30
python3 jobs/find_avwap_anchors.py --ticker NVDA --lookback-days 10
```

### Individual Screeners
```bash
# Run specific screeners
python3 screeners/gapgo.py --tickers TSLA,NVDA
python3 screeners/orb.py --or-window 15
python3 screeners/avwap_reclaim.py --min-score 7
python3 screeners/breakout.py --min-consolidation-days 5
python3 screeners/ema_pullback.py --ema-period 21
python3 screeners/exhaustion_reversal.py --volume-spike-threshold 2.0
```

### Dashboard Operations
```bash
# Generate master dashboard
python3 dashboard/master_dashboard.py --hours-lookback 24

# View current signals
python3 dashboard/master_dashboard.py --verbose
```

### Data Management
```bash
# Comprehensive data rebuilds
python3 jobs/backfill_rebuilder.py --operation rebuild-all
python3 jobs/backfill_rebuilder.py --operation rebuild-daily --symbols NVDA,AAPL
python3 jobs/backfill_rebuilder.py --operation verify --check-gaps

# Data cleanup (respects retention policies)
python3 jobs/backfill_rebuilder.py --operation cleanup
```

## Troubleshooting

### Common Issues

#### 1. API Rate Limiting
**Symptoms**: "Rate limit exceeded" errors
**Solution**:
```bash
# Check rate limit status
python3 tools/health_check.py --check-api-limits

# Wait for reset or use compact mode
python3 jobs/data_fetch_manager.py --job daily --force-compact
```

#### 2. Missing Data
**Symptoms**: Gaps in time series data
**Solution**:
```bash
# Detect and fill gaps
python3 jobs/backfill_rebuilder.py --operation detect-gaps
python3 jobs/backfill_rebuilder.py --operation fill-gaps --symbol NVDA
```

#### 3. No Signals Generated
**Symptoms**: Empty signal files
**Solution**:
```bash
# Check data availability
python3 tools/health_check.py --check-data

# Run screener with verbose logging
python3 screeners/gapgo.py --verbose --tickers NVDA,AAPL,TSLA

# Check if universe is updated
python3 jobs/data_fetch_manager.py --job universe --force
```

#### 4. Dashboard Not Updating
**Symptoms**: Stale dashboard data
**Solution**:
```bash
# Regenerate dashboard
python3 dashboard/master_dashboard.py --verbose

# Check signal availability
python3 tools/health_check.py --check-signals
```

### Emergency Procedures

#### Complete System Reset
```bash
# 1. Stop all running processes
pkill -f "python3"

# 2. Verify environment
python3 tools/verify_deployment.py

# 3. Rebuild all data
python3 jobs/backfill_rebuilder.py --operation rebuild-all

# 4. Test system
python3 tools/smoke_test_e2e.py
```

#### Data Corruption Recovery
```bash
# 1. Backup current state
python3 jobs/backfill_rebuilder.py --operation backup

# 2. Restore from known good state
python3 jobs/backfill_rebuilder.py --operation restore --date 2024-01-15

# 3. Verify integrity
python3 jobs/backfill_rebuilder.py --operation verify
```

## Monitoring

### Health Checks
```bash
# Comprehensive health check
python3 tools/health_check.py

# Specific checks
python3 tools/health_check.py --check-api
python3 tools/health_check.py --check-data
python3 tools/health_check.py --check-signals
python3 tools/health_check.py --check-storage
```

### Performance Monitoring
```bash
# Check API usage
python3 tools/health_check.py --api-usage

# Storage usage
python3 tools/health_check.py --storage-usage

# Data freshness
python3 tools/health_check.py --data-freshness
```

## Logging

Logs are stored in structured JSON format with:
- **Location**: Console output (captured by deployment platform)
- **Format**: JSON with timestamp, level, component, message
- **Levels**: DEBUG, INFO, WARNING, ERROR, CRITICAL

### Log Analysis
```bash
# View recent logs
tail -f /var/log/tradingstation.log | jq '.'

# Filter by component
grep '"component":"data_fetch_manager"' /var/log/tradingstation.log | jq '.'

# Error analysis
grep '"level":"ERROR"' /var/log/tradingstation.log | jq '.'
```

## Deployment

### Manual Deployment
```bash
# 1. Verify configuration
python3 tools/verify_deployment.py

# 2. Run smoke tests
python3 tools/smoke_test_e2e.py

# 3. Start orchestrator
python3 orchestrator/run_all.py
```

### Configuration Updates
1. Update environment variables in DigitalOcean App Platform
2. Redeploy application
3. Run verification: `python3 tools/verify_deployment.py`
4. Test with: `python3 tools/smoke_test_e2e.py`

## Data Retention Policies

- **1min intraday**: 7 days rolling
- **30min intraday**: 500 bars per symbol (~6 months)
- **Daily data**: 200 days per symbol
- **Signals**: 30 days rolling
- **Dashboard data**: 7 days rolling

Cleanup runs automatically during daily maintenance mode.