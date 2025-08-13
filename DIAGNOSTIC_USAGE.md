# Diagnostic Script Usage Guide

## Overview
The `run_diagnostic_check.py` script is a standalone diagnostic tool that bypasses the deployment environment and directly executes the core logic in a controlled, local test.

## Location
- **File**: `run_diagnostic_check.py`
- **Directory**: Project root (`/home/runner/work/Tradingstation/Tradingstation/`)

## Purpose
To generate detailed logs from the two critical data engines:
1. **Data Health Check Engine** (`jobs.data_health_check`)
2. **Compact Update Engine** (`jobs.compact_update`)

## Usage

### Basic Execution
```bash
cd /home/runner/work/Tradingstation/Tradingstation
python run_diagnostic_check.py
```

### Expected Output
The script will produce detailed logs showing:

1. **Master Diagnostic Script Status**
   - Script startup and completion messages

2. **Data Health Check Engine**
   - Reads master ticker list (currently 9 tickers: AAPL, AMD, AMZN, GOOGL, MSFT, NFLX, NVDA, PLTR, TSLA)
   - Performs health checks on daily, 30-minute, and 1-minute data
   - Identifies deficient tickers (expected when no data files exist)
   - Attempts targeted full fetch recovery (will show API key warnings in test mode)

3. **Compact Update Engine**
   - Validates market hours (4:00 AM - 8:00 PM ET)
   - Checks trading window logic
   - Shows API configuration status

### Sample Output Indicators
Look for these key messages:
- `MASTER DIAGNOSTIC SCRIPT STARTING`
- `DATA HEALTH & RECOVERY JOB STARTING`
- `COMPACT UPDATE JOB STARTING`
- Ticker processing details with row counts
- Error handling for missing API credentials
- `MASTER DIAGNOSTIC SCRIPT COMPLETE`

## Features
- **Sequential Execution**: No schedule library dependency
- **Error Handling**: Catches and logs exceptions without crashing
- **Detailed Logging**: Comprehensive output for debugging
- **Standalone Operation**: Bypasses deployment environment issues
- **Console Output**: Forces logs to stdout for immediate visibility

## Test Mode Behavior
Without API credentials, the script will:
- Show credential warnings (expected behavior)
- Identify all tickers as deficient due to missing data files
- Demonstrate the complete health check workflow
- Validate market hours logic
- Complete successfully without crashes

This provides a complete test of the core logic without requiring actual API access.