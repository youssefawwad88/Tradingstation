# Tradingstation — Modular Python Trading System

**ALWAYS follow these instructions first and fallback to additional search and context gathering only if the information here is incomplete or found to be in error.**

Tradingstation is a professional-grade, automated trading platform built in Python that combines real-time data fetching, advanced screeners, and a Streamlit dashboard for trade discovery and execution. The system is designed for intraday and swing trading strategies, hosted on DigitalOcean.

## Environment Setup & Dependencies

### Prerequisites
- Python 3.9+ (tested with Python 3.12.3)
- pip package manager
- Network access for API calls and package installation

### Install Dependencies
```bash
# Install main dependencies - takes ~90 seconds. NEVER CANCEL. Set timeout to 120+ seconds.
pip install -r requirements.txt

# Install development dependencies (optional, may have network timeouts)
pip install -r requirements-dev.txt
# Note: If dev tools installation fails due to network issues, core functionality still works
```

### Environment Variables Required
Set these environment variables for full functionality:
```bash
# Required for data fetching
export ALPHA_VANTAGE_API_KEY="your_api_key_here"

# Required for cloud storage (DigitalOcean Spaces)
export SPACES_ACCESS_KEY_ID="your_access_key"
export SPACES_SECRET_ACCESS_KEY="your_secret_key"
export SPACES_BUCKET_NAME="your_bucket_name"
export SPACES_REGION="nyc3"

# Optional configuration
export DEBUG_MODE="true"
export TEST_MODE="auto"
```

**NOTE**: The system runs in test mode without API keys (shows warnings but doesn't crash).

## Building & Testing

### No Traditional Build Step
This is a Python application that runs directly - there is no compilation or build process required.

### Validate Installation
```bash
# Quick dependency check - takes ~1 second
python3 -c "import pandas; import requests; import streamlit; print('Main dependencies OK')"

# Validate all Python files compile - takes ~10 seconds
find . -name "*.py" -exec python3 -m py_compile {} \; && echo "All Python files compile successfully"

# Test local module imports - takes ~1 second
python3 -c "import sys; sys.path.append('.'); from utils.config import *; print('Local modules can be imported')"
```

### Code Quality Tools
```bash
# Install dev tools individually if pip install -r requirements-dev.txt fails:
pip install pytest black flake8 mypy isort

# Run linting (if tools available)
black --check .
flake8 .
mypy .
isort --check-only .

# Run tests (if pytest available) - timing unknown, set timeout to 300+ seconds
python3 -m pytest tests/ -v
```

**WARNING**: Development tools may not install due to network connectivity issues. Core functionality works without them.

## Running the Application

### Core Data Fetching Scripts
```bash
# Generate master ticker list (requires API key)
python3 generate_master_tickerlist.py

# Fetch daily data (200 rows) - timing varies based on ticker count, set timeout to 300+ seconds
python3 fetch_daily.py

# Fetch 30-minute data (500 rows) - timing varies, set timeout to 300+ seconds  
python3 fetch_30min.py

# Fetch real-time intraday data (every minute during market hours)
python3 fetch_intraday_compact.py
```

### Streamlit Dashboard
```bash
# Start dashboard - takes ~15 seconds to startup. NEVER CANCEL.
streamlit run dashboard/streamlit_app.py --server.headless=true

# Dashboard will be available at http://localhost:8501
# Startup complete when you see "You can now view your Streamlit app"
```

### Testing Core Scripts (Without API Keys)
All scripts can be tested without API keys - they will show warnings but won't crash:
```bash
# Test mode execution (safe without credentials)
python3 generate_master_tickerlist.py  # Shows credential warnings
python3 fetch_daily.py                 # Shows API key warnings
```

## Repository Structure & Navigation

### Key Directories
- `core/` - Base screener classes, configuration management, logging
- `utils/` - API wrappers, data fetchers, helpers, configuration
- `screeners/` - Trading strategy implementations (Gap & Go, AVWAP, ORB, etc.)
- `dashboard/` - Streamlit web interface and pages
- `orchestrator/` - Master scheduler and job coordination
- `data/` - Local data storage (created automatically)
- `tests/` - Unit and integration tests (pytest configuration)

### Important Files
- `requirements.txt` - Main dependencies
- `requirements-dev.txt` - Development tools
- `pyproject.toml` - Project configuration, pytest setup, code quality settings
- `.pre-commit-config.yaml` - Pre-commit hooks configuration
- `master_tickerlist.csv` - Generated ticker list for processing
- `utils/config.py` - Central configuration and environment variables

### Trading Strategies Implemented
- **Gap & Go** (`screeners/gapgo.py`) - Umar Ashraf's gap trading strategy
- **AVWAP Reclaim** (`screeners/avwap.py`) - Brian Shannon's anchor reclaim logic
- **Opening Range Breakout** (`screeners/orb.py`) - Opening range breakouts
- **Breakout Squeeze** (`screeners/breakout.py`) - Bollinger Band squeezes
- **EMA Pullback** (`screeners/ema_pullback.py`) - EMA bounce strategies

## Validation & Testing

### Manual Validation Required
After making changes, ALWAYS test core functionality:

1. **Module Import Test**:
   ```bash
   python3 -c "import sys; sys.path.append('.'); from utils.config import *"
   ```

2. **Streamlit Dashboard Test**:
   ```bash
   # Start dashboard and verify it loads
   streamlit run dashboard/streamlit_app.py --server.headless=true
   # Should see "You can now view your Streamlit app" message
   ```

3. **Core Script Test**:
   ```bash
   # Test without API keys (should show warnings, not errors)
   python3 generate_master_tickerlist.py
   ```

### Production Deployment
- **Target Platform**: DigitalOcean Apps
- **Branch Strategy**: `dev` → `main` (auto-deployed)
- **Scheduled Jobs**: 614 total jobs for data fetching and analysis
- **Market Hours**: 9:30 AM - 4:00 PM ET for most operations

## Common Issues & Solutions

### Import Errors
- **Always run from repository root**: `cd /path/to/Tradingstation`
- **Module path**: Scripts add current directory to Python path automatically

### Missing Dependencies
- **Network timeouts**: Common with dev dependencies, retry or install individually
- **Core vs dev deps**: Main functionality works with just `requirements.txt`

### API Key Warnings
- **Expected behavior**: Scripts show warnings but run in test mode without keys
- **Full functionality**: Requires Alpha Vantage API key and DigitalOcean Spaces credentials

### Data Directory
- **Auto-created**: `data/` directory and subdirectories created automatically
- **Local storage**: Used for caching and local processing
- **Cloud sync**: Data synced to DigitalOcean Spaces when credentials available

## Performance Expectations

- **Dependency installation**: 90 seconds for main deps, variable for dev deps
- **Dashboard startup**: 15 seconds to ready state
- **Data fetching**: Varies by ticker count and API rate limits
- **Module imports**: <1 second
- **Syntax validation**: ~10 seconds for all files

**CRITICAL**: Always set timeouts of 120+ seconds for dependency installation and 300+ seconds for data fetching operations. NEVER CANCEL long-running operations.