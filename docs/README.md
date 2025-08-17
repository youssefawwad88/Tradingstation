# Tradingstation — Modular Python Trading System

## 📊 Overview

Tradingstation is a professional-grade, automated trading platform built in Python that combines real-time data fetching, advanced screeners, and a Streamlit dashboard for trade discovery and execution. The system is designed for intraday and swing trading strategies, hosted on DigitalOcean.

## 🚀 Quick Start

### Prerequisites
- Python 3.9+ (tested with Python 3.12.3)
- Network access for API calls and package installation

### Installation

```bash
# 1. Clone the repository
git clone https://github.com/youssefawwad88/Tradingstation.git
cd Tradingstation

# 2. Install dependencies (takes ~90 seconds - NEVER CANCEL)
pip install -r requirements.txt

# 3. Install development tools (optional)
pip install -r requirements-dev.txt

# 4. Set up environment variables
export ALPHA_VANTAGE_API_KEY="your_api_key_here"
export SPACES_ACCESS_KEY_ID="your_access_key"
export SPACES_SECRET_ACCESS_KEY="your_secret_key"
export SPACES_BUCKET_NAME="your_bucket_name"
export SPACES_REGION="nyc3"
```

### First Run

```bash
# Quick validation - tests core dependencies (1 second)
python3 -c "import pandas; import requests; import streamlit; print('✅ Core dependencies OK')"

# Start the dashboard (15 seconds startup)
streamlit run dashboard/streamlit_app.py --server.headless=true

# Access dashboard at http://localhost:8501
```

## 🏗️ Architecture

### Core Components

- **`core/`** - Base screener classes, configuration management, logging system
- **`utils/`** - API wrappers, data fetchers, helpers, configuration utilities  
- **`screeners/`** - Trading strategy implementations (Gap & Go, AVWAP, ORB, etc.)
- **`dashboard/`** - Streamlit web interface and interactive pages
- **`orchestrator/`** - Master scheduler and job coordination system
- **`jobs/`** - Automated data fetching and processing tasks
- **`tests/`** - Comprehensive unit and integration test suite

### Data Flow

```
Alpha Vantage API → Data Fetchers → Local Storage → Cloud Sync → Screeners → Dashboard
```

## 📈 Trading Strategies

### Implemented Strategies

1. **Gap & Go** (`screeners/gapgo.py`)
   - Umar Ashraf's gap trading methodology
   - Pre-market gap analysis and momentum detection

2. **AVWAP Reclaim** (`screeners/avwap.py`)
   - Brian Shannon's anchor VWAP reclaim logic
   - Institutional support/resistance levels

3. **Opening Range Breakout** (`screeners/orb.py`)
   - Traditional ORB with modern volume confirmation
   - Customizable time ranges (15min, 30min, 1hr)

4. **Breakout Squeeze** (`screeners/breakout.py`)
   - Bollinger Band squeeze detection
   - Volume expansion confirmation

5. **EMA Pullback** (`screeners/ema_pullback.py`)
   - EMA bounce strategies with trend confirmation
   - Multiple timeframe analysis

## 🔧 Configuration

### Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `ALPHA_VANTAGE_API_KEY` | ✅ | Alpha Vantage API key for data fetching |
| `SPACES_ACCESS_KEY_ID` | ✅ | DigitalOcean Spaces access key |
| `SPACES_SECRET_ACCESS_KEY` | ✅ | DigitalOcean Spaces secret key |
| `SPACES_BUCKET_NAME` | ✅ | DigitalOcean Spaces bucket name |
| `SPACES_REGION` | ✅ | DigitalOcean Spaces region (default: nyc3) |
| `DEBUG_MODE` | ❌ | Enable debug logging (default: false) |
| `TEST_MODE` | ❌ | Run in test mode without API calls |

### Configuration Files

- **`utils/config.py`** - Central configuration management
- **`pyproject.toml`** - Project metadata and tool configurations
- **`requirements.txt`** - Production dependencies with pinned versions
- **`requirements-dev.txt`** - Development and testing tools

## 🚀 Usage Examples

### Core Data Fetching

```bash
# Generate master ticker list (requires API key)
python3 generate_master_tickerlist.py

# Fetch daily data for all tickers (300+ second timeout)
python3 fetch_daily.py

# Fetch 30-minute intraday data (300+ second timeout)
python3 fetch_30min.py

# Real-time intraday updates (runs continuously)
python3 fetch_intraday_compact.py
```

### Strategy Execution

```bash
# Run all strategies through orchestrator
python3 orchestrator/run_all.py

# Run specific screener
python3 -c "from screeners.gapgo import GapGoScreener; GapGoScreener().run()"
```

### Dashboard Usage

```bash
# Start full dashboard
streamlit run dashboard/streamlit_app.py

# Navigate to specific pages:
# - 📈 Master Screener Hub: Main strategy overview
# - 🎯 Strategy Tabs: Individual strategy results
# - 📋 Trade Journal: Track and analyze trades
# - 📊 Performance Dashboard: Strategy performance metrics
# - 🔧 System Settings: Configuration management
# - ⏱️ Scheduler Monitor: Job status and scheduling
```

## 🧪 Testing & Quality

### Running Tests

```bash
# Run all tests (300+ second timeout for network tests)
python3 -m pytest tests/ -v

# Run specific test categories
python3 -m pytest tests/unit/ -v          # Unit tests only
python3 -m pytest tests/integration/ -v   # Integration tests only

# Run with coverage
python3 -m pytest tests/ --cov=. --cov-report=html
```

### Code Quality

```bash
# Format code
black . --line-length=88

# Sort imports
isort . --profile=black

# Lint code
flake8 . --max-line-length=88

# Type checking
mypy .

# Security audit
bandit -r . --exclude ./tests/
```

### Validation Commands

```bash
# Validate all Python files compile
find . -name "*.py" -exec python3 -m py_compile {} \;

# Test module imports
python3 -c "import sys; sys.path.append('.'); from utils.config import *"

# Verify dependencies
pip check
```

## 📊 Performance & Monitoring

### Expected Performance

- **Dependency installation**: ~90 seconds for main dependencies
- **Dashboard startup**: ~15 seconds to ready state
- **Data fetching**: Variable based on ticker count and API limits
- **Module imports**: <1 second
- **Syntax validation**: ~10 seconds for all files

### Monitoring

The system includes comprehensive monitoring:

- **Health checks**: Automated system health monitoring
- **Performance metrics**: Response times and success rates
- **Error tracking**: Centralized logging with different levels
- **Resource usage**: Memory and CPU monitoring

## 🚀 Deployment

### Production Environment

- **Platform**: DigitalOcean Apps
- **Branch Strategy**: `dev` → `main` (auto-deployed)
- **Scheduled Jobs**: 614 total jobs for comprehensive market coverage
- **Market Hours**: 9:30 AM - 4:00 PM ET for live operations
- **Data Retention**: Local + cloud backup for redundancy

### Environment Setup

```bash
# Production environment variables (example)
export ENVIRONMENT="production"
export LOG_LEVEL="INFO"
export CACHE_TTL="300"
export MAX_WORKERS="4"
export API_RATE_LIMIT="5"  # calls per minute
```

## 🛠️ Troubleshooting

### Common Issues

1. **Import Errors**
   ```bash
   # Solution: Always run from repository root
   cd /path/to/Tradingstation
   python3 your_script.py
   ```

2. **Missing Dependencies**
   ```bash
   # Solution: Reinstall with timeout
   pip install -r requirements.txt --timeout=300
   ```

3. **API Key Warnings**
   ```bash
   # Expected: Scripts show warnings but run in test mode
   # Solution: Set ALPHA_VANTAGE_API_KEY environment variable
   ```

4. **Dashboard Won't Start**
   ```bash
   # Solution: Check port availability and dependencies
   streamlit run dashboard/streamlit_app.py --server.port=8502
   ```

### Performance Issues

- **Slow API calls**: Check API rate limits and network connectivity
- **High memory usage**: Reduce ticker list size or enable data cleanup
- **Dashboard lag**: Clear cache and restart Streamlit server

## 📋 Development Guidelines

### Code Style

- **Line length**: 88 characters (Black formatter)
- **Import organization**: isort with Black profile
- **Type hints**: Use for all public functions
- **Documentation**: Docstrings for all classes and functions

### Testing

- **Coverage target**: >80% for core modules
- **Test categories**: Unit, integration, and performance tests
- **Mocking**: Use for external API calls in tests
- **Fixtures**: Reusable test data in `tests/conftest.py`

### Git Workflow

```bash
# 1. Create feature branch
git checkout -b feature/your-feature-name

# 2. Make changes with proper formatting
black . && isort . && flake8 .

# 3. Run tests
python3 -m pytest tests/ -v

# 4. Commit with descriptive message
git commit -m "feat: add your feature description"

# 5. Push and create PR
git push origin feature/your-feature-name
```

## 📚 Additional Resources

- **API Documentation**: [Alpha Vantage Docs](https://www.alphavantage.co/documentation/)
- **Cloud Storage**: [DigitalOcean Spaces](https://docs.digitalocean.com/products/spaces/)
- **Streamlit Guide**: [Streamlit Documentation](https://docs.streamlit.io/)
- **Trading Concepts**: Strategy-specific documentation in `docs/strategies/`

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Follow code style guidelines
4. Add comprehensive tests
5. Update documentation
6. Submit a pull request

## 📜 License

This project is proprietary. All rights reserved.

---

**⚠️ Important**: Always set timeouts of 120+ seconds for dependency installation and 300+ seconds for data fetching operations. Never cancel long-running operations as they may corrupt the installation.