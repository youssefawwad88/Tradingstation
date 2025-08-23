# Trading Station - Modular Python Trading System

A professional-grade, automated trading platform built in Python that combines real-time data fetching, advanced screeners, and a Streamlit dashboard for trade discovery and execution.

## Quick Start

### Prerequisites
- Python 3.9+ (tested with Python 3.12.3)
- DigitalOcean Spaces credentials
- MarketData.io API key

### Installation
```bash
# Install dependencies
pip install -r requirements.txt

# Set environment variables (see docs/ENV.md)
export MARKETDATA_TOKEN="your_key"
export SPACES_ACCESS_KEY_ID="your_key"
export SPACES_SECRET_ACCESS_KEY="your_secret"
export SPACES_BUCKET_NAME="your_bucket"
export SPACES_REGION="nyc3"
```

### Basic Usage
```bash
# Fetch data for all tickers
python3 jobs/data_fetch_manager.py

# Run specific interval
python3 jobs/data_fetch_manager.py --interval 1min

# Run screeners
python3 screeners/gapgo.py

# Start dashboard
streamlit run dashboard/master_dashboard.py
```

## Architecture

### System Discovery & Inventory

For comprehensive system analysis and troubleshooting, use the automated discovery workflow:

1. **GitHub Actions**: Go to Actions → Discovery → Run workflow → select branch (default: `dev`)
2. **Manual execution**: `python tools/discovery.py`
3. **Output**: Downloads `discovery_artifacts.zip` containing `DISCOVERY_REPORT.md` with complete system inventory

The discovery report includes repository structure, runtime configuration, CI/CD workflows, environment variables, DigitalOcean app status, Spaces structure, and recommended fixes.

### Data Layer
- **Single Source of Truth**: DigitalOcean Spaces CSV files
- **Unified Fetch Manager**: Self-healing data pipeline
- **Retention Policies**: Automated data trimming and validation

### Strategy Screeners
- **Gap & Go**: Umar Ashraf's momentum continuation strategy
- **Opening Range Breakout**: Classic ORB with volume confirmation
- **AVWAP Reclaim**: Brian Shannon's anchor reclaim methodology
- **Breakout**: Daily consolidation breakouts with volume
- **EMA Pullback**: Trend continuation pullbacks
- **Exhaustion Reversal**: Mean reversion after climactic moves

### Trade Management
- **R-Multiple Framework**: Consistent risk/reward calculations
- **Position Sizing**: Automated based on account risk tolerance
- **Trade Plans**: Entry, stop, and multiple target calculations

## Folder Structure

```
trading-station/
├── orchestrator/          # Master scheduling and coordination
├── jobs/                  # Data fetching and processing
├── screeners/            # Trading strategy implementations
├── dashboard/            # Streamlit web interface
├── utils/                # Core utilities and helpers
├── data/                 # Local development data (ignored in prod)
├── docs/                 # Documentation
└── tools/                # Deployment and health check tools
```

## Environment Variables

See `docs/ENV.md` for complete environment variable documentation.

## Documentation

- [Environment Setup](docs/ENV.md)
- [Data Schemas](docs/SCHEMAS.md)
- [Strategy Guide](docs/STRATEGY_GUIDE.md)
- [Operations Runbook](docs/RUNBOOK.md)
- [Deployment Guide](docs/DEPLOYMENT_VERIFICATION_GUIDE.md)

## License

MIT License - see LICENSE file for details.