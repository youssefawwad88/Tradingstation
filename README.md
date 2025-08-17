# 🧠 Tradingstation — Modular Python Trading System

A professional-grade, fully automated trading platform for intraday and swing strategies, combining real-time data, advanced screeners, and a dynamic Streamlit dashboard — all hosted on DigitalOcean.

> **🎉 Latest Update**: Comprehensive codebase audit completed with security hardening, performance optimization, and enhanced documentation.

---

## 🎯 Project Vision

To build an autonomous, institutional-style trade discovery and execution system that removes the need for manual charting. Inspired by Umar Ashraf and Brian Shannon, this platform scans, validates, and executes trade ideas with clean risk/reward logic.

---

## 📚 Documentation

### Complete Documentation Suite
- **[📖 Full Documentation](docs/README.md)** - Comprehensive setup and usage guide
- **[🔧 API Optimization](docs/api-optimization.md)** - Performance optimization strategies  
- **[🔒 Security Guide](docs/security.md)** - Security best practices and audit results

### Quick Links
- [Installation Guide](docs/README.md#-quick-start)
- [Trading Strategies](docs/README.md#-trading-strategies)
- [Configuration](docs/README.md#-configuration)
- [Troubleshooting](docs/README.md#-troubleshooting)

---

## 🚀 Quick Start

### Prerequisites
- Python 3.9+ (tested with Python 3.12.3)
- Alpha Vantage API key
- DigitalOcean Spaces credentials (optional)

## 🚀 New: Unified Ticker Management System

The system now includes a unified ticker management strategy that merges two sources:

- **Manual Tickers** (from `tickerlist.txt`) - Always included, no filters
- **S&P 500 Tickers** (filtered using Ashraf's breakout logic)

### Quick Start
```bash
# Generate master ticker list (daily at 6 AM ET)
python generate_master_tickerlist.py

# Run full data fetch (once per day)
python fetch_daily.py      # Daily data (200 rows)
python fetch_30min.py      # 30-min data (500 rows)

# Run intraday updates (every minute) - NEW IMPROVED VERSION
python jobs/intraday_fetcher.py

# Or test both 1min and 30min intervals
python jobs/intraday_fetcher.py --test
```

📖 **Full Documentation**: See [TICKER_MANAGEMENT.md](TICKER_MANAGEMENT.md) for complete details.

---

## 🆕 Improved Intraday Data Fetcher

The new `jobs/intraday_fetcher.py` script provides a comprehensive, intelligent solution for intraday data fetching:

### ✅ Key Features Fixed
- **No Infinite Loop**: Fixed recursive function call that caused crashes
- **Intelligent 10KB Rule**: Uses cloud storage file size to determine fetch strategy
- **Set-and-Forget Configuration**: Edit `QUICK_SETUP` values at the top of the file
- **Dual Interval Support**: Works with both 1-minute and 30-minute data
- **Self-Contained**: No external dependencies on utils modules

### 📋 Configuration
```python
QUICK_SETUP = {
    "DATA_INTERVAL": "1min",           # "1min" or "30min" 
    "TEST_TICKER": "AAPL",             # Stock symbol to fetch
    "API_KEY": "your_api_key_here",    # Your Alpha Vantage API key
    "FILE_SIZE_THRESHOLD_KB": 10,      # File size threshold for full vs compact
}
```

### 🎯 Smart Fetching Logic
- **Files ≤ 10KB**: Triggers `outputsize='full'` for complete historical data
- **Files > 10KB**: Uses `outputsize='compact'` for recent data only
- **Automatic Merging**: Intelligently combines new and existing data

---

## 🧱 Folder Structure

| Folder | Description |
|--------|-------------|
| `jobs/` | **Production-ready data fetchers and core processing scripts** |
| `core/` | Base screener classes, configuration management, logging |
| `utils/` | API wrappers, data fetchers, helpers, configuration |
| `screeners/` | Trading strategy implementations (Gap & Go, AVWAP, ORB, etc.) |
| `dashboard/` | Streamlit web interface and pages |
| `orchestrator/` | Master scheduler and job coordination |
| `data/` | Local data storage (created automatically) |
| `tests/` | Unit and integration tests (pytest configuration) |
| `README.md` | You’re reading it |
| `ticker_selectors/` | Pre-filter logic (market cap, float, avg vol) |

---

## 📈 Supported Trading Strategies

| Strategy | Mentor | Timeframe | Trigger Conditions |
|---------|--------|-----------|--------------------|
| Gap & Go | Umar Ashraf | 1-minute | Gap %, Pre-market high breakout, VWAP reclaim |
| AVWAP Reclaim | Brian Shannon | 30-min / Daily | Anchor reclaim + Volume spike |
| ORB (Opening Range Breakout) | Umar Ashraf | 1-minute | Break of opening range + volume confirmation |
| Breakout Squeeze | Umar Ashraf | Daily | Bollinger Band squeeze + Volume |
| Exhaustion Reversal | Custom | Daily | Oversold conditions after multiple red days |
| EMA Pullback | Custom | Daily | EMA21 bounce with trend confirmation |

---

## ⚙️ Core System Features

- 🔁 **Automated Data Pipeline**  
  Fetches and stores live daily + 1-min intraday candles (Alpha Vantage).

- 🧠 **Modular Signal Engines**  
  Each strategy runs as an independent script, outputs signals to CSV.

- 🎯 **Risk-Aware Trade Plans**  
  Every signal includes Entry, Stop, 2R/3R targets based on 1R risk logic.

- 📊 **Streamlit Dashboard (Live)**  
  Central cockpit to view signals, manage trades, and journal outcomes.

- ☁️ **Cloud-Native on DigitalOcean**  
  Fast and scalable deployment with zero Colab limits or manual execution.

---

## 🌐 Deployment & Branching (GitHub + DigitalOcean)

| Branch | Purpose | Auto-Deployed? |
|--------|---------|----------------|
| `dev` | Development zone — all features built and tested here | ❌ No |
| `main` | Production branch — auto-deployed to DigitalOcean app | ✅ Yes |

**Workflow Summary**  
→ Work in `dev`  
→ Validate features  
→ Create Pull Request into `main`  
→ Merge after confirmation  
→ Live app auto-updates via GitHub-DigitalOcean integration

---

## 🛠️ In Progress

- ✅ Fully automated screener outputs
- ✅ Modular architecture with clean strategy files
- ✅ AVWAP anchor automation
- 🟡 Trade Execution Journal (Streamlit)
- 🟡 Account performance tracking (PnL %, Win Rate, Drawdown)
- 🟡 AI-assisted premarket analysis (planned)

---

## 📎 Mentors & References

- **Umar Ashraf** — Gap & Go, ORB, Intraday Volume Confirmation
- **Brian Shannon** — AVWAP logic, anchor psychology, reclaim strength

---

## 🔒 Repo Status

This is a private alpha build for personal use. All systems are under continuous improvement. Future versions may be opened to external traders or contributors.

---

## 💬 Contact

If you're reading this and want to collaborate or join the beta, stay tuned — collaboration will open after core testing is complete.

