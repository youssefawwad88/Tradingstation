# 🧠 Tradingstation — Modular Python Trading System

A professional-grade, fully automated trading platform for intraday and swing strategies, combining real-time data, advanced screeners, and a dynamic Streamlit dashboard — all hosted on DigitalOcean.

---

## 🎯 Project Vision

To build an autonomous, institutional-style trade discovery and execution system that removes the need for manual charting. Inspired by Umar Ashraf and Brian Shannon, this platform scans, validates, and executes trade ideas with clean risk/reward logic.

---

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

# Run intraday updates (every minute)
python fetch_intraday_compact.py
```

📖 **Full Documentation**: See [TICKER_MANAGEMENT.md](TICKER_MANAGEMENT.md) for complete details.

---

## 🧱 Folder Structure

| Folder | Description |
|--------|-------------|
| `notebooks/` | Development notebooks for strategy design/testing |
| `data/` | Price data: daily, intraday, signal outputs |
| `scripts/` | Modular strategy logic (Gap & Go, AVWAP, etc.) |
| `orchestrator/` | Master scheduler (e.g., `run_all.py`) |
| `selectors/` | Ticker discovery (earnings, gainers, custom filters) |
| `utils/` | Shared config, helpers, and API wrappers |
| `dashboards/` | Streamlit UI & trade journal exports |
| `ticker_selectors/` | Pre-filter logic (market cap, float, avg vol) |
| `README.md` | You’re reading it |
| `.gitignore` | Prevents caching/temp data from bloating repo |

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

