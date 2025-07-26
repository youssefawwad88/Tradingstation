# 🧠 Tradingstation — Modular Python Trading System

A professional-grade, modular trading automation system designed for intraday and swing traders.  
This platform applies strategies inspired by Umar Ashraf and Brian Shannon to identify high-probability setups and deliver full trade plans, all built in Python and Google Colab.

---

## 🎯 Project Vision

To build a **fully autonomous**, institutional-grade trade discovery and execution platform that eliminates manual charting. The system analyzes real-time and historical data, identifies setups, calculates trade plans, and logs execution performance.

---

## 🧱 Folder Structure (In Progress)

📁 notebooks/ → Colab notebooks (strategy validation, testing)
📁 data/ → All market data (daily, intraday, signals)
📁 scripts/ → Core system logic (jobs, screeners, utilities)
📁 dashboards/ → Outputs and execution logs
📁 orchestrator/ → Master automation scripts (e.g., run_all.py)
📁 selectors/ → Screener input logic (e.g., earnings, gainers)
📁 utils/ → Shared tools, config, API management
📄 README.md → You're reading it
📄 .gitignore → Hides system/cache/temp files from repo


---

## 📈 Supported Strategies (So Far)

| Strategy             | Mentor           | Timeframe   | Trigger Conditions                        |
|----------------------|------------------|-------------|-------------------------------------------|
| Gap & Go             | Umar Ashraf      | 1-minute    | Gap %, Pre-market High Breakout, Volume   |
| AVWAP Reclaim        | Brian Shannon    | 30-min/Daily| AVWAP reclaim + Volume Spike              |
| Opening Range Breakout (ORB) | Umar Ashraf | 1-minute    | Break of OR high/low with volume spike    |
| Breakout Squeeze     | Umar Ashraf      | Daily       | Bollinger Squeeze + Body% + Volume        |
| Exhaustion Reversal  | Ashraf / Shannon | Daily       | Gap Down + Multi-day selloff reversal     |
| EMA Pullback         | Custom (Inspired)| Daily       | Bounce off EMA21 + Trend confirmation     |

---

## ⚙️ Current System Highlights

- 🔁 **Auto Data Sync**: Fetches daily & intraday data from Alpha Vantage.
- 🎯 **Signal Generation**: Saves Gap & Go / AVWAP / ORB outputs to `signals/`.
- 🧠 **Trade Plan Logic**: Calculates Entry, Stop, Target based on risk multiples.
- 📊 **Modular Screeners**: Strategy logic lives in individual files under `screeners/`.
- 🧪 **Live Backtest Style**: System validates setups post-breakout with strict time rules.
- 📂 **Cloud Compatible**: Built to run in Google Colab and migrate to Firestore when needed.

---

## 🛠️ In Development

✅ Screeners are functional  
✅ Data fetchers are modular  
🟡 Orchestration layer (run_all.py) in progress  
🟡 Firestore migration to eliminate Google Sheets bottlenecks  
🟡 Full real-time dashboard (Execution + Signal Tracker)

---

## 📎 References & Mentors

- **Umar Ashraf** — [Gap & Go, ORB, Reversal strategies](https://www.youtube.com/@umarashraf)
- **Brian Shannon** — [Anchored VWAP, AVWAP Reclaim logic](https://www.youtube.com/@alphatrends)

---

## 🔒 Repo Status

This is a **private**, work-in-progress system intended for personal trading automation.  
All logic, architecture, and strategies are under continuous development and will evolve.

---

## 💬 Want to Collaborate Later?

Once complete, parts of this system may be opened to contributors or clients. Stay tuned for updates.

---
