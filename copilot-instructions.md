# 🧠 Project Overview

This is a professional multi-strategy stock and crypto trading system called **Tradingstation**. It includes real-time screeners, daily signal generation, AVWAP-based swing logic, a Gap & Go day trading screener, and an automated Master Dashboard. It is deployed on **DigitalOcean Spaces (nyc3)** and uses **Alpha Vantage** as the main data provider.

---

# ⚙️ Goals & Priorities

- **Blazing fast execution** (every second matters)
- **Modular Python design** using separation of concerns
- **Minimal memory footprint** – no over-fetching or over-processing
- **Real-time and end-of-day sync** (intraday, 30min, and daily candles)
- **Reliable Cloud Storage** using `boto3` and `s3fs` for reading/writing `.csv` and `.json` to DigitalOcean Spaces
- **Centralized orchestrator** for scheduling with `schedule` and `pytz` (ET timezone)

---

# 📁 Folder Structure

- `orchestrator/`: Master scheduling and runtime controller
- `screeners/`: Each screener (Gap & Go, ORB, AVWAP, etc.) as its own .py file
- `jobs/`: Data fetching and updating (1-min, 30-min, daily)
- `dashboard/`: Final signal consolidation logic for trade plans
- `utils/`: Shared logic, constants, helpers, logging
- `data/`: All CSV/JSON outputs, managed via DigitalOcean Spaces

---

# 📅 Timing Logic

- **Daily Data Job**: 1x per day at **5:00 PM ET**
- **1-minute Intraday Updates**: Every minute **24/7** for live tickers
- **30-minute Candles**: Every **15 minutes**, from **4:00 AM – 8:00 PM ET**
- **Gap & Go Screener**:
  - Pre-market: Every 30 min **7:00–9:30 AM ET**
  - Regular: Every minute **9:30–10:30 AM ET**
- **ORB Screener**: Once **at 9:40 AM ET**
- **AVWAP/Breakout/EMA/Exhaustion Screeners**: Every **1 hour**, from **6:00 AM – 7:00 PM ET**
- **Signal Consolidation**: Every **5 minutes** during market hours

---

# 📦 Cloud Storage (DigitalOcean Spaces)

- Bucket name: `trading-station-data-youssef`
- Region: `nyc3`
- Data files are stored as `.csv` and `.json` and read/written using `boto3` and `s3fs`.
- Use compressed I/O where possible (gzip, feather).

---

# 💻 Coding Standards

- **All config settings** must come from `utils/config.py`
- **All reusable functions** must live in `utils/helpers.py`
- Use **type hints** for all functions
- Use `logging` (not `print`) with appropriate log levels (info, warning, error)
- All scripts must run **standalone AND via orchestrator**
- Use `tqdm` only for development/testing output

---

# 🔁 Job Design Principles

- Never pull today's data from cache — always fetch fresh candles for current day
- Always store compact (5-day) version of 1-min and 30-min candles
- For new tickers, fetch full history, then trim and shift to compact mode
- Avoid redundant writes to Spaces — use hash checking or timestamp guard

---

# 🔒 Error Handling

- Each job runs **independently** — failure of one should not break others
- Use **timeout guards** (30 min max per job)
- Avoid nested dependencies between screeners

---

# 🧪 Testing & Deployment

- Run `run_all.py` locally to simulate orchestrator schedule
- Final deployment is triggered via GitHub > DigitalOcean build pipeline
- Logs are stored and monitored for runtime anomalies

---

# 🧠 AI/CoPilot Behavior

- Always **check the current project files and structure first** before writing code
- Assume that **existing project files are always the source of truth**
- Respect all recent changes and current design — do not overwrite or suggest old logic
- Suggest efficient, production-quality Python 3.11+ code
- Use only required imports per module — avoid bloated `import *`
- Refactor logic into reusable functions whenever possible
- Always optimize for speed, memory, and modularity

