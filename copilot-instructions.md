# Tradingstation — Repository-wide Copilot Instructions

These instructions guide GitHub Copilot coding agent and contributors on how to understand, build, test, and optimize this Python trading system and dashboard(s). They merge your existing standards with additional best practices for fast trading logic and web app performance. Additions are only made where non-conflicting.

---

## 🧠 Project Overview
This is a professional multi-strategy stock and crypto trading system called Tradingstation. It includes real-time screeners, daily signal generation, AVWAP-based swing logic, a Gap & Go day trading screener, and an automated Master Dashboard. It is deployed on DigitalOcean Spaces (nyc3) and uses Alpha Vantage as the main data provider.

Additional clarifications:
- Strategies: Gap & Go, ORB, AVWAP, EMA Pullback, Breakout, Exhaustion.
- Emphasis on modular, testable components and independently runnable jobs with robust logging and error handling.

---

## ⚙️ Goals & Priorities
- Blazing fast execution (every second matters)
- Modular Python design using separation of concerns
- Minimal memory footprint – no over-fetching or over-processing
- Real-time and end-of-day sync (intraday, 30min, and daily candles)
- Reliable Cloud Storage using boto3 and s3fs for reading/writing .csv and .json to DigitalOcean Spaces
- Centralized orchestrator for scheduling with schedule and pytz (ET timezone)

Additional performance goals:
- Prefer vectorized operations in pandas/numpy; avoid row-wise apply in hot paths.
- Favor precomputation in scheduled jobs; dashboards should read pre-aggregated data.
- Avoid duplicate network calls with caching and idempotent keys.
- Keep functions small, pure when possible, and testable.

Python version target: 3.11+ (ensure all tooling and CI use this baseline).

---

## 📁 Folder Structure
- orchestrator/: Master scheduling and runtime controller
- screeners/: Each screener (Gap & Go, ORB, AVWAP, etc.) as its own .py file
- jobs/: Data fetching and updating (1-min, 30-min, daily)
- dashboard/: Final signal consolidation logic for trade plans
- utils/: Shared logic, constants, helpers, logging
- data/: All CSV/JSON outputs, managed via DigitalOcean Spaces

Additional notes:
- tests/: Unit and integration tests with fixtures for strategy logic
- config source of truth in utils/config.py; common helpers in utils/helpers.py

---

## 📅 Timing Logic
- Daily Data Job: 1x per day at 5:00 PM ET
- 1-minute Intraday Updates: Every minute 24/7 for live tickers
- 30-minute Candles: Every 15 minutes, from 4:00 AM – 8:00 PM ET
- Gap & Go Screener:
  - Pre-market: Every 30 min 7:00–9:30 AM ET
  - Regular: Every minute 9:30–10:30 AM ET
- ORB Screener: Once at 9:40 AM ET
- AVWAP/Breakout/EMA/Exhaustion Screeners: Every 1 hour, from 6:00 AM – 7:00 PM ET
- Signal Consolidation: Every 5 minutes during market hours

Additional scheduling guidelines:
- All schedules must be timezone-aware (America/New_York via pytz); never use naive datetimes.
- Jobs must be idempotent: safe to re-run without duplicating or corrupting data.
- Use backoff and jitter when calling external APIs; respect provider rate limits.

---

## 📦 Cloud Storage (DigitalOcean Spaces)
- Bucket name: trading-station-data-youssef
- Region: nyc3
- Data files are stored as .csv and .json and read/written using boto3 and s3fs.
- Use compressed I/O where possible (gzip, feather).

Additional storage best practices:
- Prefer Parquet or Feather for large datasets for speed and size; keep CSV/JSON for compatibility and interchange.
- Organize objects with deterministic, idempotent keys:
  - Example: data/{symbol}/{interval}/{date}.parquet or signals/{strategy}/{date}.json.gz
- Use ETag/MD5 or content hashes to skip redundant uploads; honor Last-Modified where applicable.
- For Spaces access, configure endpoint URL: https://nyc3.digitaloceanspaces.com
- When writing via pandas + s3fs, set compression and dtype hints to reduce memory and I/O.

---

## 💻 Coding Standards
- All config settings must come from utils/config.py
- All reusable functions must live in utils/helpers.py
- Use type hints for all functions
- Use logging (not print) with appropriate log levels (info, warning, error)
- All scripts must run standalone AND via orchestrator
- Use tqdm only for development/testing output

Additional standards:
- Format with black, lint with flake8, type-check with mypy, sort imports with isort.
- No hardcoded secrets; use environment variables exclusively.
- Consistent module interfaces per strategy: minimal inputs, standardized outputs.
- Avoid tight coupling; share only via well-defined helpers or adapters.

---

## 🔁 Job Design Principles
- Never pull today’s data from cache — always fetch fresh candles for current day
- Always store compact (5-day) version of 1-min and 30-min candles
- For new tickers, fetch full history, then trim and shift to compact mode
- Avoid redundant writes to Spaces — use hash checking or timestamp guard

Additional job principles:
- Log start/end, input universe size, durations, and error context.
- Fail fast per job; never crash the scheduler/orchestrator.
- For I/O-heavy tasks, use asyncio or a thread pool; cap concurrency to respect rate limits.
- Deduplicate by stable keys (symbol, timestamp, provider) before persisting.

---

## 🔒 Error Handling
- Each job runs independently — failure of one should not break others
- Use timeout guards (30 min max per job)
- Avoid nested dependencies between screeners

Additional error handling:
- Wrap network calls with exponential backoff, jitter, and bounded retries.
- Never swallow exceptions silently; log error with context and re-raise if necessary.
- Use circuit breakers to pause noisy jobs on repeated failures with alerts logged.

---

## 🧪 Testing & Deployment
- Run run_all.py locally to simulate orchestrator schedule
- Final deployment is triggered via GitHub > DigitalOcean build pipeline
- Logs are stored and monitored for runtime anomalies

Additional testing:
- Unit tests for each screener with table-driven fixtures.
- Integration tests for data fetchers against stubbed providers or recorded cassettes.
- Performance tests for hot paths (e.g., dedup, joins) with target thresholds.

---

## 🌐 Web App/Dashboard Performance Best Practices
These apply whether using Streamlit, FastAPI, or another Python web UI.

- Caching:
  - Use in-memory + file/object cache: e.g., st.cache_data/st.cache_resource (Streamlit) or FastAPI dependencies with TTL.
  - Choose sensible TTLs (e.g., 30–60s intraday).
- Minimize reruns:
  - Debounce expensive actions; isolate user inputs from heavy compute.
  - Move heavy compute to scheduled jobs; web reads precomputed snapshots from Spaces.
- Rendering:
  - Avoid rendering very large tables; paginate and show top-N.
  - Prefer pre-aggregated metrics and charts fed from Parquet/Feather snapshots.
- Background work:
  - Offload non-critical refreshes to background tasks/threads with locks.
- Startup time:
  - Lazy-load models/resources; reuse clients (boto3, requests) as singletons.

---

## 📈 Trading Logic Performance Best Practices
- Dataframes:
  - Use vectorized operations; avoid row-wise apply in loops.
  - Downcast dtypes (float64→float32, int64→int32) where precision allows.
  - Use categoricals for repeated strings (symbols, sectors).
  - Prefer merge_asof for time-based joins; index by timestamp.
- I/O:
  - Read/write Parquet/Feather with compression (zstd/snappy); CSV only when needed.
  - Batch and chunk processing for large universes; avoid loading “everything”.
- API:
  - Cache GET responses with TTL to reduce duplicate calls.
  - Respect rate limits; use retry+backoff with jitter.
- Idempotency:
  - Deterministic object keys and dedup before writes.
  - “Fetch today → merge → dedup → compact” as a standard pipeline.

---

## 🔧 Environment and Configuration
Required environment variables (set locally and in CI/CD; secrets via GitHub/DO secrets):
- ALPHA_VANTAGE_API_KEY
- SPACES_ACCESS_KEY_ID
- SPACES_SECRET_ACCESS_KEY
- SPACES_BUCKET_NAME=trading-station-data-youssef
- SPACES_REGION=nyc3
- SPACES_ENDPOINT_URL=https://nyc3.digitaloceanspaces.com
- TZ=America/New_York
- DEBUG_MODE=true|false
- TEST_MODE=auto|true|false

Install and quick checks:
```
pip install -r requirements.txt
# Optional dev tools:
pip install -r requirements-dev.txt || true

python - <<'PY'
import pandas, numpy, requests, boto3
print("Main dependencies OK")
PY

# Syntax pass
find . -name "*.py" -exec python -m py_compile {} \;
```

Quality gates:
```
black --check .
flake8 .
mypy .
isort --check-only .
pytest -q
```

---

## 🧩 Orchestrator Guidelines
- The orchestrator coordinates; it must not embed strategy logic.
- Each job is a small CLI entrypoint callable standalone and by the scheduler.
- Guard rails:
  - Skip if newer data already exists for the same window.
  - Global concurrency caps to avoid API bans.
  - Circuit breaker on repeated failures; surface clear logs.

---

## 📝 Logging
- Use the logging module; no print statements.
- Structured fields when possible: job, strategy, symbol_count, duration_ms, status, error_code, attempt, trace_id.
- Levels: INFO for lifecycle, DEBUG for details, WARNING for recoverables, ERROR for failures.

---

## ✅ PR Acceptance Checklist
- Code formatted (black), linted (flake8), type-checked (mypy), imports sorted (isort).
- Tests added/updated; pytest passes.
- No print statements; logging only.
- Functions small, pure when possible, and independently testable.
- No tight coupling introduced; screeners remain isolated.
- I/O is resilient: retries/backoff, idempotent writes, rate-limit aware.
- Performance aligned with guidelines (vectorization, caching, Parquet/Feather where appropriate).
- Documentation updated where relevant (README/dashboard help/comments).

---

## 🤖 Working with Copilot
- Prefer well-scoped issues with clear acceptance criteria and target files.
- Good tasks: bug fixes, small refactors, test coverage, logging, reliability, targeted perf wins.
- Avoid assigning Copilot: large cross-cutting refactors, security/critical paths without strict guidance, ambiguous tasks.
- Iterate via PR review comments; batch comments with “Start a review” and mention @copilot for changes.

---

## 🧭 Examples for Copilot
Do:
- “Optimize intraday dedup to O(n) with stable key; add unit tests for duplicate timestamp merges.”
- “Add hourly AVWAP screener using config-driven anchors; persist Parquet by date; update dashboard aggregator.”
- “Cache Alpha Vantage daily calls with 24h TTL; add exponential backoff and jitter on HTTP 429.”

Don’t:
- “Rewrite core architecture across orchestrator/screeners/jobs in one PR.”
- “Introduce new storage backends or remove boto3/s3fs.”
- “Block the dashboard while recomputing full universes.”

---

## 🧰 Copilot Environment Bootstrap
To speed up Copilot’s ephemeral environment and make builds deterministic, include .github/copilot-setup-steps.yml (provided alongside this file). It pre-installs Python 3.11, runtime dependencies, and runs quick sanity checks.
