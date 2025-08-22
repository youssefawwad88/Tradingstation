# Smoke Test Documentation for PR

This PR implements the paths helper, universe loader, CLI tools, and enhanced data fetch manager as specified in the requirements.

## Components Added

### 1. Paths Helper (`utils/paths.py`)
- **Environment Variables**: Reads `SPACES_BASE_PREFIX`, `DATA_ROOT`, `UNIVERSE_KEY`
- **Functions**:
  - `s3_key(*parts)` → builds complete S3 keys with base prefix
  - `key_intraday_1min(sym)` → `"data/intraday/1min/{sym}.csv"`
  - `key_intraday_30min(sym)` → `"data/intraday/30min/{sym}.csv"`
  - `key_daily(sym)` → `"data/daily/{sym}.csv"`
  - `universe_key()` → returns `UNIVERSE_KEY`

### 2. Universe Loader (`utils/universe.py`)
- **Main Function**: `load_universe()` returns `List[str]` of active symbols
- **Caching**: In-memory caching with `clear_universe_cache()` for testing
- **Fallback**: Uses `config.FALLBACK_TICKERS` on 404 or errors
- **Logging**: Success logging with s3_key, count, sample symbols

### 3. CLI Tools

#### Seed Universe (`tools/seed_universe.py`)
```bash
# Generate from tickers
python -m tools.seed_universe --tickers NVDA AAPL TSLA

# Upload existing CSV  
python -m tools.seed_universe --csv path/to/universe.csv
```
- Uploads CSV with ContentType=text/csv
- Shows confirmation: `s3://bucket/key size=<bytes> etag=<etag>`

#### Inspect Spaces (`tools/inspect_spaces.py`)
```bash
# Inspect specific symbols
python -m tools.inspect_spaces --symbols AAPL NVDA

# Inspect by prefix
python -m tools.inspect_spaces --prefix data/intraday/1min/
```
- Shows path, size, last_modified for each object
- Downloads last few bytes to show last 3 timestamps

### 4. Enhanced Data Fetch Manager
- **Path Building**: All S3 paths now use `utils.paths` helpers
- **Universe Loading**: Uses `utils.universe.load_universe()`
- **Write Policy**: 
  - Write if file didn't exist
  - Write if appended > 0
  - Write if mode == "heal" (even when appended == 0)
- **Enhanced Logging**: Each upload logs:
  ```
  provider=marketdata interval=1min|30min|daily mode=compact|heal
  appended=<n> final_rows=<m> latest_ts_utc=<iso>
  s3_key=<key> size=<bytes> etag=<etag>
  ```
- **Freshness Lines**: For 1min updates:
  ```
  health=fresh interval=1min symbol=<sym> age_sec=<seconds> rows=<m>
  ```

### 5. GitHub Actions

#### Seed Universe Workflow (`.github/workflows/seed-universe.yml`)
- Manual trigger with tickers input
- Uses secrets: `SPACES_ACCESS_KEY_ID`, `SPACES_SECRET_ACCESS_KEY`
- Uses variables: `SPACES_BUCKET_NAME`, `SPACES_REGION`, etc.

#### Redeploy Workflow (`.github/workflows/redeploy-do-app.yml`)
- Uses `digitalocean/action-doctl@v2`
- Triggers: `doctl apps create-deployment ${{ secrets.DO_APP_ID }}`

### 6. Configuration Updates
- Added environment variable support:
  - `SPACES_BASE_PREFIX` (default: "trading-system")
  - `DATA_ROOT` (default: "data") 
  - `UNIVERSE_KEY` (default: "data/universe/master_tickerlist.csv")

### 7. Cleanup
- Removed AlphaVantage `outputsize` terminology
- Uses config constants for retention policies

## Manual Smoke Test Steps

To test this implementation:

### 1. Set Environment Variables
```bash
export SPACES_ACCESS_KEY_ID=your_key_id
export SPACES_SECRET_ACCESS_KEY=your_secret_key  
export SPACES_BUCKET_NAME=your_bucket
export SPACES_REGION=nyc3
export SPACES_BASE_PREFIX=trading-system
export DATA_ROOT=data
export UNIVERSE_KEY=data/universe/master_tickerlist.csv
```

### 2. Seed Universe via GitHub Actions
- Go to Actions → Seed Universe
- Input: `NVDA AAPL TSLA`
- Verify output shows: `s3://bucket/key size=... etag=...`

### 3. Verify in Spaces UI
Check that `trading-system/data/universe/master_tickerlist.csv` exists with:
```csv
symbol,active,fetch_1min,fetch_30min,fetch_daily
NVDA,1,1,1,1
AAPL,1,1,1,1
TSLA,1,1,1,1
```

### 4. Run Data Fetch Manager
```bash
python -m jobs.data_fetch_manager --job intraday --interval 1min
```

Check logs for:
- `universe_loaded s3_key=... count=3 sample=[NVDA, AAPL, TSLA]`
- Per-symbol logs: `provider=marketdata interval=1min mode=... appended=... s3_key=... etag=...`
- Freshness logs: `health=fresh interval=1min symbol=AAPL age_sec=... rows=...`

### 5. Verify Data in Spaces
Check `trading-system/data/intraday/1min/AAPL.csv`:
- Has today's UTC timestamps
- Recently updated "Last modified"
- File size > 0 bytes

### 6. Test CLI Tools
```bash
# Inspect the uploaded data
python -m tools.inspect_spaces --symbols AAPL NVDA TSLA

# Should show paths, sizes, timestamps for each symbol/interval
```

### 7. Redeploy via Actions
- Go to Actions → Redeploy DigitalOcean App
- Verify deployment triggers successfully
- Check app logs for universe loading and data updates

## Acceptance Checklist

✅ All S3 keys built via `utils/paths.py`  
✅ `utils/universe.load_universe()` reads `UNIVERSE_KEY`, falls back cleanly  
✅ `tools/seed_universe.py` uploads and prints `s3://... size ... etag ...`  
✅ Manager writes per policy and logs `s3_key/etag/size/last_modified`  
✅ Freshness lines present for each 1-min update  
✅ No AlphaVantage terminology remains  
✅ `tools/inspect_spaces.py` shows last timestamps for symbols  
✅ GitHub Actions workflows added for seed and redeploy  

**Final verification**: Spaces should show today's data under `trading-system/data/...` with proper structure and recent timestamps.