# DigitalOcean App Platform Configuration

## Worker Run Command
```bash
python -m orchestrator.run_all
```

**Important:** No Working Directory override needed - the script computes project_root itself.

## Required Environment Variables

```bash
# Required for data fetching
ALPHA_VANTAGE_API_KEY=your_alpha_vantage_key

# Required for cloud storage  
SPACES_ACCESS_KEY_ID=DO801DFNAMQ3QXPGAQYH
SPACES_SECRET_ACCESS_KEY=your_secret_key_here
SPACES_BUCKET_NAME=trading-station-data-youssef
SPACES_REGION=nyc3
SPACES_ENDPOINT=https://nyc3.digitaloceanspaces.com

# Application environment
APP_ENV=production
DEPLOYMENT_TAG=<short_sha>

# Extended hours trading
FETCH_EXTENDED_HOURS=true
```

## Expected Startup Logs

When the fix is deployed, you should see:

```json
{"message":"Running orchestrator in production mode","environment":"production","extra":{"cwd":"/app","deployment":"<TAG>"}}
{"message":"Project root: /app"}
{"message":"Executing: Update 1min Intraday Data"}
{"message":"Command: /usr/bin/python -m jobs.data_fetch_manager --job intraday --interval 1min"}
```

**No more:** `sh: 1: cd: can't cd to /home/runner/work/Tradingstation/Tradingstation`
**No more:** `failed with exit code 512`

## Verification Commands

After deployment, test with:
```bash
# Bootstrap data
python -m jobs.data_fetch_manager --job daily --force-full
python -m jobs.data_fetch_manager --job intraday --interval all --force-full

# Health check
python -m tools.health_check
```