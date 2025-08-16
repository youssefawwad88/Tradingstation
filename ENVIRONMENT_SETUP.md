# Environment Configuration Guide

This document explains how to configure API keys and credentials for the Tradingstation system.

## Quick Setup

1. Copy the `.env` file in the repository root (already created with your provided credentials)
2. Add your missing `SPACES_SECRET_ACCESS_KEY` to the `.env` file
3. Install dependencies: `pip install -r requirements.txt`
4. Run any script to test: `python3 generate_master_tickerlist.py`

## Current Configuration

Based on the credentials you provided, the following have been configured:

### ✅ Alpha Vantage API
- **API Key**: `LF4A4K5UCTYB93VZ` (configured)
- **Purpose**: Fetches real market data for stocks

### ⚠️ DigitalOcean Spaces (Partial Configuration)
- **Access Key ID**: `DO801DFNAMQ3QXPGAQYH` (configured)
- **Bucket Name**: `trading-station-data-youssef` (configured)  
- **Region**: `nyc3` (configured)
- **Secret Access Key**: ❌ **MISSING** - You need to provide this

## Missing Credential

You provided most DigitalOcean Spaces credentials but are missing:

```bash
SPACES_SECRET_ACCESS_KEY=your_secret_key_here
```

This is the secret part of your DigitalOcean Spaces API key pair. You can find it in your DigitalOcean control panel under API > Spaces Keys.

## How to Add the Missing Key

1. Open the `.env` file in the repository root
2. Replace `YOUR_SECRET_ACCESS_KEY_HERE` with your actual secret key
3. Save the file

## Testing the Configuration

Once you add the secret key, test the full configuration:

```bash
# Test API key (should work now)
python3 generate_master_tickerlist.py

# Test cloud storage (will work after you add secret key)
python3 fetch_daily.py

# Test 30-minute data (will work after you add secret key) 
python3 fetch_30min.py
```

## Security Notes

- The `.env` file is automatically excluded from version control (via `.gitignore`)
- Never commit API keys or credentials to the repository
- Keep your credentials secure and don't share them

## What Happens Now

With the Alpha Vantage API key configured:
- ✅ Scripts will fetch real market data instead of running in test mode
- ✅ You'll get actual stock prices and trading data
- ✅ Master ticker list generation will work with real data

Once you add the DigitalOcean Spaces secret key:
- ✅ Data will be automatically uploaded to cloud storage
- ✅ Data will be downloaded from cloud storage when needed
- ✅ Full production functionality will be available

## Environment Variables Reference

All configuration is done through environment variables. The `.env` file is just a convenient way to set them locally.

```bash
# Required for data fetching
ALPHA_VANTAGE_API_KEY=your_alpha_vantage_key

# Required for cloud storage
SPACES_ACCESS_KEY_ID=your_access_key_id
SPACES_SECRET_ACCESS_KEY=your_secret_access_key
SPACES_BUCKET_NAME=your_bucket_name
SPACES_REGION=nyc3

# Optional settings
DEBUG_MODE=true
TEST_MODE=auto
WEEKEND_TEST_MODE_ENABLED=true
```