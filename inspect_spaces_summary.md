# Inspect Spaces Summary

## Bucket Information
- **Bucket**: trading-station-data-youssef
- **Region**: nyc3
- **Endpoint**: https://nyc3.digitaloceanspaces.com
- **Base Prefix**: data/

## Connection Status
❌ **Cannot connect to Spaces** (placeholder credentials in use)

## Local Data Structure (for reference)
Based on canonical structure created locally:

### Object Counts Per Prefix
- **data/daily/**: 1 file (sample_daily.csv)
- **data/intraday/**: 1 file (sample_1min.csv)  
- **data/intraday_30min/**: 1 file (sample_30min.csv)
- **data/signals/**: 6 files (one per screener)
  - avwap_reclaim.csv
  - breakout.csv
  - ema_pullback.csv
  - exhaustion_reversal.csv
  - gapgo.csv
  - orb.csv
- **data/universe/**: 1 file (master_tickerlist.csv)

### Permissions
- **List Permission**: ❓ Cannot verify (credentials needed)
- **Write Permission**: ❓ Cannot verify (credentials needed)

## Recommendations
1. Verify SPACES_ACCESS_KEY_ID and SPACES_SECRET_ACCESS_KEY are set correctly
2. Test connection with: `aws s3 ls s3://trading-station-data-youssef/ --endpoint-url=https://nyc3.digitaloceanspaces.com`
3. Once connected, re-run inspect_spaces.py to get actual object counts

