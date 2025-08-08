# Deployment Guide: Standardized Spaces Paths & Data Retention

This guide provides step-by-step instructions for deploying the enhanced data retention system to DigitalOcean Apps.

## Step 1: DigitalOcean App Environment Variables

Add these environment variables to your DigitalOcean App > Components > Worker > Settings > Environment Variables:

### Path Structure Variables
```
SPACES_BASE_PREFIX=data
SPACES_STRUCTURE_VERSION=v2
```

### Data Retention Configuration - KEEP TODAY'S DATA
```
INTRADAY_TRIM_DAYS=7
INTRADAY_EXCLUDE_TODAY=false
INTRADAY_INCLUDE_PREMARKET=true
INTRADAY_INCLUDE_AFTERHOURS=true
TIMEZONE=America/New_York
```

### Processing Controls
```
PROCESS_MANUAL_TICKERS=true
MAX_TICKERS_PER_RUN=25
INTRADAY_BATCH_SIZE=25
MARKET_HOURS_ONLY=false
SKIP_IF_FRESH_MINUTES=0
DEBUG_MODE=true
```

## Step 2: Deploy the Updated Code

1. **Redeploy** your DigitalOcean App after setting the environment variables
2. The new code will automatically use the enhanced data retention logic
3. **TODAY'S DATA WILL BE PRESERVED** by default

## Step 3: Optional Migration (One-time)

If you have existing data in old path structures, run the migration script:

### From Worker Console:
```bash
cd /workspace
python migrate_spaces_paths.py --dry-run
```

If the dry-run looks good, run the actual migration:
```bash
python migrate_spaces_paths.py
```

### Or set automated migration:
```bash
AUTOMATED_MIGRATION=true python migrate_spaces_paths.py
```

## Step 4: Verification

### From Worker Console:
```bash
cd /workspace
python verify_implementation.py
```

### Check Data Files:
```bash
# List TSLA files specifically (as mentioned in problem statement)
find . -name "*TSLA*" | xargs ls -la

# Check recent data
head -20 data/intraday/TSLA_1min.csv
tail -20 data/intraday/TSLA_1min.csv  # Should show TODAY'S data!
```

### Test Data Retention:
```bash
python test_data_retention.py
```

## Step 5: Monitor Logs

After deployment, monitor the application logs for:

- ✅ "TODAY'S DATA CONFIRMED PRESENT after filtering"
- ✅ "KEEPING TODAY'S DATA (default behavior)"
- ✅ "KEEPING ALL MARKET SESSIONS"

### Warning Signs to Watch For:
- ❌ "TODAY'S DATA MISSING after filtering"
- ❌ "EXCLUDING TODAY'S DATA"

## Expected Behavior Changes

### Before Implementation:
- Data retention might have excluded today's data
- Inconsistent path structures
- Manual configuration of market sessions

### After Implementation:
- **TODAY'S DATA IS ALWAYS PRESERVED** (unless explicitly configured otherwise)
- All data uses standardized `data/` paths
- Pre-market, regular hours, and after-hours data included by default
- Enhanced logging shows exactly what data is being kept/filtered
- Configurable retention via environment variables

## Key Files Changed:

1. **`utils/config.py`** - Added 13 new environment variables
2. **`utils/helpers.py`** - New `apply_data_retention()` and `is_today_present_enhanced()` functions
3. **`jobs/update_intraday_compact.py`** - Updated to use enhanced retention logic
4. **`migrate_spaces_paths.py`** - New migration script for old paths
5. **`test_data_retention.py`** - Test script for retention logic
6. **`verify_implementation.py`** - Comprehensive verification script

## Troubleshooting

### If Today's Data is Missing:
1. Check `INTRADAY_EXCLUDE_TODAY` environment variable (should be `false`)
2. Verify timezone configuration (`TIMEZONE=America/New_York`)
3. Check application logs for retention filtering messages
4. Run `python test_data_retention.py` to test the logic

### If Migration Fails:
1. Ensure DigitalOcean Spaces credentials are configured
2. Run with `--dry-run` first to check what would be migrated
3. Check logs for specific error messages

### If Processing Fails:
1. Verify all environment variables are set correctly
2. Check `DEBUG_MODE=true` for detailed logging
3. Run `python verify_implementation.py` to check configuration

## Configuration Options

### To Exclude Today's Data (NOT RECOMMENDED):
```
INTRADAY_EXCLUDE_TODAY=true
```

### To Include Only Regular Market Hours:
```
INTRADAY_INCLUDE_PREMARKET=false
INTRADAY_INCLUDE_AFTERHOURS=false
```

### To Change Retention Period:
```
INTRADAY_TRIM_DAYS=14  # Keep 14 days instead of 7
```

## Success Metrics

After deployment, you should see:
- ✅ All manual tickers processing successfully
- ✅ Today's data present in all ticker files
- ✅ Consistent data/ path structure
- ✅ Enhanced logging showing retention decisions
- ✅ Pre-market and after-hours data included
- ✅ No data loss during retention filtering

## Support

If you encounter issues:
1. Check the application logs first
2. Run the verification script: `python verify_implementation.py`
3. Test data retention: `python test_data_retention.py` 
4. Verify environment variables are set correctly in DigitalOcean App settings