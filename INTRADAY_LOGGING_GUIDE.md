# Enhanced Intraday Logging Guide

## Overview
The `update_intraday_compact` job now provides comprehensive logging to track ticker processing and storage locations.

## Production Logs (Always Visible)

### Environment Configuration
- Shows DigitalOcean Spaces configuration status
- Indicates whether data will be saved to cloud or local filesystem only
- Lists local save path

### Ticker Processing
- Lists all tickers being processed at job start
- Shows per-ticker processing status (✅/❌)
- Indicates save location for each ticker
- Reports manual ticker status (critical for production)

### Job Summary
- Success/failure rate for all operations
- List of successful vs failed tickers
- Final storage location confirmation

### Orchestrator Integration
The orchestrator logs now show concise summaries:
```
INFO - SUCCESS: update_intraday_compact finished
INFO -   📋 ORCHESTRATOR SUMMARY: Processed 5/11 tickers | Storage: Cloud (Spaces) + Local | Manual tickers: 8/10 OK
INFO - [timestamp] Job 'update_intraday_compact' completed successfully
```

## Debug Mode (--debug flag or DEBUG_MODE=true)

### Additional Information
- Detailed API request/response logging
- Enhanced environment variable verification
- File directory listings
- Verbose per-ticker processing details
- Debug-specific error troubleshooting

### Activation
```bash
# Command line
python jobs/update_intraday_compact.py --debug

# Environment variable
export DEBUG_MODE=true
python jobs/update_intraday_compact.py
```

## Key Information Provided

### ✅ What tickers are being updated
- Shows full ticker list at job start
- Per-ticker processing status
- Success/failure counts

### ✅ Where data is being saved
- Environment configuration status
- Storage location (Cloud + Local vs Local only)
- Per-ticker save confirmation

### ✅ Manual ticker status
- Critical ticker processing results
- Production readiness indicators
- Failure alerts for important tickers

## Sample Production Output
```
2025-08-07 18:27:20,654 - INFO - 📦 DigitalOcean Spaces Configuration Check:
2025-08-07 18:27:20,654 - INFO -     SPACES_BUCKET_NAME = trading-data-bucket
2025-08-07 18:27:20,654 - INFO -     Local save path: /workspace/data/intraday/
2025-08-07 18:27:20,654 - WARNING - ✅ DigitalOcean Spaces: FULLY CONFIGURED - Data will be uploaded to cloud
2025-08-07 18:27:20,654 - INFO - 🚀 Processing 11 tickers for intraday updates: ['NVDA', 'AAPL', 'TSLA', ...]
2025-08-07 18:27:20,654 - INFO - 📊 Storage configuration: Cloud (Spaces) + Local
2025-08-07 18:27:25,123 - INFO - 📊 TICKER COMPLETED: AAPL | 1min: ✅ | 30min: ✅ | Storage: Cloud (Spaces) + Local
2025-08-07 18:27:30,456 - INFO - 🏁 Enhanced Intraday Data Update Job Completed
2025-08-07 18:27:30,456 - INFO - 📈 Success rate: 20/22 operations
2025-08-07 18:27:30,456 - INFO - ✅ Successfully processed 10 tickers:
2025-08-07 18:27:30,456 - INFO -    📊 AAPL: Saved to Cloud (Spaces) + Local
```

## Troubleshooting
- Check environment configuration section for Spaces setup issues
- Review per-ticker status for individual failures
- Use debug mode for detailed troubleshooting
- Monitor manual ticker status for production readiness