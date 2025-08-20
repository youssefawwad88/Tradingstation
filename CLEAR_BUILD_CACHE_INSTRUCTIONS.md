# DigitalOcean Build Cache Clear Instructions

## Problem Statement
The production environment is in an inconsistent state where the orchestrator has been updated to VERSION 2.0 but the data_fetch_manager is still running an old version. This is causing the missing "--- DATA FETCH MANAGER VERSION 2.0 RUNNING ---" log message.

## Root Cause
DigitalOcean App Platform build cache is corrupted/stale, causing some files to be updated while others remain old.

## Solution: Manual DigitalOcean Dashboard Actions Required

### Step 1: Clear Build Cache
1. **Log in** to your DigitalOcean account dashboard
2. **Navigate** to the "Apps" section 
3. **Select** your tradestation application
4. **Go** to the "Settings" tab for your application
5. **Scroll down** to find the configuration for your tradestation-worker component
6. **Look for** an option related to the build process - should be a button or link that says **"Clear Build Cache"** or similar
7. **Click** the "Clear Build Cache" button

### Step 2: Trigger Fresh Deployment
1. **Go** to the "Actions" menu in the top-right corner of the app dashboard
2. **Select** "Deploy" to trigger a new, completely fresh deployment
3. **Wait** for the deployment to complete

### Step 3: Monitor Deployment
1. **Go** to the "Deployments" tab 
2. **Watch** the new deployment until it shows "Active" and successful
3. **Open** the "Runtime Logs" for your tradestation-worker component

## Success Criteria
After the fresh deployment, you MUST see all three diagnostic log messages appear in the correct order for a single job run:

### 1. Orchestrator Startup
```
--- ORCHESTRATOR VERSION 2.0 RUNNING [DEPLOYMENT v...] ---
```

### 2. Job Command Execution
```
ORCHESTRATOR: Preparing to execute command: '...data_fetch_manager.py --interval 1min'
```

### 3. Data Fetch Manager Startup
```
--- DATA FETCH MANAGER VERSION 2.0 RUNNING [DEPLOYMENT v...] ---
```

## Verification
- **Local verification**: Run `python3 verify_deployment.py` to confirm code is correct
- **Production verification**: Check runtime logs for all three messages above
- **Data processing**: Should see logs like "Downloading master_tickerlist.csv...", "Processing Ticker: SPY..."
- **Performance**: Job runtime should increase significantly as data processing resumes

## Next Steps After Success
1. ✅ Confirm all three diagnostic messages appear in production logs
2. ✅ Verify data processing logs resume (CSV downloads, ticker processing)
3. ✅ Check that CSV files in Spaces bucket begin updating
4. ✅ Monitor job performance and completion times
5. 🔄 Proceed with "Super Cleanup" task if needed

## Important Notes
- **DO NOT** modify code during this process - the issue is deployment environment, not code
- **WAIT** for complete deployment before checking logs
- **MONITOR** logs immediately after deployment completes
- **CLEAR CACHE FIRST** - this is the critical step that forces fresh build