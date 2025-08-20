# Deployment Verification Guide

## Overview

This guide provides step-by-step instructions for verifying a TradingStation deployment to ensure all components are working correctly.

## Pre-Deployment Checklist

### 1. Environment Setup
- [ ] All environment variables configured in DigitalOcean App Platform
- [ ] API keys and secrets properly set (see [ENV.md](ENV.md))
- [ ] Application deployed and running
- [ ] No build or deployment errors

### 2. Dependencies
- [ ] Python 3.9+ runtime available
- [ ] All packages from `requirements.txt` installed
- [ ] DigitalOcean Spaces bucket created and accessible
- [ ] Alpha Vantage API key valid and active

## Verification Steps

### Step 1: Basic System Verification

Run the deployment verification tool:

```bash
python3 tools/verify_deployment.py
```

**Expected Output:**
```
✅ Environment Variables: PASS
✅ Alpha Vantage Connectivity: PASS  
✅ Spaces Connectivity: PASS
✅ Data Structure: PASS
✅ System Components: PASS

🎉 DEPLOYMENT VERIFICATION SUCCESSFUL
```

**If Failed:** Review the specific failure details and fix issues before proceeding.

### Step 2: Health Check

Run comprehensive health check:

```bash
python3 tools/health_check.py
```

**Expected Behavior:**
- All major components show "healthy" or "degraded" status
- API connectivity confirmed
- Storage accessible
- No critical errors

**Acceptable Issues:**
- Some data may show as "stale" on first deployment
- Signal files may be empty initially

### Step 3: Data Pipeline Test

Test data fetching capabilities:

```bash
# Test universe update
python3 jobs/data_fetch_manager.py --job universe --test-mode

# Test daily data fetch
python3 jobs/data_fetch_manager.py --job daily --tickers AAPL --test-mode

# Test intraday data fetch  
python3 jobs/data_fetch_manager.py --job intraday_1min --tickers AAPL --test-mode
```

**Expected Behavior:**
- Commands complete without errors
- Data files created in Spaces
- Proper logging output
- No API rate limit errors

### Step 4: Screener Verification

Test each screener individually:

```bash
# Test each screener with limited scope
python3 screeners/gapgo.py --test-mode --tickers AAPL
python3 screeners/orb.py --test-mode --tickers AAPL
python3 screeners/avwap_reclaim.py --test-mode --tickers AAPL
python3 screeners/breakout.py --test-mode --tickers AAPL
python3 screeners/ema_pullback.py --test-mode --tickers AAPL
python3 screeners/exhaustion_reversal.py --test-mode --tickers AAPL
```

**Expected Behavior:**
- All screeners run without errors
- Signal files created (may be empty if no signals)
- Proper logging and status messages
- No data access errors

### Step 5: Dashboard Verification

Test dashboard generation:

```bash
python3 dashboard/master_dashboard.py --test-mode --hours-lookback 24
```

**Expected Behavior:**
- Dashboard runs without errors
- Aggregates signals from all screeners
- Generates trade plans and risk analysis
- Creates dashboard data file

### Step 6: Orchestrator Testing

Test orchestrator in dry-run mode:

```bash
# Test each market mode
python3 orchestrator/run_all.py --mode premarket --dry-run
python3 orchestrator/run_all.py --mode market --dry-run
python3 orchestrator/run_all.py --mode postmarket --dry-run
python3 orchestrator/run_all.py --mode daily --dry-run
```

**Expected Behavior:**
- Shows planned jobs for each mode
- No execution errors
- Proper job scheduling logic
- Appropriate job lists for each mode

### Step 7: End-to-End Smoke Test

Run comprehensive smoke test:

```bash
python3 tools/smoke_test_e2e.py
```

**Expected Behavior:**
- All test suites pass or show acceptable partial results
- No critical component failures
- System demonstrates end-to-end functionality

## Post-Verification Steps

### 1. Initial Data Population

If verification passes, populate initial data:

```bash
# Update universe
python3 jobs/data_fetch_manager.py --job universe

# Fetch initial daily data for key symbols
python3 jobs/data_fetch_manager.py --job daily --tickers NVDA,AAPL,TSLA,AMD,QQQ,SPY

# Find AVWAP anchors
python3 jobs/find_avwap_anchors.py --lookback-days 30
```

### 2. Run Initial Screening

Generate initial signals:

```bash
# Run premarket screeners
python3 screeners/gapgo.py
python3 screeners/orb.py

# Run market-hours screeners  
python3 screeners/avwap_reclaim.py
python3 screeners/breakout.py
python3 screeners/ema_pullback.py

# Generate dashboard
python3 dashboard/master_dashboard.py
```

### 3. Verify Signal Generation

Check that signals are properly generated and stored:

```bash
python3 tools/health_check.py --check signals
```

### 4. Set Up Monitoring

Configure monitoring and alerting:

```bash
# Set up health check monitoring (run periodically)
python3 tools/health_check.py

# Monitor API usage
python3 tools/health_check.py --check api

# Monitor storage usage
python3 tools/health_check.py --check storage
```

## Troubleshooting Common Issues

### Environment Variable Issues

**Problem:** Missing or incorrect environment variables

**Solution:**
1. Verify all required variables are set in DigitalOcean App Platform
2. Check variable names match exactly (case-sensitive)
3. Ensure no extra spaces or special characters
4. Redeploy application after changes

### API Connectivity Issues

**Problem:** Alpha Vantage API errors

**Symptoms:**
- "Invalid API key" errors
- Rate limit exceeded messages
- No data returned from API calls

**Solutions:**
1. Verify API key is correct and active
2. Check Alpha Vantage account status
3. Wait for rate limit reset
4. Use `--test-mode` to bypass API during testing

### Storage Access Issues

**Problem:** DigitalOcean Spaces connectivity

**Symptoms:**
- "Access denied" errors
- Cannot upload/download files
- Bucket not found errors

**Solutions:**
1. Verify Spaces credentials are correct
2. Check bucket name and region
3. Verify bucket exists and is accessible
4. Test with reduced scope initially

### Data Pipeline Issues

**Problem:** Data fetching failures

**Symptoms:**
- Empty data files
- Incomplete datasets
- Inconsistent timestamps

**Solutions:**
1. Check API quotas and limits
2. Verify symbol list is valid
3. Run with verbose logging
4. Use smaller symbol batches
5. Check market hours and data availability

### Component Missing Issues

**Problem:** System components not found

**Symptoms:**
- "File not found" errors
- Import errors
- Missing screener files

**Solutions:**
1. Verify complete deployment
2. Check file permissions
3. Ensure all directories created
4. Re-run deployment if necessary

## Performance Validation

### Response Time Benchmarks

Expected execution times (approximate):

- **Universe update**: 30-60 seconds
- **Daily data (100 symbols)**: 10-20 minutes
- **Individual screener**: 1-5 minutes
- **Dashboard generation**: 30-60 seconds
- **Health check**: 10-30 seconds

### Resource Usage

Monitor these metrics:
- **Memory usage**: Should remain stable
- **API call rate**: Stay within Alpha Vantage limits
- **Storage growth**: Predictable based on data retention
- **Error rates**: Should be minimal (<5%)

## Security Verification

### Secrets Management
- [ ] No secrets committed to code
- [ ] Environment variables properly secured
- [ ] API keys rotated regularly
- [ ] Access logs monitored

### Data Access
- [ ] Spaces bucket properly secured
- [ ] No public access to sensitive data
- [ ] Proper authentication for all operations
- [ ] Data encryption in transit and at rest

## Deployment Sign-Off

Complete this checklist before considering deployment verified:

- [ ] All verification steps passed
- [ ] Initial data populated successfully
- [ ] Signals generating properly
- [ ] Dashboard functioning
- [ ] Monitoring configured
- [ ] Documentation reviewed
- [ ] Team trained on operations
- [ ] Rollback plan prepared
- [ ] Production readiness confirmed

## Next Steps

After successful verification:

1. **Production Monitoring**: Set up continuous monitoring
2. **Operational Procedures**: Establish daily operational routines
3. **Performance Optimization**: Monitor and optimize based on usage
4. **Scaling Planning**: Plan for increased load and data volume
5. **Backup/Recovery**: Implement backup and disaster recovery procedures

## Support and Escalation

If issues persist after following this guide:

1. Check logs for detailed error messages
2. Review system resource usage
3. Verify network connectivity
4. Check for service outages (Alpha Vantage, DigitalOcean)
5. Escalate to technical team with:
   - Error logs and messages
   - Steps taken to resolve
   - System configuration details
   - Timeline of issues