# System Discovery & Inventory — Tradingstation@6a350458 on 2025-08-23 18:18:46 UTC

**Quick Start**: Run [Validate Environment](../../actions/workflows/validate-env.yml) first to confirm vars and connectivity; then run System Discovery & Inventory (manual) for the full report.

## 1) Repo Architecture & Runtime

### Python Version Resolution

- **python_version_file**: 3.11.9

- **pyproject_toml**: >=3.11,<3.12

- **runtime_txt**: python-3.11.9

- **final_resolution**: python-3.11.9 (DigitalOcean runtime.txt)


**Final Resolution**: python-3.11.9 (DigitalOcean runtime.txt)

### Start/Entry Commands

- **orchestrator**: python orchestrator/run_all.py


### Key Directories

- **jobs/**: 4 Python files - Data fetching and processing

- **screeners/**: 7 Python files - Strategies: exhaustion_reversal, orb, ema_pullback, breakout, avwap_reclaim, gapgo

- **dashboard/**: 4 Python files - Signal consolidation and trade planning

- **orchestrator/**: 4 Python files - Master scheduling and runtime controller

- **utils/**: 15 Python files - Shared utilities and configuration

- **tools/**: 8 Python files

- **docs/**: 0 Python files


### Scheduling Information




## 2) CI/CD Workflows (GitHub Actions)

| Workflow | Triggers | Purpose | Needs Secrets | File |
|----------|----------|---------|---------------|------|

| System Discovery & Inventory (manual) |  | Ops - System discovery and inventory | DO_API_TOKEN, DO_APP_ID, SPACES_ACCESS_KEY_ID, SPACES_SECRET_ACCESS_KEY | discovery.yml |

| Repair S3 Paths |  | Ops - Maintenance and repair | SPACES_ACCESS_KEY_ID, SPACES_SECRET_ACCESS_KEY | repair-paths.yml |

| Redeploy DO App |  | Deploy - Application deployment | DO_API_TOKEN, DO_APP_ID | redeploy-do-app.yml |

| Inspect Spaces |  | Ops - Utility workflow | SPACES_ACCESS_KEY_ID, SPACES_SECRET_ACCESS_KEY | inspect-spaces.yml |

| Fetch Once |  | Ops - Utility workflow | MARKETDATA_TOKEN, SPACES_ACCESS_KEY_ID, SPACES_SECRET_ACCESS_KEY | fetch-once.yml |

| Validate Environment |  | Ops - Utility workflow | DO_API_TOKEN, DO_APP_ID, SPACES_ACCESS_KEY_ID, SPACES_SECRET_ACCESS_KEY | validate-env.yml |

| Seed Universe |  | Ops - Utility workflow | SPACES_ACCESS_KEY_ID, SPACES_SECRET_ACCESS_KEY | seed-universe.yml |


**Last Runs**: GitHub API access not available in this context

## 3) Environment & Secrets (names only, redacted values)

| Name | Used In (files) | Required? | Default | 
|------|----------------|-----------|---------|

| `SPACES_BASE_PREFIX` | 5 files | ✅ Yes | None |

| `DATA_ROOT` | 5 files | ✅ Yes | None |

| `UNIVERSE_KEY` | 4 files | ✅ Yes | None |

| `SPACES_ENDPOINT` | 3 files | ✅ Yes | None |

| `SPACES_REGION` | 5 files | ✅ Yes | None |

| `DO_APP_ID` | 4 files | ✅ Yes | None |

| `PROVIDER_DEGRADED_ALLOWED` | 3 files | ❌ No | true |

| `DO_API_TOKEN` | 1 files | ✅ Yes | None |

| `SPACES_ACCESS_KEY_ID` | 2 files | ✅ Yes | None |

| `SPACES_SECRET_ACCESS_KEY` | 2 files | ✅ Yes | None |

| `SPACES_BUCKET_NAME` | 2 files | ✅ Yes | None |

| `MARKET_TZ` | 1 files | ❌ No | America/New_York |

| `MARKETDATA_TOKEN` | 2 files | ✅ Yes | None |

| `INTRADAY_EXTENDED` | 1 files | ❌ No | false |

| `DEGRADE_INTRADAY_ON_STALE_MINUTES` | 2 files | ❌ No | 5 |

| `APP_ENV` | 1 files | ❌ No | development |

| `DEPLOYMENT_TAG` | 1 files | ✅ Yes | None |

| `FETCH_EXTENDED_HOURS` | 1 files | ❌ No | true |

| `TEST_MODE_INIT_ALLOWED` | 1 files | ❌ No | true |

| `DEBUG_MODE` | 1 files | ❌ No | false |

| `INTRADAY_1MIN_RETENTION_DAYS` | 1 files | ❌ No | 7 |

| `INTRADAY_30MIN_RETENTION_ROWS` | 1 files | ❌ No | 500 |

| `DAILY_RETENTION_ROWS` | 1 files | ❌ No | 200 |

| `MAX_TICKERS_PER_RUN` | 1 files | ❌ No | 25 |

| `API_RATE_LIMIT_CALLS_PER_MINUTE` | 1 files | ❌ No | 150 |

| `TIMEZONE` | 1 files | ❌ No | America/New_York |

| `MIN_GAP_LONG_PCT` | 1 files | ❌ No | 2.0 |

| `MIN_GAP_SHORT_PCT` | 1 files | ❌ No | -2.0 |

| `VOLUME_SPIKE_THRESHOLD` | 1 files | ❌ No | 1.15 |

| `BREAKOUT_TIME_GUARD_MINUTES` | 1 files | ❌ No | 6 |

| `ACCOUNT_SIZE` | 1 files | ❌ No | 100000 |

| `MAX_RISK_PER_TRADE_PCT` | 1 files | ❌ No | 2.0 |

| `MAX_DAILY_RISK_PCT` | 1 files | ❌ No | 6.0 |

| `DEFAULT_POSITION_SIZE_SHARES` | 1 files | ❌ No | 100 |

| `MIN_FILE_SIZE_BYTES` | 1 files | ❌ No | 10240 |

| `TEST_MODE` | 1 files | ❌ No | auto |

| `PROVIDER` | 2 files | ❌ No | marketdata |

| `MARKETDATA_ENDPOINT` | 1 files | ❌ No | https://api.marketdata.app |


**GitHub Secrets Status**: API access not available

## 4) DigitalOcean App Inventory


**Error**: Missing DO_APP_ID or DO_API_TOKEN


## 5) Spaces (S3) Structure & Freshness


**Error**: Missing Spaces credentials


## 6) Market Data Provider Wiring

### Configured Providers

- **router**:
  - File: utils/providers/router.py
  - Endpoints: None found
  - Token Variables: None found

- **marketdata**:
  - File: utils/providers/marketdata.py
  - Endpoints: MARKETDATA_ENDPOINT
  - Token Variables: MARKETDATA_TOKEN


### Configuration Variables
MARKETDATA_TOKEN, PROVIDER_DEGRADED_ALLOWED

## 7) Strategies & Trade Plan Outputs

### Screeners Present

- **exhaustion_reversal**: Exhaustion Reversal - Overextended reversal patterns
  - File: screeners/exhaustion_reversal.py
  - Outputs: No CSV outputs detected

- **orb**: Opening Range Breakout - First 30min range breakout strategy
  - File: screeners/orb.py
  - Outputs: No CSV outputs detected

- **ema_pullback**: EMA Pullback - Moving average pullback entries
  - File: screeners/ema_pullback.py
  - Outputs: No CSV outputs detected

- **breakout**: Breakout - Support/resistance level breakouts
  - File: screeners/breakout.py
  - Outputs: No CSV outputs detected

- **avwap_reclaim**: AVWAP Reclaim - Price reclaim of AVWAP anchor levels
  - File: screeners/avwap_reclaim.py
  - Outputs: No CSV outputs detected

- **gapgo**: Gap & Go - Pre-market gap detection and breakout confirmation
  - File: screeners/gapgo.py
  - Outputs: No CSV outputs detected


### Dashboard Consolidation

- **risk_management**: R-multiple calculation found

- **trade_planning**: Entry/Stop/Target logic found


## 8) Risks & Recommended Fixes

| Issue | Evidence | Impact | Recommended Fix | Owner |
|-------|----------|--------|----------------|-------|

| Inconsistent Python versions | Multiple version sources: ['python_version_file', 'pyproject_toml', 'runtime_txt', 'final_resolution'] | Deployment and development environment mismatches | Add runtime.txt with single Python version for DigitalOcean | Code |

| Required environment variables without defaults | 12 variables: SPACES_BASE_PREFIX, DATA_ROOT, UNIVERSE_KEY, SPACES_ENDPOINT, SPACES_REGION... | Runtime failures in production | Add validation in config.py or provide defaults | Code |

| DigitalOcean API access limited | Missing DO_APP_ID or DO_API_TOKEN | Cannot validate deployment configuration | Verify DO_API_TOKEN and DO_APP_ID in GitHub Secrets | GitHub Actions |

| Spaces connectivity issues | Missing Spaces credentials | Data storage and retrieval failures | Verify Spaces credentials in GitHub Secrets | GitHub Actions |


## Appendix

### Generated Files
- **DO App Spec**: Not available
- **Discovery Artifacts**: All files saved in `discovery_artifacts/` directory

### Analysis Metadata
- **Repository**: Tradingstation
- **Branch**: copilot/fix-f2e0f530-d951-417a-a0d5-a7f3cebd2c23
- **Commit**: 6a350458
- **Analysis Time**: 2025-08-23 18:18:46 UTC
- **Discovery Script**: tools/discovery.py

---
*This report was generated automatically by the System Discovery & Inventory workflow.*
*For manual execution: `python tools/discovery.py`*