# Repository Variables Update - Canonical Layout

## Overview

This document outlines the required repository variable updates to normalize to canonical layout as specified in the environment standardization requirements.

## Required Variable Updates

The following repository Variables (not Secrets) need to be updated in the GitHub repository settings:

### Variable Changes Table

| Variable Name | Current/Default Value | New Canonical Value | Notes |
|---------------|----------------------|-------------------|-------|
| `SPACES_ENDPOINT` | `https://{SPACES_REGION}.digitaloceanspaces.com` | `https://nyc3.digitaloceanspaces.com` | Must be the region endpoint, not a bucket URL |
| `SPACES_REGION` | `nyc3` (default) | `nyc3` | ✅ Already correct |
| `SPACES_BUCKET_NAME` | *varies* | `trading-station-data-youssef` | Bucket name standardization |
| `SPACES_BASE_PREFIX` | `trading-system` (default) | `trading-system` | ✅ Already correct |
| `DATA_ROOT` | `data` (default) | `data` | ✅ Must be just "data" (no trading-system/ prefix) |
| `UNIVERSE_KEY` | `data/Universe/master_tickerlist.csv` (default) | `data/Universe/master_tickerlist.csv` | ✅ Case-sensitive: "Universe" with capital U |

## Validation Notes

### Critical Requirements:
- **`DATA_ROOT`** must be just `data` (we never prefix with `trading-system/` here; the write code prepends it)
- **`UNIVERSE_KEY`** path is case‑sensitive → `Universe` (capital U) and no `trading-system/` prefix
- **`SPACES_ENDPOINT`** must be the region endpoint, not a bucket URL

### Environment Variable Validation
The system now includes startup validation that will check these values at runtime:
- `validate_spaces_endpoint()` - ensures endpoint equals `https://nyc3.digitaloceanspaces.com` when region is `nyc3`
- `validate_paths()` - asserts `DATA_ROOT == "data"` and `UNIVERSE_KEY == "data/Universe/master_tickerlist.csv"`

## Manual Update Instructions

**To update these variables manually:**

1. Go to GitHub repository settings
2. Navigate to Settings → Secrets and Variables → Actions
3. Click on the "Variables" tab
4. Update each variable to the canonical value shown above

**To verify the updates:**

1. Run the new [Validate Environment workflow](../../actions/workflows/validate-env.yml)
2. Check that all variable validations pass ✅
3. Verify Spaces connectivity works with the updated values

## Deployment Impact

After updating these variables:
- The next deployment will use the canonical values
- Environment validation will pass during startup
- All data paths will be consistent across the system
- Discovery and maintenance workflows will work correctly

The application startup validation will fail loudly if any variables are incorrect, preventing deployment with wrong configurations.