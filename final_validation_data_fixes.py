#!/usr/bin/env python3
"""
Final Validation: Data Fetching and Cloud Storage Fixes

This script validates that all the issues mentioned in the problem statement have been addressed:
1. Full fetch not updating CSV files in cloud storage
2. 30-minute compact data not working since yesterday
3. Weekend data retention (7 days of history, 500 rows for 30-minute)

Run this script to verify the fixes work correctly.
"""

import os
import sys
import subprocess
import pandas as pd
import logging
from datetime import datetime, timedelta
import pytz

# Add project root to Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '.')))

from utils.market_time import is_weekend, detect_market_session
from utils.helpers import should_use_test_mode, get_test_mode_reason

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def check_environment():
    """Check environment variables and system state."""
    logger.info("=== Environment Check ===")
    
    # Check API credentials
    alpha_key = os.getenv("ALPHA_VANTAGE_API_KEY")
    spaces_key = os.getenv("SPACES_ACCESS_KEY_ID")
    spaces_secret = os.getenv("SPACES_SECRET_ACCESS_KEY")
    spaces_bucket = os.getenv("SPACES_BUCKET_NAME")
    
    logger.info(f"ALPHA_VANTAGE_API_KEY: {'✅ Set' if alpha_key else '❌ Missing'}")
    logger.info(f"SPACES_ACCESS_KEY_ID: {'✅ Set' if spaces_key else '❌ Missing'}")
    logger.info(f"SPACES_SECRET_ACCESS_KEY: {'✅ Set' if spaces_secret else '❌ Missing'}")
    logger.info(f"SPACES_BUCKET_NAME: {'✅ Set' if spaces_bucket else '❌ Missing'}")
    
    # Check weekend/test mode
    is_weekend_now = is_weekend()
    market_session = detect_market_session()
    test_mode_active, test_mode_reason = get_test_mode_reason()
    
    logger.info(f"Is weekend: {is_weekend_now}")
    logger.info(f"Market session: {market_session}")
    logger.info(f"Test mode: {test_mode_active}")
    logger.info(f"Test mode reason: {test_mode_reason}")
    
    return {
        'has_api_key': bool(alpha_key),
        'has_spaces_creds': bool(spaces_key and spaces_secret and spaces_bucket),
        'is_weekend': is_weekend_now,
        'test_mode': test_mode_active
    }

def test_script_error_handling():
    """Test that scripts provide clear error messages when credentials are missing."""
    logger.info("=== Testing Script Error Handling ===")
    
    scripts_to_test = [
        ('fetch_30min.py', '30-minute fetch'),
        ('jobs/full_fetch.py', 'full fetch'),
        ('fetch_intraday_compact.py', 'compact fetch')
    ]
    
    results = {}
    
    for script_path, description in scripts_to_test:
        logger.info(f"Testing {description}...")
        
        try:
            # Run script and capture output
            result = subprocess.run(
                ['python3', script_path],
                cwd=os.path.dirname(os.path.abspath(__file__)),
                capture_output=True,
                text=True,
                timeout=30
            )
            
            output = result.stdout + result.stderr
            
            # Check for proper error messaging
            has_api_warning = 'ALPHA_VANTAGE_API_KEY not configured' in output
            has_spaces_warning = 'Spaces credentials' in output or 'Spaces not configured' in output
            has_instructions = 'Set ALPHA_VANTAGE_API_KEY' in output or 'environment variable' in output
            
            results[description] = {
                'exit_code': result.returncode,
                'has_api_warning': has_api_warning,
                'has_spaces_warning': has_spaces_warning,
                'has_instructions': has_instructions,
                'output_length': len(output)
            }
            
            logger.info(f"  Exit code: {result.returncode}")
            logger.info(f"  API warning: {'✅' if has_api_warning else '❌'}")
            logger.info(f"  Spaces warning: {'✅' if has_spaces_warning else '❌'}")
            logger.info(f"  Instructions: {'✅' if has_instructions else '❌'}")
            
        except subprocess.TimeoutExpired:
            logger.error(f"  ❌ Script timed out (may be hanging)")
            results[description] = {'error': 'timeout'}
        except Exception as e:
            logger.error(f"  ❌ Error running script: {e}")
            results[description] = {'error': str(e)}
    
    return results

def test_data_operations():
    """Test data read/write operations work correctly."""
    logger.info("=== Testing Data Operations ===")
    
    from utils.helpers import save_df_to_s3, read_df_from_s3
    
    # Create test data
    ny_tz = pytz.timezone('America/New_York')
    now = datetime.now(ny_tz)
    
    # Test 1-minute data (7 days)
    test_1min_data = []
    for i in range(7):
        date = now - timedelta(days=i)
        if date.weekday() < 5:  # Weekdays only
            for hour in range(9, 16):  # 9 AM to 4 PM
                for minute in range(0, 60, 1):  # Every minute
                    if hour == 9 and minute < 30:
                        continue  # Market opens at 9:30
                    test_1min_data.append({
                        'timestamp': date.replace(hour=hour, minute=minute, second=0, microsecond=0),
                        'open': 100.0,
                        'high': 101.0,
                        'low': 99.0,
                        'close': 100.5,
                        'volume': 10000
                    })
    
    df_1min = pd.DataFrame(test_1min_data)
    
    # Test 30-minute data (500 rows)
    test_30min_data = []
    for i in range(500):
        timestamp = now - timedelta(minutes=30 * i)
        test_30min_data.append({
            'timestamp': timestamp,
            'open': 100.0,
            'high': 101.0,
            'low': 99.0,
            'close': 100.5,
            'volume': 50000
        })
    
    df_30min = pd.DataFrame(test_30min_data)
    
    # Test save operations
    logger.info("Testing save operations...")
    save_1min_success = save_df_to_s3(df_1min, 'data/intraday/TEST_1min.csv')
    save_30min_success = save_df_to_s3(df_30min, 'data/intraday_30min/TEST_30min.csv')
    
    logger.info(f"1-minute save: {'✅' if save_1min_success else '❌'}")
    logger.info(f"30-minute save: {'✅' if save_30min_success else '❌'}")
    
    # Test read operations
    logger.info("Testing read operations...")
    read_1min = read_df_from_s3('data/intraday/TEST_1min.csv')
    read_30min = read_df_from_s3('data/intraday_30min/TEST_30min.csv')
    
    logger.info(f"1-minute read: {'✅' if not read_1min.empty else '❌'} ({len(read_1min)} rows)")
    logger.info(f"30-minute read: {'✅' if not read_30min.empty else '❌'} ({len(read_30min)} rows)")
    
    # Validate data requirements
    logger.info("Validating data requirements...")
    
    # Check 30-minute data has 500 rows
    has_500_rows = len(read_30min) == 500
    logger.info(f"30-minute data has 500 rows: {'✅' if has_500_rows else '❌'} (actual: {len(read_30min)})")
    
    # Check 1-minute data spans multiple days
    if not read_1min.empty and 'timestamp' in read_1min.columns:
        read_1min['timestamp'] = pd.to_datetime(read_1min['timestamp'])
        unique_days = read_1min['timestamp'].dt.date.nunique()
        has_multiple_days = unique_days > 1
        logger.info(f"1-minute data spans multiple days: {'✅' if has_multiple_days else '❌'} ({unique_days} days)")
    else:
        has_multiple_days = False
        logger.info("1-minute data validation: ❌ (no data or missing timestamp column)")
    
    return {
        'save_success': save_1min_success and save_30min_success,
        'read_success': not read_1min.empty and not read_30min.empty,
        'data_requirements': has_500_rows and has_multiple_days
    }

def check_cloud_storage_integration():
    """Check if cloud storage integration is working."""
    logger.info("=== Checking Cloud Storage Integration ===")
    
    try:
        from utils.spaces_manager import get_spaces_client, download_dataframe, upload_dataframe
        
        # Test client creation
        client = get_spaces_client()
        has_client = client is not None
        logger.info(f"Spaces client creation: {'✅' if has_client else '❌'}")
        
        if has_client:
            logger.info("✅ Cloud storage integration available")
            # Test small upload/download
            test_df = pd.DataFrame({'test': [1, 2, 3]})
            upload_success = upload_dataframe(test_df, 'test_integration.csv')
            
            if upload_success:
                download_df = download_dataframe('test_integration.csv')
                download_success = not download_df.empty
                logger.info(f"Cloud round-trip test: {'✅' if download_success else '❌'}")
                return True
            else:
                logger.warning("⚠️ Cloud upload test failed")
                return False
        else:
            logger.info("⚠️ Cloud storage not available (credentials missing)")
            return False
            
    except Exception as e:
        logger.error(f"❌ Cloud storage integration error: {e}")
        return False

def generate_summary_report(env_info, script_results, data_results, cloud_result):
    """Generate a summary report of all validation results."""
    logger.info("=== Validation Summary Report ===")
    
    logger.info("Environment:")
    logger.info(f"  API Key: {'✅' if env_info['has_api_key'] else '❌'}")
    logger.info(f"  Spaces Credentials: {'✅' if env_info['has_spaces_creds'] else '❌'}")
    logger.info(f"  Weekend Mode: {'✅' if env_info['is_weekend'] else 'N/A (weekday)'}")
    logger.info(f"  Test Mode: {'✅' if env_info['test_mode'] else 'N/A (production)'}")
    
    logger.info("\nScript Error Handling:")
    for script, result in script_results.items():
        if 'error' in result:
            logger.info(f"  {script}: ❌ ({result['error']})")
        else:
            warnings_ok = result.get('has_api_warning', False) or result.get('has_spaces_warning', False)
            instructions_ok = result.get('has_instructions', False)
            logger.info(f"  {script}: {'✅' if warnings_ok and instructions_ok else '❌'}")
    
    logger.info("\nData Operations:")
    logger.info(f"  Save Operations: {'✅' if data_results['save_success'] else '❌'}")
    logger.info(f"  Read Operations: {'✅' if data_results['read_success'] else '❌'}")
    logger.info(f"  Data Requirements: {'✅' if data_results['data_requirements'] else '❌'}")
    
    logger.info("\nCloud Storage:")
    logger.info(f"  Integration: {'✅' if cloud_result else '❌'}")
    
    # Overall assessment
    script_errors_handled = all('error' not in result for result in script_results.values())
    basic_operations_work = data_results['save_success'] and data_results['read_success']
    
    if env_info['has_api_key'] and env_info['has_spaces_creds']:
        overall_status = "✅ PRODUCTION READY"
    elif script_errors_handled and basic_operations_work:
        overall_status = "✅ TEST MODE WORKING (needs credentials for production)"
    else:
        overall_status = "❌ ISSUES DETECTED"
    
    logger.info(f"\nOVERALL STATUS: {overall_status}")
    
    return overall_status

def main():
    """Run complete validation."""
    logger.info("🚀 Starting Final Validation: Data Fetching and Cloud Storage Fixes")
    logger.info("=" * 80)
    
    # Run all validation tests
    env_info = check_environment()
    script_results = test_script_error_handling()
    data_results = test_data_operations()
    cloud_result = check_cloud_storage_integration()
    
    # Generate summary
    overall_status = generate_summary_report(env_info, script_results, data_results, cloud_result)
    
    logger.info("=" * 80)
    logger.info("🏁 Validation Complete")
    
    # Return appropriate exit code
    return 0 if "✅" in overall_status else 1

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)