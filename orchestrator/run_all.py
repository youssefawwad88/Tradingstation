import argparse
import asyncio
import logging
import os
import subprocess
import sys
import threading
import time
from datetime import datetime, timedelta

import pytz
import schedule

# Add project root to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# Strategic imports for new architecture
from config import get_config, validate_config
from core.data_manager import update_data
from core.logging_system import setup_logging, get_logger

from utils.helpers import (
    detect_market_session,
    get_test_mode_reason,
    should_use_test_mode,
    update_scheduler_status,
)

# Set up strategic logging system
config = get_config()
setup_logging()
logger = get_logger(__name__)

# Validate configuration on startup
if not validate_config():
    logger.critical("Configuration validation failed - exiting")
    sys.exit(1)

# Global configuration and flags
TEST_MODE_ACTIVE = False
TEST_MODE_REASON = ""
KILL_SWITCH_ACTIVE = False


def detect_and_log_test_mode():
    """
    Detect if test mode should be active and log the reason.

    Returns:
        bool: True if test mode is active
    """
    global TEST_MODE_ACTIVE, TEST_MODE_REASON

    TEST_MODE_ACTIVE, TEST_MODE_REASON = get_test_mode_reason()

    # Log the detection reason at startup
    logger.info("=" * 60)
    logger.info("MASTER ORCHESTRATOR STARTUP - MODE DETECTION")
    logger.info("=" * 60)
    logger.info(TEST_MODE_REASON)

    if TEST_MODE_ACTIVE:
        logger.info(
            "[TEST MODE] All jobs will simulate operations without live API calls"
        )
        logger.info("[TEST MODE] Detailed logging enabled for all operations")
    else:
        logger.info("[LIVE MODE] Jobs will make live API calls and process real data")

    logger.info("=" * 60)

    return TEST_MODE_ACTIVE


def run_job(script_path, job_name):
    """
    Runs a Python script sequentially and waits for it to complete.
    Returns True on success, False on failure.
    
    Args:
        script_path: Can be just script path or "script_path args" format
        job_name: Name for logging and status tracking
    """
    mode_prefix = "[TEST MODE]" if TEST_MODE_ACTIVE else "[LIVE MODE]"
    logger.info(f"{mode_prefix} Starting Job: {job_name}")
    update_scheduler_status(job_name, "Running")

    try:
        # Parse script path and arguments
        script_parts = script_path.split()
        script_only = script_parts[0]
        script_args = script_parts[1:] if len(script_parts) > 1 else []
        
        full_path = os.path.abspath(
            os.path.join(os.path.dirname(__file__), "..", script_only)
        )

        # Set environment variable to ensure jobs run in the same mode
        env = os.environ.copy()
        if TEST_MODE_ACTIVE:
            env["TEST_MODE"] = "enabled"
            env["MODE"] = "test"

        # Build command with script and arguments
        cmd = [sys.executable, full_path] + script_args

        result = subprocess.run(
            cmd,
            check=True,
            capture_output=True,
            text=True,
            timeout=1800,  # 30-minute timeout for any single job
            env=env,
        )

        logger.info(f"{mode_prefix} SUCCESS: {job_name} finished")
        if result.stdout:
            # Show orchestrator summary line for intraday jobs
            for line in result.stdout.strip().split("\n"):
                if "ORCHESTRATOR SUMMARY:" in line:
                    logger.info(f"  {mode_prefix} {line}")
                    break
                # Also log test mode completion messages
                elif TEST_MODE_ACTIVE and any(
                    keyword in line
                    for keyword in ["TEST MODE", "test data", "simulated"]
                ):
                    logger.info(f"  {mode_prefix} {line}")
            logger.debug(f"STDOUT for {job_name}: {result.stdout}")
        update_scheduler_status(job_name, "Success")
        return True

    except subprocess.TimeoutExpired:
        logger.error(f"{mode_prefix} TIMEOUT: {job_name} failed after 30 minutes")
        update_scheduler_status(job_name, "Fail", "Job timed out.")
        return False
    except subprocess.CalledProcessError as e:
        error_details = f"Exited with code {e.returncode}. STDERR: {e.stderr}"
        logger.error(f"{mode_prefix} ERROR: {job_name} failed - {error_details}")
        update_scheduler_status(job_name, "Fail", error_details)
        return False
    except Exception as e:
        logger.error(f"{mode_prefix} UNEXPECTED ERROR in {job_name}: {e}")
        update_scheduler_status(job_name, "Fail", str(e))
        return False


def run_daily_data_jobs():
    """Run daily data fetching and processing jobs with new two-engine architecture."""
    mode_prefix = "[TEST MODE]" if TEST_MODE_ACTIVE else "[LIVE MODE]"
    logger.info(f"{mode_prefix} Starting daily data jobs")

    # Stage 1: Generate master ticker list (manual sources only)
    if not run_job("generate_master_tickerlist.py", "generate_master_tickerlist"):
        return False

    # Stage 2: Full Fetch Engine - MUST complete before live updates begin
    logger.info(
        f"{mode_prefix} Running Full Fetch Engine (complete historical rebuild)"
    )
    if not run_job("jobs/full_fetch.py", "full_fetch"):
        logger.error(
            f"{mode_prefix} Full Fetch Engine failed - cannot proceed with live updates"
        )
        return False

    # Stage 3: Additional analysis jobs
    if not run_job("jobs/find_avwap_anchors.py", "find_avwap_anchors"):
        return False

    logger.info(f"{mode_prefix} Daily data jobs completed successfully")
    logger.info(
        f"{mode_prefix} Full Fetch Engine completed - live updates can now begin"
    )
    if TEST_MODE_ACTIVE:
        logger.info(
            "[TEST MODE] All daily operations simulated successfully - no live API calls made"
        )
    return True


def run_intraday_updates():
    """Run intraday data updates using the unified DataFetchManager system."""
    mode_prefix = "[TEST MODE]" if TEST_MODE_ACTIVE else "[LIVE MODE]"
    logger.info(
        f"{mode_prefix} Starting intraday updates (DataFetchManager)"
    )
    logger.info(
        f"{mode_prefix} ENHANCED SYSTEM: Using unified data fetch manager with self-healing"
    )
    result = run_job("jobs/data_fetch_manager.py --interval 1min", "data_fetch_manager")
    if TEST_MODE_ACTIVE and result:
        logger.info(
            "[TEST MODE] DataFetchManager simulation completed successfully"
        )
    return result


def run_30min_updates():
    """Run 30-minute intraday data updates using the unified DataFetchManager system."""
    mode_prefix = "[TEST MODE]" if TEST_MODE_ACTIVE else "[LIVE MODE]"
    logger.info(
        f"{mode_prefix} Starting 30-minute intraday updates (DataFetchManager)"
    )
    logger.info(
        f"{mode_prefix} ENHANCED SYSTEM: Using unified data fetch manager (30min interval only)"
    )
    result = run_job("jobs/data_fetch_manager.py --interval 30min", "data_fetch_manager_30min")
    if TEST_MODE_ACTIVE and result:
        logger.info(
            "[TEST MODE] DataFetchManager (30min) simulation completed successfully"
        )
    return result


def run_screener(screener_name, script_path):
    """Run a specific screener."""
    result = run_job(script_path, screener_name)
    if TEST_MODE_ACTIVE and result:
        logger.info(f"[TEST MODE] {screener_name} screener simulation completed")
    return result


def run_gap_go_screener():
    """Run Gap & Go screener."""
    return run_screener("gapgo", "screeners/gapgo.py")


def run_orb_screener():
    """Run ORB screener."""
    return run_screener("orb", "screeners/orb.py")


def run_hourly_screeners():
    """Run screeners that operate on hourly frequency."""
    mode_prefix = "[TEST MODE]" if TEST_MODE_ACTIVE else "[LIVE MODE]"
    logger.info(f"{mode_prefix} Starting hourly screeners")

    screeners = [
        ("avwap", "screeners/avwap.py"),
        ("ema_pullback", "screeners/ema_pullback.py"),
        ("breakout", "screeners/breakout.py"),
        ("exhaustion", "screeners/exhaustion.py"),
    ]

    results = []
    for name, script in screeners:
        try:
            result = run_screener(name, script)
            results.append(result)
        except Exception as e:
            logger.error(f"Error running {name} screener: {e}")
            results.append(False)

    success = all(results)
    if TEST_MODE_ACTIVE and success:
        logger.info(
            "[TEST MODE] All hourly screener simulations completed successfully"
        )
    return success


def run_consolidation():
    """Run final signal consolidation."""
    result = run_job("dashboard/master_dashboard.py", "master_dashboard")
    if TEST_MODE_ACTIVE and result:
        logger.info("[TEST MODE] Signal consolidation simulation completed")
    return result


def run_data_health_check():
    """Run data health check and auto-repair."""
    mode_prefix = "[TEST MODE]" if TEST_MODE_ACTIVE else "[LIVE MODE]"
    logger.info(f"{mode_prefix} Starting data health check")
    result = run_job("jobs/data_health_check.py", "data_health_check")
    if TEST_MODE_ACTIVE and result:
        logger.info("[TEST MODE] Data health check simulation completed")
    return result


def setup_production_schedule():
    """Set up all production job schedules."""
    ny_tz = pytz.timezone("America/New_York")

    # Import the data health check function
    from jobs.data_health_check import run_health_check

    # Daily data jobs - 5:00 PM ET
    schedule.every().day.at("17:00").do(run_daily_data_jobs).tag("daily")

    # Data health check - every 6 hours (4 times a day)
    schedule.every(6).hours.do(run_data_health_check).tag("health_check")

    # Intraday updates - every minute during extended hours (4:00 AM - 8:00 PM ET)
    for hour in range(4, 20):
        for minute in range(0, 60):
            time_str = f"{hour:02d}:{minute:02d}"
            schedule.every().day.at(time_str).do(run_intraday_updates).tag(
                "intraday_1min"
            )

    # 30-minute intraday updates - every 15 minutes
    for hour in range(4, 20):  # Extended hours coverage
        for minute in [0, 15, 30, 45]:
            time_str = f"{hour:02d}:{minute:02d}"
            schedule.every().day.at(time_str).do(run_30min_updates).tag(
                "intraday_30min"
            )

    # Gap & Go screener schedules
    # Pre-market: Every 30 minutes (7:00 AM - 9:30 AM)
    for hour in range(7, 9):
        for minute in [0, 30]:
            time_str = f"{hour:02d}:{minute:02d}"
            schedule.every().day.at(time_str).do(run_gap_go_screener).tag(
                "gapgo_premarket"
            )
    schedule.every().day.at("09:00").do(run_gap_go_screener).tag("gapgo_premarket")

    # Gap & Go during regular session: Every minute (9:30 AM - 10:30 AM)
    for hour in range(9, 11):
        for minute in range(0, 60):
            if hour == 9 and minute < 30:
                continue  # Skip before 9:30 AM
            if hour == 10 and minute > 30:
                continue  # Skip after 10:30 AM
            time_str = f"{hour:02d}:{minute:02d}"
            schedule.every().day.at(time_str).do(run_gap_go_screener).tag(
                "gapgo_regular"
            )

    # ORB screener - once at 9:40 AM
    schedule.every().day.at("09:40").do(run_orb_screener).tag("orb")

    # Hourly screeners - every hour during extended hours
    for hour in range(6, 20):
        time_str = f"{hour:02d}:00"
        schedule.every().day.at(time_str).do(run_hourly_screeners).tag(
            "hourly_screeners"
        )

    # Signal consolidation - every 5 minutes during market hours
    for hour in range(9, 16):
        for minute in range(0, 60, 5):
            if hour == 9 and minute < 30:
                continue
            time_str = f"{hour:02d}:{minute:02d}"
            schedule.every().day.at(time_str).do(run_consolidation).tag("consolidation")

    logger.info("Production schedule setup complete")


def should_run_jobs():
    """Check if we should run jobs based on market schedule and test mode."""
    # In test mode, we can run jobs any time for testing purposes
    if TEST_MODE_ACTIVE:
        return True

    session = detect_market_session()
    ny_tz = pytz.timezone("America/New_York")
    current_time = datetime.now(ny_tz)
    current_weekday = current_time.weekday()  # 0=Monday, 6=Sunday

    # Don't run on weekends in live mode
    if current_weekday >= 5:  # Saturday=5, Sunday=6
        return False

    # Run during market hours and extended hours (4 AM - 8 PM ET)
    market_start = current_time.replace(hour=4, minute=0, second=0, microsecond=0)
    market_end = current_time.replace(hour=20, minute=0, second=0, microsecond=0)

    return market_start <= current_time <= market_end


def main():
    """
    Strategic Trading System Orchestrator - Main Entry Point
    
    This is the command-line interface for the new strategic system.
    It parses command-line arguments and calls the appropriate data_manager.update_data() 
    function with the correct parameters, as specified in Phase 2.
    """
    # Parse command-line arguments
    args = parse_command_line_arguments()
    
    # Handle debug mode
    if args.debug:
        import logging
        logging.getLogger().setLevel(logging.DEBUG)
        logger.debug("Debug logging enabled")
    
    # Handle configuration validation
    if args.config_validate:
        logger.info("🔧 Validating configuration...")
        if validate_config():
            logger.info("✅ Configuration validation passed")
            return 0
        else:
            logger.error("❌ Configuration validation failed")
            return 1
    
    # Handle kill switch
    if args.kill_switch:
        success = execute_kill_switch()
        return 0 if success else 1
    
    # Handle data integrity check
    if args.data_integrity_check:
        success = run_data_integrity_check()
        return 0 if success else 1
    
    # Handle single ticker data update
    if args.ticker:
        logger.info(f"🎯 Strategic single-ticker mode: {args.ticker}")
        success = run_strategic_data_update(
            ticker=args.ticker,
            interval=args.interval,
            data_type=args.data_type,
            force_full=args.force_full
        )
        return 0 if success else 1
    
    # Default: Run in scheduled orchestrator mode
    logger.info("🚀 Starting Strategic Trading System Orchestrator")
    
    # Check kill switch status
    if KILL_SWITCH_ACTIVE:
        logger.critical("🚨 Kill switch is active - orchestrator cannot start")
        logger.critical("🔧 Restart required to resume operations")
        return 1
    
    # Override mode if specified
    if args.mode:
        config.MODE = args.mode.lower()
        logger.info(f"🎛️ Mode override: {args.mode}")
    
    # Detect and log test mode status at startup
    test_mode_active = detect_and_log_test_mode()
    
    mode_str = "Test Mode" if test_mode_active else "Production Mode"
    logger.info(f"🎯 Strategic Orchestrator starting in {mode_str}")
    
    # Set up production schedule
    setup_production_schedule()
    
    logger.info(f"⏰ Orchestrator running in {mode_str.lower()} - 24/7 operation")
    logger.info(f"📅 Scheduled jobs: {len(schedule.jobs)}")
    
    if test_mode_active:
        logger.info("🧪 [TEST MODE] Jobs will run with simulated data for testing purposes")
        logger.info("🧪 [TEST MODE] No live API calls will be made")
    
    # Main scheduling loop with strategic enhancements
    logger.info("🔄 Entering main orchestration loop")
    
    while True:
        try:
            # Check kill switch
            if KILL_SWITCH_ACTIVE:
                logger.critical("🚨 Kill switch activated - shutting down orchestrator")
                break
            
            if should_run_jobs():
                schedule.run_pending()
            else:
                if not test_mode_active:
                    logger.debug("⏰ Outside market hours - skipping job checks")
            
            time.sleep(30)  # Check every 30 seconds
            
        except KeyboardInterrupt:
            logger.info("⏹️ Orchestrator stopped by user")
            if test_mode_active:
                logger.info("🧪 [TEST MODE] Test session ended - all operations were simulated")
            break
        except Exception as e:
            logger.error(f"💥 Unexpected error in main loop: {e}")
            time.sleep(60)  # Wait 1 minute before retrying
    
    logger.info("🏁 Strategic Orchestrator shutdown complete")
    return 0


def parse_command_line_arguments():
    """
    Parse command-line arguments for the strategic orchestrator.
    
    Returns:
        Parsed arguments namespace
    """
    parser = argparse.ArgumentParser(
        description="Strategic Trading System Orchestrator",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --ticker AAPL --interval 1min --data-type INTRADAY
  %(prog)s --mode production --schedule
  %(prog)s --ticker MSFT --force-full
  %(prog)s --kill-switch
        """
    )
    
    # Data management arguments
    parser.add_argument(
        "--ticker",
        type=str,
        help="Stock ticker symbol to process"
    )
    
    parser.add_argument(
        "--interval",
        type=str,
        default="1min",
        choices=["1min", "5min", "15min", "30min", "60min"],
        help="Time interval for data (default: 1min)"
    )
    
    parser.add_argument(
        "--data-type",
        type=str,
        default="INTRADAY",
        choices=["INTRADAY", "DAILY", "QUOTE"],
        help="Type of data to fetch (default: INTRADAY)"
    )
    
    parser.add_argument(
        "--force-full",
        action="store_true",
        help="Force full fetch regardless of file size"
    )
    
    # Orchestrator mode arguments
    parser.add_argument(
        "--mode",
        type=str,
        choices=["production", "test"],
        help="Override mode (production or test)"
    )
    
    parser.add_argument(
        "--schedule",
        action="store_true",
        help="Run in scheduled mode (default behavior)"
    )
    
    # Safety features
    parser.add_argument(
        "--kill-switch",
        action="store_true",
        help="Emergency stop - kill all scheduled jobs"
    )
    
    parser.add_argument(
        "--data-integrity-check",
        action="store_true",
        help="Run data integrity validation"
    )
    
    # Configuration
    parser.add_argument(
        "--config-validate",
        action="store_true",
        help="Validate configuration and exit"
    )
    
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug logging"
    )
    
    return parser.parse_args()


def run_strategic_data_update(ticker: str, interval: str, data_type: str, force_full: bool = False):
    """
    Run strategic data update using the new unified architecture.
    
    Args:
        ticker: Stock ticker symbol
        interval: Time interval
        data_type: Type of data
        force_full: Force full fetch
        
    Returns:
        True if successful
    """
    logger.info(f"🚀 Strategic data update initiated for {ticker}")
    
    try:
        success = update_data(
            ticker=ticker,
            interval=interval,
            data_type=data_type,
            force_full=force_full
        )
        
        if success:
            logger.info(f"✅ Strategic data update completed successfully for {ticker}")
        else:
            logger.error(f"❌ Strategic data update failed for {ticker}")
            
        return success
        
    except Exception as e:
        logger.error(f"💥 Strategic data update error for {ticker}: {e}")
        return False


def execute_kill_switch():
    """
    Emergency kill switch - stop all scheduled jobs and prevent further actions.
    
    This is one of the advanced safety features specified in Phase 3.
    """
    logger.critical("🚨 KILL SWITCH ACTIVATED - Stopping all operations")
    
    try:
        # Clear all scheduled jobs
        schedule.clear()
        logger.info("✅ All scheduled jobs cleared")
        
        # Set global flag to prevent new jobs
        global KILL_SWITCH_ACTIVE
        KILL_SWITCH_ACTIVE = True
        
        # Log kill switch activation
        logger.critical("🛑 Kill switch active - all trading operations halted")
        logger.critical("🔧 To resume operations, restart the orchestrator")
        
        return True
        
    except Exception as e:
        logger.error(f"💥 Error activating kill switch: {e}")
        return False


def run_data_integrity_check():
    """
    Run comprehensive data integrity validation.
    
    This is one of the advanced safety features specified in Phase 3.
    """
    logger.info("🔍 Running comprehensive data integrity check")
    
    try:
        # Check critical tickers
        critical_tickers = config.DEFAULT_TICKERS[:5]  # Check first 5 tickers
        
        integrity_results = {}
        
        for ticker in critical_tickers:
            logger.info(f"Checking data integrity for {ticker}")
            
            # Use the intelligent data manager for validation
            from core.data_manager import intelligent_manager
            
            # Check intraday data integrity
            intraday_valid = intelligent_manager._validate_data_integrity(
                ticker, "1min", "INTRADAY"
            )
            
            # Check daily data integrity  
            daily_valid = intelligent_manager._validate_data_integrity(
                ticker, "1D", "DAILY"
            )
            
            integrity_results[ticker] = {
                "intraday": intraday_valid,
                "daily": daily_valid,
                "overall": intraday_valid and daily_valid
            }
            
            if integrity_results[ticker]["overall"]:
                logger.info(f"✅ Data integrity validated for {ticker}")
            else:
                logger.error(f"❌ Data integrity issues found for {ticker}")
        
        # Summary
        total_tickers = len(integrity_results)
        valid_tickers = sum(1 for r in integrity_results.values() if r["overall"])
        
        logger.info(f"📊 Data integrity check complete: {valid_tickers}/{total_tickers} tickers valid")
        
        if valid_tickers == total_tickers:
            logger.info("✅ All data integrity checks passed")
            return True
        else:
            logger.warning(f"⚠️ {total_tickers - valid_tickers} tickers failed integrity check")
            return False
            
    except Exception as e:
        logger.error(f"💥 Error in data integrity check: {e}")
        return False


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
