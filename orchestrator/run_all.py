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

from utils.helpers import (
    detect_market_session,
    get_test_mode_reason,
    should_use_test_mode,
    update_scheduler_status,
)

# Set up logging
import tempfile

# Create secure temp directory for logs
TEMP_DIR = tempfile.mkdtemp(prefix="orchestrator-")
LOG_FILE = os.path.join(TEMP_DIR, "orchestrator.log")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(), logging.FileHandler(LOG_FILE)],
)
logger = logging.getLogger(__name__)

# Global test mode state
TEST_MODE_ACTIVE = False
TEST_MODE_REASON = ""


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
    """
    mode_prefix = "[TEST MODE]" if TEST_MODE_ACTIVE else "[LIVE MODE]"
    logger.info(f"{mode_prefix} Starting Job: {job_name}")
    update_scheduler_status(job_name, "Running")

    try:
        full_path = os.path.abspath(
            os.path.join(os.path.dirname(__file__), "..", script_path)
        )

        # Set environment variable to ensure jobs run in the same mode
        env = os.environ.copy()
        if TEST_MODE_ACTIVE:
            env["TEST_MODE"] = "enabled"
            env["MODE"] = "test"

        result = subprocess.run(
            [sys.executable, full_path],
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
    """Run intraday data updates using the new Master Compact Fetcher system."""
    mode_prefix = "[TEST MODE]" if TEST_MODE_ACTIVE else "[LIVE MODE]"
    logger.info(
        f"{mode_prefix} Starting intraday updates (Master Compact Fetcher)"
    )
    logger.info(
        f"{mode_prefix} ENHANCED SYSTEM: Using intelligent master compact fetcher with self-healing"
    )
    result = run_job("jobs/master_compact_fetcher.py", "master_compact_fetcher")
    if TEST_MODE_ACTIVE and result:
        logger.info(
            "[TEST MODE] Master Compact Fetcher simulation completed successfully"
        )
    return result


def run_30min_updates():
    """Run 30-minute intraday data updates using the new Master Compact Fetcher system."""
    mode_prefix = "[TEST MODE]" if TEST_MODE_ACTIVE else "[LIVE MODE]"
    logger.info(
        f"{mode_prefix} Starting 30-minute intraday updates (Master Compact Fetcher)"
    )
    logger.info(
        f"{mode_prefix} ENHANCED SYSTEM: Using intelligent master compact fetcher (30min interval)"
    )
    result = run_job("jobs/master_compact_fetcher.py --interval 30min", "master_compact_fetcher_30min")
    if TEST_MODE_ACTIVE and result:
        logger.info(
            "[TEST MODE] Master Compact Fetcher (30min) simulation completed successfully"
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
    The main entry point for the trading station orchestrator.
    Runs in production mode with proper scheduling, with automatic weekend/test mode detection.
    """
    # Detect and log test mode status at startup
    test_mode_active = detect_and_log_test_mode()

    mode_str = "Test Mode" if test_mode_active else "Production Mode"
    logger.info(f"Starting Master Orchestrator ({mode_str})")

    # Set up production schedule
    setup_production_schedule()

    logger.info(f"Orchestrator running in {mode_str.lower()} - 24/7 operation")
    logger.info(f"Scheduled jobs: {len(schedule.jobs)}")

    if test_mode_active:
        logger.info(
            "[TEST MODE] Jobs will run with simulated data for testing purposes"
        )
        logger.info("[TEST MODE] No live API calls will be made")

    # Main scheduling loop
    while True:
        try:
            if should_run_jobs():
                schedule.run_pending()
            else:
                if not test_mode_active:
                    logger.debug("Outside market hours - skipping job checks")

            time.sleep(30)  # Check every 30 seconds

        except KeyboardInterrupt:
            logger.info("Orchestrator stopped by user")
            if test_mode_active:
                logger.info(
                    "[TEST MODE] Test session ended - all operations were simulated"
                )
            break
        except Exception as e:
            logger.error(f"Unexpected error in main loop: {e}")
            time.sleep(60)  # Wait 1 minute before retrying


if __name__ == "__main__":
    main()
