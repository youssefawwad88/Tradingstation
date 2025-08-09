import sys
import os
import time
import asyncio
import schedule
from datetime import datetime, timedelta
import pytz
import threading
import subprocess
import logging

# Add project root to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from utils.helpers import update_scheduler_status, detect_market_session

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('/tmp/orchestrator.log')
    ]
)
logger = logging.getLogger(__name__)

def run_job(script_path, job_name):
    """
    Runs a Python script sequentially and waits for it to complete.
    Returns True on success, False on failure.
    """
    logger.info(f"Starting Job: {job_name}")
    update_scheduler_status(job_name, "Running")
    try:
        full_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', script_path))
        result = subprocess.run(
            [sys.executable, full_path],
            check=True,
            capture_output=True,
            text=True,
            timeout=1800  # 30-minute timeout for any single job
        )
        logger.info(f"SUCCESS: {job_name} finished")
        if result.stdout:
            # Show orchestrator summary line for intraday jobs
            for line in result.stdout.strip().split('\n'):
                if "ORCHESTRATOR SUMMARY:" in line:
                    logger.info(f"  {line}")
                    break
            logger.debug(f"STDOUT for {job_name}: {result.stdout}")
        update_scheduler_status(job_name, "Success")
        return True
    except subprocess.TimeoutExpired:
        logger.error(f"TIMEOUT: {job_name} failed after 30 minutes")
        update_scheduler_status(job_name, "Fail", "Job timed out.")
        return False
    except subprocess.CalledProcessError as e:
        error_details = f"Exited with code {e.returncode}. STDERR: {e.stderr}"
        logger.error(f"ERROR: {job_name} failed - {error_details}")
        update_scheduler_status(job_name, "Fail", error_details)
        return False
    except Exception as e:
        logger.error(f"UNEXPECTED ERROR in {job_name}: {e}")
        update_scheduler_status(job_name, "Fail", str(e))
        return False

def run_daily_data_jobs():
    """Run daily data fetching and processing jobs."""
    logger.info("Starting daily data jobs")
    
    # Stage 1: Generate master ticker list (manual sources only)
    if not run_job("generate_master_tickerlist.py", "generate_master_tickerlist"): 
        return False
    
    # Stage 2: Data Fetching 
    if not run_job("jobs/update_all_data.py", "update_all_data"): 
        return False
    if not run_job("jobs/find_avwap_anchors.py", "find_avwap_anchors"): 
        return False
    
    logger.info("Daily data jobs completed successfully")
    return True

def run_intraday_updates():
    """Run intraday data updates (1min and 30min)."""
    logger.info("Starting intraday updates")
    return run_job("jobs/update_intraday_compact.py", "update_intraday_compact")

def run_screener(screener_name, script_path):
    """Run a specific screener."""
    return run_job(script_path, screener_name)

def run_gap_go_screener():
    """Run Gap & Go screener."""
    return run_screener("gapgo", "screeners/gapgo.py")

def run_orb_screener():
    """Run ORB screener."""
    return run_screener("orb", "screeners/orb.py")

def run_hourly_screeners():
    """Run screeners that operate on hourly frequency."""
    logger.info("Starting hourly screeners")
    
    screeners = [
        ("avwap", "screeners/avwap.py"),
        ("ema_pullback", "screeners/ema_pullback.py"), 
        ("breakout", "screeners/breakout.py"),
        ("exhaustion", "screeners/exhaustion.py")
    ]
    
    results = []
    for name, script in screeners:
        try:
            result = run_screener(name, script)
            results.append(result)
        except Exception as e:
            logger.error(f"Error running {name} screener: {e}")
            results.append(False)
    
    return all(results)

def run_consolidation():
    """Run final signal consolidation."""
    return run_job("dashboard/master_dashboard.py", "master_dashboard")

def setup_production_schedule():
    """Set up all production job schedules."""
    ny_tz = pytz.timezone('America/New_York')
    
    # Daily data jobs - 5:00 PM ET
    schedule.every().day.at("17:00").do(run_daily_data_jobs).tag('daily')
    
    # Intraday updates - every minute during extended hours (4:00 AM - 8:00 PM ET)
    for hour in range(4, 20):
        for minute in range(0, 60):
            time_str = f"{hour:02d}:{minute:02d}"
            schedule.every().day.at(time_str).do(run_intraday_updates).tag('intraday_1min')
    
    # 30-minute intraday updates - every 15 minutes
    for hour in range(4, 20):  # Extended hours coverage
        for minute in [0, 15, 30, 45]:
            time_str = f"{hour:02d}:{minute:02d}"
            schedule.every().day.at(time_str).do(run_intraday_updates).tag('intraday_30min')
    
    # Gap & Go screener schedules
    # Pre-market: Every 30 minutes (7:00 AM - 9:30 AM)
    for hour in range(7, 9):
        for minute in [0, 30]:
            time_str = f"{hour:02d}:{minute:02d}"
            schedule.every().day.at(time_str).do(run_gap_go_screener).tag('gapgo_premarket')
    schedule.every().day.at("09:00").do(run_gap_go_screener).tag('gapgo_premarket')
    
    # Gap & Go during regular session: Every minute (9:30 AM - 10:30 AM)
    for hour in range(9, 11):
        for minute in range(0, 60):
            if hour == 9 and minute < 30:
                continue  # Skip before 9:30 AM
            if hour == 10 and minute > 30:
                continue  # Skip after 10:30 AM
            time_str = f"{hour:02d}:{minute:02d}"
            schedule.every().day.at(time_str).do(run_gap_go_screener).tag('gapgo_regular')
    
    # ORB screener - once at 9:40 AM
    schedule.every().day.at("09:40").do(run_orb_screener).tag('orb')
    
    # Hourly screeners - every hour during extended hours
    for hour in range(6, 20):
        time_str = f"{hour:02d}:00"
        schedule.every().day.at(time_str).do(run_hourly_screeners).tag('hourly_screeners')
    
    # Signal consolidation - every 5 minutes during market hours
    for hour in range(9, 16):
        for minute in range(0, 60, 5):
            if hour == 9 and minute < 30:
                continue
            time_str = f"{hour:02d}:{minute:02d}"
            schedule.every().day.at(time_str).do(run_consolidation).tag('consolidation')
    
    logger.info("Production schedule setup complete")

def should_run_jobs():
    """Check if we should run jobs based on market schedule."""
    session = detect_market_session()
    ny_tz = pytz.timezone('America/New_York')
    current_time = datetime.now(ny_tz)
    current_weekday = current_time.weekday()  # 0=Monday, 6=Sunday
    
    # Don't run on weekends
    if current_weekday >= 5:  # Saturday=5, Sunday=6
        return False
    
    # Run during market hours and extended hours (4 AM - 8 PM ET)
    market_start = current_time.replace(hour=4, minute=0, second=0, microsecond=0)
    market_end = current_time.replace(hour=20, minute=0, second=0, microsecond=0)
    
    return market_start <= current_time <= market_end

def main():
    """
    The main entry point for the trading station orchestrator.
    Runs in production mode with proper scheduling.
    """
    logger.info("Starting Master Orchestrator (Production Mode)")
    
    # Set up production schedule
    setup_production_schedule()
    
    logger.info("Orchestrator running in production mode - 24/7 operation")
    logger.info(f"Scheduled jobs: {len(schedule.jobs)}")
    
    # Main scheduling loop
    while True:
        try:
            if should_run_jobs():
                schedule.run_pending()
            else:
                logger.debug("Outside market hours - skipping job checks")
            
            time.sleep(30)  # Check every 30 seconds
            
        except KeyboardInterrupt:
            logger.info("Orchestrator stopped by user")
            break
        except Exception as e:
            logger.error(f"Unexpected error in main loop: {e}")
            time.sleep(60)  # Wait 1 minute before retrying


if __name__ == "__main__":
    main()
