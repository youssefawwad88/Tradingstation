"""
Master Orchestrator (run_all.py)

This is the central "conductor" of the entire trading system. It runs continuously
and executes all data jobs and screeners on a predefined, strategy-aligned schedule
that mirrors a professional trading workflow.
"""

import schedule
import time
from datetime import datetime
import pytz
import sys
import os

# --- System Path Setup ---
PROJECT_ROOT = '/content/drive/MyDrive/trading-system'
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

# --- Import All Runnable Modules ---
from ticker_selectors.opportunity_ticker_finder import run_opportunity_finder
from jobs.find_avwap_anchors import run_anchor_finder
from jobs.update_all_data import run_all_data_updates
from jobs.update_intraday_compact import run_compact_intraday_update
from screeners.gapgo import run_gapgo_screener
from screeners.orb import run_orb_screener
from screeners.avwap import run_avwap_screener
from screeners.breakout import run_breakout_screener
from screeners.ema_pullback import run_ema_pullback_screener
from screeners.exhaustion import run_exhaustion_screener
from dashboard.master_dashboard import run_master_dashboard

# --- Timezone & Time Window Configuration ---
NY_TIMEZONE = pytz.timezone('America/New_York')

def is_time_in_window(start_str, end_str):
    """Checks if the current NY time is within a given window."""
    now_ny = datetime.now(NY_TIMEZONE).time()
    start = datetime.strptime(start_str, "%H:%M").time()
    end = datetime.strptime(end_str, "%H:%M").time()
    return start <= now_ny < end

def is_premarket_hours(): return is_time_in_window("04:00", "09:30")
def is_market_hours(): return is_time_in_window("09:30", "16:00")
def is_early_market_hours(): return is_time_in_window("09:30", "10:30")

def run_job(job_func):
    """A wrapper to run a job and catch any potential errors."""
    try:
        print(f"\n--- [{datetime.now(NY_TIMEZONE).strftime('%H:%M:%S')}] Triggering job: {job_func.__name__} ---")
        job_func()
    except Exception as e:
        print(f"--- ERROR running job {job_func.__name__}: {e} ---")

# --- FIX: Create dedicated functions for each scheduled task for clarity and reliability ---
def scheduled_gapgo_premarket():
    if is_premarket_hours():
        run_job(run_gapgo_screener)

def scheduled_gapgo_early_market():
    if is_early_market_hours():
        run_job(run_gapgo_screener)

def scheduled_swing_screeners():
    if is_market_hours():
        run_job(run_avwap_screener)
        run_job(run_breakout_screener)
        run_job(run_ema_pullback_screener)
        run_job(run_exhaustion_screener)

def scheduled_dashboard_early():
    if is_early_market_hours():
        run_job(run_master_dashboard)

def scheduled_dashboard_regular():
    if is_market_hours() and not is_early_market_hours():
        run_job(run_master_dashboard)

# --- 1. Schedule All System Tasks ---
print("--- Initializing Master Orchestrator ---")
print("--- Scheduling all jobs and screeners based on the final blueprint... ---")

# --- Daily, One-Time Tasks (Pre-Market) ---
schedule.every().day.at("06:30", "America/New_York").do(run_job, run_opportunity_finder)
schedule.every().day.at("06:45", "America/New_York").do(run_job, run_anchor_finder)
schedule.every().day.at("07:00", "America/New_York").do(run_job, run_all_data_updates)

# --- Continuous Data Updates ---
schedule.every(1).minutes.do(run_job, run_compact_intraday_update)

# --- Time-Sensitive Intraday Screeners ---
schedule.every(30).minutes.do(scheduled_gapgo_premarket)
schedule.every(1).minutes.do(scheduled_gapgo_early_market)
schedule.every().day.at("09:40", "America/New_York").do(run_job, run_orb_screener)

# --- Swing & Slower Intraday Screeners ---
schedule.every(15).minutes.do(scheduled_swing_screeners)

# --- Master Dashboard Schedule ---
schedule.every(5).minutes.do(scheduled_dashboard_early)
schedule.every(15).minutes.do(scheduled_dashboard_regular)

print("--- Scheduling complete. Orchestrator is now live. ---")
print(f"Current NY Time: {datetime.now(NY_TIMEZONE).strftime('%Y-%m-%d %H:%M:%S')}")
print("Waiting for scheduled tasks to run...")

# --- 2. Main Execution Loop ---
while True:
    if datetime.now(NY_TIMEZONE).weekday() < 5:
        schedule.run_pending()
    time.sleep(1)
