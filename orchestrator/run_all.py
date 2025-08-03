import sys
import os
import time
from datetime import datetime, timedelta
import pytz
import threading
import subprocess

# Add project root to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from utils.helpers import update_scheduler_status, detect_market_session

# --- CONFIGURATION ---
# Set to True to run the full sequence once for a test, then exit.
# Set to False for normal, 24/7 live operation.
TEST_MODE = True 

def run_job(script_path, job_name):
    """
    Runs a Python script sequentially and waits for it to complete.
    Returns True on success, False on failure.
    """
    print(f"\n--- Starting Job: {job_name} ---")
    update_scheduler_status(job_name, "Running")
    try:
        full_path = os.path.join('/workspace', script_path)
        result = subprocess.run(
            [sys.executable, full_path],
            check=True,
            capture_output=True,
            text=True,
            timeout=1800  # 30-minute timeout for any single job
        )
        print(f"--- SUCCESS: {job_name} finished. ---")
        if result.stdout:
            print(f"--- STDOUT for {job_name} ---\n{result.stdout}\n--- End STDOUT ---")
        update_scheduler_status(job_name, "Success")
        return True
    except subprocess.TimeoutExpired:
        print(f"--- TIMEOUT: {job_name} failed after 30 minutes. ---")
        update_scheduler_status(job_name, "Fail", "Job timed out.")
        return False
    except subprocess.CalledProcessError as e:
        error_details = f"Exited with code {e.returncode}. STDERR:\n{e.stderr}"
        print(f"--- ERROR: {job_name} failed. ---\n{error_details}")
        update_scheduler_status(job_name, "Fail", error_details)
        return False
    except Exception as e:
        print(f"--- UNEXPECTED ERROR in {job_name}: {e} ---")
        update_scheduler_status(job_name, "Fail", str(e))
        return False

def main():
    """
    The main entry point for the trading station orchestrator.
    """
    print(f"--- Starting Master Orchestrator (Final Sequential Logic) ---")
    
    if TEST_MODE:
        print("\n--- !!! RUNNING IN TEST MODE (SEQUENTIAL) !!! ---")
        
        # --- Stage 1: Prospecting ---
        if not run_job("jobs/run_weekly_universe_scan.py", "weekly_universe_scan"): return
        if not run_job("ticker_selectors/opportunity_ticker_finder.py", "opportunity_ticker_finder"): return
        
        # --- Stage 2: Data Fetching ---
        if not run_job("jobs/update_all_data.py", "update_all_data"): return
        if not run_job("jobs/find_avwap_anchors.py", "find_avwap_anchors"): return

        # --- Stage 3: Screening (run sequentially in test mode for clear logs) ---
        print("\n--- Running Screener Jobs Sequentially for Debugging... ---")
        screener_jobs = {
            "breakout": "screeners/breakout.py", 
            "ema_pullback": "screeners/ema_pullback.py",
            "exhaustion": "screeners/exhaustion.py", 
            "gapgo": "screeners/gapgo.py",
            "orb": "screeners/orb.py", 
            "avwap": "screeners/avwap.py"
        }
        for job_name, script_path in screener_jobs.items():
            if not run_job(script_path, job_name):
                print(f"\n--- 🛑 Test stopped due to failure in {job_name}. ---")
                return # Stop the test on the first failure

        # --- Stage 4: Final Consolidation ---
        if not run_job("dashboard/master_dashboard.py", "master_dashboard"): return

        print("\n--- ✅ Diagnostic test run finished successfully. Exiting. ---")
        return

    # --- LIVE OPERATION LOOP ---
    print("--- Live mode is not yet implemented. Set TEST_MODE to True. ---")


if __name__ == "__main__":
    main()
