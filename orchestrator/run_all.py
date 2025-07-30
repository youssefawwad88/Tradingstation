import schedule
import time
import subprocess
import sys
from datetime import datetime
import os
import threading

# Adjust the path to include the parent directory
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from utils.helpers import read_df_from_s3, upload_initial_data_to_s3

def run_script_and_log(script_path):
    """
    Runs a script and waits for it to complete, capturing its output.
    This is designed to be run inside a thread so it doesn't block the main loop.
    """
    try:
        full_path = os.path.join('/workspace', script_path)
        print(f"--- EXECUTING {full_path} at {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC ---")
        result = subprocess.run(
            [sys.executable, full_path], 
            check=True, 
            capture_output=True, 
            text=True,
            timeout=600 # Add a 10-minute timeout to prevent frozen jobs
        )
        # We print the output here for better real-time logging
        print(f"--- Output for {script_path} ---\n{result.stdout}\n--- End Output ---")
        if result.stderr:
            print(f"--- Errors for {script_path} ---\n{result.stderr}\n--- End Errors ---")
    except subprocess.TimeoutExpired:
        print(f"!!! TIMEOUT ERROR: {script_path} ran for more than 10 minutes and was terminated.")
    except Exception as e:
        print(f"!!! An unexpected error occurred while EXECUTING {script_path}: {e}")

def run_job_in_thread(job_func, script_path):
    """
    Wrapper to run a scheduled job in its own thread.
    This prevents any single job from blocking the main scheduler loop.
    """
    job_thread = threading.Thread(target=job_func, args=(script_path,))
    job_thread.start()

def main():
    """Main function to schedule and run all trading system jobs and screeners."""
    print("--- Starting Master Orchestrator (High-Performance Threaded) ---")

    upload_initial_data_to_s3()

    # --- Define Paths to Scripts ---
    opportunity_finder_script = 'ticker_selectors/opportunity_ticker_finder.py'
    avwap_anchor_script = 'jobs/find_avwap_anchors.py'
    update_daily_data_script = 'jobs/update_all_data.py'
    update_intraday_script = 'jobs/update_intraday_compact.py'
    
    gapgo_screener = 'screeners/gapgo.py'
    orb_screener = 'screeners/orb.py'
    avwap_screener = 'screeners/avwap.py'
    breakout_screener = 'screeners/breakout.py'
    ema_pullback_screener = 'screeners/ema_pullback.py'
    exhaustion_screener = 'screeners/exhaustion.py'
    master_dashboard_script = 'dashboard/master_dashboard.py'

    # --- Schedule ALL Tasks to Run in Separate Threads ---
    
    # Pre-Market Jobs
    schedule.every().day.at("10:30").do(run_job_in_thread, run_script_and_log, opportunity_finder_script)
    schedule.every().day.at("10:35").do(run_job_in_thread, run_script_and_log, avwap_anchor_script)
    schedule.every().day.at("10:40").do(run_job_in_thread, run_script_and_log, update_daily_data_script)

    # High-Frequency Intraday Jobs
    schedule.every(1).minutes.do(run_job_in_thread, run_script_and_log, update_intraday_script)
    schedule.every(1).minutes.do(run_job_in_thread, run_script_and_log, gapgo_screener)
    
    # Standard-Frequency Screeners
    schedule.every().day.at("13:40").do(run_job_in_thread, run_script_and_log, orb_screener) # 9:40 AM ET
    schedule.every(15).minutes.do(run_job_in_thread, run_script_and_log, avwap_screener)
    schedule.every(15).minutes.do(run_job_in_thread, run_script_and_log, breakout_screener)
    schedule.every(15).minutes.do(run_job_in_thread, run_script_and_log, ema_pullback_screener)
    schedule.every(15).minutes.do(run_job_in_thread, run_script_and_log, exhaustion_screener)
    
    # Dashboard Update
    schedule.every(5).minutes.do(run_job_in_thread, run_script_and_log, master_dashboard_script)

    print("--- All jobs scheduled. Orchestrator is now in its main loop. ---")
    
    # --- Main Loop ---
    while True:
        schedule.run_pending()
        time.sleep(1)

if __name__ == "__main__":
    main()
