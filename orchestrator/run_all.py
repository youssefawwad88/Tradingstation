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

def run_script_blocking(script_path):
    """
    Runs a script and waits for it to complete, capturing its output.
    This is for sequential, dependent tasks.
    """
    try:
        full_path = os.path.join('/workspace', script_path)
        print(f"--- EXECUTING {full_path} (blocking) ---")
        result = subprocess.run([sys.executable, full_path], check=True, capture_output=True, text=True)
        # We print the output here for better real-time logging
        print(f"--- Output for {script_path} ---\n{result.stdout}\n--- End Output ---")
        if result.stderr:
            print(f"--- Errors for {script_path} ---\n{result.stderr}\n--- End Errors ---")
    except Exception as e:
        print(f"!!! An unexpected error occurred while EXECUTING {script_path}: {e}")

def run_script_non_blocking(script_path):
    """
    Runs a script as a non-blocking background process.
    This is for independent tasks that can run in parallel.
    """
    try:
        full_path = os.path.join('/workspace', script_path)
        print(f"--- LAUNCHING {full_path} (non-blocking) ---")
        subprocess.Popen([sys.executable, full_path], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    except Exception as e:
        print(f"!!! An unexpected error occurred while LAUNCHING {script_path}: {e}")

def run_high_frequency_sequence():
    """
    This is the critical, time-sensitive sequence for intraday trading.
    It ensures data is fetched BEFORE the screeners are run.
    """
    print(f"--- STARTING High-Frequency Sequence at {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC ---")
    # Step 1: Update the latest 1-minute data and wait for it to finish.
    run_script_blocking('jobs/update_intraday_compact.py')
    # Step 2: Immediately run the GapGo screener on the fresh data.
    run_script_blocking('screeners/gapgo.py')
    print(f"--- FINISHED High-Frequency Sequence ---")

def run_job_in_thread(job_func):
    """
    Wrapper to run a scheduled job in its own thread.
    This prevents a long job from blocking the main scheduler loop.
    """
    job_thread = threading.Thread(target=job_func)
    job_thread.start()

def main():
    """Main function to schedule and run all trading system jobs and screeners."""
    print("--- Starting Master Orchestrator (High-Performance) ---")

    upload_initial_data_to_s3()

    # --- Define Paths to Scripts ---
    opportunity_finder_script = 'ticker_selectors/opportunity_ticker_finder.py'
    avwap_anchor_script = 'jobs/find_avwap_anchors.py'
    update_daily_data_script = 'jobs/update_all_data.py'
    orb_screener = 'screeners/orb.py'
    avwap_screener = 'screeners/avwap.py'
    breakout_screener = 'screeners/breakout.py'
    ema_pullback_screener = 'screeners/ema_pullback.py'
    exhaustion_screener = 'screeners/exhaustion.py'
    master_dashboard_script = 'dashboard/master_dashboard.py'

    # --- Schedule Tasks (Times are UTC on the server) ---
    
    # 1. High-Frequency, Time-Sensitive Sequence (runs in its own thread)
    schedule.every(1).minutes.do(run_job_in_thread, run_high_frequency_sequence)

    # 2. Pre-Market Jobs (non-blocking)
    schedule.every().day.at("10:30").do(run_script_non_blocking, opportunity_finder_script)
    schedule.every().day.at("10:35").do(run_script_non_blocking, avwap_anchor_script)
    schedule.every().day.at("10:40").do(run_script_non_blocking, update_daily_data_script)

    # 3. Standard-Frequency Screeners (non-blocking)
    schedule.every().day.at("13:40").do(run_script_non_blocking, orb_screener) # 9:40 AM ET
    schedule.every(15).minutes.do(run_script_non_blocking, avwap_screener)
    schedule.every(15).minutes.do(run_script_non_blocking, breakout_screener)
    schedule.every(15).minutes.do(run_script_non_blocking, ema_pullback_screener)
    schedule.every(15).minutes.do(run_script_non_blocking, exhaustion_screener)
    
    # 4. Dashboard Update (non-blocking)
    schedule.every(5).minutes.do(run_script_non_blocking, master_dashboard_script)

    print("--- All jobs scheduled. Orchestrator is now in its main loop. ---")
    
    # --- Main Loop ---
    while True:
        schedule.run_pending()
        time.sleep(1)

if __name__ == "__main__":
    main()
