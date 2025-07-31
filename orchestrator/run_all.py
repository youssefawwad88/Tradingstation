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
    Runs a script and waits for it to complete, passing the environment.
    """
    try:
        full_path = os.path.join('/workspace', script_path)
        print(f"--- EXECUTING {full_path} (blocking) ---")
        # CRITICAL FIX: Pass the current environment to the subprocess
        result = subprocess.run(
            [sys.executable, full_path], 
            check=True, 
            capture_output=True, 
            text=True,
            timeout=600, # 10-minute timeout
            env=os.environ 
        )
        print(f"--- Output for {script_path} ---\n{result.stdout}\n--- End Output ---")
        if result.stderr:
            print(f"--- Errors for {script_path} ---\n{result.stderr}\n--- End Errors ---")
    except subprocess.TimeoutExpired:
        print(f"!!! TIMEOUT ERROR: {script_path} ran for more than 10 minutes.")
    except Exception as e:
        print(f"!!! An unexpected error occurred while EXECUTING {script_path}: {e}")

def run_script_non_blocking(script_path):
    """
    Runs a script as a non-blocking background process, passing the environment.
    """
    try:
        full_path = os.path.join('/workspace', script_path)
        print(f"--- LAUNCHING {full_path} (non-blocking) ---")
        # CRITICAL FIX: Pass the current environment to the subprocess
        subprocess.Popen([sys.executable, full_path], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, env=os.environ)
    except Exception as e:
        print(f"!!! An unexpected error occurred while LAUNCHING {script_path}: {e}")

def run_high_frequency_sequence():
    """
    The critical, time-sensitive sequence for intraday trading.
    """
    print(f"--- STARTING High-Frequency Sequence at {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC ---")
    run_script_blocking('jobs/update_intraday_compact.py')
    run_script_blocking('screeners/gapgo.py')
    print(f"--- FINISHED High-Frequency Sequence ---")

def run_job_in_thread(job_func, *args):
    """
    Wrapper to run a scheduled job in its own thread.
    """
    # --- NEW: Added logging to show when a job is being triggered ---
    print(f"Scheduler is creating a new thread for job: {job_func.__name__}")
    job_thread = threading.Thread(target=job_func, args=args)
    job_thread.start()

def main():
    """Main function to schedule and run all trading system jobs and screeners."""
    print("--- Starting Master Orchestrator (Final Version with Heartbeat) ---")
    upload_initial_data_to_s3()

    # Define script paths
    opportunity_finder_script = 'ticker_selectors/opportunity_ticker_finder.py'
    avwap_anchor_script = 'jobs/find_avwap_anchors.py'
    update_daily_data_script = 'jobs/update_all_data.py'
    orb_screener = 'screeners/orb.py'
    avwap_screener = 'screeners/avwap.py'
    breakout_screener = 'screeners/breakout.py'
    ema_pullback_screener = 'screeners/ema_pullback.py'
    exhaustion_screener = 'screeners/exhaustion.py'
    master_dashboard_script = 'dashboard/master_dashboard.py'

    # Schedule Tasks
    schedule.every(1).minutes.do(run_job_in_thread, run_high_frequency_sequence)
    schedule.every().day.at("10:30").do(run_job_in_thread, run_script_non_blocking, opportunity_finder_script)
    schedule.every().day.at("10:35").do(run_job_in_thread, run_script_non_blocking, avwap_anchor_script)
    schedule.every().day.at("10:40").do(run_job_in_thread, run_script_non_blocking, update_daily_data_script)
    schedule.every().day.at("13:40").do(run_job_in_thread, run_script_non_blocking, orb_screener)
    schedule.every(15).minutes.do(run_job_in_thread, run_script_non_blocking, avwap_screener)
    schedule.every(15).minutes.do(run_job_in_thread, run_script_non_blocking, breakout_screener)
    schedule.every(15).minutes.do(run_job_in_thread, run_script_non_blocking, ema_pullback_screener)
    schedule.every(15).minutes.do(run_job_in_thread, run_script_non_blocking, exhaustion_screener)
    schedule.every(5).minutes.do(run_job_in_thread, run_script_non_blocking, master_dashboard_script)

    print("--- All jobs scheduled. Orchestrator is now in its main loop. ---")
    
    # --- Main Loop with Heartbeat ---
    heartbeat_counter = 0
    while True:
        schedule.run_pending()
        time.sleep(1)
        heartbeat_counter += 1
        # --- NEW: Re-added the heartbeat for visibility ---
        if heartbeat_counter % 60 == 0:
            print(f"Heartbeat: Orchestrator is alive. Current UTC time: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}")

if __name__ == "__main__":
    main()
