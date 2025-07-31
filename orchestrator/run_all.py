import schedule
import time
import threading
import subprocess
import os
import sys
from datetime import datetime, UTC

# --- System Path Setup ---
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

try:
    from utils.helpers import update_scheduler_status
except ImportError:
    print("FATAL ERROR: Could not import 'update_scheduler_status' from 'utils.helpers'.", flush=True)
    def update_scheduler_status(job_name, status, details=""):
        print(f"DUMMY LOG (IMPORT FAILED): Job: {job_name}, Status: {status}, Details: {details}", flush=True)

# --- Job Execution Logic ---
def run_job(script_path):
    job_name = os.path.basename(script_path).replace('.py', '')
    full_path = os.path.join('/workspace', script_path)
    
    print(f"--- Thread started for job: {job_name} at {datetime.now()} ---", flush=True)
    update_scheduler_status(job_name, "Running")
    
    try:
        process = subprocess.run(
            [sys.executable, full_path],
            capture_output=True, text=True, check=True,
            timeout=600, env=os.environ
        )
        print(f"SUCCESS: {job_name} completed.", flush=True)
        update_scheduler_status(job_name, "Success", "Completed without errors.")
    except subprocess.CalledProcessError as e:
        error_message = f"Exit Code: {e.returncode}\nError:\n{e.stderr}"
        print(f"ERROR: {job_name} failed. Details:\n{error_message}", flush=True)
        update_scheduler_status(job_name, "Fail", error_message)
    except Exception as e:
        error_message = f"An unexpected error occurred: {str(e)}"
        print(f"!!! FATAL ERROR running {job_name}. Details:\n{error_message}", flush=True)
        update_scheduler_status(job_name, "Fail", error_message)

def run_job_in_thread(job_func, *args):
    print(f"Scheduler creating new thread for: {job_func.__name__}", flush=True)
    job_thread = threading.Thread(target=job_func, args=args)
    job_thread.start()

def run_high_frequency_sequence():
    print(f"--- STARTING High-Frequency Sequence at {datetime.now(UTC).strftime('%Y-%m-%d %H:%M:%S')} UTC ---", flush=True)
    run_job('jobs/update_intraday_compact.py')
    run_job('screeners/gapgo.py')
    print(f"--- FINISHED High-Frequency Sequence ---", flush=True)

def simple_test_job():
    """A simple diagnostic job that just prints a message."""
    print(f"--- Simple test job running! --- Time: {datetime.now(UTC).strftime('%Y-%m-%d %H:%M:%S')} UTC", flush=True)

def main():
    print("--- Starting Master Orchestrator ---", flush=True)
    
    # --- Add a simple diagnostic job to test the scheduler ---
    schedule.every(15).seconds.do(simple_test_job)
    
    # --- Original Job Schedule ---
    schedule.every(1).minutes.do(run_job_in_thread, run_high_frequency_sequence)
    schedule.every().day.at("10:30").do(run_job_in_thread, run_job, 'ticker_selectors/opportunity_ticker_finder.py')
    schedule.every().day.at("10:35").do(run_job_in_thread, run_job, 'jobs/find_avwap_anchors.py')
    schedule.every().day.at("10:40").do(run_job_in_thread, run_job, 'jobs/update_all_data.py')
    schedule.every().day.at("13:40").do(run_job_in_thread, run_job, 'screeners/orb.py')
    schedule.every(15).minutes.do(run_job_in_thread, run_job, 'screeners/avwap.py')
    schedule.every(15).minutes.do(run_job_in_thread, run_job, 'screeners/breakout.py')
    schedule.every(15).minutes.do(run_job_in_thread, run_job, 'screeners/ema_pullback.py')
    schedule.every(15).minutes.do(run_job_in_thread, run_job, 'screeners/exhaustion.py')

    print("--- All jobs scheduled. Orchestrator is now in its main loop. ---", flush=True)
    update_scheduler_status("orchestrator", "Success", "System online and scheduler running.")
    
    # --- Main Loop with Heartbeat for Debugging ---
    last_heartbeat_time = time.time()
    while True:
        schedule.run_pending()
        
        # Heartbeat log every 10 seconds to show the loop is alive
        current_time = time.time()
        if current_time - last_heartbeat_time >= 10:
            # Use timezone-aware datetime object
            print(f"Heartbeat: Main loop is running. Time: {datetime.now(UTC).strftime('%Y-%m-%d %H:%M:%S')} UTC", flush=True)
            last_heartbeat_time = current_time
            
        time.sleep(1)

if __name__ == "__main__":
    main()
