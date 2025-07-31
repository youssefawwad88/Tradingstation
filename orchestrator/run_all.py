import schedule
import time
import threading
import subprocess
import os
import sys
from datetime import datetime

# --- System Path Setup ---
# This ensures the app can find the 'utils' module in the parent directory
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

try:
    # Import our logging helper. This is critical for the dashboard monitor.
    from utils.helpers import update_scheduler_status
except ImportError:
    print("FATAL ERROR: Could not import 'update_scheduler_status' from 'utils.helpers'. The dashboard monitor will not work.")
    # Define a dummy function to prevent a crash, functionality will be lost.
    def update_scheduler_status(job_name, status, details=""):
        print(f"DUMMY LOG (IMPORT FAILED): Job: {job_name}, Status: {status}, Details: {details}")

# --- Job Execution Logic ---
def run_job(script_path, blocking=True):
    """
    Runs a Python script as a subprocess, logs its status, and handles errors.
    This function combines the best of both previous versions.
    """
    job_name = os.path.basename(script_path).replace('.py', '')
    full_path = os.path.join('/workspace', script_path) # Assuming code is in /workspace
    
    print(f"--- Thread started for job: {job_name} at {datetime.now()} ---", flush=True)
    
    try:
        # Log that the job is starting to run
        update_scheduler_status(job_name, "Running")

        # Execute the script using the system's Python executable and environment
        # This is a robust way to ensure consistency.
        process = subprocess.run(
            [sys.executable, full_path],
            capture_output=True,
            text=True,
            check=True,  # Raises CalledProcessError for non-zero exit codes
            timeout=600, # 10-minute timeout
            env=os.environ # Pass environment variables (like API keys)
        )
        
        # If we get here, the script ran successfully
        print(f"SUCCESS: {job_name} completed.", flush=True)
        if process.stdout:
            print(f"--- Output for {job_name} ---\n{process.stdout}\n--- End Output ---", flush=True)
        update_scheduler_status(job_name, "Success", "Completed without errors.")

    except subprocess.CalledProcessError as e:
        # The script ran but returned an error
        error_message = f"Exit Code: {e.returncode}\nOutput:\n{e.stdout}\nError:\n{e.stderr}"
        print(f"ERROR: {job_name} failed. Details:\n{error_message}", flush=True)
        update_scheduler_status(job_name, "Fail", error_message)
    
    except subprocess.TimeoutExpired:
        timeout_error = f"Job ran for more than 10 minutes and was terminated."
        print(f"!!! TIMEOUT ERROR: {job_name}", flush=True)
        update_scheduler_status(job_name, "Fail", timeout_error)

    except Exception as e:
        # A different error occurred (e.g., file not found)
        error_message = f"An unexpected error occurred: {str(e)}"
        print(f"!!! FATAL ERROR running {job_name}. Details:\n{error_message}", flush=True)
        update_scheduler_status(job_name, "Fail", error_message)

    print(f"--- Thread finished for job: {job_name} at {datetime.now()} ---", flush=True)

def run_job_in_thread(job_func, *args):
    """Wrapper to run any function in its own thread."""
    print(f"Scheduler creating new thread for: {job_func.__name__}", flush=True)
    job_thread = threading.Thread(target=job_func, args=args)
    job_thread.start()

def run_high_frequency_sequence():
    """
    The critical, time-sensitive sequence for intraday trading.
    These jobs run sequentially (blocking) to ensure data integrity.
    """
    print(f"--- STARTING High-Frequency Sequence at {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC ---", flush=True)
    # Each step is logged individually by run_job
    run_job('jobs/update_intraday_compact.py')
    run_job('screeners/gapgo.py')
    print(f"--- FINISHED High-Frequency Sequence ---", flush=True)

def main():
    """Main function to schedule and run all trading system jobs."""
    print("--- Starting Master Orchestrator ---", flush=True)
    
    # Schedule Tasks using your specific logic
    schedule.every(1).minutes.do(run_job_in_thread, run_high_frequency_sequence)
    
    # Non-blocking jobs (run in parallel)
    schedule.every().day.at("10:30").do(run_job_in_thread, run_job, 'ticker_selectors/opportunity_ticker_finder.py')
    schedule.every().day.at("10:35").do(run_job_in_thread, run_job, 'jobs/find_avwap_anchors.py')
    schedule.every().day.at("10:40").do(run_job_in_thread, run_job, 'jobs/update_all_data.py')
    schedule.every().day.at("13:40").do(run_job_in_thread, run_job, 'screeners/orb.py')
    
    schedule.every(15).minutes.do(run_job_in_thread, run_job, 'screeners/avwap.py')
    schedule.every(15).minutes.do(run_job_in_thread, run_job, 'screeners/breakout.py')
    schedule.every(15).minutes.do(run_job_in_thread, run_job, 'screeners/ema_pullback.py')
    schedule.every(15).minutes.do(run_job_in_thread, run_job, 'screeners/exhaustion.py')
    
    # Note: It's unusual to run a dashboard script from the orchestrator. 
    # The dashboard runs as its own service. This line can likely be removed.
    # schedule.every(5).minutes.do(run_job_in_thread, run_job, 'dashboard/master_dashboard.py')

    print("--- All jobs scheduled. Orchestrator is now in its main loop. ---", flush=True)
    update_scheduler_status("orchestrator", "Success", "System online and scheduler running.")
    
    # --- Main Loop ---
    while True:
        schedule.run_pending()
        time.sleep(1)

if __name__ == "__main__":
    main()
