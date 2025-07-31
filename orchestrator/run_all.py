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
    """
    Runs a script as a subprocess with enhanced, robust logging to capture all outcomes.
    """
    job_name = os.path.basename(script_path).replace('.py', '')
    full_path = os.path.join('/workspace', script_path)
    
    print(f"--- Thread started for job: {job_name} at {datetime.now(UTC)} ---", flush=True)
    update_scheduler_status(job_name, "Running")
    
    process = None
    try:
        process = subprocess.run(
            [sys.executable, full_path],
            capture_output=True, text=True, check=True,
            timeout=600, env=os.environ
        )
        print(f"SUCCESS: {job_name} completed.", flush=True)
        details = f"Completed successfully. Output:\n{process.stdout}"
        update_scheduler_status(job_name, "Success", details)

    except subprocess.CalledProcessError as e:
        error_message = f"Script exited with error code {e.returncode}.\n"
        error_message += f"--- STDOUT ---\n{e.stdout}\n"
        error_message += f"--- STDERR ---\n{e.stderr}\n"
        print(f"ERROR: {job_name} failed.\n{error_message}", flush=True)
        update_scheduler_status(job_name, "Fail", error_message)

    except subprocess.TimeoutExpired as e:
        error_message = f"Job timed out after 10 minutes.\n"
        error_message += f"--- STDOUT ---\n{e.stdout}\n"
        error_message += f"--- STDERR ---\n{e.stderr}\n"
        print(f"TIMEOUT: {job_name} failed.\n{error_message}", flush=True)
        update_scheduler_status(job_name, "Fail", error_message)

    except Exception as e:
        error_message = f"An unexpected exception occurred: {str(e)}"
        print(f"!!! FATAL ORCHESTRATOR ERROR running {job_name}. Details:\n{error_message}", flush=True)
        update_scheduler_status(job_name, "Fail", error_message)
    
    finally:
        print(f"--- Thread finished for job: {job_name} at {datetime.now(UTC)} ---", flush=True)


def run_job_in_thread(job_func, *args):
    """Wrapper to run any function in its own thread."""
    thread_name = job_func.__name__ if 'args' not in job_func.__name__ else args[0]
    print(f"Scheduler creating new thread for: {thread_name}", flush=True)
    job_thread = threading.Thread(target=job_func, args=args, name=thread_name)
    job_thread.start()

def run_high_frequency_sequence():
    """The critical, time-sensitive sequence for intraday trading."""
    print(f"--- STARTING High-Frequency Sequence at {datetime.now(UTC).strftime('%Y-%m-%d %H:%M:%S')} UTC ---", flush=True)
    # These jobs are blocking and will run one after the other.
    run_job('jobs/update_intraday_compact.py')
    run_job('screeners/gapgo.py')
    print(f"--- FINISHED High-Frequency Sequence ---", flush=True)

def main():
    print("--- Starting Master Orchestrator ---", flush=True)
    
    # --- Job Schedule (Simplified for Debugging) ---
    # We are calling the main job directly to see if it runs without the thread wrapper.
    schedule.every(1).minutes.do(run_high_frequency_sequence)
    
    # Other jobs remain threaded
    schedule.every().day.at("10:30", "America/New_York").do(run_job_in_thread, run_job, 'ticker_selectors/opportunity_ticker_finder.py')
    schedule.every().day.at("10:35", "America/New_York").do(run_job_in_thread, run_job, 'jobs/find_avwap_anchors.py')
    schedule.every().day.at("10:40", "America/New_York").do(run_job_in_thread, run_job, 'jobs/update_all_data.py')
    schedule.every().day.at("13:40", "America/New_York").do(run_job_in_thread, run_job, 'screeners/orb.py')
    schedule.every(15).minutes.do(run_job_in_thread, run_job, 'screeners/avwap.py')
    schedule.every(15).minutes.do(run_job_in_thread, run_job, 'screeners/breakout.py')
    schedule.every(15).minutes.do(run_job_in_thread, run_job, 'screeners/ema_pullback.py')
    schedule.every(15).minutes.do(run_job_in_thread, run_job, 'screeners/exhaustion.py')

    print("--- All jobs scheduled. Orchestrator is now in its main loop. ---", flush=True)
    update_scheduler_status("orchestrator", "Success", "System online and scheduler running.")
    
    # --- Main Loop ---
    while True:
        schedule.run_pending()
        time.sleep(1)

if __name__ == "__main__":
    main()
