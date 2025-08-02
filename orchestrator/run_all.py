import time
import threading
import subprocess
import os
import sys
from datetime import datetime
import pytz

# --- System Path Setup ---
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

try:
    from utils.helpers import update_scheduler_status, detect_market_session
except ImportError:
    print("FATAL ERROR: Could not import helper functions from utils.helpers.", flush=True)
    # Define dummy functions to prevent immediate crash if helpers.py is missing/broken
    def update_scheduler_status(job_name, status, details=""): pass
    def detect_market_session(): return "CLOSED"

# --- Job Execution Logic ---
def run_job(script_path):
    """
    Runs a given script as a subprocess with robust logging.
    This is the core execution function.
    """
    job_name = os.path.basename(script_path).replace('.py', '')
    full_path = os.path.join('/workspace', script_path)
    
    print(f"--- Starting job: {job_name} ---", flush=True)
    update_scheduler_status(job_name, "Running")
    try:
        # Execute the script as a subprocess
        process = subprocess.run(
            [sys.executable, full_path],
            capture_output=True, text=True, check=True,
            timeout=900, env=os.environ # 15 minute timeout
        )
        # Log success if the script completes with exit code 0
        details = f"Completed successfully."
        update_scheduler_status(job_name, "Success", details)
        print(f"--- SUCCESS: {job_name} finished. ---", flush=True)
    except subprocess.CalledProcessError as e:
        # Log a failure if the script runs but exits with an error
        error_message = f"Script exited with error code {e.returncode}.\n--- STDERR ---\n{e.stderr}"
        print(f"--- ERROR: {job_name} failed. ---\n{error_message}", flush=True)
        update_scheduler_status(job_name, "Fail", error_message)
    except Exception as e:
        # Log any other unexpected error during execution
        error_message = f"An unexpected exception occurred: {str(e)}"
        print(f"--- FATAL ERROR running {job_name}. ---\n{error_message}", flush=True)
        update_scheduler_status(job_name, "Fail", error_message)

def run_job_in_thread(script_path):
    """Wrapper to run the job logic in a background thread for normal operation."""
    thread = threading.Thread(target=run_job, args=(script_path,), name=os.path.basename(script_path))
    thread.start()

# --- Main Orchestrator ---
def main():
    """
    Main orchestrator entry point. Uses a simple, robust, time-based loop
    to run jobs according to the market session.
    """
    # --- TEST MODE FLAG ---
    # Set to True to run all jobs once sequentially for testing, then exit.
    # Set to False for normal, live operation.
    TEST_MODE = True

    print("--- Orchestrator main() function started. ---", flush=True)
    
    if TEST_MODE:
        print("--- !!! RUNNING IN DIAGNOSTIC TEST MODE (SEQUENTIAL) !!! ---", flush=True)
        
        # Run all jobs sequentially to isolate the failing script
        run_job('jobs/update_all_data.py')
        run_job('screeners/breakout.py')
        run_job('screeners/ema_pullback.py')
        run_job('screeners/exhaustion.py')
        run_job('jobs/update_intraday_compact.py')
        run_job('screeners/gapgo.py')
        run_job('screeners/orb.py')
        run_job('screeners/avwap.py')
        
        print("\n--- Diagnostic test run finished. Exiting. ---", flush=True)
        return # Exit after the test run

    # --- NORMAL OPERATION LOOP (runs if TEST_MODE is False) ---
    try:
        update_scheduler_status("orchestrator", "Success", "System online.")
        print("--- Initial status logged successfully. ---", flush=True)
    except Exception as e:
        print(f"--- FATAL ERROR during initial status update: {e} ---", flush=True)
        return 

    last_run = {
        "daily_setup": None, "intraday_data": None, "gapgo_premarket": None,
        "gapgo_early": None, "intraday_screeners": None
    }
    
    print("--- Last run dictionary initialized. Entering main loop... ---", flush=True)
    
    while True:
        try:
            ny_timezone = pytz.timezone('America/New_York')
            ny_time = datetime.now(ny_timezone)
            session = detect_market_session()
            
            print(f"Heartbeat: Loop running. NY Time: {ny_time.strftime('%H:%M:%S')}. Market Session: {session}", flush=True)
            
            if ny_time.hour >= 8 and (last_run["daily_setup"] is None or last_run["daily_setup"].date() < ny_time.date()):
                print("--- Triggering Daily Setup Jobs (Data + Daily Screeners) ---", flush=True)
                run_job_in_thread('jobs/update_all_data.py')
                time.sleep(10) 
                run_job_in_thread('screeners/breakout.py')
                run_job_in_thread('screeners/ema_pullback.py')
                run_job_in_thread('screeners/exhaustion.py')
                last_run["daily_setup"] = ny_time

            if session == 'PRE-MARKET':
                if last_run["intraday_data"] is None or (ny_time - last_run["intraday_data"]).total_seconds() >= 60:
                    run_job_in_thread('jobs/update_intraday_compact.py')
                    last_run["intraday_data"] = ny_time
                if last_run["gapgo_premarket"] is None or (ny_time - last_run["gapgo_premarket"]).total_seconds() >= 900:
                    run_job_in_thread('screeners/gapgo.py')
                    last_run["gapgo_premarket"] = ny_time

            elif session == 'REGULAR':
                if last_run["intraday_data"] is None or (ny_time - last_run["intraday_data"]).total_seconds() >= 60:
                    run_job_in_thread('jobs/update_intraday_compact.py')
                    last_run["intraday_data"] = ny_time
                if last_run["intraday_screeners"] is None or (ny_time - last_run["intraday_screeners"]).total_seconds() >= 900:
                    run_job_in_thread('screeners/orb.py')
                    run_job_in_thread('screeners/avwap.py')
                    last_run["intraday_screeners"] = ny_time
                if (ny_time.hour == 9 and ny_time.minute >= 30) or (ny_time.hour == 10 and ny_time.minute < 30):
                    if last_run["gapgo_early"] is None or (ny_time - last_run["gapgo_early"]).total_seconds() >= 60:
                        run_job_in_thread('screeners/gapgo.py')
                        last_run["gapgo_early"] = ny_time
            
            time.sleep(20)

        except Exception as e:
            print(f"--- CRITICAL ERROR IN MAIN ORCHESTRATOR LOOP! ---\n{e}", flush=True)
            print("Restarting loop in 30 seconds to ensure stability...", flush=True)
            time.sleep(30)

if __name__ == "__main__":
    main()
