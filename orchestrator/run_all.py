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
    print("FATAL ERROR: Could not import helper functions.", flush=True)
    def update_scheduler_status(job_name, status, details=""): pass
    def detect_market_session(): return "CLOSED"

# --- Job Execution Logic ---
def run_job_in_thread(script_path):
    """
    Runs a script in a background thread with robust logging.
    """
    job_name = os.path.basename(script_path).replace('.py', '')
    full_path = os.path.join('/workspace', script_path)
    
    def job_logic():
        print(f"--- Thread starting for job: {job_name} ---", flush=True)
        update_scheduler_status(job_name, "Running")
        try:
            process = subprocess.run(
                [sys.executable, full_path],
                capture_output=True, text=True, check=True,
                timeout=900, env=os.environ # 15 minute timeout
            )
            details = f"Completed successfully."
            update_scheduler_status(job_name, "Success", details)
            print(f"--- SUCCESS: {job_name} finished. ---", flush=True)
        except subprocess.CalledProcessError as e:
            error_message = f"Script exited with error code {e.returncode}.\n--- STDERR ---\n{e.stderr}"
            print(f"--- ERROR: {job_name} failed. ---\n{error_message}", flush=True)
            update_scheduler_status(job_name, "Fail", error_message)
        except Exception as e:
            error_message = f"An unexpected exception occurred: {str(e)}"
            print(f"--- FATAL ERROR running {job_name}. ---\n{error_message}", flush=True)
            update_scheduler_status(job_name, "Fail", error_message)

    thread = threading.Thread(target=job_logic, name=job_name)
    thread.start()

# --- Main Orchestrator ---
def main():
    """
    Main orchestrator entry point with a simple, time-based loop.
    """
    print("--- Starting Master Orchestrator (Stable Version) ---", flush=True)
    update_scheduler_status("orchestrator", "Success", "System online.")

    # Dictionary to track the last run time of job groups
    last_run = {
        "daily_setup": None,
        "intraday_data": None,
        "gapgo_premarket": None,
        "gapgo_early": None,
        "intraday_screeners": None
    }
    
    while True:
        try:
            ny_timezone = pytz.timezone('America/New_York')
            ny_time = datetime.now(ny_timezone)
            session = detect_market_session()
            
            # --- Daily Setup (runs once per day after 8 AM) ---
            if ny_time.hour >= 8 and (last_run["daily_setup"] is None or last_run["daily_setup"].date() < ny_time.date()):
                print("--- Running Daily Setup Jobs ---", flush=True)
                run_job_in_thread('jobs/update_all_data.py')
                run_job_in_thread('screeners/breakout.py')
                run_job_in_thread('screeners/ema_pullback.py')
                run_job_in_thread('screeners/exhaustion.py')
                last_run["daily_setup"] = ny_time

            # --- Intraday Logic ---
            if session == 'PRE-MARKET':
                # Fetch 1-min data every 60 seconds
                if last_run["intraday_data"] is None or (ny_time - last_run["intraday_data"]).total_seconds() >= 60:
                    print("--- Running 1-min data fetch (Pre-Market) ---", flush=True)
                    run_job_in_thread('jobs/update_intraday_compact.py')
                    last_run["intraday_data"] = ny_time
                
                # Run Gap & Go every 15 minutes
                if last_run["gapgo_premarket"] is None or (ny_time - last_run["gapgo_premarket"]).total_seconds() >= 900:
                    print("--- Running Gap & Go (Pre-Market) ---", flush=True)
                    run_job_in_thread('screeners/gapgo.py')
                    last_run["gapgo_premarket"] = ny_time

            elif session == 'REGULAR':
                # Fetch 1-min data every 60 seconds
                if last_run["intraday_data"] is None or (ny_time - last_run["intraday_data"]).total_seconds() >= 60:
                    print("--- Running 1-min data fetch (Regular) ---", flush=True)
                    run_job_in_thread('jobs/update_intraday_compact.py')
                    last_run["intraday_data"] = ny_time

                # Run regular screeners every 15 minutes
                if last_run["intraday_screeners"] is None or (ny_time - last_run["intraday_screeners"]).total_seconds() >= 900:
                    print("--- Running Intraday Screeners (ORB, AVWAP) ---", flush=True)
                    run_job_in_thread('screeners/orb.py')
                    run_job_in_thread('screeners/avwap.py')
                    last_run["intraday_screeners"] = ny_time

                # Run Gap & Go every 1 minute for the first hour
                if ny_time.hour == 9 and ny_time.minute >= 30 or ny_time.hour == 10 and ny_time.minute < 30:
                    if last_run["gapgo_early"] is None or (ny_time - last_run["gapgo_early"]).total_seconds() >= 60:
                        print("--- Running Gap & Go (Early Session) ---", flush=True)
                        run_job_in_thread('screeners/gapgo.py')
                        last_run["gapgo_early"] = ny_time
            
            # Main loop sleep
            time.sleep(10)

        except Exception as e:
            print(f"--- CRITICAL ERROR IN MAIN LOOP! ---\n{e}", flush=True)
            print("Restarting loop in 30 seconds...", flush=True)
            time.sleep(30)

if __name__ == "__main__":
    main()
