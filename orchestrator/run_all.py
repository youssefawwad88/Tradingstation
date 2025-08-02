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
# Set to True to run all jobs once sequentially for a full system test, then exit.
# Set to False for normal, 24/7 live operation.
TEST_MODE = True 

def run_job_in_thread(script_path, job_name):
    """
    Runs a Python script in a separate thread and logs its status.
    This is for jobs that can run in parallel without interfering with each other.
    """
    def job_runner():
        print(f"--- Thread starting for job: {job_name} ---")
        update_scheduler_status(job_name, "Running")
        try:
            # Use subprocess to run the script in an isolated process
            full_path = os.path.join('/workspace', script_path)
            result = subprocess.run(
                [sys.executable, full_path],
                check=True,
                capture_output=True,
                text=True,
                timeout=900  # 15-minute timeout for any single job
            )
            print(f"--- SUCCESS: {job_name} finished. ---")
            if result.stdout:
                print(f"--- STDOUT for {job_name} ---\n{result.stdout}\n--- End STDOUT ---")
            update_scheduler_status(job_name, "Success")
        except subprocess.TimeoutExpired:
            print(f"--- TIMEOUT: {job_name} failed. ---")
            update_scheduler_status(job_name, "Fail", "Job timed out after 15 minutes.")
        except subprocess.CalledProcessError as e:
            print(f"--- ERROR: {job_name} failed. ---")
            print(f"Script exited with error code {e.returncode}.")
            error_details = f"--- STDERR ---\n{e.stderr}"
            print(error_details)
            update_scheduler_status(job_name, "Fail", f"Exited with code {e.returncode}. See logs for details.")
        except Exception as e:
            print(f"--- UNEXPECTED ERROR in {job_name} thread: {e} ---")
            update_scheduler_status(job_name, "Fail", f"An unexpected error occurred: {e}")

    thread = threading.Thread(target=job_runner)
    thread.start()
    return thread

def main():
    """
    The main entry point for the trading station orchestrator.
    """
    print(f"--- Starting Master Orchestrator (Final Production Version) ---")
    
    if TEST_MODE:
        print("\n--- !!! RUNNING IN TEST MODE (SEQUENTIAL) !!! ---")
        print("--- This will run all jobs once in order and then exit. ---\n")
        
        # In test mode, we run everything sequentially to get clean logs.
        jobs_to_run = {
            "opportunity_ticker_finder": "ticker_selectors/opportunity_ticker_finder.py",
            "update_all_data": "jobs/update_all_data.py",
            "find_avwap_anchors": "jobs/find_avwap_anchors.py",
            "update_intraday_compact": "jobs/update_intraday_compact.py",
            "breakout": "screeners/breakout.py",
            "ema_pullback": "screeners/ema_pullback.py",
            "exhaustion": "screeners/exhaustion.py",
            "gapgo": "screeners/gapgo.py",
            "orb": "screeners/orb.py",
            "avwap": "screeners/avwap.py",
            "master_dashboard": "dashboard/master_dashboard.py"
        }

        for job_name, script_path in jobs_to_run.items():
            run_job_in_thread(script_path, job_name).join() # .join() waits for the thread to complete

        print("\n--- Diagnostic test run finished. Exiting. ---")
        return

    # --- LIVE OPERATION LOOP ---
    print("--- Entering main operational loop for live 24/7 trading. ---")
    last_run = {job: None for job in ["daily_rebuild", "daily_screeners", "intraday_1min", "intraday_15min"]}

    while True:
        try:
            now = datetime.now(pytz.utc)
            ny_time = now.astimezone(pytz.timezone("America/New_York"))
            market_session = detect_market_session()

            print(f"Heartbeat: Loop running. NY Time: {ny_time.strftime('%H:%M:%S')}. Market Session: {market_session}")

            # --- Daily Full Rebuild (Once a day, e.g., at 6 AM ET) ---
            if ny_time.hour == 6 and (last_run["daily_rebuild"] is None or last_run["daily_rebuild"].date() != now.date()):
                print("\n--- Triggering Daily Full Rebuild Job ---")
                run_job_in_thread("jobs/update_all_data.py", "update_all_data")
                run_job_in_thread("jobs/find_avwap_anchors.py", "find_avwap_anchors")
                last_run["daily_rebuild"] = now

            # --- Daily Screeners & Ticker Finder (Once a day, after rebuild) ---
            if ny_time.hour == 6 and ny_time.minute >= 5 and (last_run["daily_screeners"] is None or last_run["daily_screeners"].date() != now.date()):
                print("\n--- Triggering Daily Screeners & Ticker Finder ---")
                run_job_in_thread("ticker_selectors/opportunity_ticker_finder.py", "opportunity_ticker_finder")
                run_job_in_thread("screeners/breakout.py", "breakout")
                run_job_in_thread("screeners/ema_pullback.py", "ema_pullback")
                run_job_in_thread("screeners/exhaustion.py", "exhaustion")
                last_run["daily_screeners"] = now

            # --- 1-Minute Jobs (During market hours) ---
            if market_session in ["PRE-MARKET", "REGULAR"]:
                if last_run["intraday_1min"] is None or (now - last_run["intraday_1min"]) >= timedelta(minutes=1):
                    print("\n--- Triggering 1-Minute Intraday Jobs ---")
                    run_job_in_thread("jobs/update_intraday_compact.py", "update_intraday_compact")
                    run_job_in_thread("screeners/gapgo.py", "gapgo")
                    last_run["intraday_1min"] = now
            
            # --- 15-Minute Jobs (During regular session) ---
            if market_session == "REGULAR":
                if last_run["intraday_15min"] is None or (now - last_run["intraday_15min"]) >= timedelta(minutes=15):
                    print("\n--- Triggering 15-Minute Intraday Jobs ---")
                    run_job_in_thread("screeners/orb.py", "orb")
                    run_job_in_thread("screeners/avwap.py", "avwap")
                    run_job_in_thread("dashboard/master_dashboard.py", "master_dashboard")
                    last_run["intraday_15min"] = now

            time.sleep(15)  # Main loop sleeps for 15 seconds

        except Exception as e:
            print(f"--- CRITICAL ERROR IN MAIN ORCHESTRATOR LOOP: {e} ---")
            print("--- Restarting loop in 30 seconds... ---")
            time.sleep(30)

if __name__ == "__main__":
    # A simple wrapper to ensure the orchestrator restarts if it ever exits unexpectedly
    try:
        main()
    except Exception as e:
        print(f"Orchestrator 'main' function crashed with unhandled exception: {e}")
        # In a real production system, this could trigger an external alert.
