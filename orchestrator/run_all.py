import schedule
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
    # Define dummy functions to prevent immediate crash, though functionality is lost.
    def update_scheduler_status(job_name, status, details=""): pass
    def detect_market_session(): return "CLOSED"

# --- Job Execution Logic ---
def run_job(script_path):
    """
    Runs a script as a subprocess with robust logging.
    This function is now designed to be called in a thread.
    """
    job_name = os.path.basename(script_path).replace('.py', '')
    full_path = os.path.join('/workspace', script_path)
    
    # This function will be the target of a thread
    def job_logic():
        print(f"--- Thread starting for job: {job_name} ---", flush=True)
        update_scheduler_status(job_name, "Running")
        try:
            process = subprocess.run(
                [sys.executable, full_path],
                capture_output=True, text=True, check=True,
                timeout=600, env=os.environ
            )
            details = f"Completed successfully. Output:\n{process.stdout}"
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

    # Create and start the thread
    thread = threading.Thread(target=job_logic, name=job_name)
    thread.start()

# --- Main Orchestrator Logic ---
def schedule_jobs():
    """Sets up all scheduled jobs with market-aware logic."""
    print("Setting up job schedule...", flush=True)
    
    # --- Daily Setup ---
    schedule.every().day.at("08:00", "America/New_York").do(run_job, 'jobs/update_all_data.py')
    schedule.every().day.at("08:05", "America/New_York").do(run_job, 'screeners/breakout.py')
    schedule.every().day.at("08:05", "America/New_York").do(run_job, 'screeners/ema_pullback.py')
    schedule.every().day.at("08:05", "America/New_York").do(run_job, 'screeners/exhaustion.py')

    # --- High-Frequency and Intraday Jobs ---
    # These will be checked every minute by the main loop, but only run if the session is right.
    schedule.every(1).minutes.do(run_job, 'jobs/update_intraday_compact.py').tag('intraday_data')
    schedule.every(1).minutes.do(run_job, 'screeners/gapgo.py').tag('gapgo_early')
    schedule.every(15).minutes.do(run_job, 'screeners/gapgo.py').tag('gapgo_premarket')
    schedule.every(15).minutes.do(run_job, 'screeners/orb.py').tag('intraday_screeners')
    schedule.every(15).minutes.do(run_job, 'screeners/avwap.py').tag('intraday_screeners')
    
    print("✅ Schedule setup complete.", flush=True)

def main():
    """
    Main orchestrator entry point with a robust loop.
    """
    schedule_jobs()
    update_scheduler_status("orchestrator", "Success", "System online and scheduler running.")
    
    while True:
        try:
            session = detect_market_session()
            ny_time = datetime.now(pytz.timezone('America/New_York')).time()

            # Run all pending jobs that don't have specific session tags
            schedule.run_pending()

            # --- Market-Aware Job Execution ---
            # CORRECTED LOGIC: Iterate through jobs and run them based on tags.
            if session == 'PRE-MARKET':
                for job in schedule.get_jobs('intraday_data'):
                    schedule.run_job(job)
                for job in schedule.get_jobs('gapgo_premarket'):
                    schedule.run_job(job)

            elif session == 'REGULAR':
                for job in schedule.get_jobs('intraday_data'):
                    schedule.run_job(job)
                for job in schedule.get_jobs('intraday_screeners'):
                    schedule.run_job(job)
                
                if (ny_time.hour == 9 and ny_time.minute >= 30) or (ny_time.hour == 10 and ny_time.minute < 30):
                    for job in schedule.get_jobs('gapgo_early'):
                        schedule.run_job(job)

            # Sleep for a short duration to prevent high CPU usage
            time.sleep(5)

        except Exception as e:
            print(f"--- CRITICAL ERROR IN MAIN LOOP! ---", flush=True)
            print(f"Error: {e}", flush=True)
            print("Restarting loop in 15 seconds...", flush=True)
            time.sleep(15)

if __name__ == "__main__":
    main()
